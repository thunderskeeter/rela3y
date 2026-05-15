const crypto = require('crypto');
const zlib = require('zlib');
const { CAL_OAUTH_REDIRECT_BASE } = require('../config/runtime');
const { sendEmailCampaign } = require('./emailDeliveryService');
const {
  loadData,
  saveDataDebounced,
  getAccountById,
  getConversation
} = require('../store/dataStore');

function ensureCustomerBillingStore(account) {
  account.customerBilling = account.customerBilling && typeof account.customerBilling === 'object'
    ? account.customerBilling
    : {};
  account.customerBilling.invoices = Array.isArray(account.customerBilling.invoices)
    ? account.customerBilling.invoices
    : [];
  return account.customerBilling;
}

function normalizePhone(raw) {
  const input = String(raw || '').trim();
  if (!input) return '';
  const digits = input.replace(/[^\d+]/g, '');
  if (!digits) return '';
  if (digits.startsWith('+')) return digits;
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  return `+${digits}`;
}

function parseNumericAmount(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'string') {
    const cleaned = value.replace(/[^0-9.\-]/g, '');
    if (!cleaned) return null;
    const n = Number(cleaned);
    if (!Number.isFinite(n) || n <= 0) return null;
    if (n >= 10000 && Number.isInteger(n) && n % 100 === 0) return n / 100;
    return n;
  }
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  if (n >= 10000 && Number.isInteger(n) && n % 100 === 0) return n / 100;
  return n;
}

function extractDollarAmountFromText(text) {
  const s = String(text || '');
  const m = s.match(/\$(\d{1,6}(?:\.\d{1,2})?)/);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function resolveConversationAmount(convo) {
  const ld = convo?.leadData || {};
  const candidates = [
    convo?.amount,
    convo?.bookingAmount,
    convo?.booking_amount,
    ld?.amount,
    ld?.price,
    ld?.quoted_amount,
    ld?.estimate_amount,
    ld?.booking_amount,
    ld?.invoice_amount,
    ld?.final_amount,
    ld?.total
  ];
  for (const c of candidates) {
    const n = parseNumericAmount(c);
    if (Number.isFinite(n) && n > 0) return n;
  }
  const messages = Array.isArray(convo?.messages) ? convo.messages : [];
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const m = messages[i] || {};
    const payloadCandidates = [
      m?.amount,
      m?.meta?.amount,
      m?.meta?.amountCents
    ];
    for (const c of payloadCandidates) {
      const n = parseNumericAmount(c);
      if (Number.isFinite(n) && n > 0) return n;
    }
    const textAmount = extractDollarAmountFromText(m?.text || m?.body || '');
    if (Number.isFinite(textAmount) && textAmount > 0) return textAmount;
  }
  return null;
}

function normalizePaymentMethod(raw) {
  const v = String(raw || '').trim().toLowerCase();
  if (!v) return '';
  if (v.includes('cash')) return 'cash';
  if (v.includes('card') || v.includes('credit') || v.includes('stripe')) return 'card';
  return v;
}

function isPaidState(raw) {
  const v = String(raw || '').trim().toLowerCase();
  return v === 'paid' || v === 'succeeded' || v === 'complete' || v === 'completed' || v === 'captured' || v === 'settled';
}

function resolvePaymentMeta(convo) {
  const ld = convo?.leadData || {};
  let method = normalizePaymentMethod(ld?.payment_method || ld?.paymentMethod || convo?.paymentMethod || convo?.payment_method);
  let paid = isPaidState(ld?.payment_status || ld?.paymentStatus || convo?.paymentStatus || convo?.payment_status);
  const messages = Array.isArray(convo?.messages) ? convo.messages : [];
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const m = messages[i] || {};
    if (!method) {
      method = normalizePaymentMethod(
        m?.meta?.paymentMethod
        || m?.meta?.payment_method
      );
    }
    if (!paid) {
      const statusRaw =
        m?.meta?.paymentStatus
        || m?.meta?.payment_status;
      paid = isPaidState(statusRaw) || m?.meta?.paid === true || m?.meta?.paymentSucceeded === true;
    }
    if (method && paid) break;
  }
  const status = (paid || method === 'cash') ? 'paid' : 'open';
  return { method: method || 'unknown', status };
}

function resolveInvoiceLifecycleStatus(convo) {
  const status = String(convo?.status || '').trim().toLowerCase();
  const stage = String(convo?.stage || '').trim().toLowerCase();
  if (status === 'closed' || stage === 'closed') return 'close';
  if (status === 'booked' || /booked|appointment_booked|scheduled/.test(stage) || Number.isFinite(Number(convo?.bookingTime || 0))) {
    return 'booked';
  }
  return 'open';
}

function humanizeServiceLabel(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (raw.includes(' ')) return raw;
  return raw
    .replace(/_/g, ' ')
    .split(' ')
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

function parseSummaryLines(value) {
  const raw = String(value || '').trim();
  if (!raw) return [];
  return raw
    .split(/\r?\n/g)
    .map((line) => String(line || '').replace(/^\s*[-*]\s*/, '').trim())
    .filter(Boolean)
    .map(humanizeServiceLabel);
}

function splitServices(value) {
  return String(value || '')
    .split(/\+|,| and /i)
    .map((x) => humanizeServiceLabel(x))
    .filter(Boolean);
}

function resolveConversationServiceItems(convo) {
  const ld = convo?.leadData || {};
  const items = [];
  const fromList = Array.isArray(ld?.services_list) ? ld.services_list : [];
  for (const v of fromList) items.push(humanizeServiceLabel(v));
  items.push(...parseSummaryLines(ld?.services_summary));
  items.push(...splitServices(ld?.service_required || ld?.request || ld?.intent || convo?.service || ''));
  const unique = [];
  const seen = new Set();
  for (const item of items) {
    const key = String(item || '').trim().toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    unique.push(String(item).trim());
  }
  if (!unique.length) return ['Service request'];
  return unique.slice(0, 8);
}

function safeInvoiceNumber(ts) {
  const base = String(Number(ts || Date.now())).replace(/\D/g, '');
  return `INV-${base.slice(-8)}`;
}

function renderUsdFromCents(cents) {
  const amount = Number(cents || 0) / 100;
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(Number.isFinite(amount) ? amount : 0);
}

function stripeAuthHeader(secretKey) {
  const token = Buffer.from(`${String(secretKey || '').trim()}:`, 'utf8').toString('base64');
  return `Basic ${token}`;
}

async function stripeRequest(secretKey, path, { method = 'GET', form = null } = {}) {
  const body = form ? new URLSearchParams(form).toString() : undefined;
  const res = await fetch(`https://api.stripe.com${path}`, {
    method,
    headers: {
      Authorization: stripeAuthHeader(secretKey),
      Accept: 'application/json',
      ...(body ? { 'Content-Type': 'application/x-www-form-urlencoded' } : {})
    },
    body
  });
  const raw = await res.text();
  let parsed = {};
  try { parsed = JSON.parse(raw); } catch {}
  if (!res.ok) {
    const detail = String(parsed?.error?.message || parsed?.message || '').trim();
    throw new Error(detail || `Stripe request failed (${res.status})`);
  }
  return parsed;
}

function resolveTenantStripeConfig(account) {
  const cfg = account?.integrations?.stripe && typeof account.integrations.stripe === 'object'
    ? account.integrations.stripe
    : {};
  const secretKey = String(cfg.secretKey || '').trim();
  if (cfg.enabled !== true || !secretKey) return null;
  if (!/^sk_(test|live)_[A-Za-z0-9]+$/.test(secretKey)) return null;
  return cfg;
}

function appendUrlParams(url, params = {}) {
  const parsed = new URL(String(url || 'http://127.0.0.1:3001'));
  for (const [key, value] of Object.entries(params)) {
    parsed.searchParams.set(key, String(value || ''));
  }
  return parsed.toString();
}

async function ensureStripeCheckoutForCustomerInvoice({
  account,
  invoice,
  accountId,
  to,
  returnUrl = ''
}) {
  const amountCents = Math.max(0, Math.round(Number(invoice?.amountCents || 0)));
  if (!amountCents) return { available: false, reason: 'missing_amount' };
  if (String(invoice?.paymentStatus || '').toLowerCase() === 'paid') {
    return { available: false, reason: 'already_paid' };
  }
  const cfg = resolveTenantStripeConfig(account);
  if (!cfg) return { available: false, reason: 'stripe_not_connected' };
  if (invoice?.payment?.provider === 'stripe_checkout' && String(invoice?.payment?.url || '').trim()) {
    return { available: true, payment: invoice.payment };
  }

  const base = String(CAL_OAUTH_REDIRECT_BASE || '').replace(/\/$/, '') || 'http://127.0.0.1:3001';
  const fallbackReturn = `${base}/api/public/invoice/${encodeURIComponent(String(invoice?.pdfToken || ''))}/pdf`;
  const redirectBase = String(returnUrl || '').trim() || fallbackReturn;
  const successUrl = appendUrlParams(redirectBase, { payment: 'success', invoice: String(invoice?.id || '') });
  const cancelUrl = appendUrlParams(redirectBase, { payment: 'cancel', invoice: String(invoice?.id || '') });
  const customerEmail = String(invoice?.email || '').trim().toLowerCase();

  const session = await stripeRequest(cfg.secretKey, '/v1/checkout/sessions', {
    method: 'POST',
    form: {
      mode: 'payment',
      success_url: successUrl,
      cancel_url: cancelUrl,
      ...(customerEmail ? { customer_email: customerEmail } : {}),
      'line_items[0][quantity]': '1',
      'line_items[0][price_data][currency]': 'usd',
      'line_items[0][price_data][unit_amount]': String(amountCents),
      'line_items[0][price_data][product_data][name]': String(invoice?.service || 'Service booking').trim() || 'Service booking',
      'metadata[accountId]': String(accountId || ''),
      'metadata[to]': String(to || ''),
      'metadata[invoiceId]': String(invoice?.id || ''),
      'metadata[invoiceNumber]': String(invoice?.invoiceNumber || ''),
      'metadata[conversationId]': String(invoice?.conversationId || '')
    }
  });

  invoice.payment = {
    provider: 'stripe_checkout',
    status: 'open',
    checkoutSessionId: String(session?.id || ''),
    url: String(session?.url || ''),
    amountCents,
    currency: 'usd',
    createdAt: Date.now()
  };
  invoice.updatedAt = Date.now();
  return { available: true, payment: invoice.payment };
}

function escapePdfText(value) {
  return String(value || '')
    .replace(/\\/g, '\\\\')
    .replace(/\(/g, '\\(')
    .replace(/\)/g, '\\)');
}

function makePdfObj(index, body) {
  const bodyBuf = Buffer.isBuffer(body) ? body : Buffer.from(String(body || ''), 'binary');
  return Buffer.concat([
    Buffer.from(`${index} 0 obj\n`, 'ascii'),
    bodyBuf,
    Buffer.from('\nendobj\n', 'ascii')
  ]);
}

function buildSimplePdf(contentStream, imageSpec = null) {
  const objects = [];
  objects.push(makePdfObj(1, '<< /Type /Catalog /Pages 2 0 R >>'));
  objects.push(makePdfObj(2, '<< /Type /Pages /Kids [3 0 R] /Count 1 >>'));

  const hasImage = Boolean(imageSpec && imageSpec.buffer && imageSpec.width > 0 && imageSpec.height > 0);
  const xObjRef = hasImage ? ' /XObject << /Im1 6 0 R >>' : '';
  objects.push(makePdfObj(
    3,
    `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >>${xObjRef} >> >>`
  ));

  const contentBuf = Buffer.from(String(contentStream || ''), 'utf8');
  objects.push(makePdfObj(4, Buffer.concat([
    Buffer.from(`<< /Length ${contentBuf.length} >>\nstream\n`, 'ascii'),
    contentBuf,
    Buffer.from('\nendstream', 'ascii')
  ])));
  objects.push(makePdfObj(5, '<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>'));

  if (hasImage) {
    const filter = imageSpec.filter || '/FlateDecode';
    const colorSpace = imageSpec.colorSpace || '/DeviceRGB';
    const bits = Number(imageSpec.bitsPerComponent || 8);
    const imageDict = [
      '<< /Type /XObject',
      '/Subtype /Image',
      `/Width ${Number(imageSpec.width)}`,
      `/Height ${Number(imageSpec.height)}`,
      `/ColorSpace ${colorSpace}`,
      `/BitsPerComponent ${bits}`,
      `/Filter ${filter}`,
      `/Length ${imageSpec.buffer.length}`,
      '>>'
    ].join(' ');
    objects.push(makePdfObj(6, Buffer.concat([
      Buffer.from(`${imageDict}\nstream\n`, 'ascii'),
      imageSpec.buffer,
      Buffer.from('\nendstream', 'ascii')
    ])));
  }

  const header = Buffer.from('%PDF-1.4\n', 'ascii');
  const parts = [header];
  const offsets = [0];
  let cursor = header.length;
  for (const obj of objects) {
    offsets.push(cursor);
    parts.push(obj);
    cursor += obj.length;
  }
  const xrefPos = cursor;
  const lines = [`xref\n0 ${objects.length + 1}\n`, '0000000000 65535 f \n'];
  for (let i = 1; i < offsets.length; i += 1) {
    lines.push(`${String(offsets[i]).padStart(10, '0')} 00000 n \n`);
  }
  lines.push(`trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xrefPos}\n%%EOF\n`);
  parts.push(Buffer.from(lines.join(''), 'ascii'));
  return Buffer.concat(parts);
}

function parsePngChunked(buffer) {
  const sig = Buffer.from([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]);
  if (!Buffer.isBuffer(buffer) || buffer.length < 8 || !buffer.subarray(0, 8).equals(sig)) return null;
  let off = 8;
  let width = 0;
  let height = 0;
  let bitDepth = 0;
  let colorType = 0;
  const idat = [];
  while (off + 12 <= buffer.length) {
    const len = buffer.readUInt32BE(off); off += 4;
    const type = buffer.subarray(off, off + 4).toString('ascii'); off += 4;
    if (off + len + 4 > buffer.length) return null;
    const data = buffer.subarray(off, off + len); off += len;
    off += 4; // crc
    if (type === 'IHDR') {
      width = data.readUInt32BE(0);
      height = data.readUInt32BE(4);
      bitDepth = Number(data[8] || 0);
      colorType = Number(data[9] || 0);
    } else if (type === 'IDAT') {
      idat.push(data);
    } else if (type === 'IEND') {
      break;
    }
  }
  if (!width || !height || !idat.length) return null;
  if (bitDepth !== 8) return null;
  if (!(colorType === 2 || colorType === 6)) return null;
  const compressed = Buffer.concat(idat);
  const inflated = zlib.inflateSync(compressed);
  const bpp = colorType === 6 ? 4 : 3;
  const stride = width * bpp;
  const expected = (stride + 1) * height;
  if (inflated.length < expected) return null;
  const raw = Buffer.alloc(stride * height);
  const alpha = colorType === 6 ? Buffer.alloc(width * height) : null;

  function paeth(a, b, c) {
    const p = a + b - c;
    const pa = Math.abs(p - a);
    const pb = Math.abs(p - b);
    const pc = Math.abs(p - c);
    if (pa <= pb && pa <= pc) return a;
    if (pb <= pc) return b;
    return c;
  }

  for (let y = 0; y < height; y += 1) {
    const rowIn = y * (stride + 1);
    const filter = inflated[rowIn];
    const src = inflated.subarray(rowIn + 1, rowIn + 1 + stride);
    const rowOut = Buffer.alloc(stride);
    for (let x = 0; x < stride; x += 1) {
      const left = x >= bpp ? rowOut[x - bpp] : 0;
      const up = y > 0 ? raw[(y - 1) * stride + x] : 0;
      const upLeft = (y > 0 && x >= bpp) ? raw[(y - 1) * stride + x - bpp] : 0;
      let v = src[x];
      if (filter === 1) v = (v + left) & 0xFF;
      else if (filter === 2) v = (v + up) & 0xFF;
      else if (filter === 3) v = (v + Math.floor((left + up) / 2)) & 0xFF;
      else if (filter === 4) v = (v + paeth(left, up, upLeft)) & 0xFF;
      rowOut[x] = v;
    }
    rowOut.copy(raw, y * stride);
  }

  if (colorType === 2) {
    return {
      width,
      height,
      image: zlib.deflateSync(raw),
      alpha: null
    };
  }

  const rgb = Buffer.alloc(width * height * 3);
  for (let i = 0, p = 0, a = 0; i < raw.length; i += 4) {
    rgb[p++] = raw[i];
    rgb[p++] = raw[i + 1];
    rgb[p++] = raw[i + 2];
    alpha[a++] = raw[i + 3];
  }
  return {
    width,
    height,
    image: zlib.deflateSync(rgb),
    alpha: zlib.deflateSync(alpha)
  };
}

function resolveInvoiceLogoImage(account) {
  const asset = account?.workspace?.identity?.logoAsset;
  const mimeType = String(asset?.mimeType || '').trim().toLowerCase();
  const dataBase64 = String(asset?.dataBase64 || asset?.base64 || '').trim();
  if (!mimeType || !dataBase64) return null;
  let bin = null;
  try {
    bin = Buffer.from(dataBase64, 'base64');
  } catch {
    return null;
  }
  if (!bin || !bin.length) return null;
  const width = Number(asset?.width || 0);
  const height = Number(asset?.height || 0);
  if (mimeType === 'image/jpeg' || mimeType === 'image/jpg') {
    return {
      width: Math.max(1, width || 320),
      height: Math.max(1, height || 80),
      buffer: bin,
      filter: '/DCTDecode',
      colorSpace: '/DeviceRGB',
      bitsPerComponent: 8
    };
  }
  if (mimeType === 'image/png') {
    const parsed = parsePngChunked(bin);
    if (!parsed) return null;
    return {
      width: parsed.width,
      height: parsed.height,
      buffer: parsed.image,
      filter: '/FlateDecode',
      colorSpace: '/DeviceRGB',
      bitsPerComponent: 8
    };
  }
  return null;
}

function buildInvoicePdfBuffer({ account, invoice }) {
  const businessName = String(account?.workspace?.identity?.businessName || account?.businessName || 'Business').trim();
  const issuedAt = Number(invoice?.createdAt || invoice?.bookedAt || Date.now());
  const bookedAt = Number(invoice?.bookedAt || 0);
  const invoiceNumber = String(invoice?.invoiceNumber || invoice?.id || '--');
  const customerLine = `${String(invoice?.customerName || 'Unknown')} ${invoice?.phone ? `(${String(invoice.phone)})` : ''}`.trim();
  const serviceItems = Array.isArray(invoice?.serviceItems) && invoice.serviceItems.length
    ? invoice.serviceItems.map((x) => String(x || '').trim()).filter(Boolean).slice(0, 6)
    : [String(invoice?.service || 'Service request').trim() || 'Service request'];
  const logoImage = resolveInvoiceLogoImage(account);
  const ops = [];

  // Header background stripe
  ops.push('0.97 g 0 736 612 56 re f 0 g');
  // Title
  ops.push(`BT /F1 24 Tf 50 760 Td (${escapePdfText(businessName)}) Tj ET`);
  ops.push('BT /F1 11 Tf 50 742 Td (INVOICE) Tj ET');

  // Header logo slot
  ops.push('0.92 g 430 742 132 34 re f 0.75 G 430 742 132 34 re S 0 g');
  if (logoImage) {
    const boxW = 126;
    const boxH = 28;
    const x = 433;
    const y = 745;
    const scale = Math.min(boxW / Number(logoImage.width), boxH / Number(logoImage.height));
    const drawW = Math.max(1, Number(logoImage.width) * scale);
    const drawH = Math.max(1, Number(logoImage.height) * scale);
    const drawX = x + ((boxW - drawW) / 2);
    const drawY = y + ((boxH - drawH) / 2);
    ops.push('q');
    ops.push(`${drawW.toFixed(2)} 0 0 ${drawH.toFixed(2)} ${drawX.toFixed(2)} ${drawY.toFixed(2)} cm`);
    ops.push('/Im1 Do');
    ops.push('Q');
  } else {
    const headerWordmark = String(account?.workspace?.identity?.businessName || account?.businessName || 'Relay')
      .trim()
      .slice(0, 24);
    ops.push(`BT /F1 10 Tf 436 755 Td (${escapePdfText(headerWordmark || 'Relay')}) Tj ET`);
  }

  // Invoice meta card
  ops.push('0.98 g 50 668 512 58 re f 0.8 G 50 668 512 58 re S 0 g');
  ops.push(`BT /F1 11 Tf 62 706 Td (Invoice #: ${escapePdfText(invoiceNumber)}) Tj ET`);
  ops.push(`BT /F1 11 Tf 62 690 Td (Issued: ${escapePdfText(new Date(issuedAt).toLocaleString())}) Tj ET`);
  ops.push(`BT /F1 11 Tf 320 706 Td (Status: ${escapePdfText(String(invoice?.status || 'open').toUpperCase())}) Tj ET`);
  ops.push(`BT /F1 11 Tf 320 690 Td (Booked: ${escapePdfText(bookedAt ? new Date(bookedAt).toLocaleString() : '--')}) Tj ET`);

  // Billing details
  ops.push('BT /F1 10 Tf 50 640 Td (BILL TO) Tj ET');
  ops.push(`BT /F1 12 Tf 50 622 Td (${escapePdfText(customerLine)}) Tj ET`);
  if (String(invoice?.email || '').trim()) {
    ops.push(`BT /F1 10 Tf 50 606 Td (${escapePdfText(String(invoice.email).trim())}) Tj ET`);
  }

  // Line item box
  ops.push('0.97 g 50 528 512 88 re f 0.8 G 50 528 512 88 re S 0 g');
  ops.push('BT /F1 10 Tf 62 600 Td (SERVICE REQUEST) Tj ET');
  ops.push('BT /F1 10 Tf 430 600 Td (AMOUNT) Tj ET');
  let serviceY = 582;
  for (const item of serviceItems) {
    if (serviceY < 542) break;
    ops.push(`BT /F1 11 Tf 62 ${serviceY} Td (${escapePdfText(`- ${item}`)}) Tj ET`);
    serviceY -= 14;
  }
  ops.push(`BT /F1 12 Tf 430 566 Td (${escapePdfText(renderUsdFromCents(Number(invoice?.amountCents || 0)))}) Tj ET`);

  // Payment summary
  ops.push('0.95 G 50 506 m 562 506 l S 0 g');
  ops.push(`BT /F1 11 Tf 50 488 Td (Payment Method: ${escapePdfText(String(invoice?.paymentMethod || 'unknown').toUpperCase())}) Tj ET`);
  ops.push(`BT /F1 16 Tf 420 486 Td (TOTAL ${escapePdfText(renderUsdFromCents(Number(invoice?.amountCents || 0)))}) Tj ET`);

  // Footer
  ops.push('BT /F1 9 Tf 50 60 Td (Thank you for your business.) Tj ET');
  ops.push('BT /F1 8 Tf 50 46 Td (Generated by Relay Customer Billing) Tj ET');

  return buildSimplePdf(ops.join('\n'), logoImage);
}

function getCustomerInvoiceByPdfToken(token) {
  const data = loadData();
  const t = String(token || '').trim();
  if (!t) return null;
  for (const [to, account] of Object.entries(data.accounts || {})) {
    const billing = ensureCustomerBillingStore(account);
    const invoice = billing.invoices.find((inv) => String(inv?.pdfToken || '') === t);
    if (invoice) return { to, account, invoice };
  }
  return null;
}

function listCustomerInvoices(accountId) {
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  if (!accountRef?.account) return [];
  const billing = ensureCustomerBillingStore(accountRef.account);
  return billing.invoices
    .slice()
    .sort((a, b) => Number(b?.bookedAt || b?.createdAt || 0) - Number(a?.bookedAt || a?.createdAt || 0));
}

async function ensureInvoiceForBookedConversation({
  accountId,
  to,
  from,
  bookingStart = 0,
  bookingEnd = 0,
  bookingId = '',
  source = 'booking',
  customerPaymentReturnUrl = ''
}) {
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  if (!accountRef?.account) return { ok: false, reason: 'account_not_found' };
  const account = accountRef.account;
  const convo = getConversation(data, String(to || ''), String(from || ''), accountId, false);
  if (!convo) return { ok: false, reason: 'conversation_not_found' };

  const billing = ensureCustomerBillingStore(account);
  const ld = convo?.leadData || {};
  const bookingTs = Number(bookingStart || convo?.bookingTime || ld?.booking_time || Date.now()) || Date.now();
  const dedupeBookingId = String(bookingId || ld?.booking_id || '').trim();
  const conversationId = `${String(to)}__${String(from)}`;
  let invoice = billing.invoices.find((inv) => {
    if (dedupeBookingId && String(inv?.bookingId || '') === dedupeBookingId) return true;
    return String(inv?.conversationId || '') === conversationId && Math.abs(Number(inv?.bookedAt || 0) - bookingTs) < (10 * 60 * 1000);
  });

  const amount = resolveConversationAmount(convo);
  const amountCents = Math.max(0, Math.round(Number(amount || 0) * 100));
  const payment = resolvePaymentMeta(convo);
  const lifecycleStatus = resolveInvoiceLifecycleStatus(convo);
  const serviceItems = resolveConversationServiceItems(convo);
  const customerName = String(ld?.customer_name || '').trim() || 'Customer';
  const customerPhone = normalizePhone(from || convo?.from || ld?.customer_phone || '');
  const customerEmail = String(ld?.customer_email || ld?.email || '').trim().toLowerCase();
  const service = String(serviceItems[0] || 'Service request').trim();

  if (!invoice) {
    invoice = {
      id: `cinv_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`,
      invoiceNumber: safeInvoiceNumber(bookingTs),
      bookingId: dedupeBookingId || null,
      conversationId,
      source: String(source || 'booking'),
      customerName,
      phone: customerPhone,
      email: customerEmail,
      service,
      serviceItems,
      amountCents,
      bookedAt: bookingTs,
      bookingEndAt: Number(bookingEnd || convo?.bookingEndTime || ld?.booking_end_time || 0) || null,
      paymentMethod: payment.method,
      paymentStatus: payment.status,
      status: lifecycleStatus,
      pdfToken: crypto.randomBytes(18).toString('hex'),
      createdAt: Date.now(),
      updatedAt: Date.now(),
      emailSentAt: null,
      emailLastError: '',
      pdfGeneratedAt: null,
      pdfLastError: ''
    };
    billing.invoices.unshift(invoice);
    billing.invoices = billing.invoices.slice(0, 1000);
  } else {
    invoice.customerName = customerName || invoice.customerName;
    invoice.phone = customerPhone || invoice.phone;
    invoice.email = customerEmail || invoice.email;
    invoice.service = service || invoice.service;
    invoice.serviceItems = Array.isArray(serviceItems) && serviceItems.length ? serviceItems : (Array.isArray(invoice.serviceItems) ? invoice.serviceItems : []);
    if (amountCents > 0) invoice.amountCents = amountCents;
    invoice.paymentMethod = payment.method || invoice.paymentMethod || 'unknown';
    invoice.paymentStatus = payment.status || invoice.paymentStatus || 'open';
    invoice.status = lifecycleStatus || invoice.status || 'open';
    invoice.updatedAt = Date.now();
  }

  let pdfGenerated = false;
  let pdfError = '';
  try {
    buildInvoicePdfBuffer({ account, invoice });
    invoice.pdfGeneratedAt = Date.now();
    invoice.pdfLastError = '';
    pdfGenerated = true;
  } catch (err) {
    pdfError = String(err?.message || 'pdf_generation_failed');
    invoice.pdfLastError = pdfError;
  }

  let emailDelivery = null;
  const base = String(CAL_OAUTH_REDIRECT_BASE || '').replace(/\/$/, '') || '';
  const pdfUrl = `${base}/api/public/invoice/${encodeURIComponent(String(invoice.pdfToken || ''))}/pdf`;
  let paymentLink = { available: false, reason: 'not_attempted' };
  try {
    paymentLink = await ensureStripeCheckoutForCustomerInvoice({
      account,
      invoice,
      accountId,
      to,
      returnUrl: customerPaymentReturnUrl
    });
  } catch (err) {
    paymentLink = { available: false, reason: String(err?.message || 'stripe_checkout_failed') };
    invoice.paymentLastError = paymentLink.reason;
    invoice.updatedAt = Date.now();
  }

  if (customerEmail && !invoice.emailSentAt) {
    const businessName = String(account?.workspace?.identity?.businessName || account?.businessName || 'Business').trim();
    const identity = account?.workspace?.identity && typeof account.workspace.identity === 'object'
      ? account.workspace.identity
      : {};
    const logoAsset = identity?.logoAsset && typeof identity.logoAsset === 'object' ? identity.logoAsset : null;
    const logoMime = String(logoAsset?.mimeType || '').trim().toLowerCase();
    const logoBase64 = String(logoAsset?.dataBase64 || logoAsset?.base64 || '').trim();
    const logoDataUrl = (logoMime.startsWith('image/') && logoBase64 && logoBase64.length <= (1024 * 1024 * 2))
      ? `data:${logoMime};base64,${logoBase64}`
      : '';
    const logoUrl = String(identity?.logoUrl || '').trim();
    const fallbackLogoUrl = base ? `${base}/logos/main.png` : '';
    const subject = `${businessName} invoice ${invoice.invoiceNumber}`;
    const body = [
      `Hi ${customerName},`,
      '',
      `Thanks for booking with ${businessName}.`,
      `Invoice: ${invoice.invoiceNumber}`,
      `Service: ${invoice.service}`,
      `Amount: ${renderUsdFromCents(Number(invoice.amountCents || 0))}`,
      `Booked: ${new Date(Number(invoice.bookedAt || Date.now())).toLocaleString()}`,
      '',
      invoice?.payment?.url ? `Pay securely by card: ${invoice.payment.url}` : '',
      invoice?.payment?.url ? '' : '',
      `Download your invoice PDF: ${pdfUrl}`
    ].filter((line, index, arr) => line !== '' || arr[index - 1] !== '').join('\n');
    try {
      const delivery = await sendEmailCampaign({
        subject,
        body,
        recipients: [{ email: customerEmail, name: customerName, phone: customerPhone }],
        fromEmailCandidate: String(identity?.businessEmail || account?.billing?.details?.billingEmail || account?.email || '').trim(),
        fromNameCandidate: businessName,
        branding: {
          brandName: businessName,
          logoDataUrl,
          logoUrl: logoUrl || fallbackLogoUrl
        }
      });
      emailDelivery = {
        attempted: Number(delivery?.attempted || 0),
        delivered: Number(delivery?.delivered || 0),
        failed: Number(delivery?.failed || 0),
        provider: String(delivery?.provider || ''),
        firstError: String(delivery?.failures?.[0]?.error || '')
      };
      if (Number(delivery?.delivered || 0) > 0) {
        invoice.emailSentAt = Date.now();
        invoice.emailLastError = '';
      } else {
        invoice.emailLastError = String(delivery?.failures?.[0]?.error || 'email_send_failed');
      }
    } catch (err) {
      invoice.emailLastError = String(err?.message || 'email_send_failed');
      emailDelivery = {
        attempted: 1,
        delivered: 0,
        failed: 1,
        provider: '',
        firstError: String(err?.message || 'email_send_failed')
      };
    }
    invoice.updatedAt = Date.now();
  } else {
    emailDelivery = {
      attempted: customerEmail ? 0 : 0,
      delivered: 0,
      failed: 0,
      provider: '',
      firstError: customerEmail ? '' : 'missing_customer_email'
    };
  }

  saveDataDebounced(data);
  return {
    ok: true,
    invoice,
    pdf: {
      generated: pdfGenerated,
      url: pdfUrl,
      error: pdfError
    },
    email: emailDelivery,
    payment: {
      available: paymentLink.available === true,
      reason: String(paymentLink.reason || ''),
      provider: String(invoice?.payment?.provider || ''),
      status: String(invoice?.payment?.status || invoice?.paymentStatus || ''),
      url: String(invoice?.payment?.url || ''),
      amountCents: Number(invoice?.payment?.amountCents || invoice?.amountCents || 0),
      currency: String(invoice?.payment?.currency || 'usd')
    }
  };
}

function syncInvoiceLifecycleForConversation({ accountId, to, from, lifecycleStatus = '' } = {}) {
  const aid = String(accountId || '').trim();
  const toNum = String(to || '').trim();
  const fromNum = String(from || '').trim();
  if (!aid || !toNum || !fromNum) return { ok: false, reason: 'missing_params' };
  const data = loadData();
  const accountRef = getAccountById(data, aid);
  if (!accountRef?.account) return { ok: false, reason: 'account_not_found' };
  const account = accountRef.account;
  const billing = ensureCustomerBillingStore(account);
  const convo = getConversation(data, toNum, fromNum, aid, false);
  const nextStatus = String(lifecycleStatus || resolveInvoiceLifecycleStatus(convo || {})).trim().toLowerCase();
  const convoId = `${toNum}__${fromNum}`;
  const invoice = billing.invoices.find((inv) => String(inv?.conversationId || '') === convoId);
  if (!invoice) return { ok: false, reason: 'invoice_not_found' };
  if (nextStatus && String(invoice.status || '').toLowerCase() !== nextStatus) {
    invoice.status = nextStatus;
    invoice.updatedAt = Date.now();
    saveDataDebounced(data);
  }
  return { ok: true, invoiceId: String(invoice.id || '') };
}

module.exports = {
  ensureInvoiceForBookedConversation,
  listCustomerInvoices,
  getCustomerInvoiceByPdfToken,
  buildInvoicePdfBuffer,
  syncInvoiceLifecycleForConversation
};
