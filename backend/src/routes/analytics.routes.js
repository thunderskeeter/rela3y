const express = require('express');
const { getConversations, getContacts, loadData, getAccountById } = require('../store/dataStore');
const { z, validateBody, validateParams, validateQuery } = require('../utils/validate');
const { getAtRiskOpportunities, evaluateOpportunity } = require('../services/revenueIntelligenceService');
const { decideActionPlan } = require('../services/aiDecisionEngine');
const { runRecommendedAutomation } = require('../services/automationEngine');
const { computeWinsSummaryFromData } = require('../services/opsVisibilityService');
const { listCustomerInvoices, buildInvoicePdfBuffer } = require('../services/customerInvoiceService');
const { getFunnelMetrics, getOpportunityTimeline, getAgentMetrics } = require('../services/opportunitiesService');
const { getActivityFeed, getPlaybookPerformance } = require('../services/actionsService');

const analyticsRouter = express.Router();
const DAY_MS = 24 * 60 * 60 * 1000;
const STAGE_RANK = {
  ask_service: 1,
  wait_for_service: 1,
  detect_intent: 2,
  summarize_services: 2,
  wait_for_vehicle: 2,
  validate_vehicle_claude: 3,
  ask_vehicle_again: 2,
  send_booking: 4,
  booked: 5,
  closed: 6
};

const summaryQuerySchema = z.object({
  range: z.coerce.number().int().optional()
});

const limitQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).optional().default(50)
});

const rangeQuerySchema = z.object({
  range: z.union([
    z.string().trim().regex(/^(\d{1,3}d|\d{1,3})$/i),
    z.coerce.number().int().min(1).max(365)
  ]).optional().default('30d')
});

const customerRecoveredQuerySchema = z.object({
  from: z.string().trim().min(3).max(32)
});

const idParamSchema = z.object({
  id: z.string().trim().min(1).max(120)
});

const runRecommendedSchema = z.object({
  opportunityId: z.string().trim().min(1).max(120).optional()
});

function parseRangeDays(raw) {
  const n = Number(raw);
  if (n === 7 || n === 30 || n === 90) return n;
  return 7;
}

function isInboundMessage(msg) {
  const dir = String(msg?.dir || msg?.direction || '').toLowerCase();
  return dir === 'in' || dir === 'inbound';
}

function isOutboundMessage(msg) {
  const dir = String(msg?.dir || msg?.direction || '').toLowerCase();
  return dir === 'out' || dir === 'outbound';
}

function msgTs(msg) {
  return Number(msg?.ts || msg?.createdAt || 0) || 0;
}

function dayKeyFromTs(ts, timezone) {
  const n = Number(ts || 0);
  if (!n) return '';
  const d = new Date(n);
  if (timezone) {
    try {
      return new Intl.DateTimeFormat('en-CA', { timeZone: timezone, year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
    } catch {
      // Invalid tenant timezone falls back to UTC day keys.
    }
  }
  return d.toISOString().slice(0, 10);
}

function buildDailySeed(rangeDays, timezone) {
  const now = Date.now();
  const out = [];
  for (let i = rangeDays - 1; i >= 0; i -= 1) {
    const dayTs = now - (i * DAY_MS);
    const day = dayKeyFromTs(dayTs, timezone);
    out.push({ day, inboundLeads: 0, bookedLeads: 0 });
  }
  return out;
}

function hasBookedTag(value) {
  if (!value) return false;
  if (Array.isArray(value)) {
    return value.some((x) => String(x || '').toLowerCase() === 'booked');
  }
  if (typeof value === 'object') {
    return Object.values(value).some((v) => hasBookedTag(v));
  }
  return String(value).toLowerCase() === 'booked';
}

function isBookedConversation(convo) {
  const status = String(convo?.status || '').toLowerCase();
  const stage = String(convo?.stage || '').toLowerCase();
  const lifecycle = String(convo?.lifecycle?.leadStatus || convo?.leadData?.lifecycle?.leadStatus || '').toLowerCase();
  return status === 'booked' || stage === 'booked' || lifecycle === 'booked' || hasBookedTag(convo?.tags) || hasBookedTag(convo?.leadData?.tags);
}

function conversationBookedAt(convo) {
  const audits = Array.isArray(convo?.audit) ? convo.audit : [];
  for (let i = audits.length - 1; i >= 0; i -= 1) {
    const a = audits[i];
    const type = String(a?.type || '');
    const nextStatus = String(a?.meta?.status || a?.meta?.newStatus || '').toLowerCase();
    if ((type === 'status_change' || type === 'status_changed') && nextStatus === 'booked') {
      const t = Number(a?.ts || 0);
      if (t) return t;
    }
  }
  return Number(convo?.lastActivityAt || convo?.updatedAt || convo?.createdAt || 0) || 0;
}

function isQualifiedConversation(convo) {
  const stage = String(convo?.stage || '').toLowerCase();
  const lifecycle = String(convo?.lifecycle?.leadStatus || convo?.leadData?.lifecycle?.leadStatus || '').toLowerCase();
  if (lifecycle && lifecycle !== 'new') return true;
  const rank = STAGE_RANK[stage] || 0;
  const askServiceRank = STAGE_RANK.ask_service || 1;
  if (rank >= askServiceRank && stage) return true;
  return false;
}

analyticsRouter.get('/analytics/summary', validateQuery(summaryQuerySchema), (req, res) => {
  const tenant = req.tenant;
  const rangeDays = parseRangeDays(req?.query?.range);
  const now = Date.now();
  const sinceMs = now - (rangeDays * DAY_MS);
  const data = loadData();
  const account = data?.accounts?.[String(tenant?.to || '')] || null;
  const timezone = String(account?.workspace?.timezone || '').trim() || null;

  const conversations = getConversations(tenant.accountId).map((x) => x.conversation || {});
  const contacts = getContacts(tenant.accountId);
  const daily = buildDailySeed(rangeDays, timezone);
  const dailyMap = new Map(daily.map((d) => [d.day, d]));

  let inboundLeads = 0;
  let respondedConversations = 0;
  let qualifiedConversations = 0;
  let bookedLeads = 0;
  let firstResponseSumMinutes = 0;
  let firstResponseCount = 0;
  let under5 = 0;
  let min5to15 = 0;
  let over15 = 0;

  const bookedLeadKeys = new Set();

  for (const convo of conversations) {
    const messages = Array.isArray(convo?.messages) ? convo.messages : [];
    let firstInboundTs = 0;
    let firstOutboundAfterInboundTs = 0;

    for (const m of messages) {
      const ts = msgTs(m);
      if (!ts) continue;
      if (!firstInboundTs && isInboundMessage(m) && ts >= sinceMs && ts <= now) {
        firstInboundTs = ts;
      }
      if (firstInboundTs && !firstOutboundAfterInboundTs && isOutboundMessage(m) && ts > firstInboundTs && ts <= now && ts >= sinceMs) {
        firstOutboundAfterInboundTs = ts;
      }
    }

    if (firstInboundTs) {
      inboundLeads += 1;
      const day = dayKeyFromTs(firstInboundTs, timezone);
      const bucket = dailyMap.get(day);
      if (bucket) bucket.inboundLeads += 1;
      if (isQualifiedConversation(convo)) {
        qualifiedConversations += 1;
      }
    }

    if (firstInboundTs && firstOutboundAfterInboundTs) {
      respondedConversations += 1;
      const minutes = (firstOutboundAfterInboundTs - firstInboundTs) / 60000;
      firstResponseSumMinutes += minutes;
      firstResponseCount += 1;
      if (minutes < 5) under5 += 1;
      else if (minutes <= 15) min5to15 += 1;
      else over15 += 1;
    }

    if (isBookedConversation(convo)) {
      const bookedAt = conversationBookedAt(convo);
      if (bookedAt >= sinceMs && bookedAt <= now) {
        const key = String(convo?.from || convo?.id || bookedAt);
        if (!bookedLeadKeys.has(key)) {
          bookedLeadKeys.add(key);
          bookedLeads += 1;
        }
        const day = dayKeyFromTs(bookedAt, timezone);
        const bucket = dailyMap.get(day);
        if (bucket) bucket.bookedLeads += 1;
      }
    }
  }

  for (const contact of contacts) {
    const lifecycle = String(contact?.lifecycle?.leadStatus || '').toLowerCase();
    const isBooked = lifecycle === 'booked' || hasBookedTag(contact?.tags);
    if (!isBooked) continue;
    const ts = Number(contact?.updatedAt || contact?.createdAt || 0) || 0;
    if (!ts || ts < sinceMs || ts > now) continue;
    const key = String(contact?.phone || `contact_${ts}`);
    if (bookedLeadKeys.has(key)) continue;
    bookedLeadKeys.add(key);
    bookedLeads += 1;
    const day = dayKeyFromTs(ts, timezone);
    const bucket = dailyMap.get(day);
    if (bucket) bucket.bookedLeads += 1;
  }

  const responseRate = inboundLeads > 0 ? Math.round((respondedConversations / inboundLeads) * 100) : 0;
  const conversionRate = inboundLeads > 0 ? Math.round((bookedLeads / inboundLeads) * 100) : 0;
  const avgFirstResponseMinutes = firstResponseCount > 0 ? Math.round(firstResponseSumMinutes / firstResponseCount) : null;

  // If no qualification signals exist, responded count is the safest fallback.
  const qualified = qualifiedConversations > 0 ? qualifiedConversations : respondedConversations;

  return res.json({
    rangeDays,
    totals: {
      inboundLeads,
      respondedConversations,
      bookedLeads,
      responseRate,
      conversionRate
    },
    speed: {
      avgFirstResponseMinutes,
      buckets: {
        under5,
        min5to15,
        over15
      }
    },
    daily,
    funnel: {
      inboundLeads,
      responded: respondedConversations,
      qualified,
      booked: bookedLeads
    }
  });
});

function scopedLeadEvents(data, accountId) {
  return (data.leadEvents || [])
    .filter((e) => String(e?.accountId || '') === String(accountId))
    .sort((a, b) => Number(a?.ts || 0) - Number(b?.ts || 0));
}

function scopedOpportunities(data, accountId) {
  return (data.revenueOpportunities || [])
    .filter((o) => String(o?.accountId || '') === String(accountId));
}

function getRevenueEventsForAccount(accountId) {
  const data = loadData();
  const aid = String(accountId || '').trim();
  if (!aid) return [];
  return (data.revenueEvents || [])
    .filter((e) => String(e?.business_id || e?.accountId || '') === aid);
}

function dayKeys(days) {
  const now = Date.now();
  const out = [];
  for (let i = days - 1; i >= 0; i -= 1) {
    const d = new Date(now - (i * DAY_MS));
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

function parseRangeQuery(raw) {
  const text = String(raw || '30d').trim().toLowerCase();
  const m = text.match(/^(\d{1,3})d$/);
  if (m) {
    const n = Number(m[1]);
    if (n >= 1 && n <= 365) return n;
  }
  const n = Number(text);
  if (Number.isFinite(n) && n >= 1 && n <= 365) return n;
  return 30;
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

function isBookedConversationSignal(convo) {
  const status = String(convo?.status || '').toLowerCase();
  const stage = String(convo?.stage || '').toLowerCase();
  if (status === 'booked' || status === 'closed') return true;
  if (/booked|appointment_booked|scheduled|closed/.test(stage)) return true;
  if (Number.isFinite(Number(convo?.bookingTime || 0)) && Number(convo.bookingTime) > 0) return true;
  const messages = Array.isArray(convo?.messages) ? convo.messages : [];
  return messages.some((m) => {
    const confirmed = Boolean(m?.meta?.bookingConfirmed);
    const bookingMs = Number(m?.meta?.bookingTime || m?.bookingTime || 0);
    return confirmed || (Number.isFinite(bookingMs) && bookingMs > 0);
  });
}

function resolveBookedConversationAmount(convo) {
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

function normalizeInvoiceStatus(raw) {
  const v = String(raw || '').trim().toLowerCase();
  if (!v) return '';
  if (v.includes('refund')) return 'refunded';
  if (v.includes('paid') || v.includes('succeeded') || v.includes('captured') || v.includes('settled') || v.includes('complete')) return 'paid';
  if (v.includes('open') || v.includes('pending') || v.includes('unpaid') || v.includes('failed') || v.includes('declined')) return 'open';
  return '';
}

function resolveConversationPaymentMeta(convo) {
  const ld = convo?.leadData || {};
  const methodCandidates = [
    convo?.paymentMethod,
    convo?.payment_method,
    ld?.paymentMethod,
    ld?.payment_method
  ];
  let method = '';
  for (const candidate of methodCandidates) {
    method = normalizePaymentMethod(candidate);
    if (method) break;
  }

  const statusCandidates = [
    convo?.paymentStatus,
    convo?.payment_status,
    ld?.paymentStatus,
    ld?.payment_status
  ];
  let status = '';
  for (const candidate of statusCandidates) {
    status = normalizeInvoiceStatus(candidate);
    if (status) break;
  }

  const messages = Array.isArray(convo?.messages) ? convo.messages : [];
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const m = messages[i] || {};
    if (!method) {
      method = normalizePaymentMethod(
        m?.meta?.paymentMethod
        || m?.meta?.payment_method
      );
    }
    if (!status) {
      status = normalizeInvoiceStatus(
        m?.meta?.paymentStatus
        || m?.meta?.payment_status
      );
    }
    if (!status) {
      const paidFlag = m?.meta?.paid === true || m?.meta?.paymentSucceeded === true;
      if (paidFlag) status = 'paid';
    }
    if (method && status) break;
  }

  if (!status && method === 'cash' && isBookedConversationSignal(convo)) status = 'paid';
  if (!status && isBookedConversationSignal(convo)) status = 'open';
  return { method: method || 'unknown', status: status || 'open' };
}

function avgResponseTimeMinutes(events) {
  const inByConvo = {};
  const deltas = [];
  for (const e of events) {
    const convo = String(e?.convoKey || '');
    if (!convo) continue;
    const type = String(e?.type || '');
    const ts = Number(e?.ts || 0);
    if (!ts) continue;
    if (type === 'inbound_message') {
      inByConvo[convo] = ts;
      continue;
    }
    if (type === 'outbound_message') {
      const inTs = Number(inByConvo[convo] || 0);
      if (inTs > 0 && ts > inTs) {
        deltas.push((ts - inTs) / 60000);
        delete inByConvo[convo];
      }
    }
  }
  if (!deltas.length) return null;
  return Math.round(deltas.reduce((a, b) => a + b, 0) / deltas.length);
}

analyticsRouter.get('/analytics/customer-recovered', validateQuery(customerRecoveredQuerySchema), (req, res) => {
  const tenant = req.tenant;
  const accountId = String(tenant?.accountId || '').trim();
  const targetFrom = normalizePhone(req.query?.from);
  if (!accountId || !targetFrom) {
    return res.status(400).json({ error: 'accountId and from are required' });
  }

  const conversations = getConversations(accountId);
  let totalRecovered = 0;
  let bookingsCount = 0;
  for (const row of conversations) {
    const convo = row?.conversation || {};
    const from = normalizePhone(convo?.from || '');
    if (!from || from !== targetFrom) continue;
    if (!isBookedConversationSignal(convo)) continue;
    const amount = resolveBookedConversationAmount(convo);
    if (!Number.isFinite(amount) || amount <= 0) continue;
    bookingsCount += 1;
    totalRecovered += amount;
  }

  return res.json({
    from: targetFrom,
    bookingsCount,
    totalRecovered: Number(totalRecovered.toFixed(2))
  });
});

analyticsRouter.get('/analytics/customer-invoices', validateQuery(limitQuerySchema), (req, res) => {
  const tenant = req.tenant;
  const accountId = String(tenant?.accountId || '').trim();
  const limit = Number(req.query?.limit || 50);
  if (!accountId) return res.status(400).json({ error: 'accountId is required' });

  const contacts = getContacts(accountId);
  const contactsByPhone = new Map();
  for (const c of contacts) {
    const phone = normalizePhone(c?.phone || '');
    if (phone) contactsByPhone.set(phone, c);
  }

  const rows = [];
  const stored = listCustomerInvoices(accountId).map((inv) => ({
    id: String(inv?.id || ''),
    invoiceNumber: String(inv?.invoiceNumber || ''),
    conversationId: String(inv?.conversationId || ''),
    contactName: String(inv?.customerName || 'Unknown'),
    phone: String(inv?.phone || ''),
    email: String(inv?.email || ''),
    service: String(inv?.service || ''),
    serviceItems: Array.isArray(inv?.serviceItems) ? inv.serviceItems.map((x) => String(x || '').trim()).filter(Boolean).slice(0, 8) : [],
    amount: Number((Number(inv?.amountCents || 0) / 100).toFixed(2)),
    amountCents: Number(inv?.amountCents || 0),
    status: String(inv?.status || 'open'),
    paymentMethod: String(inv?.paymentMethod || 'unknown'),
    bookedAt: Number(inv?.bookedAt || inv?.createdAt || 0)
  }));
  const conversations = getConversations(accountId);
  for (const row of conversations) {
    const convo = row?.conversation || {};
    if (!isBookedConversationSignal(convo)) continue;
    const amount = resolveBookedConversationAmount(convo);
    if (!Number.isFinite(amount) || amount <= 0) continue;

    const fromPhone = normalizePhone(convo?.from || '');
    const ld = convo?.leadData || {};
    const contact = contactsByPhone.get(fromPhone) || null;
    const bookedAt = Number(
      convo?.bookingTime
      || ld?.booking_time
      || ld?.bookingTime
      || conversationBookedAt(convo)
      || convo?.lastActivityAt
      || convo?.updatedAt
      || convo?.createdAt
      || 0
    ) || 0;
      const payment = resolveConversationPaymentMeta(convo);
      const lifecycleStatus = String(convo?.status || '').toLowerCase() === 'closed' ? 'close' : 'booked';
      const amountCents = Math.round(Number(amount) * 100);
      rows.push({
      id: String(row?.id || `inv_${bookedAt}_${fromPhone || 'unknown'}`),
      invoiceNumber: `INV-${String(bookedAt || Date.now()).slice(-8)}`,
      conversationId: String(row?.id || ''),
      contactName: String(ld?.customer_name || contact?.name || '').trim() || 'Unknown',
      phone: fromPhone || String(convo?.from || '').trim(),
      email: String(ld?.customer_email || ld?.email || contact?.email || '').trim().toLowerCase(),
      service: String(ld?.service || ld?.request || convo?.service || '').trim(),
      serviceItems: Array.isArray(ld?.services_list) ? ld.services_list.map((x) => String(x || '').trim()).filter(Boolean).slice(0, 8) : [],
      amount: Number(Number(amount).toFixed(2)),
      amountCents,
      status: lifecycleStatus,
      paymentStatus: payment.status,
      paymentMethod: payment.method,
      bookedAt
    });
  }

  const mergedById = new Map();
  for (const row of rows) mergedById.set(String(row?.id || ''), row);
  for (const row of stored) mergedById.set(String(row?.id || ''), row);
  const merged = Array.from(mergedById.values())
    .sort((a, b) => Number(b?.bookedAt || 0) - Number(a?.bookedAt || 0));
  const limited = merged.slice(0, limit);
  const summary = {
    total: merged.length,
    paidCount: merged.filter((r) => r.status === 'paid').length,
    openCount: merged.filter((r) => r.status === 'open').length,
    refundedCount: merged.filter((r) => r.status === 'refunded').length,
    paidAmountCents: merged.filter((r) => r.status === 'paid').reduce((acc, r) => acc + Number(r?.amountCents || 0), 0),
    openAmountCents: merged.filter((r) => r.status === 'open').reduce((acc, r) => acc + Number(r?.amountCents || 0), 0),
    refundedAmountCents: merged.filter((r) => r.status === 'refunded').reduce((acc, r) => acc + Number(r?.amountCents || 0), 0)
  };

  return res.json({
    invoices: limited,
    summary
  });
});

analyticsRouter.get('/analytics/customer-invoices/:id/pdf', validateParams(idParamSchema), (req, res) => {
  const tenant = req.tenant;
  const invoiceId = String(req.params?.id || '').trim();
  const invoices = listCustomerInvoices(tenant.accountId);
  let match = invoices.find((inv) => String(inv?.id || '') === invoiceId);
  if (!match) {
    const conversations = getConversations(tenant.accountId);
    const fallbackRow = conversations.find((row) => {
      const convoId = String(row?.id || '');
      if (convoId !== invoiceId) return false;
      const convo = row?.conversation || {};
      if (!isBookedConversationSignal(convo)) return false;
      const amount = resolveBookedConversationAmount(convo);
      return Number.isFinite(amount) && amount > 0;
    });
    if (fallbackRow) {
      const convo = fallbackRow.conversation || {};
      const ld = convo?.leadData || {};
      const bookedAt = Number(
        convo?.bookingTime
        || ld?.booking_time
        || ld?.bookingTime
        || conversationBookedAt(convo)
        || convo?.lastActivityAt
        || convo?.updatedAt
        || convo?.createdAt
        || Date.now()
      ) || Date.now();
      const amount = resolveBookedConversationAmount(convo);
      const payment = resolveConversationPaymentMeta(convo);
      const lifecycleStatus = String(convo?.status || '').toLowerCase() === 'closed' ? 'close' : 'booked';
      match = {
        id: invoiceId,
        invoiceNumber: `INV-${String(bookedAt).slice(-8)}`,
        customerName: String(ld?.customer_name || 'Unknown'),
        phone: String(convo?.from || ''),
        email: String(ld?.customer_email || ld?.email || ''),
        service: String(ld?.service_required || ld?.request || ld?.intent || convo?.service || 'Service request'),
        serviceItems: Array.isArray(ld?.services_list) ? ld.services_list.map((x) => String(x || '').trim()).filter(Boolean).slice(0, 8) : [],
        amountCents: Math.round(Number(amount || 0) * 100),
        paymentMethod: payment.method || 'unknown',
        status: lifecycleStatus || 'booked',
        paymentStatus: payment.status || 'open',
        bookedAt,
        createdAt: bookedAt
      };
    }
  }
  if (!match) return res.status(404).json({ error: 'Invoice not found' });
  const data = loadData();
  const accountRef = getAccountById(data, tenant.accountId);
  if (!accountRef?.account) return res.status(404).json({ error: 'Account not found' });
  const pdf = buildInvoicePdfBuffer({ account: accountRef.account, invoice: match });
  const filename = `${String(match?.invoiceNumber || match?.id || 'invoice')}.pdf`.replace(/[^\w.\-]+/g, '_');
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  return res.send(pdf);
});

analyticsRouter.get('/analytics/revenue-overview', (req, res) => {
  const tenant = req.tenant;
  const data = loadData();
  const accountId = tenant.accountId;
  const events = scopedLeadEvents(data, accountId);
  const opportunities = scopedOpportunities(data, accountId).map((o) => evaluateOpportunity(accountId, o.id) || o);
  const now = Date.now();
  const weekAgo = now - (7 * DAY_MS);
  const monthAgo = now - (30 * DAY_MS);
  const open = opportunities.filter((o) => ['open', 'recovered'].includes(String(o?.status || '')));
  const atRisk = opportunities.filter((o) => String(o?.status || '') === 'at_risk');
  const atRiskValueCents = atRisk.reduce((acc, o) => acc + Number(o?.estimatedValueCents || 0), 0);
  const projectedRecoveryCents = opportunities
    .filter((o) => ['open', 'at_risk'].includes(String(o?.status || '').toLowerCase()))
    .reduce((acc, o) => acc + Number(o?.projectedRecoveryCents || 0), 0);
  const recoveredWeek = opportunities.filter((o) =>
    ['recovered', 'won'].includes(String(o?.status || '')) && Number(o?.updatedAt || 0) >= weekAgo
  );
  const recoveredMonth = opportunities.filter((o) =>
    ['recovered', 'won'].includes(String(o?.status || '')) && Number(o?.updatedAt || 0) >= monthAgo
  );
  const staleThreshold = now - (3 * DAY_MS);
  const lostEstimate = atRisk
    .filter((o) => Number(o?.updatedAt || 0) < staleThreshold)
    .reduce((acc, o) => acc + Number(o?.estimatedValueCents || 0), 0);

  const recoveredCount = opportunities.filter((o) => ['recovered', 'won'].includes(String(o?.status || ''))).length;
  const recoveryRate = opportunities.length ? Number((recoveredCount / opportunities.length).toFixed(3)) : 0;

  const revenueEvents = getRevenueEventsForAccount(accountId);
  const missedCallEvents = revenueEvents.filter((e) => String(e?.revenue_event_type || '') === 'opportunity_created'
    && String(e?.metadata_json?.signalType || '').includes('missed_call'));
  const estimatedLostRevenueCents = revenueEvents
    .filter((e) => String(e?.revenue_event_type || '') === 'opportunity_created' && String(e?.status || '') === 'open')
    .reduce((acc, e) => acc + Number(e?.estimated_value_cents || 0), 0);
  const recoveredRevenueCents = revenueEvents
    .filter((e) => ['opportunity_recovered', 'appointment_booked', 'sale_closed'].includes(String(e?.revenue_event_type || '')) || String(e?.status || '') === 'won')
    .reduce((acc, e) => acc + Number(e?.estimated_value_cents || 0), 0);
  const revenueRecoveryRate = (recoveredRevenueCents + estimatedLostRevenueCents) > 0
    ? Number((recoveredRevenueCents / (recoveredRevenueCents + estimatedLostRevenueCents)).toFixed(3))
    : 0;

  const sparklineDays = dayKeys(30);
  const recoveredSparkline = sparklineDays.map((key) => {
    return opportunities.filter((o) =>
      ['recovered', 'won'].includes(String(o?.status || '')) &&
      new Date(Number(o?.updatedAt || 0)).toISOString().slice(0, 10) === key
    ).length;
  });
  const createdSparkline = sparklineDays.map((key) => {
    return opportunities.filter((o) =>
      new Date(Number(o?.createdAt || 0)).toISOString().slice(0, 10) === key
    ).length;
  });

  res.json({
    openOpportunities: open.length,
    atRiskOpportunities: atRisk.length,
    atRiskValueCents,
    recoveredThisWeek: recoveredWeek.reduce((acc, o) => acc + Number(o?.estimatedValueCents || 0), 0),
    recoveredThisMonth: recoveredMonth.reduce((acc, o) => acc + Number(o?.estimatedValueCents || 0), 0),
    projectedRecoveryCents,
    lostEstimate,
    recoveryRate,
    missedCallCount: missedCallEvents.length,
    estimatedLostRevenueCents,
    recoveredRevenueCents,
    revenueRecoveryRate,
    revenueEvents: revenueEvents
      .sort((a, b) => Number(b?.created_at || 0) - Number(a?.created_at || 0))
      .map((e) => ({
        id: e.id,
        type: e.revenue_event_type,
        status: e.status,
        estimatedValueCents: Number(e?.estimated_value_cents || 0),
        confidence: Number(e?.confidence || 0),
        signalType: String(e?.metadata_json?.signalType || ''),
        contactId: e.contact_id,
        createdAt: Number(e.created_at || 0),
        metadata: e.metadata_json
      })),
    responseTimeAvg: avgResponseTimeMinutes(events),
    sparkline: {
      days: sparklineDays,
      recovered: recoveredSparkline,
      opportunities: createdSparkline
    }
  });
});

analyticsRouter.get('/analytics/at-risk', validateQuery(limitQuerySchema), (req, res) => {
  const tenant = req.tenant;
  const data = loadData();
  const contacts = getContacts(tenant.accountId);
  const byPhone = new Map(contacts.map((c) => [String(c?.phone || ''), c]));
  const list = getAtRiskOpportunities(tenant.accountId, { limit: Number(req.query?.limit || 50) });
  const mapped = list.map((opp) => {
    const convoKey = String(opp?.convoKey || '');
    const from = convoKey.includes('__') ? convoKey.split('__')[1] : '';
    const contact = byPhone.get(String(from)) || null;
    const plan = decideActionPlan(tenant.accountId, { leadEvent: null, opportunity: opp });
    const convo = convoKey ? (data.conversations?.[convoKey] || null) : null;
    return {
      opportunityId: String(opp?.id || ''),
      contact: contact?.name || from || 'Unknown lead',
      convoKey: convoKey || null,
      riskScore: Number(opp?.riskScore || 0),
      reasons: Array.isArray(opp?.riskReasons) ? opp.riskReasons : [],
      estimatedValue: Number(opp?.estimatedValueCents || 0),
      projectedRecoveryCents: Number(opp?.projectedRecoveryCents || 0),
      probability: opp?.projectedRecoveryProbability == null ? null : Number(opp.projectedRecoveryProbability),
      stage: String(opp?.stage || 'NEW'),
      cooldownUntil: Number(opp?.cooldownUntil || 0) || null,
      lastActivityAt: Number(convo?.lastActivityAt || opp?.updatedAt || opp?.createdAt || 0),
      recommendedActionSummary: String(plan?.nextAction || 'do_nothing')
    };
  });
  res.json({ opportunities: mapped });
});

analyticsRouter.get('/analytics/funnel', async (req, res, next) => {
  try {
    const tenant = req.tenant;
    const payload = await getFunnelMetrics(tenant.accountId, {
      route: 'GET /analytics/funnel',
      requestId: req.requestId,
      identifiers: { route: 'analytics/funnel' }
    });
    return res.json(payload);
  } catch (err) {
    return next(err);
  }
});

analyticsRouter.get('/analytics/todays-wins', (req, res) => {
  const tenant = req.tenant;
  const data = loadData();
  const accountId = tenant.accountId;
  const todayFrom = (() => {
    const d = new Date();
    d.setHours(0, 0, 0, 0);
    return d.getTime();
  })();
  const now = Date.now();
  const today = computeWinsSummaryFromData(data, accountId, 1, now);
  const week = computeWinsSummaryFromData(data, accountId, 7, now);
  const optimizationEvents = (data.optimizationEvents || [])
    .filter((e) => String(e?.accountId || '') === String(accountId))
    .filter((e) => String(e?.type || '') === 'weekly_owner_digest_generated')
    .sort((a, b) => Number(b?.ts || 0) - Number(a?.ts || 0));
  const latestDigest = optimizationEvents[0] || null;
  const todayScoped = {
    ...today,
    fromTs: todayFrom,
    toTs: now
  };
  res.json({
    today: todayScoped,
    week,
    latestDigest: latestDigest
      ? {
        ts: Number(latestDigest.ts || 0),
        payload: latestDigest.payload || null,
        delivery: latestDigest.delivery || null
      }
      : null
  });
});

analyticsRouter.get('/analytics/heatmaps', (req, res) => {
  const tenant = req.tenant;
  const events = scopedLeadEvents(loadData(), tenant.accountId);
  const byHourMissed = Array.from({ length: 24 }, (_, h) => ({ hour: h, count: 0 }));
  const byDayMissed = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map((d) => ({ day: d, count: 0 }));
  const oppHour = Array.from({ length: 24 }, (_, h) => ({ hour: h, count: 0 }));
  const oppDay = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map((d) => ({ day: d, count: 0 }));

  for (const e of events) {
    const ts = Number(e?.ts || 0);
    if (!ts) continue;
    const d = new Date(ts);
    const hour = d.getHours();
    const dayIdx = (d.getDay() + 6) % 7;
    const type = String(e?.type || '');
    if (type === 'missed_call') {
      byHourMissed[hour].count += 1;
      byDayMissed[dayIdx].count += 1;
    }
    if (['missed_call', 'inbound_message', 'form_submit', 'after_hours_inquiry'].includes(type)) {
      oppHour[hour].count += 1;
      oppDay[dayIdx].count += 1;
    }
  }

  res.json({
    missedCallsByHour: byHourMissed,
    missedCallsByDay: byDayMissed,
    opportunitiesByHour: oppHour,
    opportunitiesByDay: oppDay
  });
});

analyticsRouter.get('/analytics/opportunity/:id/timeline', validateParams(idParamSchema), async (req, res, next) => {
  try {
    const tenant = req.tenant;
    const payload = await getOpportunityTimeline(tenant.accountId, req.params?.id, {
      route: 'GET /analytics/opportunity/:id/timeline',
      requestId: req.requestId,
      identifiers: { route: 'analytics/opportunity/:id/timeline', opportunityId: String(req.params?.id || '') }
    });
    if (!payload) return res.status(404).json({ error: 'Opportunity not found' });
    return res.json(payload);
  } catch (err) {
    return next(err);
  }
});

analyticsRouter.get('/analytics/activity-feed', validateQuery(limitQuerySchema), async (req, res, next) => {
  try {
    const tenant = req.tenant;
    const n = Math.max(1, Math.min(200, Number(req.query?.limit || 50)));
    const payload = await getActivityFeed(tenant.accountId, n, {
      route: 'GET /analytics/activity-feed',
      requestId: req.requestId,
      identifiers: { route: 'analytics/activity-feed', limit: n }
    });
    return res.json(payload);
  } catch (err) {
    return next(err);
  }
});

analyticsRouter.get('/analytics/optimization-log', validateQuery(limitQuerySchema), (req, res) => {
  const tenant = req.tenant;
  const n = Math.max(1, Math.min(200, Number(req.query?.limit || 50)));
  const data = loadData();
  const items = (data.optimizationEvents || [])
    .filter((e) => String(e?.accountId || '') === String(tenant.accountId))
    .sort((a, b) => Number(b?.ts || 0) - Number(a?.ts || 0))
    .slice(0, n);
  res.json({ items });
});

analyticsRouter.post('/automation/run-recommended', validateBody(runRecommendedSchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const opportunityId = String(req.query?.opportunityId || req.body?.opportunityId || '').trim();
    const result = await runRecommendedAutomation(tenant.accountId, opportunityId);
    if (!result?.ok) return res.status(400).json({ error: result?.reason || 'Failed to run recommended action' });
    return res.json({ ok: true, result });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Internal server error' });
  }
});

analyticsRouter.get('/analytics/agent-metrics', validateQuery(rangeQuerySchema), async (req, res, next) => {
  try {
    const tenant = req.tenant;
    const payload = await getAgentMetrics(tenant.accountId, req.query?.range, {
      route: 'GET /analytics/agent-metrics',
      requestId: req.requestId,
      identifiers: { route: 'analytics/agent-metrics', rangeDays: String(req.query?.range || '30d') }
    });
    return res.json(payload);
  } catch (err) {
    return next(err);
  }
});

analyticsRouter.get('/analytics/playbook-performance', validateQuery(rangeQuerySchema), async (req, res, next) => {
  try {
    const tenant = req.tenant;
    const payload = await getPlaybookPerformance(tenant.accountId, req.query?.range, {
      route: 'GET /analytics/playbook-performance',
      requestId: req.requestId,
      identifiers: { route: 'analytics/playbook-performance', rangeDays: String(req.query?.range || '30d') }
    });
    return res.json(payload);
  } catch (err) {
    return next(err);
  }
});

module.exports = { analyticsRouter };
