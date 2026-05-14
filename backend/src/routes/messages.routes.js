const express = require('express');
const { z, validateBody, validateParams, validateQuery } = require('../utils/validate');
const {
  deleteConversation,
  flushDataNow,
  getConversations,
  getConversationById,
  loadData,
  saveDataDebounced,
  getAccountById
} = require('../store/dataStore');
const { pushBookingToConnectedCalendars } = require('../services/calendarIcsService');
const { ensureInvoiceForBookedConversation, syncInvoiceLifecycleForConversation } = require('../services/customerInvoiceService');
const {
  recordOutboundAttempt,
  recordBookingSync,
  updateConversationStatusLegacy,
  deleteConversationMessageLegacy,
  deleteConversationMessageById
} = require('../services/messagingBoundaryService');
const {
  listThreads,
  listConversationsForTenant,
  getConversationDetail
} = require('../services/conversationsService');

const messagesRouter = express.Router();

const conversationIdSchema = z.object({
  id: z.string().trim().regex(/^.+__.+$/)
});

const conversationQuerySchema = z.object({
  from: z.string().trim().min(1).max(32)
});

const sendMessageSchema = z.object({
  text: z.string().trim().min(1).max(4000),
  consentConfirmed: z.boolean().optional().default(false),
  consentSource: z.string().trim().max(120).optional().nullable(),
  transactional: z.boolean().optional().default(false)
});

const sendLegacySchema = z.object({
  from: z.string().trim().min(1).max(32),
  text: z.string().trim().min(1).max(4000),
  consentConfirmed: z.boolean().optional().default(false),
  consentSource: z.string().trim().max(120).optional().nullable(),
  transactional: z.boolean().optional().default(false)
});
const messageIndexParamSchema = z.object({
  id: z.string().trim().regex(/^.+__.+$/),
  index: z.coerce.number().int().min(0)
});
const messageIdParamSchema = z.object({
  id: z.string().trim().regex(/^.+__.+$/),
  messageId: z.string().trim().min(1).max(200)
});

const statusSchema = z.object({
  status: z.enum(['new', 'contacted', 'booked', 'closed']),
  bookingTime: z.coerce.number().int().positive().optional(),
  bookingEndTime: z.coerce.number().int().positive().optional(),
  service: z.string().trim().max(120).optional(),
  vehicle: z.string().trim().max(120).optional(),
  amount: z.coerce.number().min(0).max(1_000_000).optional(),
  location: z.string().trim().max(240).optional()
});

const manualBookingSchema = z.object({
  customerName: z.string().trim().min(1).max(160),
  customerPhone: z.string().trim().min(3).max(32),
  customerEmail: z.string().trim().email().max(254).optional().or(z.literal('')).default(''),
  service: z.string().trim().max(160).optional().default(''),
  vehicle: z.string().trim().max(160).optional().default(''),
  notes: z.string().trim().max(2000).optional().default(''),
  amount: z.coerce.number().min(0).max(1_000_000).optional(),
  bookingTime: z.coerce.number().int().positive(),
  bookingEndTime: z.coerce.number().int().positive().optional(),
  location: z.string().trim().max(240).optional().default('')
});

function normalizePhone(raw) {
  const v = String(raw || '').trim();
  if (!v) return '';
  const digits = v.replace(/[^\d+]/g, '');
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
  const m = s.match(/\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?|\d{1,6}(?:\.\d{1,2})?)/);
  if (!m) return null;
  const n = Number(String(m[1] || '').replace(/,/g, ''));
  return Number.isFinite(n) && n > 0 ? n : null;
}

function resolveConversationAmount(convo) {
  const ld = convo?.leadData || {};
  const candidates = [
    convo?.resolvedAmount,
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
    ld?.total,
    ld?.total_price
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

function normalizeServiceSummaryText(value) {
  const parts = String(value || '')
    .split(/\+|,| and /i)
    .map((x) => String(x || '').trim())
    .filter(Boolean);
  const seen = new Set();
  const out = [];
  for (const p of parts) {
    const key = p.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(p);
  }
  return out.join(' + ');
}

function splitServiceParts(value) {
  return String(value || '')
    .split(/\+|,| and /i)
    .map((x) => String(x || '').trim())
    .filter(Boolean);
}

function dedupeServiceParts(parts) {
  const seen = new Set();
  const out = [];
  for (const p of Array.isArray(parts) ? parts : []) {
    const raw = String(p || '').trim();
    if (!raw) continue;
    const key = raw.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(raw);
  }
  return out;
}

function findServiceLineFromMessages(convo) {
  const messages = Array.isArray(convo?.messages) ? convo.messages : [];
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const text = String(messages[i]?.text || messages[i]?.body || '').trim();
    if (!text) continue;
    const lc = text.toLowerCase();
    if (/i see you booked on|booked on|appointment booked|scheduled/i.test(lc)) continue;
    const textNoUrl = text.replace(/https?:\/\/\S+/gi, ' ').replace(/\s+/g, ' ').trim();
    const summaryPattern = text.match(/\bservice summary\s*\(([^)]+)\)/i);
    if (summaryPattern?.[1]) {
      const normalized = normalizeServiceSummaryText(summaryPattern[1]);
      if (normalized) return normalized;
    }
    // Booking form payloads can include a labeled Request line.
    const requestLine = textNoUrl.match(/(?:^|\n)\s*request\s*[:\-]?\s*([^\n]+)/i);
    if (requestLine?.[1]) {
      const normalized = normalizeServiceSummaryText(requestLine[1]);
      if (normalized) return normalized;
    }
    // Only accept explicit service-summary lines, not conversational sentences.
    // Examples we want: "engine bay degrease and polish + mat shampoo"
    if (textNoUrl.length <= 220 && textNoUrl.includes('+') && !/[.?!]/.test(textNoUrl)) {
      const normalized = normalizeServiceSummaryText(textNoUrl);
      if (normalized) return normalized;
    }
  }
  return '';
}

function mergeServiceLeadData(convo) {
  const leadDataMerged = {
    ...(convo?.leadData || {})
  };
  const flowServices = Array.isArray(convo?.flow?.data?.services_list)
    ? convo.flow.data.services_list
    : [];
  const existingServices = Array.isArray(leadDataMerged.services_list)
    ? leadDataMerged.services_list
    : [];
  const normalizedServices = dedupeServiceParts([
    ...existingServices.map((x) => String(x || '').trim()),
    ...flowServices.map((x) => String(x || '').trim())
  ]).slice(0, 8);
  if (!Array.isArray(leadDataMerged.services_list) || !leadDataMerged.services_list.length) {
    if (normalizedServices.length) leadDataMerged.services_list = normalizedServices;
  }

  const serviceFallback = findServiceLineFromMessages(convo);
  const serviceText = String(
    leadDataMerged.service_required
    || leadDataMerged.request
    || leadDataMerged.issue
    || convo?.service
    || serviceFallback
    || ''
  ).trim();

  if (!String(leadDataMerged.service_required || '').trim() && serviceText) {
    leadDataMerged.service_required = serviceText;
  }
  if (!String(leadDataMerged.request || '').trim() && serviceText) {
    leadDataMerged.request = serviceText;
  }

  if ((!Array.isArray(leadDataMerged.services_list) || !leadDataMerged.services_list.length) && serviceText) {
    const pieces = dedupeServiceParts(splitServiceParts(serviceText)).slice(0, 8);
    if (pieces.length) leadDataMerged.services_list = pieces;
  }
  return leadDataMerged;
}

function resolveBookedLifecycle(convo) {
  if (isSimulatedConversation(convo)) {
    return { bookedLike: false, bookingMs: null };
  }
  const ld = convo?.leadData || {};
  const status = String(convo?.status || '').trim().toLowerCase();
  const stage = String(convo?.stage || '').trim().toLowerCase();
  let bookingMs = Number(convo?.bookingTime || ld?.booking_time || ld?.bookingTime || 0);
  let bookingConfirmed = false;
  let bookedTextSignal = false;
  const messages = Array.isArray(convo?.messages) ? convo.messages : [];
  for (const m of messages) {
    const text = String(m?.text || m?.body || '').toLowerCase();
    const msgBookingMs = Number(m?.meta?.bookingTime || m?.bookingTime || 0);
    if (Number.isFinite(msgBookingMs) && msgBookingMs > 0) {
      bookingMs = Math.max(Number.isFinite(bookingMs) ? bookingMs : 0, msgBookingMs);
    }
    if (m?.meta?.bookingConfirmed === true) {
      bookingConfirmed = true;
    }
    if (/i see you booked on|booked on|appointment booked|scheduled/i.test(text)) {
      bookedTextSignal = true;
    }
  }
  const hasBookingMs = Number.isFinite(bookingMs) && bookingMs > 0;
  const bookedLike = status === 'booked'
    || status === 'closed'
    || /booked|appointment_booked|scheduled/.test(stage)
    || hasBookingMs
    || bookingConfirmed
    || bookedTextSignal;
  return {
    bookedLike,
    bookingMs: hasBookingMs ? bookingMs : null
  };
}

function isSimulatedConversation(convo) {
  const ld = convo?.leadData || {};
  return convo?.isSimulated === true
    || String(convo?.source || '').toLowerCase() === 'simulated'
    || String(convo?.status || '').toLowerCase() === 'simulated'
    || String(convo?.stage || '').toLowerCase() === 'simulated'
    || ld?.simulated === true;
}

// List threads (basic)
messagesRouter.get('/threads', async (req, res) => {
  const tenant = req.tenant;
  const threads = await listThreads(tenant.accountId, {
    identifiers: { route: '/api/threads' }
  });
  res.json({ threads });
});

// -----------------------------
// Back-compat routes (frontend expects these)
// -----------------------------

// List conversations for a specific business number ("to")
messagesRouter.get('/conversations', async (req, res) => {
  const tenant = req.tenant;
  const items = (await listConversationsForTenant(tenant.accountId, {
    identifiers: { route: '/api/conversations' }
  })).map((conversation) => ({ key: conversation.id, convo: conversation }));

  const conversations = items.map(({ key, convo: c }) => {
    const simulated = isSimulatedConversation(c);
    const booked = resolveBookedLifecycle(c);
    const resolvedAmount = resolveConversationAmount(c);
    const resolvedAmountCents = Number.isFinite(resolvedAmount) && resolvedAmount > 0
      ? Math.round(Number(resolvedAmount) * 100)
      : null;
    const leadDataMerged = mergeServiceLeadData(c);
    const normalizedStatus = simulated
      ? 'simulated'
      : booked.bookedLike && String(c?.status || '').toLowerCase() !== 'closed'
      ? 'booked'
      : (c?.status || '');
    const normalizedStage = simulated
      ? 'simulated'
      : booked.bookedLike
      ? 'booked'
      : (c?.stage || '');
    return {
      id: key,
      from: c.from,
      to: c.to,
      lastText: c.messages?.length ? c.messages[c.messages.length - 1].text : '',
      stage: normalizedStage,
      status: normalizedStatus,
      source: simulated ? 'simulated' : (c.source || ''),
      isSimulated: simulated,
      amount: c.amount ?? resolvedAmount ?? null,
      bookingAmount: c.bookingAmount ?? resolvedAmount ?? null,
      resolvedAmount: resolvedAmount ?? null,
      resolvedAmountCents,
      bookingTime: simulated ? null : (c.bookingTime ?? booked.bookingMs ?? null),
      paymentStatus: c.paymentStatus || c.payment_status || '',
      accountId: c.accountId || '',
      lastActivityAt: c.lastActivityAt || null,
      updatedAt: c.flow?.updatedAt || null,
      createdAt: c.createdAt || null,
      flow: c.flow || null,
      audit: c.audit || [],
      leadData: leadDataMerged
    };
  });

  res.json({ conversations });
});

// Get a single conversation by its id ("to__from")
messagesRouter.get('/conversations/:id', validateParams(conversationIdSchema), async (req, res) => {
  const tenant = req.tenant;
  const id = String(req.params.id || '');
  const [to, from] = id.split('__');
  if (!to || !from) return res.status(400).json({ error: 'Invalid conversation id' });
  if (String(to) !== String(tenant.to)) return res.status(404).json({ error: 'Conversation not found' });
  const convo = await getConversationDetail(tenant.accountId, id, {
    identifiers: { route: '/api/conversations/:id' }
  });
  if (!convo) return res.status(404).json({ error: 'Conversation not found' });
  
  // Add the ID to the conversation object so frontend can use it
  convo.id = id;
  const resolvedAmount = resolveConversationAmount(convo);
  if (Number.isFinite(resolvedAmount) && resolvedAmount > 0) {
    convo.resolvedAmount = resolvedAmount;
    convo.resolvedAmountCents = Math.round(Number(resolvedAmount) * 100);
    if (!(Number.isFinite(Number(convo.amount || 0)) && Number(convo.amount) > 0)) {
      convo.amount = resolvedAmount;
    }
    if (!(Number.isFinite(Number(convo.bookingAmount || 0)) && Number(convo.bookingAmount) > 0)) {
      convo.bookingAmount = resolvedAmount;
    }
  }
  convo.leadData = mergeServiceLeadData(convo);
  if (isSimulatedConversation(convo)) {
    convo.source = 'simulated';
    convo.isSimulated = true;
    convo.status = 'simulated';
    convo.stage = 'simulated';
    convo.bookingTime = null;
    convo.bookingEndTime = null;
    delete convo.leadData.booking_time;
    delete convo.leadData.bookingTime;
    delete convo.leadData.booking_end_time;
    delete convo.leadData.bookingEndTime;
  }
  
  res.json({ conversation: convo });
});

// Delete a conversation by its id ("to__from")
messagesRouter.delete('/conversations/:id', validateParams(conversationIdSchema), async (req, res) => {
  const tenant = req.tenant;
  const id = String(req.params.id || '');
  const [to, from] = id.split('__');
  if (!to || !from) return res.status(400).json({ error: 'Invalid conversation id' });
  if (String(to) !== String(tenant.to)) return res.status(404).json({ error: 'Conversation not found' });
  const ok = deleteConversation(to, from, tenant.accountId);
  if (!ok) return res.status(404).json({ error: 'Conversation not found' });
  try {
    await flushDataNow();
  } catch (err) {
    return res.status(500).json({ error: 'Conversation deleted but failed to persist state', detail: err?.message || String(err) });
  }
  res.json({ ok });
});

// NEW: Send a message from a specific conversation by id
messagesRouter.post('/conversations/:id/send', validateParams(conversationIdSchema), validateBody(sendMessageSchema), (req, res) => {
  const tenant = req.tenant;
  const id = String(req.params.id || '');
  const [to, from] = id.split('__');
  if (!to || !from) return res.status(400).json({ error: 'Invalid conversation id' });
  if (String(to) !== String(tenant.to)) return res.status(404).json({ error: 'Conversation not found' });
  
  const { text, consentConfirmed, consentSource, transactional } = req.body || {};
  if (!text) return res.status(400).json({ error: 'Missing text' });

  recordOutboundAttempt({
    tenant,
    to,
    from,
    text: String(text),
    source: 'manual_dashboard',
    consentConfirmed: consentConfirmed === true,
    consentSource: consentSource ? String(consentSource) : null,
    transactional: transactional === true,
    route: '/api/conversations/:id/send',
    requireExisting: false,
    afterSuccess(conversation) {
      conversation.lastActivityAt = Date.now();
      if (conversation.status === 'new') {
        conversation.status = 'contacted';
        conversation.audit.push({
          ts: Date.now(),
          type: 'status_change',
          meta: { status: 'contacted', source: 'dashboard_reply' }
        });
      }
    }
  }).then(({ conversation, sendResult }) => {
    if (!conversation) return res.status(404).json({ error: 'Conversation not found' });
    conversation.id = id;
    if (!sendResult.ok) {
      return res.status(403).json({
        code: sendResult.error.code,
        message: sendResult.error.message
      });
    }
    return res.json({ ok: true, conversation });
  }).catch((err) => {
    return res.status(500).json({ error: err?.message || 'Failed to send message' });
  });
});

// Update conversation status (New / Contacted / Booked / Closed)
messagesRouter.post('/conversations/:id/status', validateParams(conversationIdSchema), validateBody(statusSchema), async (req, res) => {
  const tenant = req.tenant;
  const id = String(req.params.id || '');
  const [to, from] = id.split('__');
  if (!to || !from) return res.status(400).json({ error: 'Invalid conversation id' });
  if (String(to) !== String(tenant.to)) return res.status(404).json({ error: 'Conversation not found' });

  const { status, bookingTime, bookingEndTime, service, vehicle, amount, location } = req.body || {};

  const normalizedStatus = String(status).toLowerCase();
  const startMs = Number(bookingTime || Date.now());
  const endMs = Number(bookingEndTime || (startMs + 60 * 60 * 1000));
  const convo = await updateConversationStatusLegacy({
    tenant,
    to,
    from,
    status: normalizedStatus,
    bookingTime: startMs,
    bookingEndTime: endMs,
    amount,
    source: 'dashboard_status_update',
    route: '/api/conversations/:id/status',
    requireExisting: false
  });
  if (!convo) return res.status(404).json({ error: 'Conversation not found' });

  convo.id = id;

  if (normalizedStatus === 'booked') {
    const labelParts = [];
    if (service) labelParts.push(String(service));
    if (vehicle) labelParts.push(String(vehicle));
    const amountNum = Number(amount);
    const amountLabel = Number.isFinite(amountNum) ? ` - $${Math.round(amountNum)}` : '';
    const title = labelParts.length ? `${labelParts.join(' · ')}${amountLabel}` : `Booked Appointment${amountLabel}`;
    pushBookingToConnectedCalendars(tenant, {
      remoteId: id,
      title,
      start: startMs,
      end: endMs,
      location: String(location || '')
    });
    try {
      await ensureInvoiceForBookedConversation({
        accountId: tenant.accountId,
        to: String(to),
        from: String(from),
        bookingStart: startMs,
        bookingEnd: endMs,
        source: 'status_update'
      });
    } catch (err) {
      console.error('invoice generation failed:', err?.message || err);
    }
  }
  if (normalizedStatus === 'closed') {
    try {
      syncInvoiceLifecycleForConversation({
        accountId: tenant.accountId,
        to: String(to),
        from: String(from),
        lifecycleStatus: 'close'
      });
    } catch (err) {
      console.error('invoice lifecycle sync failed:', err?.message || err);
    }
  }

  res.json({ ok: true, conversation: convo });
});

messagesRouter.post('/bookings/manual', validateBody(manualBookingSchema), async (req, res) => {
  const tenant = req.tenant;
  const to = String(tenant?.to || '').trim();
  const accountId = String(tenant?.accountId || '').trim();
  const customerName = String(req.body?.customerName || '').trim();
  const customerPhone = normalizePhone(req.body?.customerPhone);
  const customerEmail = String(req.body?.customerEmail || '').trim().toLowerCase();
  const service = String(req.body?.service || '').trim() || 'Appointment';
  const vehicle = String(req.body?.vehicle || '').trim();
  const notes = String(req.body?.notes || '').trim();
  const amountNum = Number(req.body?.amount);
  const amount = Number.isFinite(amountNum) && amountNum > 0 ? amountNum : null;
  const startMs = Number(req.body?.bookingTime || Date.now());
  const endMs = Number(req.body?.bookingEndTime || (startMs + 60 * 60 * 1000));
  const location = String(req.body?.location || '').trim();

  if (!to || !accountId) return res.status(400).json({ error: 'Missing tenant context' });
  if (!customerPhone) return res.status(400).json({ error: 'Invalid customer phone' });
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    return res.status(400).json({ error: 'Invalid booking time range' });
  }

  const manualBookingId = `manual_${Date.now()}`;
  const msg = `Manual booking created: ${service}${vehicle ? ` | ${vehicle}` : ''}${amount != null ? ` | $${Math.round(amount)}` : ''}`;
  const convo = await recordBookingSync({
    tenant,
    to,
    from: customerPhone,
    bookingStart: startMs,
    bookingEnd: endMs,
    bookingId: manualBookingId,
    source: 'manual_booking',
    service,
    serviceRequired: notes || service,
    customerName,
    customerPhone,
    customerEmail,
    vehicle,
    amount,
    notes: notes || service,
    appendMessage: true,
    messageText: msg,
    patchConversation(conversation) {
      conversation.leadData = conversation.leadData && typeof conversation.leadData === 'object' ? conversation.leadData : {};
      conversation.leadData.availability = new Date(startMs).toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit'
      });
    },
    route: '/api/bookings/manual'
  });

  if (!convo) return res.status(500).json({ error: 'Failed to create booking conversation' });
  convo.id = `${to}__${customerPhone}`;

  try {
    const data = loadData();
    const accountRef = getAccountById(data, accountId);
    if (accountRef?.account) {
      const account = accountRef.account;
      account.internalBookings = Array.isArray(account.internalBookings) ? account.internalBookings : [];
      const bookingId = `mbk_${Date.now()}_${Math.floor(Math.random() * 10000)}`;
      account.internalBookings.push({
        id: bookingId,
        start: startMs,
        end: endMs,
        status: 'booked',
        customerName,
        customerPhone,
        customerEmail,
        serviceId: '',
        serviceName: service,
        notes,
        manageToken: '',
        createdAt: Date.now(),
        source: 'manual_booking'
      });
      account.internalBookings = account.internalBookings.slice(-5000);
      saveDataDebounced(data);
    }
  } catch (err) {
    console.error('manual booking internalBookings update failed:', err?.message || err);
  }

  const amountLabel = amount != null ? ` - $${Math.round(amount)}` : '';
  const title = `${service}${vehicle ? ` · ${vehicle}` : ''}${amountLabel}`;
  pushBookingToConnectedCalendars(tenant, {
    remoteId: `manual_${to}__${customerPhone}_${startMs}`,
    title,
    start: startMs,
    end: endMs,
    location
  });

  let invoice = null;
  try {
    invoice = await ensureInvoiceForBookedConversation({
      accountId,
      to,
      from: customerPhone,
      bookingStart: startMs,
      bookingEnd: endMs,
      source: 'manual_booking'
    });
  } catch (err) {
    console.error('manual booking invoice generation failed:', err?.message || err);
  }

  return res.json({
    ok: true,
    booking: {
      id: `manual_${Date.now()}`,
      to,
      from: customerPhone,
      customerName,
      customerEmail,
      service,
      vehicle,
      amount,
      bookingTime: startMs,
      bookingEndTime: endMs,
      location
    },
    conversation: convo,
    invoice: invoice || null
  });
});

// Delete a single message from a conversation by stable message id.
messagesRouter.delete('/conversations/:id/messages/by-id/:messageId', validateParams(messageIdParamSchema), async (req, res) => {
  const tenant = req.tenant;
  const id = String(req.params.id || '');
  const [to, from] = id.split('__');
  if (!to || !from) return res.status(400).json({ error: 'Invalid conversation id' });
  if (String(to) !== String(tenant.to)) return res.status(404).json({ error: 'Conversation not found' });
  const messageId = String(req.params.messageId || '').trim();
  if (!messageId) return res.status(400).json({ error: 'Invalid message id' });

  let convo = null;
  try {
    convo = await deleteConversationMessageById({
      tenant,
      to,
      from,
      messageId,
      route: '/api/conversations/:id/messages/by-id/:messageId'
    });
  } catch (err) {
    const status = Number(err?.status || 0);
    return res.status(status === 404 ? 404 : 400).json({ error: err?.message || 'Message not found' });
  }
  if (!convo) return res.status(404).json({ error: 'Conversation not found' });

  convo.id = id;
  return res.json({ ok: true, conversation: convo });
});

// Deprecated compatibility shim: resolves legacy list index to a message id, then delegates to the ID-first delete path.
messagesRouter.delete('/conversations/:id/messages/:index', validateParams(messageIndexParamSchema), async (req, res) => {
  const tenant = req.tenant;
  const id = String(req.params.id || '');
  const [to, from] = id.split('__');
  if (!to || !from) return res.status(400).json({ error: 'Invalid conversation id' });
  if (String(to) !== String(tenant.to)) return res.status(404).json({ error: 'Conversation not found' });
  const index = Number(req.params.index);
  if (!Number.isFinite(index) || index < 0) return res.status(400).json({ error: 'Invalid message index' });

  let convo = null;
  try {
    convo = await deleteConversationMessageLegacy({
      tenant,
      to,
      from,
      index,
      route: '/api/conversations/:id/messages/:index'
    });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Message index out of range' });
  }
  if (!convo) return res.status(404).json({ error: 'Conversation not found' });

  convo.id = id;
  return res.json({ ok: true, conversation: convo });
});

// Get a conversation by to/from
messagesRouter.get('/conversation', validateQuery(conversationQuerySchema), async (req, res) => {
  const tenant = req.tenant;
  const from = req.query?.from;
  const to = tenant.to;
  if (!from) return res.status(400).json({ error: 'Missing from' });
  const id = `${to}__${from}`;
  const convo = await getConversationDetail(tenant.accountId, id, {
    identifiers: { route: '/api/conversation' }
  });
  if (!convo) return res.status(404).json({ error: 'Conversation not found' });
  
  // Add the ID to the conversation object
  convo.id = id;
  res.json({ conversation: convo });
});

// Send a dashboard message (simulated) - LEGACY ROUTE
messagesRouter.post('/send', validateBody(sendLegacySchema), (req, res) => {
  const tenant = req.tenant;
  const { from, text, consentConfirmed, consentSource, transactional } = req.body || {};
  const to = tenant.to;
  if (!from || !text) return res.status(400).json({ error: 'Missing from/text' });
  recordOutboundAttempt({
    tenant,
    to,
    from,
    text: String(text),
    source: 'manual_dashboard_legacy',
    consentConfirmed: consentConfirmed === true,
    consentSource: consentSource ? String(consentSource) : null,
    transactional: transactional === true,
    route: '/api/send',
    requireExisting: false,
    afterSuccess(conversation) {
      conversation.lastActivityAt = Date.now();
      if (conversation.status === 'new') {
        conversation.status = 'contacted';
        conversation.audit.push({
          ts: Date.now(),
          type: 'status_change',
          meta: { status: 'contacted', source: 'dashboard_reply' }
        });
      }
    }
  }).then(({ conversation, sendResult }) => {
    if (!conversation) return res.status(404).json({ error: 'Conversation not found' });
    const id = `${to}__${from}`;
    conversation.id = id;
    if (!sendResult.ok) {
      return res.status(403).json({
        code: sendResult.error.code,
        message: sendResult.error.message
      });
    }
    return res.json({ ok: true, conversation });
  }).catch((err) => {
    return res.status(500).json({ error: err?.message || 'Failed to send message' });
  });
});

module.exports = { messagesRouter };
