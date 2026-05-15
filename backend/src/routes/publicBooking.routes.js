const express = require('express');
const crypto = require('crypto');
const { loadData, saveDataDebounced, flushDataNow } = require('../store/dataStore');
const { z, validateBody, validateParams, validateQuery } = require('../utils/validate');
const { pushBookingToConnectedCalendars } = require('../services/calendarIcsService');
const { evaluateTrigger } = require('../services/automationEngine');
const { createLeadEvent } = require('../services/revenueIntelligenceService');
const { handleSignal } = require('../services/revenueOrchestrator');
const {
  ensureInvoiceForBookedConversation,
  getCustomerInvoiceByPdfToken,
  buildInvoicePdfBuffer
} = require('../services/customerInvoiceService');
const {
  ensureSchedulingConfig,
  publicBookingUrlForAccount,
  findAccountByBookingToken,
  readWorkspaceServices,
  serviceDurationMin,
  listAvailability,
  normalizeTimezone
} = require('../services/publicBookingService');
const { recordBookingSync } = require('../services/messagingBoundaryService');

const publicBookingRouter = express.Router();

const bookingTokenParamSchema = z.object({
  token: z.string().trim().min(1).max(200)
});
const invoicePdfTokenParamSchema = z.object({
  token: z.string().trim().min(8).max(200)
});

const bookingManageParamSchema = z.object({
  token: z.string().trim().min(1).max(200),
  manageToken: z.string().trim().min(1).max(200)
});

const bookingAvailabilityQuerySchema = z.object({
  serviceId: z.string().trim().max(120).optional().default(''),
  date: z.string().trim().regex(/^\d{4}-\d{2}-\d{2}$/).optional().default(''),
  days: z.coerce.number().int().min(1).max(14).optional().default(5),
  durationMin: z.coerce.number().int().min(15).max(1440).optional(),
  ignoreBookingId: z.string().trim().max(120).optional().default('')
});

const bookingCreateSchema = z.object({
  customerName: z.string().trim().min(1).max(160),
  customerPhone: z.string().trim().min(3).max(32),
  customerEmail: z.string().trim().email().max(254),
  serviceId: z.string().trim().max(120).optional().default(''),
  serviceName: z.string().trim().max(160).optional().default(''),
  notes: z.string().trim().max(2000).optional().default(''),
  start: z.coerce.number().int().positive(),
  end: z.coerce.number().int().positive()
});

const bookingRescheduleSchema = z.object({
  start: z.coerce.number().int().positive(),
  end: z.coerce.number().int().positive()
});
const noBodySchema = z.object({}).strict().optional().default({});

function bookingId() {
  return `bk_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
}
function bookingManageToken() {
  return `bm_${crypto.randomBytes(12).toString('hex')}`;
}

function normalizePhone(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const digits = raw.replace(/[^\d+]/g, '');
  if (!digits) return '';
  if (digits.startsWith('+')) return digits;
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  return `+${digits}`;
}

function trimText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function inferPublicBaseUrl(req) {
  const forwardedProto = String(req?.headers?.['x-forwarded-proto'] || '').split(',')[0].trim();
  const forwardedHost = String(req?.headers?.['x-forwarded-host'] || '').split(',')[0].trim();
  const proto = forwardedProto || String(req?.protocol || 'http');
  const host = forwardedHost || String(req?.get?.('host') || req?.headers?.host || '');
  return `${proto}://${host}`.replace(/\/+$/, '');
}

function parseServiceAmount(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const matches = raw.match(/\d+(?:\.\d{1,2})?/g);
  if (!matches || !matches.length) return null;
  const nums = matches
    .map((x) => Number(x))
    .filter((n) => Number.isFinite(n) && n > 0);
  if (!nums.length) return null;
  return Math.round((nums[nums.length - 1] + Number.EPSILON) * 100) / 100;
}

function buildServiceRequired(item) {
  const service = trimText(item?.serviceName || item?.serviceId || 'Service request');
  const notes = trimText(item?.notes || '');
  // Keep service_required focused on the customer's described work.
  // Service name is already stored separately.
  return notes || service;
}

function bookingSummaryLines(item) {
  return [
    String(item?.customerName || '').trim() || 'Unknown name',
    String(item?.customerPhone || '').trim() || 'Unknown number',
    buildServiceRequired(item)
  ];
}

function inferServicesFromWorkspace(account, text) {
  const t = String(text || '').toLowerCase();
  if (!t) return [];
  const services = readWorkspaceServices(account);
  const hits = [];
  for (const [id, svc] of Object.entries(services || {})) {
    const name = String(svc?.name || id).toLowerCase();
    const idWords = String(id || '').toLowerCase().split('_').filter(Boolean);
    const nameWords = name.split(/[^a-z0-9]+/).filter((w) => w && w.length >= 4);
    const probes = [name, ...idWords, ...nameWords];
    let score = 0;
    for (const p of probes) {
      if (!p) continue;
      if (t.includes(p)) score += (p.includes(' ') ? 3 : 1);
    }
    if (score > 0) hits.push({ id, score, name: String(svc?.name || id) });
  }
  hits.sort((a, b) => b.score - a.score);
  return hits;
}

async function syncBookingToConversation({ accountId, to, account, item }) {
  const from = normalizePhone(item?.customerPhone);
  if (!accountId || !to || !from) return;

  const inferredFromNotes = (!trimText(item?.serviceName) && !trimText(item?.serviceId))
    ? inferServicesFromWorkspace(account || {}, item?.notes || '')
    : [];
  if (!trimText(item?.serviceName) && inferredFromNotes.length > 0) {
    item.serviceId = String(inferredFromNotes[0].id || '');
    item.serviceName = String(inferredFromNotes[0].name || inferredFromNotes[0].id || 'Service request');
  }

  const lines = bookingSummaryLines(item);
  const serviceRequired = lines[2];
  const summaryText = lines.join('\n');
  const bookingText = summaryText;
  const configuredService = readWorkspaceServices(account || {})?.[String(item?.serviceId || '').trim()] || {};
  const bookingAmount = parseServiceAmount(item?.amount || configuredService?.price);
  const inferredList = inferServicesFromWorkspace(account || {}, `${item?.serviceName || ''} ${item?.notes || ''}`);
  const servicesList = inferredList.slice(0, 5).map((x) => String(x.id));
  const servicesSummary = inferredList.slice(0, 5).map((x) => `- ${x.name}`).join('\n');

  await recordBookingSync({
    tenant: { accountId: String(accountId), to: String(to) },
    to: String(to),
    from: String(from),
    bookingStart: Number(item?.start || Date.now()),
    bookingEnd: Number(item?.end || (Date.now() + 60 * 60 * 1000)),
    bookingId: String(item?.id || ''),
    source: 'public_booking',
    service: String(item?.serviceName || item?.serviceId || 'Appointment').trim(),
    serviceRequired,
    servicesList,
    servicesSummary,
    customerName: String(item?.customerName || '').trim(),
    customerPhone: String(from),
    customerEmail: String(item?.customerEmail || '').trim(),
    amount: bookingAmount,
    notes: summaryText,
    appendMessage: true,
    messageText: bookingText,
    route: '/api/public/booking/:token/book'
  });

  let invoiceSync = {
    ok: false,
    pdf: { generated: false, url: '', error: 'not_attempted' },
    email: { attempted: 0, delivered: 0, failed: 0, provider: '', firstError: '' },
    payment: { available: false, provider: '', status: '', url: '', amountCents: 0, currency: 'usd', reason: 'not_attempted' }
  };
  try {
    const invoiceResult = await ensureInvoiceForBookedConversation({
      accountId: String(accountId),
      to: String(to),
      from: String(from),
      bookingStart: Number(item?.start || Date.now()),
      bookingEnd: Number(item?.end || (Date.now() + 60 * 60 * 1000)),
      bookingId: String(item?.id || ''),
      source: 'public_booking',
      customerPaymentReturnUrl: String(item?.customerPaymentReturnUrl || '')
    });
    if (invoiceResult && typeof invoiceResult === 'object') {
      invoiceSync = {
        ok: invoiceResult.ok === true,
        invoiceId: String(invoiceResult?.invoice?.id || ''),
        invoiceNumber: String(invoiceResult?.invoice?.invoiceNumber || ''),
        pdf: {
          generated: invoiceResult?.pdf?.generated === true,
          url: String(invoiceResult?.pdf?.url || ''),
          error: String(invoiceResult?.pdf?.error || '')
        },
        email: {
          attempted: Number(invoiceResult?.email?.attempted || 0),
          delivered: Number(invoiceResult?.email?.delivered || 0),
          failed: Number(invoiceResult?.email?.failed || 0),
          provider: String(invoiceResult?.email?.provider || ''),
          firstError: String(invoiceResult?.email?.firstError || '')
        },
        payment: {
          available: invoiceResult?.payment?.available === true,
          provider: String(invoiceResult?.payment?.provider || ''),
          status: String(invoiceResult?.payment?.status || ''),
          url: String(invoiceResult?.payment?.url || ''),
          amountCents: Number(invoiceResult?.payment?.amountCents || 0),
          currency: String(invoiceResult?.payment?.currency || 'usd'),
          reason: String(invoiceResult?.payment?.reason || '')
        }
      };
    }
  } catch (err) {
    console.error('public booking invoice sync failed:', err?.message || err);
    invoiceSync = {
      ok: false,
      pdf: { generated: false, url: '', error: String(err?.message || 'invoice_sync_failed') },
      email: { attempted: 0, delivered: 0, failed: 0, provider: '', firstError: String(err?.message || 'invoice_sync_failed') }
    };
  }

  await evaluateTrigger('booking_created', {
    tenant: { accountId: String(accountId), to: String(to) },
    to: String(to),
    from: String(from),
    eventData: {
      bookingTime: Number(item?.start || Date.now()),
      bookingEndTime: Number(item?.end || (Date.now() + 60 * 60 * 1000)),
      service: String(item?.serviceName || item?.serviceId || 'Appointment'),
      customerName: String(item?.customerName || '').trim(),
      customerPhone: String(from),
      serviceRequired
    }
  });

  const leadEvent = createLeadEvent(accountId, {
    convoKey: `${to}__${from}`,
    channel: 'web',
    type: 'booking_created',
    payload: {
      bookingId: String(item?.id || ''),
      service: String(item?.serviceName || item?.serviceId || 'Appointment'),
      customerName: String(item?.customerName || '').trim()
    }
  });
  await handleSignal(accountId, leadEvent);
  return { invoiceSync };
}

publicBookingRouter.get('/invoice/:token/pdf', validateParams(invoicePdfTokenParamSchema), (req, res) => {
  const token = String(req.params?.token || '').trim();
  const found = getCustomerInvoiceByPdfToken(token);
  if (!found?.invoice || !found?.account) return res.status(404).json({ error: 'Invoice not found' });
  const pdf = buildInvoicePdfBuffer({ account: found.account, invoice: found.invoice });
  const filename = `${String(found.invoice?.invoiceNumber || found.invoice?.id || 'invoice')}.pdf`.replace(/[^\w.\-]+/g, '_');
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  return res.send(pdf);
});

function getAccountByTokenOr404(token, res) {
  const data = loadData();
  const found = findAccountByBookingToken(data, token);
  if (!found?.account) {
    res.status(404).json({ error: 'Booking page not found' });
    return null;
  }
  return { data, found };
}

function buildBusyRangesForBooking(account, { ignoreBookingId = '' } = {}) {
  const busy = [];
  const bufferMin = Math.max(0, Number(account?.scheduling?.bufferMin || 0));
  const padMs = bufferMin * 60 * 1000;
  for (const ev of Array.isArray(account?.calendarEvents) ? account.calendarEvents : []) {
    const s = Number(ev?.start);
    const e = Number(ev?.end);
    if (!Number.isFinite(s) || !Number.isFinite(e) || e <= s) continue;
    busy.push([s - padMs, e + padMs]);
  }
  for (const b of Array.isArray(account?.internalBookings) ? account.internalBookings : []) {
    if (ignoreBookingId && String(b?.id || '') === String(ignoreBookingId)) continue;
    const status = String(b?.status || 'booked').toLowerCase();
    if (status === 'canceled') continue;
    const s = Number(b?.start);
    const e = Number(b?.end);
    if (!Number.isFinite(s) || !Number.isFinite(e) || e <= s) continue;
    busy.push([s - padMs, e + padMs]);
  }
  return busy;
}

function slotConflicts(account, start, end, opts = {}) {
  const busy = buildBusyRangesForBooking(account, opts);
  for (const [s, e] of busy) {
    if (start < e && end > s) return true;
  }
  return false;
}

function dayKeyInTz(ms, tz) {
  const d = new Date(ms);
  const safeTz = normalizeTimezone(tz);
  return new Intl.DateTimeFormat('en-CA', { timeZone: safeTz, year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
}

function canBookUnderDailyLimit(account, startMs, { ignoreBookingId = '' } = {}) {
  const maxPerDay = Math.max(0, Number(account?.scheduling?.maxBookingsPerDay || 0));
  if (maxPerDay <= 0) return true;
  const tz = normalizeTimezone(account?.workspace?.timezone);
  const key = dayKeyInTz(startMs, tz);
  const count = (Array.isArray(account?.internalBookings) ? account.internalBookings : [])
    .filter((b) => String(b?.status || '').toLowerCase() !== 'canceled')
    .filter((b) => !ignoreBookingId || String(b?.id || '') !== String(ignoreBookingId))
    .filter((b) => Number.isFinite(Number(b?.start)))
    .filter((b) => dayKeyInTz(Number(b.start), tz) === key)
    .length;
  return count < maxPerDay;
}

function nextAvailableSlots(account, { startMs, durationMin, ignoreBookingId = '' } = {}) {
  const tz = normalizeTimezone(account?.workspace?.timezone);
  const date = dayKeyInTz(Number(startMs || Date.now()), tz);
  const availability = listAvailability(account, {
    date,
    days: 3,
    durationMin: Math.max(15, Number(durationMin || 60)),
    ignoreBookingId
  });
  const out = [];
  for (const day of Array.isArray(availability?.days) ? availability.days : []) {
    for (const slot of Array.isArray(day?.slots) ? day.slots : []) {
      out.push({
        start: Number(slot?.start || 0),
        end: Number(slot?.end || 0),
        label: String(slot?.label || '')
      });
      if (out.length >= 5) return { timezone: availability.timezone, slots: out };
    }
  }
  return { timezone: availability.timezone, slots: out };
}

function isExactSlotAvailable(account, { start, end, durationMin, ignoreBookingId = '' } = {}) {
  const tz = normalizeTimezone(account?.workspace?.timezone);
  const date = dayKeyInTz(Number(start || 0), tz);
  const availability = listAvailability(account, {
    date,
    days: 1,
    durationMin: Math.max(15, Number(durationMin || 60)),
    ignoreBookingId
  });
  const day = Array.isArray(availability?.days) ? availability.days[0] : null;
  const slots = Array.isArray(day?.slots) ? day.slots : [];
  const match = slots.find((s) => Number(s?.start || 0) === Number(start) && Number(s?.end || 0) === Number(end));
  return { ok: Boolean(match), timezone: availability.timezone };
}

function findRecentDuplicateBooking(account, { customerPhone, start, end, serviceId = '', serviceName = '' } = {}) {
  const phone = normalizePhone(customerPhone);
  const sid = String(serviceId || '').trim().toLowerCase();
  const sname = String(serviceName || '').trim().toLowerCase();
  const now = Date.now();
  const recentWindowMs = 10 * 60 * 1000;
  return (Array.isArray(account?.internalBookings) ? account.internalBookings : []).find((b) => {
    const status = String(b?.status || 'booked').toLowerCase();
    if (status === 'canceled') return false;
    if (Number(b?.start) !== Number(start) || Number(b?.end) !== Number(end)) return false;
    if (normalizePhone(b?.customerPhone) !== phone) return false;
    const bid = String(b?.serviceId || '').trim().toLowerCase();
    const bname = String(b?.serviceName || '').trim().toLowerCase();
    if (sid && bid && sid !== bid) return false;
    if (!sid && sname && bname && sname !== bname) return false;
    const createdAt = Number(b?.createdAt || 0);
    if (createdAt > 0 && (now - createdAt) > recentWindowMs) return false;
    return true;
  }) || null;
}

publicBookingRouter.get('/booking/:token/config', validateParams(bookingTokenParamSchema), (req, res) => {
  const resolved = getAccountByTokenOr404(req.params?.token, res);
  if (!resolved) return;
  const { data, found } = resolved;
  const account = found.account;
  const scheduling = ensureSchedulingConfig(account);
  saveDataDebounced(data);

  const servicesRaw = readWorkspaceServices(account);
  const services = Object.entries(servicesRaw || {}).map(([id, svc]) => ({
    id,
    name: String(svc?.name || id),
    price: String(svc?.price || ''),
    durationMin: Math.max(30, Math.round(Number(svc?.hoursMin || 1) * 60))
  }));

  return res.json({
    ok: true,
    businessName: String(account?.workspace?.identity?.businessName || account?.businessName || 'Business'),
    timezone: String(account?.workspace?.timezone || 'America/New_York'),
    mode: String(scheduling?.mode || 'manual'),
    bookingUrl: publicBookingUrlForAccount(account),
    services
  });
});

publicBookingRouter.get('/booking/:token/availability', validateParams(bookingTokenParamSchema), validateQuery(bookingAvailabilityQuerySchema), (req, res) => {
  const resolved = getAccountByTokenOr404(req.params?.token, res);
  if (!resolved) return;
  const { data, found } = resolved;
  const account = found.account;
  ensureSchedulingConfig(account);
  saveDataDebounced(data);

  const serviceId = String(req.query?.serviceId || '').trim();
  const date = String(req.query?.date || '').trim();
  const days = Math.max(1, Math.min(14, Number(req.query?.days) || 5));
  const durationMin = Number(req.query?.durationMin) > 0
    ? Number(req.query.durationMin)
    : serviceDurationMin(account, serviceId);

  const ignoreBookingId = String(req.query?.ignoreBookingId || '').trim();
  const availability = listAvailability(account, { date, days, durationMin });
  if (ignoreBookingId) {
    const adjusted = listAvailability(account, { date, days, durationMin, ignoreBookingId });
    return res.json({ ok: true, ...adjusted });
  }
  return res.json({ ok: true, ...availability });
});

publicBookingRouter.post('/booking/:token/book', validateParams(bookingTokenParamSchema), validateBody(bookingCreateSchema), async (req, res) => {
  const resolved = getAccountByTokenOr404(req.params?.token, res);
  if (!resolved) return;
  const { data, found } = resolved;
  const to = String(found.to || '');
  const account = found.account;
  ensureSchedulingConfig(account);

  const customerName = String(req.body?.customerName || '').trim();
  const customerPhone = normalizePhone(req.body?.customerPhone);
  const customerEmail = String(req.body?.customerEmail || '').trim();
  const serviceId = String(req.body?.serviceId || '').trim();
  const serviceName = String(req.body?.serviceName || '').trim();
  const notes = String(req.body?.notes || '').trim();
  const start = Number(req.body?.start);
  const end = Number(req.body?.end);

  if (!customerName) return res.status(400).json({ error: 'customerName is required' });
  if (!customerPhone) return res.status(400).json({ error: 'customerPhone is required' });
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
    return res.status(400).json({ error: 'Invalid start/end time' });
  }
  if (start < Date.now() - 60000) {
    return res.status(400).json({ error: 'Cannot book in the past' });
  }

  const durationMin = Math.max(15, Math.round((end - start) / 60000));
  const duplicate = findRecentDuplicateBooking(account, { customerPhone, start, end, serviceId, serviceName });
  if (duplicate) {
    const manageUrlExisting = `/book/${encodeURIComponent(String(req.params?.token || ''))}?manage=${encodeURIComponent(String(duplicate.manageToken || ''))}`;
    return res.json({ ok: true, duplicate: true, booking: duplicate, manageUrl: manageUrlExisting });
  }
  if (slotConflicts(account, start, end)) {
    const next = nextAvailableSlots(account, { startMs: start, durationMin });
    return res.status(409).json({
      error: 'Selected slot is no longer available',
      code: 'slot_conflict',
      nextAvailable: next.slots,
      timezone: next.timezone
    });
  }
  const exactSlot = isExactSlotAvailable(account, { start, end, durationMin });
  if (!exactSlot.ok) {
    const next = nextAvailableSlots(account, { startMs: start, durationMin });
    return res.status(409).json({
      error: 'Selected slot is unavailable in current timezone/business hours',
      code: 'invalid_slot',
      timezone: exactSlot.timezone,
      nextAvailable: next.slots
    });
  }

  if (!canBookUnderDailyLimit(account, start)) {
    const next = nextAvailableSlots(account, { startMs: start, durationMin });
    return res.status(409).json({
      error: 'Daily booking limit reached for selected date',
      code: 'daily_limit',
      nextAvailable: next.slots,
      timezone: next.timezone
    });
  }

  account.internalBookings = Array.isArray(account.internalBookings) ? account.internalBookings : [];
  const item = {
    id: bookingId(),
    start,
    end,
    status: 'booked',
    customerName,
    customerPhone,
    customerEmail,
    serviceId,
    serviceName: serviceName || serviceId || 'Appointment',
    notes,
    manageToken: bookingManageToken(),
    createdAt: Date.now(),
    source: 'public_booking'
  };
  const manageUrl = `/book/${encodeURIComponent(String(req.params?.token || ''))}?manage=${encodeURIComponent(item.manageToken)}`;
  item.customerPaymentReturnUrl = `${inferPublicBaseUrl(req)}${manageUrl}`;
  account.internalBookings.push(item);
  account.internalBookings = account.internalBookings.slice(-5000);
  saveDataDebounced(data);
  flushDataNow();

  const accountId = String(account?.accountId || account?.id || '');
  if (to && accountId) {
    const titleService = String(item.serviceName || 'Appointment').trim();
    const titleCustomer = String(customerName || '').trim();
    const title = titleCustomer ? `${titleService} - ${titleCustomer}` : titleService;
    pushBookingToConnectedCalendars(
      { to, accountId },
      {
        remoteId: item.id,
        title,
        start: item.start,
        end: item.end,
        location: ''
      }
    );
    let syncResult = null;
    try {
      syncResult = await syncBookingToConversation({ accountId, to, account, item });
    } catch (err) {
      console.error('public booking conversation sync failed:', err?.message || err);
    }
    return res.json({
      ok: true,
      booking: item,
      manageUrl,
      invoice: syncResult?.invoiceSync || null
    });
  }
  return res.json({ ok: true, booking: item, manageUrl, invoice: null });
});

publicBookingRouter.get('/booking/:token/manage/:manageToken', validateParams(bookingManageParamSchema), (req, res) => {
  const resolved = getAccountByTokenOr404(req.params?.token, res);
  if (!resolved) return;
  const account = resolved.found.account;
  const manageToken = String(req.params?.manageToken || '').trim();
  const booking = (Array.isArray(account?.internalBookings) ? account.internalBookings : [])
    .find((b) => String(b?.manageToken || '') === manageToken);
  if (!booking) return res.status(404).json({ error: 'Booking not found' });
  return res.json({ ok: true, booking });
});

publicBookingRouter.post('/booking/:token/manage/:manageToken/cancel', validateParams(bookingManageParamSchema), validateBody(noBodySchema), (req, res) => {
  const resolved = getAccountByTokenOr404(req.params?.token, res);
  if (!resolved) return;
  const { data, found } = resolved;
  const account = found.account;
  const manageToken = String(req.params?.manageToken || '').trim();
  const booking = (Array.isArray(account?.internalBookings) ? account.internalBookings : [])
    .find((b) => String(b?.manageToken || '') === manageToken);
  if (!booking) return res.status(404).json({ error: 'Booking not found' });
  booking.status = 'canceled';
  booking.canceledAt = Date.now();
  saveDataDebounced(data);
  flushDataNow();
  return res.json({ ok: true, booking });
});

publicBookingRouter.post('/booking/:token/manage/:manageToken/reschedule', validateParams(bookingManageParamSchema), validateBody(bookingRescheduleSchema), (req, res) => {
  const resolved = getAccountByTokenOr404(req.params?.token, res);
  if (!resolved) return;
  const { data, found } = resolved;
  const account = found.account;
  const manageToken = String(req.params?.manageToken || '').trim();
  const booking = (Array.isArray(account?.internalBookings) ? account.internalBookings : [])
    .find((b) => String(b?.manageToken || '') === manageToken);
  if (!booking) return res.status(404).json({ error: 'Booking not found' });
  if (String(booking?.status || '').toLowerCase() === 'canceled') {
    return res.status(400).json({ error: 'Canceled booking cannot be rescheduled' });
  }

  const start = Number(req.body?.start);
  const end = Number(req.body?.end);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
    return res.status(400).json({ error: 'Invalid start/end time' });
  }
  if (start < Date.now() - 60000) return res.status(400).json({ error: 'Cannot reschedule into the past' });
  const durationMin = Math.max(15, Math.round((end - start) / 60000));
  if (slotConflicts(account, start, end, { ignoreBookingId: booking.id })) {
    const next = nextAvailableSlots(account, { startMs: start, durationMin, ignoreBookingId: booking.id });
    return res.status(409).json({
      error: 'Selected slot is no longer available',
      code: 'slot_conflict',
      nextAvailable: next.slots,
      timezone: next.timezone
    });
  }
  const exactSlot = isExactSlotAvailable(account, { start, end, durationMin, ignoreBookingId: booking.id });
  if (!exactSlot.ok) {
    const next = nextAvailableSlots(account, { startMs: start, durationMin, ignoreBookingId: booking.id });
    return res.status(409).json({
      error: 'Selected slot is unavailable in current timezone/business hours',
      code: 'invalid_slot',
      timezone: exactSlot.timezone,
      nextAvailable: next.slots
    });
  }
  if (!canBookUnderDailyLimit(account, start, { ignoreBookingId: booking.id })) {
    const next = nextAvailableSlots(account, { startMs: start, durationMin, ignoreBookingId: booking.id });
    return res.status(409).json({
      error: 'Daily booking limit reached for selected date',
      code: 'daily_limit',
      nextAvailable: next.slots,
      timezone: next.timezone
    });
  }

  booking.start = start;
  booking.end = end;
  booking.updatedAt = Date.now();
  booking.status = 'booked';
  saveDataDebounced(data);
  flushDataNow();
  return res.json({ ok: true, booking });
});

module.exports = { publicBookingRouter };

