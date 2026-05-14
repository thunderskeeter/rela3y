const express = require("express");
const crypto = require('crypto');
const { getConversationById, loadData } = require("../store/dataStore");
const { advanceFlow } = require("../services/flowEngine");
const { evaluateTrigger, isWithinBusinessHours } = require("../services/automationEngine");
const { cancelJobsForConvo } = require("../services/scheduler");
const { emitEvent } = require("../services/notificationService");
const { pushBookingToConnectedCalendars } = require("../services/calendarIcsService");
const { getTenantTwilioConfig } = require("../services/twilioIntegrationService");
const { createLeadEvent, updateIntelligence } = require('../services/revenueIntelligenceService');
const { handleSignal } = require('../services/revenueOrchestrator');
const { claimWebhookEvent } = require('../services/webhookIdempotencyService');
const { ensureInvoiceForBookedConversation } = require('../services/customerInvoiceService');
const { processTwilioStatusCallback } = require('../services/providerCallbackService');
const { z, validateBody } = require('../utils/validate');
const { DEV_MODE, WEBHOOK_AUTH_TOKEN, WEBHOOK_DEV_SECRET } = require('../config/runtime');
const { requireTenant, assertTenantScope } = require('../utils/tenant');
const {
  recordInboundSms,
  recordMissedCall,
  recordBookingSync,
  recordOutboundAttempt,
  updateConversationStatusLegacy
} = require('../services/messagingBoundaryService');
const { getConversationDetail } = require('../services/conversationsService');

const webhooksRouter = express.Router();
const smsSchema = z.object({
  From: z.string().trim().min(3).optional(),
  from: z.string().trim().min(3).optional(),
  Body: z.string().trim().min(1).max(4000).optional(),
  text: z.string().trim().min(1).max(4000).optional(),
  MessageSid: z.string().trim().min(6).optional(),
  SmsSid: z.string().trim().min(6).optional()
}).passthrough();

const missedCallSchema = z.object({
  From: z.string().trim().min(3).optional(),
  from: z.string().trim().min(3).optional(),
  CallSid: z.string().trim().min(6).optional()
}).passthrough();

const voiceSchema = z.object({
  From: z.string().trim().optional(),
  CallSid: z.string().trim().optional(),
  DialCallStatus: z.string().trim().optional()
}).passthrough();

const twilioStatusSchema = z.object({
  MessageSid: z.string().trim().min(6).optional(),
  SmsSid: z.string().trim().min(6).optional(),
  MessageStatus: z.string().trim().min(1).optional(),
  SmsStatus: z.string().trim().min(1).optional(),
  ErrorCode: z.union([z.string(), z.number()]).optional(),
  ErrorMessage: z.string().trim().optional(),
  To: z.string().trim().optional(),
  From: z.string().trim().optional()
}).passthrough();

const eventSchema = z.object({
  id: z.string().trim().min(6).optional(),
  type: z.string().trim().min(1).max(128),
  from: z.string().trim().min(3).max(64),
  data: z.record(z.any()).optional()
}).passthrough();
const ALLOWED_EVENT_TYPES = new Set([
  'booking_created',
  'booking_reminder',
  'service_completed',
  'quote_sent',
  'lead_lost',
  'inactive_customer',
  'seasonal',
  'missed_call',
  'after_hours_inquiry',
  'inbound_message',
  'form_submit',
  'lead_stalled'
]);

function fingerprintWebhookEvent(parts = [], bucketMs = 15_000) {
  const bucket = Math.floor(Date.now() / Math.max(1, Number(bucketMs || 15_000)));
  const raw = [...parts.map((x) => String(x || '')), String(bucket)].join('|');
  return crypto.createHash('sha1').update(raw).digest('hex');
}

function safeEqual(a, b) {
  const aa = Buffer.from(String(a || ''), 'utf8');
  const bb = Buffer.from(String(b || ''), 'utf8');
  if (aa.length !== bb.length) return false;
  return crypto.timingSafeEqual(aa, bb);
}

function computeTwilioSignature(url, body, authToken) {
  const params = body && typeof body === 'object' && !Array.isArray(body) ? body : {};
  const keys = Object.keys(params).sort();
  let data = String(url || '');
  for (const key of keys) {
    const value = params[key];
    if (Array.isArray(value)) {
      for (const item of value) data += `${key}${item == null ? '' : String(item)}`;
    } else {
      data += `${key}${value == null ? '' : String(value)}`;
    }
  }
  return crypto.createHmac('sha1', String(authToken || '')).update(data).digest('base64');
}

function inferredRequestUrl(req) {
  const forwardedProto = String(req?.headers?.['x-forwarded-proto'] || '').split(',')[0].trim();
  const forwardedHost = String(req?.headers?.['x-forwarded-host'] || '').split(',')[0].trim();
  const proto = forwardedProto || String(req?.protocol || 'http');
  const host = forwardedHost || String(req?.get?.('host') || req?.headers?.host || '');
  return `${proto}://${host}${String(req?.originalUrl || req?.url || '')}`;
}

function inferredBaseUrl(req) {
  const forwardedProto = String(req?.headers?.['x-forwarded-proto'] || '').split(',')[0].trim();
  const forwardedHost = String(req?.headers?.['x-forwarded-host'] || '').split(',')[0].trim();
  const proto = forwardedProto || String(req?.protocol || 'http');
  const host = forwardedHost || String(req?.get?.('host') || req?.headers?.host || '');
  return `${proto}://${host}`;
}

function escapeXml(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function twiml(res, xmlBody) {
  res.set('Content-Type', 'text/xml; charset=utf-8');
  return res.status(200).send(xmlBody);
}

function hasValidTwilioSignature(req) {
  const token = WEBHOOK_AUTH_TOKEN;
  const sig = String(req?.headers?.['x-twilio-signature'] || '').trim();
  if (!token || !sig) return false;
  const url = inferredRequestUrl(req);
  const expected = computeTwilioSignature(url, req.body, token);
  return safeEqual(sig, expected);
}

function hasValidDevWebhookSecret(req) {
  if (DEV_MODE !== true) return false;
  const expected = String(WEBHOOK_DEV_SECRET || '').trim();
  if (!expected) return false;
  const provided = String(req?.headers?.['x-dev-webhook-secret'] || '').trim();
  return safeEqual(provided, expected);
}

webhooksRouter.use((req, res, next) => {
  // accountContext.requireTenantForWebhook already validated auth in the common case.
  if (req.webhookAuthOk === true) return next();
  if (hasValidTwilioSignature(req)) return next();
  if (hasValidDevWebhookSecret(req)) return next();
  return res.status(403).json({ error: 'Invalid webhook signature' });
});

function isVipContact(tenant, from) {
  const data = loadData();
  const to = String(tenant?.to || '');
  const list = Array.isArray(data?.vipList?.[to]) ? data.vipList[to] : [];
  const normalize = (value) => String(value || '').replace(/\D/g, '');
  const fromNorm = normalize(from);
  return list.some((v) =>
    String(v?.accountId || tenant?.accountId || '') === String(tenant?.accountId || '') &&
    v?.neverAutoReply === true &&
    normalize(v?.phone) === fromNorm
  );
}

function emitWebhookFailure(tenant, route, err, from = '') {
  if (!tenant?.accountId || !tenant?.to) return;
  emitEvent(tenant, {
    type: 'failed_webhook',
    to: String(tenant.to),
    from: String(from || ''),
    conversationId: '',
    meta: {
      route: String(route || ''),
      error: String(err?.message || 'webhook_failed')
    }
  });
}

async function processMissedCall({ tenant, from, to }) {
  const id = `${to}__${from}`;
  let result = await recordMissedCall({
    tenant,
    to: String(to),
    from: String(from),
    eventKey: `missed_call:${tenant.accountId}:${id}`,
    route: '/webhooks/missed-call'
  });
  let convo = result?.conversation || null;
  if (!convo) return { ok: false, status: 404, error: "Conversation not found" };

  const afterHours = !isWithinBusinessHours(String(to), loadData());
  const leadEvent = createLeadEvent(tenant.accountId, {
    convoKey: id,
    channel: 'call',
    type: afterHours ? 'after_hours_inquiry' : 'missed_call',
    payload: {
      source: 'webhook_missed_call',
      from: String(from),
      to: String(to),
      afterHours
    }
  });
  await handleSignal(tenant.accountId, leadEvent);
  convo = await getConversationDetail(tenant.accountId, id, { identifiers: { route: '/webhooks/missed-call' } }) || convo;
  if (convo?.accountId) {
    assertTenantScope(tenant.accountId, convo.accountId, { entity: 'webhook missed-call conversation' });
  }

  emitEvent(tenant, {
    type: "missed_call",
    to: String(to),
    from: String(from),
    conversationId: id
  });

  return { ok: true, conversation: convo };
}

// SMS webhook (Twilio-style + simulator JSON)
webhooksRouter.post("/sms", validateBody(smsSchema), async (req, res) => {
  try {
    const tenant = requireTenant(req);
    const from = req.body.From || req.body.from;
    const to = tenant.to;
    const text = req.body.Body || req.body.text;
    const mediaCountRaw = req.body.NumMedia;
    const mediaCount = Number.isFinite(Number(mediaCountRaw)) ? Number(mediaCountRaw) : 0;
    const hasMedia = mediaCount > 0;

    if (!from || !to || (!text && !hasMedia)) {
      return res.status(400).json({ error: "Missing from/to/text" });
    }
    const messageSid = String(req.body?.MessageSid || req.body?.SmsSid || '').trim();
    const smsEventId = messageSid || fingerprintWebhookEvent(['sms', tenant.accountId, to, from, text, mediaCount], 15_000);
    const dedupe = await claimWebhookEvent(tenant.accountId, 'twilio_sms', smsEventId);
    if (dedupe.duplicate === true) return res.json({ ok: true, duplicate: true, messageSid: smsEventId });

    const bodyText = String(text || (hasMedia ? '[photos uploaded]' : ''));
    const id = `${to}__${from}`;

    let leadEvent = null;
    let { conversation: convo, compliance } = await recordInboundSms({
      tenant,
      to: String(to),
      from: String(from),
      text: bodyText,
      mediaCount: hasMedia ? mediaCount : 0,
      eventKey: smsEventId,
      route: '/webhooks/sms'
    });
    if (!convo) return res.status(404).json({ error: "Conversation not found" });

    leadEvent = createLeadEvent(tenant.accountId, {
      convoKey: id,
      channel: 'sms',
      type: 'inbound_message',
      payload: {
        source: 'webhook_sms',
        text: bodyText,
        mediaCount: hasMedia ? mediaCount : 0,
        from: String(from),
        to: String(to)
      }
    });
    updateIntelligence(tenant.accountId, id, {
      intent: 'unknown',
      urgencyScore: 40,
      sentimentScore: 0,
      leadQualityScore: 50,
      notes: ['Inbound SMS received']
    });
    await handleSignal(tenant.accountId, leadEvent);

    cancelJobsForConvo(String(to), String(from), tenant.accountId, "customer_replied");

    if (compliance?.handled) {
      if (compliance.type === "opt_out") {
        cancelJobsForConvo(String(to), String(from), tenant.accountId, "opt_out");
        const stopEvent = createLeadEvent(tenant.accountId, {
          convoKey: id,
          channel: 'sms',
          type: 'opt_out',
          payload: {
            source: 'webhook_sms',
            stopAutomation: true,
            text: bodyText
          }
        });
        await handleSignal(tenant.accountId, stopEvent);
      }
      return res.json({ ok: true, compliance, conversation: convo });
    }

    const flowResult = await advanceFlow({ tenant, to: String(to), from: String(from), text: bodyText });
    if (flowResult) {
      convo = await getConversationDetail(tenant.accountId, id, { identifiers: { route: '/webhooks/sms' } }) || convo;
    }

    const latest = await getConversationDetail(tenant.accountId, id, { identifiers: { route: '/webhooks/sms' } });
    if (!latest?.flow || latest.flow.status !== "active") {
      const automationResults = await evaluateTrigger("inbound_sms", {
        tenant,
        to: String(to),
        from: String(from)
      });
      if (automationResults?.length > 0) {
        convo = await getConversationDetail(tenant.accountId, id, { identifiers: { route: '/webhooks/sms' } }) || convo;
      }
    }

    if (isVipContact(tenant, from)) {
      emitEvent(tenant, {
        type: "vip_message",
        to: String(to),
        from: String(from),
        conversationId: id
      });
    }

    return res.json({ ok: true, conversation: convo });
  } catch (err) {
    console.error("SMS webhook error:", err);
    try {
      const tenant = req?.tenant;
      const from = req?.body?.From || req?.body?.from || '';
      emitWebhookFailure(tenant, '/webhooks/sms', err, from);
    } catch {}
    const status = Number(err?.status || 500);
    return res.status(status).json({ error: status >= 500 ? "Internal server error" : (err?.message || 'Webhook rejected') });
  }
});

// Missed call webhook
webhooksRouter.post("/missed-call", validateBody(missedCallSchema), async (req, res) => {
  try {
    const tenant = requireTenant(req);
    const from = req.body.From || req.body.from;
    const to = tenant.to;

    if (!from || !to) {
      return res.status(400).json({ error: "Missing from/to" });
    }
    const callSid = String(req.body?.CallSid || '').trim();
    const callEventId = callSid || fingerprintWebhookEvent(['missed_call', tenant.accountId, to, from], 30_000);
    const dedupe = await claimWebhookEvent(tenant.accountId, 'twilio_call', callEventId);
    if (dedupe.duplicate === true) return res.json({ ok: true, duplicate: true, callSid: callEventId });

    const result = await processMissedCall({ tenant, from: String(from), to: String(to) });
    if (!result.ok) return res.status(result.status || 400).json({ error: result.error || "Failed to process missed call" });
    return res.json({ ok: true, conversation: result.conversation });
  } catch (err) {
    console.error("Missed call webhook error:", err);
    try {
      const tenant = req?.tenant;
      const from = req?.body?.From || req?.body?.from || '';
      emitWebhookFailure(tenant, '/webhooks/missed-call', err, from);
    } catch {}
    const status = Number(err?.status || 500);
    return res.status(status).json({ error: status >= 500 ? "Internal server error" : (err?.message || 'Webhook rejected') });
  }
});

// Twilio Voice webhook (incoming call on Twilio number)
webhooksRouter.post("/voice/incoming", validateBody(voiceSchema), async (req, res) => {
  try {
    const tenant = requireTenant(req);
    const caller = String(req.body?.From || '').trim();
    const to = String(tenant?.to || '').trim();
    const twilioCfg = getTenantTwilioConfig(tenant);
    const forwardTo = String(twilioCfg?.voiceForwardTo || '').trim();
    const timeoutSec = Number(twilioCfg?.voiceDialTimeoutSec || 20) || 20;
    const baseUrl = inferredBaseUrl(req);
    const actionUrl = `${baseUrl}/webhooks/voice/dial-result?to=${encodeURIComponent(to)}`;

    if (!forwardTo) {
      return twiml(
        res,
        `<?xml version="1.0" encoding="UTF-8"?><Response><Say>Sorry, we are unavailable right now. We will text you shortly.</Say><Hangup/></Response>`
      );
    }

    return twiml(
      res,
      `<?xml version="1.0" encoding="UTF-8"?><Response><Dial timeout="${Math.max(10, Math.min(60, timeoutSec))}" action="${escapeXml(actionUrl)}" method="POST"><Number>${escapeXml(forwardTo)}</Number></Dial></Response>`
    );
  } catch (err) {
    console.error("Voice incoming webhook error:", err);
    try {
      emitWebhookFailure(req?.tenant, '/webhooks/voice/incoming', err, req?.body?.From || '');
    } catch {}
    return twiml(
      res,
      `<?xml version="1.0" encoding="UTF-8"?><Response><Say>Sorry, we are unavailable right now.</Say><Hangup/></Response>`
    );
  }
});

// Twilio Voice dial status callback (after attempting to ring personal phone)
webhooksRouter.post("/voice/dial-result", validateBody(voiceSchema), async (req, res) => {
  try {
    const tenant = requireTenant(req);
    const from = String(req.body?.From || '').trim();
    const to = String(tenant?.to || '').trim();
    const dialStatus = String(req.body?.DialCallStatus || '').toLowerCase();
    const callSid = String(req.body?.CallSid || '').trim();
    if (callSid) {
      const dedupe = await claimWebhookEvent(tenant.accountId, 'twilio_dial_result', callSid);
      if (dedupe.duplicate === true) {
        return twiml(
          res,
          `<?xml version="1.0" encoding="UTF-8"?><Response><Hangup/></Response>`
        );
      }
    }
    const shouldTreatAsMissed = ['no-answer', 'busy', 'failed', 'canceled'].includes(dialStatus);

    if (from && to && shouldTreatAsMissed) {
      await processMissedCall({ tenant, from, to });
      return twiml(
        res,
        `<?xml version="1.0" encoding="UTF-8"?><Response><Say>Sorry we missed your call. We will text you now.</Say><Hangup/></Response>`
      );
    }

    return twiml(
      res,
      `<?xml version="1.0" encoding="UTF-8"?><Response><Hangup/></Response>`
    );
  } catch (err) {
    console.error("Voice dial-result webhook error:", err);
    try {
      emitWebhookFailure(req?.tenant, '/webhooks/voice/dial-result', err, req?.body?.From || '');
    } catch {}
    return twiml(
      res,
      `<?xml version="1.0" encoding="UTF-8"?><Response><Hangup/></Response>`
    );
  }
});

webhooksRouter.post("/twilio/status", validateBody(twilioStatusSchema), async (req, res) => {
  try {
    const tenant = requireTenant(req);
    const providerMessageId = String(req.body?.MessageSid || req.body?.SmsSid || '').trim();
    const providerStatus = String(req.body?.MessageStatus || req.body?.SmsStatus || '').trim();
    const errorCode = req.body?.ErrorCode == null ? '' : String(req.body.ErrorCode).trim();
    const errorMessage = String(req.body?.ErrorMessage || '').trim();
    if (!providerMessageId || !providerStatus) {
      return res.status(400).json({ error: 'Missing provider callback fields' });
    }
    const eventId = `twilio_status:${providerMessageId}:${providerStatus}:${errorCode || 'none'}`;
    const result = await processTwilioStatusCallback({
      tenant,
      providerMessageId,
      providerStatus,
      errorCode,
      errorMessage,
      eventId,
      rawPayload: req.body
    });
    return res.json({ ok: true, duplicate: result?.duplicate === true, unmatched: result?.unmatched === true, blocked: result?.blocked === true });
  } catch (err) {
    console.error("Twilio status webhook error:", err);
    try {
      emitWebhookFailure(req?.tenant, '/webhooks/twilio/status', err, req?.body?.To || req?.query?.to || '');
    } catch {}
    const status = Number(err?.status || 500);
    return res.status(status).json({ error: status >= 500 ? "Internal server error" : (err?.message || 'Webhook rejected') });
  }
});

// Generic event endpoint for status/event driven automations
webhooksRouter.post("/event", validateBody(eventSchema), async (req, res) => {
  try {
    const tenant = requireTenant(req);
    const { type, from, data: eventData } = req.body || {};
    const to = tenant.to;
    const data = loadData();

    if (!type || !to || !from) {
      return res.status(400).json({ error: "Missing type/to/from" });
    }
    const normalizedType = String(type || '').trim().toLowerCase();
    if (!ALLOWED_EVENT_TYPES.has(normalizedType)) {
      return res.status(400).json({ error: 'Unsupported event type' });
    }
    const explicitEventId = String(req.body?.id || '').trim();
    const derivedEventId = explicitEventId || fingerprintWebhookEvent(
      ['event', tenant.accountId, to, from, normalizedType, JSON.stringify(eventData || {})],
      15_000
    );
    const eventDedupe = await claimWebhookEvent(tenant.accountId, 'generic_event', derivedEventId);
    if (eventDedupe.duplicate === true) return res.json({ ok: true, duplicate: true, eventId: derivedEventId, actions: [] });

    if (normalizedType === "lead_lost") {
      await updateConversationStatusLegacy({
        tenant,
        to: String(to),
        from: String(from),
        status: 'lost',
        source: 'event_webhook',
        route: '/webhooks/event',
        requireExisting: false
      });
    }

    let bookingConversation = null;
    if (normalizedType === "booking_created") {
      const bookingStart = Number(eventData?.bookingTime || Date.now());
      const bookingEnd = Number(eventData?.bookingEndTime || (bookingStart + 60 * 60 * 1000));
      const serviceText = String(eventData?.service || '').trim();
      const vehicleText = String(eventData?.vehicle || '').trim();
      const amountNum = Number(eventData?.amount);
      bookingConversation = await recordBookingSync({
        tenant,
        to: String(to),
        from: String(from),
        bookingStart,
        bookingEnd,
        bookingId: String(eventData?.bookingId || derivedEventId),
        source: 'event_webhook',
        service: serviceText,
        serviceRequired: serviceText,
        customerName: String(eventData?.customerName || '').trim(),
        vehicle: vehicleText,
        amount: Number.isFinite(amountNum) ? amountNum : null,
        appendMessage: false,
        patchConversation(conversation) {
          conversation.leadData = conversation.leadData && typeof conversation.leadData === 'object' ? conversation.leadData : {};
          if (vehicleText) conversation.leadData.vehicle_model = conversation.leadData.vehicle_model || vehicleText;
          if (serviceText) {
            conversation.leadData.request = conversation.leadData.request || serviceText;
            conversation.leadData.service_required = conversation.leadData.service_required || serviceText;
            conversation.leadData.intent = conversation.leadData.intent || serviceText;
            if (!Array.isArray(conversation.leadData.services_list) || !conversation.leadData.services_list.length) {
              conversation.leadData.services_list = [serviceText];
            }
          }
          if (Number.isFinite(amountNum) && amountNum > 0) {
            conversation.amount = Number.isFinite(Number(conversation.amount || 0)) && Number(conversation.amount) > 0
              ? conversation.amount
              : amountNum;
            conversation.leadData.amount = Number.isFinite(Number(conversation.leadData.amount || 0)) && Number(conversation.leadData.amount) > 0
              ? conversation.leadData.amount
              : amountNum;
          }
          conversation.leadData.availability = conversation.leadData.availability || new Date(bookingStart).toLocaleString("en-US", {
            month: "short",
            day: "numeric",
            hour: "numeric",
            minute: "2-digit"
          });
        },
        route: '/webhooks/event'
      });

      const titleParts = [];
      if (eventData?.service) titleParts.push(String(eventData.service));
      if (eventData?.vehicle) titleParts.push(String(eventData.vehicle));
      const bookingAmountNum = Number(eventData?.amount);
      const amountLabel = Number.isFinite(bookingAmountNum) ? ` - $${Math.round(bookingAmountNum)}` : '';
      const bookingTitle = titleParts.length ? `${titleParts.join(' · ')}${amountLabel}` : `Booked Appointment${amountLabel}`;
      pushBookingToConnectedCalendars(tenant, {
        remoteId: `${to}__${from}__booking_created`,
        title: bookingTitle,
        start: bookingStart,
        end: bookingEnd,
        location: String(eventData?.location || '')
      });
      try {
        await ensureInvoiceForBookedConversation({
          accountId: tenant.accountId,
          to: String(to),
          from: String(from),
          bookingStart,
          bookingEnd,
          bookingId: String(eventData?.bookingId || ''),
          source: 'event_webhook'
        });
      } catch (err) {
        console.error('webhook invoice generation failed:', err?.message || err);
      }

      const dateObj = new Date(bookingStart);
      const prettyDate = dateObj.toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric" });
      const prettyTime = dateObj.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
      const followupText = `I see you booked on ${prettyDate} at ${prettyTime} — we will see you then. If you have any questions, feel free to ask.`;
      const convoForFollowup = bookingConversation || await getConversationDetail(tenant.accountId, `${to}__${from}`, { identifiers: { route: '/webhooks/event' } });
      if (convoForFollowup) {
        await recordOutboundAttempt({
          tenant,
          to: String(to),
          from: String(from),
          text: followupText,
          source: 'booking_confirmation',
          route: '/webhooks/event',
          requireExisting: true,
          meta: {
            auto: true,
            status: 'sent',
            bookingTime: bookingStart,
            outboundCorrelationId: `booking_confirmation:${tenant.accountId}:${String(eventData?.bookingId || derivedEventId)}`
          }
        });
      }
    }

    const enrichedEventData = { ...(eventData || {}) };
    if (normalizedType === "booking_created" && enrichedEventData.bookingTime) {
      enrichedEventData.bookingTime = Number(enrichedEventData.bookingTime);
    }

    const leadEvent = createLeadEvent(tenant.accountId, {
      convoKey: `${to}__${from}`,
      channel: normalizedType === 'booking_created' ? 'web' : 'chat',
      type: normalizedType,
      payload: {
        source: 'webhook_event',
        ...enrichedEventData
      }
    });
    await handleSignal(tenant.accountId, leadEvent);

    const results = await evaluateTrigger(normalizedType, {
      tenant,
      to: String(to),
      from: String(from),
      eventData: enrichedEventData
    });

    if (normalizedType === "booking_created" && enrichedEventData.bookingTime) {
      const reminderResults = await evaluateTrigger("booking_reminder", {
        tenant,
        to: String(to),
        from: String(from),
        eventData: enrichedEventData
      });
      results.push(...(reminderResults || []));
    }

    if (normalizedType === "booking_created") {
      emitEvent(tenant, {
        type: "new_booking",
        to: String(to),
        from: String(from),
        conversationId: `${to}__${from}`
      });
    }

    return res.json({ ok: true, actions: results });
  } catch (err) {
    console.error("Event webhook error:", err);
    try {
      emitWebhookFailure(req?.tenant, '/webhooks/event', err, req?.body?.from || '');
    } catch {}
    const status = Number(err?.status || 500);
    return res.status(status).json({ error: status >= 500 ? "Internal server error" : (err?.message || 'Webhook rejected') });
  }
});

module.exports = { webhooksRouter };
