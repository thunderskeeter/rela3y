const express = require('express');
const crypto = require('crypto');
const {
  getStripeWebhookSecretByTo,
  applyStripeWebhookEventForTo
} = require('../services/stripeIntegrationService');
const { claimWebhookEvent } = require('../services/webhookIdempotencyService');
const { loadData, getAccountByTo } = require('../store/dataStore');
const { emitEvent } = require('../services/notificationService');

const stripeWebhooksRouter = express.Router();
const ALLOWED_STRIPE_EVENT_PREFIX = ['invoice.', 'customer.subscription.'];

function safeEqual(a, b) {
  const aa = Buffer.from(String(a || ''), 'utf8');
  const bb = Buffer.from(String(b || ''), 'utf8');
  if (aa.length !== bb.length) return false;
  return crypto.timingSafeEqual(aa, bb);
}

function parseStripeSignature(header) {
  const raw = String(header || '').trim();
  const out = { t: '', v1: [] };
  if (!raw) return out;
  for (const part of raw.split(',')) {
    const [k, v] = part.split('=', 2).map((x) => String(x || '').trim());
    if (!k || !v) continue;
    if (k === 't') out.t = v;
    if (k === 'v1') out.v1.push(v);
  }
  return out;
}

function verifyStripeSignature(rawPayload, signatureHeader, webhookSecret) {
  const parsed = parseStripeSignature(signatureHeader);
  if (!parsed.t || !parsed.v1.length || !webhookSecret) return false;
  const ts = Number(parsed.t || 0);
  if (!Number.isFinite(ts)) return false;
  const ageMs = Math.abs(Date.now() - (ts * 1000));
  if (ageMs > 5 * 60 * 1000) return false;
  const signedPayload = `${parsed.t}.${rawPayload}`;
  const expected = crypto
    .createHmac('sha256', String(webhookSecret))
    .update(signedPayload, 'utf8')
    .digest('hex');
  return parsed.v1.some((candidate) => safeEqual(candidate, expected));
}

stripeWebhooksRouter.post('/', async (req, res) => {
  try {
    const to = String(req.query?.to || '').trim();
    if (to.length > 32 || !/^\+\d{8,15}$/.test(to)) return res.status(400).json({ error: 'Invalid tenant selector: to' });
    if (!to) return res.status(400).json({ error: 'Missing tenant selector: to' });
    const webhookSecret = getStripeWebhookSecretByTo(to);
    if (!webhookSecret) return res.status(403).json({ error: 'Stripe webhook is not configured for tenant' });

    const signature = String(req.headers['stripe-signature'] || '').trim();
    const rawPayload = Buffer.isBuffer(req.body) ? req.body.toString('utf8') : String(req.body || '');
    if (!verifyStripeSignature(rawPayload, signature, webhookSecret)) {
      return res.status(400).json({ error: 'Invalid Stripe signature' });
    }

    let event = {};
    try {
      event = JSON.parse(rawPayload);
    } catch {
      return res.status(400).json({ error: 'Invalid JSON payload' });
    }
    const eventType = String(event?.type || '').trim().toLowerCase();
    if (!eventType) return res.status(400).json({ error: 'Missing Stripe event type' });
    if (!ALLOWED_STRIPE_EVENT_PREFIX.some((p) => eventType.startsWith(p))) {
      return res.status(400).json({ error: 'Unsupported Stripe event type' });
    }
    if (!event?.data || typeof event.data !== 'object' || !event.data.object || typeof event.data.object !== 'object') {
      return res.status(400).json({ error: 'Invalid Stripe event payload' });
    }
    const eventId = String(event?.id || '').trim();
    if (!eventId) return res.status(400).json({ error: 'Missing Stripe event id' });
    const dedupe = await claimWebhookEvent(`to:${to}`, 'stripe', eventId);
    if (dedupe.duplicate === true) {
      return res.json({ ok: true, received: true, duplicate: true, eventId });
    }

    const result = applyStripeWebhookEventForTo(to, event);
    return res.json({ ok: true, received: true, eventId, ...result });
  } catch (err) {
    try {
      const to = String(req.query?.to || '').trim();
      if (to) {
        const data = loadData();
        const accountRef = getAccountByTo(data, to);
        const accountId = String(accountRef?.id || accountRef?.accountId || '').trim();
        if (accountId) {
          emitEvent({ accountId, to }, {
            type: 'failed_webhook',
            to,
            from: 'stripe',
            conversationId: '',
            meta: { route: '/webhooks/stripe', error: String(err?.message || 'stripe_webhook_failed') }
          });
        }
      }
    } catch {}
    return res.status(400).json({ error: err?.message || 'Stripe webhook handling failed' });
  }
});

module.exports = { stripeWebhooksRouter };
