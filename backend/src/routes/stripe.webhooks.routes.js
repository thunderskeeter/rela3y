const express = require('express');
const crypto = require('crypto');
const {
  getStripeWebhookSecretByTo,
  applyStripeWebhookEventForTo
} = require('../services/stripeIntegrationService');
const { claimWebhookEvent } = require('../services/webhookIdempotencyService');
const { loadData, saveDataDebounced, getAccountByTo, getAccountById } = require('../store/dataStore');
const { emitEvent } = require('../services/notificationService');
const {
  ensureBillingShape,
  normalizeBillingStatus,
  recordPaymentFailure,
  recordPaymentSuccess
} = require('../services/billingPolicyService');

const stripeWebhooksRouter = express.Router();
const ALLOWED_STRIPE_EVENT_PREFIX = ['invoice.', 'customer.subscription.', 'checkout.session.'];
const PLATFORM_PLAN_CATALOG = {
  starter: { key: 'starter', name: 'Starter', priceMonthly: 79 },
  pro: { key: 'pro', name: 'Pro', priceMonthly: 129 },
  growth: { key: 'growth', name: 'Growth', priceMonthly: 249 }
};

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

function ensurePlatformStripeConfig(data) {
  data.dev = data.dev && typeof data.dev === 'object' ? data.dev : {};
  const cfg = data.dev.platformBillingStripe && typeof data.dev.platformBillingStripe === 'object'
    ? data.dev.platformBillingStripe
    : {};
  data.dev.platformBillingStripe = {
    enabled: cfg.enabled === true,
    secretKey: String(cfg.secretKey || '').trim(),
    publishableKey: String(cfg.publishableKey || '').trim(),
    webhookSecret: String(cfg.webhookSecret || '').trim(),
    accountId: String(cfg.accountId || '').trim(),
    accountEmail: String(cfg.accountEmail || '').trim(),
    accountDisplayName: String(cfg.accountDisplayName || '').trim(),
    connectedAt: cfg.connectedAt ? Number(cfg.connectedAt) : null,
    lastTestedAt: cfg.lastTestedAt ? Number(cfg.lastTestedAt) : null,
    lastStatus: cfg.lastStatus ? String(cfg.lastStatus) : null,
    lastError: cfg.lastError ? String(cfg.lastError) : null
  };
  return data.dev.platformBillingStripe;
}

function parseStripeEventPayload(req) {
  const rawPayload = Buffer.isBuffer(req.body) ? req.body.toString('utf8') : String(req.body || '');
  let event = {};
  try {
    event = JSON.parse(rawPayload);
  } catch {
    const err = new Error('Invalid JSON payload');
    err.status = 400;
    throw err;
  }
  const eventType = String(event?.type || '').trim().toLowerCase();
  if (!eventType) {
    const err = new Error('Missing Stripe event type');
    err.status = 400;
    throw err;
  }
  if (!ALLOWED_STRIPE_EVENT_PREFIX.some((p) => eventType.startsWith(p))) {
    const err = new Error('Unsupported Stripe event type');
    err.status = 400;
    throw err;
  }
  if (!event?.data || typeof event.data !== 'object' || !event.data.object || typeof event.data.object !== 'object') {
    const err = new Error('Invalid Stripe event payload');
    err.status = 400;
    throw err;
  }
  const eventId = String(event?.id || '').trim();
  if (!eventId) {
    const err = new Error('Missing Stripe event id');
    err.status = 400;
    throw err;
  }
  return { rawPayload, event, eventType, eventId };
}

function upsertPlatformInvoice(account, invoice) {
  const next = {
    id: String(invoice?.id || ''),
    number: String(invoice?.number || invoice?.id || ''),
    date: Number(invoice?.created || 0) * 1000,
    amount: Number(invoice?.amount_due || invoice?.amount_paid || 0),
    status: String(invoice?.status || 'open'),
    pdfUrl: invoice?.invoice_pdf ? String(invoice.invoice_pdf) : null
  };
  if (!next.id) return null;
  account.billing.invoices = Array.isArray(account.billing.invoices) ? account.billing.invoices : [];
  const idx = account.billing.invoices.findIndex((x) => String(x?.id || '') === next.id);
  if (idx >= 0) account.billing.invoices[idx] = next;
  else account.billing.invoices.unshift(next);
  account.billing.invoices = account.billing.invoices.slice(0, 100);
  return next;
}

function resolvePlatformWebhookAccount(data, obj) {
  const metadata = obj?.metadata && typeof obj.metadata === 'object' ? obj.metadata : {};
  const subscriptionMetadata = obj?.subscription_details?.metadata && typeof obj.subscription_details.metadata === 'object'
    ? obj.subscription_details.metadata
    : {};
  const accountId = String(metadata.accountId || subscriptionMetadata.accountId || '').trim();
  const to = String(metadata.to || subscriptionMetadata.to || '').trim();
  if (accountId) {
    const found = getAccountById(data, accountId);
    if (found?.account) return { account: found.account, to: String(found.to || to || '') };
  }
  if (to) {
    const found = getAccountByTo(data, to);
    const account = found?.account || found;
    if (account) return { account, to };
  }
  const customerId = String(obj?.customer || '').trim();
  const subscriptionId = String(obj?.subscription || obj?.id || '').trim();
  for (const [candidateTo, account] of Object.entries(data.accounts || {})) {
    const billing = account?.billing && typeof account.billing === 'object' ? account.billing : {};
    if (customerId && String(billing.platformStripeCustomerId || '') === customerId) return { account, to: candidateTo };
    if (subscriptionId && String(billing.platformStripeSubscriptionId || '') === subscriptionId) return { account, to: candidateTo };
  }
  return null;
}

function applyPlatformStripeEvent(data, event) {
  const type = String(event?.type || '').trim().toLowerCase();
  const obj = event?.data?.object || {};
  const resolved = resolvePlatformWebhookAccount(data, obj);
  if (!resolved?.account) {
    const err = new Error('Platform Stripe event did not match a tenant subscription');
    err.status = 404;
    throw err;
  }
  const { account, to } = resolved;
  const billing = ensureBillingShape(account);
  billing.provider = 'stripe';
  billing.isLive = true;
  billing.updatedAt = Date.now();

  let message = `Platform Stripe event: ${type}`;
  if (type === 'checkout.session.completed') {
    const metadata = obj?.metadata && typeof obj.metadata === 'object' ? obj.metadata : {};
    const planKey = String(metadata.planKey || billing?.pendingCheckout?.planKey || billing?.plan?.key || 'pro').toLowerCase();
    const cadence = String(metadata.cadence || billing?.pendingCheckout?.cadence || 'monthly').toLowerCase();
    const plan = PLATFORM_PLAN_CATALOG[planKey] || PLATFORM_PLAN_CATALOG.pro;
    billing.platformStripeCustomerId = String(obj?.customer || billing.platformStripeCustomerId || '');
    billing.platformStripeSubscriptionId = String(obj?.subscription || billing.platformStripeSubscriptionId || '');
    billing.plan.key = plan.key;
    billing.plan.name = plan.name;
    billing.plan.priceMonthly = plan.priceMonthly;
    billing.plan.interval = cadence === 'annual' ? 'year' : 'month';
    billing.plan.status = normalizeBillingStatus(String(obj?.payment_status || '').toLowerCase() === 'paid' ? 'active' : billing.plan.status);
    billing.pendingCheckout = null;
    recordPaymentSuccess(account, { now: Date.now() });
    message = `Subscription checkout completed (${billing.plan.key})`;
  }

  if (type.startsWith('customer.subscription.')) {
    const rawStatus = String(obj?.status || billing.plan.status || 'active').toLowerCase();
    billing.platformStripeSubscriptionId = String(obj?.id || billing.platformStripeSubscriptionId || '');
    billing.platformStripeCustomerId = String(obj?.customer || billing.platformStripeCustomerId || '');
    billing.plan.status = normalizeBillingStatus(rawStatus);
    if (obj?.trial_end) billing.plan.trialEndsAt = Number(obj.trial_end) * 1000;
    if (obj?.current_period_end) billing.plan.nextBillingAt = Number(obj.current_period_end) * 1000;
    if (obj?.canceled_at) billing.plan.endsAt = Number(obj.canceled_at) * 1000;
    message = `Subscription ${billing.plan.status}`;
  }

  if (type.startsWith('invoice.')) {
    const inv = upsertPlatformInvoice(account, obj);
    if (obj?.customer) billing.platformStripeCustomerId = String(obj.customer);
    if (obj?.subscription) billing.platformStripeSubscriptionId = String(obj.subscription);
    if (type === 'invoice.payment_failed') {
      recordPaymentFailure(account, {
        now: Date.now(),
        reason: String(obj?.last_finalization_error?.message || obj?.failure_message || 'invoice_payment_failed')
      });
    }
    if (type === 'invoice.paid' || type === 'invoice.payment_succeeded') {
      recordPaymentSuccess(account, { now: Date.now() });
      if (obj?.period_end) billing.plan.nextBillingAt = Number(obj.period_end) * 1000;
    }
    message = inv ? `Subscription invoice ${inv.number || inv.id} ${inv.status}` : message;
  }

  billing.activity = [
    {
      id: `ba_${Date.now()}`,
      ts: Date.now(),
      type: 'platform_stripe_webhook',
      message
    },
    ...(Array.isArray(billing.activity) ? billing.activity : [])
  ].slice(0, 50);
  return { ok: true, type, to, accountId: String(account.accountId || account.id || ''), message };
}

stripeWebhooksRouter.post('/platform', async (req, res) => {
  try {
    const data = loadData();
    const cfg = ensurePlatformStripeConfig(data);
    const webhookSecret = String(cfg.webhookSecret || '').trim();
    if (!webhookSecret) return res.status(403).json({ error: 'Platform Stripe webhook is not configured' });

    const rawPayload = Buffer.isBuffer(req.body) ? req.body.toString('utf8') : String(req.body || '');
    const signature = String(req.headers['stripe-signature'] || '').trim();
    if (!verifyStripeSignature(rawPayload, signature, webhookSecret)) {
      return res.status(400).json({ error: 'Invalid Stripe signature' });
    }

    const { event, eventId } = parseStripeEventPayload(req);
    const dedupe = await claimWebhookEvent('platform', 'stripe', eventId);
    if (dedupe.duplicate === true) {
      return res.json({ ok: true, received: true, duplicate: true, eventId });
    }

    const result = applyPlatformStripeEvent(data, event);
    cfg.lastStatus = 'ok';
    cfg.lastError = null;
    cfg.lastTestedAt = Date.now();
    saveDataDebounced(data);
    return res.json({ ok: true, received: true, eventId, ...result });
  } catch (err) {
    try {
      const data = loadData();
      const cfg = ensurePlatformStripeConfig(data);
      cfg.lastStatus = 'error';
      cfg.lastError = String(err?.message || 'platform_stripe_webhook_failed');
      cfg.lastTestedAt = Date.now();
      saveDataDebounced(data);
    } catch {}
    return res.status(err?.status || 400).json({ error: err?.message || 'Platform Stripe webhook handling failed' });
  }
});

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

    const { event, eventId } = parseStripeEventPayload(req);
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
