const express = require('express');
const { loadData, saveDataDebounced, ensureAccountForTo } = require('../store/dataStore');
const { z, validateBody } = require('../utils/validate');
const {
  normalizeBillingStatus,
  ensureBillingShape,
  computeBillingLockState,
  startTrial
} = require('../services/billingPolicyService');
const { APP_PUBLIC_BASE_URL, NODE_ENV } = require('../config/runtime');

const billingRouter = express.Router();

const checkoutSchema = z.object({
  planKey: z.enum(['starter', 'pro', 'growth']).optional().default('pro'),
  cadence: z.enum(['monthly', 'annual']).optional().default('monthly'),
  returnUrl: z.string().trim().url().max(2048).optional()
});

const billingDetailsPatchSchema = z.object({
  companyName: z.string().trim().max(160).optional(),
  billingEmail: z.string().trim().email().max(254).optional(),
  addressLine1: z.string().trim().max(160).optional(),
  addressLine2: z.string().trim().max(160).optional(),
  city: z.string().trim().max(120).optional(),
  state: z.string().trim().max(120).optional(),
  postalCode: z.string().trim().max(32).optional(),
  country: z.string().trim().max(64).optional(),
  taxId: z.string().trim().max(64).optional()
});

const planChangeSchema = z.object({
  planKey: z.enum(['starter', 'pro', 'growth']),
  seatsTotal: z.coerce.number().int().min(1).max(500).optional()
});

const trialStartSchema = z.object({
  days: z.coerce.number().int().min(1).max(30).optional().default(14)
});

const PLAN_CATALOG = {
  starter: { key: 'starter', name: 'Starter', priceMonthly: 79, interval: 'month', seatDefault: 3 },
  pro: { key: 'pro', name: 'Pro', priceMonthly: 129, interval: 'month', seatDefault: 10 },
  growth: { key: 'growth', name: 'Growth', priceMonthly: 249, interval: 'month', seatDefault: 25 }
};

function defaultAccountBilling() {
  const now = Date.now();
  return {
    provider: 'demo',
    isLive: false,
    plan: {
      key: 'pro',
      name: 'Pro',
      priceMonthly: 129,
      interval: 'month',
      status: 'active',
      trialEndsAt: null,
      nextBillingAt: now + (1000 * 60 * 60 * 24 * 18),
      endsAt: null,
      seats: { used: 4, total: 10 }
    },
    usage: {
      cycleResetsAt: now + (1000 * 60 * 60 * 24 * 18),
      messagesSent: { used: 12480, limit: 20000 },
      automationsRun: { used: 3870, limit: 10000 },
      activeConversations: { used: 196, limit: 500 }
    },
    paymentMethod: {
      brand: 'Visa',
      last4: '4242',
      expMonth: 12,
      expYear: 2027
    },
    details: {
      companyName: '',
      billingEmail: '',
      addressLine1: '',
      addressLine2: '',
      city: '',
      state: '',
      postalCode: '',
      country: 'US',
      taxId: ''
    },
    invoices: [
      { id: 'inv_demo_001', number: 'INV-1001', date: now - (1000 * 60 * 60 * 24 * 3), amount: 12900, status: 'paid', pdfUrl: null },
      { id: 'inv_demo_000', number: 'INV-1000', date: now - (1000 * 60 * 60 * 24 * 33), amount: 12900, status: 'paid', pdfUrl: null }
    ],
    activity: [
      { id: 'ba_001', ts: now - (1000 * 60 * 40), type: 'invoice_paid', message: 'Invoice INV-1001 paid' },
      { id: 'ba_002', ts: now - (1000 * 60 * 60 * 2), type: 'payment_method_updated', message: 'Payment method updated' }
    ],
    portalUrl: null,
    platformStripeCustomerId: '',
    updatedAt: now
  };
}

function normalizeInvoiceStatus(status) {
  const v = String(status || '').toLowerCase();
  const allowed = ['paid', 'open', 'past_due', 'refunded'];
  return allowed.includes(v) ? v : 'open';
}

function normalizeBillingDetails(details) {
  const d = details && typeof details === 'object' ? details : {};
  return {
    companyName: String(d.companyName || '').trim(),
    billingEmail: String(d.billingEmail || '').trim(),
    addressLine1: String(d.addressLine1 || '').trim(),
    addressLine2: String(d.addressLine2 || '').trim(),
    city: String(d.city || '').trim(),
    state: String(d.state || '').trim(),
    postalCode: String(d.postalCode || '').trim(),
    country: String(d.country || 'US').trim(),
    taxId: String(d.taxId || '').trim()
  };
}

function normalizeReturnUrl(value) {
  const raw = String(value || '').trim();
  const fallback = `${String(APP_PUBLIC_BASE_URL || 'http://127.0.0.1:3001').replace(/\/$/, '')}/settings/billing`;
  if (!raw) return fallback;
  try {
    const parsed = new URL(raw);
    if (!/^https?:$/i.test(parsed.protocol)) {
      return fallback;
    }
    if (NODE_ENV === 'production' && parsed.protocol !== 'https:') {
      return fallback;
    }
    const host = String(parsed.hostname || '').toLowerCase();
    if (NODE_ENV === 'production' && (host === 'localhost' || host === '127.0.0.1' || host === '::1')) {
      return fallback;
    }
    return parsed.toString();
  } catch {
    return fallback;
  }
}

function getTenantAccount(data, tenant) {
  const to = String(tenant?.to || '').trim();
  const accountId = String(tenant?.accountId || '').trim();
  if (!to || !accountId) throw new Error('Missing tenant context');
  const account = ensureAccountForTo(data, to, { autoCreate: true });
  if (!account) throw new Error('Account not found');
  if (String(account.accountId || account.id || '') !== accountId) {
    const err = new Error('Tenant/account mismatch');
    err.status = 403;
    throw err;
  }
  account.billing = account.billing || defaultAccountBilling();
  ensureBillingShape(account);
  return { to, account };
}

function stripeAuthHeader(secretKey) {
  const token = Buffer.from(`${String(secretKey || '').trim()}:`, 'utf8').toString('base64');
  return `Basic ${token}`;
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

async function stripeRequest(secretKey, path, { method = 'GET', form = null } = {}) {
  const headers = {
    Authorization: stripeAuthHeader(secretKey),
    Accept: 'application/json'
  };
  let body;
  if (form && typeof form === 'object') {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(form)) {
      if (v == null) continue;
      params.set(String(k), String(v));
    }
    body = params.toString();
    headers['Content-Type'] = 'application/x-www-form-urlencoded';
  }
  const res = await fetch(`https://api.stripe.com${path}`, { method, headers, body });
  const raw = await res.text();
  let parsed = {};
  try { parsed = JSON.parse(raw); } catch {}
  if (!res.ok) {
    const detail = String(parsed?.error?.message || parsed?.message || '').trim();
    const err = new Error(detail || `Stripe request failed (${res.status})`);
    err.status = res.status;
    throw err;
  }
  return parsed;
}

async function createStripeBillingPortalSession({ secretKey, customerId, returnUrl }) {
  const parsed = await stripeRequest(secretKey, '/v1/billing_portal/sessions', {
    method: 'POST',
    form: {
      customer: String(customerId || '').trim(),
      return_url: String(returnUrl || '').trim()
    }
  });
  return String(parsed?.url || '').trim();
}

async function createStripeSubscriptionCheckoutSession({ secretKey, customerId, plan, cadence, returnUrl, tenant }) {
  const selectedCadence = String(cadence || 'monthly') === 'annual' ? 'annual' : 'monthly';
  const monthlyPrice = Number(plan?.priceMonthly || 0);
  const amount = selectedCadence === 'annual'
    ? Math.round(monthlyPrice * 12 * 0.85 * 100)
    : Math.round(monthlyPrice * 100);
  const interval = selectedCadence === 'annual' ? 'year' : 'month';
  const normalizedReturn = String(returnUrl || '').trim();
  const joiner = normalizedReturn.includes('?') ? '&' : '?';
  const successUrl = `${normalizedReturn}${joiner}billing=success&session_id={CHECKOUT_SESSION_ID}`;
  const cancelUrl = `${normalizedReturn}${joiner}billing=cancel`;

  const parsed = await stripeRequest(secretKey, '/v1/checkout/sessions', {
    method: 'POST',
    form: {
      mode: 'subscription',
      customer: String(customerId || '').trim(),
      success_url: successUrl,
      cancel_url: cancelUrl,
      'line_items[0][quantity]': '1',
      'line_items[0][price_data][currency]': 'usd',
      'line_items[0][price_data][unit_amount]': String(amount),
      'line_items[0][price_data][recurring][interval]': interval,
      'line_items[0][price_data][product_data][name]': `Arc Relay ${String(plan?.name || 'Plan')}`,
      'metadata[accountId]': String(tenant?.accountId || ''),
      'metadata[to]': String(tenant?.to || ''),
      'metadata[planKey]': String(plan?.key || ''),
      'metadata[cadence]': selectedCadence,
      'subscription_data[metadata][accountId]': String(tenant?.accountId || ''),
      'subscription_data[metadata][to]': String(tenant?.to || ''),
      'subscription_data[metadata][planKey]': String(plan?.key || ''),
      'subscription_data[metadata][cadence]': selectedCadence
    }
  });
  return {
    id: String(parsed?.id || '').trim(),
    url: String(parsed?.url || '').trim()
  };
}

async function createPlatformStripeCustomer({ secretKey, account, tenant, billing }) {
  const details = billing?.details && typeof billing.details === 'object' ? billing.details : {};
  const businessName = String(details.companyName || account?.businessName || account?.workspace?.identity?.businessName || '').trim();
  const billingEmail = String(details.billingEmail || '').trim();
  const response = await stripeRequest(secretKey, '/v1/customers', {
    method: 'POST',
    form: {
      name: businessName || `Workspace ${String(tenant?.accountId || '')}`,
      email: billingEmail || undefined,
      'metadata[accountId]': String(tenant?.accountId || ''),
      'metadata[to]': String(tenant?.to || '')
    }
  });
  return String(response?.id || '').trim();
}

billingRouter.get('/billing/summary', (req, res) => {
  try {
    const data = loadData();
    const { account } = getTenantAccount(data, req.tenant);
    const billing = ensureBillingShape(account);
    const plan = billing.plan || {};
    const status = normalizeBillingStatus(plan.status);
    const now = Date.now();
    const daysLeft = status === 'trialing' && plan.trialEndsAt
      ? Math.max(0, Math.ceil((Number(plan.trialEndsAt) - now) / (1000 * 60 * 60 * 24)))
      : null;
    const lock = computeBillingLockState(account, now);
    saveDataDebounced(data);
    return res.json({
      demoMode: billing.isLive !== true,
      accountId: String(account.accountId || account.id || ''),
      billing: {
        provider: billing.provider || 'demo',
        isLive: billing.isLive === true,
        plan: {
          key: String(plan.key || 'pro'),
          name: String(plan.name || 'Pro'),
          priceMonthly: Number(plan.priceMonthly || 0),
          interval: String(plan.interval || 'month'),
          status,
          trialEndsAt: plan.trialEndsAt ? Number(plan.trialEndsAt) : null,
          trialDaysLeft: daysLeft,
          nextBillingAt: plan.nextBillingAt ? Number(plan.nextBillingAt) : null,
          endsAt: plan.endsAt ? Number(plan.endsAt) : null,
          seats: {
            used: Number(plan?.seats?.used || 0),
            total: Number(plan?.seats?.total || 0)
          }
        },
        lock: {
          locked: lock.locked === true,
          reason: lock.reason || null
        },
        dunning: {
          attempts: Number(billing?.dunning?.attempts || 0),
          maxAttempts: Number(billing?.dunning?.maxAttempts || 4),
          nextRetryAt: billing?.dunning?.nextRetryAt ? Number(billing.dunning.nextRetryAt) : null,
          graceEndsAt: billing?.dunning?.graceEndsAt ? Number(billing.dunning.graceEndsAt) : null,
          lockedAt: billing?.dunning?.lockedAt ? Number(billing.dunning.lockedAt) : null
        },
        usage: billing.usage || {},
        paymentMethod: billing.paymentMethod || null,
        details: normalizeBillingDetails(billing.details),
        activity: Array.isArray(billing.activity) ? billing.activity.slice(0, 5) : [],
        updatedAt: Number(billing.updatedAt || now)
      }
    });
  } catch (err) {
    return res.status(err?.status || 400).json({ error: err?.message || 'Failed to load billing summary' });
  }
});

billingRouter.get('/billing/plans', (_req, res) => {
  const plans = Object.values(PLAN_CATALOG).map((p) => ({
    key: p.key,
    name: p.name,
    priceMonthly: p.priceMonthly,
    interval: p.interval,
    seatDefault: p.seatDefault
  }));
  return res.json({ ok: true, plans });
});

billingRouter.get('/billing/invoices', (req, res) => {
  try {
    const data = loadData();
    const { account } = getTenantAccount(data, req.tenant);
    const billing = account.billing || defaultAccountBilling();
    const invoices = (Array.isArray(billing.invoices) ? billing.invoices : []).map((inv) => ({
      id: String(inv.id || ''),
      number: String(inv.number || ''),
      date: Number(inv.date || Date.now()),
      amount: Number(inv.amount || 0),
      status: normalizeInvoiceStatus(inv.status),
      pdfUrl: inv.pdfUrl ? String(inv.pdfUrl) : null
    }));
    saveDataDebounced(data);
    return res.json({
      demoMode: billing.isLive !== true,
      invoices,
      total: invoices.length
    });
  } catch (err) {
    return res.status(err?.status || 400).json({ error: err?.message || 'Failed to load billing invoices' });
  }
});

billingRouter.get('/billing/portal', (req, res) => {
  try {
    const data = loadData();
    const { account } = getTenantAccount(data, req.tenant);
    const billing = account.billing || defaultAccountBilling();
    const url = billing.isLive === true && billing.portalUrl ? String(billing.portalUrl) : null;
    saveDataDebounced(data);
    return res.json({
      demoMode: billing.isLive !== true,
      url,
      message: url ? 'ok' : 'Billing Portal available after connecting Stripe'
    });
  } catch (err) {
    return res.status(err?.status || 400).json({ error: err?.message || 'Failed to load billing portal' });
  }
});

billingRouter.post('/billing/checkout', validateBody(checkoutSchema), async (req, res) => {
  try {
    const data = loadData();
    const tenant = req.tenant;
    const { account } = getTenantAccount(data, tenant);
    const billing = account.billing || defaultAccountBilling();
    const fallbackReturnUrl = normalizeReturnUrl(req.body?.returnUrl);
    const planKey = String(req.body?.planKey || billing?.plan?.key || 'pro').toLowerCase();
    const plan = PLAN_CATALOG[planKey] || PLAN_CATALOG.pro;
    const cadence = String(req.body?.cadence || 'monthly') === 'annual' ? 'annual' : 'monthly';

    const platformStripe = ensurePlatformStripeConfig(data);
    const secretKey = String(platformStripe.secretKey || '').trim();
    if (!secretKey || platformStripe.enabled !== true) {
      return res.status(400).json({ error: 'Billing checkout is unavailable until superadmin connects platform Stripe in Developer settings' });
    }

    let customerId = String(billing.platformStripeCustomerId || '').trim();
    if (!customerId) {
      customerId = await createPlatformStripeCustomer({ secretKey, account, tenant, billing });
    }
    let session = null;
    try {
      session = await createStripeSubscriptionCheckoutSession({
        secretKey,
        customerId,
        plan,
        cadence,
        returnUrl: fallbackReturnUrl,
        tenant
      });
    } catch (err) {
      const noSuchCustomer = String(err?.message || '').toLowerCase().includes('no such customer');
      if (!noSuchCustomer) throw err;
      customerId = await createPlatformStripeCustomer({ secretKey, account, tenant, billing });
      session = await createStripeSubscriptionCheckoutSession({
        secretKey,
        customerId,
        plan,
        cadence,
        returnUrl: fallbackReturnUrl,
        tenant
      });
    }
    account.billing = {
      ...billing,
      provider: 'stripe',
      isLive: true,
      platformStripeCustomerId: customerId,
      pendingCheckout: {
        sessionId: String(session?.id || ''),
        planKey: plan.key,
        cadence,
        createdAt: Date.now()
      },
      updatedAt: Date.now(),
      activity: [
        {
          id: `ba_${Date.now()}`,
          ts: Date.now(),
          type: 'checkout_created',
          message: `Stripe checkout started for ${plan.key} (${cadence})`
        },
        ...(Array.isArray(billing.activity) ? billing.activity : [])
      ].slice(0, 20)
    };
    saveDataDebounced(data);
    return res.json({ ok: true, url: session?.url || '', sessionId: session?.id || '', source: 'stripe_checkout' });
  } catch (err) {
    return res.status(err?.status || 400).json({ error: err?.message || 'Failed to create checkout session' });
  }
});

billingRouter.patch('/billing/details', validateBody(billingDetailsPatchSchema), (req, res) => {
  try {
    const patch = req.body || {};
    const data = loadData();
    const { account } = getTenantAccount(data, req.tenant);
    const billing = account.billing || defaultAccountBilling();
    const nextDetails = normalizeBillingDetails({
      ...(billing.details || {}),
      ...patch
    });
    account.billing = {
      ...billing,
      details: nextDetails,
      updatedAt: Date.now(),
      activity: [
        {
          id: `ba_${Date.now()}`,
          ts: Date.now(),
          type: 'settings_changed',
          message: 'Billing details updated'
        },
        ...(Array.isArray(billing.activity) ? billing.activity : [])
      ].slice(0, 20)
    };
    saveDataDebounced(data);
    return res.json({
      ok: true,
      demoMode: account.billing.isLive !== true,
      details: account.billing.details,
      updatedAt: account.billing.updatedAt
    });
  } catch (err) {
    return res.status(err?.status || 400).json({ error: err?.message || 'Failed to update billing details' });
  }
});

billingRouter.patch('/billing/plan', validateBody(planChangeSchema), (req, res) => {
  try {
    const { planKey, seatsTotal } = req.body || {};
    const selected = PLAN_CATALOG[String(planKey || '').trim().toLowerCase()];
    if (!selected) return res.status(400).json({ error: 'Invalid plan key' });

    const data = loadData();
    const { account } = getTenantAccount(data, req.tenant);
    const billing = ensureBillingShape(account);
    const currentKey = String(billing?.plan?.key || 'pro');
    const direction = selected.priceMonthly > Number(billing?.plan?.priceMonthly || 0) ? 'upgrade' : (selected.priceMonthly < Number(billing?.plan?.priceMonthly || 0) ? 'downgrade' : 'change');
    const nextSeatsTotal = Number.isFinite(Number(seatsTotal))
      ? Math.max(1, Number(seatsTotal))
      : Math.max(Number(billing?.plan?.seats?.total || selected.seatDefault || 1), Number(selected.seatDefault || 1));

    billing.plan.key = selected.key;
    billing.plan.name = selected.name;
    billing.plan.priceMonthly = selected.priceMonthly;
    billing.plan.interval = selected.interval;
    billing.plan.seats = {
      used: Math.min(Number(billing?.plan?.seats?.used || 0), nextSeatsTotal),
      total: nextSeatsTotal
    };
    if (!billing.plan.nextBillingAt) billing.plan.nextBillingAt = Date.now() + (1000 * 60 * 60 * 24 * 30);
    if (String(billing.plan.status || '') === 'canceled' || String(billing.plan.status || '') === 'unpaid') {
      billing.plan.status = 'active';
    }
    billing.updatedAt = Date.now();
    billing.activity = [
      {
        id: `ba_${Date.now()}`,
        ts: Date.now(),
        type: 'plan_changed',
        message: `${direction} ${currentKey} -> ${selected.key}`
      },
      ...(Array.isArray(billing.activity) ? billing.activity : [])
    ].slice(0, 50);
    saveDataDebounced(data);
    return res.json({ ok: true, plan: billing.plan, updatedAt: billing.updatedAt });
  } catch (err) {
    return res.status(err?.status || 400).json({ error: err?.message || 'Failed to update plan' });
  }
});

billingRouter.post('/billing/trial/start', validateBody(trialStartSchema), (req, res) => {
  try {
    const days = Number(req.body?.days || 14);
    const data = loadData();
    const { account } = getTenantAccount(data, req.tenant);
    const billing = ensureBillingShape(account);
    startTrial(account, { now: Date.now(), days });
    billing.updatedAt = Date.now();
    billing.activity = [
      {
        id: `ba_${Date.now()}`,
        ts: Date.now(),
        type: 'trial_started',
        message: `Trial started for ${days} day(s)`
      },
      ...(Array.isArray(billing.activity) ? billing.activity : [])
    ].slice(0, 50);
    saveDataDebounced(data);
    return res.json({
      ok: true,
      plan: billing.plan,
      dunning: billing.dunning,
      updatedAt: billing.updatedAt
    });
  } catch (err) {
    return res.status(err?.status || 400).json({ error: err?.message || 'Failed to start trial' });
  }
});

module.exports = { billingRouter };
