const crypto = require('crypto');
const { loadData, saveDataDebounced, ensureAccountForTo } = require('../store/dataStore');
const {
  ensureBillingShape,
  normalizeBillingStatus,
  recordPaymentFailure,
  recordPaymentSuccess
} = require('./billingPolicyService');

function maskSecret(value, { left = 6, right = 4 } = {}) {
  const v = String(value || '').trim();
  if (!v) return '';
  if (v.length <= left + right) return `${v.slice(0, 2)}***`;
  return `${v.slice(0, left)}...${v.slice(-right)}`;
}

function defaultStripeConfig() {
  return {
    enabled: false,
    secretKey: '',
    publishableKey: '',
    webhookSecret: '',
    customerId: '',
    accountId: '',
    accountEmail: '',
    accountDisplayName: '',
    connectedAt: null,
    lastTestedAt: null,
    lastStatus: null,
    lastError: null
  };
}

function appendIntegrationLog(account, type, message) {
  account.integrationLogs = Array.isArray(account.integrationLogs) ? account.integrationLogs : [];
  account.integrationLogs.unshift({
    id: crypto.randomUUID(),
    ts: Date.now(),
    type: String(type || ''),
    message: String(message || '')
  });
  account.integrationLogs = account.integrationLogs.slice(0, 200);
}

function ensureStripeConfig(account) {
  account.integrations = account.integrations && typeof account.integrations === 'object'
    ? account.integrations
    : {};
  const existing = account.integrations.stripe && typeof account.integrations.stripe === 'object'
    ? account.integrations.stripe
    : {};
  account.integrations.stripe = {
    ...defaultStripeConfig(),
    ...existing
  };
  return account.integrations.stripe;
}

function accountByTenant(data, tenant) {
  const to = String(tenant?.to || '').trim();
  const accountId = String(tenant?.accountId || '').trim();
  if (!to || !accountId) throw new Error('Missing tenant context');
  const account = ensureAccountForTo(data, to, { autoCreate: true });
  if (!account) throw new Error('Account not found');
  if (String(account.accountId || account.id || '') !== accountId) {
    throw new Error('Tenant/account mismatch');
  }
  return { account, to };
}

function stripeSnapshotFromConfig(cfg) {
  const current = cfg && typeof cfg === 'object' ? cfg : defaultStripeConfig();
  return {
    enabled: current.enabled === true,
    secretKeyMasked: maskSecret(current.secretKey),
    hasSecretKey: Boolean(String(current.secretKey || '').trim()),
    publishableKeyMasked: maskSecret(current.publishableKey),
    hasPublishableKey: Boolean(String(current.publishableKey || '').trim()),
    webhookSecretMasked: maskSecret(current.webhookSecret, { left: 4, right: 4 }),
    hasWebhookSecret: Boolean(String(current.webhookSecret || '').trim()),
    customerId: String(current.customerId || ''),
    accountId: String(current.accountId || ''),
    accountEmail: String(current.accountEmail || ''),
    accountDisplayName: String(current.accountDisplayName || ''),
    connectedAt: current.connectedAt ? Number(current.connectedAt) : null,
    lastTestedAt: current.lastTestedAt ? Number(current.lastTestedAt) : null,
    lastStatus: current.lastStatus ? String(current.lastStatus) : null,
    lastError: current.lastError ? String(current.lastError) : null
  };
}

function validateStripeInput(input, current = null) {
  const src = input && typeof input === 'object' ? input : {};
  const secretKey = String(src.secretKey || '').trim() || String(current?.secretKey || '').trim();
  const publishableKey = String(src.publishableKey || '').trim() || String(current?.publishableKey || '').trim();
  const webhookSecret = String(src.webhookSecret || '').trim() || String(current?.webhookSecret || '').trim();
  const customerId = String(src.customerId || '').trim() || String(current?.customerId || '').trim();

  if (!secretKey) throw new Error('Stripe secret key is required');
  if (!/^sk_(test|live)_[A-Za-z0-9]+$/.test(secretKey)) {
    throw new Error('Stripe secret key format is invalid');
  }
  if (publishableKey && !/^pk_(test|live)_[A-Za-z0-9]+$/.test(publishableKey)) {
    throw new Error('Stripe publishable key format is invalid');
  }
  if (customerId && !/^cus_[A-Za-z0-9]+$/.test(customerId)) {
    throw new Error('Stripe customer ID format is invalid');
  }

  return {
    secretKey,
    publishableKey,
    webhookSecret,
    customerId
  };
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

async function testStripeCredentials(secretKey) {
  const account = await stripeRequest(secretKey, '/v1/account');
  return {
    id: String(account?.id || ''),
    email: String(account?.email || ''),
    businessName: String(account?.business_profile?.name || ''),
    chargesEnabled: account?.charges_enabled === true
  };
}

async function fetchStripeInvoices(secretKey, customerId) {
  if (!customerId) return [];
  const invoices = await stripeRequest(secretKey, `/v1/invoices?customer=${encodeURIComponent(customerId)}&limit=10`);
  const items = Array.isArray(invoices?.data) ? invoices.data : [];
  return items.map((inv) => ({
    id: String(inv?.id || ''),
    number: String(inv?.number || inv?.id || ''),
    date: Number(inv?.created || 0) * 1000,
    amount: Number(inv?.amount_due || inv?.amount_paid || 0),
    status: String(inv?.status || 'open'),
    pdfUrl: inv?.invoice_pdf ? String(inv.invoice_pdf) : null
  }));
}

async function syncStripeBillingForTenant(tenant) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const cfg = ensureStripeConfig(account);
  if (cfg.enabled !== true) throw new Error('Stripe is not connected');
  if (!cfg.secretKey) throw new Error('Stripe secret key is missing');

  const [acctInfo, invoices] = await Promise.all([
    testStripeCredentials(cfg.secretKey),
    fetchStripeInvoices(cfg.secretKey, cfg.customerId)
  ]);

  account.billing = account.billing && typeof account.billing === 'object' ? account.billing : {};
  account.billing.provider = 'stripe';
  account.billing.isLive = true;
  account.billing.updatedAt = Date.now();
  account.billing.invoices = invoices;
  account.billing.portalUrl = account.billing.portalUrl || null;
  account.billing.activity = [
    {
      id: `ba_${Date.now()}`,
      ts: Date.now(),
      type: 'stripe_sync',
      message: `Stripe sync completed (${invoices.length} invoices)`
    },
    ...(Array.isArray(account.billing.activity) ? account.billing.activity : [])
  ].slice(0, 20);

  account.integrations.stripe = {
    ...cfg,
    accountId: acctInfo.id,
    accountEmail: acctInfo.email,
    accountDisplayName: acctInfo.businessName,
    lastTestedAt: Date.now(),
    lastStatus: 'ok',
    lastError: null
  };

  appendIntegrationLog(account, 'stripe.sync', `Stripe sync completed (${invoices.length} invoices)`);
  saveDataDebounced(data);
  return {
    ok: true,
    invoicesImported: invoices.length,
    stripe: stripeSnapshotFromConfig(account.integrations.stripe)
  };
}

async function connectStripeForTenant(tenant, input = {}) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const current = ensureStripeConfig(account);
  const validated = validateStripeInput(input, current);
  const tested = await testStripeCredentials(validated.secretKey);
  const now = Date.now();

  account.integrations.stripe = {
    ...current,
    ...validated,
    enabled: true,
    accountId: tested.id,
    accountEmail: tested.email,
    accountDisplayName: tested.businessName,
    connectedAt: current.connectedAt || now,
    lastTestedAt: now,
    lastStatus: 'ok',
    lastError: null
  };

  account.billing = account.billing && typeof account.billing === 'object' ? account.billing : {};
  account.billing.provider = 'stripe';
  account.billing.isLive = true;
  account.billing.updatedAt = now;

  appendIntegrationLog(account, current.enabled ? 'stripe.update' : 'stripe.connect', `Stripe connected (${tested.id})`);
  saveDataDebounced(data);

  return {
    ok: true,
    stripe: stripeSnapshotFromConfig(account.integrations.stripe)
  };
}

async function testStripeForTenant(tenant) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const cfg = ensureStripeConfig(account);
  if (cfg.enabled !== true) throw new Error('Stripe is not connected');
  const tested = await testStripeCredentials(cfg.secretKey);
  account.integrations.stripe = {
    ...cfg,
    accountId: tested.id,
    accountEmail: tested.email,
    accountDisplayName: tested.businessName,
    lastTestedAt: Date.now(),
    lastStatus: 'ok',
    lastError: null
  };
  appendIntegrationLog(account, 'stripe.test', `Stripe test succeeded (${tested.id})`);
  saveDataDebounced(data);
  return {
    ok: true,
    accountId: tested.id,
    accountEmail: tested.email,
    stripe: stripeSnapshotFromConfig(account.integrations.stripe)
  };
}

function disconnectStripeForTenant(tenant) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const current = ensureStripeConfig(account);
  account.integrations.stripe = {
    ...defaultStripeConfig(),
    connectedAt: current.connectedAt || null,
    lastTestedAt: Date.now()
  };
  account.billing = account.billing && typeof account.billing === 'object' ? account.billing : {};
  account.billing.provider = 'demo';
  account.billing.isLive = false;
  account.billing.portalUrl = null;
  account.billing.updatedAt = Date.now();
  appendIntegrationLog(account, 'stripe.disconnect', 'Stripe disconnected');
  saveDataDebounced(data);
  return {
    ok: true,
    stripe: stripeSnapshotFromConfig(account.integrations.stripe)
  };
}

function getTenantStripeSnapshot(tenant) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const cfg = ensureStripeConfig(account);
  return { stripe: stripeSnapshotFromConfig(cfg) };
}

function getStripeWebhookSecretByTo(to) {
  const data = loadData();
  const account = ensureAccountForTo(data, String(to || '').trim(), { autoCreate: false });
  if (!account) return '';
  const cfg = ensureStripeConfig(account);
  return String(cfg.webhookSecret || '').trim();
}

function upsertInvoice(account, invoice) {
  const next = {
    id: String(invoice?.id || ''),
    number: String(invoice?.number || invoice?.id || ''),
    date: Number(invoice?.created || 0) * 1000,
    amount: Number(invoice?.amount_due || invoice?.amount_paid || 0),
    status: String(invoice?.status || 'open'),
    pdfUrl: invoice?.invoice_pdf ? String(invoice.invoice_pdf) : null
  };
  if (!next.id) return null;
  account.billing = account.billing && typeof account.billing === 'object' ? account.billing : {};
  account.billing.invoices = Array.isArray(account.billing.invoices) ? account.billing.invoices : [];
  const idx = account.billing.invoices.findIndex((x) => String(x?.id || '') === next.id);
  if (idx >= 0) account.billing.invoices[idx] = next;
  else account.billing.invoices.unshift(next);
  account.billing.invoices = account.billing.invoices.slice(0, 100);
  return next;
}

function applyCustomerCheckoutSession(account, session) {
  const metadata = session?.metadata && typeof session.metadata === 'object' ? session.metadata : {};
  const invoiceId = String(metadata.invoiceId || '').trim();
  if (!invoiceId) return null;
  account.customerBilling = account.customerBilling && typeof account.customerBilling === 'object'
    ? account.customerBilling
    : {};
  account.customerBilling.invoices = Array.isArray(account.customerBilling.invoices)
    ? account.customerBilling.invoices
    : [];
  const invoice = account.customerBilling.invoices.find((inv) => String(inv?.id || '') === invoiceId);
  if (!invoice) return null;
  const paid = String(session?.payment_status || '').toLowerCase() === 'paid'
    || String(session?.status || '').toLowerCase() === 'complete';
  invoice.payment = invoice.payment && typeof invoice.payment === 'object' ? invoice.payment : {};
  invoice.payment.provider = 'stripe_checkout';
  invoice.payment.checkoutSessionId = String(session?.id || invoice.payment.checkoutSessionId || '');
  invoice.payment.paymentIntentId = String(session?.payment_intent || invoice.payment.paymentIntentId || '');
  invoice.payment.status = paid ? 'paid' : String(session?.payment_status || session?.status || 'open');
  invoice.payment.paidAt = paid ? Date.now() : (invoice.payment.paidAt || null);
  invoice.paymentStatus = paid ? 'paid' : invoice.payment.status;
  invoice.paymentMethod = 'card';
  invoice.status = paid ? 'close' : (invoice.status || 'booked');
  invoice.updatedAt = Date.now();
  return invoice;
}

function applyStripeWebhookEventForTo(to, event) {
  const tenantTo = String(to || '').trim();
  if (!tenantTo) throw new Error('Missing tenant number');
  const data = loadData();
  const account = ensureAccountForTo(data, tenantTo, { autoCreate: false });
  if (!account) throw new Error('Account not found');

  const cfg = ensureStripeConfig(account);
  const billing = ensureBillingShape(account);
  billing.provider = 'stripe';
  billing.isLive = true;
  billing.updatedAt = Date.now();

  const type = String(event?.type || '');
  const obj = event?.data?.object || {};
  let message = `Stripe event: ${type}`;

  if (type.startsWith('invoice.')) {
    const inv = upsertInvoice(account, obj);
    if (inv) message = `Invoice ${inv.number || inv.id} ${inv.status}`;
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
  }

  if (type.startsWith('customer.subscription.')) {
    const sub = obj || {};
    const rawStatus = String(sub?.status || billing.plan.status || 'active').toLowerCase();
    let normalized = normalizeBillingStatus(rawStatus);
    if (normalized === 'active' && (rawStatus === 'incomplete_expired' || rawStatus === 'incomplete')) {
      normalized = 'unpaid';
    }
    if (rawStatus === 'paused') normalized = 'past_due';
    billing.plan.status = normalized;
    if (sub?.trial_end) billing.plan.trialEndsAt = Number(sub.trial_end) * 1000;
    if (sub?.current_period_end) billing.plan.nextBillingAt = Number(sub.current_period_end) * 1000;
    if (sub?.canceled_at) billing.plan.endsAt = Number(sub.canceled_at) * 1000;
    if (normalized === 'unpaid' || normalized === 'canceled') {
      billing.dunning.lockedAt = Date.now();
    }
    message = `Subscription ${normalized}`;
  }

  if (type === 'checkout.session.completed') {
    const updated = applyCustomerCheckoutSession(account, obj);
    if (updated) {
      message = `Customer invoice ${String(updated.invoiceNumber || updated.id)} paid`;
    }
  }

  billing.activity = [
    {
      id: `ba_${Date.now()}`,
      ts: Date.now(),
      type: 'stripe_webhook',
      message
    },
    ...(Array.isArray(billing.activity) ? billing.activity : [])
  ].slice(0, 50);

  cfg.lastStatus = 'ok';
  cfg.lastError = null;
  cfg.lastTestedAt = Date.now();
  appendIntegrationLog(account, 'stripe.webhook', message);
  saveDataDebounced(data);

  return { ok: true, type, message };
}

module.exports = {
  connectStripeForTenant,
  testStripeForTenant,
  syncStripeBillingForTenant,
  disconnectStripeForTenant,
  getTenantStripeSnapshot,
  getStripeWebhookSecretByTo,
  applyStripeWebhookEventForTo
};
