const express = require('express');
const { z, validateBody, validateParams, validateQuery } = require('../utils/validate');
const {
  loadData,
  saveDataDebounced,
  ensureAccountForTo,
  getAccountById
} = require('../store/dataStore');
const {
  connectTwilioForTenant,
  testTwilioForTenant,
  disconnectTwilioForTenant,
  getTenantTwilioSnapshot
} = require('../services/twilioIntegrationService');
const {
  getMessagingAnalyticsSummary,
  getConversationByConvoKey,
  getConversationByRowId,
  getMessageById,
  getRecentFailedMessages
} = require('../services/messagingAnalyticsService');
const {
  ROLE_SET,
  normalizeEmail,
  normalizeRole,
  hashPassword,
  sanitizeUser,
  destroySessionsForUser
} = require('../utils/auth');
const { APP_PUBLIC_BASE_URL } = require('../config/runtime');

const adminRouter = express.Router();
const NICHE_TEMPLATE_KEYS = ['detailer'];
const NICHE_TEMPLATE_PRESETS = {
  detailer: {
    industry: 'detailer',
    defaultFlowId: 'detailing_missed_call_v1',
    services: {
      interior: { name: 'Interior Detail', price: '$100-150', hoursMin: 2, hoursMax: 2 },
      exterior: { name: 'Exterior Wash & Wax', price: '$80-120', hoursMin: 1, hoursMax: 2 },
      full: { name: 'Full Detail', price: '$200-300', hoursMin: 3, hoursMax: 4 },
      ceramic: { name: 'Ceramic Coating', price: '$500-800', hoursMin: 8, hoursMax: 16 },
      tint: { name: 'Window Tint', price: '$200-400', hoursMin: 2, hoursMax: 4 },
      headlight: { name: 'Headlight Restoration', price: '$80-160', hoursMin: 1, hoursMax: 2 },
      paint_correction: { name: 'Paint Correction', price: '$300-600', hoursMin: 4, hoursMax: 8 },
      ppf: { name: 'PPF', price: '$1200-2000', hoursMin: 8, hoursMax: 16 }
    },
    avgTicketValueCents: 27900
  }
};

const accountIdParamSchema = z.object({
  accountId: z.string().trim().min(1).max(80)
});

const userIdParamSchema = z.object({
  id: z.string().trim().min(1).max(80)
});

const platformStripeConnectSchema = z.object({
  secretKey: z.string().trim().max(256).optional().default(''),
  publishableKey: z.string().trim().max(256).optional().default(''),
  webhookSecret: z.string().trim().max(256).optional().default('')
});

const twilioAvailableNumbersQuerySchema = z.object({
  country: z.string().trim().length(2).optional().default('US'),
  areaCode: z.string().trim().regex(/^\d{0,6}$/).optional().default(''),
  contains: z.string().trim().max(16).optional().default(''),
  limit: z.coerce.number().int().min(1).max(50).optional().default(20)
});

const twilioPurchaseSchema = z.object({
  phoneNumber: z.string().trim().regex(/^\+[1-9]\d{1,14}$/),
  label: z.string().trim().max(64).optional().default(''),
  setPrimary: z.boolean().optional().default(true),
  webhookBaseUrl: z.string().trim().url().max(2048).optional().default('')
});

const twilioConnectSchema = z.object({
  accountSid: z.string().trim().max(128).optional(),
  apiKeySid: z.string().trim().max(128).optional(),
  apiKeySecret: z.string().trim().max(256).optional(),
  messagingServiceSid: z.string().trim().max(128).optional(),
  phoneNumber: z.string().trim().max(32).optional(),
  voiceForwardTo: z.string().trim().max(32).optional(),
  voiceDialTimeoutSec: z.number().int().min(10).max(60).optional(),
  webhookAuthToken: z.string().trim().max(256).optional()
}).refine((value) => Object.keys(value).length > 0, {
  message: 'At least one field must be provided'
});

const createAccountSchema = z.object({
  to: z.string().trim().regex(/^\+[1-9]\d{1,14}$/),
  businessName: z.string().trim().max(120).optional().default(''),
  nicheTemplate: z.enum(NICHE_TEMPLATE_KEYS).optional().default('detailer'),
  activateSubscription: z.boolean().optional().default(false)
});

const createUserSchema = z.object({
  email: z.string().trim().email().max(254),
  password: z.string().min(8).max(200),
  role: z.string().trim().min(1).max(32),
  accountIds: z.array(z.string().trim().min(1).max(80)).optional().default([])
});

const updateUserSchema = z.object({
  role: z.string().trim().min(1).max(32).optional(),
  disabled: z.boolean().optional(),
  accountIds: z.array(z.string().trim().min(1).max(80)).optional(),
  password: z.string().min(8).max(200).optional()
}).refine((value) => Object.keys(value).length > 0, {
  message: 'At least one field must be provided'
});
const noBodySchema = z.object({}).strict().optional().default({});
const messagingRangeQuerySchema = z.object({
  range: z.enum(['24h', '7d', '30d']).optional().default('7d')
});
const messagingLimitQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(100).optional().default(25)
});
const messagingConvoKeyParamSchema = z.object({
  accountId: z.string().trim().min(1).max(80),
  convoKey: z.string().trim().min(3).max(200)
});
const messagingRowIdParamSchema = z.object({
  accountId: z.string().trim().min(1).max(80),
  rowId: z.string().trim().min(8).max(64)
});
const messagingMessageIdParamSchema = z.object({
  accountId: z.string().trim().min(1).max(80),
  messageId: z.string().trim().min(1).max(200)
});

function ensureUsers(data) {
  if (!Array.isArray(data.users)) data.users = [];
  return data.users;
}

function ensureSessions(data) {
  if (!data.sessions || typeof data.sessions !== 'object' || Array.isArray(data.sessions)) data.sessions = {};
  return data.sessions;
}

function randomId() {
  const crypto = require('crypto');
  if (crypto.randomUUID) return crypto.randomUUID();
  return crypto.randomBytes(16).toString('hex');
}

function listAccounts(data) {
  return Object.entries(data.accounts || {}).map(([to, account]) => ({
    to: String(to),
    accountId: String(account?.accountId || account?.id || ''),
    businessName: String(account?.businessName || account?.workspace?.identity?.businessName || '').trim(),
    createdAt: Number(account?.createdAt || 0) || null
  })).filter((a) => a.accountId);
}

function sanitizeUserForList(user) {
  const safe = sanitizeUser(user);
  return safe ? { ...safe } : null;
}

function validateAccountIds(data, accountIds) {
  const ids = Array.isArray(accountIds) ? accountIds.map((x) => String(x || '').trim()).filter(Boolean) : [];
  const valid = [];
  for (const id of ids) {
    const found = getAccountById(data, id);
    if (!found?.account) return { ok: false, error: `Unknown accountId: ${id}` };
    valid.push(id);
  }
  return { ok: true, accountIds: [...new Set(valid)] };
}

function applyNicheTemplate(account, templateKey) {
  const key = String(templateKey || 'detailer').trim().toLowerCase();
  const preset = NICHE_TEMPLATE_PRESETS[key] || NICHE_TEMPLATE_PRESETS.detailer;
  account.workspace = account.workspace && typeof account.workspace === 'object' ? account.workspace : {};
  account.workspace.identity = account.workspace.identity && typeof account.workspace.identity === 'object'
    ? account.workspace.identity
    : {};
  account.workspace.identity.industry = String(preset.industry || key);
  account.workspace.pricing = account.workspace.pricing && typeof account.workspace.pricing === 'object'
    ? account.workspace.pricing
    : {};
  const pricingServices = JSON.parse(JSON.stringify(preset.services || {}));
  account.workspace.pricing.services = pricingServices;
  account.workspace.pricingByFlow = account.workspace.pricingByFlow && typeof account.workspace.pricingByFlow === 'object'
    ? account.workspace.pricingByFlow
    : {};
  if (preset.defaultFlowId) {
    account.workspace.pricingByFlow[preset.defaultFlowId] = {
      ...(account.workspace.pricingByFlow[preset.defaultFlowId] && typeof account.workspace.pricingByFlow[preset.defaultFlowId] === 'object'
        ? account.workspace.pricingByFlow[preset.defaultFlowId]
        : {}),
      services: JSON.parse(JSON.stringify(pricingServices))
    };
    account.defaults = account.defaults && typeof account.defaults === 'object' ? account.defaults : {};
    account.defaults.defaultFlowId = String(preset.defaultFlowId);
  }
  account.settings = account.settings && typeof account.settings === 'object' ? account.settings : {};
  account.settings.finance = account.settings.finance && typeof account.settings.finance === 'object'
    ? account.settings.finance
    : {};
  account.settings.finance.averageTicketValueCents = Number(preset.avgTicketValueCents || 0);
  account.businessProfile = account.businessProfile && typeof account.businessProfile === 'object' ? account.businessProfile : {};
  account.businessProfile.businessType = String(preset.industry || key);
  account.nicheTemplate = key;
  return key;
}

function setAccountProvisioningState(account, { activateSubscription = false } = {}) {
  account.billing = account.billing && typeof account.billing === 'object' ? account.billing : {};
  account.billing.plan = account.billing.plan && typeof account.billing.plan === 'object' ? account.billing.plan : {};
  account.billing.provider = activateSubscription ? 'stripe' : 'pending';
  account.billing.isLive = activateSubscription === true;
  account.billing.plan.key = String(account.billing.plan.key || 'pro');
  account.billing.plan.name = String(account.billing.plan.name || 'Pro');
  account.billing.plan.interval = String(account.billing.plan.interval || 'month');
  account.billing.plan.status = activateSubscription ? 'active' : 'unpaid';
  account.billing.updatedAt = Date.now();
  account.provisioning = {
    status: activateSubscription ? 'active' : 'pending_payment',
    activatedAt: activateSubscription ? Date.now() : null
  };
}

function normalizeInvoiceStatus(status) {
  const v = String(status || '').toLowerCase();
  if (['paid', 'open', 'past_due', 'refunded'].includes(v)) return v;
  return 'open';
}

function maskSecret(value, { left = 6, right = 4 } = {}) {
  const v = String(value || '').trim();
  if (!v) return '';
  if (v.length <= left + right) return `${v.slice(0, 2)}***`;
  return `${v.slice(0, left)}...${v.slice(-right)}`;
}

function ensurePlatformStripeConfig(data) {
  data.dev = data.dev && typeof data.dev === 'object' ? data.dev : {};
  const current = data.dev.platformBillingStripe && typeof data.dev.platformBillingStripe === 'object'
    ? data.dev.platformBillingStripe
    : {};
  data.dev.platformBillingStripe = {
    enabled: current.enabled === true,
    secretKey: String(current.secretKey || '').trim(),
    publishableKey: String(current.publishableKey || '').trim(),
    webhookSecret: String(current.webhookSecret || '').trim(),
    accountId: String(current.accountId || '').trim(),
    accountEmail: String(current.accountEmail || '').trim(),
    accountDisplayName: String(current.accountDisplayName || '').trim(),
    connectedAt: current.connectedAt ? Number(current.connectedAt) : null,
    lastTestedAt: current.lastTestedAt ? Number(current.lastTestedAt) : null,
    lastStatus: current.lastStatus ? String(current.lastStatus) : null,
    lastError: current.lastError ? String(current.lastError) : null
  };
  return data.dev.platformBillingStripe;
}

function stripeAuthHeader(secretKey) {
  const token = Buffer.from(`${String(secretKey || '').trim()}:`, 'utf8').toString('base64');
  return `Basic ${token}`;
}

async function stripeRequest(secretKey, path, { method = 'GET', form = null } = {}) {
  const headers = {
    Authorization: stripeAuthHeader(secretKey),
    Accept: 'application/json'
  };
  let body;
  if (form && typeof form === 'object') {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(form)) {
      if (value == null) continue;
      params.set(String(key), String(value));
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
    throw new Error(detail || `Stripe request failed (${res.status})`);
  }
  return parsed;
}

async function testStripeCredentials(secretKey) {
  const account = await stripeRequest(secretKey, '/v1/account');
  return {
    accountId: String(account?.id || '').trim(),
    accountEmail: String(account?.email || '').trim(),
    accountDisplayName: String(account?.business_profile?.name || account?.settings?.dashboard?.display_name || '').trim()
  };
}

function platformStripeSnapshot(cfg) {
  const current = cfg && typeof cfg === 'object' ? cfg : {};
  const base = String(APP_PUBLIC_BASE_URL || '').replace(/\/$/, '');
  return {
    enabled: current.enabled === true,
    accountId: String(current.accountId || ''),
    accountEmail: String(current.accountEmail || ''),
    accountDisplayName: String(current.accountDisplayName || ''),
    publishableKey: String(current.publishableKey || ''),
    webhookSecretMasked: current.webhookSecret ? maskSecret(current.webhookSecret) : '',
    webhookUrl: base ? `${base}/webhooks/stripe/platform` : '/webhooks/stripe/platform',
    secretKeyMasked: current.secretKey ? maskSecret(current.secretKey) : '',
    connectedAt: current.connectedAt ? Number(current.connectedAt) : null,
    lastTestedAt: current.lastTestedAt ? Number(current.lastTestedAt) : null,
    lastStatus: current.lastStatus ? String(current.lastStatus) : null,
    lastError: current.lastError ? String(current.lastError) : null
  };
}

function collectAccountNumbers(account, to) {
  const out = [];
  if (to) out.push(String(to));
  const workspaceNumbers = Array.isArray(account?.workspace?.phoneNumbers) ? account.workspace.phoneNumbers : [];
  for (const row of workspaceNumbers) {
    const num = String(row?.number || '').trim();
    if (num) out.push(num);
  }
  return [...new Set(out)];
}

function buildDeveloperOpsOverview(data) {
  const users = Array.isArray(data?.users) ? data.users : [];
  const workspaces = [];
  const twilioSidCount = new Map();

  for (const [to, account] of Object.entries(data.accounts || {})) {
    if (!account || typeof account !== 'object') continue;
    const accountId = String(account.accountId || account.id || '').trim();
    if (!accountId) continue;
    const businessName = String(account.businessName || account?.workspace?.identity?.businessName || '').trim() || 'Workspace';
    const numbers = collectAccountNumbers(account, to);
    const twilioRaw = account?.integrations?.twilio && typeof account.integrations.twilio === 'object'
      ? account.integrations.twilio
      : {};
    const twilioAccountSid = String(twilioRaw.accountSid || '').trim();
    if (twilioRaw.enabled === true && twilioAccountSid) {
      twilioSidCount.set(twilioAccountSid, Number(twilioSidCount.get(twilioAccountSid) || 0) + 1);
    }
    const assignedUsers = users
      .filter((u) => String(u?.role || '').toLowerCase() !== 'superadmin' && Array.isArray(u?.accountIds) && u.accountIds.includes(accountId))
      .map((u) => ({
        id: String(u?.id || ''),
        email: String(u?.email || ''),
        role: String(u?.role || 'agent')
      }));
    const billing = account?.billing && typeof account.billing === 'object' ? account.billing : {};
    const billingPlan = billing?.plan && typeof billing.plan === 'object' ? billing.plan : {};
    workspaces.push({
      to: String(to),
      accountId,
      businessName,
      numbers,
      assignedUsers,
      twilio: {
        enabled: twilioRaw.enabled === true,
        accountSid: twilioAccountSid,
        accountSidMasked: maskSecret(twilioAccountSid),
        apiKeySid: String(twilioRaw.apiKeySid || ''),
        hasApiKeySecret: Boolean(String(twilioRaw.apiKeySecret || '').trim()),
        hasWebhookAuthToken: Boolean(String(twilioRaw.webhookAuthToken || '').trim()),
        messagingServiceSid: String(twilioRaw.messagingServiceSid || ''),
        phoneNumber: String(twilioRaw.phoneNumber || ''),
        voiceForwardTo: String(twilioRaw.voiceForwardTo || ''),
        voiceDialTimeoutSec: Number(twilioRaw.voiceDialTimeoutSec || 20) || 20,
        lastStatus: twilioRaw.lastStatus ? String(twilioRaw.lastStatus) : null,
        lastTestedAt: twilioRaw.lastTestedAt ? Number(twilioRaw.lastTestedAt) : null
      },
      billing: {
        provider: String(billing.provider || 'demo'),
        isLive: billing.isLive === true,
        planName: String(billingPlan.name || billingPlan.key || 'Pro'),
        planStatus: String(billingPlan.status || 'active'),
        priceMonthly: Number(billingPlan.priceMonthly || 0),
        nextBillingAt: billingPlan.nextBillingAt ? Number(billingPlan.nextBillingAt) : null,
        billingEmail: String(billing?.details?.billingEmail || '')
      }
    });
  }

  workspaces.sort((a, b) => String(a.businessName).localeCompare(String(b.businessName)));
  let mainTwilioAccountSid = '';
  let mainTwilioWorkspaceCount = 0;
  for (const [sid, count] of twilioSidCount.entries()) {
    if (count > mainTwilioWorkspaceCount) {
      mainTwilioAccountSid = sid;
      mainTwilioWorkspaceCount = count;
    }
  }

  return {
    asOf: Date.now(),
    summary: {
      workspaceCount: workspaces.length,
      twilioConnectedCount: workspaces.filter((w) => w.twilio.enabled).length,
      mainTwilioAccountSid: mainTwilioAccountSid ? maskSecret(mainTwilioAccountSid) : '',
      mainTwilioWorkspaceCount
    },
    workspaces
  };
}

function normalizePhone(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const hasPlus = raw.startsWith('+');
  const digits = raw.replace(/[^\d]/g, '');
  return hasPlus ? `+${digits}` : digits;
}

async function listTwilioIncomingNumbers({ accountSid, apiKeySid, apiKeySecret }) {
  const auth = Buffer.from(`${apiKeySid}:${apiKeySecret}`, 'utf8').toString('base64');
  const all = [];
  let nextPath = `/2010-04-01/Accounts/${encodeURIComponent(accountSid)}/IncomingPhoneNumbers.json?PageSize=100`;
  let safety = 0;

  while (nextPath && safety < 20) {
    safety += 1;
    const url = `https://api.twilio.com${nextPath}`;
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        Authorization: `Basic ${auth}`,
        Accept: 'application/json'
      }
    });
    const raw = await res.text();
    let parsed = {};
    try { parsed = JSON.parse(raw); } catch {}
    if (!res.ok) {
      const detail = String(parsed?.message || parsed?.detail || '').trim();
      throw new Error(detail || `Twilio number inventory failed (${res.status})`);
    }

    const rows = Array.isArray(parsed?.incoming_phone_numbers) ? parsed.incoming_phone_numbers : [];
    for (const row of rows) {
      all.push({
        sid: String(row?.sid || ''),
        phoneNumber: String(row?.phone_number || ''),
        friendlyName: String(row?.friendly_name || ''),
        capabilities: row?.capabilities && typeof row.capabilities === 'object' ? row.capabilities : {}
      });
    }
    nextPath = parsed?.next_page_uri ? String(parsed.next_page_uri) : '';
  }
  return all;
}

function getTwilioCredentialsForAccount(data, accountId) {
  const found = getAccountById(data, accountId);
  if (!found?.account) return { ok: false, status: 404, error: 'Account not found' };
  const account = found.account;
  const tw = account?.integrations?.twilio && typeof account.integrations.twilio === 'object'
    ? account.integrations.twilio
    : {};
  const enabled = tw.enabled === true;
  const accountSid = String(tw.accountSid || '').trim();
  const apiKeySid = String(tw.apiKeySid || '').trim();
  const apiKeySecret = String(tw.apiKeySecret || '').trim();
  if (!enabled || !accountSid || !apiKeySid || !apiKeySecret) {
    const platformTwilio = data?.dev?.platformTwilio && typeof data.dev.platformTwilio === 'object'
      ? data.dev.platformTwilio
      : {};
    const platformEnabled = platformTwilio.enabled === true;
    const platformAccountSid = String(platformTwilio.accountSid || '').trim();
    const platformApiKeySid = String(platformTwilio.apiKeySid || '').trim();
    const platformApiKeySecret = String(platformTwilio.apiKeySecret || '').trim();
    if (platformEnabled && platformAccountSid && platformApiKeySid && platformApiKeySecret) {
      return {
        ok: true,
        found,
        source: 'platform',
        creds: {
          accountSid: platformAccountSid,
          apiKeySid: platformApiKeySid,
          apiKeySecret: platformApiKeySecret
        }
      };
    }
    return { ok: false, status: 400, error: 'Twilio is not connected for this workspace or the platform account' };
  }
  return {
    ok: true,
    found,
    source: 'workspace',
    creds: { accountSid, apiKeySid, apiKeySecret }
  };
}

function ensureMessagingAccount(data, accountId) {
  const found = getAccountById(data, accountId);
  if (!found?.account) return null;
  return found;
}

async function twilioRequestJson({ accountSid, apiKeySid, apiKeySecret, method = 'GET', path, form }) {
  const auth = Buffer.from(`${apiKeySid}:${apiKeySecret}`, 'utf8').toString('base64');
  const url = `https://api.twilio.com${path}`;
  const headers = {
    Authorization: `Basic ${auth}`,
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
  const res = await fetch(url, { method, headers, body });
  const raw = await res.text();
  let parsed = {};
  try { parsed = JSON.parse(raw); } catch {}
  if (!res.ok) {
    const detail = String(parsed?.message || parsed?.detail || '').trim();
    const code = String(parsed?.code || '').trim();
    const extra = code ? ` (Twilio code ${code})` : '';
    throw new Error((detail || `Twilio request failed (${res.status})`) + extra);
  }
  return parsed;
}

async function searchTwilioAvailableNumbers({ accountSid, apiKeySid, apiKeySecret, country = 'US', areaCode = '', contains = '', limit = 20 }) {
  const qs = new URLSearchParams();
  qs.set('SmsEnabled', 'true');
  qs.set('VoiceEnabled', 'true');
  qs.set('PageSize', String(Math.max(1, Math.min(50, Number(limit || 20)))));
  if (String(areaCode || '').trim()) qs.set('AreaCode', String(areaCode).trim());
  if (String(contains || '').trim()) qs.set('Contains', String(contains).trim());
  const path = `/2010-04-01/Accounts/${encodeURIComponent(accountSid)}/AvailablePhoneNumbers/${encodeURIComponent(String(country || 'US').toUpperCase())}/Local.json?${qs.toString()}`;
  const parsed = await twilioRequestJson({ accountSid, apiKeySid, apiKeySecret, method: 'GET', path });
  const rows = Array.isArray(parsed?.available_phone_numbers) ? parsed.available_phone_numbers : [];
  return rows.map((row) => ({
    phoneNumber: String(row?.phone_number || ''),
    friendlyName: String(row?.friendly_name || ''),
    locality: String(row?.locality || ''),
    region: String(row?.region || ''),
    postalCode: String(row?.postal_code || ''),
    capabilities: row?.capabilities && typeof row.capabilities === 'object' ? row.capabilities : {}
  }));
}

async function purchaseTwilioNumber({ accountSid, apiKeySid, apiKeySecret, phoneNumber, friendlyName = '', voiceWebhookUrl = '' }) {
  const form = {
    PhoneNumber: String(phoneNumber || '').trim()
  };
  if (friendlyName) form.FriendlyName = String(friendlyName).trim();
  if (voiceWebhookUrl) {
    form.VoiceUrl = String(voiceWebhookUrl).trim();
    form.VoiceMethod = 'POST';
  }
  const path = `/2010-04-01/Accounts/${encodeURIComponent(accountSid)}/IncomingPhoneNumbers.json`;
  const parsed = await twilioRequestJson({ accountSid, apiKeySid, apiKeySecret, method: 'POST', path, form });
  return {
    sid: String(parsed?.sid || ''),
    phoneNumber: String(parsed?.phone_number || ''),
    friendlyName: String(parsed?.friendly_name || ''),
    voiceUrl: String(parsed?.voice_url || '')
  };
}

adminRouter.get('/accounts', (_req, res) => {
  const data = loadData();
  return res.json({ ok: true, accounts: listAccounts(data) });
});

adminRouter.get('/billing/overview', (_req, res) => {
  const data = loadData();
  const now = Date.now();
  const THIRTY_DAYS = 1000 * 60 * 60 * 24 * 30;
  let workspaceCount = 0;
  let stripeConnectedCount = 0;
  let monthlyRunRateCents = 0;
  let paidRevenueCents = 0;
  let outstandingCents = 0;
  let recentPaymentsCents = 0;
  let recentPaymentsCount = 0;
  let invoiceCount = 0;
  const topWorkspaces = [];

  for (const [to, account] of Object.entries(data.accounts || {})) {
    if (!account || typeof account !== 'object') continue;
    workspaceCount += 1;

    const billing = account.billing && typeof account.billing === 'object' ? account.billing : {};
    const provider = String(billing.provider || 'demo').toLowerCase();
    const isLive = billing.isLive === true;
    const plan = billing.plan && typeof billing.plan === 'object' ? billing.plan : {};
    const planStatus = String(plan.status || '').toLowerCase();
    const planMonthly = Number(plan.priceMonthly || 0);
    if (provider === 'stripe' && isLive) stripeConnectedCount += 1;
    if (['active', 'trialing', 'past_due'].includes(planStatus) && Number.isFinite(planMonthly)) {
      monthlyRunRateCents += Math.max(0, Math.round(planMonthly * 100));
    }

    const invoices = Array.isArray(billing.invoices) ? billing.invoices : [];
    let workspacePaid = 0;
    for (const inv of invoices) {
      const status = normalizeInvoiceStatus(inv?.status);
      const amount = Number(inv?.amount || 0);
      const date = Number(inv?.date || 0);
      if (!Number.isFinite(amount)) continue;
      invoiceCount += 1;
      if (status === 'paid') {
        paidRevenueCents += amount;
        workspacePaid += amount;
        if (date && (now - date) <= THIRTY_DAYS) {
          recentPaymentsCents += amount;
          recentPaymentsCount += 1;
        }
      } else if (status === 'open' || status === 'past_due') {
        outstandingCents += amount;
      }
    }

    topWorkspaces.push({
      accountId: String(account.accountId || account.id || ''),
      to: String(to),
      businessName: String(account.businessName || account?.workspace?.identity?.businessName || '').trim() || 'Workspace',
      paidRevenueCents: workspacePaid
    });
  }

  topWorkspaces.sort((a, b) => Number(b.paidRevenueCents || 0) - Number(a.paidRevenueCents || 0));

  return res.json({
    ok: true,
    asOf: now,
    summary: {
      workspaceCount,
      stripeConnectedCount,
      monthlyRunRateCents,
      paidRevenueCents,
      outstandingCents,
      recentPaymentsCents,
      recentPaymentsCount,
      invoiceCount
    },
    topWorkspaces: topWorkspaces.slice(0, 8)
  });
});

adminRouter.get('/developer/ops-overview', (_req, res) => {
  const data = loadData();
  return res.json({ ok: true, ...buildDeveloperOpsOverview(data) });
});

adminRouter.get('/developer/platform-billing/stripe', (_req, res) => {
  const data = loadData();
  const cfg = ensurePlatformStripeConfig(data);
  saveDataDebounced(data);
  return res.json({ ok: true, stripe: platformStripeSnapshot(cfg) });
});

adminRouter.put('/developer/platform-billing/stripe', validateBody(platformStripeConnectSchema), async (req, res) => {
  const data = loadData();
  const cfg = ensurePlatformStripeConfig(data);
  const secretKey = String(req.body?.secretKey || '').trim() || String(cfg.secretKey || '').trim();
  const publishableKey = String(req.body?.publishableKey || '').trim();
  const webhookSecret = String(req.body?.webhookSecret || '').trim() || String(cfg.webhookSecret || '').trim();
  if (!secretKey) return res.status(400).json({ error: 'secretKey is required' });

  try {
    const tested = await testStripeCredentials(secretKey);
    data.dev.platformBillingStripe = {
      ...cfg,
      enabled: true,
      secretKey,
      publishableKey,
      webhookSecret,
      accountId: tested.accountId || cfg.accountId || '',
      accountEmail: tested.accountEmail || cfg.accountEmail || '',
      accountDisplayName: tested.accountDisplayName || cfg.accountDisplayName || '',
      connectedAt: cfg.connectedAt || Date.now(),
      lastTestedAt: Date.now(),
      lastStatus: 'ok',
      lastError: null
    };
    saveDataDebounced(data);
    return res.json({ ok: true, stripe: platformStripeSnapshot(data.dev.platformBillingStripe) });
  } catch (err) {
    data.dev.platformBillingStripe = {
      ...cfg,
      enabled: false,
      secretKey,
      publishableKey,
      webhookSecret,
      lastTestedAt: Date.now(),
      lastStatus: 'error',
      lastError: String(err?.message || 'Stripe authentication failed')
    };
    saveDataDebounced(data);
    return res.status(400).json({ error: err?.message || 'Failed to connect platform Stripe' });
  }
});

adminRouter.post('/developer/platform-billing/stripe/test', validateBody(noBodySchema), async (_req, res) => {
  const data = loadData();
  const cfg = ensurePlatformStripeConfig(data);
  const secretKey = String(cfg.secretKey || '').trim();
  if (!secretKey) return res.status(400).json({ error: 'Platform Stripe secret key is not configured' });
  try {
    const tested = await testStripeCredentials(secretKey);
    data.dev.platformBillingStripe = {
      ...cfg,
      enabled: true,
      accountId: tested.accountId || cfg.accountId || '',
      accountEmail: tested.accountEmail || cfg.accountEmail || '',
      accountDisplayName: tested.accountDisplayName || cfg.accountDisplayName || '',
      lastTestedAt: Date.now(),
      lastStatus: 'ok',
      lastError: null
    };
    saveDataDebounced(data);
    return res.json({ ok: true, accountId: tested.accountId || '', stripe: platformStripeSnapshot(data.dev.platformBillingStripe) });
  } catch (err) {
    data.dev.platformBillingStripe = {
      ...cfg,
      enabled: false,
      lastTestedAt: Date.now(),
      lastStatus: 'error',
      lastError: String(err?.message || 'Stripe test failed')
    };
    saveDataDebounced(data);
    return res.status(400).json({ error: err?.message || 'Stripe test failed' });
  }
});

adminRouter.delete('/developer/platform-billing/stripe', validateBody(noBodySchema), (_req, res) => {
  const data = loadData();
  const cfg = ensurePlatformStripeConfig(data);
  data.dev.platformBillingStripe = {
    ...cfg,
    enabled: false,
    secretKey: '',
    publishableKey: '',
    webhookSecret: '',
    accountId: '',
    accountEmail: '',
    accountDisplayName: '',
    connectedAt: null,
    lastTestedAt: Date.now(),
    lastStatus: 'disconnected',
    lastError: null
  };
  saveDataDebounced(data);
  return res.json({ ok: true, stripe: platformStripeSnapshot(data.dev.platformBillingStripe) });
});

adminRouter.get('/developer/twilio-number-inventory', async (_req, res) => {
  const data = loadData();
  const overview = buildDeveloperOpsOverview(data);
  const workspaces = Array.isArray(overview?.workspaces) ? overview.workspaces : [];

  const workspaceById = new Map();
  const normalizedWorkspaceNumbers = new Map();
  const credentialGroups = new Map();

  for (const ws of workspaces) {
    const accountId = String(ws?.accountId || '').trim();
    if (!accountId) continue;
    workspaceById.set(accountId, ws);
    const nums = Array.isArray(ws?.numbers) ? ws.numbers : [];
    const normalized = [...new Set(nums.map(normalizePhone).filter(Boolean))];
    normalizedWorkspaceNumbers.set(accountId, normalized);
  }

  for (const [to, account] of Object.entries(data.accounts || {})) {
    if (!account || typeof account !== 'object') continue;
    const accountId = String(account?.accountId || account?.id || '').trim();
    if (!accountId) continue;
    const tw = account?.integrations?.twilio && typeof account.integrations.twilio === 'object'
      ? account.integrations.twilio
      : {};
    const enabled = tw.enabled === true;
    const accountSid = String(tw.accountSid || '').trim();
    const apiKeySid = String(tw.apiKeySid || '').trim();
    const apiKeySecret = String(tw.apiKeySecret || '').trim();
    if (!enabled || !accountSid || !apiKeySid || !apiKeySecret) continue;
    const key = `${accountSid}||${apiKeySid}||${apiKeySecret}`;
    if (!credentialGroups.has(key)) {
      credentialGroups.set(key, { accountSid, apiKeySid, apiKeySecret, workspaceAccountIds: [] });
    }
    credentialGroups.get(key).workspaceAccountIds.push(accountId);
  }

  const inventoryMap = new Map();
  const errors = [];

  for (const group of credentialGroups.values()) {
    try {
      const numbers = await listTwilioIncomingNumbers(group);
      for (const row of numbers) {
        const normalized = normalizePhone(row.phoneNumber);
        if (!normalized) continue;
        if (!inventoryMap.has(normalized)) {
          inventoryMap.set(normalized, {
            phoneNumber: normalized,
            sid: String(row.sid || ''),
            friendlyName: String(row.friendlyName || ''),
            twilioAccountSidMasked: maskSecret(group.accountSid),
            capabilities: row.capabilities || {},
            discoveredViaWorkspaces: [],
            assignedWorkspaces: []
          });
        }
        const slot = inventoryMap.get(normalized);
        for (const wsId of group.workspaceAccountIds) {
          if (!slot.discoveredViaWorkspaces.includes(wsId)) slot.discoveredViaWorkspaces.push(wsId);
        }
      }
    } catch (err) {
      errors.push({
        twilioAccountSidMasked: maskSecret(group.accountSid),
        workspaceCount: group.workspaceAccountIds.length,
        error: err?.message || 'Failed to read Twilio numbers'
      });
    }
  }

  for (const item of inventoryMap.values()) {
    for (const [accountId, numbers] of normalizedWorkspaceNumbers.entries()) {
      if (!numbers.includes(item.phoneNumber)) continue;
      const ws = workspaceById.get(accountId) || {};
      item.assignedWorkspaces.push({
        accountId,
        to: String(ws?.to || ''),
        businessName: String(ws?.businessName || 'Workspace')
      });
    }
    item.discoveredViaWorkspaces = item.discoveredViaWorkspaces.map((accountId) => {
      const ws = workspaceById.get(accountId) || {};
      return {
        accountId,
        to: String(ws?.to || ''),
        businessName: String(ws?.businessName || 'Workspace')
      };
    });
    item.assignedWorkspaces.sort((a, b) => String(a.businessName).localeCompare(String(b.businessName)));
    item.discoveredViaWorkspaces.sort((a, b) => String(a.businessName).localeCompare(String(b.businessName)));
  }

  const numbers = Array.from(inventoryMap.values())
    .sort((a, b) => String(a.phoneNumber).localeCompare(String(b.phoneNumber)));

  return res.json({
    ok: true,
    asOf: Date.now(),
    summary: {
      twilioNumberCount: numbers.length,
      assignedCount: numbers.filter((n) => Array.isArray(n.assignedWorkspaces) && n.assignedWorkspaces.length > 0).length,
      unassignedCount: numbers.filter((n) => !Array.isArray(n.assignedWorkspaces) || n.assignedWorkspaces.length === 0).length,
      credentialSetCount: credentialGroups.size
    },
    numbers,
    errors
  });
});

adminRouter.get('/developer/messaging/:accountId/analytics', validateParams(accountIdParamSchema), validateQuery(messagingRangeQuerySchema), async (req, res) => {
  const accountId = String(req.params?.accountId || '').trim();
  const data = loadData();
  if (!ensureMessagingAccount(data, accountId)) return res.status(404).json({ error: 'Account not found' });
  const summary = await getMessagingAnalyticsSummary(accountId, { range: req.query?.range || '7d' });
  return res.json({ ok: true, accountId, analytics: summary });
});

adminRouter.get('/developer/messaging/:accountId/failures', validateParams(accountIdParamSchema), validateQuery(messagingLimitQuerySchema), async (req, res) => {
  const accountId = String(req.params?.accountId || '').trim();
  const data = loadData();
  if (!ensureMessagingAccount(data, accountId)) return res.status(404).json({ error: 'Account not found' });
  const failures = await getRecentFailedMessages(accountId, { limit: req.query?.limit || 25 });
  return res.json({ ok: true, accountId, failures });
});

adminRouter.get('/developer/messaging/:accountId/conversations/by-key/:convoKey', validateParams(messagingConvoKeyParamSchema), async (req, res) => {
  const accountId = String(req.params?.accountId || '').trim();
  const data = loadData();
  if (!ensureMessagingAccount(data, accountId)) return res.status(404).json({ error: 'Account not found' });
  const conversation = await getConversationByConvoKey(accountId, req.params?.convoKey);
  if (!conversation) return res.status(404).json({ error: 'Conversation not found' });
  return res.json({ ok: true, accountId, conversation });
});

adminRouter.get('/developer/messaging/:accountId/conversations/by-row/:rowId', validateParams(messagingRowIdParamSchema), async (req, res) => {
  const accountId = String(req.params?.accountId || '').trim();
  const data = loadData();
  if (!ensureMessagingAccount(data, accountId)) return res.status(404).json({ error: 'Account not found' });
  const conversation = await getConversationByRowId(accountId, req.params?.rowId);
  if (!conversation) return res.status(404).json({ error: 'Conversation not found' });
  return res.json({ ok: true, accountId, conversation });
});

adminRouter.get('/developer/messaging/:accountId/messages/:messageId', validateParams(messagingMessageIdParamSchema), async (req, res) => {
  const accountId = String(req.params?.accountId || '').trim();
  const data = loadData();
  if (!ensureMessagingAccount(data, accountId)) return res.status(404).json({ error: 'Account not found' });
  const message = await getMessageById(accountId, req.params?.messageId);
  if (!message) return res.status(404).json({ error: 'Message not found' });
  return res.json({ ok: true, accountId, message });
});

adminRouter.get('/developer/twilio/:accountId/available-numbers', validateParams(accountIdParamSchema), validateQuery(twilioAvailableNumbersQuerySchema), async (req, res) => {
  const accountId = String(req.params?.accountId || '').trim();
  if (!accountId) return res.status(400).json({ error: 'accountId is required' });
  const data = loadData();
  const resolved = getTwilioCredentialsForAccount(data, accountId);
  if (!resolved.ok) return res.status(resolved.status || 400).json({ error: resolved.error });

  const country = String(req.query?.country || 'US').trim() || 'US';
  const areaCode = String(req.query?.areaCode || '').trim();
  const contains = String(req.query?.contains || '').trim();
  const limit = Number(req.query?.limit || 20);

  try {
    const numbers = await searchTwilioAvailableNumbers({
      ...resolved.creds,
      country,
      areaCode,
      contains,
      limit
    });
    return res.json({
      ok: true,
      accountId,
      country: String(country).toUpperCase(),
      areaCode,
      contains,
      total: numbers.length,
      numbers
    });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to search available Twilio numbers' });
  }
});

adminRouter.post('/developer/twilio/:accountId/purchase-number', validateParams(accountIdParamSchema), validateBody(twilioPurchaseSchema), async (req, res) => {
  const accountId = String(req.params?.accountId || '').trim();
  if (!accountId) return res.status(400).json({ error: 'accountId is required' });
  const data = loadData();
  const resolved = getTwilioCredentialsForAccount(data, accountId);
  if (!resolved.ok) return res.status(resolved.status || 400).json({ error: resolved.error });

  const phoneNumber = String(req.body?.phoneNumber || '').trim();
  const label = String(req.body?.label || '').trim();
  const setPrimary = req.body?.setPrimary !== false;
  const webhookBaseUrl = String(req.body?.webhookBaseUrl || '').trim().replace(/\/+$/g, '');
  if (!phoneNumber) return res.status(400).json({ error: 'phoneNumber is required' });

  let voiceWebhookUrl = '';
  if (webhookBaseUrl) {
    try {
      const u = new URL(webhookBaseUrl);
      if (!/^https?:$/i.test(u.protocol)) throw new Error('invalid protocol');
      voiceWebhookUrl = `${webhookBaseUrl}/webhooks/voice/incoming`;
    } catch {
      return res.status(400).json({ error: 'webhookBaseUrl must be a valid absolute URL' });
    }
  }

  try {
    const purchased = await purchaseTwilioNumber({
      ...resolved.creds,
      phoneNumber,
      friendlyName: label,
      voiceWebhookUrl
    });

    const account = resolved.found.account;
    account.workspace = account.workspace && typeof account.workspace === 'object' ? account.workspace : {};
    account.workspace.phoneNumbers = Array.isArray(account.workspace.phoneNumbers) ? account.workspace.phoneNumbers : [];

    const normalizedPurchased = normalizePhone(purchased.phoneNumber || phoneNumber);
    const existing = account.workspace.phoneNumbers.find((n) => normalizePhone(n?.number) === normalizedPurchased);
    if (existing) {
      if (label) existing.label = label;
      if (setPrimary) existing.isPrimary = true;
    } else {
      account.workspace.phoneNumbers.push({
        number: purchased.phoneNumber || phoneNumber,
        label: label || 'Twilio',
        isPrimary: setPrimary
      });
    }
    if (setPrimary) {
      for (const row of account.workspace.phoneNumbers) {
        if (normalizePhone(row?.number) !== normalizedPurchased) row.isPrimary = false;
      }
    } else if (!account.workspace.phoneNumbers.some((x) => x?.isPrimary === true) && account.workspace.phoneNumbers.length) {
      account.workspace.phoneNumbers[0].isPrimary = true;
    }

    account.integrations = account.integrations && typeof account.integrations === 'object' ? account.integrations : {};
    account.integrations.twilio = account.integrations.twilio && typeof account.integrations.twilio === 'object'
      ? account.integrations.twilio
      : {};
    if (!String(account.integrations.twilio.phoneNumber || '').trim() || setPrimary) {
      account.integrations.twilio.phoneNumber = purchased.phoneNumber || phoneNumber;
    }

    saveDataDebounced(data);

    return res.json({
      ok: true,
      accountId,
      to: String(resolved.found.to || ''),
      purchased,
      workspacePhoneNumbers: account.workspace.phoneNumbers
    });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to purchase Twilio number' });
  }
});

adminRouter.put('/developer/twilio/:accountId', validateParams(accountIdParamSchema), validateBody(twilioConnectSchema), async (req, res) => {
  const accountId = String(req.params?.accountId || '').trim();
  if (!accountId) return res.status(400).json({ error: 'accountId is required' });
  const data = loadData();
  const found = getAccountById(data, accountId);
  if (!found?.account) return res.status(404).json({ error: 'Account not found' });
  try {
    const tenant = { to: String(found.to || ''), accountId };
    const result = await connectTwilioForTenant(tenant, req.body || {});
    return res.json({
      ok: true,
      accountId,
      to: String(found.to || ''),
      twilio: result?.twilio || getTenantTwilioSnapshot(tenant)?.twilio || {}
    });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to save Twilio config' });
  }
});

adminRouter.post('/developer/twilio/:accountId/test', validateParams(accountIdParamSchema), validateBody(noBodySchema), async (req, res) => {
  const accountId = String(req.params?.accountId || '').trim();
  if (!accountId) return res.status(400).json({ error: 'accountId is required' });
  const data = loadData();
  const found = getAccountById(data, accountId);
  if (!found?.account) return res.status(404).json({ error: 'Account not found' });
  try {
    const tenant = { to: String(found.to || ''), accountId };
    const result = await testTwilioForTenant(tenant);
    return res.json({ ok: true, accountId, to: String(found.to || ''), ...result });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Twilio test failed' });
  }
});

adminRouter.delete('/developer/twilio/:accountId', validateParams(accountIdParamSchema), validateBody(noBodySchema), (req, res) => {
  const accountId = String(req.params?.accountId || '').trim();
  if (!accountId) return res.status(400).json({ error: 'accountId is required' });
  const data = loadData();
  const found = getAccountById(data, accountId);
  if (!found?.account) return res.status(404).json({ error: 'Account not found' });
  try {
    const tenant = { to: String(found.to || ''), accountId };
    const result = disconnectTwilioForTenant(tenant);
    return res.json({ ok: true, accountId, to: String(found.to || ''), ...result });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to disconnect Twilio' });
  }
});

adminRouter.post('/accounts', validateBody(createAccountSchema), (req, res) => {
  const to = String(req?.body?.to || '').trim();
  const businessName = String(req?.body?.businessName || '').trim();
  const nicheTemplate = String(req?.body?.nicheTemplate || 'home_services').trim().toLowerCase();
  const activateSubscription = req?.body?.activateSubscription === true;
  if (!to) return res.status(400).json({ error: 'to is required' });

  const data = loadData();
  const account = ensureAccountForTo(data, to, { autoCreate: true });
  if (!account) return res.status(400).json({ error: 'Failed to create account' });
  if (businessName) {
    account.businessName = businessName;
    account.workspace = account.workspace || {};
    account.workspace.identity = account.workspace.identity || {};
    account.workspace.identity.businessName = businessName;
  }
  if (!account.createdAt) account.createdAt = Date.now();
  const appliedTemplate = applyNicheTemplate(account, nicheTemplate);
  setAccountProvisioningState(account, { activateSubscription });
  saveDataDebounced(data);
  return res.json({
    ok: true,
    account: {
      to: String(to),
      accountId: String(account.accountId || account.id || ''),
      businessName: String(account.businessName || ''),
      createdAt: Number(account.createdAt || 0) || null,
      nicheTemplate: appliedTemplate,
      provisioningStatus: String(account?.provisioning?.status || 'pending_payment'),
      subscriptionStatus: String(account?.billing?.plan?.status || 'unpaid')
    }
  });
});

adminRouter.get('/users', (_req, res) => {
  const data = loadData();
  const users = ensureUsers(data).map(sanitizeUserForList).filter(Boolean);
  return res.json({ ok: true, users });
});

adminRouter.post('/users', validateBody(createUserSchema), (req, res) => {
  const email = normalizeEmail(req?.body?.email);
  const password = String(req?.body?.password || '');
  const role = normalizeRole(req?.body?.role);
  const data = loadData();
  const users = ensureUsers(data);
  ensureSessions(data);

  if (!email) return res.status(400).json({ error: 'email is required' });
  if (!password || password.length < 8) return res.status(400).json({ error: 'password must be at least 8 characters' });
  if (!role || !ROLE_SET.has(role)) return res.status(400).json({ error: 'invalid role' });
  if (users.some((u) => normalizeEmail(u?.email) === email)) {
    return res.status(409).json({ error: 'email already exists' });
  }

  const validated = validateAccountIds(data, req?.body?.accountIds);
  if (!validated.ok) return res.status(400).json({ error: validated.error });

  const user = {
    id: randomId(),
    email,
    passwordHash: hashPassword(password),
    role,
    accountIds: role === 'superadmin' ? [] : validated.accountIds,
    createdAt: Date.now(),
    lastLoginAt: null,
    disabled: false
  };
  users.push(user);
  saveDataDebounced(data);
  return res.json({ ok: true, user: sanitizeUserForList(user) });
});

adminRouter.put('/users/:id', validateParams(userIdParamSchema), validateBody(updateUserSchema), (req, res) => {
  const userId = String(req.params.id || '').trim();
  if (!userId) return res.status(400).json({ error: 'user id is required' });

  const data = loadData();
  const users = ensureUsers(data);
  ensureSessions(data);
  const user = users.find((u) => String(u.id || '') === userId);
  if (!user) return res.status(404).json({ error: 'User not found' });

  if (Object.prototype.hasOwnProperty.call(req.body || {}, 'role')) {
    const nextRole = normalizeRole(req.body.role);
    if (!nextRole || !ROLE_SET.has(nextRole)) return res.status(400).json({ error: 'invalid role' });
    user.role = nextRole;
    if (nextRole === 'superadmin') user.accountIds = [];
  }

  if (Object.prototype.hasOwnProperty.call(req.body || {}, 'disabled')) {
    user.disabled = req.body.disabled === true;
    if (user.disabled) {
      for (const [sid, session] of Object.entries(data.sessions || {})) {
        if (String(session?.userId || '') === userId) delete data.sessions[sid];
      }
    }
  }

  if (Object.prototype.hasOwnProperty.call(req.body || {}, 'accountIds')) {
    const validated = validateAccountIds(data, req?.body?.accountIds);
    if (!validated.ok) return res.status(400).json({ error: validated.error });
    if (String(user.role || '') !== 'superadmin') {
      user.accountIds = validated.accountIds;
    }
  }

  if (Object.prototype.hasOwnProperty.call(req.body || {}, 'password')) {
    const nextPassword = String(req.body.password || '');
    if (nextPassword.length < 8) return res.status(400).json({ error: 'password must be at least 8 characters' });
    user.passwordHash = hashPassword(nextPassword);
    destroySessionsForUser(userId);
  }

  saveDataDebounced(data);
  return res.json({ ok: true, user: sanitizeUserForList(user) });
});

module.exports = { adminRouter };
