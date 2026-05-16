const crypto = require('crypto');
const { DEV_MODE, WEBHOOK_AUTH_TOKEN, WEBHOOK_DEV_SECRET } = require('../config/runtime');
const {
  loadData,
  saveDataDebounced,
  getDevSettings,
  getAccountById: getAccountByIdFromData,
  getAccountByTo: getAccountByToFromData,
  ensureAccountForTo: ensureAccountForToInData
} = require('../store/dataStore');
const { canUserAccessAccount } = require('./auth');
const { attachUserFromSession } = require('./authMiddleware');

function debugLog(...args) {
  if (DEV_MODE === true) console.log(...args);
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

function hasValidTwilioSignature(req) {
  const token = WEBHOOK_AUTH_TOKEN;
  return hasValidTwilioSignatureForToken(req, token);
}

function hasValidTwilioSignatureForToken(req, token) {
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

function getQuery(req, key) {
  return String(req?.query?.[key] || '').trim();
}

function getHeader(req, key) {
  return String(req?.headers?.[key] || '').trim();
}

function normalizePhone(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const hasPlus = raw.startsWith('+');
  const digits = raw.replace(/[^\d]/g, '');
  return hasPlus ? `+${digits}` : digits;
}

function accountHasNumber(account, accountTo, phoneNumber) {
  const target = normalizePhone(phoneNumber);
  if (!target || !account || typeof account !== 'object') return false;
  const candidates = [
    accountTo,
    account.to,
    account?.integrations?.twilio?.phoneNumber,
    ...(Array.isArray(account?.workspace?.phoneNumbers)
      ? account.workspace.phoneNumbers.map((row) => row?.number)
      : [])
  ];
  return candidates.some((candidate) => normalizePhone(candidate) === target);
}

function resolveTenantSelector(req, { allowBodyTo = false } = {}) {
  const accountId = getQuery(req, 'accountId');
  const to = getQuery(req, 'to');
  const headerAccountId = getHeader(req, 'x-account-id');
  const headerTo = getHeader(req, 'x-tenant-to');
  const bodyTo = allowBodyTo ? String(req?.body?.to || req?.body?.To || '').trim() : '';

  if (accountId) return { type: 'accountId', value: accountId };
  if (to) return { type: 'to', value: to };
  if (headerAccountId) return { type: 'accountId', value: headerAccountId };
  if (headerTo) return { type: 'to', value: headerTo };
  if (bodyTo) return { type: 'to', value: bodyTo };
  return null;
}

function resolveWebhookTenantSelector(req) {
  // Webhook tenant is derived from Twilio-owned numbers only.
  // Status callbacks often contain the customer in body To and the Twilio
  // sender in body From, while inbound SMS/voice usually contain Twilio in To.
  const bodyTo = String(req?.body?.To || req?.body?.to || '').trim();
  const bodyFrom = String(req?.body?.From || req?.body?.from || '').trim();
  const queryTo = String(req?.query?.to || '').trim();
  const candidates = [
    queryTo ? { value: queryTo, source: 'queryTo' } : null,
    bodyTo ? { value: bodyTo, source: 'bodyTo' } : null,
    bodyFrom ? { value: bodyFrom, source: 'bodyFrom' } : null
  ].filter(Boolean);
  if (!candidates.length) return null;
  return { type: 'to', value: candidates[0].value, candidates };
}

function loadAccountByTo(to) {
  const data = loadData();
  const account = getAccountByToFromData(data, to);
  if (!account) return null;
  return { to: String(to), account };
}

function loadAccountByPhoneNumber(phoneNumber) {
  const data = loadData();
  const exact = getAccountByToFromData(data, String(phoneNumber || '').trim());
  if (exact) return { to: String(phoneNumber || '').trim(), account: exact };

  const target = normalizePhone(phoneNumber);
  if (!target) return null;
  for (const [to, account] of Object.entries(data.accounts || {})) {
    if (accountHasNumber(account, to, target)) {
      return { to: String(to), account };
    }
  }
  return null;
}

function loadAccountById(accountId) {
  const data = loadData();
  return getAccountByIdFromData(data, accountId);
}

function ensureAccountForTo(to) {
  const data = loadData();
  const account = ensureAccountForToInData(data, to, { autoCreate: true });
  saveDataDebounced(data);
  return account;
}

function attachTenant(req, tenant) {
  req.tenant = {
    accountId: String(tenant.accountId),
    to: String(tenant.to),
    account: tenant.account || null
  };
  return req.tenant;
}

function resolveTenant(req, { allowBodyTo = false } = {}) {
  const dev = getDevSettings();
  const role = String(req?.user?.role || '').toLowerCase();
  const isSuperadmin = role === 'superadmin';
  const allowAutoCreate = DEV_MODE === true && dev.enabled === true && dev.autoCreateTenants === true && isSuperadmin;
  const verbose = dev.verboseTenantLogs === true;
  const selector = resolveTenantSelector(req, { allowBodyTo });
  if (!selector) {
    return { ok: false, status: 400, error: 'Missing tenant selector. Provide accountId or to.' };
  }

  if (selector.type === 'accountId') {
    const found = loadAccountById(selector.value);
    if (!found?.account) {
      return { ok: false, status: 404, error: `Account not found for accountId=${selector.value}` };
    }
    const resolvedAccountId = String(found.account.id || found.account.accountId);
    if (req?.user && !canUserAccessAccount(req.user, resolvedAccountId)) {
      return { ok: false, status: 404, error: 'Account not found' };
    }
    if (verbose) {
      debugLog(`[tenant] resolved by accountId=${selector.value} to=${found.to}`);
    }
    return {
      ok: true,
      tenant: {
        accountId: String(found.account.id || found.account.accountId),
        to: String(found.to || found.account.to),
        account: found.account
      }
    };
  }

  const to = String(selector.value);
  let found = loadAccountByTo(to);
  if (!found) {
    if (!allowAutoCreate) {
      return { ok: false, status: 404, error: `Account not found for to=${to}` };
    }
    const account = ensureAccountForTo(to);
    if (verbose) {
      debugLog(`[tenant] auto-created account for to=${to}`);
    }
    found = { to, account };
  }
  const resolvedAccountId = String(found.account.id || found.account.accountId);
  if (req?.user && !canUserAccessAccount(req.user, resolvedAccountId)) {
    return { ok: false, status: 404, error: 'Account not found' };
  }
  if (verbose) {
    debugLog(`[tenant] resolved by to=${to} accountId=${found.account?.id || found.account?.accountId}`);
  }

  return {
    ok: true,
    tenant: {
      accountId: String(found.account.id || found.account.accountId),
      to: String(found.to || found.account.to),
      account: found.account
    }
  };
}

function createRequireTenant({ allowBodyTo = false } = {}) {
  return function requireTenant(req, res, next) {
    const result = resolveTenant(req, { allowBodyTo });
    if (!result.ok) return res.status(result.status || 400).json({ error: result.error });
    attachTenant(req, result.tenant);
    return next();
  };
}

const requireTenant = createRequireTenant({ allowBodyTo: false });

function requireTenantForWebhook(req, res, next) {
  // Webhook tenant must be derived from Twilio destination number only.
  // Do NOT accept tenant selectors from query params or headers.
  const selector = resolveWebhookTenantSelector(req);
  if (!selector) {
    return res.status(403).json({ error: 'Missing webhook tenant (To)' });
  }

  const candidates = Array.isArray(selector.candidates) && selector.candidates.length
    ? selector.candidates
    : [{ value: selector.value, source: 'selector' }];
  const matched = candidates
    .map((candidate) => ({
      source: String(candidate?.source || 'selector'),
      found: loadAccountByPhoneNumber(candidate?.value)
    }))
    .find((match) => match?.found?.account);
  const found = matched?.found;
  if (!found?.account) {
    // Avoid account enumeration from webhooks.
    return res.status(403).json({ error: 'Invalid webhook tenant' });
  }
  const resolvedAccountId = String(found.account.id || found.account.accountId);

  const tenantWebhookToken = String(found.account?.integrations?.twilio?.webhookAuthToken || '').trim();
  const signatureOk = hasValidTwilioSignatureForToken(req, tenantWebhookToken) || hasValidTwilioSignature(req);
  if (matched?.source === 'bodyFrom' && !signatureOk) {
    return res.status(403).json({ error: 'Invalid webhook signature' });
  }
  const simulatorUser = DEV_MODE === true ? attachUserFromSession(req) : null;
  const simulatorOk = Boolean(
    simulatorUser &&
    canUserAccessAccount(simulatorUser, resolvedAccountId)
  );
  if (!signatureOk && !hasValidDevWebhookSecret(req) && !simulatorOk) {
    return res.status(403).json({ error: 'Invalid webhook signature' });
  }
  req.webhookAuthOk = true;

  attachTenant(req, { accountId: resolvedAccountId, to: String(found.to || found.account.to), account: found.account });
  return next();
}

module.exports = {
  resolveTenantSelector,
  resolveWebhookTenantSelector,
  loadAccountByTo,
  loadAccountByPhoneNumber,
  loadAccountById,
  ensureAccountForTo,
  attachTenant,
  requireTenant,
  requireTenantForWebhook
};
