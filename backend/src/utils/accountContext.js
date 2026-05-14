const crypto = require('crypto');
const { DEV_MODE, WEBHOOK_AUTH_TOKEN, WEBHOOK_DEV_SECRET } = require('../config/runtime');
const { getTenantWebhookAuthTokenByTo } = require('../services/twilioIntegrationService');
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
  // Webhook tenant is derived from destination number.
  // Prefer signed query `to` when present because voice callbacks can contain
  // destination numbers that are not tenant numbers in body To.
  const bodyTo = String(req?.body?.To || req?.body?.to || '').trim();
  const queryTo = String(req?.query?.to || '').trim();
  const value = queryTo || bodyTo;
  if (!value) return null;
  return { type: 'to', value };
}

function loadAccountByTo(to) {
  const data = loadData();
  const account = getAccountByToFromData(data, to);
  if (!account) return null;
  return { to: String(to), account };
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

  const to = String(selector.value);
  const found = loadAccountByTo(to);
  if (!found?.account) {
    // Avoid account enumeration from webhooks.
    return res.status(403).json({ error: 'Invalid webhook tenant' });
  }
  const resolvedAccountId = String(found.account.id || found.account.accountId);

  const tenantWebhookToken = getTenantWebhookAuthTokenByTo(to);
  const signatureOk = hasValidTwilioSignatureForToken(req, tenantWebhookToken) || hasValidTwilioSignature(req);
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
  loadAccountById,
  ensureAccountForTo,
  attachTenant,
  requireTenant,
  requireTenantForWebhook
};
