const crypto = require('crypto');
const { WEBHOOK_PUBLIC_BASE } = require('../config/runtime');
const {
  loadData,
  saveDataDebounced,
  ensureAccountForTo
} = require('../store/dataStore');

const E164_REGEX = /^\+[1-9]\d{1,14}$/;
const SID_ACCOUNT_REGEX = /^AC[0-9a-fA-F]{32}$/;
const SID_API_KEY_REGEX = /^SK[0-9a-fA-F]{32}$/;
const SID_MESSAGING_SERVICE_REGEX = /^MG[0-9a-fA-F]{32}$/;

function buildConvoKey(to, from) {
  return `${String(to || '').trim()}__${String(from || '').trim()}`;
}

function maskSecret(value, { left = 4, right = 4 } = {}) {
  const v = String(value || '').trim();
  if (!v) return '';
  if (v.length <= left + right) return `${v.slice(0, 2)}***`;
  return `${v.slice(0, left)}...${v.slice(-right)}`;
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

function ensureTwilioConfig(account) {
  account.integrations = account.integrations && typeof account.integrations === 'object'
    ? account.integrations
    : {};
  const existing = account.integrations.twilio && typeof account.integrations.twilio === 'object'
    ? account.integrations.twilio
    : {};
  account.integrations.twilio = {
    enabled: existing.enabled === true,
    accountSid: String(existing.accountSid || '').trim(),
    apiKeySid: String(existing.apiKeySid || '').trim(),
    apiKeySecret: String(existing.apiKeySecret || '').trim(),
    messagingServiceSid: String(existing.messagingServiceSid || '').trim(),
    phoneNumber: String(existing.phoneNumber || '').trim(),
    voiceForwardTo: String(existing.voiceForwardTo || '').trim(),
    voiceDialTimeoutSec: Number(existing.voiceDialTimeoutSec || 20) || 20,
    webhookAuthToken: String(existing.webhookAuthToken || '').trim(),
    connectedAt: existing.connectedAt ? Number(existing.connectedAt) : null,
    lastTestedAt: existing.lastTestedAt ? Number(existing.lastTestedAt) : null,
    lastStatus: existing.lastStatus ? String(existing.lastStatus) : null,
    lastError: existing.lastError ? String(existing.lastError) : null
  };
  return account.integrations.twilio;
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

function validateTwilioInput(input, currentCfg = null) {
  const src = input && typeof input === 'object' ? input : {};
  const accountSid = String(src.accountSid || '').trim() || String(currentCfg?.accountSid || '').trim();
  const apiKeySid = String(src.apiKeySid || '').trim() || String(currentCfg?.apiKeySid || '').trim();
  const apiKeySecretRaw = String(src.apiKeySecret || '').trim();
  const apiKeySecret = apiKeySecretRaw || String(currentCfg?.apiKeySecret || '').trim();
  const messagingServiceSid = String(src.messagingServiceSid || '').trim();
  const phoneNumber = String(src.phoneNumber || '').trim();
  const voiceForwardTo = String(src.voiceForwardTo || '').trim();
  const voiceDialTimeoutRaw = src.voiceDialTimeoutSec;
  const voiceDialTimeoutSec = Number.isFinite(Number(voiceDialTimeoutRaw))
    ? Math.round(Number(voiceDialTimeoutRaw))
    : Number(currentCfg?.voiceDialTimeoutSec || 20);
  const webhookAuthToken = String(src.webhookAuthToken || '').trim() || String(currentCfg?.webhookAuthToken || '').trim();

  if (!SID_ACCOUNT_REGEX.test(accountSid)) {
    throw new Error('accountSid must be a valid Twilio Account SID (AC...)');
  }
  const credentialSidLooksValid = SID_API_KEY_REGEX.test(apiKeySid) || SID_ACCOUNT_REGEX.test(apiKeySid);
  if (!credentialSidLooksValid) {
    throw new Error('apiKeySid must be a valid Twilio credential SID (SK... API Key SID or AC... Account SID)');
  }
  if (!apiKeySecret) {
    throw new Error('apiKeySecret is required');
  }
  if (messagingServiceSid && !SID_MESSAGING_SERVICE_REGEX.test(messagingServiceSid)) {
    throw new Error('messagingServiceSid must be a valid Messaging Service SID (MG...)');
  }
  if (phoneNumber && !E164_REGEX.test(phoneNumber)) {
    throw new Error('phoneNumber must be E.164 format (example: +18145551234)');
  }
  if (voiceForwardTo && !E164_REGEX.test(voiceForwardTo)) {
    throw new Error('voiceForwardTo must be E.164 format (example: +18145551234)');
  }
  if (!Number.isFinite(voiceDialTimeoutSec) || voiceDialTimeoutSec < 10 || voiceDialTimeoutSec > 60) {
    throw new Error('voiceDialTimeoutSec must be between 10 and 60');
  }

  return {
    accountSid,
    apiKeySid,
    apiKeySecret,
    messagingServiceSid,
    phoneNumber,
    voiceForwardTo,
    voiceDialTimeoutSec,
    webhookAuthToken
  };
}

async function testTwilioCredentials(cfg) {
  const usingAccountTokenAuth = SID_ACCOUNT_REGEX.test(String(cfg.apiKeySid || '').trim());
  if (usingAccountTokenAuth && String(cfg.apiKeySid || '').trim() !== String(cfg.accountSid || '').trim()) {
    throw new Error(
      `Account SID mismatch. When using AC... + Auth Token, apiKeySid must equal accountSid. Received apiKeySid=${cfg.apiKeySid} and accountSid=${cfg.accountSid}.`
    );
  }

  const auth = Buffer.from(`${cfg.apiKeySid}:${cfg.apiKeySecret}`, 'utf8').toString('base64');

  async function twilioGetJson(url) {
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
    return { res, parsed, raw };
  }

  const accountUrl = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(cfg.accountSid)}.json`;
  const primary = await twilioGetJson(accountUrl);
  if (primary.res.ok) {
    return {
      accountSid: String(primary.parsed?.sid || cfg.accountSid),
      friendlyName: String(primary.parsed?.friendly_name || ''),
      status: String(primary.parsed?.status || '')
    };
  }

  const detail = String(primary.parsed?.message || primary.parsed?.detail || primary.parsed?.code || '').trim();
  const code = String(primary.parsed?.code || '').trim();
  const isAuthError = primary.res.status === 401 || code === '20003' || /authenticate/i.test(detail);

  if (isAuthError) {
    if (usingAccountTokenAuth) {
      throw new Error(
        `Auth Token authentication failed for account ${cfg.accountSid}. Verify you pasted the current Auth Token for this exact account (not API Key Secret, not from another account).`
      );
    }
    // Secondary probe:
    // If listing accounts works, API key/secret are valid and Account SID is mismatched.
    // If this also fails auth, key SID/secret pair is invalid/revoked/wrong type.
    const listProbe = await twilioGetJson('https://api.twilio.com/2010-04-01/Accounts.json?PageSize=1');
    if (listProbe.res.ok) {
      const firstAccountSid = String(listProbe.parsed?.accounts?.[0]?.sid || '').trim();
      if (firstAccountSid && firstAccountSid !== String(cfg.accountSid).trim()) {
        throw new Error(
          `Account SID mismatch. API Key credentials are valid for ${firstAccountSid}, but you entered ${cfg.accountSid}. Use matching Account SID + API Key from the same account/subaccount.`
        );
      }
      throw new Error(
        'Twilio authentication failed for the provided Account SID route. Verify the Account SID belongs to the same account context as the API Key.'
      );
    }

    const listDetail = String(listProbe.parsed?.message || '').trim();
    const listCode = String(listProbe.parsed?.code || '').trim();
    const listAuthError = listProbe.res.status === 401 || listCode === '20003' || /authenticate/i.test(listDetail);
    if (listAuthError) {
      throw new Error(
        'Twilio credential authentication failed. Verify credentials are correct and active. Use either SK... + API Key Secret, or AC... + Auth Token from the same account.'
      );
    }

    throw new Error(detail || `Twilio auth failed (${primary.res.status})`);
  }

  throw new Error(detail || `Twilio auth failed (${primary.res.status})`);
}

function twilioSnapshotFromConfig(cfg) {
  const current = cfg && typeof cfg === 'object' ? cfg : {};
  return {
    enabled: current.enabled === true,
    accountSid: String(current.accountSid || ''),
    accountSidMasked: maskSecret(current.accountSid, { left: 6, right: 4 }),
    apiKeySidMasked: maskSecret(current.apiKeySid, { left: 6, right: 4 }),
    hasApiKeySecret: Boolean(String(current.apiKeySecret || '').trim()),
    messagingServiceSid: String(current.messagingServiceSid || ''),
    phoneNumber: String(current.phoneNumber || ''),
    voiceForwardTo: String(current.voiceForwardTo || ''),
    voiceDialTimeoutSec: Number(current.voiceDialTimeoutSec || 20) || 20,
    hasWebhookAuthToken: Boolean(String(current.webhookAuthToken || '').trim()),
    webhookAuthTokenMasked: maskSecret(current.webhookAuthToken, { left: 3, right: 3 }),
    connectedAt: current.connectedAt ? Number(current.connectedAt) : null,
    lastTestedAt: current.lastTestedAt ? Number(current.lastTestedAt) : null,
    lastStatus: current.lastStatus ? String(current.lastStatus) : null,
    lastError: current.lastError ? String(current.lastError) : null
  };
}

async function connectTwilioForTenant(tenant, input = {}) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const current = ensureTwilioConfig(account);
  const validated = validateTwilioInput(input, current);
  const verified = await testTwilioCredentials(validated);
  const now = Date.now();

  account.integrations.twilio = {
    ...current,
    ...validated,
    enabled: true,
    connectedAt: current.connectedAt || now,
    lastTestedAt: now,
    lastStatus: 'ok',
    lastError: null
  };
  appendIntegrationLog(account, current.enabled ? 'twilio.update' : 'twilio.connect', `Twilio connected (${verified.accountSid})`);
  saveDataDebounced(data);
  return {
    ok: true,
    twilio: twilioSnapshotFromConfig(account.integrations.twilio)
  };
}

async function testTwilioForTenant(tenant) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const cfg = ensureTwilioConfig(account);
  if (!cfg.enabled) throw new Error('Twilio is not connected');
  const validated = validateTwilioInput(cfg, cfg);
  try {
    const result = await testTwilioCredentials(validated);
    account.integrations.twilio = {
      ...cfg,
      lastTestedAt: Date.now(),
      lastStatus: 'ok',
      lastError: null
    };
    appendIntegrationLog(account, 'twilio.test', `Twilio test succeeded (${result.accountSid})`);
    saveDataDebounced(data);
    return {
      ok: true,
      accountSid: result.accountSid,
      friendlyName: result.friendlyName,
      status: result.status,
      twilio: twilioSnapshotFromConfig(account.integrations.twilio)
    };
  } catch (err) {
    account.integrations.twilio = {
      ...cfg,
      lastTestedAt: Date.now(),
      lastStatus: 'error',
      lastError: err?.message || 'Twilio test failed'
    };
    appendIntegrationLog(account, 'twilio.error', `Twilio test failed (${err?.message || 'unknown error'})`);
    saveDataDebounced(data);
    throw err;
  }
}

function disconnectTwilioForTenant(tenant) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const cfg = ensureTwilioConfig(account);
  account.integrations.twilio = {
    enabled: false,
    accountSid: '',
    apiKeySid: '',
    apiKeySecret: '',
    messagingServiceSid: '',
    phoneNumber: '',
    voiceForwardTo: '',
    voiceDialTimeoutSec: Number(cfg.voiceDialTimeoutSec || 20) || 20,
    webhookAuthToken: '',
    connectedAt: cfg.connectedAt || null,
    lastTestedAt: Date.now(),
    lastStatus: null,
    lastError: null
  };
  appendIntegrationLog(account, 'twilio.disconnect', 'Twilio disconnected');
  saveDataDebounced(data);
  return { ok: true, twilio: twilioSnapshotFromConfig(account.integrations.twilio) };
}

function getTenantTwilioSnapshot(tenant) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const cfg = ensureTwilioConfig(account);
  return { twilio: twilioSnapshotFromConfig(cfg) };
}

function getTenantWebhookAuthTokenByTo(to) {
  const data = loadData();
  const account = ensureAccountForTo(data, String(to || '').trim(), { autoCreate: false });
  if (!account) return '';
  const cfg = ensureTwilioConfig(account);
  return String(cfg.webhookAuthToken || '').trim();
}

function getTenantTwilioConfig(tenant) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const cfg = ensureTwilioConfig(account);
  return {
    enabled: cfg.enabled === true,
    accountSid: String(cfg.accountSid || '').trim(),
    apiKeySid: String(cfg.apiKeySid || '').trim(),
    apiKeySecret: String(cfg.apiKeySecret || '').trim(),
    messagingServiceSid: String(cfg.messagingServiceSid || '').trim(),
    phoneNumber: String(cfg.phoneNumber || '').trim(),
    voiceForwardTo: String(cfg.voiceForwardTo || '').trim(),
    voiceDialTimeoutSec: Number(cfg.voiceDialTimeoutSec || 20) || 20
  };
}

function twilioReadyForSend(tenant) {
  const cfg = getTenantTwilioConfig(tenant);
  if (cfg.enabled !== true) return { ok: false, reason: 'Twilio is not connected' };
  if (!cfg.accountSid || !cfg.apiKeySid || !cfg.apiKeySecret) {
    return { ok: false, reason: 'Twilio credentials are incomplete' };
  }
  if (!cfg.messagingServiceSid && !cfg.phoneNumber) {
    return { ok: false, reason: 'Set either Twilio phoneNumber or messagingServiceSid' };
  }
  return { ok: true, cfg };
}

function buildTwilioStatusCallbackUrl(tenant) {
  const base = String(WEBHOOK_PUBLIC_BASE || '').trim().replace(/\/$/, '');
  const tenantTo = String(tenant?.to || '').trim();
  if (!base || !tenantTo) return '';
  return `${base}/webhooks/twilio/status?to=${encodeURIComponent(tenantTo)}`;
}

async function sendTwilioMessageForTenant(tenant, { to, body, statusCallbackUrl = '' } = {}) {
  const ready = twilioReadyForSend(tenant);
  if (!ready.ok) throw new Error(ready.reason);
  const cfg = ready.cfg;
  const toNumber = String(to || '').trim();
  const text = String(body || '').trim();
  if (!E164_REGEX.test(toNumber)) throw new Error('Destination phone number must be E.164');
  if (!text) throw new Error('Message body is required');

  const form = new URLSearchParams();
  form.set('To', toNumber);
  form.set('Body', text);
  if (cfg.messagingServiceSid) form.set('MessagingServiceSid', cfg.messagingServiceSid);
  else form.set('From', cfg.phoneNumber);
  if (String(statusCallbackUrl || '').trim()) {
    form.set('StatusCallback', String(statusCallbackUrl).trim());
  }

  const url = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(cfg.accountSid)}/Messages.json`;
  const auth = Buffer.from(`${cfg.apiKeySid}:${cfg.apiKeySecret}`, 'utf8').toString('base64');
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${auth}`,
      'Content-Type': 'application/x-www-form-urlencoded',
      Accept: 'application/json'
    },
    body: form.toString()
  });
  const raw = await res.text();
  let parsed = {};
  try { parsed = JSON.parse(raw); } catch {}
  if (!res.ok) {
    const detail = String(parsed?.message || parsed?.detail || parsed?.code || '').trim();
    throw new Error(detail || `Twilio send failed (${res.status})`);
  }
  return {
    sid: String(parsed?.sid || ''),
    status: String(parsed?.status || ''),
    to: String(parsed?.to || toNumber),
    from: String(parsed?.from || cfg.phoneNumber || ''),
    messagingServiceSid: String(parsed?.messaging_service_sid || cfg.messagingServiceSid || ''),
    raw: parsed
  };
}

module.exports = {
  buildConvoKey,
  connectTwilioForTenant,
  testTwilioForTenant,
  disconnectTwilioForTenant,
  getTenantTwilioSnapshot,
  getTenantWebhookAuthTokenByTo,
  getTenantTwilioConfig,
  twilioReadyForSend,
  buildTwilioStatusCallbackUrl,
  sendTwilioMessageForTenant
};
