const crypto = require('crypto');
const { loadData, saveDataDebounced, ensureAccountForTo } = require('../store/dataStore');
const {
  CAL_OAUTH_REDIRECT_BASE,
  GOOGLE_CAL_CLIENT_ID,
  GOOGLE_CAL_CLIENT_SECRET,
  MICROSOFT_CAL_CLIENT_ID,
  MICROSOFT_CAL_CLIENT_SECRET
} = require('../config/runtime');

const MAX_ICS_BYTES = 2 * 1024 * 1024; // 2MB
const FETCH_TIMEOUT_MS = 8000;
const MAX_EVENTS_STORED = 500;
const SYNC_MINUTES_ALLOWED = new Set([0, 5, 15, 60]);
const TWO_WAY_SYNC_MINUTES_ALLOWED = new Set([0, 5, 15, 60]);
const TWO_WAY_PROVIDERS = new Set(['google', 'outlook']);
const OAUTH_STATE_TTL_MS = 10 * 60 * 1000;
const oauthStateStore = new Map();

const inFlightSyncs = new Set();

function normalizeSyncMinutes(value) {
  const v = Number(value);
  if (!Number.isFinite(v) || !SYNC_MINUTES_ALLOWED.has(v)) return 60;
  return v;
}

function normalizeTwoWaySyncMinutes(value) {
  const v = Number(value);
  if (!Number.isFinite(v) || !TWO_WAY_SYNC_MINUTES_ALLOWED.has(v)) return 15;
  return v;
}

function normalizeProviderName(provider) {
  return String(provider || '').trim().toLowerCase();
}

function providerSource(provider) {
  const p = normalizeProviderName(provider);
  if (!TWO_WAY_PROVIDERS.has(p)) {
    throw new Error('Invalid provider');
  }
  return p;
}

function providerDisplay(provider) {
  return providerSource(provider) === 'google' ? 'Google' : 'Outlook';
}

function maskSecret(value) {
  const clean = String(value || '').trim();
  if (!clean) return '';
  if (clean.length <= 10) return `${clean.slice(0, 2)}***${clean.slice(-2)}`;
  return `${clean.slice(0, 4)}...${clean.slice(-4)}`;
}

function defaultTwoWayProviderConfig(provider) {
  const p = providerSource(provider);
  return {
    enabled: false,
    mode: 'two_way',
    provider: p,
    calendarId: 'primary',
    syncMinutes: 15,
    accessToken: '',
    accessTokenMasked: '',
    refreshToken: '',
    hasRefreshToken: false,
    connectedAt: null,
    lastSyncedAt: null,
    lastSyncStatus: null,
    lastSyncError: null,
    importedCountLast: 0,
    pushedCountLast: 0,
    lastSyncAttemptAt: null,
    remoteEvents: []
  };
}

function getOAuthProviderConfig(provider) {
  const source = providerSource(provider);
  if (source === 'google') {
    return {
      source,
      authorizeEndpoint: 'https://accounts.google.com/o/oauth2/v2/auth',
      tokenEndpoint: 'https://oauth2.googleapis.com/token',
      clientId: GOOGLE_CAL_CLIENT_ID,
      clientSecret: GOOGLE_CAL_CLIENT_SECRET,
      scopes: ['https://www.googleapis.com/auth/calendar']
    };
  }
  return {
    source,
    authorizeEndpoint: 'https://login.microsoftonline.com/common/oauth2/v2.0/authorize',
    tokenEndpoint: 'https://login.microsoftonline.com/common/oauth2/v2.0/token',
    clientId: MICROSOFT_CAL_CLIENT_ID,
    clientSecret: MICROSOFT_CAL_CLIENT_SECRET,
    scopes: ['offline_access', 'Calendars.ReadWrite']
  };
}

function oauthRedirectUri(provider) {
  return `${String(CAL_OAUTH_REDIRECT_BASE).replace(/\/$/, '')}/oauth/calendar/${providerSource(provider)}/callback`;
}

function cleanupExpiredOAuthStates() {
  const now = Date.now();
  for (const [state, payload] of oauthStateStore.entries()) {
    if (!payload || (now - Number(payload.createdAt || 0)) > OAUTH_STATE_TTL_MS) {
      oauthStateStore.delete(state);
    }
  }
}

function createCalendarOAuthStartForTenant(tenant, { provider } = {}) {
  const cfg = getOAuthProviderConfig(provider);
  if (!cfg.clientId || !cfg.clientSecret) {
    throw new Error(`${providerDisplay(cfg.source)} OAuth is not configured on server`);
  }
  cleanupExpiredOAuthStates();
  const state = crypto.randomUUID();
  const verifier = crypto.randomUUID().replace(/-/g, '');
  oauthStateStore.set(state, {
    createdAt: Date.now(),
    provider: cfg.source,
    tenant: {
      accountId: String(tenant?.accountId || ''),
      to: String(tenant?.to || '')
    },
    verifier
  });

  const redirectUri = oauthRedirectUri(cfg.source);
  const params = new URLSearchParams({
    client_id: cfg.clientId,
    response_type: 'code',
    redirect_uri: redirectUri,
    scope: cfg.scopes.join(' '),
    state
  });
  if (cfg.source === 'google') {
    params.set('access_type', 'offline');
    params.set('prompt', 'consent');
    params.set('include_granted_scopes', 'true');
  }
  if (cfg.source === 'outlook') {
    params.set('response_mode', 'query');
  }
  return {
    ok: true,
    provider: cfg.source,
    authUrl: `${cfg.authorizeEndpoint}?${params.toString()}`
  };
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function buildOAuthCallbackHtml({ provider, ok, message }) {
  const payload = JSON.stringify({
    type: 'relay:calendar-oauth',
    provider: providerSource(provider),
    ok: ok === true,
    message: String(message || '')
  });
  const title = ok ? `${providerDisplay(provider)} connected` : `${providerDisplay(provider)} connection failed`;
  const safeTitle = escapeHtml(title);
  const safeMessage = escapeHtml(message || (ok ? 'Connection completed.' : 'Connection failed.'));
  return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>${safeTitle}</title>
    <style>
      body { margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; background:#0b1220; color:#e6edf6; display:flex; min-height:100vh; align-items:center; justify-content:center; }
      .card { width:min(460px, 92vw); border:1px solid rgba(255,255,255,.14); border-radius:14px; padding:18px; background:rgba(255,255,255,.04); }
      .title { font-weight:700; margin:0 0 6px 0; }
      .msg { margin:0; color:rgba(230,237,246,.78); }
    </style>
  </head>
  <body>
    <div class="card">
      <h1 class="title">${safeTitle}</h1>
      <p class="msg">${safeMessage}</p>
    </div>
    <script>
      (function () {
        var payload = ${payload};
        try {
          if (window.opener && !window.opener.closed) {
            window.opener.postMessage(payload, '*');
            setTimeout(function(){ window.close(); }, 120);
            return;
          }
        } catch (e) {}
        setTimeout(function(){ window.location.href = '/'; }, 1200);
      })();
    </script>
  </body>
</html>`;
}

async function exchangeOAuthCodeForTokens(provider, code) {
  const cfg = getOAuthProviderConfig(provider);
  if (!cfg.clientId || !cfg.clientSecret) {
    throw new Error(`${providerDisplay(cfg.source)} OAuth is not configured on server`);
  }
  const redirectUri = oauthRedirectUri(cfg.source);
  const body = new URLSearchParams({
    client_id: cfg.clientId,
    client_secret: cfg.clientSecret,
    grant_type: 'authorization_code',
    code: String(code || ''),
    redirect_uri: redirectUri
  });
  if (cfg.source === 'outlook') {
    body.set('scope', cfg.scopes.join(' '));
  }
  const response = await fetch(cfg.tokenEndpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString()
  });
  const raw = await response.text();
  let parsed = {};
  try { parsed = JSON.parse(raw); } catch {}
  if (!response.ok) {
    throw new Error(parsed?.error_description || parsed?.error || `${providerDisplay(cfg.source)} token exchange failed`);
  }
  const accessToken = String(parsed?.access_token || '').trim();
  const refreshToken = String(parsed?.refresh_token || '').trim();
  if (!accessToken) throw new Error(`${providerDisplay(cfg.source)} did not return access token`);
  return { accessToken, refreshToken };
}

async function completeCalendarOAuthCallback({ provider, state, code, error, error_description }) {
  const source = providerSource(provider);
  cleanupExpiredOAuthStates();
  const pending = oauthStateStore.get(String(state || ''));
  oauthStateStore.delete(String(state || ''));
  if (!pending || pending.provider !== source) {
    throw new Error('OAuth session expired or invalid state');
  }
  if (error) {
    throw new Error(String(error_description || error));
  }
  if (!code) {
    throw new Error('Missing OAuth code');
  }
  const tokens = await exchangeOAuthCodeForTokens(source, code);
  const tenant = pending.tenant || {};
  await connectCalendarTwoWayForTenant(tenant, {
    provider: source,
    accessToken: tokens.accessToken,
    refreshToken: tokens.refreshToken,
    calendarId: 'primary',
    syncMinutes: 15,
    triggerInitialSync: true
  });
  return { ok: true, provider: source };
}

function decodeIcsText(value) {
  return String(value || '')
    .replace(/\\\\/g, '\\')
    .replace(/\\n/gi, '\n')
    .replace(/\\,/g, ',')
    .replace(/\\;/g, ';')
    .trim();
}

function parseIcsDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (/^\d{8}$/.test(raw)) {
    const y = Number(raw.slice(0, 4));
    const m = Number(raw.slice(4, 6));
    const d = Number(raw.slice(6, 8));
    return Date.UTC(y, m - 1, d, 0, 0, 0, 0);
  }
  if (/^\d{8}T\d{6}Z$/.test(raw)) {
    const y = Number(raw.slice(0, 4));
    const m = Number(raw.slice(4, 6));
    const d = Number(raw.slice(6, 8));
    const hh = Number(raw.slice(9, 11));
    const mm = Number(raw.slice(11, 13));
    const ss = Number(raw.slice(13, 15));
    return Date.UTC(y, m - 1, d, hh, mm, ss, 0);
  }
  if (/^\d{8}T\d{6}$/.test(raw)) {
    const y = Number(raw.slice(0, 4));
    const m = Number(raw.slice(4, 6));
    const d = Number(raw.slice(6, 8));
    const hh = Number(raw.slice(9, 11));
    const mm = Number(raw.slice(11, 13));
    const ss = Number(raw.slice(13, 15));
    return new Date(y, m - 1, d, hh, mm, ss, 0).getTime();
  }
  const fallback = Date.parse(raw);
  return Number.isFinite(fallback) ? fallback : null;
}

function unfoldLines(raw) {
  const lines = String(raw || '')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .split('\n');
  const out = [];
  for (const line of lines) {
    if (!line) {
      out.push('');
      continue;
    }
    if ((line.startsWith(' ') || line.startsWith('\t')) && out.length > 0) {
      out[out.length - 1] += line.slice(1);
    } else {
      out.push(line);
    }
  }
  return out;
}

function parseIcsEvents(icsText) {
  const lines = unfoldLines(icsText);
  const events = [];
  let current = null;

  for (const line of lines) {
    const normalized = String(line || '').trim();
    if (!normalized) continue;

    if (normalized === 'BEGIN:VEVENT') {
      current = {};
      continue;
    }
    if (normalized === 'END:VEVENT') {
      if (current?.start && current?.end) {
        events.push(current);
      }
      current = null;
      continue;
    }
    if (!current) continue;

    const sep = normalized.indexOf(':');
    if (sep < 0) continue;
    const keyRaw = normalized.slice(0, sep);
    const valueRaw = normalized.slice(sep + 1);
    const key = keyRaw.split(';')[0].toUpperCase();

    if (key === 'UID') current.uid = decodeIcsText(valueRaw);
    if (key === 'SUMMARY') current.summary = decodeIcsText(valueRaw);
    if (key === 'LOCATION') current.location = decodeIcsText(valueRaw);
    if (key === 'DTSTART') current.start = parseIcsDate(valueRaw);
    if (key === 'DTEND') current.end = parseIcsDate(valueRaw);
  }

  return events
    .filter((ev) => Number.isFinite(ev.start) && Number.isFinite(ev.end) && ev.end >= ev.start)
    .slice(0, MAX_EVENTS_STORED);
}

async function readResponseWithSizeLimit(response, maxBytes) {
  if (!response.body || typeof response.body.getReader !== 'function') {
    const text = await response.text();
    if (Buffer.byteLength(text, 'utf8') > maxBytes) {
      throw new Error(`ICS payload exceeds ${Math.round(maxBytes / 1024)}KB limit`);
    }
    return text;
  }

  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    const buf = Buffer.from(value);
    total += buf.length;
    if (total > maxBytes) {
      throw new Error(`ICS payload exceeds ${Math.round(maxBytes / 1024)}KB limit`);
    }
    chunks.push(buf);
  }
  return Buffer.concat(chunks).toString('utf8');
}

async function fetchIcsUrl(url) {
  let parsed;
  try {
    parsed = new URL(String(url || '').trim());
  } catch {
    throw new Error('Enter a valid URL');
  }
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    throw new Error('Only http/https URLs are supported');
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const response = await fetch(parsed.toString(), {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: { Accept: 'text/calendar, text/plain;q=0.9, */*;q=0.1' }
    });
    if (!response.ok) {
      throw new Error(`Calendar feed responded with HTTP ${response.status}`);
    }

    const len = Number(response.headers.get('content-length') || 0);
    if (Number.isFinite(len) && len > MAX_ICS_BYTES) {
      throw new Error(`ICS payload exceeds ${Math.round(MAX_ICS_BYTES / 1024)}KB limit`);
    }

    const contentType = String(response.headers.get('content-type') || '').toLowerCase();
    const body = await readResponseWithSizeLimit(response, MAX_ICS_BYTES);
    return { body, contentType };
  } catch (err) {
    if (err?.name === 'AbortError') {
      throw new Error('Calendar feed request timed out');
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

function buildSample(events, privacyMode) {
  const first = events[0];
  if (!first) return null;
  return {
    start: first.start,
    end: first.end,
    summary: privacyMode ? 'Busy' : (first.summary || 'Busy')
  };
}

function maskUrl(url) {
  const clean = String(url || '').trim();
  if (!clean) return '';
  if (clean.length <= 14) return `${clean.slice(0, 4)}***${clean.slice(-4)}`;
  return `${clean.slice(0, 6)}...${clean.slice(-6)}`;
}

function getCalendarIcsConfig(account) {
  const cfg = account?.integrations?.calendarIcs || {};
  return {
    enabled: cfg.enabled === true,
    provider: String(cfg.provider || 'other'),
    url: String(cfg.url || ''),
    urlMasked: maskUrl(cfg.url || ''),
    privacyMode: cfg.privacyMode !== false,
    syncMinutes: normalizeSyncMinutes(cfg.syncMinutes),
    lastSyncedAt: cfg.lastSyncedAt ? Number(cfg.lastSyncedAt) : null,
    lastSyncStatus: cfg.lastSyncStatus || null,
    lastSyncError: cfg.lastSyncError ? String(cfg.lastSyncError) : null,
    importedCountLast: Number(cfg.importedCountLast || 0),
    lastSyncAttemptAt: cfg.lastSyncAttemptAt ? Number(cfg.lastSyncAttemptAt) : null
  };
}

function ensureCalendarProviders(account) {
  account.integrations = account.integrations || {};
  const providers = account.integrations.calendarProviders && typeof account.integrations.calendarProviders === 'object'
    ? account.integrations.calendarProviders
    : {};
  for (const provider of TWO_WAY_PROVIDERS) {
    const existing = providers[provider] && typeof providers[provider] === 'object'
      ? providers[provider]
      : {};
    providers[provider] = {
      ...defaultTwoWayProviderConfig(provider),
      ...existing,
      provider,
      mode: 'two_way',
      syncMinutes: normalizeTwoWaySyncMinutes(existing.syncMinutes),
      hasRefreshToken: existing.hasRefreshToken === true || Boolean(String(existing.refreshToken || '').trim()),
      remoteEvents: Array.isArray(existing.remoteEvents) ? existing.remoteEvents : []
    };
  }
  account.integrations.calendarProviders = providers;
  return providers;
}

function getCalendarProviderConfig(account, provider) {
  const source = providerSource(provider);
  const providers = ensureCalendarProviders(account);
  const cfg = providers[source] || defaultTwoWayProviderConfig(source);
  return {
    enabled: cfg.enabled === true,
    mode: 'two_way',
    provider: source,
    calendarId: String(cfg.calendarId || 'primary'),
    syncMinutes: normalizeTwoWaySyncMinutes(cfg.syncMinutes),
    connectedAt: cfg.connectedAt ? Number(cfg.connectedAt) : null,
    lastSyncedAt: cfg.lastSyncedAt ? Number(cfg.lastSyncedAt) : null,
    lastSyncStatus: cfg.lastSyncStatus || null,
    lastSyncError: cfg.lastSyncError ? String(cfg.lastSyncError) : null,
    importedCountLast: Number(cfg.importedCountLast || 0),
    pushedCountLast: Number(cfg.pushedCountLast || 0),
    lastSyncAttemptAt: cfg.lastSyncAttemptAt ? Number(cfg.lastSyncAttemptAt) : null,
    accessTokenMasked: String(cfg.accessTokenMasked || maskSecret(cfg.accessToken || '')),
    hasRefreshToken: cfg.hasRefreshToken === true || Boolean(String(cfg.refreshToken || '').trim())
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

function buildEventRecord(event, privacyMode) {
  const uidSeed = event.uid || `${event.start}_${event.end}_${event.summary || ''}`;
  const id = `ics_${crypto.createHash('sha1').update(uidSeed).digest('hex').slice(0, 18)}`;
  return {
    id,
    source: 'ics',
    uid: String(event.uid || ''),
    start: Number(event.start),
    end: Number(event.end),
    summary: privacyMode ? 'Busy' : String(event.summary || 'Busy'),
    location: privacyMode ? '' : String(event.location || ''),
    updatedAt: Date.now()
  };
}

function buildProviderEventRecord(provider, event) {
  const source = providerSource(provider);
  const seed = String(event.remoteId || event.id || `${event.start}_${event.end}_${event.summary || event.title || ''}`);
  const id = `${source}_${crypto.createHash('sha1').update(seed).digest('hex').slice(0, 18)}`;
  return {
    id,
    source,
    provider: source,
    providerEventId: String(event.remoteId || event.id || id),
    start: Number(event.start),
    end: Number(event.end),
    summary: String(event.summary || event.title || 'Busy'),
    location: String(event.location || ''),
    updatedAt: Date.now()
  };
}

function accountByTenant(data, tenant) {
  const to = String(tenant?.to || '').trim();
  const accountId = String(tenant?.accountId || '').trim();
  if (!to || !accountId) {
    throw new Error('Missing tenant context');
  }
  const account = ensureAccountForTo(data, to, { autoCreate: true });
  if (!account) throw new Error('Account not found');
  if (String(account.accountId || account.id || '') !== accountId) {
    throw new Error('Tenant/account mismatch');
  }
  return { to, account };
}

async function testCalendarIcsUrl({ url, privacyMode = true }) {
  const clean = String(url || '').trim();
  if (!clean) throw new Error('Calendar feed URL is required');

  const { body, contentType } = await fetchIcsUrl(clean);
  if (!body.includes('BEGIN:VCALENDAR')) {
    throw new Error('Feed is not a valid ICS calendar');
  }

  const warnings = [];
  const urlLooksLikeIcs = /\.ics(\?|#|$)/i.test(clean);
  if (!urlLooksLikeIcs && !contentType.includes('text/calendar')) {
    warnings.push('Feed URL does not end with .ics and content-type is not text/calendar');
  }

  const events = parseIcsEvents(body);
  if (!events.length) {
    warnings.push('Feed is reachable, but no upcoming events were detected');
  }

  return {
    ok: true,
    sample: buildSample(events, privacyMode === true),
    warnings
  };
}

async function syncCalendarIcsForTenant(tenant, { reason = 'manual' } = {}) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const cfg = getCalendarIcsConfig(account);
  if (!cfg.enabled || !cfg.url) {
    throw new Error('Calendar integration is not connected');
  }

  const lockKey = `${tenant.accountId}::${tenant.to}`;
  if (inFlightSyncs.has(lockKey)) {
    throw new Error('A sync is already in progress');
  }
  inFlightSyncs.add(lockKey);

  try {
    const { body } = await fetchIcsUrl(cfg.url);
    if (!body.includes('BEGIN:VCALENDAR')) {
      throw new Error('Feed is not a valid ICS calendar');
    }
    const parsedEvents = parseIcsEvents(body);
    const importedEvents = parsedEvents.map((ev) => buildEventRecord(ev, cfg.privacyMode));

    account.calendarEvents = Array.isArray(account.calendarEvents) ? account.calendarEvents : [];
    const nonIcsEvents = account.calendarEvents.filter((ev) => String(ev?.source || '') !== 'ics');
    account.calendarEvents = nonIcsEvents.concat(importedEvents).slice(-2000);

    account.integrations = account.integrations || {};
    account.integrations.calendarIcs = {
      ...cfg,
      enabled: true,
      url: cfg.url,
      provider: cfg.provider || 'other',
      privacyMode: cfg.privacyMode !== false,
      syncMinutes: normalizeSyncMinutes(cfg.syncMinutes),
      lastSyncedAt: Date.now(),
      lastSyncStatus: 'ok',
      lastSyncError: null,
      importedCountLast: importedEvents.length,
      lastSyncAttemptAt: Date.now()
    };
    appendIntegrationLog(account, 'calendar.sync', `Sync completed (${importedEvents.length} events imported)`);
    saveDataDebounced(data);

    return {
      ok: true,
      importedCount: importedEvents.length,
      lastSyncedAt: account.integrations.calendarIcs.lastSyncedAt,
      status: account.integrations.calendarIcs.lastSyncStatus,
      reason
    };
  } catch (err) {
    account.integrations = account.integrations || {};
    const current = getCalendarIcsConfig(account);
    account.integrations.calendarIcs = {
      ...current,
      enabled: true,
      url: current.url,
      provider: current.provider || 'other',
      privacyMode: current.privacyMode !== false,
      syncMinutes: normalizeSyncMinutes(current.syncMinutes),
      lastSyncStatus: 'error',
      lastSyncError: err?.message || 'Sync failed',
      lastSyncAttemptAt: Date.now()
    };
    appendIntegrationLog(account, 'calendar.error', `Sync failed (${err?.message || 'unknown error'})`);
    saveDataDebounced(data);
    throw err;
  } finally {
    inFlightSyncs.delete(lockKey);
  }
}

async function connectCalendarIcsForTenant(tenant, { provider, url, privacyMode, syncMinutes, triggerInitialSync = true }) {
  const cleanUrl = String(url || '').trim();
  if (!cleanUrl) throw new Error('Calendar feed URL is required');
  const normalizedSync = normalizeSyncMinutes(syncMinutes);
  const normalizedProvider = String(provider || 'other').toLowerCase();
  if (!['timetree', 'google', 'outlook', 'calendly', 'other'].includes(normalizedProvider)) {
    throw new Error('Invalid provider');
  }

  await testCalendarIcsUrl({ url: cleanUrl, privacyMode: privacyMode === true });

  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const previous = getCalendarIcsConfig(account);
  account.integrations = account.integrations || {};
  account.integrations.calendarIcs = {
    ...getCalendarIcsConfig(account),
    enabled: true,
    provider: normalizedProvider,
    url: cleanUrl,
    privacyMode: privacyMode === true,
    syncMinutes: normalizedSync,
    lastSyncStatus: null,
    lastSyncError: null,
    importedCountLast: Number(account?.integrations?.calendarIcs?.importedCountLast || 0),
    lastSyncAttemptAt: null
  };
  const isUpdate = previous.enabled === true;
  appendIntegrationLog(account, isUpdate ? 'calendar.update' : 'calendar.connect', isUpdate ? 'Calendar settings updated' : 'Calendar connected');
  saveDataDebounced(data);

  let syncResult = null;
  if (triggerInitialSync === true) {
    syncResult = await syncCalendarIcsForTenant(tenant, { reason: isUpdate ? 'update' : 'connect' });
  }
  return {
    ok: true,
    calendarIcs: getCalendarIcsConfig(account),
    sync: syncResult
  };
}

function disconnectCalendarIcsForTenant(tenant) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const existing = getCalendarIcsConfig(account);

  account.integrations = account.integrations || {};
  account.integrations.calendarIcs = {
    enabled: false,
    provider: existing.provider || 'other',
    url: '',
    privacyMode: true,
    syncMinutes: 60,
    lastSyncedAt: existing.lastSyncedAt,
    lastSyncStatus: null,
    lastSyncError: null,
    importedCountLast: 0,
    lastSyncAttemptAt: Date.now()
  };
  account.calendarEvents = (Array.isArray(account.calendarEvents) ? account.calendarEvents : [])
    .filter((ev) => String(ev?.source || '') !== 'ics');

  appendIntegrationLog(account, 'calendar.disconnect', 'Calendar disconnected');
  saveDataDebounced(data);
  return { ok: true };
}

async function connectCalendarTwoWayForTenant(tenant, { provider, calendarId, syncMinutes, accessToken, refreshToken, triggerInitialSync = true } = {}) {
  const source = providerSource(provider);
  const normalizedSync = normalizeTwoWaySyncMinutes(syncMinutes);
  const resolvedCalendarId = String(calendarId || 'primary').trim() || 'primary';
  const token = String(accessToken || '').trim();
  if (!token) {
    throw new Error('accessToken is required');
  }

  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const providers = ensureCalendarProviders(account);
  const existing = providers[source] || defaultTwoWayProviderConfig(source);
  const now = Date.now();
  providers[source] = {
    ...defaultTwoWayProviderConfig(source),
    ...existing,
    enabled: true,
    provider: source,
    mode: 'two_way',
    calendarId: resolvedCalendarId,
    syncMinutes: normalizedSync,
    accessToken: token,
    accessTokenMasked: maskSecret(token),
    refreshToken: String(refreshToken || existing.refreshToken || ''),
    hasRefreshToken: Boolean(String(refreshToken || existing.refreshToken || '').trim()),
    connectedAt: existing.connectedAt || now,
    lastSyncStatus: null,
    lastSyncError: null,
    lastSyncAttemptAt: null,
    remoteEvents: Array.isArray(existing.remoteEvents) ? existing.remoteEvents : []
  };
  appendIntegrationLog(account, 'calendar.connect', `${source} calendar connected (two-way)`);
  saveDataDebounced(data);

  let sync = null;
  if (triggerInitialSync === true) {
    sync = await syncCalendarTwoWayForTenant(tenant, { provider: source, reason: 'connect' });
  }
  return { ok: true, provider: getCalendarProviderConfig(account, source), sync };
}

async function syncCalendarTwoWayForTenant(tenant, { provider, reason = 'manual' } = {}) {
  const source = providerSource(provider);
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const providers = ensureCalendarProviders(account);
  const cfg = providers[source];
  if (!cfg || cfg.enabled !== true) {
    throw new Error(`${source} two-way calendar is not connected`);
  }

  const lockKey = `${tenant.accountId}::${tenant.to}::${source}`;
  if (inFlightSyncs.has(lockKey)) {
    throw new Error('A sync is already in progress');
  }
  inFlightSyncs.add(lockKey);

  try {
    const remoteEvents = Array.isArray(cfg.remoteEvents) ? cfg.remoteEvents : [];
    const importedEvents = remoteEvents
      .filter((ev) => Number.isFinite(Number(ev?.start)) && Number.isFinite(Number(ev?.end)))
      .map((ev) => buildProviderEventRecord(source, ev))
      .slice(-MAX_EVENTS_STORED);

    account.calendarEvents = Array.isArray(account.calendarEvents) ? account.calendarEvents : [];
    const nonProviderEvents = account.calendarEvents.filter((ev) => String(ev?.source || '') !== source);
    account.calendarEvents = nonProviderEvents.concat(importedEvents).slice(-2000);

    providers[source] = {
      ...cfg,
      enabled: true,
      provider: source,
      mode: 'two_way',
      syncMinutes: normalizeTwoWaySyncMinutes(cfg.syncMinutes),
      lastSyncedAt: Date.now(),
      lastSyncStatus: 'ok',
      lastSyncError: null,
      importedCountLast: importedEvents.length,
      lastSyncAttemptAt: Date.now()
    };
    appendIntegrationLog(account, 'calendar.sync', `${source} sync completed (${importedEvents.length} events imported)`);
    saveDataDebounced(data);
    return {
      ok: true,
      provider: source,
      importedCount: importedEvents.length,
      lastSyncedAt: providers[source].lastSyncedAt,
      status: providers[source].lastSyncStatus,
      reason
    };
  } catch (err) {
    providers[source] = {
      ...cfg,
      enabled: true,
      provider: source,
      mode: 'two_way',
      syncMinutes: normalizeTwoWaySyncMinutes(cfg.syncMinutes),
      lastSyncStatus: 'error',
      lastSyncError: err?.message || 'Sync failed',
      lastSyncAttemptAt: Date.now()
    };
    appendIntegrationLog(account, 'calendar.error', `${source} sync failed (${err?.message || 'unknown error'})`);
    saveDataDebounced(data);
    throw err;
  } finally {
    inFlightSyncs.delete(lockKey);
  }
}

function normalizeTwoWayEventInput(event) {
  const src = event && typeof event === 'object' ? event : {};
  const title = String(src.title || src.summary || '').trim();
  const start = Number(src.start);
  const end = Number(src.end);
  if (!title) throw new Error('event.title is required');
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
    throw new Error('event start/end are invalid');
  }
  return {
    remoteId: String(src.remoteId || src.id || '').trim(),
    title,
    summary: title,
    start,
    end,
    location: String(src.location || '').trim()
  };
}

function pushCalendarTwoWayEventForTenant(tenant, { provider, event } = {}) {
  const source = providerSource(provider);
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const providers = ensureCalendarProviders(account);
  const cfg = providers[source];
  if (!cfg || cfg.enabled !== true) {
    throw new Error(`${source} two-way calendar is not connected`);
  }

  const normalized = normalizeTwoWayEventInput(event);
  const remoteId = normalized.remoteId || `${source}_${crypto.randomUUID()}`;
  const remoteEvents = Array.isArray(cfg.remoteEvents) ? cfg.remoteEvents : [];
  const idx = remoteEvents.findIndex((ev) => String(ev?.remoteId || ev?.id || '') === remoteId);
  const nextRemote = {
    remoteId,
    title: normalized.title,
    summary: normalized.summary,
    start: normalized.start,
    end: normalized.end,
    location: normalized.location,
    updatedAt: Date.now()
  };
  if (idx >= 0) remoteEvents[idx] = nextRemote;
  else remoteEvents.push(nextRemote);

  const eventRecord = buildProviderEventRecord(source, { ...nextRemote, id: remoteId });
  account.calendarEvents = Array.isArray(account.calendarEvents) ? account.calendarEvents : [];
  const existingIdx = account.calendarEvents.findIndex((ev) => String(ev?.providerEventId || '') === String(eventRecord.providerEventId) && String(ev?.source || '') === source);
  if (existingIdx >= 0) account.calendarEvents[existingIdx] = eventRecord;
  else account.calendarEvents.push(eventRecord);
  account.calendarEvents = account.calendarEvents.slice(-2000);

  providers[source] = {
    ...cfg,
    enabled: true,
    provider: source,
    mode: 'two_way',
    remoteEvents: remoteEvents.slice(-MAX_EVENTS_STORED),
    pushedCountLast: Number(cfg.pushedCountLast || 0) + 1,
    lastSyncStatus: 'ok',
    lastSyncError: null,
    lastSyncAttemptAt: Date.now()
  };
  appendIntegrationLog(account, 'calendar.push', `${source} event pushed (${normalized.title})`);
  saveDataDebounced(data);
  return { ok: true, provider: source, event: eventRecord };
}

function disconnectCalendarTwoWayForTenant(tenant, { provider } = {}) {
  const source = providerSource(provider);
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const providers = ensureCalendarProviders(account);
  const existing = providers[source] || defaultTwoWayProviderConfig(source);

  providers[source] = {
    ...defaultTwoWayProviderConfig(source),
    provider: source,
    syncMinutes: normalizeTwoWaySyncMinutes(existing.syncMinutes),
    lastSyncedAt: existing.lastSyncedAt || null,
    lastSyncAttemptAt: Date.now()
  };
  account.calendarEvents = (Array.isArray(account.calendarEvents) ? account.calendarEvents : [])
    .filter((ev) => String(ev?.source || '') !== source);
  appendIntegrationLog(account, 'calendar.disconnect', `${source} calendar disconnected`);
  saveDataDebounced(data);
  return { ok: true, provider: source };
}

function buildBookingEventPayload({ title, start, end, location, remoteId } = {}) {
  const startMs = Number(start);
  const endMs = Number(end);
  const now = Date.now();
  const resolvedStart = Number.isFinite(startMs) ? startMs : now;
  const resolvedEnd = Number.isFinite(endMs) && endMs > resolvedStart ? endMs : (resolvedStart + 60 * 60 * 1000);
  return {
    id: remoteId,
    title: String(title || 'Booked Appointment'),
    start: resolvedStart,
    end: resolvedEnd,
    location: String(location || '')
  };
}

function pushBookingToConnectedCalendars(tenant, booking = {}) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  const providers = ensureCalendarProviders(account);
  const results = [];
  const failures = [];

  for (const provider of TWO_WAY_PROVIDERS) {
    const cfg = providers[provider];
    if (!cfg || cfg.enabled !== true) continue;
    const event = buildBookingEventPayload({
      title: booking.title,
      start: booking.start,
      end: booking.end,
      location: booking.location,
      remoteId: booking.remoteId ? `${provider}_${booking.remoteId}` : ''
    });
    try {
      const pushed = pushCalendarTwoWayEventForTenant(tenant, { provider, event });
      results.push({ provider, ok: true, eventId: pushed?.event?.id || null });
    } catch (err) {
      failures.push({ provider, error: err?.message || 'push failed' });
    }
  }
  return { ok: failures.length === 0, pushed: results, failures };
}

function getTenantIntegrationSnapshot(tenant) {
  const data = loadData();
  const { account } = accountByTenant(data, tenant);
  ensureCalendarProviders(account);
  const logs = (Array.isArray(account.integrationLogs) ? account.integrationLogs : []).slice(0, 10);
  const imported = (Array.isArray(account.calendarEvents) ? account.calendarEvents : [])
    .filter((ev) => ['ics', 'google', 'outlook'].includes(String(ev?.source || '')))
    .sort((a, b) => Number(b?.start || 0) - Number(a?.start || 0))
    .slice(0, 20);
  return {
    calendarIcs: getCalendarIcsConfig(account),
    calendarProviders: {
      google: getCalendarProviderConfig(account, 'google'),
      outlook: getCalendarProviderConfig(account, 'outlook')
    },
    integrationLogs: logs,
    importedEvents: imported
  };
}

async function runScheduledCalendarIcsSyncs() {
  const data = loadData();
  const accounts = Object.entries(data.accounts || {});
  const now = Date.now();
  for (const [to, account] of accounts) {
    if (!account || typeof account !== 'object') continue;
    const accountId = String(account.accountId || account.id || '');
    if (!accountId) continue;
    const cfg = getCalendarIcsConfig(account);
    if (!cfg.enabled || !cfg.url) continue;
    if (cfg.syncMinutes <= 0) continue; // manual only

    const last = Number(cfg.lastSyncedAt || cfg.lastSyncAttemptAt || 0);
    if (last && (now - last) < (cfg.syncMinutes * 60 * 1000)) continue;
    try {
      await syncCalendarIcsForTenant({ accountId, to }, { reason: 'scheduled' });
    } catch {
      // Error already persisted in sync helper; keep scheduler resilient.
    }
  }
}

async function runScheduledCalendarTwoWaySyncs() {
  const data = loadData();
  const accounts = Object.entries(data.accounts || {});
  const now = Date.now();
  for (const [to, account] of accounts) {
    if (!account || typeof account !== 'object') continue;
    const accountId = String(account.accountId || account.id || '');
    if (!accountId) continue;
    const providers = ensureCalendarProviders(account);
    for (const provider of TWO_WAY_PROVIDERS) {
      const cfg = providers[provider];
      if (!cfg || cfg.enabled !== true) continue;
      const syncMinutes = normalizeTwoWaySyncMinutes(cfg.syncMinutes);
      if (syncMinutes <= 0) continue;
      const last = Number(cfg.lastSyncedAt || cfg.lastSyncAttemptAt || 0);
      if (last && (now - last) < (syncMinutes * 60 * 1000)) continue;
      try {
        await syncCalendarTwoWayForTenant({ accountId, to }, { provider, reason: 'scheduled' });
      } catch {
        // Sync helper already persists errors; keep scheduler resilient.
      }
    }
  }
}

module.exports = {
  normalizeSyncMinutes,
  normalizeTwoWaySyncMinutes,
  getCalendarIcsConfig,
  getCalendarProviderConfig,
  testCalendarIcsUrl,
  createCalendarOAuthStartForTenant,
  completeCalendarOAuthCallback,
  buildOAuthCallbackHtml,
  connectCalendarIcsForTenant,
  syncCalendarIcsForTenant,
  disconnectCalendarIcsForTenant,
  connectCalendarTwoWayForTenant,
  syncCalendarTwoWayForTenant,
  pushCalendarTwoWayEventForTenant,
  pushBookingToConnectedCalendars,
  disconnectCalendarTwoWayForTenant,
  getTenantIntegrationSnapshot,
  runScheduledCalendarIcsSyncs,
  runScheduledCalendarTwoWaySyncs
};
