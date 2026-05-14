const crypto = require('crypto');
const { DEV_MODE, NODE_ENV, COOKIE_SECURE, COOKIE_SAMESITE } = require('../config/runtime');
const {
  loadData,
  saveDataDebounced,
  flushDataNow,
  getAccountById
} = require('../store/dataStore');

const SESSION_COOKIE = 'relay_sid';
const CSRF_COOKIE = 'relay_csrf';
const ADMIN_UNLOCK_COOKIE = 'relay_admin_unlock';
const SESSION_TTL_MS = 1000 * 60 * 60 * 12; // 12h
const SESSION_TTL_PERSIST_MS = 1000 * 60 * 60 * 24 * 30; // 30d
const ADMIN_UNLOCK_TTL_MS = 1000 * 60 * 60 * 8; // 8h
const AUTH_SECRET = String(process.env.AUTH_SECRET || '').trim();
const ROLE_SET = new Set(['superadmin', 'owner', 'admin', 'agent', 'readonly']);
const IS_PROD = NODE_ENV === 'production';

if (!AUTH_SECRET) {
  if (IS_PROD) {
    throw new Error('AUTH_SECRET is required in production');
  }
  console.warn('[auth] AUTH_SECRET is missing. Set AUTH_SECRET even in development to avoid session invalidation.');
}

function normalizeEmail(email) {
  return String(email || '').trim().toLowerCase();
}

function normalizeRole(role) {
  const v = String(role || '').trim().toLowerCase();
  return ROLE_SET.has(v) ? v : null;
}

function randomId() {
  if (crypto.randomUUID) return crypto.randomUUID();
  return crypto.randomBytes(16).toString('hex');
}

function hmac(value) {
  return crypto.createHmac('sha256', AUTH_SECRET).update(String(value)).digest('hex');
}

function safeBase64UrlEncode(input) {
  return Buffer.from(String(input || ''), 'utf8').toString('base64url');
}

function safeBase64UrlDecode(input) {
  try {
    return Buffer.from(String(input || ''), 'base64url').toString('utf8');
  } catch {
    return '';
  }
}

function unlockSig(payloadB64) {
  return hmac(`admin_unlock:${String(payloadB64 || '')}`);
}

function hashPassword(password) {
  const plain = String(password || '');
  if (!plain) throw new Error('Password is required');
  const salt = crypto.randomBytes(16).toString('hex');
  const digest = crypto.scryptSync(plain, salt, 64).toString('hex');
  return `scrypt$${salt}$${digest}`;
}

function verifyPassword(password, passwordHash) {
  const raw = String(passwordHash || '');
  const parts = raw.split('$');
  if (parts.length !== 3 || parts[0] !== 'scrypt') return false;
  const salt = parts[1];
  const expected = parts[2];
  const actual = crypto.scryptSync(String(password || ''), salt, 64).toString('hex');
  const expectedBuf = Buffer.from(expected, 'hex');
  const actualBuf = Buffer.from(actual, 'hex');
  if (expectedBuf.length !== actualBuf.length) return false;
  return crypto.timingSafeEqual(expectedBuf, actualBuf);
}

function sanitizeUser(user) {
  if (!user || typeof user !== 'object') return null;
  return {
    id: String(user.id || ''),
    name: String(user.name || '').trim(),
    email: normalizeEmail(user.email),
    role: normalizeRole(user.role) || 'readonly',
    accountIds: Array.isArray(user.accountIds) ? user.accountIds.map((x) => String(x)).filter(Boolean) : [],
    developerAccess: user.developerAccess === true,
    createdAt: Number(user.createdAt || 0),
    lastLoginAt: user.lastLoginAt ? Number(user.lastLoginAt) : null,
    disabled: user.disabled === true
  };
}

function hasDeveloperAccess(user) {
  const safeUser = sanitizeUser(user);
  if (!safeUser || safeUser.disabled === true) return false;
  if (safeUser.role === 'superadmin') return true;
  return safeUser.developerAccess === true;
}

function canUserAccessAccount(user, accountId) {
  if (!user || !accountId) return false;
  const role = normalizeRole(user.role);
  if (role === 'superadmin') return true;
  const allowed = Array.isArray(user.accountIds) ? user.accountIds.map(String) : [];
  return allowed.includes(String(accountId));
}

function ensureAuthCollections(data) {
  if (!Array.isArray(data.users)) data.users = [];
  if (!data.sessions || typeof data.sessions !== 'object' || Array.isArray(data.sessions)) data.sessions = {};
  return data;
}

function issueSessionForUser(userId, { persist = false } = {}) {
  const data = ensureAuthCollections(loadData());
  const sid = randomId();
  const csrfToken = randomId();
  const now = Date.now();
  const ttl = persist ? SESSION_TTL_PERSIST_MS : SESSION_TTL_MS;
  data.sessions[sid] = {
    userId: String(userId || ''),
    createdAt: now,
    expiresAt: now + ttl,
    csrfToken
  };
  saveDataDebounced(data);
  const signature = hmac(sid);
  return {
    token: `${sid}.${signature}`,
    maxAgeMs: ttl,
    csrfToken
  };
}

function parseSessionToken(token) {
  const raw = String(token || '').trim();
  if (!raw || !raw.includes('.')) return null;
  const [sid, sig] = raw.split('.', 2);
  if (!sid || !sig) return null;
  const expectedSig = hmac(sid);
  const a = Buffer.from(sig);
  const b = Buffer.from(expectedSig);
  if (a.length !== b.length) return null;
  if (!crypto.timingSafeEqual(a, b)) return null;
  return { sid };
}

function destroySessionToken(token) {
  const parsed = parseSessionToken(token);
  if (!parsed?.sid) return;
  const data = ensureAuthCollections(loadData());
  if (data.sessions[parsed.sid]) {
    delete data.sessions[parsed.sid];
    saveDataDebounced(data);
  }
}

function destroySessionsForUser(userId) {
  const targetUserId = String(userId || '').trim();
  if (!targetUserId) return 0;
  const data = ensureAuthCollections(loadData());
  let removed = 0;
  for (const [sid, session] of Object.entries(data.sessions || {})) {
    if (String(session?.userId || '') !== targetUserId) continue;
    delete data.sessions[sid];
    removed += 1;
  }
  if (removed > 0) saveDataDebounced(data);
  return removed;
}

function getUserFromSessionToken(token) {
  const parsed = parseSessionToken(token);
  if (!parsed?.sid) return null;
  const data = ensureAuthCollections(loadData());
  const session = data.sessions[parsed.sid];
  if (!session) return null;
  if (Number(session.expiresAt || 0) <= Date.now()) {
    delete data.sessions[parsed.sid];
    saveDataDebounced(data);
    return null;
  }
  const user = data.users.find((u) => String(u.id || '') === String(session.userId || ''));
  if (!user || user.disabled === true) {
    return null;
  }
  return sanitizeUser(user);
}

function sessionCookie(token, maxAgeMs) {
  const maxAgeSeconds = Math.max(1, Math.floor(Number(maxAgeMs || 0) / 1000));
  const secure = COOKIE_SECURE ? '; Secure' : '';
  const sameSite = COOKIE_SAMESITE || 'Lax';
  return `${SESSION_COOKIE}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=${sameSite}; Max-Age=${maxAgeSeconds}${secure}`;
}

function csrfCookie(token, maxAgeMs) {
  const maxAgeSeconds = Math.max(1, Math.floor(Number(maxAgeMs || 0) / 1000));
  const secure = COOKIE_SECURE ? '; Secure' : '';
  const sameSite = COOKIE_SAMESITE || 'Lax';
  return `${CSRF_COOKIE}=${encodeURIComponent(String(token || ''))}; Path=/; SameSite=${sameSite}; Max-Age=${maxAgeSeconds}${secure}`;
}

function clearSessionCookie() {
  const secure = COOKIE_SECURE ? '; Secure' : '';
  const sameSite = COOKIE_SAMESITE || 'Lax';
  return `${SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=${sameSite}; Max-Age=0${secure}`;
}

function clearCsrfCookie() {
  const secure = COOKIE_SECURE ? '; Secure' : '';
  const sameSite = COOKIE_SAMESITE || 'Lax';
  return `${CSRF_COOKIE}=; Path=/; SameSite=${sameSite}; Max-Age=0${secure}`;
}

function issueAdminUnlockToken({ userId, accountId, ttlMs = ADMIN_UNLOCK_TTL_MS } = {}) {
  const uid = String(userId || '').trim();
  const aid = String(accountId || '').trim();
  const ttl = Math.max(60_000, Number(ttlMs || ADMIN_UNLOCK_TTL_MS) || ADMIN_UNLOCK_TTL_MS);
  if (!uid || !aid) return null;
  const now = Date.now();
  const payload = {
    uid,
    aid,
    iat: now,
    exp: now + ttl,
    nonce: randomId()
  };
  const payloadB64 = safeBase64UrlEncode(JSON.stringify(payload));
  const sig = unlockSig(payloadB64);
  return {
    token: `${payloadB64}.${sig}`,
    maxAgeMs: ttl,
    expiresAt: payload.exp
  };
}

function parseAdminUnlockToken(token) {
  const raw = String(token || '').trim();
  if (!raw || !raw.includes('.')) return null;
  const [payloadB64, sig] = raw.split('.', 2);
  if (!payloadB64 || !sig) return null;
  const expectedSig = unlockSig(payloadB64);
  const a = Buffer.from(sig);
  const b = Buffer.from(expectedSig);
  if (a.length !== b.length) return null;
  if (!crypto.timingSafeEqual(a, b)) return null;
  const payloadRaw = safeBase64UrlDecode(payloadB64);
  if (!payloadRaw) return null;
  let payload = null;
  try {
    payload = JSON.parse(payloadRaw);
  } catch {
    return null;
  }
  if (!payload || typeof payload !== 'object') return null;
  const uid = String(payload.uid || '').trim();
  const aid = String(payload.aid || '').trim();
  const exp = Number(payload.exp || 0);
  if (!uid || !aid || !Number.isFinite(exp) || exp <= Date.now()) return null;
  return { uid, aid, exp };
}

function verifyAdminUnlockToken(token, { userId, accountId } = {}) {
  const parsed = parseAdminUnlockToken(token);
  if (!parsed) return { ok: false, reason: 'invalid_or_expired' };
  const uid = String(userId || '').trim();
  const aid = String(accountId || '').trim();
  if (!uid || !aid) return { ok: false, reason: 'missing_context' };
  if (parsed.uid !== uid || parsed.aid !== aid) return { ok: false, reason: 'context_mismatch' };
  return { ok: true, payload: parsed };
}

function adminUnlockCookie(token, maxAgeMs) {
  const maxAgeSeconds = Math.max(1, Math.floor(Number(maxAgeMs || 0) / 1000));
  const secure = COOKIE_SECURE ? '; Secure' : '';
  const sameSite = COOKIE_SAMESITE || 'Lax';
  return `${ADMIN_UNLOCK_COOKIE}=${encodeURIComponent(String(token || ''))}; Path=/; HttpOnly; SameSite=${sameSite}; Max-Age=${maxAgeSeconds}${secure}`;
}

function clearAdminUnlockCookie() {
  const secure = COOKIE_SECURE ? '; Secure' : '';
  const sameSite = COOKIE_SAMESITE || 'Lax';
  return `${ADMIN_UNLOCK_COOKIE}=; Path=/; HttpOnly; SameSite=${sameSite}; Max-Age=0${secure}`;
}

function getCsrfTokenForSessionToken(token) {
  const parsed = parseSessionToken(token);
  if (!parsed?.sid) return null;
  const data = ensureAuthCollections(loadData());
  const session = data.sessions[parsed.sid];
  if (!session) return null;
  if (Number(session.expiresAt || 0) <= Date.now()) return null;
  return String(session.csrfToken || '').trim() || null;
}

function getAllowedAccountsForUser(user) {
  const safeUser = sanitizeUser(user);
  if (!safeUser) return [];
  const data = loadData();
  const entries = Object.entries(data.accounts || {});

  if (safeUser.role === 'superadmin') {
    return entries.map(([to, account]) => ({
      to: String(to),
      accountId: String(account?.accountId || account?.id || ''),
      businessName: String(account?.businessName || account?.workspace?.identity?.businessName || '').trim()
    })).filter((a) => a.accountId);
  }

  const out = [];
  for (const accountId of safeUser.accountIds) {
    const found = getAccountById(data, accountId);
    if (!found?.account) continue;
    out.push({
      to: String(found.to || found.account.to || ''),
      accountId: String(found.account.accountId || found.account.id || ''),
      businessName: String(found.account.businessName || found.account.workspace?.identity?.businessName || '').trim()
    });
  }
  return out.filter((a) => a.accountId && a.to);
}

function ensureDefaultSuperadminUser() {
  if (!DEV_MODE) return;
  const allowBootstrap = String(process.env.ALLOW_DEFAULT_SUPERADMIN || '').trim().toLowerCase() === 'true';
  if (!allowBootstrap) return;

  const data = ensureAuthCollections(loadData());
  const email = normalizeEmail(process.env.DEV_DEFAULT_SUPERADMIN_EMAIL || '');
  const password = String(process.env.DEV_DEFAULT_SUPERADMIN_PASSWORD || '');
  if (!email || !password) {
    console.warn('[auth] ALLOW_DEFAULT_SUPERADMIN=true but DEV_DEFAULT_SUPERADMIN_EMAIL/PASSWORD is missing; skipping bootstrap.');
    return;
  }
  const superadmins = data.users.filter((u) => normalizeRole(u.role) === 'superadmin');

  let changed = false;
  if (!superadmins.length) {
    const user = {
      id: randomId(),
      email,
      passwordHash: hashPassword(password),
      role: 'superadmin',
      accountIds: [],
      createdAt: Date.now(),
      lastLoginAt: null,
      disabled: false
    };
    data.users.push(user);
    changed = true;
    console.log(`[auth] DEV default superadmin created: ${email} / ${password}`);
  } else {
    for (const user of superadmins) {
      if (verifyPassword(password, user.passwordHash)) continue;
      user.passwordHash = hashPassword(password);
      changed = true;
    }
    if (changed) {
      console.log('[auth] DEV superadmin password reset to configured default');
    }
  }

  if (changed) {
    saveDataDebounced(data);
    flushDataNow();
  }
}

function isStrongBootstrapPassword(password) {
  const value = String(password || '');
  if (value.length < 12) return false;
  if (!/[A-Z]/.test(value)) return false;
  if (!/[a-z]/.test(value)) return false;
  if (!/[0-9]/.test(value)) return false;
  if (!/[^A-Za-z0-9]/.test(value)) return false;
  return true;
}

function ensureBootstrapBilling(account) {
  const now = Date.now();
  account.billing = account.billing && typeof account.billing === 'object' ? account.billing : {};
  account.billing.provider = account.billing.provider || 'bootstrap';
  account.billing.isLive = true;
  account.billing.updatedAt = now;
  account.billing.plan = account.billing.plan && typeof account.billing.plan === 'object' ? account.billing.plan : {};
  account.billing.plan.key = account.billing.plan.key || 'pilot';
  account.billing.plan.status = 'active';
  account.billing.plan.nextBillingAt = account.billing.plan.nextBillingAt || now + (30 * 24 * 60 * 60 * 1000);
  account.billing.dunning = account.billing.dunning && typeof account.billing.dunning === 'object' ? account.billing.dunning : {};
  account.billing.dunning.lockedAt = null;
  account.billing.dunning.graceEndsAt = null;
}

function shouldGrantBootstrapDeveloperAccess(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return false;
  const bootstrapOptOut = String(process.env.BOOTSTRAP_OWNER_DEVELOPER_ACCESS || '').trim().toLowerCase();
  if (bootstrapOptOut === 'false' || bootstrapOptOut === '0' || bootstrapOptOut === 'no' || bootstrapOptOut === 'off') {
    return false;
  }
  const configured = String(process.env.DEVELOPER_ACCESS_EMAILS || '')
    .split(',')
    .map((entry) => normalizeEmail(entry))
    .filter(Boolean);
  if (configured.length > 0) return configured.includes(normalized);
  return normalizeEmail(process.env.BOOTSTRAP_OWNER_EMAIL || '') === normalized;
}

function accountLooksLikeArcRelayOwnerAccount(account) {
  const names = [
    account?.businessName,
    account?.workspace?.identity?.businessName,
    account?.workspace?.businessName
  ].map((value) => String(value || '').trim().toLowerCase());
  return names.includes('arc relay') || names.includes('arcrelay');
}

function ensureDeveloperAccessUsers() {
  const data = ensureAuthCollections(loadData());
  const configuredEmails = new Set(
    [
      ...String(process.env.DEVELOPER_ACCESS_EMAILS || '').split(','),
      process.env.BOOTSTRAP_OWNER_EMAIL
    ]
      .map((entry) => normalizeEmail(entry))
      .filter(Boolean)
  );
  const allowArcRelayOwner = String(process.env.ARC_RELAY_OWNER_DEVELOPER_ACCESS || '').trim().toLowerCase() !== 'false';
  let changed = false;

  for (const user of data.users) {
    const email = normalizeEmail(user?.email);
    const role = normalizeRole(user?.role);
    let shouldGrant = configuredEmails.has(email);
    if (!shouldGrant && allowArcRelayOwner && role === 'owner') {
      const accountIds = Array.isArray(user.accountIds) ? user.accountIds : [];
      shouldGrant = accountIds.some((accountId) => {
        const found = getAccountById(data, accountId);
        return accountLooksLikeArcRelayOwnerAccount(found?.account);
      });
    }
    if (shouldGrant && user.developerAccess !== true) {
      user.developerAccess = true;
      changed = true;
    }
  }

  if (changed) {
    saveDataDebounced(data);
    flushDataNow();
    console.warn('[auth] developer access ensured for configured Arc Relay owner user(s).');
  }
}

function ensureBootstrapOwnerUser() {
  const email = normalizeEmail(process.env.BOOTSTRAP_OWNER_EMAIL || '');
  const password = String(process.env.BOOTSTRAP_OWNER_PASSWORD || '');
  if (!email && !password) return;
  if (!email || !password) {
    throw new Error('BOOTSTRAP_OWNER_EMAIL and BOOTSTRAP_OWNER_PASSWORD must be set together');
  }
  if (!isStrongBootstrapPassword(password)) {
    throw new Error('BOOTSTRAP_OWNER_PASSWORD must be 12+ chars with uppercase, lowercase, number, and symbol');
  }

  const data = ensureAuthCollections(loadData());
  const to = String(process.env.BOOTSTRAP_OWNER_TO || '+18145550001').trim();
  const accountId = String(process.env.BOOTSTRAP_OWNER_ACCOUNT_ID || `acct_${to.replace(/[^\d]/g, '') || 'bootstrap'}`).trim();
  const accountRef = getAccountById(data, accountId);
  let account = accountRef?.account || null;
  if (!account) {
    data.accounts = data.accounts && typeof data.accounts === 'object' ? data.accounts : {};
    account = data.accounts[to] || { to };
    data.accounts[to] = account;
  }
  account.to = account.to || to;
  account.id = accountId;
  account.accountId = accountId;
  account.businessName = account.businessName || process.env.BOOTSTRAP_BUSINESS_NAME || 'Arc Relay';
  ensureBootstrapBilling(account);

  const existing = data.users.find((u) => normalizeEmail(u?.email) === email);
  if (existing) {
    existing.passwordHash = hashPassword(password);
    existing.role = 'owner';
    existing.accountIds = Array.from(new Set([...(Array.isArray(existing.accountIds) ? existing.accountIds : []), accountId]));
    if (shouldGrantBootstrapDeveloperAccess(email)) existing.developerAccess = true;
    existing.disabled = false;
  } else {
    data.users.push({
      id: randomId(),
      name: String(process.env.BOOTSTRAP_OWNER_NAME || 'Owner').trim(),
      email,
      passwordHash: hashPassword(password),
      role: 'owner',
      accountIds: [accountId],
      developerAccess: shouldGrantBootstrapDeveloperAccess(email),
      createdAt: Date.now(),
      lastLoginAt: null,
      disabled: false
    });
  }
  saveDataDebounced(data);
  flushDataNow();
  console.warn('[auth] bootstrap owner ensured from env; remove BOOTSTRAP_OWNER_* after first login.');
}

module.exports = {
  SESSION_COOKIE,
  CSRF_COOKIE,
  ADMIN_UNLOCK_COOKIE,
  ROLE_SET,
  normalizeEmail,
  normalizeRole,
  hashPassword,
  verifyPassword,
  sanitizeUser,
  canUserAccessAccount,
  hasDeveloperAccess,
  ensureDeveloperAccessUsers,
  issueSessionForUser,
  parseSessionToken,
  destroySessionToken,
  destroySessionsForUser,
  getUserFromSessionToken,
  getCsrfTokenForSessionToken,
  issueAdminUnlockToken,
  parseAdminUnlockToken,
  verifyAdminUnlockToken,
  adminUnlockCookie,
  clearAdminUnlockCookie,
  sessionCookie,
  csrfCookie,
  clearSessionCookie,
  clearCsrfCookie,
  getAllowedAccountsForUser,
  ensureDefaultSuperadminUser,
  ensureBootstrapOwnerUser
};
