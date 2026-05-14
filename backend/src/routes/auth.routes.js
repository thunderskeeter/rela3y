const crypto = require('crypto');
const express = require('express');
const { loadData, saveDataDebounced, getAccountById } = require('../store/dataStore');
const { DEV_MODE, COOKIE_SECURE, COOKIE_SAMESITE } = require('../config/runtime');
const {
  normalizeEmail,
  verifyPassword,
  sanitizeUser,
  issueSessionForUser,
  destroySessionToken,
  sessionCookie,
  csrfCookie,
  clearSessionCookie,
  clearCsrfCookie,
  getAllowedAccountsForUser
} = require('../utils/auth');
const { z, validateBody, validateQuery } = require('../utils/validate');
const { requireAuth, resolveSessionToken, attachUserFromSession, parseCookies } = require('../utils/authMiddleware');
const { canAccountAccessProduct, computeBillingLockState } = require('../services/billingPolicyService');

const authRouter = express.Router();
const QR_REQUEST_COOKIE = 'relay_qr_req';
const QR_SESSION_TTL_MS = 1000 * 60 * 3;
const loginSchema = z.object({
  email: z.string().trim().min(3).max(254),
  password: z.string().min(1).max(256),
  persist: z.boolean().optional()
});
const noBodySchema = z.object({}).strict().optional().default({});
const qrTokenQuerySchema = z.object({
  token: z.string().trim().min(12).max(256)
});
const qrApproveSchema = z.object({
  token: z.string().trim().min(12).max(256),
  email: z.string().trim().min(3).max(254).optional(),
  password: z.string().min(1).max(256).optional(),
  persist: z.boolean().optional()
});

function randomToken(size = 24) {
  return crypto.randomBytes(size).toString('base64url');
}

function authPayloadForUser(user) {
  const safe = sanitizeUser(user);
  return {
    user: safe,
    accounts: getAllowedAccountsForUser(safe)
  };
}

function accountIsProvisionedForLogin(account) {
  if (DEV_MODE === true) return true;
  return canAccountAccessProduct(account, Date.now());
}

function userHasProvisionedAccess(data, user) {
  const role = String(user?.role || '').toLowerCase();
  if (role === 'superadmin') return true;
  const accountIds = Array.isArray(user?.accountIds) ? user.accountIds : [];
  if (!accountIds.length) return false;
  for (const accountId of accountIds) {
    const found = getAccountById(data, accountId);
    if (!found?.account) continue;
    if (accountIsProvisionedForLogin(found.account)) return true;
  }
  return false;
}

function getFirstBlockedReason(data, user) {
  const role = String(user?.role || '').toLowerCase();
  if (role === 'superadmin') return null;
  const accountIds = Array.isArray(user?.accountIds) ? user.accountIds : [];
  for (const accountId of accountIds) {
    const found = getAccountById(data, accountId);
    if (!found?.account) continue;
    const lock = computeBillingLockState(found.account, Date.now());
    if (lock.locked) return lock.reason || 'account_locked';
  }
  return null;
}

function ensureQrAuthSessions(data) {
  if (!data.qrAuthSessions || typeof data.qrAuthSessions !== 'object' || Array.isArray(data.qrAuthSessions)) {
    data.qrAuthSessions = {};
  }
  return data.qrAuthSessions;
}

function purgeExpiredQrAuthSessions(data) {
  const sessions = ensureQrAuthSessions(data);
  const now = Date.now();
  let changed = false;
  for (const [token, session] of Object.entries(sessions)) {
    const expiresAt = Number(session?.expiresAt || 0);
    if (expiresAt > now) continue;
    delete sessions[token];
    changed = true;
  }
  if (changed) saveDataDebounced(data);
}

function qrRequestCookie(token, maxAgeMs) {
  const maxAgeSeconds = Math.max(1, Math.floor(Number(maxAgeMs || 0) / 1000));
  const secure = COOKIE_SECURE ? '; Secure' : '';
  const sameSite = COOKIE_SAMESITE || 'Lax';
  return `${QR_REQUEST_COOKIE}=${encodeURIComponent(String(token || ''))}; Path=/; HttpOnly; SameSite=${sameSite}; Max-Age=${maxAgeSeconds}${secure}`;
}

function clearQrRequestCookie() {
  const secure = COOKIE_SECURE ? '; Secure' : '';
  const sameSite = COOKIE_SAMESITE || 'Lax';
  return `${QR_REQUEST_COOKIE}=; Path=/; HttpOnly; SameSite=${sameSite}; Max-Age=0${secure}`;
}

function getQrRequestCookie(req) {
  const cookies = parseCookies(req);
  return String(cookies[QR_REQUEST_COOKIE] || '').trim();
}

function getPublicAppOrigin(req) {
  const configured = String(process.env.PUBLIC_APP_ORIGIN || '').trim();
  if (configured) return configured.replace(/\/+$/, '');
  const protocol = String(req?.headers?.['x-forwarded-proto'] || req?.protocol || 'http').trim();
  const host = String(req?.headers?.['x-forwarded-host'] || req?.get?.('host') || '').trim();
  if (!host) return '';
  return `${protocol}://${host}`.replace(/\/+$/, '');
}

function authenticateUserCredentials(data, { email, password } = {}) {
  const normalized = normalizeEmail(email);
  const rawPassword = String(password || '');
  if (!normalized || !rawPassword) return { ok: false, status: 400, error: 'email and password are required' };
  const users = Array.isArray(data.users) ? data.users : [];
  const user = users.find((entry) => normalizeEmail(entry?.email) === normalized);
  if (!user || user.disabled === true) {
    return { ok: false, status: 401, error: 'Invalid credentials' };
  }
  if (!verifyPassword(rawPassword, user.passwordHash)) {
    return { ok: false, status: 401, error: 'Invalid credentials' };
  }
  if (!userHasProvisionedAccess(data, user)) {
    const reason = getFirstBlockedReason(data, user);
    if (reason) {
      return {
        ok: false,
        status: 403,
        error: 'Account access is locked due to billing status',
        reason
      };
    }
    return { ok: false, status: 403, error: 'Account access is pending payment verification' };
  }
  return { ok: true, user };
}

authRouter.post('/auth/login', validateBody(loginSchema), (req, res) => {
  const email = normalizeEmail(req?.body?.email);
  const password = String(req?.body?.password || '');
  const persist = req?.body?.persist === true;

  if (!email || !password) return res.status(400).json({ error: 'email and password are required' });

  const data = loadData();
  const check = authenticateUserCredentials(data, { email, password });
  if (!check.ok) {
    return res.status(check.status || 401).json({
      error: check.error || 'Invalid credentials',
      ...(check.reason ? { reason: check.reason } : {})
    });
  }
  const user = check.user;

  user.lastLoginAt = Date.now();
  saveDataDebounced(data);

  const session = issueSessionForUser(user.id, { persist });
  res.setHeader('Set-Cookie', [
    sessionCookie(session.token, session.maxAgeMs),
    csrfCookie(session.csrfToken, session.maxAgeMs)
  ]);
  return res.json({ ok: true, csrfToken: session.csrfToken, ...authPayloadForUser(user) });
});

authRouter.post('/auth/logout', validateBody(noBodySchema), (req, res) => {
  const token = resolveSessionToken(req);
  if (token) destroySessionToken(token);
  res.setHeader('Set-Cookie', [clearSessionCookie(), clearCsrfCookie(), clearQrRequestCookie()]);
  return res.json({ ok: true });
});

authRouter.get('/auth/me', requireAuth, (req, res) => {
  return res.json({ ok: true, ...authPayloadForUser(req.user) });
});

authRouter.post('/auth/qr/start', validateBody(noBodySchema), (req, res) => {
  const data = loadData();
  purgeExpiredQrAuthSessions(data);
  const sessions = ensureQrAuthSessions(data);
  const now = Date.now();
  const token = randomToken(18);
  const browserBinding = randomToken(18);
  const expiresAt = now + QR_SESSION_TTL_MS;
  const origin = getPublicAppOrigin(req);
  const approvalUrl = `${origin}/login/qr?token=${encodeURIComponent(token)}`;
  sessions[token] = {
    token,
    browserBinding,
    status: 'pending',
    createdAt: now,
    expiresAt,
    approvedAt: null,
    approvedByUserId: null,
    completedAt: null,
    desktopSessionToken: null,
    desktopCsrfToken: null,
    desktopSessionMaxAgeMs: null
  };
  saveDataDebounced(data);
  res.setHeader('Set-Cookie', qrRequestCookie(browserBinding, QR_SESSION_TTL_MS));
  return res.json({ ok: true, token, approvalUrl, expiresAt });
});

authRouter.get('/auth/qr/inspect', validateQuery(qrTokenQuerySchema), (req, res) => {
  const data = loadData();
  purgeExpiredQrAuthSessions(data);
  const sessions = ensureQrAuthSessions(data);
  const qrSession = sessions[String(req.query.token || '')];
  if (!qrSession) return res.status(404).json({ error: 'QR sign-in request not found or expired' });
  const user = attachUserFromSession(req);
  return res.json({
    ok: true,
    status: String(qrSession.status || 'pending'),
    expiresAt: Number(qrSession.expiresAt || 0),
    approvedAt: qrSession.approvedAt ? Number(qrSession.approvedAt) : null,
    currentUser: user ? sanitizeUser(user) : null
  });
});

authRouter.get('/auth/qr/status', validateQuery(qrTokenQuerySchema), (req, res) => {
  const data = loadData();
  purgeExpiredQrAuthSessions(data);
  const sessions = ensureQrAuthSessions(data);
  const token = String(req.query.token || '');
  const qrSession = sessions[token];
  if (!qrSession) {
    res.setHeader('Set-Cookie', clearQrRequestCookie());
    return res.status(404).json({ error: 'QR sign-in request not found or expired' });
  }
  const browserBinding = getQrRequestCookie(req);
  if (!browserBinding || browserBinding !== String(qrSession.browserBinding || '')) {
    return res.status(404).json({ error: 'QR sign-in request not found' });
  }
  const status = String(qrSession.status || 'pending');
  if (status !== 'approved') {
    return res.json({ ok: true, status, expiresAt: Number(qrSession.expiresAt || 0) });
  }
  const users = Array.isArray(data.users) ? data.users : [];
  const user = users.find((entry) => String(entry?.id || '') === String(qrSession.approvedByUserId || ''));
  if (!user) {
    delete sessions[token];
    saveDataDebounced(data);
    res.setHeader('Set-Cookie', clearQrRequestCookie());
    return res.status(404).json({ error: 'Approved user no longer exists' });
  }
  qrSession.status = 'completed';
  qrSession.completedAt = Date.now();
  saveDataDebounced(data);
  res.setHeader('Set-Cookie', [
    sessionCookie(qrSession.desktopSessionToken, qrSession.desktopSessionMaxAgeMs),
    csrfCookie(qrSession.desktopCsrfToken, qrSession.desktopSessionMaxAgeMs),
    clearQrRequestCookie()
  ]);
  return res.json({
    ok: true,
    status: 'approved',
    csrfToken: String(qrSession.desktopCsrfToken || ''),
    ...authPayloadForUser(user)
  });
});

authRouter.post('/auth/qr/approve', validateBody(qrApproveSchema), (req, res) => {
  const data = loadData();
  purgeExpiredQrAuthSessions(data);
  const sessions = ensureQrAuthSessions(data);
  const token = String(req.body.token || '');
  const qrSession = sessions[token];
  if (!qrSession) return res.status(404).json({ error: 'QR sign-in request not found or expired' });
  if (String(qrSession.status || '') !== 'pending') {
    return res.status(409).json({ error: 'QR sign-in request is no longer pending' });
  }

  let user = attachUserFromSession(req);
  const persist = req?.body?.persist === true;
  const cookies = [];

  if (!user) {
    const check = authenticateUserCredentials(data, {
      email: req?.body?.email,
      password: req?.body?.password
    });
    if (!check.ok) {
      return res.status(check.status || 401).json({
        error: check.error || 'Invalid credentials',
        ...(check.reason ? { reason: check.reason } : {})
      });
    }
    user = check.user;
    user.lastLoginAt = Date.now();
    saveDataDebounced(data);
    const phoneSession = issueSessionForUser(user.id, { persist });
    cookies.push(
      sessionCookie(phoneSession.token, phoneSession.maxAgeMs),
      csrfCookie(phoneSession.csrfToken, phoneSession.maxAgeMs)
    );
  }

  const desktopSession = issueSessionForUser(user.id, { persist: false });
  qrSession.status = 'approved';
  qrSession.approvedAt = Date.now();
  qrSession.approvedByUserId = String(user.id || '');
  qrSession.desktopSessionToken = String(desktopSession.token || '');
  qrSession.desktopCsrfToken = String(desktopSession.csrfToken || '');
  qrSession.desktopSessionMaxAgeMs = Number(desktopSession.maxAgeMs || 0);
  saveDataDebounced(data);

  if (cookies.length) {
    res.setHeader('Set-Cookie', cookies);
  }
  return res.json({
    ok: true,
    status: 'approved',
    currentUser: sanitizeUser(user)
  });
});

module.exports = { authRouter };
