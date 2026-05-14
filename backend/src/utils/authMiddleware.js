const {
  SESSION_COOKIE,
  CSRF_COOKIE,
  ADMIN_UNLOCK_COOKIE,
  getUserFromSessionToken,
  canUserAccessAccount,
  verifyAdminUnlockToken
} = require('./auth');

function parseCookies(req) {
  const header = String(req?.headers?.cookie || '');
  if (!header) return {};
  const out = {};
  for (const part of header.split(';')) {
    const idx = part.indexOf('=');
    if (idx < 0) continue;
    const key = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    if (!key) continue;
    out[key] = decodeURIComponent(value || '');
  }
  return out;
}

function resolveSessionToken(req) {
  const cookies = parseCookies(req);
  return String(cookies[SESSION_COOKIE] || '').trim();
}

function isBearerAuth(req) {
  const authHeader = String(req?.headers?.authorization || '').trim();
  return authHeader.toLowerCase().startsWith('bearer ');
}

function attachUserFromSession(req) {
  if (req.user) return req.user;
  const token = resolveSessionToken(req);
  if (!token) return null;
  const user = getUserFromSessionToken(token);
  if (!user) return null;
  req.user = user;
  return user;
}

function requireAuth(req, res, next) {
  const user = attachUserFromSession(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  return next();
}

function requireRole(...roles) {
  const allowed = new Set((roles || []).map((r) => String(r || '').toLowerCase()));
  return function roleGuard(req, res, next) {
    const user = req.user || attachUserFromSession(req);
    if (!user) return res.status(401).json({ error: 'Unauthorized' });
    const role = String(user.role || '').toLowerCase();
    if (role === 'superadmin') return next();
    if (!allowed.has(role)) return res.status(403).json({ error: 'Forbidden' });
    return next();
  };
}

function requireAccountAccess(req, res, next) {
  const user = req.user || attachUserFromSession(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  if (String(user.role || '').toLowerCase() === 'superadmin') return next();
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });
  if (!canUserAccessAccount(user, accountId)) return res.status(404).json({ error: 'Not found' });
  return next();
}

function resolveAdminUnlockToken(req) {
  const cookies = parseCookies(req);
  const byCookie = String(cookies[ADMIN_UNLOCK_COOKIE] || '').trim();
  if (byCookie) return byCookie;
  const byHeader = String(req?.headers?.['x-admin-unlock-token'] || '').trim();
  return byHeader;
}

function requireAdminUnlock(req, res, next) {
  const user = req.user || attachUserFromSession(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });
  const token = resolveAdminUnlockToken(req);
  const check = verifyAdminUnlockToken(token, {
    userId: String(user.id || ''),
    accountId
  });
  if (!check.ok) {
    return res.status(423).json({
      error: 'Admin unlock required',
      code: 'ADMIN_UNLOCK_REQUIRED'
    });
  }
  return next();
}

module.exports = {
  parseCookies,
  resolveSessionToken,
  isBearerAuth,
  SESSION_COOKIE,
  CSRF_COOKIE,
  attachUserFromSession,
  requireAuth,
  requireRole,
  requireAccountAccess,
  requireAdminUnlock
};
