const { getCsrfTokenForSessionToken } = require('./auth');
const {
  parseCookies,
  resolveSessionToken,
  CSRF_COOKIE
} = require('./authMiddleware');

function isMutatingMethod(method) {
  const m = String(method || '').toUpperCase();
  return m === 'POST' || m === 'PUT' || m === 'PATCH' || m === 'DELETE';
}

function requireCsrf(req, res, next) {
  if (!isMutatingMethod(req.method)) return next();

  const token = resolveSessionToken(req);
  if (!token) return res.status(401).json({ error: 'Unauthorized' });

  const cookies = parseCookies(req);
  const cookieToken = String(cookies[CSRF_COOKIE] || '').trim();
  const headerToken = String(req?.headers?.['x-csrf-token'] || '').trim();
  const expected = String(getCsrfTokenForSessionToken(token) || '').trim();

  if (!cookieToken || !headerToken || !expected) {
    return res.status(403).json({ error: 'CSRF token required' });
  }
  if (cookieToken !== expected || headerToken !== expected) {
    return res.status(403).json({ error: 'Invalid CSRF token' });
  }
  return next();
}

module.exports = {
  requireCsrf
};
