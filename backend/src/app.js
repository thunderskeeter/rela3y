const express = require('express');
const cors = require('cors');
const compression = require('compression');
const path = require('path');
const { CORS_ORIGINS, NODE_ENV, DEV_MODE } = require('./config/runtime');
const { createRateLimiter } = require('./utils/rateLimit');
const { requireCsrf } = require('./utils/csrfMiddleware');
const { requestLogger } = require('./utils/requestLogger');
const { pool } = require('./db/pool');

const { messagesRouter } = require('./routes/messages.routes');
const { rulesRouter } = require('./routes/rules.routes');
const { flowsRouter } = require('./routes/flows.routes');
const { accountRouter } = require('./routes/account.routes');
const { billingRouter } = require('./routes/billing.routes');
const { contactsRouter } = require('./routes/contacts.routes');
const { devRouter } = require('./routes/dev.routes');
const { integrationsRouter, publicIntegrationsRouter } = require('./routes/integrations.routes');
const { notificationsRouter } = require('./routes/notifications.routes');
const { emailRouter } = require('./routes/email.routes');
const { authRouter } = require('./routes/auth.routes');
const { adminRouter } = require('./routes/admin.routes');
const { analyticsRouter } = require('./routes/analytics.routes');
const { onboardingRouter, publicOnboardingRouter } = require('./routes/onboarding.routes');
const { agentRouter } = require('./routes/agent.routes');
const { publicBookingRouter } = require('./routes/publicBooking.routes');
const { webhooksRouter } = require("./routes/webhooks.routes");
const { stripeWebhooksRouter } = require('./routes/stripe.webhooks.routes');
const { requireTenant, requireTenantForWebhook } = require('./utils/accountContext');
const { requireAuth, requireRole, requireAccountAccess } = require('./utils/authMiddleware');

function stripQuery(url) {
  const raw = String(url || '');
  const idx = raw.indexOf('?');
  return idx >= 0 ? raw.slice(0, idx) : raw;
}

function assetCacheControlForPath(reqPath) {
  const ext = String(path.extname(reqPath || '') || '').toLowerCase();
  if (!ext) return 'no-store';
  if (DEV_MODE) return 'no-store';
  if (ext === '.js' || ext === '.css') return 'public, max-age=31536000, immutable';
  if (ext === '.png' || ext === '.jpg' || ext === '.jpeg' || ext === '.webp' || ext === '.svg' || ext === '.ico' || ext === '.woff2') {
    return 'public, max-age=604800, stale-while-revalidate=86400';
  }
  return 'public, max-age=3600';
}

function sendHtmlFile(res, filePath) {
  res.setHeader('Cache-Control', 'no-store');
  return res.sendFile(filePath);
}

function createApp({ staticDir } = {}) {
  const app = express();
  app.set('etag', false);
  const authLimiterMax = DEV_MODE ? 300 : 12;
  const authLimiter = createRateLimiter({ windowMs: 60_000, max: authLimiterMax, pool });
  const webhookLimiter = createRateLimiter({
    windowMs: 60_000,
    max: DEV_MODE ? 600 : 180,
    pool,
    keyFn: (req) => {
      const ip = String(req?.headers?.['x-forwarded-for'] || req?.socket?.remoteAddress || req?.ip || 'unknown').split(',')[0].trim();
      const path = stripQuery(req?.originalUrl || req?.url || '');
      const to = String(req?.body?.To || req?.body?.to || req?.query?.to || '').trim();
      const from = String(req?.body?.From || req?.body?.from || '').trim();
      return `${ip}|${path}|${to}|${from}`;
    }
  });
  const webhookEventLimiter = createRateLimiter({
    windowMs: 60_000,
    max: DEV_MODE ? 240 : 45,
    pool,
    keyFn: (req) => {
      const ip = String(req?.headers?.['x-forwarded-for'] || req?.socket?.remoteAddress || req?.ip || 'unknown').split(',')[0].trim();
      const to = String(req?.body?.to || req?.body?.To || req?.query?.to || '').trim();
      const from = String(req?.body?.from || req?.body?.From || '').trim();
      const type = String(req?.body?.type || '').trim().toLowerCase();
      return `event|${ip}|${to}|${from}|${type}`;
    }
  });
  const apiWriteLimiterMax = DEV_MODE ? 1200 : 240;
  const apiWriteLimiter = createRateLimiter({
    windowMs: 60_000,
    max: apiWriteLimiterMax,
    pool,
    keyFn: (req) => {
      const uid = String(req?.user?.id || 'anon');
      const aid = String(req?.tenant?.accountId || req?.query?.accountId || req?.headers?.['x-account-id'] || '');
      const ip = String(req?.headers?.['x-forwarded-for'] || req?.socket?.remoteAddress || req?.ip || 'unknown').split(',')[0].trim();
      return `${uid}|${aid}|${ip}`;
    }
  });

  const allowlist = new Set(Array.isArray(CORS_ORIGINS) ? CORS_ORIGINS : []);
  const isProd = NODE_ENV === 'production';
  app.use(cors({
    credentials: true,
    origin(origin, callback) {
      if (!origin) return callback(null, true);
      if (!isProd) return callback(null, true);
      if (allowlist.has(origin)) return callback(null, true);
      return callback(new Error('CORS origin not allowed'));
    }
  }));
  app.disable('x-powered-by');
  app.use(compression({
    threshold: 1024,
    filter(req, res) {
      const reqPath = stripQuery(req?.originalUrl || req?.url || '');
      if (reqPath.startsWith('/webhooks/')) return false;
      return compression.filter(req, res);
    }
  }));
  app.use(requestLogger);
  app.use((req, res, next) => {
    const reqPath = stripQuery(req?.originalUrl || req?.url || '');
    const allowSameOriginFrame = reqPath.startsWith('/book/');
    const frameAncestors = allowSameOriginFrame ? "frame-ancestors 'self'" : "frame-ancestors 'none'";
    const csp = [
      "default-src 'self'",
      "base-uri 'self'",
      "object-src 'none'",
      "script-src 'self'",
      "style-src 'self' 'unsafe-inline'",
      "img-src 'self' data: blob: https: http:",
      "font-src 'self' data:",
      "connect-src 'self' ws: wss: https: http:",
      "frame-src 'self' https: http:",
      frameAncestors
    ].join('; ');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', allowSameOriginFrame ? 'SAMEORIGIN' : 'DENY');
    res.setHeader('Content-Security-Policy', csp);
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    res.setHeader('Cross-Origin-Opener-Policy', 'same-origin-allow-popups');
    if (NODE_ENV === 'production') {
      res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
    }
    return next();
  });
  app.use('/webhooks/stripe', webhookLimiter);
  app.use('/webhooks/stripe', express.raw({ type: 'application/json' }), stripeWebhooksRouter);
  app.use(express.json({ limit: '2mb' }));
  app.use(express.urlencoded({ extended: false }));
  app.use('/webhooks', webhookLimiter);
  app.use('/webhooks/event', webhookEventLimiter);
  app.use('/', publicIntegrationsRouter);
  app.use('/api/public', publicBookingRouter);
  app.use('/api/public', publicOnboardingRouter);

  // API routes
  app.use('/api/auth', authLimiter);
  app.use('/api', (_req, res, next) => {
    res.setHeader('Cache-Control', 'no-store');
    return next();
  });
  app.use('/api', authRouter);
  app.use('/api/admin', requireAuth, requireRole('superadmin'), requireCsrf, adminRouter);
  app.use('/api', requireAuth);
  app.use('/api', (req, res, next) => {
    const method = String(req?.method || 'GET').toUpperCase();
    if (method === 'GET' || method === 'HEAD' || method === 'OPTIONS') return next();
    return apiWriteLimiter(req, res, next);
  });
  app.use('/api', requireCsrf);
  app.use('/api', requireTenant);
  app.use('/api', requireAccountAccess);
  app.use('/api', messagesRouter);
  app.use('/api', rulesRouter);
  app.use('/api', flowsRouter);
  app.use('/api', billingRouter);
  app.use('/api', accountRouter);
  app.use('/api', contactsRouter);
  app.use('/api', devRouter);
  app.use('/api', integrationsRouter);
  app.use('/api', notificationsRouter);
  app.use('/api', emailRouter);
  app.use('/api', analyticsRouter);
  app.use('/api', onboardingRouter);
  app.use('/api', agentRouter);
  // Static frontend
  if (staticDir) {
    app.use(express.static(staticDir, {
      etag: false,
      maxAge: 0,
      setHeaders(res, filePath) {
        const reqPath = res.req?.path || filePath || '';
        res.setHeader('Cache-Control', assetCacheControlForPath(reqPath));
      }
    }));
    app.get('/', (_req, res) => res.redirect(302, '/home'));
    app.get('/home', (_req, res) => sendHtmlFile(res, path.join(staticDir, 'landing.html')));
    app.get('/dashboard', (_req, res) => sendHtmlFile(res, path.join(staticDir, 'index.html')));
    app.get('/demo', (_req, res) => sendHtmlFile(res, path.join(staticDir, 'index.html')));
    app.get('/login', (_req, res) => sendHtmlFile(res, path.join(staticDir, 'index.html')));
    app.get('/login/qr', (_req, res) => sendHtmlFile(res, path.join(staticDir, 'qr-approve.html')));
    app.get('/book/:token', (_req, res) => sendHtmlFile(res, path.join(staticDir, 'book.html')));
    app.get('*', (req, res, next) => {
      const reqPath = String(req.path || '');
      if (reqPath === '/' || reqPath === '/home' || reqPath === '/health' || reqPath === '/ready') return next();
      if (reqPath === '/dashboard' || reqPath === '/login' || reqPath === '/demo') return next();
      if (reqPath === '/api' || reqPath.startsWith('/api/')) return next();
      if (reqPath === '/webhooks' || reqPath.startsWith('/webhooks/')) return next();
      if (reqPath === '/book' || reqPath.startsWith('/book/')) return next();
      // Skip direct asset/file requests and let static middleware handle 404s.
      if (path.extname(reqPath)) return next();
      return sendHtmlFile(res, path.join(staticDir, 'index.html'));
    });
  }
  
  app.use("/webhooks", requireTenantForWebhook, webhooksRouter);

  // Health
  app.get('/health', (_req, res) => res.json({ ok: true }));
  app.get('/ready', async (_req, res) => {
    try {
      await pool.query('SELECT 1');
      return res.json({ ok: true, db: 'up' });
    } catch (err) {
      return res.status(503).json({ ok: false, db: 'down', error: err?.message || 'db unavailable' });
    }
  });

  // 404 JSON for API
  app.use('/api', (_req, res) => res.status(404).json({ error: 'Not found' }));
  app.use((err, req, res, _next) => {
    const line = {
      level: 'error',
      type: 'unhandled_error',
      requestId: req?.requestId || null,
      path: stripQuery(req?.originalUrl || req?.url || ''),
      method: req?.method || '',
      tenantId: req?.tenant?.accountId || null,
      userId: req?.user?.id || null,
      message: err?.message || 'unknown_error'
    };
    console.error(JSON.stringify(line));
    if (res.headersSent) return;
    res.status(500).json({
      error: 'Internal server error',
      requestId: req?.requestId || null
    });
  });

  return app;
}

module.exports = { createApp };





