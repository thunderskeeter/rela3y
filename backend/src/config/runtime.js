function parseBool(value, fallback = false) {
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) return fallback;
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
  if (['0', 'false', 'no', 'off'].includes(raw)) return false;
  return fallback;
}

function parseCsv(value) {
  return String(value || '')
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
}

function stripTrailingSlash(value) {
  return String(value || '').trim().replace(/\/+$/, '');
}

function isLocalHostName(hostname) {
  const host = String(hostname || '').trim().toLowerCase();
  return host === 'localhost' || host === '127.0.0.1' || host === '::1' || host.endsWith('.local');
}

function parseHttpUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    if (!['http:', 'https:'].includes(parsed.protocol)) return null;
    return parsed;
  } catch {
    return null;
  }
}

const NODE_ENV = String(process.env.NODE_ENV || 'development').trim().toLowerCase();
const DEV_MODE = parseBool(process.env.DEV_MODE, false);
const COOKIE_SECURE = parseBool(process.env.COOKIE_SECURE, NODE_ENV === 'production');
const COOKIE_SAMESITE = String(process.env.COOKIE_SAMESITE || 'Lax').trim();
const CORS_ORIGINS = parseCsv(process.env.CORS_ORIGINS || '');
const WEBHOOK_AUTH_TOKEN = String(process.env.WEBHOOK_AUTH_TOKEN || '').trim();
const WEBHOOK_DEV_SECRET = String(process.env.WEBHOOK_DEV_SECRET || '').trim();
const APP_PUBLIC_BASE_URL = stripTrailingSlash(process.env.APP_PUBLIC_BASE_URL || '');
const WEBHOOK_PUBLIC_BASE = stripTrailingSlash(process.env.WEBHOOK_PUBLIC_BASE || '');
const CAL_OAUTH_REDIRECT_BASE = stripTrailingSlash(process.env.CAL_OAUTH_REDIRECT_BASE || APP_PUBLIC_BASE_URL || 'http://127.0.0.1:3001');
const GOOGLE_CAL_CLIENT_ID = String(process.env.GOOGLE_CAL_CLIENT_ID || '').trim();
const GOOGLE_CAL_CLIENT_SECRET = String(process.env.GOOGLE_CAL_CLIENT_SECRET || '').trim();
const MICROSOFT_CAL_CLIENT_ID = String(process.env.MICROSOFT_CAL_CLIENT_ID || '').trim();
const MICROSOFT_CAL_CLIENT_SECRET = String(process.env.MICROSOFT_CAL_CLIENT_SECRET || '').trim();
const EMAIL_PROVIDER = String(process.env.EMAIL_PROVIDER || '').trim().toLowerCase();
const EMAIL_FROM = String(process.env.EMAIL_FROM || '').trim();
const RESEND_API_KEY = String(process.env.RESEND_API_KEY || '').trim();
const SENDGRID_API_KEY = String(process.env.SENDGRID_API_KEY || '').trim();
const EMAIL_DRY_RUN = parseBool(process.env.EMAIL_DRY_RUN, false);
const ENABLE_PARITY_CHECKS = parseBool(process.env.ENABLE_PARITY_CHECKS, DEV_MODE);
const USE_DB_CONTACTS = parseBool(process.env.USE_DB_CONTACTS, true);
const USE_DB_OPPORTUNITIES = parseBool(process.env.USE_DB_OPPORTUNITIES, false);
const USE_DB_ACTIONS = parseBool(process.env.USE_DB_ACTIONS, false);
const USE_DB_CONVERSATIONS = parseBool(process.env.USE_DB_CONVERSATIONS, false);
const USE_DB_MESSAGES = parseBool(process.env.USE_DB_MESSAGES, false);
const USE_DB_SCHEDULER = parseBool(process.env.USE_DB_SCHEDULER, false);

function requireProductionUrl(name, value) {
  const parsed = parseHttpUrl(value);
  if (!parsed) return `${name} must be a valid http(s) URL`;
  if (parsed.protocol !== 'https:') return `${name} must use https in production`;
  if (isLocalHostName(parsed.hostname)) return `${name} cannot point to localhost in production`;
  return null;
}

function validateRuntimeConfig() {
  if (NODE_ENV !== 'production') return;

  const errors = [];
  const databaseUrl = String(process.env.DATABASE_URL || '').trim();
  const authSecret = String(process.env.AUTH_SECRET || '').trim();

  if (DEV_MODE === true) errors.push('DEV_MODE must be false in production');
  if (!databaseUrl) errors.push('DATABASE_URL is required in production');
  if (!authSecret || authSecret.length < 32) errors.push('AUTH_SECRET must be at least 32 characters in production');
  if (COOKIE_SECURE !== true) errors.push('COOKIE_SECURE must be true in production');
  if (!APP_PUBLIC_BASE_URL) errors.push('APP_PUBLIC_BASE_URL is required in production');

  const appBaseErr = requireProductionUrl('APP_PUBLIC_BASE_URL', APP_PUBLIC_BASE_URL);
  if (APP_PUBLIC_BASE_URL && appBaseErr) errors.push(appBaseErr);

  for (const origin of CORS_ORIGINS) {
    const parsed = parseHttpUrl(origin);
    if (!parsed) {
      errors.push(`CORS_ORIGINS contains invalid URL: ${origin}`);
      continue;
    }
    if (parsed.protocol !== 'https:' || isLocalHostName(parsed.hostname)) {
      errors.push(`CORS_ORIGINS must contain only public https origins in production: ${origin}`);
    }
  }

  if (CAL_OAUTH_REDIRECT_BASE) {
    const err = requireProductionUrl('CAL_OAUTH_REDIRECT_BASE', CAL_OAUTH_REDIRECT_BASE);
    if (err) errors.push(err);
  }
  if (WEBHOOK_PUBLIC_BASE) {
    const err = requireProductionUrl('WEBHOOK_PUBLIC_BASE', WEBHOOK_PUBLIC_BASE);
    if (err) errors.push(err);
  }

  if (errors.length) {
    throw new Error(`Invalid production runtime config: ${errors.join('; ')}`);
  }
}

module.exports = {
  NODE_ENV,
  DEV_MODE,
  COOKIE_SECURE,
  COOKIE_SAMESITE,
  CORS_ORIGINS,
  APP_PUBLIC_BASE_URL,
  WEBHOOK_AUTH_TOKEN,
  WEBHOOK_DEV_SECRET,
  WEBHOOK_PUBLIC_BASE,
  CAL_OAUTH_REDIRECT_BASE,
  GOOGLE_CAL_CLIENT_ID,
  GOOGLE_CAL_CLIENT_SECRET,
  MICROSOFT_CAL_CLIENT_ID,
  MICROSOFT_CAL_CLIENT_SECRET,
  EMAIL_PROVIDER,
  EMAIL_FROM,
  RESEND_API_KEY,
  SENDGRID_API_KEY,
  EMAIL_DRY_RUN,
  ENABLE_PARITY_CHECKS,
  USE_DB_CONTACTS,
  USE_DB_OPPORTUNITIES,
  USE_DB_ACTIONS,
  USE_DB_CONVERSATIONS,
  USE_DB_MESSAGES,
  USE_DB_SCHEDULER,
  validateRuntimeConfig
};
