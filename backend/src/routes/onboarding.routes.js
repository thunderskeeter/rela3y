const express = require('express');
const crypto = require('crypto');
const { loadData, saveDataDebounced, getAccountByTo, ensureAccountForTo, getAccountById } = require('../store/dataStore');
const { getOutcomePacks } = require('../services/outcomePackService');
const { OUTCOME_PACKS } = require('../services/flowTemplates');
const { generateId } = require('../utils/id');
const { normalizeEmail, normalizeRole, hashPassword, sanitizeUser } = require('../utils/auth');
const { z, validateBody } = require('../utils/validate');
const { APP_PUBLIC_BASE_URL, NODE_ENV } = require('../config/runtime');

const onboardingRouter = express.Router();
const publicOnboardingRouter = express.Router();

const workspaceRequestSchema = z.object({
  email: z.string().trim().email().max(254),
  businessName: z.string().trim().min(1).max(160),
  industry: z.string().trim().max(120).optional().default(''),
  description: z.string().trim().max(1000).optional().default(''),
  preferCall: z.boolean().optional().default(false)
});
const checkoutSessionSchema = z.object({
  planKey: z.enum(['starter', 'pro', 'growth']),
  email: z.string().trim().email().max(254),
  businessName: z.string().trim().min(1).max(160),
  successUrl: z.string().trim().url().max(2048).optional(),
  cancelUrl: z.string().trim().url().max(2048).optional()
});

const setupSchema = z.object({
  outcomePacks: z.array(z.string().trim().min(1).max(120)).max(50).optional().default([]),
  avgTicketValueCents: z.coerce.number().int().min(0).max(10_000_000).optional(),
  avgTicketValue: z.coerce.number().int().min(0).max(10_000_000).optional(),
  bookingUrl: z.string().trim().url().max(2048).optional().or(z.literal('')).default(''),
  phoneConnected: z.boolean().optional().default(false),
  goLive: z.boolean().optional().default(false),
  calendarConnected: z.boolean().optional().default(false),
  businessType: z.string().trim().max(120).optional().default('')
});
const inviteAcceptSchema = z.object({
  name: z.string().trim().min(1).max(120).optional().default(''),
  email: z.string().trim().email().max(254).optional(),
  password: z.string().min(10).max(200)
});

function hashInviteToken(token) {
  return crypto.createHash('sha256').update(String(token || ''), 'utf8').digest('hex');
}

function isStrongPassword(password) {
  const value = String(password || '');
  if (value.length < 10) return false;
  if (!/[A-Z]/.test(value)) return false;
  if (!/[a-z]/.test(value)) return false;
  if (!/[0-9]/.test(value)) return false;
  if (!/[^A-Za-z0-9]/.test(value)) return false;
  return true;
}

function normalizePublicReturnUrl(value, fallback) {
  const raw = String(value || '').trim();
  if (!raw) return fallback;
  try {
    const parsed = new URL(raw);
    if (!/^https?:$/i.test(parsed.protocol)) return fallback;
    if (NODE_ENV === 'production' && parsed.protocol !== 'https:') return fallback;
    const host = String(parsed.hostname || '').toLowerCase();
    if (NODE_ENV === 'production' && (host === 'localhost' || host === '127.0.0.1' || host === '::1')) return fallback;
    return parsed.toString();
  } catch {
    return fallback;
  }
}

function stripeAuthHeader(secretKey) {
  const token = Buffer.from(`${String(secretKey || '').trim()}:`, 'utf8').toString('base64');
  return `Basic ${token}`;
}

function ensurePlatformStripeConfig(data) {
  data.dev = data.dev && typeof data.dev === 'object' ? data.dev : {};
  const cfg = data.dev.platformBillingStripe && typeof data.dev.platformBillingStripe === 'object'
    ? data.dev.platformBillingStripe
    : {};
  data.dev.platformBillingStripe = {
    enabled: cfg.enabled === true,
    secretKey: String(cfg.secretKey || '').trim()
  };
  return data.dev.platformBillingStripe;
}

async function stripeRequest(secretKey, path, { method = 'GET', form = null } = {}) {
  const headers = {
    Authorization: stripeAuthHeader(secretKey),
    Accept: 'application/json'
  };
  let body;
  if (form && typeof form === 'object') {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(form)) {
      if (v == null) continue;
      params.set(String(k), String(v));
    }
    body = params.toString();
    headers['Content-Type'] = 'application/x-www-form-urlencoded';
  }
  const res = await fetch(`https://api.stripe.com${path}`, { method, headers, body });
  const raw = await res.text();
  let parsed = {};
  try { parsed = JSON.parse(raw); } catch {}
  if (!res.ok) {
    const detail = String(parsed?.error?.message || parsed?.message || '').trim();
    const err = new Error(detail || `Stripe request failed (${res.status})`);
    err.status = res.status;
    throw err;
  }
  return parsed;
}

function findInvitationByToken(data, token) {
  const hashed = hashInviteToken(token);
  const accounts = data.accounts && typeof data.accounts === 'object' ? data.accounts : {};
  for (const [to, account] of Object.entries(accounts)) {
    const workspace = account?.workspace && typeof account.workspace === 'object' ? account.workspace : {};
    const invitations = Array.isArray(workspace.invitations) ? workspace.invitations : [];
    const idx = invitations.findIndex((inv) => String(inv?.tokenHash || '') === hashed);
    if (idx >= 0) {
      return { to: String(to), account, invitations, invite: invitations[idx], index: idx };
    }
  }
  return null;
}

onboardingRouter.post('/onboarding/workspace-request', validateBody(workspaceRequestSchema), (req, res) => {
  try {
    const tenant = req.tenant;
    const data = loadData();
    const payload = {
      email: String(req.body?.email || '').trim(),
      businessName: String(req.body?.businessName || '').trim(),
      industry: String(req.body?.industry || '').trim(),
      description: String(req.body?.description || '').trim(),
      preferCall: Boolean(req.body?.preferCall)
    };
    const request = {
      id: generateId(),
      accountId: tenant.accountId,
      status: 'pending',
      createdAt: Date.now(),
      updatedAt: Date.now(),
      ...payload,
      source: 'frontend_request'
    };
    data.workspaceRequests = data.workspaceRequests || [];
    data.workspaceRequests.push(request);
    saveDataDebounced(data);
    return res.json({ ok: true, request });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'request_failed' });
  }
});

publicOnboardingRouter.post('/onboarding/workspace-request', validateBody(workspaceRequestSchema), (req, res) => {
  try {
    const data = loadData();
    const payload = {
      email: String(req.body?.email || '').trim(),
      businessName: String(req.body?.businessName || '').trim(),
      industry: String(req.body?.industry || '').trim(),
      description: String(req.body?.description || '').trim(),
      preferCall: Boolean(req.body?.preferCall)
    };
    const request = {
      id: generateId(),
      accountId: null,
      status: 'pending_payment_verification',
      createdAt: Date.now(),
      updatedAt: Date.now(),
      ...payload,
      source: 'public_signup_request'
    };
    data.workspaceRequests = data.workspaceRequests || [];
    data.workspaceRequests.push(request);
    saveDataDebounced(data);
    return res.json({ ok: true, requestId: request.id, status: request.status });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'request_failed' });
  }
});

publicOnboardingRouter.post('/onboarding/checkout-session', validateBody(checkoutSessionSchema), async (req, res) => {
  try {
    const data = loadData();
    const platformStripe = ensurePlatformStripeConfig(data);
    const secretKey = String(platformStripe.secretKey || '').trim();
    if (!secretKey || platformStripe.enabled !== true) {
      return res.status(400).json({ error: 'Checkout is unavailable until platform Stripe is connected.' });
    }

    const planKey = String(req.body?.planKey || '').trim().toLowerCase();
    const catalog = {
      starter: { name: 'Starter', amountCents: 14900 },
      pro: { name: 'Pro', amountCents: 29900 },
      growth: { name: 'Growth', amountCents: 54900 }
    };
    const selected = catalog[planKey];
    if (!selected) return res.status(400).json({ error: 'Invalid plan key' });

    const email = String(req.body?.email || '').trim().toLowerCase();
    const businessName = String(req.body?.businessName || '').trim();
    const request = {
      id: generateId(),
      accountId: null,
      status: 'checkout_started',
      createdAt: Date.now(),
      updatedAt: Date.now(),
      email,
      businessName,
      industry: '',
      description: `Plan=${selected.name}`,
      preferCall: false,
      source: 'public_checkout'
    };
    data.workspaceRequests = data.workspaceRequests || [];
    data.workspaceRequests.push(request);
    saveDataDebounced(data);

    const publicBase = String(APP_PUBLIC_BASE_URL || 'http://127.0.0.1:3001').replace(/\/$/, '');
    const successFallback = `${publicBase}/dashboard?newClient=1&checkout=success`;
    const cancelFallback = `${publicBase}/home?checkout=cancel`;
    const successUrl = normalizePublicReturnUrl(req.body?.successUrl, successFallback);
    const cancelUrl = normalizePublicReturnUrl(req.body?.cancelUrl, cancelFallback);

    const session = await stripeRequest(secretKey, '/v1/checkout/sessions', {
      method: 'POST',
      form: {
        mode: 'subscription',
        success_url: `${successUrl}${successUrl.includes('?') ? '&' : '?'}session_id={CHECKOUT_SESSION_ID}`,
        cancel_url: cancelUrl,
        customer_email: email,
        billing_address_collection: 'auto',
        allow_promotion_codes: 'true',
        'line_items[0][quantity]': '1',
        'line_items[0][price_data][currency]': 'usd',
        'line_items[0][price_data][unit_amount]': String(selected.amountCents),
        'line_items[0][price_data][recurring][interval]': 'month',
        'line_items[0][price_data][product_data][name]': `Arc Relay ${selected.name}`,
        'metadata[planKey]': planKey,
        'metadata[businessName]': businessName,
        'metadata[email]': email,
        'metadata[workspaceRequestId]': String(request.id)
      }
    });

    const url = String(session?.url || '').trim();
    if (!url) return res.status(500).json({ error: 'Stripe checkout URL was not returned.' });
    return res.json({ ok: true, url, requestId: request.id });
  } catch (err) {
    return res.status(err?.status || 400).json({ error: err?.message || 'Failed to create checkout session' });
  }
});

onboardingRouter.get('/onboarding/options', (req, res) => {
  const tenant = req.tenant;
  const data = loadData();
  const accountRef = getAccountById(data, tenant.accountId);
  const account = accountRef?.account || null;
  const packs = getOutcomePacks(tenant.accountId);
  const onboarding = account?.settings?.onboarding || { stage: 'welcome', completed: false, selectedPacks: [] };
  res.json({ packs, onboarding });
});

onboardingRouter.post('/onboarding/setup', validateBody(setupSchema), (req, res) => {
  try {
    const tenant = req.tenant;
    const payload = req.body || {};
    const packSelection = Array.isArray(payload?.outcomePacks) ? payload.outcomePacks.map((p) => String(p)) : [];
    const avgTicketValue = Number(payload?.avgTicketValueCents || payload?.avgTicketValue || 0);
    const bookingUrl = String(payload?.bookingUrl || '').trim();
    const phoneConnected = payload?.phoneConnected === true;
    const goLive = payload?.goLive === true;
    const data = loadData();
    const account = ensureAccountForTo(data, tenant.to, { autoCreate: true });
    if (!account) return res.status(404).json({ error: 'Account not found' });
    account.workspace = account.workspace && typeof account.workspace === 'object' ? account.workspace : {};
    account.settings = account.settings && typeof account.settings === 'object' ? account.settings : {};
    const workspace = account.workspace;
    workspace.identity = workspace.identity || {};
    workspace.identity.industry = String(payload?.businessType || workspace.identity.industry || '').trim();
    if (!workspace.identity.industry) {
      workspace.identity.industry = 'local_service';
    }
    if (bookingUrl) {
      account.bookingUrl = bookingUrl;
      account.scheduling = account.scheduling || {};
      account.scheduling.url = bookingUrl;
      account.scheduling.mode = 'link';
    }
    account.settings.finance = account.settings.finance || {};
    if (Number.isFinite(avgTicketValue) && avgTicketValue >= 0) {
      account.settings.finance.averageTicketValueCents = Math.round(avgTicketValue);
    }
    const onboarding = account.settings.onboarding || {};
    const logoUrl = String(account?.workspace?.identity?.logoUrl || '').trim();
    onboarding.logoFallback = goLive && !logoUrl;
    onboarding.stage = goLive ? 'live' : 'packs_selected';
    onboarding.completed = goLive;
    onboarding.selectedPacks = packSelection;
    onboarding.phoneConnected = phoneConnected;
    onboarding.calendarConnected = Boolean(payload?.calendarConnected === true);
    account.settings.onboarding = onboarding;
    const existingPacks = Object.keys(OUTCOME_PACKS || {});
    for (const packId of existingPacks) {
      const desired = packSelection.includes(packId) || (goLive && packId === 'recover_missed_calls');
      account.settings.outcomePacks = account.settings.outcomePacks || {};
      account.settings.outcomePacks[packId] = { enabled: desired };
    }
    saveDataDebounced(data);
    res.json({ ok: true, account });
  } catch (err) {
    res.status(500).json({ error: err?.message || 'setup_failed' });
  }
});

onboardingRouter.get('/onboarding/workspace-requests', (req, res) => {
  const data = loadData();
  const items = (data.workspaceRequests || [])
    .filter((r) => String(r?.accountId || '') === String(req.tenant.accountId))
    .sort((a, b) => Number(b?.createdAt || 0) - Number(a?.createdAt || 0));
  res.json({ items });
});

publicOnboardingRouter.get('/onboarding/invitations/:token', (req, res) => {
  const token = String(req.params?.token || '').trim();
  if (!token) return res.status(400).json({ error: 'token is required' });
  const data = loadData();
  const found = findInvitationByToken(data, token);
  if (!found?.invite) return res.status(404).json({ error: 'Invite not found' });
  const invite = found.invite;
  const status = String(invite?.status || 'pending');
  const expiresAt = Number(invite?.expiresAt || 0);
  if (status !== 'pending') return res.status(410).json({ error: 'Invite already used' });
  if (expiresAt && expiresAt <= Date.now()) return res.status(410).json({ error: 'Invite expired' });
  const role = normalizeRole(invite?.role) || 'readonly';
  const email = normalizeEmail(invite?.email || '');
  const businessName = String(found.account?.businessName || found.account?.workspace?.identity?.businessName || '').trim();
  return res.json({
    ok: true,
    invite: {
      role,
      email: email || '',
      emailLocked: Boolean(email),
      expiresAt,
      businessName
    }
  });
});

publicOnboardingRouter.post('/onboarding/invitations/:token/accept', validateBody(inviteAcceptSchema), (req, res) => {
  const token = String(req.params?.token || '').trim();
  if (!token) return res.status(400).json({ error: 'token is required' });
  const data = loadData();
  const found = findInvitationByToken(data, token);
  if (!found?.invite) return res.status(404).json({ error: 'Invite not found' });
  const invite = found.invite;
  const status = String(invite?.status || 'pending');
  const expiresAt = Number(invite?.expiresAt || 0);
  if (status !== 'pending') return res.status(410).json({ error: 'Invite already used' });
  if (expiresAt && expiresAt <= Date.now()) return res.status(410).json({ error: 'Invite expired' });

  const role = normalizeRole(invite?.role);
  if (!role || role === 'superadmin') return res.status(400).json({ error: 'Invite role is invalid' });
  const lockedEmail = normalizeEmail(invite?.email || '');
  const submittedEmail = normalizeEmail(req?.body?.email || '');
  const email = lockedEmail || submittedEmail;
  if (!email) return res.status(400).json({ error: 'email is required' });
  if (lockedEmail && email !== lockedEmail) return res.status(400).json({ error: 'Invite is restricted to a different email' });
  const nameInput = String(req?.body?.name || '').trim();
  const fallbackName = email.includes('@') ? email.split('@')[0].replace(/[._-]+/g, ' ').trim() : 'Member';
  const name = nameInput || fallbackName || 'Member';
  const password = String(req?.body?.password || '');
  if (!isStrongPassword(password)) {
    return res.status(400).json({ error: 'Password must be 10+ chars with uppercase, lowercase, number, and symbol' });
  }

  if (!Array.isArray(data.users)) data.users = [];
  if (data.users.some((u) => normalizeEmail(u?.email) === email)) {
    return res.status(409).json({ error: 'A user with this email already exists' });
  }

  const accountId = String(found.account?.accountId || found.account?.id || '').trim();
  if (!accountId) return res.status(400).json({ error: 'Invite account is invalid' });
  const user = {
    id: generateId(),
    name,
    email,
    passwordHash: hashPassword(password),
    role,
    accountIds: [accountId],
    createdAt: Date.now(),
    lastLoginAt: null,
    disabled: false
  };
  data.users.push(user);
  found.invitations[found.index] = {
    ...invite,
    status: 'accepted',
    acceptedAt: Date.now(),
    acceptedUserId: user.id,
    acceptedEmail: email
  };
  saveDataDebounced(data);
  return res.status(201).json({ ok: true, user: sanitizeUser(user) });
});

module.exports = { onboardingRouter, publicOnboardingRouter };



