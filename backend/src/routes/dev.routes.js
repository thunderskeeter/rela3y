const express = require('express');
const { getDevSettings, updateDevSettings, updateConversation, loadData, saveDataDebounced, flushDataNow, upsertContact } = require('../store/dataStore');
const { createLeadEvent, evaluateOpportunity, getConversationEvents } = require('../services/revenueIntelligenceService');
const { handleSignal } = require('../services/revenueOrchestrator');
const { runPassiveRevenueMonitoring, runReactivationScan } = require('../services/passiveRevenueMonitoring');
const { optimizeOutcomePacks } = require('../services/optimizationService');
const { startRun, replayRun } = require('../services/agentEngine');
const { recordSimulatedConversation } = require('../services/conversationsService');
const { z, validateBody } = require('../utils/validate');
const { DEV_MODE, APP_PUBLIC_BASE_URL } = require('../config/runtime');
const { hasDeveloperAccess, sanitizeUser } = require('../utils/auth');

const devRouter = express.Router();
const settingsPatchSchema = z.object({
  enabled: z.boolean().optional(),
  autoCreateTenants: z.boolean().optional(),
  verboseTenantLogs: z.boolean().optional(),
  simulateOutbound: z.boolean().optional()
}).strict();

const simulateSchema = z.object({
  scenario: z.string().trim().min(1).max(80),
  from: z.string().trim().min(3).max(64).optional(),
  text: z.string().trim().max(2000).optional()
}).passthrough();

const replaySchema = z.object({
  runId: z.string().trim().min(1).max(128),
  dryRun: z.boolean().optional()
}).strict();
const platformStripeConnectSchema = z.object({
  secretKey: z.string().trim().max(256).optional().default(''),
  publishableKey: z.string().trim().max(256).optional().default(''),
  webhookSecret: z.string().trim().max(256).optional().default('')
});
const noBodySchema = z.object({}).strict().optional().default({});

devRouter.use((req, res, next) => {
  if (!String(req?.path || '').startsWith('/dev')) return next();
  if (!hasDeveloperAccess(req?.user)) return res.status(403).json({ error: 'Forbidden' });
  if (DEV_MODE !== true && String(req?.method || '').toUpperCase() === 'GET' && String(req?.path || '') === '/dev/settings') {
    return res.json({
      settings: {
        enabled: false,
        autoCreateTenants: false,
        verboseTenantLogs: false,
        simulateOutbound: false,
        productionLocked: true
      }
    });
  }
  return next();
});

devRouter.get('/dev/settings', (_req, res) => {
  const settings = getDevSettings();
  res.json({ settings });
});

function maskSecret(value, { left = 6, right = 4 } = {}) {
  const v = String(value || '').trim();
  if (!v) return '';
  if (v.length <= left + right) return `${v.slice(0, 2)}***`;
  return `${v.slice(0, left)}...${v.slice(-right)}`;
}

function ensurePlatformStripeConfig(data) {
  data.dev = data.dev && typeof data.dev === 'object' ? data.dev : {};
  const current = data.dev.platformBillingStripe && typeof data.dev.platformBillingStripe === 'object'
    ? data.dev.platformBillingStripe
    : {};
  data.dev.platformBillingStripe = {
    enabled: current.enabled === true,
    secretKey: String(current.secretKey || '').trim(),
    publishableKey: String(current.publishableKey || '').trim(),
    webhookSecret: String(current.webhookSecret || '').trim(),
    accountId: String(current.accountId || '').trim(),
    accountEmail: String(current.accountEmail || '').trim(),
    accountDisplayName: String(current.accountDisplayName || '').trim(),
    connectedAt: current.connectedAt ? Number(current.connectedAt) : null,
    lastTestedAt: current.lastTestedAt ? Number(current.lastTestedAt) : null,
    lastStatus: current.lastStatus ? String(current.lastStatus) : null,
    lastError: current.lastError ? String(current.lastError) : null
  };
  return data.dev.platformBillingStripe;
}

function platformStripeSnapshot(cfg) {
  const current = cfg && typeof cfg === 'object' ? cfg : {};
  const base = String(APP_PUBLIC_BASE_URL || '').replace(/\/$/, '');
  return {
    enabled: current.enabled === true,
    accountId: String(current.accountId || ''),
    accountEmail: String(current.accountEmail || ''),
    accountDisplayName: String(current.accountDisplayName || ''),
    publishableKey: String(current.publishableKey || ''),
    webhookSecretMasked: current.webhookSecret ? maskSecret(current.webhookSecret) : '',
    webhookUrl: base ? `${base}/webhooks/stripe/platform` : '/webhooks/stripe/platform',
    secretKeyMasked: current.secretKey ? maskSecret(current.secretKey) : '',
    connectedAt: current.connectedAt ? Number(current.connectedAt) : null,
    lastTestedAt: current.lastTestedAt ? Number(current.lastTestedAt) : null,
    lastStatus: current.lastStatus ? String(current.lastStatus) : null,
    lastError: current.lastError ? String(current.lastError) : null
  };
}

function collectAccountNumbers(account, to) {
  const out = [];
  if (to) out.push(String(to));
  const workspaceNumbers = Array.isArray(account?.workspace?.phoneNumbers) ? account.workspace.phoneNumbers : [];
  for (const row of workspaceNumbers) {
    const num = String(row?.number || '').trim();
    if (num) out.push(num);
  }
  const twilioPhone = String(account?.integrations?.twilio?.phoneNumber || '').trim();
  if (twilioPhone) out.push(twilioPhone);
  return [...new Set(out)];
}

function buildDeveloperUsersOverview(data) {
  const users = Array.isArray(data?.users)
    ? data.users.map(sanitizeUser).filter(Boolean)
    : [];
  const workspaces = [];

  for (const [to, account] of Object.entries(data.accounts || {})) {
    if (!account || typeof account !== 'object') continue;
    const accountId = String(account.accountId || account.id || '').trim();
    if (!accountId) continue;
    const billing = account?.billing && typeof account.billing === 'object' ? account.billing : {};
    const billingPlan = billing?.plan && typeof billing.plan === 'object' ? billing.plan : {};
    workspaces.push({
      to: String(to),
      accountId,
      businessName: String(account.businessName || account?.workspace?.identity?.businessName || '').trim() || 'Workspace',
      numbers: collectAccountNumbers(account, to),
      billing: {
        provider: String(billing.provider || 'demo'),
        isLive: billing.isLive === true,
        planName: String(billingPlan.name || billingPlan.key || 'Pro'),
        planStatus: String(billingPlan.status || account?.provisioning?.status || 'unpaid'),
        priceMonthly: Number(billingPlan.priceMonthly || 0),
        nextBillingAt: billingPlan.nextBillingAt ? Number(billingPlan.nextBillingAt) : null,
        billingEmail: String(billing?.details?.billingEmail || '')
      }
    });
  }

  workspaces.sort((a, b) => String(a.businessName).localeCompare(String(b.businessName)));
  return { asOf: Date.now(), users, workspaces };
}

function stripeAuthHeader(secretKey) {
  const token = Buffer.from(`${String(secretKey || '').trim()}:`, 'utf8').toString('base64');
  return `Basic ${token}`;
}

async function stripeRequest(secretKey, path, { method = 'GET', form = null } = {}) {
  const headers = {
    Authorization: stripeAuthHeader(secretKey),
    Accept: 'application/json'
  };
  let body;
  if (form && typeof form === 'object') {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(form)) {
      if (value == null) continue;
      params.set(String(key), String(value));
    }
    body = params.toString();
    headers['Content-Type'] = 'application/x-www-form-urlencoded';
  }
  const response = await fetch(`https://api.stripe.com${path}`, { method, headers, body });
  const raw = await response.text();
  let parsed = {};
  try { parsed = JSON.parse(raw); } catch {}
  if (!response.ok) {
    const detail = String(parsed?.error?.message || parsed?.message || '').trim();
    throw new Error(detail || `Stripe request failed (${response.status})`);
  }
  return parsed;
}

async function testStripeCredentials(secretKey) {
  const account = await stripeRequest(secretKey, '/v1/account');
  return {
    accountId: String(account?.id || '').trim(),
    accountEmail: String(account?.email || '').trim(),
    accountDisplayName: String(account?.business_profile?.name || account?.settings?.dashboard?.display_name || '').trim()
  };
}

devRouter.get('/dev/platform-billing/stripe', (_req, res) => {
  const data = loadData();
  const cfg = ensurePlatformStripeConfig(data);
  saveDataDebounced(data);
  return res.json({ ok: true, stripe: platformStripeSnapshot(cfg) });
});

devRouter.get('/dev/users-overview', (_req, res) => {
  const data = loadData();
  return res.json({ ok: true, ...buildDeveloperUsersOverview(data) });
});

devRouter.put('/dev/platform-billing/stripe', validateBody(platformStripeConnectSchema), async (req, res) => {
  const data = loadData();
  const cfg = ensurePlatformStripeConfig(data);
  const secretKey = String(req.body?.secretKey || '').trim() || String(cfg.secretKey || '').trim();
  const publishableKey = String(req.body?.publishableKey || '').trim();
  const webhookSecret = String(req.body?.webhookSecret || '').trim() || String(cfg.webhookSecret || '').trim();
  if (!secretKey) return res.status(400).json({ error: 'secretKey is required' });

  try {
    const tested = await testStripeCredentials(secretKey);
    data.dev.platformBillingStripe = {
      ...cfg,
      enabled: true,
      secretKey,
      publishableKey,
      webhookSecret,
      accountId: tested.accountId || cfg.accountId || '',
      accountEmail: tested.accountEmail || cfg.accountEmail || '',
      accountDisplayName: tested.accountDisplayName || cfg.accountDisplayName || '',
      connectedAt: cfg.connectedAt || Date.now(),
      lastTestedAt: Date.now(),
      lastStatus: 'ok',
      lastError: null
    };
    saveDataDebounced(data);
    await flushDataNow();
    return res.json({ ok: true, stripe: platformStripeSnapshot(data.dev.platformBillingStripe) });
  } catch (err) {
    data.dev.platformBillingStripe = {
      ...cfg,
      enabled: false,
      secretKey,
      publishableKey,
      webhookSecret,
      lastTestedAt: Date.now(),
      lastStatus: 'error',
      lastError: String(err?.message || 'Stripe authentication failed')
    };
    saveDataDebounced(data);
    await flushDataNow();
    return res.status(400).json({ error: err?.message || 'Failed to connect platform Stripe' });
  }
});

devRouter.post('/dev/platform-billing/stripe/test', validateBody(noBodySchema), async (_req, res) => {
  const data = loadData();
  const cfg = ensurePlatformStripeConfig(data);
  const secretKey = String(cfg.secretKey || '').trim();
  if (!secretKey) return res.status(400).json({ error: 'Platform Stripe secret key is not configured' });
  try {
    const tested = await testStripeCredentials(secretKey);
    data.dev.platformBillingStripe = {
      ...cfg,
      enabled: true,
      accountId: tested.accountId || cfg.accountId || '',
      accountEmail: tested.accountEmail || cfg.accountEmail || '',
      accountDisplayName: tested.accountDisplayName || cfg.accountDisplayName || '',
      lastTestedAt: Date.now(),
      lastStatus: 'ok',
      lastError: null
    };
    saveDataDebounced(data);
    await flushDataNow();
    return res.json({ ok: true, accountId: tested.accountId || '', stripe: platformStripeSnapshot(data.dev.platformBillingStripe) });
  } catch (err) {
    data.dev.platformBillingStripe = {
      ...cfg,
      enabled: false,
      lastTestedAt: Date.now(),
      lastStatus: 'error',
      lastError: String(err?.message || 'Stripe test failed')
    };
    saveDataDebounced(data);
    await flushDataNow();
    return res.status(400).json({ error: err?.message || 'Stripe test failed' });
  }
});

devRouter.delete('/dev/platform-billing/stripe', validateBody(noBodySchema), async (_req, res) => {
  const data = loadData();
  const cfg = ensurePlatformStripeConfig(data);
  data.dev.platformBillingStripe = {
    ...cfg,
    enabled: false,
    secretKey: '',
    publishableKey: '',
    webhookSecret: '',
    accountId: '',
    accountEmail: '',
    accountDisplayName: '',
    connectedAt: null,
    lastTestedAt: Date.now(),
    lastStatus: 'disconnected',
    lastError: null
  };
  saveDataDebounced(data);
  await flushDataNow();
  return res.json({ ok: true, stripe: platformStripeSnapshot(data.dev.platformBillingStripe) });
});

devRouter.patch('/dev/settings', validateBody(settingsPatchSchema), (req, res) => {
  if (DEV_MODE !== true) {
    return res.status(403).json({ error: 'Developer settings are locked in production' });
  }
  try {
    const patch = req.body || {};
    const settings = updateDevSettings(patch);
    res.json({ ok: true, settings });
  } catch (err) {
    res.status(400).json({ error: err?.message || 'Invalid dev settings patch' });
  }
});

async function simulateSignal(tenant, { from, type, channel = 'sms', text = '', payload = {} } = {}) {
  const to = String(tenant?.to || '');
  const accountId = String(tenant?.accountId || '');
  const convoKey = `${to}__${from}`;
  if (type === 'inbound_message') {
    updateConversation(to, from, (c) => {
      c.accountId = accountId;
      c.messages = Array.isArray(c.messages) ? c.messages : [];
      c.messages.push({
        id: `dev_${Date.now()}`,
        direction: 'inbound',
        dir: 'in',
        text: String(text || 'Reply'),
        body: String(text || 'Reply'),
        to,
        from,
        ts: Date.now(),
        status: 'received',
        source: 'dev_simulator'
      });
      c.lastActivityAt = Date.now();
    }, accountId);
  }
  const leadEvent = createLeadEvent(accountId, {
    convoKey,
    channel,
    type,
    payload: {
      source: 'dev_simulator',
      text: String(text || ''),
      ...payload
    }
  });
  const result = await handleSignal(accountId, leadEvent);
  return { leadEvent, result };
}

devRouter.post('/dev/revenue/simulate', validateBody(simulateSchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const scenario = String(req.body?.scenario || '').trim().toLowerCase();
    const from = String(req.body?.from || '+18145550199');
    if (!scenario) return res.status(400).json({ error: 'scenario is required' });

    if (scenario === 'missed_call_business_hours') {
      const out = await simulateSignal(tenant, {
        from,
        type: 'missed_call',
        channel: 'call',
        payload: { businessHours: true }
      });
      return res.json({ ok: true, scenario, ...out });
    }

    if (scenario === 'inbound_sms_reply') {
      const out = await simulateSignal(tenant, {
        from,
        type: 'inbound_message',
        channel: 'sms',
        text: String(req.body?.text || 'Yes, still interested.'),
        payload: { intent: 'book' }
      });
      return res.json({ ok: true, scenario, ...out });
    }

    if (scenario === 'after_hours_missed_call') {
      const out = await simulateSignal(tenant, {
        from,
        type: 'after_hours_inquiry',
        channel: 'call',
        payload: { afterHours: true }
      });
      return res.json({ ok: true, scenario, ...out });
    }

    if (scenario === 'silent_lead') {
      await simulateSignal(tenant, {
        from,
        type: 'inbound_message',
        channel: 'sms',
        text: String(req.body?.text || 'Can I get pricing?')
      });
      await runPassiveRevenueMonitoring();
      return res.json({ ok: true, scenario, message: 'Created inbound event and forced PRM tick.' });
    }

    if (scenario === 'reactivation') {
      await runReactivationScan();
      return res.json({ ok: true, scenario, message: 'Triggered reactivation scan.' });
    }

    if (scenario === 'prm_cooldown') {
      await simulateSignal(tenant, { from, type: 'missed_call', channel: 'call' });
      await runPassiveRevenueMonitoring();
      await runPassiveRevenueMonitoring();
      return res.json({ ok: true, scenario, message: 'Triggered PRM twice to validate cooldown behavior.' });
    }

    if (scenario === 'daily_cap') {
      const data = loadData();
      const convoKey = `${tenant.to}__${from}`;
      const opp = (data.revenueOpportunities || []).find((o) =>
        String(o?.accountId || '') === String(tenant.accountId) && String(o?.convoKey || '') === convoKey
      );
      if (opp) {
        opp.followupsSentToday = 99;
        opp.followupsDayKey = new Date().toISOString().slice(0, 10);
        saveDataDebounced(data);
      }
      await runPassiveRevenueMonitoring();
      return res.json({ ok: true, scenario, message: 'Forced followup cap and executed PRM.' });
    }

    if (scenario === 'quiet_hours_schedule') {
      const data = loadData();
      const account = data.accounts?.[String(tenant.to)] || null;
      if (account) {
        account.settings = account.settings || {};
        account.settings.policies = account.settings.policies || {};
        account.settings.policies.quietHours = { startHour: 0, endHour: 23, timezone: 'America/New_York' };
        saveDataDebounced(data);
      }
      await simulateSignal(tenant, { from, type: 'missed_call', channel: 'call' });
      await runPassiveRevenueMonitoring();
      return res.json({ ok: true, scenario, message: 'Configured quiet hours and ran PRM.' });
    }

    if (scenario === 'stage_transitions') {
      await simulateSignal(tenant, { from, type: 'missed_call', channel: 'call' });
      await simulateSignal(tenant, { from, type: 'inbound_message', channel: 'sms', text: 'I want to book now' });
      await simulateSignal(tenant, { from, type: 'booking_created', channel: 'web' });
      const data = loadData();
      const convoKey = `${tenant.to}__${from}`;
      const opp = (data.revenueOpportunities || []).find((o) =>
        String(o?.accountId || '') === String(tenant.accountId) && String(o?.convoKey || '') === convoKey
      );
      if (opp) evaluateOpportunity(tenant.accountId, opp.id);
      const refreshed = loadData();
      const finalOpp = (refreshed.revenueOpportunities || []).find((o) =>
        String(o?.accountId || '') === String(tenant.accountId) && String(o?.convoKey || '') === convoKey
      );
      return res.json({
        ok: true,
        scenario,
        stage: finalOpp?.stage || null,
        stageHistory: finalOpp?.stageHistory || [],
        events: getConversationEvents(refreshed, tenant.accountId, convoKey).map((e) => e.type)
      });
    }

    if (scenario === 'agent_run_missed_call_home_services') {
      await simulateSignal(tenant, { from, type: 'missed_call', channel: 'call' });
      const data = loadData();
      const convoKey = `${tenant.to}__${from}`;
      const opp = (data.revenueOpportunities || []).find((o) =>
        String(o?.accountId || '') === String(tenant.accountId) && String(o?.convoKey || '') === convoKey
      );
      if (!opp) return res.status(400).json({ error: 'Opportunity not found after signal' });
      const started = await startRun(tenant.accountId, opp.id, { trigger: 'manual_user_start', mode: 'AUTO' });
      const refreshed = loadData();
      const run = (refreshed.agentRuns || []).find((r) => String(r?.id || '') === String(started?.run?.id || ''));
      const actions = (refreshed.actions || []).filter((a) => String(a?.runId || '') === String(run?.id || ''));
      return res.json({ ok: true, scenario, run, actions: actions.slice(-10) });
    }

    if (scenario === 'agent_run_after_hours_medspa') {
      const data = loadData();
      const account = data.accounts?.[String(tenant.to)] || null;
      if (account) {
        account.settings = account.settings || {};
        account.settings.policies = account.settings.policies || {};
        account.settings.policies.quietHours = { startHour: 0, endHour: 23, timezone: 'America/New_York' };
        account.workspace = account.workspace || {};
        account.workspace.identity = account.workspace.identity || {};
        account.workspace.identity.industry = 'med spa';
        saveDataDebounced(data);
      }
      await simulateSignal(tenant, { from, type: 'after_hours_inquiry', channel: 'sms', text: 'Can I book botox?' });
      const refreshed = loadData();
      const convoKey = `${tenant.to}__${from}`;
      const opp = (refreshed.revenueOpportunities || []).find((o) => String(o?.convoKey || '') === convoKey && String(o?.accountId || '') === String(tenant.accountId));
      const run = opp?.agentState?.activeRunId ? (refreshed.agentRuns || []).find((r) => String(r?.id || '') === String(opp.agentState.activeRunId)) : null;
      return res.json({ ok: true, scenario, opportunityId: opp?.id || null, runStatus: run?.status || null, run });
    }

    if (scenario === 'agent_review_required_escalation') {
      await simulateSignal(tenant, { from, type: 'missed_call', channel: 'call' });
      const data = loadData();
      const convoKey = `${tenant.to}__${from}`;
      const opp = (data.revenueOpportunities || []).find((o) => String(o?.convoKey || '') === convoKey && String(o?.accountId || '') === String(tenant.accountId));
      if (!opp) return res.status(400).json({ error: 'Opportunity not found' });
      opp.riskScore = 95;
      saveDataDebounced(data);
      const started = await startRun(tenant.accountId, opp.id, { trigger: 'manual_user_start', mode: 'REVIEW_REQUIRED' });
      const refreshed = loadData();
      const reviewItems = (refreshed.reviewQueue || []).filter((x) => String(x?.runId || '') === String(started?.run?.id || ''));
      return res.json({ ok: true, scenario, run: started?.run || null, reviewItems });
    }

    if (scenario === 'agent_lock_contention') {
      await simulateSignal(tenant, { from, type: 'missed_call', channel: 'call' });
      const data = loadData();
      const convoKey = `${tenant.to}__${from}`;
      const opp = (data.revenueOpportunities || []).find((o) => String(o?.convoKey || '') === convoKey && String(o?.accountId || '') === String(tenant.accountId));
      if (!opp) return res.status(400).json({ error: 'Opportunity not found' });
      opp.agentState = opp.agentState || {};
      opp.agentState.lockedUntil = Date.now() + (2 * 60 * 1000);
      opp.agentState.lockOwner = 'external_test';
      saveDataDebounced(data);
      const started = await startRun(tenant.accountId, opp.id, { trigger: 'manual_user_start', mode: 'AUTO' });
      return res.json({ ok: true, scenario, started });
    }

    if (scenario === 'agent_idempotency_double_fire') {
      await simulateSignal(tenant, { from, type: 'missed_call', channel: 'call' });
      const data = loadData();
      const convoKey = `${tenant.to}__${from}`;
      const opp = (data.revenueOpportunities || []).find((o) => String(o?.convoKey || '') === convoKey && String(o?.accountId || '') === String(tenant.accountId));
      if (!opp) return res.status(400).json({ error: 'Opportunity not found' });
      const first = await startRun(tenant.accountId, opp.id, { trigger: 'manual_user_start', mode: 'AUTO' });
      const second = await startRun(tenant.accountId, opp.id, { trigger: 'manual_user_start', mode: 'AUTO' });
      const refreshed = loadData();
      const runId = first?.run?.id || second?.run?.id || null;
      const actions = (refreshed.actions || []).filter((a) => String(a?.runId || '') === String(runId || ''));
      return res.json({ ok: true, scenario, runId, actions: actions.slice(-20) });
    }

    if (scenario === 'agent_replay_dry_run') {
      await simulateSignal(tenant, { from, type: 'missed_call', channel: 'call' });
      const data = loadData();
      const convoKey = `${tenant.to}__${from}`;
      const opp = (data.revenueOpportunities || []).find((o) => String(o?.convoKey || '') === convoKey && String(o?.accountId || '') === String(tenant.accountId));
      if (!opp) return res.status(400).json({ error: 'Opportunity not found' });
      const started = await startRun(tenant.accountId, opp.id, { trigger: 'manual_user_start', mode: 'AUTO' });
      const replay = await replayRun(tenant.accountId, started?.run?.id, { dryRun: true });
      const refreshed = loadData();
      const replayActions = (refreshed.actions || []).filter((a) => String(a?.runId || '') === String(replay?.run?.id || ''));
      return res.json({ ok: true, scenario, replayRun: replay?.run || null, replayActions: replayActions.slice(-20) });
    }

    if (scenario === 'detailing_conversation') {
      const services = [
        { name: 'ceramic coating and wheel protection', detail: 'black SUV with chrome badges' },
        { name: 'interior deep detail with leather conditioning', detail: 'white Tesla Model 3' },
        { name: 'paint correction plus sealant', detail: 'red Porsche 911 Cabriolet' },
        { name: 'headlight restoration and trim', detail: 'silver Honda Accord' },
        { name: 'engine bay degrease and polish', detail: 'lifted diesel truck' },
        { name: 'full express detail and odor removal', detail: 'family minivan' }
      ];
      const extras = ['clay bar prep', 'glass polish', 'wheel well coating', 'odor neutralizer', 'mat shampoo'];
      const selectedService = services[Math.floor(Math.random() * services.length)];
      const selectedExtra = extras[Math.floor(Math.random() * extras.length)];
      const randomSuffix = Math.floor(1000000 + Math.random() * 8999999);
      const simulatedFrom = String(req.body?.from || `+1814${randomSuffix}`);
      const firstNames = ['Jordan', 'Casey', 'Taylor', 'Alex', 'Morgan', 'Riley'];
      const lastNames = ['Bennett', 'Parker', 'Reed', 'Carter', 'Hayes', 'Brooks'];
      const customerName = `${firstNames[Math.floor(Math.random() * firstNames.length)]} ${lastNames[Math.floor(Math.random() * lastNames.length)]}`;
      const customerEmail = `lead${randomSuffix}@example.com`;
      const to = String(tenant?.to || '');
      const accountId = String(tenant?.accountId || '');
      const outboundWithLog = async (message, opts = {}) => {
        const meta = opts?.meta && typeof opts.meta === 'object' ? opts.meta : {};
        const payload = opts?.payload && typeof opts.payload === 'object' ? opts.payload : {};
        await simulateSignal(tenant, { from: simulatedFrom, type: 'outbound_message', channel: 'sms', text: message, payload });
        await updateConversation(to, simulatedFrom, (conversation) => {
          conversation.messages = conversation.messages || [];
          conversation.messages.push({
            id: `dev_out_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
            direction: 'outbound',
            dir: 'out',
            text: message,
            body: message,
            meta,
            payload,
            to,
            from: simulatedFrom,
            ts: Date.now(),
            status: 'sent',
            source: 'dev_simulator'
          });
          conversation.lastActivityAt = Date.now();
        }, accountId);
      };
      const firstCustomer = `Hey, I have a ${selectedService.name} job for my ${selectedService.detail}. Can you make time for it?`;
      const customerFollowup = `Also add ${selectedExtra}.`;
      const priceInquiry = `What would that package run me starting with the ${selectedService.name}?`;
      const aiIntro = `Hi there—sorry I missed your call. What detailing service can I help you with today?`;
      const customerIntro = `Hey, I'm looking for the ${selectedService.name} for my ${selectedService.detail}. Can you make time for it?`;
      const safeCarLabel = selectedService.detail ? selectedService.detail : "vehicle";
      const detailModelMap = {
        "black SUV with chrome badges": "Range Rover Sport",
        "white Tesla Model 3": "Tesla Model 3",
        "red Porsche 911 Cabriolet": "Porsche 911 Cabriolet",
        "silver Honda Accord": "Honda Accord",
        "lifted diesel truck": "Ford F-250 Super Duty",
        "family minivan": "Chrysler Pacifica"
      };
      const modelName = detailModelMap[selectedService.detail] || selectedService.detail;
      const detailDesc = selectedService.detail || safeCarLabel;
      const hasModelMention = modelName && detailDesc.toLowerCase().includes(modelName.toLowerCase());
      const escapedModelName = modelName ? modelName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") : "";
      const colorHint = hasModelMention && escapedModelName ? detailDesc.replace(new RegExp(escapedModelName, "i"), "").trim() : "";
      const suffix = colorHint ? ` (${colorHint})` : "";
      const aiCarSpecPrompt = `Great, thanks. Please reply with the year/make/model for your ${modelName || safeCarLabel}${suffix} so we can match the right prep.`;
      const carYear = 2015 + Math.floor(Math.random() * 8);
      const carSpecResponse = `It's the ${carYear} ${modelName}.`;
      const conditionPrompt = "Thanks! Could you describe the current condition (pet hair, stains, odors, etc.) so we prep accordingly?";
      const conditionResponse = [`It has heavy pet hair and a couple of small stains in the rear.`, `It needs pet hair removal and odor neutralizing, nothing too crazy.`][Math.floor(Math.random() * 2)];
      const availabilityReply = "I'm pulling up your booking link—pick any available slot that works for you and I'll confirm when it's locked in.";
      const firstReply = `Thanks for the details. I've added ${selectedExtra} to your summary—anything else you'd like to include before you grab the booking link?`;
      const followupReply = "Excellent, that's noted. Confirm the drop-off window after you pick a time and I'll send a reminder straight to your phone.";
      const accountData = loadData();
      const bookingAccount = accountData.accounts?.[String(to)] || null;
      const bookingUrl = bookingAccount?.scheduling?.url || bookingAccount?.bookingUrl || 'https://calendly.com/relay';
      const priceEstimate = Math.round(220 + Math.random() * 80);
      const summaryLines = `${selectedService.name} + ${selectedExtra}`;
      const priceReply = `Our ${selectedService.name} package starts at $${priceEstimate}. Here is your booking link: ${bookingUrl} When you open it, you will see your service summary (${summaryLines}) already included. Pick a time that works for you and we will confirm final pricing there.`;
      const bookingStart = Date.now() + (3 * 24 * 60 * 60 * 1000);
      const bookingDateLabel = new Date(bookingStart).toLocaleString("en-US", {
        month: "short",
        day: "numeric",
        weekday: "short",
        hour: "numeric",
        minute: "2-digit"
      });
      const bookingAck = `I see you booked on ${bookingDateLabel} -- We will see you then if you have any questions feel free to ask.`;
      const simulatedConversation = [
        { role: 'ai', text: aiIntro },
        { role: 'customer', text: customerIntro },
        { role: 'ai', text: aiCarSpecPrompt },
        { role: 'customer', text: carSpecResponse },
        { role: 'ai', text: conditionPrompt },
        { role: 'customer', text: conditionResponse },
        { role: 'ai', text: availabilityReply },
        { role: 'customer', text: customerFollowup },
        { role: 'ai', text: firstReply },
        { role: 'ai', text: followupReply },
        { role: 'customer', text: priceInquiry },
        { role: 'ai', text: priceReply },
        { role: 'ai', text: bookingAck, payload: { bookingConfirmed: true, bookingLabel: bookingDateLabel, bookingTime: bookingStart }, meta: { bookingTime: bookingStart, bookingLabel: bookingDateLabel } }
      ];
      const simulatedResponse = {
        ok: true,
        scenario,
        to,
        convoKey: `${to}__${simulatedFrom}`,
        conversationId: `${to}__${simulatedFrom}`,
        from: simulatedFrom,
        service: selectedService.name,
        detail: selectedService.detail,
        extra: selectedExtra,
        customerName,
        customerEmail,
        bookingTime: bookingStart,
        price: priceEstimate,
        bookingUrl,
        conversation: simulatedConversation
      };
      if (DEV_MODE !== true) {
        const leadData = {
          intent: selectedService.name,
          request: `${selectedService.name} + ${selectedExtra}`,
          service_required: `${selectedService.name} + ${selectedExtra}`,
          services_list: [selectedService.name, selectedExtra],
          services_summary: `- ${selectedService.name}\n- ${selectedExtra}`,
          vehicle_model: selectedService.detail,
          simulated: true
        };
        const messages = simulatedConversation.map((item, index) => ({
          id: `sim_${Date.now()}_${index}`,
          role: item.role,
          direction: item.role === 'customer' ? 'inbound' : 'outbound',
          dir: item.role === 'customer' ? 'in' : 'out',
          text: item.text,
          body: item.text,
          meta: item.meta || {},
          payload: item.payload || {},
          to,
          from: simulatedFrom,
          ts: Date.now() + index,
          status: 'simulated',
          source: 'dev_simulator'
        }));
        const persistedConversation = await recordSimulatedConversation({
          tenant,
          to,
          from: simulatedFrom,
          messages,
          leadData,
          amount: priceEstimate
        });
        return res.json({ ...simulatedResponse, persisted: true, savedConversation: persistedConversation });
      }
      await simulateSignal(tenant, { from: simulatedFrom, type: 'missed_call', channel: 'call', payload: { detail: selectedService.name } });
      await outboundWithLog(aiIntro);
      await simulateSignal(tenant, { from: simulatedFrom, type: 'inbound_message', channel: 'sms', text: customerIntro, payload: { intent: 'book', detail: selectedService.name } });
      await outboundWithLog(aiCarSpecPrompt);
      await simulateSignal(tenant, { from: simulatedFrom, type: 'inbound_message', channel: 'sms', text: carSpecResponse, payload: { intent: 'vehicle_info', detail: selectedService.detail } });
      await outboundWithLog(conditionPrompt);
      await simulateSignal(tenant, { from: simulatedFrom, type: 'inbound_message', channel: 'sms', text: conditionResponse, payload: { intent: 'condition' } });
      await upsertContact(String(tenant.accountId), {
        phone: simulatedFrom,
        name: customerName,
        email: customerEmail,
        vehicle: selectedService.detail,
        lastService: selectedService.name,
        notes: `${selectedExtra} requested`
      });
      await outboundWithLog(availabilityReply);
      await simulateSignal(tenant, { from: simulatedFrom, type: 'inbound_message', channel: 'sms', text: customerFollowup, payload: { intent: 'book', extra: selectedExtra } });
      await outboundWithLog(firstReply);
      await outboundWithLog(followupReply);
      await simulateSignal(tenant, { from: simulatedFrom, type: 'inbound_message', channel: 'sms', text: priceInquiry, payload: { intent: 'pricing' } });
      await outboundWithLog(priceReply);
      await simulateSignal(tenant, {
        from: simulatedFrom,
        type: 'booking_created',
        channel: 'web',
        payload: {
          bookingTime: bookingStart,
          bookingEndTime: bookingStart + (2 * 60 * 60 * 1000),
          customerName,
          customerEmail,
          customerPhone: simulatedFrom,
          service: selectedService.name,
          serviceRequired: `${selectedService.name} + ${selectedExtra}`,
          vehicle: selectedService.detail,
          amount: priceEstimate
        }
      });
      await updateConversation(to, simulatedFrom, (conversation) => {
        conversation.status = 'booked';
        conversation.bookingTime = bookingStart;
        conversation.bookingEndTime = bookingStart + (2 * 60 * 60 * 1000);
        conversation.leadData = conversation.leadData && typeof conversation.leadData === 'object' ? conversation.leadData : {};
        conversation.leadData.customer_name = customerName;
        conversation.leadData.customer_phone = simulatedFrom;
        conversation.leadData.customer_email = customerEmail;
        conversation.leadData.request = `${selectedService.name} + ${selectedExtra}`;
        conversation.leadData.service_required = `${selectedService.name} + ${selectedExtra}`;
      }, accountId);
      await outboundWithLog(bookingAck, {
        payload: { bookingConfirmed: true, bookingLabel: bookingDateLabel, bookingTime: bookingStart },
        meta: { bookingConfirmed: true, bookingLabel: bookingDateLabel, bookingTime: bookingStart }
      });
      return res.json({ ...simulatedResponse, persisted: true });
    }

    return res.status(400).json({ error: 'Unknown scenario' });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Simulation failed' });
  }
});

devRouter.post('/dev/agent/replay-run', validateBody(replaySchema), async (req, res) => {
  try {
    const runId = String(req.body?.runId || '').trim();
    const dryRun = req.body?.dryRun !== false;
    if (!runId) return res.status(400).json({ error: 'runId is required' });
    const out = await replayRun(req.tenant.accountId, runId, { dryRun });
    if (!out?.ok) return res.status(400).json({ error: out?.reason || 'replay_failed' });
    return res.json({ ok: true, run: out.run, dryRun });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Replay failed' });
  }
});

devRouter.post('/dev/revenue/run-optimization', validateBody(noBodySchema), async (req, res) => {
  try {
    await optimizeOutcomePacks({ force: true });
    const data = loadData();
    const events = (data.optimizationEvents || [])
      .filter((e) => String(e?.accountId || '') === String(req?.tenant?.accountId || ''))
      .sort((a, b) => Number(b?.ts || 0) - Number(a?.ts || 0))
      .slice(0, 10);
    return res.json({ ok: true, events });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Optimization run failed' });
  }
});

module.exports = { devRouter };
