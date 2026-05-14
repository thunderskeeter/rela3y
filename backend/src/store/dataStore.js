// DEPRECATED: snapshot runtime source of truth.
// Do not add new imports to this module. Migrate callers to repositories/services.
const { DEV_MODE, USE_DB_CONVERSATIONS, USE_DB_MESSAGES } = require('../config/runtime');
const { FLOW_TEMPLATES } = require('../services/flowTemplates');
const {
  emptyState,
  loadStateSnapshot,
  persistStateSnapshot
} = require('../db/stateRepository');

// In-memory cache backed by PostgreSQL.
let dataCache = null;
let dataDirty = false;
let storeReady = false;
let storeInitPromise = null;
let flushInFlight = null;
let storeShuttingDown = false;

function ensureRevenueStructures(data) {
  if (!data || typeof data !== 'object') return;
  if (!Array.isArray(data.leadEvents)) data.leadEvents = [];
  if (!Array.isArray(data.revenueOpportunities)) data.revenueOpportunities = [];
  if (!data.leadIntelligence || typeof data.leadIntelligence !== 'object' || Array.isArray(data.leadIntelligence)) {
    data.leadIntelligence = {};
  }
  if (!Array.isArray(data.alerts)) data.alerts = [];
  if (!Array.isArray(data.actions)) data.actions = [];
  if (!Array.isArray(data.optimizationEvents)) data.optimizationEvents = [];
  if (!Array.isArray(data.agentRuns)) data.agentRuns = [];
  if (!Array.isArray(data.reviewQueue)) data.reviewQueue = [];
  if (!Array.isArray(data.revenueEvents)) data.revenueEvents = [];
  if (!Array.isArray(data.workspaceRequests)) data.workspaceRequests = [];
  if (!data.webhookReceipts || typeof data.webhookReceipts !== 'object' || Array.isArray(data.webhookReceipts)) {
    data.webhookReceipts = {};
  }
  for (const account of Object.values(data.accounts || {})) {
    if (!account || typeof account !== 'object') continue;
    account.settings = account.settings && typeof account.settings === 'object' ? account.settings : {};
    account.settings.featureFlags = account.settings.featureFlags && typeof account.settings.featureFlags === 'object'
      ? account.settings.featureFlags
      : {};
    if (typeof account.settings.featureFlags.enableOptimization !== 'boolean') account.settings.featureFlags.enableOptimization = false;
    if (typeof account.settings.featureFlags.enableAIMessageVariants !== 'boolean') account.settings.featureFlags.enableAIMessageVariants = false;
    if (typeof account.settings.featureFlags.enableMoneyProjections !== 'boolean') account.settings.featureFlags.enableMoneyProjections = false;
    if (typeof account.settings.featureFlags.enableAgentMode !== 'boolean') account.settings.featureFlags.enableAgentMode = true;
    account.settings.policies = account.settings.policies && typeof account.settings.policies === 'object'
      ? account.settings.policies
      : {};
    if (!Number.isFinite(Number(account.settings.policies.dailyFollowupCapPerLead))) account.settings.policies.dailyFollowupCapPerLead = 2;
    if (!Number.isFinite(Number(account.settings.policies.minCooldownMinutes))) account.settings.policies.minCooldownMinutes = 30;
    if (!Number.isFinite(Number(account.settings.policies.maxAutomationsPerOpportunityPerDay))) account.settings.policies.maxAutomationsPerOpportunityPerDay = 4;
    const qh = account.settings.policies.quietHours && typeof account.settings.policies.quietHours === 'object'
      ? account.settings.policies.quietHours
      : {};
    account.settings.policies.quietHours = {
      startHour: Number.isFinite(Number(qh.startHour)) ? Number(qh.startHour) : 20,
      endHour: Number.isFinite(Number(qh.endHour)) ? Number(qh.endHour) : 8,
      timezone: String(qh.timezone || account?.workspace?.timezone || 'America/New_York')
    };
    account.settings.playbookOverrides = account.settings.playbookOverrides && typeof account.settings.playbookOverrides === 'object'
      ? account.settings.playbookOverrides
      : {};
    account.settings.outcomePacks = account.settings.outcomePacks && typeof account.settings.outcomePacks === 'object'
      ? account.settings.outcomePacks
      : {};
    if (!account.settings.onboarding || typeof account.settings.onboarding !== 'object') {
      account.settings.onboarding = { stage: 'welcome', completed: false, selectedPacks: [] };
    } else {
      account.settings.onboarding.stage = String(account.settings.onboarding.stage || 'welcome');
      account.settings.onboarding.completed = account.settings.onboarding.completed === true;
      account.settings.onboarding.selectedPacks = Array.isArray(account.settings.onboarding.selectedPacks)
        ? account.settings.onboarding.selectedPacks
        : [];
    }
    account.settings.finance = account.settings.finance && typeof account.settings.finance === 'object'
      ? account.settings.finance
      : {};
    if (!Number.isFinite(Number(account.settings.finance.averageTicketValueCents))) account.settings.finance.averageTicketValueCents = 50000;
    if (!Number.isFinite(Number(account.settings.finance.conversionRateBaseline))) account.settings.finance.conversionRateBaseline = 0.15;
    if (!account.settings.finance.valueByServiceType || typeof account.settings.finance.valueByServiceType !== 'object') {
      account.settings.finance.valueByServiceType = {};
    }
    if (!account.businessProfile || typeof account.businessProfile !== 'object') account.businessProfile = {};
    const profile = account.businessProfile;
    const workspace = account.workspace && typeof account.workspace === 'object' ? account.workspace : {};
    const identity = workspace.identity && typeof workspace.identity === 'object' ? workspace.identity : {};
    const scheduling = account.scheduling && typeof account.scheduling === 'object' ? account.scheduling : {};
    profile.businessType = String(
      profile.businessType
      || identity.industry
      || identity.businessType
      || account.businessType
      || account.industry
      || 'local_service'
    ).toLowerCase();
    profile.businessHours = profile.businessHours && typeof profile.businessHours === 'object'
      ? profile.businessHours
      : (workspace.businessHours && typeof workspace.businessHours === 'object' ? workspace.businessHours : {});
    profile.bookingUrl = String(
      profile.bookingUrl
      || scheduling.publicUrl
      || scheduling.url
      || workspace?.scheduling?.publicUrl
      || workspace?.scheduling?.url
      || account.bookingUrl
      || ''
    ).trim();
    profile.services = Array.isArray(profile.services)
      ? profile.services
      : (Array.isArray(workspace?.services) ? workspace.services : []);
    profile.toneStyle = String(
      profile.toneStyle
      || workspace?.settings?.tonePreference
      || 'friendly_professional'
    ).trim();
    profile.escalationRules = profile.escalationRules && typeof profile.escalationRules === 'object'
      ? profile.escalationRules
      : {};
    if (!Array.isArray(profile.escalationRules.urgentKeywords)) {
      profile.escalationRules.urgentKeywords = ['emergency', 'urgent', 'asap', 'immediately', 'now', 'help'];
    }
    if (profile.escalationRules.escalateOnVIP === undefined) {
      profile.escalationRules.escalateOnVIP = true;
    }
    if (profile.escalationRules.allowAfterHoursMessages === undefined) {
      profile.escalationRules.allowAfterHoursMessages = false;
    }
    if (profile.escalationRules.maxAfterHoursEscalations === undefined) {
      profile.escalationRules.maxAfterHoursEscalations = 1;
    }
  }

  for (const opp of data.revenueOpportunities) {
    if (!opp || typeof opp !== 'object') continue;
    if (!opp.stage) opp.stage = 'NEW';
    if (!Array.isArray(opp.stageHistory)) opp.stageHistory = [];
    if (opp.lastInboundAt === undefined) opp.lastInboundAt = null;
    if (opp.lastOutboundAt === undefined) opp.lastOutboundAt = null;
    if (opp.lastActivityAt === undefined) opp.lastActivityAt = null;
    if (!Number.isFinite(Number(opp.followupsSentToday))) opp.followupsSentToday = 0;
    if (!Number.isFinite(Number(opp.followupsSentTotal))) opp.followupsSentTotal = 0;
    if (opp.followupsDayKey === undefined) opp.followupsDayKey = '';
    if (opp.lastRecommendedActionType === undefined) opp.lastRecommendedActionType = null;
    if (opp.lastRecommendedActionAt === undefined) opp.lastRecommendedActionAt = null;
    if (opp.cooldownUntil === undefined) opp.cooldownUntil = null;
    if (typeof opp.quietHoursBypass !== 'boolean') opp.quietHoursBypass = false;
    if (typeof opp.stopAutomation !== 'boolean') opp.stopAutomation = false;
    if (!Array.isArray(opp.actionLogIds)) opp.actionLogIds = [];
    if (opp.projectedRecoveryCents === undefined) opp.projectedRecoveryCents = null;
    if (opp.projectedRecoveryProbability === undefined) opp.projectedRecoveryProbability = null;
    if (!Array.isArray(opp.riskHistory)) opp.riskHistory = [];
    if (!Number.isFinite(Number(opp.automationsSentToday))) opp.automationsSentToday = 0;
    if (opp.automationsDayKey === undefined) opp.automationsDayKey = '';
    if (!opp.agentState || typeof opp.agentState !== 'object') opp.agentState = {};
    if (opp.agentState.activeRunId === undefined) opp.agentState.activeRunId = null;
    if (opp.agentState.lastRunId === undefined) opp.agentState.lastRunId = null;
    if (opp.agentState.lockedUntil === undefined) opp.agentState.lockedUntil = null;
    if (opp.agentState.lockOwner === undefined) opp.agentState.lockOwner = null;
  }
}

async function initDataStore() {
  if (storeReady && dataCache) return dataCache;
  if (storeInitPromise) return storeInitPromise;
  storeInitPromise = (async () => {
    const snapshot = await loadStateSnapshot();
    dataCache = snapshot && typeof snapshot === 'object' ? snapshot : emptyState();
    ensureDevSettings(dataCache);
    ensureAuthStructures(dataCache);
    ensureRevenueStructures(dataCache);
    stripMessagingSnapshotState(dataCache);
    storeReady = true;
    return dataCache;
  })();
  return storeInitPromise;
}

function ensureStoreReadySyncFallback() {
  if (dataCache) return;
  // Guard for sync callers before bootstrap; server startup explicitly initializes store.
  dataCache = emptyState();
  ensureDevSettings(dataCache);
  ensureAuthStructures(dataCache);
  ensureRevenueStructures(dataCache);
  stripMessagingSnapshotState(dataCache);
}

function loadData() {
  ensureStoreReadySyncFallback();
  if (dbMessagingEnabled()) {
    dataCache.conversations = {};
  }
  return dataCache;
}

let saveTimer = null;
function stripMessagingSnapshotState(data) {
  if (!dbMessagingEnabled() || !data || typeof data !== 'object') return data;
  data.conversations = {};
  return data;
}

function snapshotPersistenceDisabled() {
  const raw = String(process.env.DISABLE_SNAPSHOT_PERSISTENCE || '').trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on';
}

async function flushDataNowAsync() {
  if (storeShuttingDown) return;
  if (!dataCache || !dataDirty) return;
  if (flushInFlight) return flushInFlight;
  if (snapshotPersistenceDisabled()) {
    dataDirty = false;
    return Promise.resolve();
  }
  ensureDevSettings(dataCache);
  ensureAuthStructures(dataCache);
  ensureRevenueStructures(dataCache);
  stripMessagingSnapshotState(dataCache);
  const snapshot = JSON.parse(JSON.stringify(dataCache));
  dataDirty = false;
  flushInFlight = persistStateSnapshot(snapshot)
    .catch((err) => {
      dataDirty = true;
      const msg = String(err?.message || err || '');
      if (storeShuttingDown && /pool after calling end on the pool/i.test(msg)) {
        return;
      }
      console.error('[store] failed to persist PostgreSQL snapshot:', msg || err);
    })
    .finally(() => {
      flushInFlight = null;
    });
  return flushInFlight;
}

function flushDataNow() {
  if (!dataCache) return Promise.resolve();
  if (saveTimer) {
    clearTimeout(saveTimer);
    saveTimer = null;
  }
  if (!dataDirty) return Promise.resolve();
  return flushDataNowAsync();
}

function saveDataDebounced(data) {
  if (storeShuttingDown) {
    dataCache = data;
    return;
  }
  ensureDevSettings(data);
  ensureAuthStructures(data);
  ensureRevenueStructures(data);
  stripMessagingSnapshotState(data);
  // Update cache immediately
  dataCache = data;
  dataDirty = true;
  
  // Debounce disk writes
  if (saveTimer) clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    void flushDataNowAsync();
  }, 150);
}

function onSigint() {
  flushDataNow()
    .catch(() => {})
    .finally(() => process.exit(0));
}

function onSigterm() {
  flushDataNow()
    .catch(() => {})
    .finally(() => process.exit(0));
}

function onBeforeExit() {
  if (storeShuttingDown) return;
  try { void flushDataNow(); } catch {}
}

function onExit() {
  if (storeShuttingDown) return;
  try { void flushDataNow(); } catch {}
}

process.once('SIGINT', onSigint);
process.once('SIGTERM', onSigterm);
process.once('beforeExit', onBeforeExit);
process.once('exit', onExit);

async function shutdownDataStore() {
  storeShuttingDown = true;
  if (saveTimer) {
    clearTimeout(saveTimer);
    saveTimer = null;
  }
  process.off('SIGINT', onSigint);
  process.off('SIGTERM', onSigterm);
  process.off('beforeExit', onBeforeExit);
  process.off('exit', onExit);
  try {
    if (snapshotPersistenceDisabled()) {
      dataDirty = false;
      return;
    }
    if (flushInFlight) await flushInFlight;
    if (dataDirty && dataCache) {
      ensureDevSettings(dataCache);
      ensureAuthStructures(dataCache);
      ensureRevenueStructures(dataCache);
      stripMessagingSnapshotState(dataCache);
      await persistStateSnapshot(JSON.parse(JSON.stringify(dataCache)));
      dataDirty = false;
    }
  } catch (err) {
    console.error('[store] shutdown flush failed:', err?.message || err);
  }
}

function convoKey(to, from) {
  return `${to}__${from}`;
}

function dbMessagingEnabled() {
  return USE_DB_CONVERSATIONS === true || USE_DB_MESSAGES === true;
}

function throwBlockedMessagingMutation(operation, to, from) {
  const conversationId = convoKey(to, from);
  console.error(JSON.stringify({
    level: 'error',
    entity: 'messaging_core',
    service: 'dataStore',
    operation,
    conversationId,
    errorType: 'snapshot_write_blocked',
    message: 'Snapshot-backed messaging mutation is blocked in DB messaging mode.'
  }));
  const err = new Error('snapshot_write_blocked');
  err.code = 'snapshot_write_blocked';
  throw err;
}

function flowStorageKey(accountId, flowId) {
  const aid = String(accountId || '').trim();
  const fid = String(flowId || '').trim();
  if (!aid || !fid) return null;
  return `${aid}__${fid}`;
}

function ensureFlowsObject(data) {
  if (!data.flows || typeof data.flows !== 'object' || Array.isArray(data.flows)) {
    data.flows = {};
  }
  return data.flows;
}

function getFlowInData(data, accountId, flowId) {
  ensureTemplateFlowsForAccount(data, accountId);
  const flows = ensureFlowsObject(data);
  const key = flowStorageKey(accountId, flowId);
  if (key && flows[key] && String(flows[key]?.accountId || '') === String(accountId)) {
    return flows[key];
  }
  const legacy = flows[String(flowId || '')];
  if (legacy && String(legacy?.accountId || '') === String(accountId)) {
    return legacy;
  }
  return null;
}

function setFlowInData(data, accountId, flow) {
  const flows = ensureFlowsObject(data);
  const fid = String(flow?.id || '').trim();
  const key = flowStorageKey(accountId, fid);
  if (!key) return null;
  flows[key] = { ...(flow || {}), id: fid, accountId: String(accountId) };
  return flows[key];
}

function deleteFlowInData(data, accountId, flowId) {
  const flows = ensureFlowsObject(data);
  const key = flowStorageKey(accountId, flowId);
  let deleted = false;
  if (key && flows[key] && String(flows[key]?.accountId || '') === String(accountId)) {
    delete flows[key];
    deleted = true;
  }
  const legacyKey = String(flowId || '');
  if (flows[legacyKey] && String(flows[legacyKey]?.accountId || '') === String(accountId)) {
    delete flows[legacyKey];
    deleted = true;
  }
  return deleted;
}

function cloneTemplateFlow(template) {
  return JSON.parse(JSON.stringify(template || {}));
}

function getTemplateFlows() {
  return Object.values(FLOW_TEMPLATES || {}).filter((flow) => flow && typeof flow === 'object');
}

function ensureTemplateFlowsForAccount(data, accountId) {
  const aid = String(accountId || '').trim();
  if (!aid) return 0;
  let changed = 0;
  const accountRef = getAccountById(data, aid);
  const accountBusinessName = String(
    accountRef?.account?.workspace?.identity?.businessName
    || accountRef?.account?.businessName
    || ''
  ).trim();
  const accountBookingUrl = String(
    accountRef?.account?.scheduling?.url
    || accountRef?.account?.bookingUrl
    || ''
  ).trim();

  for (const template of getTemplateFlows()) {
    const fid = String(template?.id || '').trim();
    if (!fid) continue;
    const existingKey = flowStorageKey(aid, fid);
    const flows = ensureFlowsObject(data);
    const existingScoped = existingKey ? flows[existingKey] : null;
    const existingLegacy = flows[fid];
    const existing = (existingScoped && String(existingScoped?.accountId || '') === aid)
      ? existingScoped
      : ((existingLegacy && String(existingLegacy?.accountId || '') === aid) ? existingLegacy : null);
    const seeded = cloneTemplateFlow(template);
    seeded.id = fid;
    seeded.accountId = aid;
    if (existing) {
      const merged = { ...seeded, ...existing, id: fid, accountId: aid, steps: seeded.steps };
      if (typeof existing?.enabled === 'boolean') merged.enabled = existing.enabled;
      if (accountBusinessName) merged.businessName = accountBusinessName;
      if (accountBookingUrl) merged.bookingUrl = accountBookingUrl;
      if (!setFlowInData(data, aid, merged)) continue;
      changed += 1;
      if (flows[fid] && String(flows[fid]?.accountId || '') === aid) {
        delete flows[fid];
      }
      continue;
    }
    if (accountBusinessName) seeded.businessName = accountBusinessName;
    if (accountBookingUrl) seeded.bookingUrl = accountBookingUrl;
    if (!setFlowInData(data, aid, seeded)) continue;
    changed += 1;
  }

  return changed;
}

function ensureDevSettings(data) {
  if (!data || typeof data !== 'object') return;
  const current = data.dev && typeof data.dev === 'object' ? data.dev : {};
  const platformBillingStripe = current.platformBillingStripe && typeof current.platformBillingStripe === 'object'
    ? current.platformBillingStripe
    : {};
  data.dev = {
    enabled: typeof current.enabled === 'boolean' ? current.enabled : DEV_MODE,
    autoCreateTenants: typeof current.autoCreateTenants === 'boolean' ? current.autoCreateTenants : DEV_MODE,
    verboseTenantLogs: typeof current.verboseTenantLogs === 'boolean' ? current.verboseTenantLogs : false,
    simulateOutbound: typeof current.simulateOutbound === 'boolean' ? current.simulateOutbound : DEV_MODE,
    platformBillingStripe: {
      enabled: platformBillingStripe.enabled === true,
      secretKey: String(platformBillingStripe.secretKey || ''),
      publishableKey: String(platformBillingStripe.publishableKey || ''),
      webhookSecret: String(platformBillingStripe.webhookSecret || ''),
      accountId: String(platformBillingStripe.accountId || ''),
      accountEmail: String(platformBillingStripe.accountEmail || ''),
      accountDisplayName: String(platformBillingStripe.accountDisplayName || ''),
      connectedAt: platformBillingStripe.connectedAt ? Number(platformBillingStripe.connectedAt) : null,
      lastTestedAt: platformBillingStripe.lastTestedAt ? Number(platformBillingStripe.lastTestedAt) : null,
      lastStatus: platformBillingStripe.lastStatus ? String(platformBillingStripe.lastStatus) : null,
      lastError: platformBillingStripe.lastError ? String(platformBillingStripe.lastError) : null
    }
  };
}

function ensureAuthStructures(data) {
  if (!data || typeof data !== 'object') return;
  if (!Array.isArray(data.users)) data.users = [];
  if (!data.sessions || typeof data.sessions !== 'object' || Array.isArray(data.sessions)) data.sessions = {};
}

function accountIdFromTo(to) {
  const digits = String(to || '').replace(/[^\d]/g, '');
  return `acct_${digits || 'unknown'}`;
}

function defaultNotificationSettings() {
  return {
    channels: { email: true, sms: false, desktop: false },
    triggers: {
      vipMessage: true,
      missedCall: true,
      newBooking: true,
      highValueLead: true,
      noResponse: true,
      failedWebhook: true,
      failedAutomation: true
    },
    quietHours: { enabled: false, start: '21:00', end: '08:00', timezone: 'America/New_York' },
    dedupeMinutes: 10,
    highValueLeadMinCents: 100000,
    escalation: { enabled: false, afterMinutes: 5, channel: 'sms' }
  };
}

function ensureAccountsObject(data) {
  if (!data.accounts || typeof data.accounts !== 'object' || Array.isArray(data.accounts)) {
    data.accounts = {};
  }
  return data.accounts;
}

function getAccountByTo(data, to) {
  ensureAccountsObject(data);
  return data.accounts[String(to)] || null;
}

function getAccountById(data, accountId) {
  ensureAccountsObject(data);
  for (const [to, account] of Object.entries(data.accounts)) {
    if (!account || typeof account !== 'object') continue;
    const id = String(account.id || account.accountId || '');
    if (id && id === String(accountId)) return { to, account };
  }
  return null;
}

function ensureAccountForTo(data, to, { autoCreate = true } = {}) {
  const key = String(to || '').trim();
  if (!key) return null;
  ensureAccountsObject(data);
  let account = data.accounts[key];
  if (!account && autoCreate) {
    account = { to: key };
    data.accounts[key] = account;
  }
  if (!account) return null;
  if (!account.to) account.to = key;
  if (!account.id) account.id = account.accountId || accountIdFromTo(key);
  if (!account.accountId) account.accountId = account.id;
  return account;
}

function inferAccountIdForTo(data, to, { autoCreate = false } = {}) {
  const account = ensureAccountForTo(data, to, { autoCreate });
  return account ? String(account.id || account.accountId) : null;
}

function getConversation(data, to, from, accountId = null, createIfMissing = true) {
  if (dbMessagingEnabled()) {
    if (createIfMissing) {
      throwBlockedMessagingMutation('getConversation', to, from);
    }
    return null;
  }
  const allowAutoAssignMissing = DEV_MODE === true && data?.dev?.enabled === true && data?.dev?.autoCreateTenants === true;
  const key = convoKey(to, from);
  const existing = data.conversations[key];
  if (existing) {
    if (existing.orphaned === true) return null;
    if (accountId && existing.accountId && String(existing.accountId) !== String(accountId)) {
      if (data?.dev?.verboseTenantLogs === true) {
        console.warn(`[tenant] cross-tenant access blocked: conversationId=${key} requestedAccountId=${accountId} ownerAccountId=${existing.accountId}`);
      }
      return null;
    }
    if (!existing.accountId) {
      const resolved = accountId
        || inferAccountIdForTo(data, to, { autoCreate: allowAutoAssignMissing })
        || (allowAutoAssignMissing ? accountIdFromTo(to) : null);
      if (!resolved) {
        existing.orphaned = true;
        existing.orphanReason = existing.orphanReason || 'missing_account';
        return null;
      }
      existing.accountId = resolved;
    }
    return existing;
  }

  if (!createIfMissing) return null;
  if (!data.conversations[key]) {
    const resolvedAccountId = accountId
      || inferAccountIdForTo(data, to, { autoCreate: allowAutoAssignMissing })
      || (allowAutoAssignMissing ? accountIdFromTo(to) : null);
    if (!resolvedAccountId) return null;
    data.conversations[key] = {
      to,
      from,
      messages: [],
      status: 'new',
      stage: 'ask_service',
      accountId: resolvedAccountId,
      flow: {
        flowId: null,
        ruleId: null,
        stepId: null,
        status: 'idle',
        startedAt: null,
        updatedAt: null,
        lastAutoSentAt: null,
        lockUntil: null
      },
      fields: {},
      audit: []
    };
  }
  return data.conversations[key];
}

function getConversationByIdInData(data, id, accountId = null, createIfMissing = false) {
  if (dbMessagingEnabled()) return null;
  const [to, from] = String(id || '').split('__');
  if (!to || !from) return null;
  return getConversation(data, to, from, accountId, createIfMissing);
}

function getContacts(accountId) {
  const data = loadData();
  return Object.values(data.contacts || {}).filter(
    (c) => c?.orphaned !== true && String(c?.accountId || '') === String(accountId)
  );
}

function getConversations(accountId) {
  if (dbMessagingEnabled()) return [];
  const data = loadData();
  return Object.entries(data.conversations || {})
    .filter(([, c]) => c?.orphaned !== true && String(c?.accountId || '') === String(accountId))
    .map(([id, convo]) => ({ id, conversation: convo }));
}

function getConversationByIdScoped(accountId, conversationId) {
  if (dbMessagingEnabled()) return null;
  const data = loadData();
  const convo = getConversationByIdInData(data, conversationId, accountId, false);
  if (!convo) return null;
  return { id: conversationId, conversation: convo };
}

function getFlows(accountId) {
  const data = loadData();
  const added = ensureTemplateFlowsForAccount(data, accountId);
  if (added > 0) saveDataDebounced(data);
  const seen = new Set();
  const out = [];
  for (const flow of Object.values(data.flows || {})) {
    if (!flow || flow.orphaned === true) continue;
    if (String(flow?.accountId || '') !== String(accountId)) continue;
    const fid = String(flow.id || '').trim();
    if (!fid || seen.has(fid)) continue;
    seen.add(fid);
    out.push(flow);
  }
  return out;
}

function getRules(accountId) {
  const data = loadData();
  if (!data.rules) return [];
  if (Array.isArray(data.rules)) {
    return data.rules.filter((r) => r?.orphaned !== true && String(r?.accountId || '') === String(accountId));
  }
  const out = [];
  for (const rules of Object.values(data.rules || {})) {
    for (const r of Array.isArray(rules) ? rules : []) {
      if (r?.orphaned !== true && String(r?.accountId || '') === String(accountId)) out.push(r);
    }
  }
  return out;
}

function upsertContact(accountId, contact) {
  if (!contact?.phone) throw new Error('contact.phone is required');
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  if (!accountRef?.to) throw new Error('Account not found');
  const to = String(contact.to || accountRef.to);
  if (String(to) !== String(accountRef.to)) {
    if (data?.dev?.verboseTenantLogs === true) {
      console.warn(`[tenant] cross-tenant access blocked: contactTo=${to} tenantTo=${accountRef.to}`);
    }
    throw new Error('Cross-tenant contact upsert blocked');
  }
  data.contacts = data.contacts || {};
  const key = `${to}__${contact.phone}`;
  const existing = data.contacts[key] || {};
  const merged = {
    ...existing,
    ...contact,
    to,
    accountId: String(accountId),
    updatedAt: Date.now(),
    createdAt: existing.createdAt || Date.now()
  };
  data.contacts[key] = merged;
  saveDataDebounced(data);
  return merged;
}

function appendMessage(accountId, conversationId, message) {
  if (dbMessagingEnabled()) {
    const [to, from] = String(conversationId || '').split('__');
    throwBlockedMessagingMutation('appendMessage', to, from);
  }
  const data = loadData();
  const convo = getConversationByIdInData(data, conversationId, accountId, false);
  if (!convo) return null;
  convo.messages = convo.messages || [];
  convo.messages.push({ ...message, accountId: String(accountId) });
  convo.lastActivityAt = Date.now();
  saveDataDebounced(data);
  return convo;
}

function updateConversation(to, from, mutator, accountId = null) {
  if (dbMessagingEnabled()) {
    throwBlockedMessagingMutation('updateConversation', to, from);
  }
  const data = loadData();
  const convo = getConversation(data, to, from, accountId, true);
  if (!convo) return null;
  const result = mutator(convo, data);
  // If mutator is async, wait for it then save
  if (result && typeof result.then === 'function') {
    return result.then(() => {
      saveDataDebounced(data);
      return convo;
    });
  }
  saveDataDebounced(data);
  return convo;
}

function deleteConversation(to, from, accountId = null) {
  if (dbMessagingEnabled()) {
    throwBlockedMessagingMutation('deleteConversation', to, from);
  }
  const data = loadData();
  const key = convoKey(to, from);
  const convo = data.conversations && data.conversations[key];
  const existed = Boolean(convo);
  if (accountId && convo && convo.accountId && String(convo.accountId) !== String(accountId)) {
    return false;
  }
  if (existed) {
    delete data.conversations[key];
    if (Array.isArray(data.revenueOpportunities)) {
      data.revenueOpportunities = data.revenueOpportunities.filter((opp) => String(opp?.convoKey || '') !== key);
    }
    if (Array.isArray(data.scheduledJobs)) {
      data.scheduledJobs = data.scheduledJobs.filter((job) => {
        const jobKey = `${String(job?.to || '')}__${String(job?.from || '')}`;
        const convoId = String(job?.conversationId || '');
        if (String(job?.accountId || '') !== String(accountId || convo?.accountId || '')) return true;
        return jobKey !== key && convoId !== key;
      });
    }
    if (Array.isArray(data.leadEvents)) {
      data.leadEvents = data.leadEvents.filter((ev) => String(ev?.convoKey || '') !== key);
    }
    if (Array.isArray(data.actions)) {
      data.actions = data.actions.filter((action) => String(action?.convoKey || action?.conversationId || '') !== key);
    }
    saveDataDebounced(data);
  }
  return existed;
}

function listConversations(data, to, accountId = null) {
  const all = Object.entries(data.conversations || {});
  const filtered = all.filter(([k, c]) => {
    if (c?.orphaned === true) return false;
    if (to && String(c.to) !== String(to)) return false;
    if (accountId && String(c.accountId || '') !== String(accountId)) return false;
    return true;
  });
  return filtered.map(([key, c]) => ({ key, convo: c }));
}

function getDevSettings() {
  const data = loadData();
  ensureDevSettings(data);
  return { ...data.dev };
}

function updateDevSettings(patch) {
  const data = loadData();
  ensureDevSettings(data);
  const next = { ...data.dev };
  const keys = ['enabled', 'autoCreateTenants', 'verboseTenantLogs', 'simulateOutbound'];
  for (const k of keys) {
    if (patch && Object.prototype.hasOwnProperty.call(patch, k)) {
      if (typeof patch[k] !== 'boolean') {
        throw new Error(`${k} must be boolean`);
      }
      next[k] = patch[k];
    }
  }
  data.dev = next;
  saveDataDebounced(data);
  return { ...data.dev };
}

function normalizeNotificationSettings(input) {
  const base = defaultNotificationSettings();
  const src = input && typeof input === 'object' ? input : {};
  const dedupe = Number(src.dedupeMinutes);
  const highValueLeadMinCentsRaw = Number(src.highValueLeadMinCents);
  const afterMinutes = Number(src?.escalation?.afterMinutes);
  const dedupeMinutes = Number.isFinite(dedupe) ? Math.max(1, Math.min(120, Math.round(dedupe))) : base.dedupeMinutes;
  const highValueLeadMinCents = Number.isFinite(highValueLeadMinCentsRaw)
    ? Math.max(0, Math.min(10_000_000_000, Math.round(highValueLeadMinCentsRaw)))
    : base.highValueLeadMinCents;
  const escalationAfter = Number.isFinite(afterMinutes) ? Math.max(1, Math.min(1440, Math.round(afterMinutes))) : base.escalation.afterMinutes;

  return {
    channels: {
      email: src?.channels?.email !== false,
      sms: src?.channels?.sms === true,
      desktop: src?.channels?.desktop === true
    },
    triggers: {
      vipMessage: src?.triggers?.vipMessage !== false,
      missedCall: src?.triggers?.missedCall !== false,
      newBooking: src?.triggers?.newBooking !== false,
      highValueLead: src?.triggers?.highValueLead !== false,
      noResponse: src?.triggers?.noResponse !== false,
      failedWebhook: src?.triggers?.failedWebhook !== false,
      failedAutomation: src?.triggers?.failedAutomation !== false
    },
    quietHours: {
      enabled: src?.quietHours?.enabled === true,
      start: String(src?.quietHours?.start || base.quietHours.start),
      end: String(src?.quietHours?.end || base.quietHours.end),
      timezone: String(src?.quietHours?.timezone || base.quietHours.timezone)
    },
    dedupeMinutes,
    highValueLeadMinCents,
    escalation: {
      enabled: src?.escalation?.enabled === true,
      afterMinutes: escalationAfter,
      channel: String(src?.escalation?.channel || base.escalation.channel)
    }
  };
}

function getNotificationSettings(accountId) {
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  if (!accountRef?.account) throw new Error('Account not found');
  accountRef.account.settings = accountRef.account.settings && typeof accountRef.account.settings === 'object'
    ? accountRef.account.settings
    : {};
  const normalized = normalizeNotificationSettings(accountRef.account.settings.notifications);
  accountRef.account.settings.notifications = normalized;
  saveDataDebounced(data);
  return normalized;
}

function setNotificationSettings(accountId, settings) {
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  if (!accountRef?.account) throw new Error('Account not found');
  accountRef.account.settings = accountRef.account.settings && typeof accountRef.account.settings === 'object'
    ? accountRef.account.settings
    : {};
  accountRef.account.settings.notifications = normalizeNotificationSettings(settings);
  saveDataDebounced(data);
  return accountRef.account.settings.notifications;
}

function appendNotificationLog(accountId, entry) {
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  if (!accountRef?.account) throw new Error('Account not found');
  const account = accountRef.account;
  account.notificationLog = Array.isArray(account.notificationLog) ? account.notificationLog : [];
  const item = {
    ts: Number(entry?.ts || Date.now()),
    eventType: String(entry?.eventType || ''),
    channel: String(entry?.channel || 'system'),
    status: String(entry?.status || 'sent'),
    reason: entry?.reason ? String(entry.reason) : null,
    eventId: entry?.eventId ? String(entry.eventId) : ''
  };
  account.notificationLog.unshift(item);
  account.notificationLog = account.notificationLog.slice(0, 200);
  saveDataDebounced(data);
  return item;
}

function getNotificationLog(accountId, limit = 50) {
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  if (!accountRef?.account) throw new Error('Account not found');
  const n = Math.max(1, Math.min(200, Number(limit) || 50));
  const list = Array.isArray(accountRef.account.notificationLog) ? accountRef.account.notificationLog : [];
  return list.slice(0, n);
}

module.exports = {
  initDataStore,
  shutdownDataStore,
  loadData,
  saveDataDebounced,
  flushDataNow,
  accountIdFromTo,
  getAccountByTo,
  getAccountById,
  ensureAccountForTo,
  inferAccountIdForTo,
  flowStorageKey,
  getFlowInData,
  setFlowInData,
  deleteFlowInData,
  getConversation,
  getConversationByIdInData,
  getContacts,
  getConversations,
  getConversationById: getConversationByIdScoped,
  getFlows,
  getRules,
  upsertContact,
  appendMessage,
  getDevSettings,
  updateDevSettings,
  defaultNotificationSettings,
  normalizeNotificationSettings,
  getNotificationSettings,
  setNotificationSettings,
  appendNotificationLog,
  getNotificationLog,
  updateConversation,
  deleteConversation,
  listConversations
};

