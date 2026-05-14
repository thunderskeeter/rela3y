const { pool } = require('../db/pool');
const { withTransaction } = require('../db/withTransaction');
const { USE_DB_OPPORTUNITIES, USE_DB_ACTIONS } = require('../config/runtime');
const { listByTenant, getById, create, updateById } = require('../repositories/opportunitiesRepo');
const { loadData, saveDataDebounced } = require('../store/dataStore');
const { verifyParity, stableNormalize } = require('./migrationParityService');

const DAY_MS = 24 * 60 * 60 * 1000;

function scopedLeadEvents(data, accountId) {
  return (data.leadEvents || [])
    .filter((e) => String(e?.accountId || '') === String(accountId))
    .sort((a, b) => Number(a?.ts || 0) - Number(b?.ts || 0));
}

function scopedSnapshotOpportunities(data, accountId) {
  return (data.revenueOpportunities || [])
    .filter((o) => String(o?.accountId || '') === String(accountId));
}

function getRevenueEventsForAccount(data, accountId) {
  return (data.revenueEvents || [])
    .filter((e) => String(e?.business_id || e?.accountId || '') === String(accountId || ''));
}

function parseRangeQuery(raw) {
  const text = String(raw || '30d').trim().toLowerCase();
  const match = text.match(/^(\d{1,3})d$/);
  if (match) {
    const value = Number(match[1]);
    if (value >= 1 && value <= 365) return value;
  }
  const value = Number(text);
  if (Number.isFinite(value) && value >= 1 && value <= 365) return value;
  return 30;
}

function normalizeOpportunitiesList(opportunities) {
  return stableNormalize(
    (Array.isArray(opportunities) ? opportunities : [])
      .map((opportunity) => ({
        id: String(opportunity?.id || ''),
        accountId: String(opportunity?.accountId || ''),
        convoKey: String(opportunity?.convoKey || ''),
        status: String(opportunity?.status || ''),
        stage: String(opportunity?.stage || ''),
        confidence: Number(opportunity?.confidence || 0),
        riskScore: Number(opportunity?.riskScore || 0),
        createdAt: Number(opportunity?.createdAt || 0),
        updatedAt: Number(opportunity?.updatedAt || 0),
        contactId: String(opportunity?.contactId || '')
      }))
      .sort((a, b) => {
        const left = `${a.updatedAt}|${a.id}`;
        const right = `${b.updatedAt}|${b.id}`;
        return left.localeCompare(right);
      })
  );
}

function normalizeFunnelResponse(response) {
  return stableNormalize({
    leadsCreated: Number(response?.leadsCreated || 0),
    replied: Number(response?.replied || 0),
    qualified: Number(response?.qualified || 0),
    booked: Number(response?.booked || 0),
    completed: Number(response?.completed || 0),
    quoteStarted: Number(response?.quoteStarted || 0),
    quoteReady: Number(response?.quoteReady || 0),
    quoteShown: Number(response?.quoteShown || 0),
    quoteAccepted: Number(response?.quoteAccepted || 0),
    startedToReadyDropoffPct: response?.startedToReadyDropoffPct == null ? null : Number(response.startedToReadyDropoffPct),
    readyToShownDropoffPct: response?.readyToShownDropoffPct == null ? null : Number(response.readyToShownDropoffPct),
    shownToAcceptedDropoffPct: response?.shownToAcceptedDropoffPct == null ? null : Number(response.shownToAcceptedDropoffPct)
  });
}

function normalizeTimelineResponse(response) {
  const timeline = (Array.isArray(response?.timeline) ? response.timeline : [])
    .map((item) => ({
      id: String(item?.id || ''),
      type: String(item?.type || ''),
      channel: String(item?.channel || ''),
      ts: Number(item?.ts || 0),
      summary: String(item?.summary || '')
    }))
    .sort((a, b) => {
      if (a.ts !== b.ts) return a.ts - b.ts;
      return String(a.id).localeCompare(String(b.id));
    });
  return stableNormalize({
    opportunityId: String(response?.opportunityId || ''),
    convoKey: String(response?.convoKey || ''),
    timeline,
    stageHistory: Array.isArray(response?.stageHistory) ? response.stageHistory : [],
    riskHistory: Array.isArray(response?.riskHistory) ? response.riskHistory : []
  });
}

function buildFunnelResponse(opportunities, data, accountId) {
  const events = scopedLeadEvents(data, accountId);
  const leadsCreated = opportunities.length;
  const repliedConvos = new Set(
    events
      .filter((e) => String(e?.type || '') === 'inbound_message')
      .map((e) => String(e?.convoKey || ''))
  ).size;
  const qualified = opportunities.filter((o) => Number(o?.confidence || 0) >= 0.5 || Number(o?.riskScore || 0) >= 25).length;
  const booked = opportunities.filter((o) => ['recovered', 'won'].includes(String(o?.status || ''))).length;
  const completed = events.filter((e) => String(e?.type || '') === 'booking_completed').length;
  const revenueEvents = getRevenueEventsForAccount(data, accountId);

  const eventKey = (event) => {
    const meta = event?.metadata_json && typeof event.metadata_json === 'object' ? event.metadata_json : {};
    const convoKey = String(meta?.convoKey || '').trim();
    if (convoKey) return convoKey;
    const contact = String(event?.contact_id || '').trim();
    if (contact) return `contact:${contact}`;
    const lead = String(event?.related_lead_event_id || '').trim();
    if (lead) return `lead:${lead}`;
    return `event:${String(event?.id || '')}`;
  };

  const countDistinctByType = (type) => {
    const keys = new Set();
    for (const event of revenueEvents) {
      if (String(event?.revenue_event_type || '') !== String(type || '')) continue;
      keys.add(eventKey(event));
    }
    return keys.size;
  };

  const pct = (from, to) => (from > 0 ? Number((((from - to) / from) * 100).toFixed(1)) : null);

  const quoteStarted = countDistinctByType('quote_started');
  const quoteReady = countDistinctByType('quote_ready');
  const quoteShown = countDistinctByType('quote_shown');
  const quoteAccepted = countDistinctByType('quote_accepted');

  return {
    leadsCreated,
    replied: repliedConvos,
    qualified,
    booked,
    completed,
    quoteStarted,
    quoteReady,
    quoteShown,
    quoteAccepted,
    startedToReadyDropoffPct: pct(quoteStarted, quoteReady),
    readyToShownDropoffPct: pct(quoteReady, quoteShown),
    shownToAcceptedDropoffPct: pct(quoteShown, quoteAccepted)
  };
}

function buildTimelineResponse(opportunity, actions, data, accountId) {
  const convoKey = String(opportunity?.convoKey || '');
  const leadEvents = (data.leadEvents || [])
    .filter((event) => String(event?.accountId || '') === String(accountId) && String(event?.convoKey || '') === convoKey)
    .map((event) => ({
      id: event.id,
      type: event.type,
      channel: event.channel,
      ts: Number(event.ts || 0),
      summary: String(event?.payload?.text || event?.payload?.body || event?.type || '').slice(0, 120),
      raw: event
    }))
    .sort((a, b) => a.ts - b.ts);

  const revenueEvents = (data.revenueEvents || [])
    .filter((event) => String(event?.business_id || '') === String(accountId))
    .filter((event) =>
      String(event?.related_lead_event_id || '') === String(leadEvents[0]?.id || '') ||
      String(event?.contact_id || '') === String(opportunity?.contactId || '')
    )
    .map((event) => ({
      id: event.id,
      type: event.revenue_event_type,
      channel: 'revenue',
      ts: Number(event.created_at || 0),
      summary: `${event.revenue_event_type} (${String(event?.status || '').toUpperCase()})`,
      raw: event
    }))
    .sort((a, b) => a.ts - b.ts);

  const mappedActions = (Array.isArray(actions) ? actions : [])
    .map((action) => ({
      id: action.id,
      type: action.actionType,
      channel: action.channel,
      ts: Number(action.ts || 0),
      summary: String(action?.payload?.messageText || action?.payload?.followupDelay || action?.actionType || '').slice(0, 120),
      raw: action
    }))
    .sort((a, b) => a.ts - b.ts);

  const timeline = [...leadEvents, ...mappedActions, ...revenueEvents].sort((a, b) => {
    if (a.ts !== b.ts) return a.ts - b.ts;
    return String(a.id || '').localeCompare(String(b.id || ''));
  });

  return {
    opportunityId: opportunity.id,
    convoKey,
    timeline,
    stageHistory: Array.isArray(opportunity?.stageHistory) ? opportunity.stageHistory : [],
    riskHistory: Array.isArray(opportunity?.riskHistory) ? opportunity.riskHistory : []
  };
}

function buildAgentMetricsResponse(opportunities, data, accountId, days) {
  const since = Date.now() - (days * DAY_MS);
  const runs = (data.agentRuns || [])
    .filter((run) => String(run?.accountId || '') === String(accountId))
    .filter((run) => Number(run?.createdAt || 0) >= since);

  const runsStarted = runs.length;
  const runsCompleted = runs.filter((run) => String(run?.status || '') === 'COMPLETED').length;
  const runsFailed = runs.filter((run) => ['FAILED', 'CANCELLED'].includes(String(run?.status || ''))).length;
  const avgStepsToRecovery = (() => {
    const completed = runs.filter((run) => String(run?.status || '') === 'COMPLETED');
    if (!completed.length) return null;
    const counts = completed.map((run) => (Array.isArray(run?.stepState?.completedSteps) ? run.stepState.completedSteps.length : 0));
    return Number((counts.reduce((a, b) => a + b, 0) / Math.max(1, counts.length)).toFixed(2));
  })();

  const eventsByConvo = new Map();
  for (const event of data.leadEvents || []) {
    if (String(event?.accountId || '') !== String(accountId)) continue;
    const convoKey = String(event?.convoKey || '');
    if (!convoKey) continue;
    if (!eventsByConvo.has(convoKey)) eventsByConvo.set(convoKey, []);
    eventsByConvo.get(convoKey).push(event);
  }

  const opportunitiesById = new Map((Array.isArray(opportunities) ? opportunities : []).map((opportunity) => [String(opportunity?.id || ''), opportunity]));
  let deltaSum = 0;
  let deltaCount = 0;
  for (const run of runs) {
    const opportunity = opportunitiesById.get(String(run?.opportunityId || ''));
    const convoKey = String(opportunity?.convoKey || '');
    if (!convoKey || !eventsByConvo.has(convoKey)) continue;
    const events = eventsByConvo.get(convoKey).slice().sort((a, b) => Number(a?.ts || 0) - Number(b?.ts || 0));
    const firstInboundAfterRun = events.find((event) => String(event?.type || '') === 'inbound_message' && Number(event?.ts || 0) >= Number(run?.createdAt || 0));
    if (!firstInboundAfterRun) continue;
    const minutes = (Number(firstInboundAfterRun.ts) - Number(run.createdAt || 0)) / 60000;
    if (minutes >= 0) {
      deltaSum += minutes;
      deltaCount += 1;
    }
  }

  const recoveredOpps = opportunities.filter((opportunity) => ['recovered', 'won'].includes(String(opportunity?.status || '').toLowerCase())).length;
  const totalOpps = opportunities.length;

  return {
    rangeDays: days,
    runsStarted,
    runsCompleted,
    runsFailed,
    averageStepsToRecovery: avgStepsToRecovery,
    recoveryRate: totalOpps ? Number((recoveredOpps / totalOpps).toFixed(3)) : 0,
    avgMinutesToFirstResponseAfterRun: deltaCount ? Number((deltaSum / deltaCount).toFixed(1)) : null
  };
}

async function listDbOpportunities(accountId, meta = {}) {
  const db = meta?.db || pool;
  return listByTenant(db, accountId);
}

async function getDbOpportunity(accountId, opportunityId, meta = {}) {
  const db = meta?.db || pool;
  return getById(db, accountId, opportunityId);
}

async function getFunnelMetrics(accountId, meta = {}) {
  const data = loadData();
  const oldFactory = async () => buildFunnelResponse(scopedSnapshotOpportunities(data, accountId), data, accountId);
  const newFactory = async () => buildFunnelResponse(await listDbOpportunities(accountId, meta), data, accountId);

  if (USE_DB_OPPORTUNITIES) {
    return verifyParity(
      {
        entity: 'opportunity',
        service: 'opportunitiesService.getFunnelMetrics',
        accountId,
        identifiers: meta?.identifiers || null
      },
      oldFactory,
      newFactory,
      normalizeFunnelResponse
    );
  }

  return oldFactory();
}

async function getOpportunityTimeline(accountId, opportunityId, meta = {}) {
  const data = loadData();
  const actionsService = require('./actionsService');
  const oldFactory = async () => {
    const opportunity = scopedSnapshotOpportunities(data, accountId).find((item) => String(item?.id || '') === String(opportunityId || ''));
    if (!opportunity) return null;
    const actions = await actionsService.listActionsForOpportunitySnapshot(accountId, opportunityId);
    return buildTimelineResponse(opportunity, actions, data, accountId);
  };
  const newFactory = async () => {
    const opportunity = USE_DB_OPPORTUNITIES
      ? await getDbOpportunity(accountId, opportunityId, meta)
      : scopedSnapshotOpportunities(data, accountId).find((item) => String(item?.id || '') === String(opportunityId || ''));
    if (!opportunity) return null;
    const actions = await actionsService.listActionsForOpportunityPreferred(accountId, opportunityId, meta);
    return buildTimelineResponse(opportunity, actions, data, accountId);
  };

  if (USE_DB_OPPORTUNITIES || USE_DB_ACTIONS) {
    return verifyParity(
      {
        entity: 'opportunity',
        service: 'opportunitiesService.getOpportunityTimeline',
        accountId,
        identifiers: { ...(meta?.identifiers || {}), opportunityId: String(opportunityId || '') }
      },
      oldFactory,
      newFactory,
      normalizeTimelineResponse
    );
  }

  return oldFactory();
}

async function getAgentMetrics(accountId, rangeDays, meta = {}) {
  const days = parseRangeQuery(rangeDays);
  const data = loadData();
  const oldFactory = async () => buildAgentMetricsResponse(scopedSnapshotOpportunities(data, accountId), data, accountId, days);
  const newFactory = async () => buildAgentMetricsResponse(await listDbOpportunities(accountId, meta), data, accountId, days);

  if (USE_DB_OPPORTUNITIES) {
    return verifyParity(
      {
        entity: 'opportunity',
        service: 'opportunitiesService.getAgentMetrics',
        accountId,
        identifiers: { ...(meta?.identifiers || {}), rangeDays: days }
      },
      oldFactory,
      newFactory,
      stableNormalize
    );
  }

  return oldFactory();
}

function logWriteFailure(fields) {
  console.error(JSON.stringify({
    level: 'error',
    entity: 'opportunity',
    service: String(fields?.service || 'opportunitiesService'),
    operation: String(fields?.operation || 'unknown'),
    accountId: String(fields?.accountId || ''),
    opportunityId: fields?.opportunityId ? String(fields.opportunityId) : null,
    route: fields?.route ? String(fields.route) : null,
    requestId: fields?.requestId ? String(fields.requestId) : null,
    errorType: String(fields?.errorType || 'unknown'),
    message: fields?.error?.message ? String(fields.error.message) : null
  }));
}

function cloneOpportunity(opportunity) {
  return opportunity && typeof opportunity === 'object'
    ? JSON.parse(JSON.stringify(opportunity))
    : null;
}

function toRepoOpportunityInput(opportunity) {
  return {
    id: String(opportunity?.id || ''),
    convoKey: opportunity?.convoKey ? String(opportunity.convoKey) : null,
    conversationId: opportunity?.convoKey ? String(opportunity.convoKey) : null,
    stage: String(opportunity?.stage || 'NEW'),
    riskScore: Number(opportunity?.riskScore ?? 0),
    createdAt: Number(opportunity?.createdAt || Date.now()),
    updatedAt: Number(opportunity?.updatedAt || Date.now()),
    payload: cloneOpportunity(opportunity) || {}
  };
}

function syncOpportunitySnapshot(opportunity) {
  const data = loadData();
  data.revenueOpportunities = Array.isArray(data.revenueOpportunities) ? data.revenueOpportunities : [];
  const idx = data.revenueOpportunities.findIndex((item) => String(item?.id || '') === String(opportunity?.id || '') && String(item?.accountId || '') === String(opportunity?.accountId || ''));
  const snapshotValue = cloneOpportunity(opportunity);
  if (idx >= 0) {
    data.revenueOpportunities[idx] = snapshotValue;
  } else {
    data.revenueOpportunities.push(snapshotValue);
  }
  saveDataDebounced(data);
  return data.revenueOpportunities[idx >= 0 ? idx : data.revenueOpportunities.length - 1] || null;
}

async function persistSnapshotOpportunity(accountId, opportunity, meta = {}) {
  if (!opportunity?.id) return null;
  try {
    const db = meta?.db || pool;
    const persisted = await withTransaction(db, async (client) => {
      const existing = await getById(client, accountId, opportunity.id);
      const input = toRepoOpportunityInput(opportunity);
      if (existing) {
        return updateById(client, accountId, opportunity.id, input);
      }
      return create(client, accountId, input);
    });
    const snapshotRef = syncOpportunitySnapshot({ ...cloneOpportunity(opportunity), ...persisted, accountId: String(accountId) });
    if (!snapshotRef) {
      const err = new Error('Opportunity snapshot sync failed');
      logWriteFailure({
        service: 'opportunitiesService.persistSnapshotOpportunity',
        operation: meta?.operation || 'persist',
        accountId,
        opportunityId: opportunity.id,
        route: meta?.route,
        requestId: meta?.requestId,
        errorType: 'snapshot_sync_failed',
        error: err
      });
      throw err;
    }
    return snapshotRef;
  } catch (err) {
    logWriteFailure({
      service: 'opportunitiesService.persistSnapshotOpportunity',
      operation: meta?.operation || 'persist',
      accountId,
      opportunityId: opportunity.id,
      route: meta?.route,
      requestId: meta?.requestId,
      errorType: 'write_consistency_failed',
      error: err
    });
    throw err;
  }
}

async function setAutomationState(accountId, opportunityId, stopAutomation, meta = {}) {
  const data = loadData();
  const current = scopedSnapshotOpportunities(data, accountId).find((item) => String(item?.id || '') === String(opportunityId || ''));
  if (!current) return null;
  const next = cloneOpportunity(current);
  next.stopAutomation = stopAutomation === true;
  next.updatedAt = Date.now();
  return persistSnapshotOpportunity(accountId, next, {
    ...meta,
    operation: stopAutomation === true ? 'pause' : 'resume'
  });
}

module.exports = {
  getFunnelMetrics,
  getOpportunityTimeline,
  getAgentMetrics,
  persistSnapshotOpportunity,
  setAutomationState,
  parseRangeQuery,
  normalizeFunnelResponse,
  normalizeOpportunitiesList,
  normalizeTimelineResponse
};
