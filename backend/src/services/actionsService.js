const { pool } = require('../db/pool');
const { withTransaction } = require('../db/withTransaction');
const { USE_DB_ACTIONS } = require('../config/runtime');
const { listRecentByTenant, listByOpportunity, listByRunIds, create, updateById, hasSuccessfulByIdempotency } = require('../repositories/actionsRepo');
const { loadData, saveDataDebounced } = require('../store/dataStore');
const { buildActivitySummary } = require('./actionLogger');
const { verifyParity, stableNormalize } = require('./migrationParityService');
const { parseRangeQuery } = require('./opportunitiesService');

const DAY_MS = 24 * 60 * 60 * 1000;

function listActionsForOpportunitySnapshot(accountId, opportunityId) {
  const data = loadData();
  return (data.actions || [])
    .filter((action) => String(action?.accountId || '') === String(accountId))
    .filter((action) => String(action?.opportunityId || '') === String(opportunityId || ''));
}

async function listActionsForOpportunityDb(accountId, opportunityId, meta = {}) {
  const db = meta?.db || pool;
  return listByOpportunity(db, accountId, opportunityId);
}

async function listActionsForOpportunityPreferred(accountId, opportunityId, meta = {}) {
  if (USE_DB_ACTIONS) {
    return listActionsForOpportunityDb(accountId, opportunityId, meta);
  }
  return listActionsForOpportunitySnapshot(accountId, opportunityId);
}

function normalizeActionForFeed(action) {
  return {
    ...(action || {}),
    summary: buildActivitySummary(action)
  };
}

function buildActivityFeedResponse(actions, limit) {
  const items = (Array.isArray(actions) ? actions : [])
    .slice(0, limit)
    .map(normalizeActionForFeed);
  const groupsMap = new Map();
  for (const item of items) {
    const key = String(item?.correlationId || item?.runId || item?.id || '');
    if (!groupsMap.has(key)) groupsMap.set(key, { correlationId: key, runId: item?.runId || null, actions: [] });
    groupsMap.get(key).actions.push(item);
  }
  const groups = Array.from(groupsMap.values())
    .map((group) => ({
      ...group,
      actions: group.actions.sort((a, b) => {
        const delta = Number(a?.ts || 0) - Number(b?.ts || 0);
        if (delta !== 0) return delta;
        return String(a?.id || '').localeCompare(String(b?.id || ''));
      })
    }))
    .sort((a, b) => {
      const left = Number(b?.actions?.[0]?.ts || 0) - Number(a?.actions?.[0]?.ts || 0);
      if (left !== 0) return left;
      return String(a?.correlationId || '').localeCompare(String(b?.correlationId || ''));
    });
  return { items, groups };
}

function normalizeActivityFeedResponse(response) {
  return stableNormalize({
    items: (Array.isArray(response?.items) ? response.items : [])
      .map((item) => ({
        id: String(item?.id || ''),
        correlationId: String(item?.correlationId || ''),
        runId: item?.runId ? String(item.runId) : null,
        actionType: String(item?.actionType || ''),
        ts: Number(item?.ts || 0),
        summary: String(item?.summary || '')
      }))
      .sort((a, b) => {
        if (a.ts !== b.ts) return b.ts - a.ts;
        return String(b.id).localeCompare(String(a.id));
      }),
    groups: (Array.isArray(response?.groups) ? response.groups : [])
      .map((group) => ({
        correlationId: String(group?.correlationId || ''),
        runId: group?.runId ? String(group.runId) : null,
        actions: (Array.isArray(group?.actions) ? group.actions : [])
          .map((item) => ({
            id: String(item?.id || ''),
            ts: Number(item?.ts || 0),
            actionType: String(item?.actionType || '')
          }))
          .sort((a, b) => {
            if (a.ts !== b.ts) return a.ts - b.ts;
            return String(a.id).localeCompare(String(b.id));
          })
      }))
      .sort((a, b) => String(a.correlationId).localeCompare(String(b.correlationId)))
  });
}

function buildPlaybookPerformanceResponse(actions, accountId, days) {
  const data = loadData();
  const since = Date.now() - (days * DAY_MS);
  const runs = (data.agentRuns || [])
    .filter((run) => String(run?.accountId || '') === String(accountId))
    .filter((run) => Number(run?.createdAt || 0) >= since);

  const aggregate = {};
  for (const run of runs) {
    const playbookId = String(run?.plan?.playbookId || 'unknown');
    if (!aggregate[playbookId]) {
      aggregate[playbookId] = {
        playbookId,
        runs: 0,
        completed: 0,
        failed: 0,
        byStepType: {}
      };
    }
    aggregate[playbookId].runs += 1;
    if (String(run?.status || '') === 'COMPLETED') aggregate[playbookId].completed += 1;
    if (['FAILED', 'CANCELLED'].includes(String(run?.status || ''))) aggregate[playbookId].failed += 1;
  }

  const runsById = new Map(runs.map((run) => [String(run?.id || ''), run]));
  for (const action of Array.isArray(actions) ? actions : []) {
    const run = runsById.get(String(action?.runId || ''));
    if (!run) continue;
    const playbookId = String(run?.plan?.playbookId || 'unknown');
    if (!aggregate[playbookId]) continue;
    const stepType = String(action?.actionType || 'unknown').toUpperCase();
    const bucket = aggregate[playbookId].byStepType[stepType] || { sent: 0, failed: 0, skipped: 0 };
    const outcome = String(action?.outcome?.status || action?.status || 'pending');
    if (outcome === 'sent') bucket.sent += 1;
    else if (outcome === 'failed') bucket.failed += 1;
    else if (outcome === 'skipped') bucket.skipped += 1;
    aggregate[playbookId].byStepType[stepType] = bucket;
  }

  const items = Object.values(aggregate)
    .map((item) => ({
      ...item,
      completionRate: item.runs ? Number((item.completed / item.runs).toFixed(3)) : 0
    }))
    .sort((a, b) => {
      const delta = b.runs - a.runs;
      if (delta !== 0) return delta;
      return String(a.playbookId).localeCompare(String(b.playbookId));
    });

  return { rangeDays: days, items };
}

function normalizePlaybookPerformanceResponse(response) {
  return stableNormalize({
    rangeDays: Number(response?.rangeDays || 0),
    items: (Array.isArray(response?.items) ? response.items : [])
      .map((item) => ({
        playbookId: String(item?.playbookId || ''),
        runs: Number(item?.runs || 0),
        completed: Number(item?.completed || 0),
        failed: Number(item?.failed || 0),
        completionRate: Number(item?.completionRate || 0),
        byStepType: item?.byStepType || {}
      }))
      .sort((a, b) => String(a.playbookId).localeCompare(String(b.playbookId)))
  });
}

async function getActivityFeed(accountId, limit, meta = {}) {
  const n = Math.max(1, Math.min(200, Number(limit || 50)));
  const data = loadData();
  const oldFactory = async () => {
    const actions = (data.actions || [])
      .filter((action) => String(action?.accountId || '') === String(accountId))
      .sort((a, b) => {
        const delta = Number(b?.ts || 0) - Number(a?.ts || 0);
        if (delta !== 0) return delta;
        return String(b?.id || '').localeCompare(String(a?.id || ''));
      });
    return buildActivityFeedResponse(actions, n);
  };
  const newFactory = async () => {
    const db = meta?.db || pool;
    const actions = await listRecentByTenant(db, accountId, n);
    return buildActivityFeedResponse(actions, n);
  };

  if (USE_DB_ACTIONS) {
    return verifyParity(
      {
        entity: 'action',
        service: 'actionsService.getActivityFeed',
        accountId,
        identifiers: { ...(meta?.identifiers || {}), limit: n }
      },
      oldFactory,
      newFactory,
      normalizeActivityFeedResponse
    );
  }

  return oldFactory();
}

async function getPlaybookPerformance(accountId, rangeDays, meta = {}) {
  const days = parseRangeQuery(rangeDays);
  const since = Date.now() - (days * DAY_MS);
  const data = loadData();
  const oldFactory = async () => {
    const actions = (data.actions || [])
      .filter((action) => String(action?.accountId || '') === String(accountId))
      .filter((action) => Number(action?.ts || 0) >= since)
      .filter((action) => action?.runId);
    return buildPlaybookPerformanceResponse(actions, accountId, days);
  };
  const newFactory = async () => {
    const db = meta?.db || pool;
    const runs = (data.agentRuns || [])
      .filter((run) => String(run?.accountId || '') === String(accountId))
      .filter((run) => Number(run?.createdAt || 0) >= since);
    const runIds = runs.map((run) => String(run?.id || ''));
    const actions = (await listByRunIds(db, accountId, runIds))
      .filter((action) => Number(action?.ts || 0) >= since)
      .filter((action) => action?.runId);
    return buildPlaybookPerformanceResponse(actions, accountId, days);
  };

  if (USE_DB_ACTIONS) {
    return verifyParity(
      {
        entity: 'action',
        service: 'actionsService.getPlaybookPerformance',
        accountId,
        identifiers: { ...(meta?.identifiers || {}), rangeDays: days }
      },
      oldFactory,
      newFactory,
      normalizePlaybookPerformanceResponse
    );
  }

  return oldFactory();
}

function logWriteFailure(fields) {
  console.error(JSON.stringify({
    level: 'error',
    entity: 'action',
    service: String(fields?.service || 'actionsService'),
    operation: String(fields?.operation || 'unknown'),
    accountId: String(fields?.accountId || ''),
    actionId: fields?.actionId ? String(fields.actionId) : null,
    opportunityId: fields?.opportunityId ? String(fields.opportunityId) : null,
    route: fields?.route ? String(fields.route) : null,
    requestId: fields?.requestId ? String(fields.requestId) : null,
    errorType: String(fields?.errorType || 'unknown'),
    message: fields?.error?.message ? String(fields.error.message) : null
  }));
}

function cloneAction(action) {
  return action && typeof action === 'object'
    ? JSON.parse(JSON.stringify(action))
    : null;
}

function deriveIdempotencyKey(input = {}) {
  if (input?.idempotencyKey) return String(input.idempotencyKey);
  const payload = input?.payload && typeof input.payload === 'object' ? input.payload : {};
  const signalId = String(payload?.signalId || '').trim();
  if (input?.runId && input?.stepId) {
    return `run:${String(input.accountId || '')}:${String(input.runId)}:${String(input.stepId)}:${String(input.actionType || 'unknown')}`;
  }
  if (signalId) {
    return `signal:${String(input.accountId || '')}:${signalId}:${String(input.actionType || 'unknown')}:${String(payload?.skippedReason || '')}`;
  }
  if (payload?.alertId) {
    return `alert:${String(input.accountId || '')}:${String(payload.alertId)}:${String(input.actionType || 'unknown')}`;
  }
  return null;
}

function normalizeActionInput(input = {}) {
  const payload = input?.payload && typeof input.payload === 'object' ? { ...input.payload } : {};
  const justification = input?.justification && typeof input.justification === 'object' ? { ...input.justification } : {};
  const outcome = input?.outcome && typeof input.outcome === 'object' ? { ...input.outcome } : { status: 'pending', error: null };
  const idempotencyKey = deriveIdempotencyKey(input);
  return {
    id: String(input?.id || require('../utils/id').generateId()),
    accountId: String(input?.accountId || ''),
    opportunityId: input?.opportunityId ? String(input.opportunityId) : null,
    contactId: input?.contactId ? String(input.contactId) : null,
    convoKey: input?.convoKey ? String(input.convoKey) : null,
    ts: Number(input?.ts || Date.now()),
    actionType: String(input?.actionType || 'unknown_action'),
    runId: input?.runId ? String(input.runId) : null,
    stepId: input?.stepId ? String(input.stepId) : null,
    correlationId: input?.correlationId ? String(input.correlationId) : null,
    idempotencyKey,
    dryRun: input?.dryRun === true,
    channel: String(input?.channel || 'sms'),
    payload,
    justification,
    outcome
  };
}

function syncActionSnapshot(action) {
  const data = loadData();
  data.actions = Array.isArray(data.actions) ? data.actions : [];
  const idx = data.actions.findIndex((item) => String(item?.id || '') === String(action?.id || ''));
  const snapshotValue = cloneAction(action);
  if (idx >= 0) {
    data.actions[idx] = snapshotValue;
  } else {
    data.actions.push(snapshotValue);
  }
  saveDataDebounced(data);
  return data.actions[idx >= 0 ? idx : data.actions.length - 1] || null;
}

async function logActionStartWrite(input = {}, meta = {}) {
  const normalized = normalizeActionInput(input);
  try {
    const db = meta?.db || pool;
    const persisted = await create(db, normalized.accountId, {
      ...normalized,
      status: String(normalized?.outcome?.status || 'pending'),
      createdAt: normalized.ts,
      payload: cloneAction(normalized)
    });
    const snapshot = syncActionSnapshot({
      ...normalized,
      ...persisted,
      outcome: normalized.outcome,
      justification: normalized.justification
    });
    if (!snapshot) throw new Error('Action snapshot sync failed');
    return snapshot;
  } catch (err) {
    logWriteFailure({
      service: 'actionsService.logActionStartWrite',
      operation: meta?.operation || 'log_start',
      accountId: normalized.accountId,
      actionId: normalized.id,
      opportunityId: normalized.opportunityId,
      route: meta?.route,
      requestId: meta?.requestId,
      errorType: 'write_consistency_failed',
      error: err
    });
    throw err;
  }
}

async function logActionResultWrite(accountId, actionId, outcome = {}, meta = {}) {
  const data = loadData();
  const snapshotAction = (data.actions || []).find((item) => String(item?.id || '') === String(actionId || '') && String(item?.accountId || '') === String(accountId || ''));
  if (!snapshotAction) return null;
  const next = cloneAction(snapshotAction);
  next.outcome = {
    status: String(outcome.status || next?.outcome?.status || 'pending'),
    error: outcome.error ? String(outcome.error) : null
  };
  if (outcome.justification && typeof outcome.justification === 'object') {
    next.justification = {
      ...(next.justification || {}),
      ...outcome.justification,
      policy: {
        ...(next?.justification?.policy || {}),
        ...(outcome?.justification?.policy || {})
      }
    };
  }
  try {
    const persisted = await updateById(meta?.db || pool, accountId, actionId, {
      status: next.outcome.status,
      payload: next
    });
    const snapshot = syncActionSnapshot({ ...next, ...persisted });
    if (!snapshot) throw new Error('Action snapshot sync failed');
    return snapshot;
  } catch (err) {
    logWriteFailure({
      service: 'actionsService.logActionResultWrite',
      operation: meta?.operation || 'log_result',
      accountId,
      actionId,
      opportunityId: next?.opportunityId,
      route: meta?.route,
      requestId: meta?.requestId,
      errorType: 'write_consistency_failed',
      error: err
    });
    throw err;
  }
}

async function attachActionToOpportunityWrite(accountId, opportunityId, actionId, meta = {}) {
  const { persistSnapshotOpportunity } = require('./opportunitiesService');
  const data = loadData();
  const opportunity = (data.revenueOpportunities || []).find((item) => String(item?.accountId || '') === String(accountId || '') && String(item?.id || '') === String(opportunityId || ''));
  if (!opportunity) return null;
  const next = JSON.parse(JSON.stringify(opportunity));
  next.actionLogIds = Array.isArray(next.actionLogIds) ? next.actionLogIds : [];
  const id = String(actionId || '');
  if (id && !next.actionLogIds.includes(id)) {
    next.actionLogIds.push(id);
    next.actionLogIds = next.actionLogIds.slice(-200);
  }
  return persistSnapshotOpportunity(accountId, next, {
    ...meta,
    operation: 'attach_action'
  });
}

async function hasSuccessfulActionByIdempotencyWrite(accountId, idempotencyKey, meta = {}) {
  try {
    return hasSuccessfulByIdempotency(meta?.db || pool, accountId, idempotencyKey);
  } catch (err) {
    logWriteFailure({
      service: 'actionsService.hasSuccessfulActionByIdempotencyWrite',
      operation: meta?.operation || 'idempotency_lookup',
      accountId,
      actionId: null,
      opportunityId: null,
      route: meta?.route,
      requestId: meta?.requestId,
      errorType: 'tenant_scope_violation_blocked',
      error: err
    });
    throw err;
  }
}

module.exports = {
  getActivityFeed,
  getPlaybookPerformance,
  logActionStartWrite,
  logActionResultWrite,
  attachActionToOpportunityWrite,
  hasSuccessfulActionByIdempotencyWrite,
  listActionsForOpportunitySnapshot,
  listActionsForOpportunityDb,
  listActionsForOpportunityPreferred,
  normalizeActivityFeedResponse,
  normalizePlaybookPerformanceResponse
};
