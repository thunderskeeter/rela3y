const { loadData, saveDataDebounced } = require('../store/dataStore');
const { generateId } = require('../utils/id');

function defaultJustification(input = {}) {
  return {
    trigger: String(input.trigger || 'signal_ingest'),
    riskScore: Number(input.riskScore || 0),
    reasons: Array.isArray(input.reasons) ? input.reasons.map((x) => String(x)) : [],
    stageBefore: String(input.stageBefore || 'NEW'),
    stageAfter: String(input.stageAfter || input.stageBefore || 'NEW'),
    decisionVersion: String(input.decisionVersion || 'deterministic_v2'),
    policy: {
      dailyCap: Number(input?.policy?.dailyCap || 2),
      cooldownMinutes: Number(input?.policy?.cooldownMinutes || 30),
      quietHours: input?.policy?.quietHours === true,
      complianceChecked: input?.policy?.complianceChecked !== false
    }
  };
}

function logActionStart(input = {}) {
  const data = loadData();
  data.actions = Array.isArray(data.actions) ? data.actions : [];
  const item = {
    id: generateId(),
    accountId: String(input.accountId || ''),
    opportunityId: input.opportunityId ? String(input.opportunityId) : null,
    contactId: input.contactId ? String(input.contactId) : null,
    convoKey: input.convoKey ? String(input.convoKey) : null,
    ts: Number(input.ts || Date.now()),
    actionType: String(input.actionType || 'unknown_action'),
    runId: input.runId ? String(input.runId) : null,
    stepId: input.stepId ? String(input.stepId) : null,
    correlationId: input.correlationId ? String(input.correlationId) : null,
    idempotencyKey: input.idempotencyKey ? String(input.idempotencyKey) : null,
    dryRun: input.dryRun === true,
    channel: String(input.channel || 'sms'),
    payload: input.payload && typeof input.payload === 'object' ? input.payload : {},
    justification: defaultJustification(input.justification),
    outcome: {
      status: 'pending',
      error: null
    }
  };
  data.actions.push(item);
  saveDataDebounced(data);
  return item;
}

function hasSuccessfulActionByIdempotency(accountId, idempotencyKey) {
  const data = loadData();
  const key = String(idempotencyKey || '').trim();
  if (!key) return false;
  return (data.actions || []).some((a) =>
    String(a?.accountId || '') === String(accountId || '') &&
    String(a?.idempotencyKey || '') === key &&
    String(a?.outcome?.status || '') === 'sent'
  );
}

function logActionResult(actionId, outcome = {}) {
  const data = loadData();
  data.actions = Array.isArray(data.actions) ? data.actions : [];
  const action = data.actions.find((a) => String(a?.id || '') === String(actionId || ''));
  if (!action) return null;
  action.outcome = {
    status: String(outcome.status || action.outcome?.status || 'pending'),
    error: outcome.error ? String(outcome.error) : null
  };
  if (outcome.justification && typeof outcome.justification === 'object') {
    action.justification = {
      ...action.justification,
      ...outcome.justification,
      policy: {
        ...(action.justification?.policy || {}),
        ...(outcome.justification.policy || {})
      }
    };
  }
  saveDataDebounced(data);
  return action;
}

function attachActionToOpportunity(accountId, opportunityId, actionId) {
  const data = loadData();
  const opp = (data.revenueOpportunities || []).find((o) =>
    String(o?.accountId || '') === String(accountId || '') &&
    String(o?.id || '') === String(opportunityId || '')
  );
  if (!opp) return null;
  opp.actionLogIds = Array.isArray(opp.actionLogIds) ? opp.actionLogIds : [];
  const id = String(actionId || '');
  if (id && !opp.actionLogIds.includes(id)) {
    opp.actionLogIds.push(id);
    opp.actionLogIds = opp.actionLogIds.slice(-200);
  }
  saveDataDebounced(data);
  return opp;
}

function buildActivitySummary(action) {
  const type = String(action?.actionType || '').replace(/_/g, ' ');
  const risk = Number(action?.justification?.riskScore || 0);
  const reasons = Array.isArray(action?.justification?.reasons) ? action.justification.reasons : [];
  const reason = reasons[0] || 'policy_evaluation';
  const prefix = action?.stepId ? `[${String(action.stepId)}] ` : '';
  return `${prefix}${type} (Risk ${risk}) - reason: ${reason}`;
}

module.exports = {
  logActionStart,
  logActionResult,
  attachActionToOpportunity,
  buildActivitySummary,
  hasSuccessfulActionByIdempotency
};
