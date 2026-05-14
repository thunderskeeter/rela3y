const {
  loadData,
  saveDataDebounced,
  getAccountById,
  getConversationById
} = require('../store/dataStore');
const {
  createLeadEvent,
  getPolicyConfig,
  ensureOpportunityDefaults
} = require('./revenueIntelligenceService');
const { evaluateTrigger } = require('./automationEngine');
const { transitionStage } = require('./opportunityLifecycle');
const { createActionPlan } = require('./decisionEngine');
const { executeRevenueAction } = require('./actionExecutor');
const { persistSnapshotOpportunity } = require('./opportunitiesService');
const { logActionStartWrite, logActionResultWrite, attachActionToOpportunityWrite } = require('./actionsService');
const OUTBOUND_ACTION_TYPES = new Set(['send_message', 'ask_qualifying', 'offer_booking', 'offer_times']);

function dayKey(ts = Date.now()) {
  return new Date(ts).toISOString().slice(0, 10);
}

function toConversationParts(convoKey) {
  const [to, from] = String(convoKey || '').split('__');
  if (!to || !from) return null;
  return { to, from };
}

function isWithinPolicyQuietHours(policy, ts = Date.now()) {
  const q = policy?.quietHours || {};
  const start = Number(q.startHour);
  const end = Number(q.endHour);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return false;
  const h = new Date(ts).getHours();
  if (start === end) return false;
  if (start < end) return h >= start && h < end;
  return h >= start || h < end;
}

function nextAllowedSendTime(policy, ts = Date.now()) {
  const q = policy?.quietHours || {};
  const end = Number(q.endHour);
  if (!Number.isFinite(end)) return ts + (60 * 60 * 1000);
  const d = new Date(ts);
  if (!isWithinPolicyQuietHours(policy, ts)) return ts;
  d.setHours(end, 0, 0, 0);
  if (d.getTime() <= ts) d.setDate(d.getDate() + 1);
  return d.getTime();
}

function refreshDailyCounters(opportunity, nowTs = Date.now()) {
  const dk = dayKey(nowTs);
  if (String(opportunity.followupsDayKey || '') !== dk) {
    opportunity.followupsDayKey = dk;
    opportunity.followupsSentToday = 0;
  }
  if (String(opportunity.automationsDayKey || '') !== dk) {
    opportunity.automationsDayKey = dk;
    opportunity.automationsSentToday = 0;
  }
}

function capDecision(opportunity, policy, actionType) {
  if (opportunity.stopAutomation === true) return { blocked: true, reason: 'stop_automation' };
  refreshDailyCounters(opportunity);
  if ((opportunity.cooldownUntil || 0) > Date.now()) return { blocked: true, reason: 'cooldown_active' };
  if (Number(opportunity.automationsSentToday || 0) >= Number(policy.maxAutomationsPerOpportunityPerDay || 4)) {
    return { blocked: true, reason: 'daily_automation_cap' };
  }
  if (['send_message', 'schedule_followup', 'offer_booking', 'ask_qualifying'].includes(actionType)) {
    if (Number(opportunity.followupsSentToday || 0) >= Number(policy.dailyFollowupCapPerLead || 2)) {
      return { blocked: true, reason: 'daily_followup_cap' };
    }
  }
  if (
    opportunity.lastRecommendedActionType &&
    opportunity.lastRecommendedActionType === actionType &&
    opportunity.lastRecommendedActionAt &&
    (Date.now() - Number(opportunity.lastRecommendedActionAt)) < (Number(policy.minCooldownMinutes || 30) * 60 * 1000)
  ) {
    return { blocked: true, reason: 'duplicate_action_within_cooldown' };
  }
  return { blocked: false };
}

function justificationFor(opportunity, policy, trigger, stageBefore, stageAfter, reasons = []) {
  return {
    trigger,
    riskScore: Number(opportunity?.riskScore || 0),
    reasons: Array.isArray(reasons) ? reasons : [],
    stageBefore: String(stageBefore || opportunity?.stage || 'NEW'),
    stageAfter: String(stageAfter || stageBefore || 'NEW'),
    decisionVersion: 'deterministic_v2',
    policy: {
      dailyCap: Number(policy.dailyFollowupCapPerLead || 2),
      cooldownMinutes: Number(policy.minCooldownMinutes || 30),
      quietHours: isWithinPolicyQuietHours(policy, Date.now()),
      complianceChecked: true
    }
  };
}

async function createPolicySkipAction({ accountId, opportunity, actionType, reason, trigger, policy, signalId = null }) {
  const action = await logActionStartWrite({
    accountId,
    opportunityId: opportunity?.id || null,
    contactId: opportunity?.contactId || null,
    convoKey: opportunity?.convoKey || null,
    actionType,
    channel: 'sms',
    payload: { skippedReason: reason, signalId },
    justification: justificationFor(opportunity, policy, trigger, opportunity?.stage, opportunity?.stage, [reason])
  });
  await logActionResultWrite(accountId, action.id, { status: 'skipped', error: reason });
  await attachActionToOpportunityWrite(accountId, opportunity?.id, action.id);
  return action;
}


function applyInboundStopSignals(opportunity, leadEvent) {
  const type = String(leadEvent?.type || '').toLowerCase();
  const txt = String(leadEvent?.payload?.text || leadEvent?.payload?.body || '').trim().toUpperCase();
  const stopWords = ['STOP', 'UNSUBSCRIBE', 'CANCEL', 'END', 'QUIT'];
  if (type === 'opt_out' || stopWords.includes(txt)) {
    opportunity.stopAutomation = true;
    opportunity.status = 'lost';
    transitionStage(opportunity, 'LOST', 'opt_out_stop_keyword');
    return true;
  }
  return false;
}

async function handleSignal(accountId, leadEvent) {
  const { upsertRevenueOpportunityFromEvent, evaluateOpportunity } = require('./revenueIntelligenceService');
  const opportunity = upsertRevenueOpportunityFromEvent(accountId, leadEvent);
  if (!opportunity) return { ok: false, reason: 'opportunity_not_found' };
  await persistSnapshotOpportunity(accountId, opportunity, { operation: 'upsert_from_signal' });
  const evaluated = evaluateOpportunity(accountId, opportunity.id);
  if (!evaluated) return { ok: false, reason: 'evaluate_failed' };
  await persistSnapshotOpportunity(accountId, evaluated, { operation: 'evaluate_from_signal' });
  ensureOpportunityDefaults(evaluated);
  refreshDailyCounters(evaluated);
  applyInboundStopSignals(evaluated, leadEvent);
  await persistSnapshotOpportunity(accountId, evaluated, { operation: 'post_signal_mutation' });

  const trigger = String(leadEvent?.payload?.source || 'signal_ingest').includes('manual_run_recommended')
    ? 'manual_run_recommended'
    : String(leadEvent?.payload?.source || '').includes('prm')
      ? 'prm_tick'
      : 'signal_ingest';

  const convoKey = String(evaluated?.convoKey || '');
  const parts = toConversationParts(convoKey);
  let missedCallAutomationSent = false;
  if (parts && String(leadEvent?.type || '') === 'missed_call') {
    const automationActions = await evaluateTrigger('missed_call', {
      tenant: { accountId: String(accountId), to: String(parts.to) },
      to: String(parts.to),
      from: String(parts.from),
      eventData: { source: 'revenue_orchestrator' }
    });
    missedCallAutomationSent = Array.isArray(automationActions) && automationActions.some((a) => String(a?.action || '') === 'sent');
  }

  const plan = createActionPlan(accountId, leadEvent, evaluated);
  let actionResult = { ok: true, skipped: true, reason: 'do_nothing' };

  const desiredActionType = String(plan.nextAction || 'do_nothing');
  const suppressOutboundForMissedCall = missedCallAutomationSent && OUTBOUND_ACTION_TYPES.has(desiredActionType);
  if (suppressOutboundForMissedCall) {
    const action = await createPolicySkipAction({
      accountId,
      opportunity: evaluated,
      actionType: desiredActionType,
      reason: 'suppressed_by_missed_call_instant_response',
      trigger,
      policy: plan.policy,
      signalId: leadEvent?.id || null
    });
    actionResult = { ok: true, skipped: true, reason: 'suppressed_by_missed_call_instant_response', actionId: action.id };
  } else {
    const cap = capDecision(evaluated, plan.policy, desiredActionType);
    if (cap.blocked) {
    const action = await createPolicySkipAction({
      accountId,
      opportunity: evaluated,
      actionType: desiredActionType,
      reason: cap.reason,
      trigger,
      policy: plan.policy,
      signalId: leadEvent?.id || null
    });
    actionResult = { ok: true, skipped: true, reason: cap.reason, actionId: action.id };
    } else if (isWithinPolicyQuietHours(plan.policy, Date.now()) && evaluated.quietHoursBypass !== true) {
    const quietAction = await createPolicySkipAction({
      accountId,
      opportunity: evaluated,
      actionType: desiredActionType,
      reason: 'quiet_hours',
      trigger,
      policy: plan.policy,
      signalId: leadEvent?.id || null
    });
    actionResult = { ok: true, skipped: true, reason: 'quiet_hours', actionId: quietAction.id };
    } else if (desiredActionType === 'do_nothing') {
    const action = await createPolicySkipAction({
      accountId,
      opportunity: evaluated,
      actionType: 'do_nothing',
      reason: 'no_action_required',
      trigger,
      policy: plan.policy,
      signalId: leadEvent?.id || null
    });
    actionResult = { ok: true, skipped: true, reason: 'no_action_required', actionId: action.id };
    } else {
    actionResult = await executeRevenueAction(accountId, leadEvent, evaluated, plan, { trigger });
    }
  }

  try {
    const { startRun, handleSignalForRun } = require('./agentEngine');
    await handleSignalForRun(accountId, evaluated.id, leadEvent);
    const activeRunId = evaluated?.agentState?.activeRunId || null;
    if (!activeRunId && evaluated.stopAutomation !== true) {
      await startRun(accountId, evaluated.id, {
        trigger: trigger === 'prm_tick' ? 'prm_tick' : 'signal_ingest',
        mode: 'AUTO'
      });
    }
  } catch {}

  saveDataDebounced(loadData());
  return { ok: true, opportunity: evaluated, actionPlan: plan, actionResult };
}

async function runRecommendedAction(accountId, opportunityId) {
  const data = loadData();
  const opportunity = (data.revenueOpportunities || []).find((o) =>
    String(o?.id || '') === String(opportunityId) && String(o?.accountId || '') === String(accountId)
  );
  if (!opportunity) return { ok: false, reason: 'not_found' };

  const lastManual = Number(opportunity?.lastManualRunAt || 0);
  if (lastManual && (Date.now() - lastManual) < (10 * 60 * 1000)) {
    const accountRef = getAccountById(data, accountId);
    const policy = getPolicyConfig(accountRef);
    const action = await createPolicySkipAction({
      accountId,
      opportunity,
      actionType: 'manual_run_recommended',
      reason: 'manual_rate_limited_10m',
      trigger: 'manual_run_recommended',
      policy
    });
    return { ok: false, reason: 'manual_rate_limited_10m', actionId: action.id };
  }

  const latestConvo = getConversationById(accountId, String(opportunity?.convoKey || ''))?.conversation || null;
  const leadEvent = createLeadEvent(accountId, {
    contactId: opportunity?.contactId || null,
    convoKey: opportunity?.convoKey || null,
    channel: 'chat',
    type: 'lead_stalled',
    payload: {
      source: 'manual_run_recommended',
      hasConversation: Boolean(latestConvo)
    }
  });
  opportunity.lastManualRunAt = Date.now();
  await persistSnapshotOpportunity(accountId, opportunity, { operation: 'manual_run_touch' });
  saveDataDebounced(data);
  return handleSignal(accountId, leadEvent);
}

async function markActionOutcomeById(accountId, actionId, { status, error = null } = {}) {
  return logActionResultWrite(accountId, actionId, { status, error });
}

function getOpportunityForAccount(accountId, opportunityId) {
  const data = loadData();
  return (data.revenueOpportunities || []).find((o) =>
    String(o?.accountId || '') === String(accountId || '') && String(o?.id || '') === String(opportunityId || '')
  ) || null;
}

module.exports = {
  handleSignal,
  runRecommendedAction,
  markActionOutcomeById,
  getOpportunityForAccount
};
