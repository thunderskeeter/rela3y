const { scheduleJob } = require('./scheduler');
const {
  createLeadEvent,
  upsertRevenueOpportunityFromEvent,
  evaluateOpportunity,
  createAlert
} = require('./revenueIntelligenceService');
const { persistSnapshotOpportunity } = require('./opportunitiesService');
const { logActionStartWrite, logActionResultWrite, attachActionToOpportunityWrite } = require('./actionsService');
const { transitionStage } = require('./opportunityLifecycle');
const { appendOutboundMessage } = require('./messagesService');

const CHANNEL = 'sms';
const outboundActionTypes = new Set(['send_message', 'ask_qualifying', 'offer_booking', 'offer_times']);

function toConversationParts(convoKey) {
  const [to, from] = String(convoKey || '').split('__');
  if (!to || !from) return null;
  return { to, from };
}

function buildJustification({ opportunity, plan, trigger, signalType }) {
  return {
    trigger: String(trigger || 'decision_engine'),
    riskScore: Number(opportunity?.riskScore || 0),
    reasons: Array.isArray(opportunity?.riskReasons) ? opportunity.riskReasons : [],
    stageBefore: String(opportunity?.stage || 'NEW'),
    stageAfter: String(opportunity?.stage || 'NEW'),
    decisionVersion: 'signal_decision_v1',
    policy: {
      dailyCap: Number(plan?.policy?.dailyFollowupCapPerLead || 2),
      cooldownMinutes: Number(plan?.policy?.minCooldownMinutes || 30),
      quietHours: Boolean(plan?.policy?.quietHours),
      complianceChecked: true
    },
    signalType: String(signalType || 'lead_stalled')
  };
}

function logOutcome(actionId, status, error = null, metadata = {}) {
  return logActionResultWrite(metadata?.accountId || '', actionId, { status, error, justification: metadata });
}

async function executeRevenueAction(accountId, leadEvent, opportunity, plan, { trigger = 'signal_ingest' } = {}) {
  const convoKey = String(opportunity?.convoKey || leadEvent?.convoKey || '');
  const parts = toConversationParts(convoKey);
  const wantsOutbound = plan?.nextAction && outboundActionTypes.has(plan.nextAction);
  const action = await logActionStartWrite({
      accountId,
      opportunityId: opportunity?.id,
      contactId: opportunity?.contactId,
      convoKey,
      actionType: String(plan?.nextAction || 'do_nothing'),
      channel: CHANNEL,
      payload: { messageText: plan?.messageText || null, signalId: leadEvent?.id || null },
      justification: buildJustification({ opportunity, plan, trigger, signalType: plan?.signalType })
    });

  if (String(plan?.nextAction || '') === 'escalate') {
    const alert = createAlert(accountId, {
      type: 'escalation',
      severity: 'critical',
      message: plan.messageText || 'Escalation requested for revenue opportunity',
      data: {
        opportunityId: opportunity?.id || null,
        contactId: opportunity?.contactId || null,
        signalId: leadEvent?.id || plan?.signalId || null,
        reason: plan?.escalation?.reason || null
      }
    });
    await logOutcome(action.id, 'sent', null, { accountId });
    await attachActionToOpportunityWrite(accountId, opportunity?.id, action.id);
    return { ok: true, actionId: action.id, alertId: alert?.id || null };
  }

  if (!parts && wantsOutbound) {
    await logOutcome(action.id, 'failed', 'missing_conversation', { accountId });
    await attachActionToOpportunityWrite(accountId, opportunity?.id, action.id);
    return { ok: false, actionId: action.id, error: 'missing_conversation' };
  }

  if (wantsOutbound && plan.messageText) {
    const { sendResult } = await appendOutboundMessage({
      tenant: { accountId, to: parts.to },
      to: parts.to,
      from: parts.from,
      text: plan.messageText,
      source: 'signal_action',
      transactional: false,
      requireExisting: true,
      meta: {
        signalId: leadEvent?.id,
        recommendedPack: plan.recommendedPack || null,
        actionId: action.id
      },
      afterSuccess(conversation) {
        conversation.lastActivityAt = Date.now();
      }
    });
    if (sendResult.ok) {
      await logOutcome(action.id, 'sent', null, { accountId });
      await attachActionToOpportunityWrite(accountId, opportunity?.id, action.id);
      transitionStage(opportunity, 'CONTACTED', 'action_send_message');
      await persistSnapshotOpportunity(accountId, opportunity, { operation: 'action_executor_stage_transition' });
      const outboundEvent = createLeadEvent(accountId, {
        convoKey,
        channel: CHANNEL,
        type: 'outbound_message',
        payload: {
          source: 'signal_action',
          text: plan.messageText
        }
      });
      upsertRevenueOpportunityFromEvent(accountId, outboundEvent);
      evaluateOpportunity(accountId, opportunity?.id);
      await persistSnapshotOpportunity(accountId, opportunity, { operation: 'action_executor_post_send' });
      if (Array.isArray(plan.followups)) {
        plan.followups.forEach((followup) => {
          const jobPayload = {
            runId: null,
            stepId: followup.stepId || followup.name || 'followup',
            signalId: leadEvent?.id,
            opportunityId: opportunity?.id
          };
          const delayMs = Math.max(1, Number(followup.delayMinutes || 30)) * 60 * 1000;
          scheduleJob({
            id: `signal_followup_${action.id}_${followup.delayMinutes}`,
            templateId: `signal-followup-${action.id}`,
            name: 'Signal followup',
            template: followup.messageText || plan.messageText,
            trigger: 'no_response',
            category: 'revenue_action',
            delayMinutes: followup.delayMinutes || 30
          }, {
            tenant: { accountId, to: parts.to },
            to: parts.to,
            from: parts.from,
            overrideScheduledFor: Date.now() + delayMs,
            eventData: jobPayload,
            source: 'signal_action'
          });
        });
      }
      return { ok: true, actionId: action.id };
    }
    await logOutcome(action.id, 'failed', sendResult.error || 'send_failed', { accountId });
    await attachActionToOpportunityWrite(accountId, opportunity?.id, action.id);
    return { ok: false, actionId: action.id, error: sendResult.error || 'send_failed' };
  }

  await logOutcome(action.id, 'sent', null, { accountId });
  await attachActionToOpportunityWrite(accountId, opportunity?.id, action.id);
  return { ok: true, actionId: action.id };
}

module.exports = {
  executeRevenueAction
};
