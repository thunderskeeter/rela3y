const {
  loadData,
  saveDataDebounced,
  getAccountById
} = require('../store/dataStore');
const { generateId } = require('../utils/id');
const { appendOutboundMessage } = require('./messagesService');
const {
  ensureOpportunityDefaults,
  getPolicyConfig,
  getFeatureFlags,
  createLeadEvent,
  evaluateOpportunity
} = require('./revenueIntelligenceService');
const { transitionStage } = require('./opportunityLifecycle');
const {
  logActionStartWrite,
  logActionResultWrite,
  attachActionToOpportunityWrite,
  hasSuccessfulActionByIdempotencyWrite
} = require('./actionsService');
const {
  getPlaybookForAccount,
  goalForSignal,
  buildGoalSteps,
  getVariants
} = require('./industryPlaybooks');
const {
  createReviewItem,
  resolveReviewItem
} = require('./reviewQueueService');

const LOCK_MS = 2 * 60 * 1000;

function nowTs() {
  return Date.now();
}

function dayKey(ts = Date.now()) {
  return new Date(ts).toISOString().slice(0, 10);
}

function toParts(convoKey) {
  const [to, from] = String(convoKey || '').split('__');
  if (!to || !from) return null;
  return { to, from };
}

function safeObj(v) {
  return v && typeof v === 'object' && !Array.isArray(v) ? v : {};
}

function isQuietHours(policy, ts = Date.now()) {
  const q = safeObj(policy?.quietHours);
  const start = Number(q.startHour);
  const end = Number(q.endHour);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return false;
  const h = new Date(ts).getHours();
  if (start === end) return false;
  if (start < end) return h >= start && h < end;
  return h >= start || h < end;
}

function nextOpenTs(policy, ts = Date.now()) {
  if (!isQuietHours(policy, ts)) return ts;
  const endHour = Number(policy?.quietHours?.endHour);
  const d = new Date(ts);
  d.setHours(Number.isFinite(endHour) ? endHour : 8, 0, 0, 0);
  if (d.getTime() <= ts) d.setDate(d.getDate() + 1);
  return d.getTime();
}

function refreshCounters(opp, ts = Date.now()) {
  const dk = dayKey(ts);
  if (String(opp.followupsDayKey || '') !== dk) {
    opp.followupsDayKey = dk;
    opp.followupsSentToday = 0;
  }
  if (String(opp.automationsDayKey || '') !== dk) {
    opp.automationsDayKey = dk;
    opp.automationsSentToday = 0;
  }
}

function incrementCounters(opp, actionType) {
  opp.automationsSentToday = Number(opp.automationsSentToday || 0) + 1;
  if (['SEND_MESSAGE', 'ASK_QUALIFYING', 'OFFER_BOOKING', 'SCHEDULE_FOLLOWUP'].includes(String(actionType || ''))) {
    opp.followupsSentToday = Number(opp.followupsSentToday || 0) + 1;
    opp.followupsSentTotal = Number(opp.followupsSentTotal || 0) + 1;
  }
}

function ensureRunCollections(data) {
  data.agentRuns = Array.isArray(data.agentRuns) ? data.agentRuns : [];
  data.reviewQueue = Array.isArray(data.reviewQueue) ? data.reviewQueue : [];
  data.alerts = Array.isArray(data.alerts) ? data.alerts : [];
  return data;
}

function findOpportunity(data, accountId, opportunityId) {
  return (data.revenueOpportunities || []).find((o) =>
    String(o?.accountId || '') === String(accountId || '') && String(o?.id || '') === String(opportunityId || '')
  ) || null;
}

function findRun(data, accountId, runId) {
  return (data.agentRuns || []).find((r) =>
    String(r?.accountId || '') === String(accountId || '') && String(r?.id || '') === String(runId || '')
  ) || null;
}

function nextPendingStep(run) {
  const completed = new Set((run?.stepState?.completedSteps || []).map((x) => String(x?.stepId || '')));
  const steps = Array.isArray(run?.plan?.steps) ? run.plan.steps : [];
  return steps.find((s) => !completed.has(String(s?.stepId || ''))) || null;
}

function completeStep(run, stepId, outcome, notes = '') {
  run.stepState = safeObj(run.stepState);
  run.stepState.completedSteps = Array.isArray(run.stepState.completedSteps) ? run.stepState.completedSteps : [];
  run.stepState.completedSteps.push({ stepId: String(stepId), ts: Date.now(), outcome: String(outcome || 'success'), notes: String(notes || '') || undefined });
  run.stepState.completedSteps = run.stepState.completedSteps.slice(-200);
}

function acquireLock(opp, owner = 'agentEngine') {
  ensureOpportunityDefaults(opp);
  const now = Date.now();
  const lockUntil = Number(opp?.agentState?.lockedUntil || 0);
  if (lockUntil && lockUntil > now && String(opp?.agentState?.lockOwner || '') !== String(owner)) {
    return false;
  }
  opp.agentState.lockedUntil = now + LOCK_MS;
  opp.agentState.lockOwner = owner;
  return true;
}

function releaseLock(opp, owner = 'agentEngine') {
  ensureOpportunityDefaults(opp);
  if (String(opp?.agentState?.lockOwner || '') && String(opp.agentState.lockOwner) !== String(owner)) return;
  opp.agentState.lockedUntil = null;
  opp.agentState.lockOwner = null;
}

function createAgentAlert(data, accountId, message, payload = {}) {
  const alert = {
    id: generateId(),
    accountId: String(accountId || ''),
    type: 'agent',
    severity: 'warning',
    message: String(message || ''),
    data: payload,
    createdAt: Date.now(),
    acknowledgedAt: null
  };
  data.alerts.push(alert);
  return alert;
}

function renderTemplate(text, vars = {}, allowed = []) {
  const src = String(text || '');
  return src.replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_m, key) => {
    const k = String(key || '');
    if (Array.isArray(allowed) && allowed.length && !allowed.includes(k)) return '';
    return vars[k] == null ? '' : String(vars[k]);
  }).replace(/\s{2,}/g, ' ').trim();
}

function pickVariant(playbook, stepType, flags, context) {
  const variants = getVariants(playbook, stepType);
  if (!variants.length) return { id: '', text: '', style: 'direct' };
  let variant = variants[0];
  if (flags.enableAIMessageVariants === true) {
    const ranked = ['friendly', 'direct', 'short'];
    const prefer = String(context?.tonePreference || 'friendly');
    const idx = ranked.indexOf(prefer);
    if (idx >= 0) {
      const found = variants.find((v) => String(v?.style || '') === prefer);
      if (found) variant = found;
    }
  }
  const vars = {
    businessName: String(context?.businessName || 'Our team'),
    bookingLink: String(context?.bookingLink || ''),
    firstName: String(context?.firstName || 'there')
  };
  return {
    id: String(variant.id || ''),
    style: String(variant.style || 'direct'),
    text: renderTemplate(variant.textTemplate, vars, variant.allowedPlaceholders)
  };
}

function buildPlan(accountId, opportunity, context = {}) {
  const data = ensureRunCollections(loadData());
  const accountRef = getAccountById(data, accountId);
  const account = accountRef?.account || {};
  const playbook = getPlaybookForAccount(accountId);
  const policy = getPolicyConfig(accountRef);
  const flags = getFeatureFlags(accountRef);
  const signalType = String(context?.signalType || opportunity?.metadata?.lastSignalType || 'inbound_message');
  const goal = goalForSignal(playbook, signalType);
  const requiresReview = Number(opportunity?.riskScore || 0) >= 85;
  const steps = buildGoalSteps(playbook, goal, { requiresReview }).map((step) => {
    const variant = pickVariant(playbook, step.type, flags, {
      businessName: account?.workspace?.identity?.businessName || account?.businessName,
      bookingLink: account?.workspace?.scheduling?.bookingUrl || account?.scheduling?.url || account?.bookingUrl || '',
      firstName: context?.firstName || 'there',
      tonePreference: account?.workspace?.settings?.tonePreference || 'friendly'
    });
    return {
      ...step,
      payload: {
        ...safeObj(step.payload),
        messageVariantId: variant.id || null,
        messageText: safeObj(step.payload).messageText || variant.text || '',
        bookingUrl: account?.workspace?.scheduling?.bookingUrl || account?.scheduling?.url || account?.bookingUrl || null
      }
    };
  });

  return {
    goal,
    playbookId: String(playbook.id || 'home_services'),
    steps,
    context: {
      signalType,
      stage: String(opportunity?.stage || 'NEW'),
      riskScore: Number(opportunity?.riskScore || 0),
      quietHours: isQuietHours(policy),
      policySnapshot: policy
    }
  };
}

function createRunRecord(accountId, opportunityId, plan, { trigger = 'signal_ingest', mode = 'AUTO' } = {}) {
  const now = Date.now();
  return {
    id: generateId(),
    accountId: String(accountId),
    opportunityId: String(opportunityId),
    createdAt: now,
    updatedAt: now,
    status: 'PLANNED',
    mode: ['AUTO', 'REVIEW_REQUIRED', 'MANUAL'].includes(String(mode || '').toUpperCase()) ? String(mode).toUpperCase() : 'AUTO',
    trigger: String(trigger || 'signal_ingest'),
    correlationId: generateId(),
    plan,
    stepState: {
      currentStepId: null,
      completedSteps: [],
      scheduledStepJobs: []
    },
    lastError: null
  };
}

function scheduleStepJob(accountId, run, step, opp, scheduledFor, actionId = null) {
  const { scheduleJob } = require('./scheduler');
  const parts = toParts(opp?.convoKey);
  if (!parts) return null;
  const rule = {
    id: `agent_step_${run.id}_${step.stepId}`,
    templateId: `agent-step-${run.id}-${step.stepId}`,
    name: `Agent Step ${step.stepId}`,
    template: String(step?.payload?.messageText || 'Agent followup'),
    trigger: 'no_response',
    category: 'revenue_recovery',
    delayMinutes: 0
  };
  const job = scheduleJob(rule, {
    tenant: { accountId: String(accountId), to: String(parts.to) },
    to: String(parts.to),
    from: String(parts.from),
    overrideScheduledFor: Number(scheduledFor || Date.now()),
    source: 'agent_engine',
    conversationId: opp.convoKey,
    eventData: {
      runId: run.id,
      stepId: step.stepId,
      opportunityId: opp.id,
      correlationId: run.correlationId,
      actionId: actionId || null
    }
  });
  if (!job) return null;
  run.stepState.scheduledStepJobs = Array.isArray(run.stepState.scheduledStepJobs) ? run.stepState.scheduledStepJobs : [];
  run.stepState.scheduledStepJobs.push({ stepId: step.stepId, jobId: job.id, runAtTs: Number(job.scheduledFor || scheduledFor) });
  run.stepState.scheduledStepJobs = run.stepState.scheduledStepJobs.slice(-200);
  return job;
}

function policyBlockReason(opp, policy, stepType) {
  refreshCounters(opp);
  if (opp.stopAutomation === true) return 'stop_automation';
  if ((Number(opp.cooldownUntil || 0) > Date.now()) && ['SEND_MESSAGE', 'ASK_QUALIFYING', 'OFFER_BOOKING'].includes(stepType)) {
    return 'cooldown_active';
  }
  if (Number(opp.automationsSentToday || 0) >= Number(policy.maxAutomationsPerOpportunityPerDay || 4)) {
    return 'daily_automation_cap';
  }
  if (['SEND_MESSAGE', 'ASK_QUALIFYING', 'OFFER_BOOKING', 'SCHEDULE_FOLLOWUP'].includes(stepType)
      && Number(opp.followupsSentToday || 0) >= Number(policy.dailyFollowupCapPerLead || 2)) {
    return 'daily_followup_cap';
  }
  return '';
}

function markRunTerminal(run, opp, status) {
  run.status = status;
  run.updatedAt = Date.now();
  run.stepState.currentStepId = null;
  if (opp?.agentState) {
    opp.agentState.activeRunId = null;
  }
  releaseLock(opp, 'agentEngine');
}

async function executeStep(accountId, runId, stepId, options = {}) {
  const data = ensureRunCollections(loadData());
  const run = findRun(data, accountId, runId);
  if (!run) return { ok: false, reason: 'run_not_found' };
  const opp = findOpportunity(data, accountId, run.opportunityId);
  if (!opp) return { ok: false, reason: 'opportunity_not_found' };
  ensureOpportunityDefaults(opp);
  const accountRef = getAccountById(data, accountId);
  const policy = getPolicyConfig(accountRef);

  if (!acquireLock(opp, 'agentEngine')) {
    const action = await logActionStartWrite({
      accountId,
      opportunityId: opp.id,
      convoKey: opp.convoKey,
      runId: run.id,
      stepId,
      correlationId: run.correlationId,
      actionType: 'lock_contention',
      channel: 'sms',
      payload: { reason: 'lock_contention' },
      justification: {
        trigger: String(options.trigger || run.trigger || 'prm_tick'),
        riskScore: Number(opp.riskScore || 0),
        reasons: ['lock_contention'],
        stageBefore: String(opp.stage || 'NEW'),
        stageAfter: String(opp.stage || 'NEW'),
        decisionVersion: 'deterministic_agent_v1',
        policy: {
          dailyCap: Number(policy.dailyFollowupCapPerLead || 2),
          cooldownMinutes: Number(policy.minCooldownMinutes || 30),
          quietHours: isQuietHours(policy),
          complianceChecked: true
        }
      }
    });
    await logActionResultWrite(accountId, action.id, { status: 'skipped', error: 'lock_contention' });
    await attachActionToOpportunityWrite(accountId, opp.id, action.id);
    return { ok: false, reason: 'lock_contention', actionId: action.id };
  }

  if (['COMPLETED', 'FAILED', 'CANCELLED'].includes(String(run.status || ''))) {
    releaseLock(opp, 'agentEngine');
    saveDataDebounced(data);
    return { ok: true, reason: 'run_terminal' };
  }

  const step = (run.plan?.steps || []).find((s) => String(s?.stepId || '') === String(stepId || '')) || nextPendingStep(run);
  if (!step) {
    markRunTerminal(run, opp, 'COMPLETED');
    saveDataDebounced(data);
    return { ok: true, reason: 'no_pending_step' };
  }

  run.status = 'RUNNING';
  run.updatedAt = Date.now();
  run.stepState.currentStepId = step.stepId;

  const forceReview = String(step.type) === 'ESCALATE'
    || (isQuietHours(policy) && opp.quietHoursBypass === true && ['SEND_MESSAGE', 'ASK_QUALIFYING', 'OFFER_BOOKING'].includes(String(step.type)));
  const needsReview = run.mode === 'REVIEW_REQUIRED' || step?.guardrails?.requiresReview === true || forceReview;
  if (needsReview && options.reviewApproved !== true) {
    const item = createReviewItem(accountId, {
      runId: run.id,
      opportunityId: opp.id,
      stepId: step.stepId,
      proposedActionPayload: {
        type: step.type,
        payload: safeObj(step.payload),
        correlationId: run.correlationId
      },
      requiredByTs: Date.now() + (60 * 60 * 1000)
    });
    createAgentAlert(data, accountId, 'Agent step pending review approval.', {
      runId: run.id,
      stepId: step.stepId,
      reviewItemId: item.id
    });
    const reviewAction = await logActionStartWrite({
      accountId,
      opportunityId: opp.id,
      contactId: opp.contactId,
      convoKey: opp.convoKey,
      runId: run.id,
      stepId: step.stepId,
      correlationId: run.correlationId,
      actionType: 'create_alert',
      channel: 'web',
      payload: { reason: 'review_required', reviewItemId: item.id },
      justification: {
        trigger: String(options.trigger || run.trigger || 'signal_ingest'),
        riskScore: Number(opp.riskScore || 0),
        reasons: ['review_required'],
        stageBefore: String(opp.stage || 'NEW'),
        stageAfter: String(opp.stage || 'NEW'),
        decisionVersion: 'deterministic_agent_v1',
        policy: {
          dailyCap: Number(policy.dailyFollowupCapPerLead || 2),
          cooldownMinutes: Number(policy.minCooldownMinutes || 30),
          quietHours: isQuietHours(policy),
          complianceChecked: true
        }
      }
    });
    await logActionResultWrite(accountId, reviewAction.id, { status: 'sent' });
    await attachActionToOpportunityWrite(accountId, opp.id, reviewAction.id);
    run.status = 'WAITING';
    run.updatedAt = Date.now();
    saveDataDebounced(data);
    releaseLock(opp, 'agentEngine');
    return { ok: true, waitingReview: true, reviewItemId: item.id };
  }

  const actionType = String(step.type || 'SEND_MESSAGE');
  const idempotencyKey = `${accountId}:${opp.id}:${run.id}:${step.stepId}:${actionType}`;
  if (await hasSuccessfulActionByIdempotencyWrite(accountId, idempotencyKey)) {
    const idemAction = await logActionStartWrite({
      accountId,
      opportunityId: opp.id,
      contactId: opp.contactId,
      convoKey: opp.convoKey,
      runId: run.id,
      stepId: step.stepId,
      correlationId: run.correlationId,
      idempotencyKey,
      actionType: String(step?.type || 'unknown').toLowerCase(),
      channel: 'sms',
      payload: { skippedReason: 'idempotent_skip' },
      justification: {
        trigger: String(options.trigger || run.trigger || 'signal_ingest'),
        riskScore: Number(opp.riskScore || 0),
        reasons: ['idempotent_skip'],
        stageBefore: String(opp.stage || 'NEW'),
        stageAfter: String(opp.stage || 'NEW'),
        decisionVersion: 'deterministic_agent_v1',
        policy: {
          dailyCap: Number(policy.dailyFollowupCapPerLead || 2),
          cooldownMinutes: Number(policy.minCooldownMinutes || 30),
          quietHours: isQuietHours(policy),
          complianceChecked: true
        }
      }
    });
    await logActionResultWrite(accountId, idemAction.id, { status: 'skipped', error: 'idempotent_skip' });
    await attachActionToOpportunityWrite(accountId, opp.id, idemAction.id);
    completeStep(run, step.stepId, 'skipped', 'idempotent_skip');
    run.updatedAt = Date.now();
    const n = nextPendingStep(run);
    if (!n) markRunTerminal(run, opp, 'COMPLETED');
    saveDataDebounced(data);
    releaseLock(opp, 'agentEngine');
    return { ok: true, skipped: true, reason: 'idempotent_skip' };
  }

  const blocked = policyBlockReason(opp, policy, actionType);
  const stageBefore = String(opp.stage || 'NEW');
  const action = await logActionStartWrite({
    accountId,
    opportunityId: opp.id,
    contactId: opp.contactId,
    convoKey: opp.convoKey,
    runId: run.id,
    stepId: step.stepId,
    correlationId: run.correlationId,
    idempotencyKey,
    dryRun: options.dryRun === true,
    actionType: actionType.toLowerCase(),
    channel: 'sms',
    payload: {
      ...safeObj(step.payload),
      playbookId: run?.plan?.playbookId || null
    },
    justification: {
      trigger: String(options.trigger || run.trigger || 'signal_ingest'),
      riskScore: Number(opp.riskScore || 0),
      reasons: Array.isArray(opp.riskReasons) ? opp.riskReasons : [],
      stageBefore,
      stageAfter: stageBefore,
      decisionVersion: 'deterministic_agent_v1',
      policy: {
        dailyCap: Number(policy.dailyFollowupCapPerLead || 2),
        cooldownMinutes: Number(policy.minCooldownMinutes || 30),
        quietHours: isQuietHours(policy),
        complianceChecked: true
      }
    }
  });

  if (blocked) {
    await logActionResultWrite(accountId, action.id, { status: 'skipped', error: blocked });
    await attachActionToOpportunityWrite(accountId, opp.id, action.id);
    completeStep(run, step.stepId, 'skipped', blocked);
    if (blocked === 'daily_followup_cap' || blocked === 'daily_automation_cap') {
      createAgentAlert(data, accountId, 'Automation paused: repeated no-response; check lead manually.', {
        runId: run.id,
        opportunityId: opp.id,
        reason: blocked
      });
    }
    run.status = 'WAITING';
    run.updatedAt = Date.now();
    saveDataDebounced(data);
    releaseLock(opp, 'agentEngine');
    return { ok: true, skipped: true, reason: blocked, actionId: action.id };
  }

  const messageTypes = new Set(['SEND_MESSAGE', 'ASK_QUALIFYING', 'OFFER_BOOKING']);
  if (messageTypes.has(actionType) && isQuietHours(policy) && opp.quietHoursBypass !== true && options.forceNow !== true) {
    const runAt = nextOpenTs(policy, Date.now());
    const job = scheduleStepJob(accountId, run, step, opp, runAt, action.id);
    if (job) {
      await logActionResultWrite(accountId, action.id, { status: 'skipped', error: 'quiet_hours_scheduled' });
      await attachActionToOpportunityWrite(accountId, opp.id, action.id);
      run.status = 'WAITING';
      run.updatedAt = Date.now();
      saveDataDebounced(data);
      releaseLock(opp, 'agentEngine');
      return { ok: true, waiting: true, reason: 'quiet_hours_scheduled', jobId: job.id };
    }
  }

  let sentOk = true;
  let sendErr = null;

  if (options.dryRun === true) {
    sentOk = false;
    sendErr = 'dry_run_would_send';
  } else if (messageTypes.has(actionType)) {
    const parts = toParts(opp.convoKey);
    if (!parts) {
      sentOk = false;
      sendErr = 'missing_conversation';
    } else {
      const text = String(step?.payload?.messageText || '').trim();
      if (!text) {
        sentOk = false;
        sendErr = 'missing_message_text';
      } else {
        const { sendResult } = await appendOutboundMessage({
          tenant: { accountId: String(accountId), to: String(parts.to) },
          to: parts.to,
          from: parts.from,
          text,
          source: 'agent_engine',
          transactional: false,
          requireExisting: true,
          meta: {
            auto: true,
            status: 'sent',
            runId: run.id,
            stepId: step.stepId,
            correlationId: run.correlationId,
            actionId: action.id
          }
        });
        sentOk = sendResult.ok === true;
        sendErr = sentOk ? null : (sendResult?.error?.code || sendResult?.error?.message || 'send_failed');
        if (sentOk) {
          createLeadEvent(accountId, {
            convoKey: opp.convoKey,
            contactId: opp.contactId,
            channel: 'sms',
            type: 'outbound_message',
            payload: {
              source: 'agent_engine',
              runId: run.id,
              stepId: step.stepId,
              correlationId: run.correlationId,
              text
            }
          });
          transitionStage(opp, 'CONTACTED', 'agent_outbound_message');
        }
      }
    }
  } else if (actionType === 'SCHEDULE_FOLLOWUP') {
    const mins = Math.max(1, Number(step?.when?.minutes || step?.failureCriteria?.minutes || 30));
    const runAt = Date.now() + (mins * 60 * 1000);
    const nextStep = nextPendingStep(run) || step;
    scheduleStepJob(accountId, run, nextStep, opp, runAt, action.id);
  } else if (actionType === 'CREATE_ALERT' || actionType === 'ESCALATE') {
    createAgentAlert(data, accountId, String(step?.payload?.alertMessage || 'Agent requested manual follow-up.'), {
      runId: run.id,
      stepId: step.stepId,
      opportunityId: opp.id,
      escalate: actionType === 'ESCALATE'
    });
  }

  if (!sentOk && messageTypes.has(actionType) && options.dryRun !== true) {
    await logActionResultWrite(accountId, action.id, { status: 'failed', error: sendErr || 'send_failed' });
    await attachActionToOpportunityWrite(accountId, opp.id, action.id);
    run.status = 'FAILED';
    run.lastError = { message: String(sendErr || 'send_failed'), ts: Date.now() };
    run.updatedAt = Date.now();
    markRunTerminal(run, opp, 'FAILED');
    saveDataDebounced(data);
    releaseLock(opp, 'agentEngine');
    return { ok: false, reason: sendErr || 'send_failed', actionId: action.id };
  }

  if (options.dryRun === true) {
    await logActionResultWrite(accountId, action.id, { status: 'skipped', error: 'dry_run_would_send' });
  } else {
    await logActionResultWrite(accountId, action.id, { status: 'sent' });
  }
  await attachActionToOpportunityWrite(accountId, opp.id, action.id);

  incrementCounters(opp, actionType);
  opp.lastRecommendedActionType = String(actionType || '').toLowerCase();
  opp.lastRecommendedActionAt = Date.now();
  opp.cooldownUntil = Date.now() + (Number(policy.minCooldownMinutes || 30) * 60 * 1000);

  completeStep(run, step.stepId, options.dryRun === true ? 'skipped' : 'success', options.dryRun === true ? 'dry_run_would_send' : 'sent');
  run.updatedAt = Date.now();

  const next = nextPendingStep(run);
  if (!next) {
    markRunTerminal(run, opp, 'COMPLETED');
    evaluateOpportunity(accountId, opp.id);
    saveDataDebounced(data);
    releaseLock(opp, 'agentEngine');
    return { ok: true, completed: true, actionId: action.id };
  }

  run.stepState.currentStepId = next.stepId;
  if (options.skipSchedule === true) {
    run.status = 'RUNNING';
  } else if (String(next?.when?.kind || 'NOW') === 'NOW') {
    run.status = 'RUNNING';
    saveDataDebounced(data);
    releaseLock(opp, 'agentEngine');
    return executeStep(accountId, run.id, next.stepId, options);
  } else {
    let scheduledFor = Date.now() + (Math.max(1, Number(next?.when?.minutes || 15)) * 60 * 1000);
    if (String(next?.when?.kind || '') === 'AT_NEXT_OPEN') {
      scheduledFor = nextOpenTs(policy, Date.now());
    }
    scheduleStepJob(accountId, run, next, opp, scheduledFor);
    run.status = 'WAITING';
  }

  saveDataDebounced(data);
  releaseLock(opp, 'agentEngine');
  return { ok: true, runId: run.id, nextStepId: next.stepId };
}

async function startRun(accountId, opportunityId, { trigger = 'manual_user_start', mode = 'AUTO' } = {}) {
  const data = ensureRunCollections(loadData());
  const opp = findOpportunity(data, accountId, opportunityId);
  if (!opp) return { ok: false, reason: 'opportunity_not_found' };
  ensureOpportunityDefaults(opp);

  if (!acquireLock(opp, 'agentEngine')) {
    return { ok: false, reason: 'lock_contention' };
  }

  if (opp.stopAutomation === true) {
    releaseLock(opp, 'agentEngine');
    saveDataDebounced(data);
    return { ok: false, reason: 'stop_automation' };
  }

  const currentRun = opp?.agentState?.activeRunId
    ? findRun(data, accountId, opp.agentState.activeRunId)
    : null;
  if (currentRun && !['COMPLETED', 'FAILED', 'CANCELLED'].includes(String(currentRun?.status || ''))) {
    releaseLock(opp, 'agentEngine');
    saveDataDebounced(data);
    return { ok: true, run: currentRun, reused: true };
  }

  const plan = buildPlan(accountId, opp, { signalType: String(opp?.metadata?.lastSignalType || 'inbound_message') });
  const run = createRunRecord(accountId, opportunityId, plan, { trigger, mode });
  data.agentRuns.push(run);
  opp.agentState.activeRunId = run.id;
  opp.agentState.lastRunId = run.id;
  saveDataDebounced(data);
  releaseLock(opp, 'agentEngine');

  await resumeRun(accountId, run.id);
  return { ok: true, run };
}

async function resumeRun(accountId, runId) {
  const data = ensureRunCollections(loadData());
  const run = findRun(data, accountId, runId);
  if (!run) return { ok: false, reason: 'run_not_found' };
  if (['COMPLETED', 'FAILED', 'CANCELLED'].includes(String(run.status || ''))) return { ok: true, run };
  const step = nextPendingStep(run);
  if (!step) {
    run.status = 'COMPLETED';
    run.updatedAt = Date.now();
    saveDataDebounced(data);
    return { ok: true, run };
  }
  return executeStep(accountId, runId, step.stepId, { trigger: 'prm_tick' });
}

function signalMatches(successCriteria, leadEvent) {
  const kind = String(successCriteria?.kind || '');
  const type = String(leadEvent?.type || '');
  if (!kind) return false;
  if (kind === 'INBOUND_REPLY') return type === 'inbound_message';
  if (kind === 'BOOKING_CREATED') return type === 'booking_created';
  if (kind === 'TAG_APPLIED') return type === 'tag_applied';
  if (kind === 'HUMAN_CONFIRMED') return type === 'human_confirmed';
  return false;
}

async function handleSignalForRun(accountId, opportunityId, leadEvent) {
  const data = ensureRunCollections(loadData());
  const opp = findOpportunity(data, accountId, opportunityId);
  if (!opp) return { ok: false, reason: 'opportunity_not_found' };
  ensureOpportunityDefaults(opp);
  const run = opp?.agentState?.activeRunId ? findRun(data, accountId, opp.agentState.activeRunId) : null;
  if (!run || ['COMPLETED', 'FAILED', 'CANCELLED'].includes(String(run.status || ''))) {
    return { ok: true, reason: 'no_active_run' };
  }

  const evtType = String(leadEvent?.type || '');
  if (['opt_out', 'unsubscribe'].includes(evtType)) {
    run.status = 'CANCELLED';
    run.updatedAt = Date.now();
    run.lastError = { message: 'opt_out_received', ts: Date.now() };
    markRunTerminal(run, opp, 'CANCELLED');
    saveDataDebounced(data);
    return { ok: true, cancelled: true };
  }

  const currentStepId = String(run?.stepState?.currentStepId || '');
  const step = (run.plan?.steps || []).find((s) => String(s?.stepId || '') === currentStepId) || nextPendingStep(run);
  if (!step) {
    run.status = 'COMPLETED';
    run.updatedAt = Date.now();
    markRunTerminal(run, opp, 'COMPLETED');
    saveDataDebounced(data);
    return { ok: true, completed: true };
  }

  if (signalMatches(step.successCriteria, leadEvent)) {
    completeStep(run, step.stepId, 'success', `signal:${evtType}`);
    run.updatedAt = Date.now();
    if (evtType === 'inbound_message') transitionStage(opp, 'ENGAGED', 'agent_success_inbound_reply');
    if (evtType === 'booking_created') transitionStage(opp, 'BOOKED', 'agent_success_booking_created');
    const next = nextPendingStep(run);
    saveDataDebounced(data);
    if (!next) {
      markRunTerminal(run, opp, 'COMPLETED');
      saveDataDebounced(data);
      return { ok: true, completed: true };
    }
    return resumeRun(accountId, run.id);
  }

  return { ok: true, reason: 'signal_no_change' };
}

async function cancelRun(accountId, runId, reason = 'cancelled_by_user') {
  const data = ensureRunCollections(loadData());
  const run = findRun(data, accountId, runId);
  if (!run) return { ok: false, reason: 'run_not_found' };
  const opp = findOpportunity(data, accountId, run.opportunityId);
  run.status = 'CANCELLED';
  run.updatedAt = Date.now();
  run.lastError = { message: String(reason || 'cancelled'), ts: Date.now() };
  if (opp) {
    ensureOpportunityDefaults(opp);
    markRunTerminal(run, opp, 'CANCELLED');
  }
  saveDataDebounced(data);
  return { ok: true, run };
}

async function replayRun(accountId, runId, { dryRun = true } = {}) {
  const data = ensureRunCollections(loadData());
  const existing = findRun(data, accountId, runId);
  if (!existing) return { ok: false, reason: 'run_not_found' };
  const replay = createRunRecord(accountId, existing.opportunityId, existing.plan, {
    trigger: 'replay',
    mode: 'MANUAL'
  });
  replay.plan = JSON.parse(JSON.stringify(existing.plan || {}));
  replay.plan.replayOfRunId = existing.id;
  data.agentRuns.push(replay);
  const opp = findOpportunity(data, accountId, existing.opportunityId);
  if (opp) {
    ensureOpportunityDefaults(opp);
    opp.agentState.lastRunId = replay.id;
  }
  saveDataDebounced(data);

  for (const step of (replay.plan?.steps || [])) {
    await executeStep(accountId, replay.id, step.stepId, {
      dryRun: dryRun === true,
      trigger: 'replay',
      forceNow: true,
      skipSchedule: true,
      reviewApproved: true
    });
  }
  return { ok: true, run: replay };
}

async function approveReviewItem(accountId, reviewId, userId, notes = '') {
  const data = ensureRunCollections(loadData());
  const item = resolveReviewItem(accountId, reviewId, 'APPROVED', { userId, notes });
  if (!item) return { ok: false, reason: 'review_item_not_found' };
  if (!item.runId || !item.stepId) return { ok: false, reason: 'invalid_review_item' };
  return executeStep(accountId, item.runId, item.stepId, {
    reviewApproved: true,
    trigger: 'review_approved'
  });
}

function rejectReviewItem(accountId, reviewId, userId, notes = '') {
  const data = ensureRunCollections(loadData());
  const item = resolveReviewItem(accountId, reviewId, 'REJECTED', { userId, notes });
  if (!item) return { ok: false, reason: 'review_item_not_found' };
  if (!item.runId) return { ok: true, item };
  return cancelRun(accountId, item.runId, 'review_rejected');
}

function getOpportunityRun(accountId, opportunityId) {
  const data = ensureRunCollections(loadData());
  const opp = findOpportunity(data, accountId, opportunityId);
  if (!opp) return null;
  ensureOpportunityDefaults(opp);
  const runId = opp?.agentState?.activeRunId || opp?.agentState?.lastRunId;
  if (!runId) return null;
  return findRun(data, accountId, runId) || null;
}

module.exports = {
  startRun,
  buildPlan,
  executeStep,
  handleSignalForRun,
  resumeRun,
  cancelRun,
  replayRun,
  approveReviewItem,
  rejectReviewItem,
  getOpportunityRun
};
