const { loadData, saveDataDebounced, getAccountById } = require('../store/dataStore');
const { generateId } = require('../utils/id');
const { emitAccountEvent } = require('./notificationService');
const {
  deriveStage,
  transitionStage,
  updateActivityTimestamps,
  normalizeStage
} = require('./opportunityLifecycle');
const { logRevenueEvent } = require('./revenueEventService');
const { normalizeSignalType } = require('./signalService');

const TERMINAL_STATUSES = new Set(['won', 'lost', 'closed']);
const OPEN_STATUSES = new Set(['open', 'at_risk', 'recovered']);
const INTEL_KEY_SEPARATOR = '::';

function nowMs() {
  return Date.now();
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function getHighValueThresholdCents(accountRef) {
  const raw = Number(accountRef?.account?.settings?.notifications?.highValueLeadMinCents);
  if (Number.isFinite(raw) && raw >= 0) return Math.round(raw);
  return 100000; // $1,000 default threshold
}

function normalizeChannel(value) {
  const v = String(value || '').trim().toLowerCase();
  return v || 'chat';
}

function normalizeEventType(value) {
  return String(value || '').trim().toLowerCase() || 'unknown';
}

function scopedIntelligenceKey(accountId, rawKey) {
  const key = String(rawKey || '').trim();
  if (!key) return '';
  return `${String(accountId)}${INTEL_KEY_SEPARATOR}${key}`;
}

function getFeatureFlags(accountRef) {
  const account = accountRef?.account || {};
  const merged = {
    enableOptimization: false,
    enableAIMessageVariants: false,
    enableMoneyProjections: false,
    ...(account?.workspace?.settings?.featureFlags || {}),
    ...(account?.settings?.featureFlags || {})
  };
  return merged;
}

function getPolicyConfig(accountRef) {
  const account = accountRef?.account || {};
  const merged = {
    dailyFollowupCapPerLead: 2,
    minCooldownMinutes: 30,
    quietHours: { startHour: 20, endHour: 8, timezone: 'America/New_York' },
    maxAutomationsPerOpportunityPerDay: 4,
    ...(account?.workspace?.settings?.policies || {}),
    ...(account?.settings?.policies || {})
  };
  merged.dailyFollowupCapPerLead = Math.max(1, Number(merged.dailyFollowupCapPerLead || 2));
  merged.minCooldownMinutes = Math.max(5, Number(merged.minCooldownMinutes || 30));
  merged.maxAutomationsPerOpportunityPerDay = Math.max(1, Number(merged.maxAutomationsPerOpportunityPerDay || 4));
  merged.quietHours = {
    startHour: Number(merged?.quietHours?.startHour ?? 20),
    endHour: Number(merged?.quietHours?.endHour ?? 8),
    timezone: String(merged?.quietHours?.timezone || 'America/New_York')
  };
  return merged;
}

function getAccountAvgTicketValueCents(data, accountId) {
  const ref = getAccountById(data, accountId);
  const account = ref?.account || {};
  const candidates = [
    account?.workspace?.settings?.avgTicketValueCents,
    account?.settings?.avgTicketValueCents,
    account?.avgTicketValueCents
  ];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n) && n >= 0) return Math.round(n);
  }
  return 0;
}

function recordRevenueStatusEvent(accountId, opportunity, eventType, metadata = {}) {
  logRevenueEvent(accountId, {
    contactId: opportunity?.contactId || null,
    relatedLeadEventId: opportunity?.sourceEventId || null,
    revenueEventType: eventType,
    estimatedValueCents: opportunity?.estimatedValueCents || null,
    confidence: opportunity?.confidence || 0.5,
    status: String(opportunity?.status || 'open'),
    metadata: {
      stage: opportunity?.stage,
      ...metadata
    }
  });
}
function ensureOpportunityDefaults(opp) {
  if (!opp || typeof opp !== 'object') return opp;
  opp.stage = normalizeStage(opp.stage);
  opp.stageHistory = Array.isArray(opp.stageHistory) ? opp.stageHistory : [];
  if (opp.lastInboundAt === undefined) opp.lastInboundAt = null;
  if (opp.lastOutboundAt === undefined) opp.lastOutboundAt = null;
  if (opp.lastActivityAt === undefined) opp.lastActivityAt = null;
  opp.followupsSentToday = Number.isFinite(Number(opp.followupsSentToday)) ? Number(opp.followupsSentToday) : 0;
  opp.followupsSentTotal = Number.isFinite(Number(opp.followupsSentTotal)) ? Number(opp.followupsSentTotal) : 0;
  opp.followupsDayKey = String(opp.followupsDayKey || '');
  opp.lastRecommendedActionType = opp.lastRecommendedActionType ? String(opp.lastRecommendedActionType) : null;
  opp.lastRecommendedActionAt = Number(opp.lastRecommendedActionAt || 0) || null;
  opp.cooldownUntil = Number(opp.cooldownUntil || 0) || null;
  if (typeof opp.quietHoursBypass !== 'boolean') opp.quietHoursBypass = false;
  if (typeof opp.stopAutomation !== 'boolean') opp.stopAutomation = false;
  opp.actionLogIds = Array.isArray(opp.actionLogIds) ? opp.actionLogIds : [];
  if (opp.projectedRecoveryCents === undefined) opp.projectedRecoveryCents = null;
  if (opp.projectedRecoveryProbability === undefined) opp.projectedRecoveryProbability = null;
  opp.riskHistory = Array.isArray(opp.riskHistory) ? opp.riskHistory : [];
  opp.automationsSentToday = Number.isFinite(Number(opp.automationsSentToday)) ? Number(opp.automationsSentToday) : 0;
  opp.automationsDayKey = String(opp.automationsDayKey || '');
  opp.agentState = opp.agentState && typeof opp.agentState === 'object' ? opp.agentState : {};
  if (opp.agentState.activeRunId === undefined) opp.agentState.activeRunId = null;
  if (opp.agentState.lastRunId === undefined) opp.agentState.lastRunId = null;
  if (opp.agentState.lockedUntil === undefined) opp.agentState.lockedUntil = null;
  if (opp.agentState.lockOwner === undefined) opp.agentState.lockOwner = null;
  opp.metadata = opp.metadata && typeof opp.metadata === 'object' ? opp.metadata : {};
  return opp;
}

function getConversationEvents(data, accountId, convoKey) {
  return (data.leadEvents || [])
    .filter((e) => String(e?.accountId || '') === String(accountId) && String(e?.convoKey || '') === String(convoKey || ''))
    .sort((a, b) => Number(a?.ts || 0) - Number(b?.ts || 0));
}

function latestEvent(events, type) {
  for (let i = events.length - 1; i >= 0; i -= 1) {
    if (String(events[i]?.type || '') === String(type || '')) return events[i];
  }
  return null;
}

function latestInboundTs(events) {
  const inboundTypes = new Set(['inbound_message', 'form_submit', 'after_hours_inquiry']);
  for (let i = events.length - 1; i >= 0; i -= 1) {
    if (inboundTypes.has(String(events[i]?.type || ''))) return Number(events[i]?.ts || 0) || 0;
  }
  return 0;
}

function latestOutboundTs(events) {
  for (let i = events.length - 1; i >= 0; i -= 1) {
    if (String(events[i]?.type || '') === 'outbound_message') return Number(events[i]?.ts || 0) || 0;
  }
  return 0;
}

function inferBusinessClosed(accountRef, ts) {
  const account = accountRef?.account || {};
  const hours = account?.workspace?.businessHours;
  if (!hours || typeof hours !== 'object') return false;
  const dayKeys = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
  const day = dayKeys[new Date(Number(ts || Date.now())).getDay()];
  const windows = Array.isArray(hours[day]) ? hours[day] : [];
  if (!windows.length) return true;
  const d = new Date(Number(ts || Date.now()));
  const nowMins = d.getHours() * 60 + d.getMinutes();
  return !windows.some((w) => {
    const start = String(w?.start || '');
    const end = String(w?.end || '');
    const m1 = start.match(/^(\d{2}):(\d{2})$/);
    const m2 = end.match(/^(\d{2}):(\d{2})$/);
    if (!m1 || !m2) return false;
    const s = Number(m1[1]) * 60 + Number(m1[2]);
    const e = Number(m2[1]) * 60 + Number(m2[2]);
    return nowMins >= s && nowMins < e;
  });
}

function computeProjectedRecovery(opp, accountRef) {
  const flags = getFeatureFlags(accountRef);
  if (flags.enableMoneyProjections !== true) {
    opp.projectedRecoveryCents = null;
    opp.projectedRecoveryProbability = null;
    return;
  }
  const risk = Number(opp?.riskScore || 0);
  let p = 0.2;
  if (risk >= 70) p = 0.7;
  else if (risk >= 50) p = 0.4;
  const value = Number(opp?.estimatedValueCents || 0);
  opp.projectedRecoveryProbability = p;
  opp.projectedRecoveryCents = Math.round(value * p);
}

function pushRiskHistory(opp, riskScore, reasons) {
  opp.riskHistory = Array.isArray(opp.riskHistory) ? opp.riskHistory : [];
  const prev = opp.riskHistory.length ? opp.riskHistory[opp.riskHistory.length - 1] : null;
  if (!prev || Number(prev.riskScore || -1) !== Number(riskScore)) {
    opp.riskHistory.push({
      ts: Date.now(),
      riskScore: Number(riskScore || 0),
      reasons: Array.isArray(reasons) ? reasons : []
    });
    opp.riskHistory = opp.riskHistory.slice(-200);
  }
}

function createLeadEvent(accountId, partialEvent = {}) {
  const data = loadData();
  const evt = {
    id: generateId(),
    accountId: String(accountId),
    contactId: partialEvent.contactId ? String(partialEvent.contactId) : null,
    convoKey: partialEvent.convoKey ? String(partialEvent.convoKey) : null,
    channel: normalizeChannel(partialEvent.channel),
    type: normalizeSignalType(partialEvent.type),
    ts: Number(partialEvent.ts || nowMs()),
    payload: partialEvent.payload && typeof partialEvent.payload === 'object' ? partialEvent.payload : {}
  };
  data.leadEvents = Array.isArray(data.leadEvents) ? data.leadEvents : [];
  data.leadEvents.push(evt);
  saveDataDebounced(data);
  return evt;
}

function upsertRevenueOpportunityFromEvent(accountId, leadEvent) {
  const data = loadData();
  data.revenueOpportunities = Array.isArray(data.revenueOpportunities) ? data.revenueOpportunities : [];
  const aid = String(accountId);
  const convoKey = leadEvent?.convoKey ? String(leadEvent.convoKey) : '';
  const contactId = leadEvent?.contactId ? String(leadEvent.contactId) : '';
  const sourceEventType = String(leadEvent?.type || '');
  const now = nowMs();

  const existing = data.revenueOpportunities.find((opp) => {
    if (String(opp?.accountId || '') !== aid) return false;
    if (!OPEN_STATUSES.has(String(opp?.status || ''))) return false;
    if (convoKey && String(opp?.convoKey || '') === convoKey) return true;
    if (contactId && String(opp?.contactId || '') === contactId) return true;
    return false;
  });

  if (existing) {
    ensureOpportunityDefaults(existing);
    updateActivityTimestamps(existing, leadEvent);
    existing.updatedAt = now;
    existing.lastEvaluatedAt = existing.lastEvaluatedAt || now;
    existing.metadata.lastSignalType = sourceEventType;
    existing.metadata.lastSignalTs = Number(leadEvent?.ts || now);
    existing.metadata.relatedEventIds = Array.isArray(existing.metadata.relatedEventIds) ? existing.metadata.relatedEventIds : [];
    if (leadEvent?.id && !existing.metadata.relatedEventIds.includes(String(leadEvent.id))) {
      existing.metadata.relatedEventIds.push(String(leadEvent.id));
      existing.metadata.relatedEventIds = existing.metadata.relatedEventIds.slice(-100);
    }
    if (sourceEventType === 'booking_created') {
      existing.status = 'won';
      existing.riskScore = 0;
      existing.riskReasons = [];
      transitionStage(existing, 'BOOKED', 'booking_created');
    } else if (sourceEventType === 'booking_completed') {
      existing.status = 'won';
      transitionStage(existing, 'WON', 'booking_completed');
    } else if (sourceEventType === 'inbound_message') {
      existing.status = 'recovered';
      existing.riskScore = clamp(Number(existing.riskScore || 0) - 40, 0, 100);
      existing.riskReasons = [];
    }
    if ((leadEvent?.payload?.stopAutomation === true) || ['opt_out', 'unsubscribe'].includes(sourceEventType)) {
      existing.stopAutomation = true;
      existing.status = 'lost';
      transitionStage(existing, 'LOST', 'opt_out_or_unsubscribe');
    }
    const events = getConversationEvents(data, accountId, existing.convoKey);
    const nextStage = deriveStage(existing, events);
    transitionStage(existing, nextStage, `event:${sourceEventType}`);
    saveDataDebounced(data);
    return existing;
  }

  const accountRef = getAccountById(data, aid);
  const opportunity = ensureOpportunityDefaults({
    id: generateId(),
    accountId: aid,
    contactId: contactId || null,
    convoKey: convoKey || null,
    status: sourceEventType === 'booking_created' ? 'won' : 'open',
    sourceEventId: leadEvent?.id ? String(leadEvent.id) : null,
    estimatedValueCents: getAccountAvgTicketValueCents(data, aid),
    confidence: 0.5,
    riskScore: 0,
    riskReasons: [],
    lastEvaluatedAt: now,
    createdAt: now,
    updatedAt: now,
    metadata: {
      serviceType: String(leadEvent?.payload?.serviceType || leadEvent?.payload?.service || '').trim() || null,
      intent: String(leadEvent?.payload?.intent || '').trim() || null,
      urgency: String(leadEvent?.payload?.urgency || '').trim() || null,
      afterHours: sourceEventType === 'after_hours_inquiry' || inferBusinessClosed(accountRef, leadEvent?.ts),
      lastSignalType: sourceEventType,
      lastSignalTs: Number(leadEvent?.ts || now),
      relatedEventIds: leadEvent?.id ? [String(leadEvent.id)] : []
    }
  });
  updateActivityTimestamps(opportunity, leadEvent);
  transitionStage(opportunity, 'NEW', 'opportunity_created');

  if (sourceEventType === 'booking_created') {
    transitionStage(opportunity, 'BOOKED', 'booking_created');
  } else if (sourceEventType === 'inbound_message') {
    transitionStage(opportunity, 'ENGAGED', 'inbound_message');
  }
  if ((leadEvent?.payload?.stopAutomation === true) || ['opt_out', 'unsubscribe'].includes(sourceEventType)) {
    opportunity.stopAutomation = true;
    opportunity.status = 'lost';
    transitionStage(opportunity, 'LOST', 'opt_out_or_unsubscribe');
  }

  data.revenueOpportunities.push(opportunity);
  const highValueThresholdCents = getHighValueThresholdCents(accountRef);
  if (Number(opportunity.estimatedValueCents || 0) >= highValueThresholdCents) {
    emitAccountEvent(aid, {
      type: 'high_value_lead',
      to: String(accountRef?.to || ''),
      from: String(opportunity?.contactId || ''),
      conversationId: String(opportunity?.convoKey || ''),
      meta: {
        opportunityId: String(opportunity?.id || ''),
        estimatedValueCents: Number(opportunity.estimatedValueCents || 0),
        thresholdCents: highValueThresholdCents
      }
    });
  }
  logRevenueEvent(accountId, {
    contactId: contactId || null,
    relatedLeadEventId: leadEvent?.id || null,
    revenueEventType: 'opportunity_created',
    estimatedValueCents: opportunity.estimatedValueCents,
    confidence: opportunity.confidence,
    status: opportunity.status,
    metadata: {
      signalType: sourceEventType,
      channel: leadEvent?.channel,
      serviceType: opportunity.metadata?.serviceType,
      afterHours: opportunity.metadata?.afterHours
    }
  });
  saveDataDebounced(data);
  return opportunity;
}

function computeRiskScore(accountId, revenueOpportunity, context = {}) {
  const data = context.data || loadData();
  const reasons = [];
  const status = String(revenueOpportunity?.status || '');
  if (TERMINAL_STATUSES.has(status)) {
    return { riskScore: 0, reasons: [] };
  }
  let riskScore = 0;

  const convoKey = String(revenueOpportunity?.convoKey || '');
  const events = getConversationEvents(data, accountId, convoKey);
  const inboundTs = latestInboundTs(events);
  const outboundTs = latestOutboundTs(events);
  const missedCallEvt = latestEvent(events, 'missed_call');
  const afterHoursEvt = latestEvent(events, 'after_hours_inquiry');

  if (status === 'open' || status === 'at_risk' || status === 'recovered') {
    if (inboundTs && (!outboundTs || outboundTs < inboundTs) && (nowMs() - inboundTs) >= (30 * 60 * 1000)) {
      riskScore += 40;
      reasons.push('no_response_30m');
    }
    if (missedCallEvt) {
      const missedTs = Number(missedCallEvt.ts || 0);
      const customerReplyAfterMissed = events.some((e) =>
        String(e?.type || '') === 'inbound_message' && Number(e?.ts || 0) > missedTs
      );
      if (!customerReplyAfterMissed && missedTs && (nowMs() - missedTs) >= (45 * 60 * 1000)) {
        riskScore += 35;
        reasons.push('missed_call_no_reply_45m');
      }
    }
    if (afterHoursEvt || revenueOpportunity?.metadata?.afterHours) {
      riskScore += 20;
      reasons.push('after_hours_lead');
    }
    const primaryKey = convoKey || String(revenueOpportunity?.contactId || '');
    const scopedKey = scopedIntelligenceKey(accountId, primaryKey);
    const intel = data.leadIntelligence?.[scopedKey] || {};
    const sentiment = Number(intel?.sentimentScore || 0);
    const urgency = Number(intel?.urgencyScore || 0);
    if (Number.isFinite(sentiment) && sentiment <= -40) {
      riskScore += 20;
      reasons.push('negative_sentiment');
    }
    if (Number.isFinite(urgency) && urgency >= 70) {
      riskScore += 25;
      reasons.push('high_urgency');
    }
  }

  return { riskScore: clamp(riskScore, 0, 100), reasons };
}

function evaluateOpportunity(accountId, opportunityId) {
  const data = loadData();
  const opp = (data.revenueOpportunities || []).find((o) =>
    String(o?.id || '') === String(opportunityId) && String(o?.accountId || '') === String(accountId)
  );
  if (!opp) return null;

  ensureOpportunityDefaults(opp);
  const convoKey = String(opp?.convoKey || '');
  const events = getConversationEvents(data, accountId, convoKey);
  const latestEvt = events.length ? events[events.length - 1] : null;
  const latestType = String(latestEvt?.type || '');
  const previousStatus = String(opp.status || '');
  const previousRiskReasons = Array.isArray(opp.riskReasons) ? [...opp.riskReasons] : [];

  if (latestType === 'booking_created') {
    opp.status = 'won';
    opp.riskScore = 0;
    opp.riskReasons = [];
    transitionStage(opp, 'BOOKED', 'booking_created');
  } else if (latestType === 'booking_completed') {
    opp.status = 'won';
    transitionStage(opp, 'WON', 'booking_completed');
  } else if (latestType === 'inbound_message') {
    opp.status = 'recovered';
  } else if (latestType === 'outbound_message' && normalizeStage(opp.stage) === 'NEW') {
    transitionStage(opp, 'CONTACTED', 'outbound_message');
  }

  const risk = computeRiskScore(accountId, opp, { data });
  opp.riskScore = risk.riskScore;
  opp.riskReasons = risk.reasons;
  const noResponseNow = opp.riskReasons.includes('no_response_30m');
  const noResponsePrev = previousRiskReasons.includes('no_response_30m');
  if (noResponseNow && !noResponsePrev) {
    emitAccountEvent(accountId, {
      type: 'no_response',
      to: String(getAccountById(data, accountId)?.to || ''),
      from: String(opp?.contactId || ''),
      conversationId: String(opp?.convoKey || ''),
      meta: {
        opportunityId: String(opp?.id || ''),
        riskScore: Number(opp.riskScore || 0)
      }
    });
  }
  pushRiskHistory(opp, opp.riskScore, opp.riskReasons);
  if (!TERMINAL_STATUSES.has(String(opp.status || ''))) {
    if (opp.status === 'recovered' && opp.riskScore <= 25) {
      // keep recovered
    } else if (opp.riskScore >= 70) {
      opp.status = 'at_risk';
    } else if (opp.status === 'at_risk' && opp.riskScore < 70) {
      opp.status = 'open';
    }
  }
  if (opp.stopAutomation === true) {
    opp.status = 'lost';
    transitionStage(opp, 'LOST', 'stop_automation');
  }

  if (opp.status === 'at_risk' && previousStatus !== 'at_risk') {
    recordRevenueStatusEvent(accountId, opp, 'opportunity_at_risk', { signalType: latestType });
  }
  if (opp.status === 'recovered' && previousStatus !== 'recovered') {
    recordRevenueStatusEvent(accountId, opp, 'opportunity_recovered', { signalType: latestType });
  }
  if (opp.status === 'won' && previousStatus !== 'won') {
    recordRevenueStatusEvent(accountId, opp, opp.stage === 'BOOKED' ? 'appointment_booked' : 'sale_closed', { signalType: latestType });
  }

  const stage = deriveStage(opp, events);
  transitionStage(opp, stage, `derive:${latestType || 'periodic'}`);
  const accountRef = getAccountById(data, accountId);
  computeProjectedRecovery(opp, accountRef);
  opp.lastEvaluatedAt = nowMs();
  opp.updatedAt = nowMs();
  saveDataDebounced(data);
  return opp;
}

function getAtRiskOpportunities(accountId, { limit = 20 } = {}) {
  const data = loadData();
  const scoped = (data.revenueOpportunities || [])
    .filter((o) => String(o?.accountId || '') === String(accountId))
    .map((o) => evaluateOpportunity(accountId, o.id))
    .filter(Boolean)
    .filter((o) => String(o?.status || '') === 'at_risk')
    .sort((a, b) => Number(b?.riskScore || 0) - Number(a?.riskScore || 0));
  return scoped.slice(0, Math.max(1, Math.min(200, Number(limit) || 20)));
}

function updateIntelligence(accountId, key, patch = {}) {
  const sourceKey = String(key || '').trim();
  if (!sourceKey) return null;
  const data = loadData();
  const scopedKey = scopedIntelligenceKey(accountId, sourceKey);
  const existing = data.leadIntelligence?.[scopedKey] || {};
  const next = {
    intent: String(patch.intent || existing.intent || 'unknown'),
    urgencyScore: clamp(Number(patch.urgencyScore ?? existing.urgencyScore ?? 0), 0, 100),
    sentimentScore: clamp(Number(patch.sentimentScore ?? existing.sentimentScore ?? 0), -100, 100),
    leadQualityScore: clamp(Number(patch.leadQualityScore ?? existing.leadQualityScore ?? 0), 0, 100),
    lastAnalyzedAt: Number(patch.lastAnalyzedAt || nowMs()),
    notes: Array.isArray(patch.notes) ? patch.notes.slice(0, 20).map((x) => String(x)) : (Array.isArray(existing.notes) ? existing.notes : []),
    model: patch.model ? String(patch.model) : (existing.model ? String(existing.model) : undefined)
  };
  data.leadIntelligence = data.leadIntelligence && typeof data.leadIntelligence === 'object' ? data.leadIntelligence : {};
  data.leadIntelligence[scopedKey] = next;
  saveDataDebounced(data);
  return next;
}

function createAlert(accountId, input = {}) {
  const data = loadData();
  const alert = {
    id: generateId(),
    accountId: String(accountId),
    type: String(input.type || 'general'),
    severity: String(input.severity || 'info'),
    message: String(input.message || ''),
    data: input.data && typeof input.data === 'object' ? input.data : {},
    createdAt: Number(input.createdAt || nowMs()),
    acknowledgedAt: input.acknowledgedAt ? Number(input.acknowledgedAt) : null
  };
  data.alerts = Array.isArray(data.alerts) ? data.alerts : [];
  data.alerts.push(alert);
  saveDataDebounced(data);
  return alert;
}

module.exports = {
  createLeadEvent,
  upsertRevenueOpportunityFromEvent,
  computeRiskScore,
  evaluateOpportunity,
  getAtRiskOpportunities,
  updateIntelligence,
  createAlert,
  scopedIntelligenceKey,
  getPolicyConfig,
  getFeatureFlags,
  ensureOpportunityDefaults,
  getConversationEvents,
  computeProjectedRecovery
};
