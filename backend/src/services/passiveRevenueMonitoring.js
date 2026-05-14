const { loadData, saveDataDebounced, getAccountById } = require('../store/dataStore');
const {
  evaluateOpportunity,
  createLeadEvent,
  createAlert,
  getPolicyConfig,
  ensureOpportunityDefaults
} = require('./revenueIntelligenceService');
const { persistSnapshotOpportunity } = require('./opportunitiesService');
const { logActionStartWrite, logActionResultWrite, attachActionToOpportunityWrite } = require('./actionsService');

const DAY_MS = 24 * 60 * 60 * 1000;

function dayKey(ts = Date.now()) {
  return new Date(ts).toISOString().slice(0, 10);
}

function refreshCountersForDay(opp, nowTs = Date.now()) {
  const dk = dayKey(nowTs);
  if (String(opp.followupsDayKey || '') !== dk) {
    opp.followupsDayKey = dk;
    opp.followupsSentToday = 0;
  }
  if (String(opp.automationsDayKey || '') !== dk) {
    opp.automationsDayKey = dk;
    opp.automationsSentToday = 0;
  }
}

function isWithinQuietHours(policy, ts = Date.now()) {
  const q = policy?.quietHours || {};
  const start = Number(q.startHour);
  const end = Number(q.endHour);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return false;
  const h = new Date(ts).getHours();
  if (start === end) return false;
  if (start < end) return h >= start && h < end;
  return h >= start || h < end;
}

function nextOpenTime(policy, ts = Date.now()) {
  const q = policy?.quietHours || {};
  const end = Number(q.endHour);
  const d = new Date(ts);
  if (!Number.isFinite(end)) return ts + (60 * 60 * 1000);
  d.setHours(end, 0, 0, 0);
  if (d.getTime() <= ts) d.setDate(d.getDate() + 1);
  return d.getTime();
}

async function runPassiveRevenueMonitoring() {
  const data = loadData();
  const { handleSignal } = require('./revenueOrchestrator');
  const byAccount = {};
  for (const opp of data.revenueOpportunities || []) {
    const aid = String(opp?.accountId || '');
    if (!aid) continue;
    if (!byAccount[aid]) byAccount[aid] = [];
    byAccount[aid].push(opp);
  }

  for (const [accountId, opps] of Object.entries(byAccount)) {
    const accountRef = getAccountById(data, accountId);
    const policy = getPolicyConfig(accountRef);
    for (const rawOpp of opps) {
      ensureOpportunityDefaults(rawOpp);
      refreshCountersForDay(rawOpp);
      if (!['open', 'at_risk', 'recovered'].includes(String(rawOpp?.status || '').toLowerCase())) continue;
      const prevRisk = Number(rawOpp?.riskScore || 0);
      const evaluated = evaluateOpportunity(accountId, rawOpp.id);
      if (!evaluated) continue;

      if ((evaluated.cooldownUntil || 0) > Date.now()) {
        continue;
      }
      const crossed = prevRisk < 70 && Number(evaluated.riskScore || 0) >= 70;
      if (!crossed) continue;

      if (Number(evaluated.followupsSentToday || 0) >= Number(policy.dailyFollowupCapPerLead || 2)) {
        const alert = createAlert(accountId, {
          type: 'automation_paused_daily_cap',
          severity: 'warning',
          message: 'Automation paused: repeated no-response; check lead manually.',
          data: { opportunityId: evaluated.id, followupsSentToday: evaluated.followupsSentToday }
        });
        const action = await logActionStartWrite({
          accountId,
          opportunityId: evaluated.id,
          contactId: evaluated.contactId,
          convoKey: evaluated.convoKey,
          actionType: 'create_alert',
          channel: 'sms',
          payload: { alertId: alert.id, message: alert.message },
          justification: {
            trigger: 'prm_tick',
            riskScore: Number(evaluated.riskScore || 0),
            reasons: ['daily_followup_cap'],
            stageBefore: evaluated.stage,
            stageAfter: evaluated.stage,
            decisionVersion: 'deterministic_v2',
            policy: {
              dailyCap: Number(policy.dailyFollowupCapPerLead || 2),
              cooldownMinutes: Number(policy.minCooldownMinutes || 30),
              quietHours: isWithinQuietHours(policy, Date.now()),
              complianceChecked: true
            }
          }
        });
        await logActionResultWrite(accountId, action.id, { status: 'sent' });
        await attachActionToOpportunityWrite(accountId, evaluated.id, action.id);
        continue;
      }

      if (
        evaluated.lastRecommendedActionType &&
        evaluated.lastRecommendedActionAt &&
        (Date.now() - Number(evaluated.lastRecommendedActionAt)) < (Number(policy.minCooldownMinutes || 30) * 60 * 1000)
      ) {
        continue;
      }

      if (isWithinQuietHours(policy, Date.now()) && evaluated.quietHoursBypass !== true) {
        const action = await logActionStartWrite({
          accountId,
          opportunityId: evaluated.id,
          contactId: evaluated.contactId,
          convoKey: evaluated.convoKey,
          actionType: 'schedule_followup',
          channel: 'sms',
          payload: {
            reason: 'quiet_hours',
            scheduledFor: nextOpenTime(policy, Date.now())
          },
          justification: {
            trigger: 'prm_tick',
            riskScore: Number(evaluated.riskScore || 0),
            reasons: ['quiet_hours'],
            stageBefore: evaluated.stage,
            stageAfter: evaluated.stage,
            decisionVersion: 'deterministic_v2',
            policy: {
              dailyCap: Number(policy.dailyFollowupCapPerLead || 2),
              cooldownMinutes: Number(policy.minCooldownMinutes || 30),
              quietHours: true,
              complianceChecked: true
            }
          }
        });
        await logActionResultWrite(accountId, action.id, { status: 'sent' });
        await attachActionToOpportunityWrite(accountId, evaluated.id, action.id);
        evaluated.cooldownUntil = Date.now() + (Number(policy.minCooldownMinutes || 30) * 60 * 1000);
        await persistSnapshotOpportunity(accountId, evaluated, { operation: 'prm_quiet_hours_cooldown' });
        continue;
      }

      const evt = createLeadEvent(accountId, {
        contactId: evaluated.contactId || null,
        convoKey: evaluated.convoKey || null,
        channel: 'chat',
        type: 'lead_stalled',
        payload: {
          source: 'prm_tick',
          previousRisk: prevRisk,
          newRisk: evaluated.riskScore
        }
      });
      await handleSignal(accountId, evt);
    }
  }
  saveDataDebounced(data);
}

async function runReactivationScan() {
  const { handleSignal } = require('./revenueOrchestrator');
  const data = loadData();
  const cutoff = Date.now() - (30 * DAY_MS);
  const seen = new Set();
  for (const [id, convo] of Object.entries(data.conversations || {})) {
    const accountId = String(convo?.accountId || '');
    if (!accountId) continue;
    const lastActivityAt = Number(convo?.lastActivityAt || 0);
    if (!lastActivityAt || lastActivityAt > cutoff) continue;
    if (seen.has(id)) continue;
    seen.add(id);
    const existingOpen = (data.revenueOpportunities || []).find((o) =>
      String(o?.accountId || '') === accountId &&
      String(o?.convoKey || '') === String(id) &&
      ['open', 'at_risk', 'recovered'].includes(String(o?.status || '').toLowerCase())
    );
    if (existingOpen) continue;
    const evt = createLeadEvent(accountId, {
      convoKey: id,
      channel: 'sms',
      type: 'lead_stalled',
      payload: {
        source: 'reactivation_scan',
        inactiveDays: Math.floor((Date.now() - lastActivityAt) / DAY_MS)
      }
    });
    await handleSignal(accountId, evt);
  }
}

function buildResponseTimePairs(leadEvents, accountId, sinceTs) {
  const scoped = (leadEvents || [])
    .filter((e) => String(e?.accountId || '') === String(accountId) && Number(e?.ts || 0) >= sinceTs)
    .sort((a, b) => Number(a?.ts || 0) - Number(b?.ts || 0));
  const lastInboundByConvo = {};
  const deltas = [];
  for (const e of scoped) {
    const convoKey = String(e?.convoKey || '');
    if (!convoKey) continue;
    if (String(e?.type || '') === 'inbound_message') {
      lastInboundByConvo[convoKey] = Number(e?.ts || 0);
      continue;
    }
    if (String(e?.type || '') === 'outbound_message') {
      const inboundTs = Number(lastInboundByConvo[convoKey] || 0);
      const outTs = Number(e?.ts || 0);
      if (inboundTs > 0 && outTs > inboundTs) {
        deltas.push((outTs - inboundTs) / 60000);
        delete lastInboundByConvo[convoKey];
      }
    }
  }
  return deltas;
}

function runPerformanceAlerts() {
  const data = loadData();
  const now = Date.now();
  const accounts = new Set((data.revenueOpportunities || []).map((o) => String(o?.accountId || '')).filter(Boolean));
  for (const accountId of accounts) {
    const since7 = now - (7 * DAY_MS);
    const since30 = now - (30 * DAY_MS);
    const opps = (data.revenueOpportunities || []).filter((o) => String(o?.accountId || '') === accountId);
    const weekOpps = opps.filter((o) => Number(o?.createdAt || 0) >= since7);
    const monthOpps = opps.filter((o) => Number(o?.createdAt || 0) >= since30);
    const weekRecovered = weekOpps.filter((o) => ['recovered', 'won'].includes(String(o?.status || '').toLowerCase())).length;
    const monthRecovered = monthOpps.filter((o) => ['recovered', 'won'].includes(String(o?.status || '').toLowerCase())).length;
    const weekRecoveryRate = weekOpps.length ? weekRecovered / weekOpps.length : 0;
    const monthRecoveryRate = monthOpps.length ? monthRecovered / monthOpps.length : 0;

    const weekResponseValues = buildResponseTimePairs(data.leadEvents, accountId, since7);
    const monthResponseValues = buildResponseTimePairs(data.leadEvents, accountId, since30);
    const weekResponse = weekResponseValues.length ? weekResponseValues.reduce((a, b) => a + b, 0) / weekResponseValues.length : 0;
    const monthResponse = monthResponseValues.length ? monthResponseValues.reduce((a, b) => a + b, 0) / monthResponseValues.length : 0;

    const weekMissedCalls = (data.leadEvents || []).filter((e) =>
      String(e?.accountId || '') === accountId &&
      String(e?.type || '') === 'missed_call' &&
      Number(e?.ts || 0) >= since7
    ).length;
    const monthMissedCallsPerWeek = (data.leadEvents || []).filter((e) =>
      String(e?.accountId || '') === accountId &&
      String(e?.type || '') === 'missed_call' &&
      Number(e?.ts || 0) >= since30
    ).length / 4.2857;

    if (monthResponse > 0 && weekResponse > monthResponse * 1.3) {
      createAlert(accountId, {
        type: 'response_time_regression',
        severity: 'warning',
        message: 'Average response time worsened vs 30-day baseline.',
        data: { weekResponse, monthResponse }
      });
    }
    if (monthRecoveryRate > 0 && weekRecoveryRate < monthRecoveryRate * 0.75) {
      createAlert(accountId, {
        type: 'recovery_rate_drop',
        severity: 'warning',
        message: 'Recovery rate dropped materially vs baseline.',
        data: { weekRecoveryRate, monthRecoveryRate }
      });
    }
    if (monthMissedCallsPerWeek > 0 && weekMissedCalls > monthMissedCallsPerWeek * 1.4) {
      createAlert(accountId, {
        type: 'missed_calls_spike',
        severity: 'warning',
        message: 'Missed call volume spiked above baseline.',
        data: { weekMissedCalls, monthMissedCallsPerWeek }
      });
    }
  }
}

module.exports = {
  runPassiveRevenueMonitoring,
  runReactivationScan,
  runPerformanceAlerts
};
