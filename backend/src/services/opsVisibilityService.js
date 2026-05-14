const { loadData, saveDataDebounced, getAccountById } = require('../store/dataStore');
const { emitEvent } = require('./notificationService');
const { generateId } = require('../utils/id');

const DAY_MS = 24 * 60 * 60 * 1000;

function startOfDayTs(ts = Date.now()) {
  const d = new Date(ts);
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}

function isoWeekKey(ts = Date.now()) {
  const d = new Date(ts);
  const utcDate = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const dayNum = utcDate.getUTCDay() || 7;
  utcDate.setUTCDate(utcDate.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(utcDate.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((utcDate - yearStart) / DAY_MS) + 1) / 7);
  return `${utcDate.getUTCFullYear()}-W${String(weekNo).padStart(2, '0')}`;
}

function safeEvents(data, accountId) {
  return (data?.revenueEvents || []).filter((e) => String(e?.business_id || '') === String(accountId));
}

function buildResponsePairs(leadEvents, accountId, fromTs, toTs) {
  const scoped = (leadEvents || [])
    .filter((e) => String(e?.accountId || '') === String(accountId))
    .filter((e) => {
      const ts = Number(e?.ts || 0);
      return ts >= fromTs && ts <= toTs;
    })
    .sort((a, b) => Number(a?.ts || 0) - Number(b?.ts || 0));
  const inboundByConvo = {};
  const deltas = [];
  for (const e of scoped) {
    const convo = String(e?.convoKey || '');
    if (!convo) continue;
    const type = String(e?.type || '');
    const ts = Number(e?.ts || 0);
    if (type === 'inbound_message') {
      inboundByConvo[convo] = ts;
      continue;
    }
    if (type === 'outbound_message') {
      const inTs = Number(inboundByConvo[convo] || 0);
      if (inTs > 0 && ts > inTs) {
        deltas.push((ts - inTs) / 60000);
        delete inboundByConvo[convo];
      }
    }
  }
  return deltas;
}

function computeWinsSummaryFromData(data, accountId, days = 1, nowTs = Date.now()) {
  const windowDays = Math.max(1, Number(days || 1));
  const toTs = Number(nowTs || Date.now());
  const fromTs = toTs - (windowDays * DAY_MS);
  const revenueEvents = safeEvents(data, accountId);
  const leadEvents = Array.isArray(data?.leadEvents) ? data.leadEvents : [];

  const windowRevenue = revenueEvents.filter((e) => {
    const ts = Number(e?.created_at || 0);
    return ts >= fromTs && ts <= toTs;
  });
  const missedCalls = leadEvents.filter((e) =>
    String(e?.accountId || '') === String(accountId) &&
    String(e?.type || '') === 'missed_call' &&
    Number(e?.ts || 0) >= fromTs &&
    Number(e?.ts || 0) <= toTs
  ).length;

  const recoveredEvents = windowRevenue.filter((e) => {
    const type = String(e?.revenue_event_type || '').toLowerCase();
    const status = String(e?.status || '').toLowerCase();
    return ['opportunity_recovered', 'appointment_booked', 'sale_closed'].includes(type) || status === 'won';
  });
  const bookedEvents = windowRevenue.filter((e) => {
    const type = String(e?.revenue_event_type || '').toLowerCase();
    return ['appointment_booked', 'booking_created'].includes(type);
  });
  const recoveredFromMissedCalls = recoveredEvents.filter((e) =>
    String(e?.metadata_json?.signalType || '').toLowerCase().includes('missed_call')
  ).length;
  const recoveredRevenueCents = recoveredEvents.reduce((acc, e) => acc + Number(e?.estimated_value_cents || 0), 0);

  const responseDeltas = buildResponsePairs(leadEvents, accountId, fromTs, toTs);
  const responseSlaMinutes = responseDeltas.length
    ? Number((responseDeltas.reduce((a, b) => a + b, 0) / responseDeltas.length).toFixed(1))
    : null;

  return {
    days: windowDays,
    fromTs,
    toTs,
    recoveredRevenueCents,
    recoveredCalls: recoveredFromMissedCalls,
    bookedJobs: bookedEvents.length,
    missedCalls,
    responseSlaMinutes
  };
}

function buildWeeklyDigestPayload(data, accountId, nowTs = Date.now()) {
  const week = computeWinsSummaryFromData(data, accountId, 7, nowTs);
  return {
    weekKey: isoWeekKey(nowTs),
    generatedAt: nowTs,
    summary: week,
    headline: `Recovered ${week.recoveredRevenueCents} cents with ${week.bookedJobs} booked jobs this week.`
  };
}

function shouldEmitWeeklyDigest(account) {
  const triggers = account?.settings?.notifications?.triggers || {};
  return triggers.weeklyDigest !== false;
}

function emitWeeklyOwnerDigestForAccount(accountId, options = {}) {
  const nowTs = Number(options?.nowTs || Date.now());
  const force = options?.force === true;
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  const account = accountRef?.account || null;
  if (!account || !accountRef?.to) return { ok: false, reason: 'account_not_found' };
  if (!shouldEmitWeeklyDigest(account)) return { ok: true, skipped: true, reason: 'weekly_digest_disabled' };

  account.settings = account.settings && typeof account.settings === 'object' ? account.settings : {};
  account.settings.notifications = account.settings.notifications && typeof account.settings.notifications === 'object'
    ? account.settings.notifications
    : {};
  account.settings.notifications.weeklyDigestState = account.settings.notifications.weeklyDigestState
    && typeof account.settings.notifications.weeklyDigestState === 'object'
    ? account.settings.notifications.weeklyDigestState
    : {};

  const weekKey = isoWeekKey(nowTs);
  const lastSentWeekKey = String(account.settings.notifications.weeklyDigestState.lastSentWeekKey || '');
  if (!force && lastSentWeekKey === weekKey) {
    return { ok: true, skipped: true, reason: 'already_sent_for_week', weekKey };
  }

  const payload = buildWeeklyDigestPayload(data, accountId, nowTs);
  const tenant = { accountId: String(accountId), to: String(accountRef.to) };
  const delivery = emitEvent(tenant, {
    type: 'weekly_digest',
    to: String(accountRef.to),
    from: 'relay_system',
    conversationId: `weekly_digest__${weekKey}`,
    meta: {
      preferredChannel: 'email',
      digest: payload
    }
  });

  data.optimizationEvents = Array.isArray(data.optimizationEvents) ? data.optimizationEvents : [];
  data.optimizationEvents.push({
    id: generateId(),
    accountId: String(accountId),
    ts: nowTs,
    type: 'weekly_owner_digest_generated',
    payload,
    delivery
  });

  if (delivery?.ok && !delivery?.blocked) {
    account.settings.notifications.weeklyDigestState.lastSentWeekKey = weekKey;
    account.settings.notifications.weeklyDigestState.lastSentAt = nowTs;
  }

  saveDataDebounced(data);
  return { ok: true, weekKey, delivery, payload };
}

function runWeeklyOwnerDigest() {
  const data = loadData();
  const accountIds = new Set((data.accounts ? Object.values(data.accounts) : [])
    .map((a) => String(a?.accountId || '').trim())
    .filter(Boolean));
  for (const accountId of accountIds) {
    try {
      emitWeeklyOwnerDigestForAccount(accountId);
    } catch (err) {
      console.error('Weekly digest generation failed:', err?.message || err);
    }
  }
}

module.exports = {
  computeWinsSummaryFromData,
  buildWeeklyDigestPayload,
  emitWeeklyOwnerDigestForAccount,
  runWeeklyOwnerDigest
};
