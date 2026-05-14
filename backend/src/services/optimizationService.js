const { loadData, saveDataDebounced } = require('../store/dataStore');
const { OUTCOME_PACKS } = require('./flowTemplates');
const { generateId } = require('../utils/id');

function localParts(ts, timezone) {
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone || 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  });
  const parts = fmt.formatToParts(new Date(ts));
  const map = {};
  for (const p of parts) map[p.type] = p.value;
  return {
    dayKey: `${map.year}-${map.month}-${map.day}`,
    hour: Number(map.hour || 0),
    minute: Number(map.minute || 0)
  };
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) return (sorted[mid - 1] + sorted[mid]) / 2;
  return sorted[mid];
}

function getPolicyDefaults(account) {
  return account?.settings?.policies || account?.workspace?.settings?.policies || {
    dailyFollowupCapPerLead: 2,
    minCooldownMinutes: 30,
    quietHours: { startHour: 20, endHour: 8, timezone: 'America/New_York' },
    maxAutomationsPerOpportunityPerDay: 4
  };
}

function ensurePackConfig(account) {
  account.settings = account.settings && typeof account.settings === 'object' ? account.settings : {};
  account.settings.packConfig = account.settings.packConfig && typeof account.settings.packConfig === 'object'
    ? account.settings.packConfig
    : {};
  for (const packId of Object.keys(OUTCOME_PACKS || {})) {
    const existing = account.settings.packConfig[packId] || {};
    account.settings.packConfig[packId] = {
      followupDelaysMinutes: Array.isArray(existing.followupDelaysMinutes) && existing.followupDelaysMinutes.length
        ? existing.followupDelaysMinutes.map((x) => Number(x)).filter((x) => Number.isFinite(x))
        : [30, 120],
      maxFollowups: Number.isFinite(Number(existing.maxFollowups)) ? Number(existing.maxFollowups) : 2,
      messageVariant: String(existing.messageVariant || 'friendly')
    };
  }
  return account.settings.packConfig;
}

function getPackMetrics(data, accountId, packId) {
  const actions = (data.actions || []).filter((a) =>
    String(a?.accountId || '') === String(accountId) &&
    String(a?.payload?.recommendedPack || '') === String(packId)
  );
  if (!actions.length) {
    return { responseRate: 0, recoveryRate: 0, medianTimeToReply: 0, followupByDelay: {} };
  }
  const byOpp = new Set(actions.map((a) => String(a?.opportunityId || '')));
  const opps = (data.revenueOpportunities || []).filter((o) =>
    String(o?.accountId || '') === String(accountId) && byOpp.has(String(o?.id || ''))
  );
  const recovered = opps.filter((o) => ['recovered', 'booked', 'won'].includes(String(o?.status || '').toLowerCase())).length;
  const recoveryRate = opps.length ? recovered / opps.length : 0;
  const replies = actions.filter((a) => String(a?.outcome?.status || '') === 'sent' && a.payload?.responseReceived === true).length;
  const responseRate = actions.length ? replies / actions.length : 0;
  const replyMins = actions
    .map((a) => Number(a?.payload?.replyMinutes || 0))
    .filter((n) => Number.isFinite(n) && n > 0);
  const followupByDelay = {};
  for (const a of actions) {
    const d = Number(a?.payload?.followupDelay || 0);
    if (!Number.isFinite(d) || d <= 0) continue;
    const bucket = d < 30 ? 'lt30' : d <= 120 ? '30to120' : 'gt120';
    if (!followupByDelay[bucket]) followupByDelay[bucket] = { total: 0, success: 0 };
    followupByDelay[bucket].total += 1;
    if (a.payload?.responseReceived === true) followupByDelay[bucket].success += 1;
  }
  return { responseRate, recoveryRate, medianTimeToReply: median(replyMins), followupByDelay };
}

function chooseVariant(enableVariants, metrics) {
  if (!enableVariants) return 'friendly';
  if (metrics.responseRate < 0.2) return 'direct';
  if (metrics.responseRate > 0.5) return 'short';
  return 'friendly';
}

async function optimizeOutcomePacks({ force = false } = {}) {
  const data = loadData();
  data.optimizationEvents = Array.isArray(data.optimizationEvents) ? data.optimizationEvents : [];
  for (const [to, account] of Object.entries(data.accounts || {})) {
    const timezone = String(account?.workspace?.timezone || 'America/New_York');
    const lp = localParts(Date.now(), timezone);
    account.settings = account.settings && typeof account.settings === 'object' ? account.settings : {};
    const lastKey = String(account.settings.lastOptimizationDayKey || '');
    if (!force) {
      if (lp.hour !== 3) continue;
      if (lastKey === lp.dayKey) continue;
    }
    const featureFlags = account?.settings?.featureFlags || account?.workspace?.settings?.featureFlags || {};
    if (featureFlags.enableOptimization !== true && !force) continue;

    const packConfig = ensurePackConfig(account);
    for (const packId of Object.keys(OUTCOME_PACKS || {})) {
      const cfg = packConfig[packId];
      const oldCfg = JSON.parse(JSON.stringify(cfg));
      const metrics = getPackMetrics(data, account.accountId || account.id, packId);

      const scale = metrics.recoveryRate < 0.25 ? 0.9 : metrics.recoveryRate > 0.55 ? 1.1 : 1.0;
      cfg.followupDelaysMinutes = cfg.followupDelaysMinutes
        .map((d) => clamp(Math.round(Number(d || 30) * scale), 15, 24 * 60));
      cfg.maxFollowups = clamp(
        metrics.responseRate < 0.15 ? Number(cfg.maxFollowups || 2) + 1 : Number(cfg.maxFollowups || 2) - (metrics.responseRate > 0.5 ? 1 : 0),
        1,
        3
      );
      cfg.messageVariant = chooseVariant(featureFlags.enableAIMessageVariants === true, metrics);

      data.optimizationEvents.push({
        id: generateId(),
        accountId: String(account.accountId || account.id || ''),
        ts: Date.now(),
        type: 'optimization',
        packId,
        oldSettings: oldCfg,
        newSettings: cfg,
        metrics
      });
    }
    account.settings.lastOptimizationDayKey = lp.dayKey;
  }
  saveDataDebounced(data);
  return { ok: true };
}

module.exports = {
  optimizeOutcomePacks
};
