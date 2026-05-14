const { loadData, saveDataDebounced, getAccountById } = require('../store/dataStore');
const { OUTCOME_PACKS } = require('./flowTemplates');

function ensurePackSettings(account) {
  if (!account.settings) account.settings = {};
  if (!account.settings.outcomePacks || typeof account.settings.outcomePacks !== 'object') {
    account.settings.outcomePacks = {};
  }
  return account.settings.outcomePacks;
}

function getPackDefinition(packId) {
  return OUTCOME_PACKS?.[packId] || null;
}

function getPackEnabled(account, packId) {
  const pack = getPackDefinition(packId);
  if (!pack) return false;
  const settings = ensurePackSettings(account);
  if (Object.prototype.hasOwnProperty.call(settings, packId)) {
    return settings[packId]?.enabled === true;
  }
  return pack.defaultEnabled === true;
}

function computePackMetrics(accountId, pack, data) {
  const signals = new Set((pack.signals || []).map((s) => String(s || '').toLowerCase()));
  const leads = (data.leadEvents || []).filter((evt) =>
    String(evt?.accountId || '') === String(accountId) && signals.has(String(evt?.type || '').toLowerCase())
  );
  const opportunities = (data.revenueOpportunities || []).filter((opp) =>
    String(opp?.accountId || '') === String(accountId)
  );
  const recoveredValueCents = opportunities
    .filter((opp) => ['recovered', 'won'].includes(String(opp?.status || '').toLowerCase()))
    .filter((opp) => signals.has(String(opp?.metadata?.lastSignalType || '').toLowerCase()))
    .reduce((sum, opp) => sum + Number(opp?.estimatedValueCents || 0), 0);
  const atRiskValueCents = opportunities
    .filter((opp) => String(opp?.status || '').toLowerCase() === 'at_risk')
    .filter((opp) => signals.has(String(opp?.metadata?.lastSignalType || '').toLowerCase()))
    .reduce((sum, opp) => sum + Number(opp?.estimatedValueCents || 0), 0);
  return {
    signalsCaptured: leads.length,
    recoveredValueCents,
    atRiskValueCents
  };
}

function buildPackPayload(accountId, pack, account, data) {
  const enabled = getPackEnabled(account, pack.id);
  const metrics = computePackMetrics(accountId, pack, data);
  return {
    ...pack,
    enabled,
    metrics,
    targetSignals: pack.signals || pack.targetSignals || []
  };
}

function getOutcomePacks(accountId) {
  const data = loadData();
  const account = getAccountById(data, accountId)?.account || {};
  const packs = [];
  for (const pack of Object.values(OUTCOME_PACKS || {})) {
    packs.push(buildPackPayload(accountId, pack, account, data));
  }
  return packs;
}

function setPackEnabled(accountId, packId, enabled = true) {
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  if (!accountRef?.account) return null;
  const account = accountRef.account;
  ensurePackSettings(account);
  account.settings.outcomePacks = {
    ...account.settings.outcomePacks,
    [packId]: { enabled: enabled === true }
  };
  saveDataDebounced(data);
  return account.settings.outcomePacks[packId];
}

function getPackSelection(accountId) {
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  if (!accountRef?.account) return [];
  const settings = ensurePackSettings(accountRef.account);
  return Object.entries(settings).map(([key, value]) => ({ packId: key, enabled: Boolean(value?.enabled) }));
}

module.exports = {
  getOutcomePacks,
  setPackEnabled,
  getPackDefinition,
  getPackSelection
};
