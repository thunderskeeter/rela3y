const { decideActionPlan, normalizeBusinessContext } = require('./aiDecisionEngine');
const { getPolicyConfig, getConversationEvents } = require('./revenueIntelligenceService');
const { loadData, getAccountById } = require('../store/dataStore');

function buildDecisionContext(accountId) {
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  const policy = getPolicyConfig(accountRef);
  return { policy, businessContext: normalizeBusinessContext(accountId) };
}

function createActionPlan(accountId, leadEvent, opportunity) {
  const context = buildDecisionContext(accountId, opportunity);
  const plan = decideActionPlan(accountId, { leadEvent, opportunity });
  const events = getConversationEvents(loadData(), accountId, String(opportunity?.convoKey || ''));

  return {
    ...plan,
    policy: context.policy,
    businessContext: context.businessContext,
    events,
    signalId: leadEvent?.id || null,
    signalType: leadEvent?.type || 'lead_stalled'
  };
}

module.exports = {
  createActionPlan
};
