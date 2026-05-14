const { pool } = require('../db/pool');
const { getDeliveryAnalyticsSummary, listRecentFailures, getById } = require('../repositories/messagesRepo');
const { getByConvoKey, getByRowId } = require('../repositories/conversationsRepo');

const RANGE_TO_MS = Object.freeze({
  '24h': 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000,
  '30d': 30 * 24 * 60 * 60 * 1000
});

function resolveRangeStart(range = '7d', now = Date.now()) {
  const windowMs = RANGE_TO_MS[String(range || '7d').trim()] || RANGE_TO_MS['7d'];
  return Number(now) - windowMs;
}

async function getMessagingAnalyticsSummary(accountId, { range = '7d', now = Date.now() } = {}) {
  const rangeStart = resolveRangeStart(range, now);
  const summary = await getDeliveryAnalyticsSummary(pool, accountId, { rangeStart });
  return {
    ...summary,
    range: String(range || '7d'),
    asOf: Number(now)
  };
}

async function getConversationByConvoKey(accountId, convoKey) {
  return getByConvoKey(pool, accountId, convoKey);
}

async function getConversationByRowId(accountId, rowId) {
  return getByRowId(pool, accountId, rowId);
}

async function getMessageById(accountId, messageId) {
  return getById(pool, accountId, messageId);
}

async function getRecentFailedMessages(accountId, { limit = 25 } = {}) {
  return listRecentFailures(pool, accountId, { limit });
}

module.exports = {
  resolveRangeStart,
  getMessagingAnalyticsSummary,
  getConversationByConvoKey,
  getConversationByRowId,
  getMessageById,
  getRecentFailedMessages
};
