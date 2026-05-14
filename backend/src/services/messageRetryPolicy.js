const MAX_RETRY_COUNT = 3;
const RETRY_DELAY_MS = {
  1: 5 * 60 * 1000,
  2: 30 * 60 * 1000
};
const { isRetryableStatus } = require('./messageStatusPolicy');

function computeNextRetryAt({ retryCount, now = Date.now() }) {
  const count = Number(retryCount || 0);
  if (!Number.isFinite(count) || count <= 0) return null;
  if (count >= MAX_RETRY_COUNT) return null;
  const delay = RETRY_DELAY_MS[count] || null;
  if (!delay) return null;
  return Number(now) + delay;
}

module.exports = {
  MAX_RETRY_COUNT,
  isRetryableStatus,
  computeNextRetryAt
};
