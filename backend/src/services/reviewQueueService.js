const { loadData, saveDataDebounced } = require('../store/dataStore');
const { generateId } = require('../utils/id');

function listReviewQueue(accountId, { status = '', limit = 100 } = {}) {
  const data = loadData();
  const n = Math.max(1, Math.min(500, Number(limit || 100)));
  return (data.reviewQueue || [])
    .filter((item) => String(item?.accountId || '') === String(accountId || ''))
    .filter((item) => !status || String(item?.status || '') === String(status))
    .sort((a, b) => Number(b?.createdAt || 0) - Number(a?.createdAt || 0))
    .slice(0, n);
}

function createReviewItem(accountId, input = {}) {
  const data = loadData();
  data.reviewQueue = Array.isArray(data.reviewQueue) ? data.reviewQueue : [];
  const item = {
    id: generateId(),
    accountId: String(accountId || ''),
    runId: input.runId ? String(input.runId) : null,
    opportunityId: input.opportunityId ? String(input.opportunityId) : null,
    createdAt: Number(input.createdAt || Date.now()),
    stepId: String(input.stepId || ''),
    proposedActionPayload: input.proposedActionPayload && typeof input.proposedActionPayload === 'object' ? input.proposedActionPayload : {},
    requiredByTs: input.requiredByTs ? Number(input.requiredByTs) : null,
    status: 'PENDING',
    resolvedByUserId: null,
    resolvedAt: null,
    resolutionNotes: null
  };
  data.reviewQueue.push(item);
  saveDataDebounced(data);
  return item;
}

function resolveReviewItem(accountId, reviewId, decision, { userId = null, notes = null } = {}) {
  const data = loadData();
  const item = (data.reviewQueue || []).find((x) =>
    String(x?.accountId || '') === String(accountId || '') && String(x?.id || '') === String(reviewId || '')
  );
  if (!item) return null;
  if (String(item.status || '') !== 'PENDING') return item;
  const upper = String(decision || '').toUpperCase();
  if (!['APPROVED', 'REJECTED', 'EXPIRED'].includes(upper)) return null;
  item.status = upper;
  item.resolvedAt = Date.now();
  item.resolvedByUserId = userId ? String(userId) : null;
  item.resolutionNotes = notes ? String(notes) : null;
  saveDataDebounced(data);
  return item;
}

module.exports = {
  listReviewQueue,
  createReviewItem,
  resolveReviewItem
};
