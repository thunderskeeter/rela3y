const MESSAGE_STATUS = Object.freeze({
  QUEUED: 'queued',
  SENDING: 'sending',
  SENT: 'sent',
  DELIVERED: 'delivered',
  FAILED: 'failed',
  UNDELIVERED: 'undelivered',
  RECEIVED: 'received',
  SIMULATED: 'simulated'
});

const ALLOWED_TRANSITIONS = Object.freeze({
  [MESSAGE_STATUS.QUEUED]: new Set([MESSAGE_STATUS.SENDING, MESSAGE_STATUS.SENT, MESSAGE_STATUS.FAILED, MESSAGE_STATUS.UNDELIVERED]),
  [MESSAGE_STATUS.SENDING]: new Set([MESSAGE_STATUS.SENT, MESSAGE_STATUS.DELIVERED, MESSAGE_STATUS.FAILED, MESSAGE_STATUS.UNDELIVERED]),
  [MESSAGE_STATUS.SENT]: new Set([MESSAGE_STATUS.DELIVERED, MESSAGE_STATUS.FAILED, MESSAGE_STATUS.UNDELIVERED]),
  [MESSAGE_STATUS.FAILED]: new Set([MESSAGE_STATUS.SENDING]),
  [MESSAGE_STATUS.UNDELIVERED]: new Set([MESSAGE_STATUS.SENDING]),
  [MESSAGE_STATUS.DELIVERED]: new Set([]),
  [MESSAGE_STATUS.RECEIVED]: new Set([]),
  [MESSAGE_STATUS.SIMULATED]: new Set([])
});

function normalizeMessageStatus(value) {
  const status = String(value || '').trim().toLowerCase();
  return Object.values(MESSAGE_STATUS).includes(status) ? status : '';
}

function normalizeTwilioStatus(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  if (raw === 'queued' || raw === 'accepted' || raw === 'sending') return MESSAGE_STATUS.SENDING;
  if (raw === 'sent') return MESSAGE_STATUS.SENT;
  if (raw === 'delivered') return MESSAGE_STATUS.DELIVERED;
  if (raw === 'undelivered') return MESSAGE_STATUS.UNDELIVERED;
  if (raw === 'failed') return MESSAGE_STATUS.FAILED;
  return normalizeMessageStatus(raw);
}

function canTransitionMessageStatus(current, next) {
  const from = normalizeMessageStatus(current);
  const to = normalizeMessageStatus(next);
  if (!to || !from || from === to) return Boolean(to);
  return Boolean(ALLOWED_TRANSITIONS[from] && ALLOWED_TRANSITIONS[from].has(to));
}

function isRetryableStatus(status) {
  const normalized = normalizeMessageStatus(status);
  return normalized === MESSAGE_STATUS.FAILED || normalized === MESSAGE_STATUS.UNDELIVERED;
}

function blockedTransitionFields({ message, previousStatus, attemptedStatus, source }) {
  return {
    messageId: message?.id ? String(message.id) : null,
    previousStatus: normalizeMessageStatus(previousStatus),
    attemptedStatus: normalizeMessageStatus(attemptedStatus),
    source: String(source || 'unknown')
  };
}

module.exports = {
  MESSAGE_STATUS,
  normalizeMessageStatus,
  normalizeTwilioStatus,
  canTransitionMessageStatus,
  isRetryableStatus,
  blockedTransitionFields
};
