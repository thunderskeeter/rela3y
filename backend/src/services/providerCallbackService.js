const { pool } = require('../db/pool');
const { claimWebhookEvent } = require('./webhookIdempotencyService');
const { getByProviderMessageId, updateStatusById } = require('../repositories/messagesRepo');
const { computeNextRetryAt, MAX_RETRY_COUNT } = require('./messageRetryPolicy');
const { buildMessagePayloadProjection } = require('./messagingPayloadService');
const {
  normalizeTwilioStatus,
  canTransitionMessageStatus,
  blockedTransitionFields,
  isRetryableStatus,
  MESSAGE_STATUS
} = require('./messageStatusPolicy');

function logProviderCallback(level, fields = {}) {
  const line = {
    level,
    entity: 'message',
    service: 'providerCallbackService',
    operation: String(fields.operation || 'processTwilioStatusCallback'),
    accountId: fields.accountId ? String(fields.accountId) : null,
    conversationId: fields.conversationId ? String(fields.conversationId) : null,
    messageId: fields.messageId ? String(fields.messageId) : null,
    providerMessageId: fields.providerMessageId ? String(fields.providerMessageId) : null,
    errorType: fields.errorType ? String(fields.errorType) : null,
    message: fields.message ? String(fields.message) : null
  };
  const fn = level === 'error' ? console.error : console.warn;
  fn(JSON.stringify(line));
}

async function processTwilioStatusCallback({
  tenant,
  providerMessageId,
  providerStatus,
  errorCode = '',
  errorMessage = '',
  eventId = '',
  rawPayload = {}
}) {
  const accountId = String(tenant?.accountId || '').trim();
  const sid = String(providerMessageId || '').trim();
  const normalizedStatus = normalizeTwilioStatus(providerStatus);
  if (!accountId || !sid || !normalizedStatus) {
    return { ok: false, reason: 'missing_callback_fields' };
  }

  const dedupeKey = String(eventId || `${sid}:${normalizedStatus}:${String(errorCode || '').trim() || 'none'}`);
  const dedupe = await claimWebhookEvent(accountId, 'twilio_status', dedupeKey);
  if (dedupe.duplicate === true) {
    logProviderCallback('warn', {
      accountId,
      providerMessageId: sid,
      errorType: 'provider_callback_duplicate',
      message: 'Duplicate Twilio status callback ignored.'
    });
    return { ok: true, duplicate: true };
  }

  const message = await getByProviderMessageId(pool, accountId, sid);
  if (!message) {
    logProviderCallback('warn', {
      accountId,
      providerMessageId: sid,
      errorType: 'provider_callback_unmatched',
      message: 'Twilio callback could not be matched to a message row.'
    });
    return { ok: true, unmatched: true };
  }

  if (!canTransitionMessageStatus(message.status, normalizedStatus)) {
    const transition = blockedTransitionFields({
      message,
      previousStatus: message.status,
      attemptedStatus: normalizedStatus,
      source: 'callback'
    });
    logProviderCallback('warn', {
      accountId,
      conversationId: message.conversationId,
      messageId: message.id,
      providerMessageId: sid,
      errorType: 'message_status_regression_blocked',
      message: JSON.stringify(transition)
    });
    return { ok: true, blocked: true, message };
  }

  const now = Date.now();
  const payload = {
    ...buildMessagePayloadProjection(message),
    failureCode: String(errorCode || '').trim(),
    failureReason: String(errorMessage || '').trim(),
    nextRetryAt: null,
    providerMessageId: sid,
    providerMeta: {
      ...(message?.providerMeta && typeof message.providerMeta === 'object' ? message.providerMeta : {}),
      provider: 'twilio',
      sid,
      deliveryStatus: normalizedStatus,
      errorCode: String(errorCode || '').trim(),
      errorMessage: String(errorMessage || '').trim(),
      lastCallbackAt: now,
      lastCallbackPayload: rawPayload && typeof rawPayload === 'object' ? rawPayload : {}
    }
  };

  const currentRetryCount = Number(message?.retryCount || 0);
  const nextRetryCount = isRetryableStatus(normalizedStatus)
    ? Math.min(MAX_RETRY_COUNT, currentRetryCount + 1)
    : currentRetryCount;
  const nextRetryAt = isRetryableStatus(normalizedStatus)
    ? computeNextRetryAt({ retryCount: nextRetryCount, now })
    : null;
  payload.failureCode = isRetryableStatus(normalizedStatus) ? String(errorCode || '').trim() : '';
  payload.failureReason = isRetryableStatus(normalizedStatus) ? String(errorMessage || normalizedStatus).trim() : '';
  payload.nextRetryAt = nextRetryAt;

  const updated = await updateStatusById(pool, accountId, message.id, {
    status: normalizedStatus,
    retryCount: nextRetryCount,
    updatedAt: now,
    providerMessageId: sid,
    failureCode: isRetryableStatus(normalizedStatus) ? String(errorCode || '').trim() : null,
    failureReason: isRetryableStatus(normalizedStatus) ? String(errorMessage || normalizedStatus).trim() : null,
    nextRetryAt,
    deliveredAt: normalizedStatus === MESSAGE_STATUS.DELIVERED ? now : null,
    failedAt: isRetryableStatus(normalizedStatus) ? now : null,
    firstProviderCallbackAt: message?.firstProviderCallbackAt || now,
    lastStatusEventAt: now,
    payload
  });
  return { ok: true, message: updated || message };
}

module.exports = {
  processTwilioStatusCallback,
  normalizeTwilioStatus,
  isAllowedTransition: canTransitionMessageStatus
};
