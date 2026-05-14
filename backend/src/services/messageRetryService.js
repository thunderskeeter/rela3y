const { pool } = require('../db/pool');
const { withTransaction } = require('../db/withTransaction');
const { claimRetryableMessages } = require('../repositories/messagesRepo');
const { sendExistingOutboundMessage } = require('./messagesService');
const { MAX_RETRY_COUNT } = require('./messageRetryPolicy');
const { isRetryableStatus, blockedTransitionFields } = require('./messageStatusPolicy');

function logRetry(level, fields = {}) {
  const line = {
    level,
    entity: 'message',
    service: 'messageRetryService',
    operation: String(fields.operation || 'runRetryBatch'),
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

function tenantFromMessage(message) {
  const convoKey = String(message?.convoKey || message?.conversationId || '');
  return {
    accountId: String(message?.accountId || message?.tenantId || ''),
    to: String(message?.to || convoKey.split('__')[0] || '')
  };
}

async function claimRetryBatch({ limit = 25, now = Date.now(), maxRetryCount = MAX_RETRY_COUNT } = {}) {
  return withTransaction(pool, async (db) => {
    return claimRetryableMessages(db, { limit, now, maxRetryCount });
  });
}

async function retryMessage(messageRow, { route = 'messageRetryService', requestId = null, maxRetryCount = MAX_RETRY_COUNT } = {}) {
  const tenant = tenantFromMessage(messageRow);
  if (!isRetryableStatus(messageRow?.status) && String(messageRow?.status || '') !== 'sending') {
    logRetry('warn', {
      operation: 'retryMessage',
      accountId: tenant.accountId,
      conversationId: messageRow?.conversationId,
      messageId: messageRow?.id,
      providerMessageId: messageRow?.providerMessageId,
      errorType: 'message_status_regression_blocked',
      message: JSON.stringify(blockedTransitionFields({
        message: messageRow,
        previousStatus: messageRow?.status,
        attemptedStatus: 'sending',
        source: 'retry'
      }))
    });
    return messageRow;
  }
  logRetry('warn', {
    operation: 'retryMessage',
    accountId: tenant.accountId,
    conversationId: messageRow?.conversationId,
    messageId: messageRow?.id,
    providerMessageId: messageRow?.providerMessageId,
    errorType: 'retry_attempted',
    message: 'Retrying outbound message.'
  });

  const updated = await sendExistingOutboundMessage({
    tenant,
    accountId: tenant.accountId,
    message: messageRow,
    route,
    requestId,
    maxRetryCount
  });

  if (String(updated?.status || '').toLowerCase() === 'sent') {
    logRetry('warn', {
      operation: 'retryMessage',
      accountId: tenant.accountId,
      conversationId: updated?.conversationId,
      messageId: updated?.id,
      providerMessageId: updated?.providerMessageId,
      errorType: 'retry_succeeded',
      message: 'Retry succeeded.'
    });
  } else if (updated?.nextRetryAt) {
    logRetry('warn', {
      operation: 'retryMessage',
      accountId: tenant.accountId,
      conversationId: updated?.conversationId,
      messageId: updated?.id,
      providerMessageId: updated?.providerMessageId,
      errorType: 'retry_failed',
      message: 'Retry failed and was rescheduled.'
    });
  } else {
    logRetry('error', {
      operation: 'retryMessage',
      accountId: tenant.accountId,
      conversationId: updated?.conversationId,
      messageId: updated?.id,
      providerMessageId: updated?.providerMessageId,
      errorType: 'retry_exhausted',
      message: 'Retry failed and exhausted retry cap.'
    });
  }
  return updated;
}

async function runRetryBatch({ limit = 25, now = Date.now(), maxRetryCount = MAX_RETRY_COUNT } = {}) {
  const claimed = await claimRetryBatch({ limit, now, maxRetryCount });
  const results = [];
  for (const message of claimed) {
    results.push(await retryMessage(message, { maxRetryCount }));
  }
  return { claimedCount: claimed.length, results };
}

module.exports = {
  claimRetryBatch,
  retryMessage,
  runRetryBatch
};
