const crypto = require('node:crypto');
const { pool } = require('../db/pool');
const { withTransaction } = require('../db/withTransaction');
const { USE_DB_MESSAGES, USE_DB_CONVERSATIONS } = require('../config/runtime');
const { loadData, getConversationById } = require('../store/dataStore');
const { verifyParity, stableNormalize } = require('./migrationParityService');
const {
  listByConversation,
  insertIdempotent,
  updateStatusById,
  deleteById
} = require('../repositories/messagesRepo');
const {
  createIfMissing,
  getByConvoKey,
  updateByConvoKey
} = require('../repositories/conversationsRepo');
const { enforceOutboundCompliance, newMessage, processInboundCompliance } = require('./complianceService');
const { twilioReadyForSend, sendTwilioMessageForTenant, buildTwilioStatusCallbackUrl } = require('./twilioIntegrationService');
const { MAX_RETRY_COUNT, computeNextRetryAt } = require('./messageRetryPolicy');
const {
  MESSAGE_STATUS,
  canTransitionMessageStatus,
  blockedTransitionFields
} = require('./messageStatusPolicy');
const {
  buildMessagePayloadProjection,
  buildConversationPayloadProjection
} = require('./messagingPayloadService');

let messageTransportOverride = null;

function logMessagingCore(level, fields = {}) {
  const line = {
    level,
    entity: String(fields.entity || 'message'),
    service: String(fields.service || 'messagesService'),
    operation: String(fields.operation || 'unknown'),
    accountId: fields.accountId ? String(fields.accountId) : null,
    conversationId: fields.conversationId ? String(fields.conversationId) : null,
    messageId: fields.messageId ? String(fields.messageId) : null,
    route: fields.route ? String(fields.route) : null,
    requestId: fields.requestId ? String(fields.requestId) : null,
    errorType: fields.errorType ? String(fields.errorType) : null,
    message: fields.message ? String(fields.message) : null
  };
  const fn = level === 'error' ? console.error : console.warn;
  fn(JSON.stringify(line));
}

function buildConvoKey(to, from) {
  return `${String(to || '')}__${String(from || '')}`;
}

function setMessageTransportForTests(fn) {
  messageTransportOverride = typeof fn === 'function' ? fn : null;
}

function resetMessageTransportForTests() {
  messageTransportOverride = null;
}

function getMessageTransport() {
  return messageTransportOverride || sendTwilioMessageForTenant;
}

function extractFailureCode(error) {
  const code = error?.code ?? error?.status ?? error?.errorCode ?? '';
  return String(code || '').trim() || 'transport_failed';
}

function extractFailureReason(error) {
  return String(error?.message || error?.errorMessage || 'Outbound provider send failed').trim();
}

function buildProviderPayload(message, providerMeta = {}) {
  const payload = buildMessagePayloadProjection(message);
  const nextMeta = {
    ...(payload.meta && typeof payload.meta === 'object' ? payload.meta : {}),
    provider: providerMeta.provider || null,
    providerMessageId: providerMeta.sid || message?.providerMessageId || '',
    deliveryStatus: providerMeta.deliveryStatus || message?.status || '',
    errorCode: providerMeta.errorCode || message?.failureCode || '',
    errorMessage: providerMeta.errorMessage || message?.failureReason || ''
  };
  payload.meta = Object.fromEntries(Object.entries(nextMeta).filter(([, value]) => value !== null && value !== ''));
  return payload;
}

function logBlockedStatusTransition(message, previousStatus, attemptedStatus, source) {
  const fields = blockedTransitionFields({ message, previousStatus, attemptedStatus, source });
  logMessagingCore('warn', {
    operation: 'blockedMessageStatusTransition',
    accountId: message?.accountId || message?.tenantId || null,
    conversationId: message?.conversationId || null,
    messageId: message?.id || null,
    errorType: 'message_status_regression_blocked',
    message: JSON.stringify(fields)
  });
}

async function sendExistingOutboundMessage({
  tenant,
  accountId,
  message,
  route = null,
  requestId = null,
  maxRetryCount = MAX_RETRY_COUNT
}) {
  const transport = getMessageTransport();
  const transportInjected = transport !== sendTwilioMessageForTenant;
  const destination = String(message?.from || '').trim();
  const body = String(message?.body || message?.text || '').trim();
  const now = Date.now();

  if (!destination || !body) {
    const err = new Error('retry_message_invalid');
    err.code = 'retry_message_invalid';
    throw err;
  }

  if (!transportInjected) {
    const ready = twilioReadyForSend(tenant);
    if (!ready.ok) {
      const err = new Error(String(ready.reason || 'Twilio not configured'));
      err.code = 'TWILIO_NOT_CONFIGURED';
      throw err;
    }
  }

  try {
    if (!canTransitionMessageStatus(message?.status, MESSAGE_STATUS.SENDING) && String(message?.status || '') !== MESSAGE_STATUS.SENDING) {
      logBlockedStatusTransition(message, message?.status, MESSAGE_STATUS.SENDING, route || 'send');
      return message;
    }
    await updateStatusById(pool, accountId, message.id, {
      status: MESSAGE_STATUS.SENDING,
      updatedAt: now,
      lastStatusEventAt: now,
      payload: buildMessagePayloadProjection(message)
    });
    const sent = await transport(tenant, {
      to: destination,
      body,
      statusCallbackUrl: buildTwilioStatusCallbackUrl(tenant)
    });
    if (!canTransitionMessageStatus(MESSAGE_STATUS.SENDING, MESSAGE_STATUS.SENT)) {
      logBlockedStatusTransition(message, MESSAGE_STATUS.SENDING, MESSAGE_STATUS.SENT, route || 'send');
      return message;
    }
    const updated = await updateStatusById(pool, accountId, message.id, {
      status: MESSAGE_STATUS.SENT,
      retryCount: Number(message?.retryCount || 0),
      lastAttemptAt: now,
      updatedAt: now,
      providerMessageId: String(sent?.sid || message?.providerMessageId || ''),
      failureCode: null,
      failureReason: null,
      nextRetryAt: null,
      to: message?.to,
      from: message?.from,
      sentAt: now,
      lastStatusEventAt: now,
      payload: buildProviderPayload({
        ...message,
        failureCode: '',
        failureReason: '',
        nextRetryAt: null,
        providerMessageId: String(sent?.sid || message?.providerMessageId || '')
      }, {
        provider: 'twilio',
        sid: String(sent?.sid || message?.providerMessageId || ''),
        deliveryStatus: String(sent?.status || 'sent'),
        to: String(sent?.to || ''),
        from: String(sent?.from || ''),
        messagingServiceSid: String(sent?.messagingServiceSid || '')
      })
    });
    return updated || { ...message, status: 'sent' };
  } catch (err) {
    const nextRetryCount = Number(message?.retryCount || 0) + 1;
    const nextRetryAt = computeNextRetryAt({ retryCount: nextRetryCount, now });
    const failureCode = extractFailureCode(err);
    const failureReason = extractFailureReason(err);
    const failureStatus = MESSAGE_STATUS.FAILED;
    if (!canTransitionMessageStatus(String(message?.status || MESSAGE_STATUS.SENDING), failureStatus)) {
      logBlockedStatusTransition(message, message?.status || MESSAGE_STATUS.SENDING, failureStatus, route || 'send');
      throw err;
    }
    const updated = await updateStatusById(pool, accountId, message.id, {
      status: failureStatus,
      retryCount: nextRetryCount,
      lastAttemptAt: now,
      updatedAt: now,
      providerMessageId: String(message?.providerMessageId || ''),
      failureCode,
      failureReason,
      nextRetryAt,
      to: message?.to,
      from: message?.from,
      failedAt: now,
      lastStatusEventAt: now,
      payload: buildProviderPayload({
        ...message,
        retryCount: nextRetryCount,
        failureCode,
        failureReason,
        nextRetryAt
      }, {
        provider: 'twilio',
        sid: String(message?.providerMessageId || ''),
        error: failureReason
      })
    });
    logMessagingCore('error', {
      operation: 'sendExistingOutboundMessage',
      accountId,
      conversationId: message?.conversationId || null,
      messageId: message?.id || null,
      route,
      requestId,
      errorType: nextRetryCount >= maxRetryCount ? 'retry_exhausted' : 'messaging_core_write_failed',
      message: failureReason
    });
    return updated || {
      ...message,
      status: 'failed',
      retryCount: nextRetryCount,
      failureCode,
      failureReason,
      nextRetryAt
    };
  }
}

function normalizeConversationForRead(conversation) {
  const convo = conversation && typeof conversation === 'object'
    ? JSON.parse(JSON.stringify(conversation))
    : null;
  if (!convo) return null;
  convo.id = String(convo?.convoKey || convo?.id || '');
  convo.messages = (Array.isArray(convo?.messages) ? convo.messages : []).sort((a, b) => {
    const left = Number(a?.ts || a?.createdAt || 0);
    const right = Number(b?.ts || b?.createdAt || 0);
    if (left !== right) return left - right;
    return String(a?.id || '').localeCompare(String(b?.id || ''));
  });
  return convo;
}

function normalizeConversationParity(value) {
  const list = Array.isArray(value) ? value : [value];
  return stableNormalize(list.filter(Boolean).map((conversation) => ({
    id: String(conversation?.id || ''),
    to: String(conversation?.to || ''),
    from: String(conversation?.from || ''),
    status: String(conversation?.status || ''),
    stage: String(conversation?.stage || ''),
    lastActivityAt: Number(conversation?.lastActivityAt || 0),
    flow: conversation?.flow || null,
    audit: Array.isArray(conversation?.audit) ? conversation.audit : [],
    leadData: conversation?.leadData || {},
    messages: (Array.isArray(conversation?.messages) ? conversation.messages : []).map((message) => ({
      id: String(message?.id || ''),
      dir: String(message?.dir || ''),
      text: String(message?.text || message?.body || ''),
      status: String(message?.status || ''),
      ts: Number(message?.ts || message?.createdAt || 0)
    }))
  })));
}

async function ensureConversationRecord(db, accountId, to, from, patch = {}) {
  const convoKey = buildConvoKey(to, from);
  const existing = await getByConvoKey(db, accountId, convoKey);
  if (existing) return existing;
    return createIfMissing(db, accountId, {
    convoKey,
    to,
    from,
    status: patch?.status || 'new',
    stage: patch?.stage || 'ask_service',
    createdAt: patch?.createdAt || Date.now(),
      updatedAt: patch?.updatedAt || Date.now(),
      lastActivityAt: patch?.lastActivityAt || null,
      flow: patch?.flow || null,
      audit: Array.isArray(patch?.audit) ? patch.audit : [],
      leadData: patch?.leadData || {},
      fields: patch?.fields || {},
      bookingTime: patch?.bookingTime || null,
      bookingEndTime: patch?.bookingEndTime || null,
      amount: patch?.amount ?? null,
      paymentStatus: patch?.paymentStatus || null,
      closedAt: patch?.closedAt || null,
      payload: buildConversationPayloadProjection({
        flow: patch?.flow || {
        flowId: null,
        ruleId: null,
        stepId: null,
        status: 'idle',
        startedAt: null,
        updatedAt: null,
        lastAutoSentAt: null,
        lockUntil: null
      },
      fields: patch?.fields || {},
      audit: Array.isArray(patch?.audit) ? patch.audit : [],
      leadData: patch?.leadData || {}
    })
  });
}

async function loadConversationForDb(accountId, convoKey, meta = {}) {
  return getByConvoKey(meta?.db || pool, accountId, convoKey);
}

async function loadConversationPreferred(accountId, convoKey, meta = {}) {
  const oldFactory = async () => normalizeConversationForRead(getConversationById(accountId, convoKey)?.conversation || null);
  const newFactory = async () => normalizeConversationForRead(await loadConversationForDb(accountId, convoKey, meta));
  if (USE_DB_CONVERSATIONS || USE_DB_MESSAGES) {
    return newFactory();
  }
  return oldFactory();
}

function deriveIdempotencyKey({ eventKey = null, messageSid = null, meta = {} } = {}) {
  if (eventKey) return String(eventKey);
  if (messageSid) return `sms:${String(messageSid)}`;
  if (meta?.idempotencyKey) return String(meta.idempotencyKey);
  if (meta?.outboundCorrelationId) return String(meta.outboundCorrelationId);
  if (meta?.actionId) return `action:${String(meta.actionId)}`;
  if (meta?.runId && meta?.stepId) return `run:${String(meta.runId)}:${String(meta.stepId)}`;
  if (meta?.correlationId) return `corr:${String(meta.correlationId)}`;
  return null;
}

async function appendOutboundMessage({
  tenant,
  to,
  from,
  text,
  source,
  consentConfirmed = false,
  consentSource = null,
  transactional = false,
  bypassCompliance = false,
  meta = {},
  route = null,
  requestId = null,
  requireExisting = true,
  afterSuccess = null,
  waitForTransport = false
}) {
  const accountId = String(tenant?.accountId || '');
  const convoKey = buildConvoKey(to, from);
  const txResult = await withTransaction(pool, async (db) => {
    let conversation = await getByConvoKey(db, accountId, convoKey);
    if (!conversation && requireExisting) {
      return { conversation: null, sendResult: { ok: false, error: { code: 'NOT_FOUND', message: 'Conversation not found' } } };
    }
    conversation = conversation || await ensureConversationRecord(db, accountId, to, from);

    const data = loadData();
    const enforcement = bypassCompliance
      ? { ok: true }
      : enforceOutboundCompliance({
        data,
        tenant,
        conversation,
        from: String(from || conversation?.from || ''),
        consentConfirmed,
        consentSource,
        transactional
      });
    if (!enforcement.ok) {
      return { conversation: normalizeConversationForRead(conversation), sendResult: enforcement };
    }

    const simulateOutbound = require('../store/dataStore').getDevSettings()?.simulateOutbound === true;
    if (!simulateOutbound && getMessageTransport() === sendTwilioMessageForTenant) {
      const ready = twilioReadyForSend(tenant);
      if (!ready.ok) {
        return {
          conversation: normalizeConversationForRead(conversation),
          sendResult: { ok: false, error: { code: 'TWILIO_NOT_CONFIGURED', message: ready.reason } }
        };
      }
    }

    const now = Date.now();
    const idempotencyKey = deriveIdempotencyKey({ meta });
    const message = newMessage('outbound', String(to), String(from), String(text || ''), {
      ...meta,
      status: simulateOutbound ? MESSAGE_STATUS.SIMULATED : String(meta?.status || MESSAGE_STATUS.QUEUED),
      idempotencyKey,
      retryCount: 0,
      lastAttemptAt: now
    });
    const persisted = await insertIdempotent(db, accountId, convoKey, {
      id: message.id,
      direction: 'outbound',
      body: message.body,
      status: message.status,
      idempotencyKey,
      retryCount: 0,
      lastAttemptAt: now,
      updatedAt: now,
      createdAt: now,
      queuedAt: now,
      lastStatusEventAt: now,
      payload: buildMessagePayloadProjection({
        meta: {
          ...(message?.meta && typeof message.meta === 'object' ? message.meta : {}),
          source: String(source || '')
        }
      }),
      to: String(to),
      from: String(from)
    });

    const nextConversation = {
      ...conversation,
      lastActivityAt: now,
      updatedAt: now,
      accountId,
      messages: [...(Array.isArray(conversation.messages) ? conversation.messages : []), persisted]
    };
    if (typeof afterSuccess === 'function') {
      afterSuccess(nextConversation, persisted);
    }
    await updateByConvoKey(db, accountId, convoKey, {
      to,
      from,
      status: nextConversation.status,
      stage: nextConversation.stage,
      flow: nextConversation.flow,
      audit: nextConversation.audit,
      leadData: nextConversation.leadData,
      fields: nextConversation.fields,
      bookingTime: nextConversation.bookingTime,
      bookingEndTime: nextConversation.bookingEndTime,
      amount: nextConversation.amount,
      paymentStatus: nextConversation.paymentStatus,
      closedAt: nextConversation.closedAt,
      lastActivityAt: nextConversation.lastActivityAt,
      updatedAt: nextConversation.updatedAt,
      payload: buildConversationPayloadProjection(nextConversation)
    });

    try {
      const { createLeadEvent, upsertRevenueOpportunityFromEvent, evaluateOpportunity } = require('./revenueIntelligenceService');
      const evt = createLeadEvent(accountId, {
        convoKey,
        contactId: null,
        channel: 'sms',
        type: 'outbound_message',
        payload: {
          source: String(source || 'outbound'),
          text: String(text || ''),
          transactional: transactional === true,
          auto: meta?.auto === true
        }
      });
      const opp = upsertRevenueOpportunityFromEvent(accountId, evt);
      if (opp?.id) evaluateOpportunity(accountId, opp.id);
    } catch {}

    return {
      conversation: normalizeConversationForRead(await getByConvoKey(db, accountId, convoKey)),
      sendResult: { ok: true, message: persisted },
      persisted,
      simulateOutbound
    };
  });

  if (!txResult?.sendResult?.ok) {
    return txResult;
  }

  if (!txResult.simulateOutbound) {
    const sendPromise = sendExistingOutboundMessage({
      tenant,
      accountId,
      message: txResult.persisted,
      route,
      requestId
    });
    if (waitForTransport) {
      await sendPromise;
    } else {
      void sendPromise;
    }
  }

  return {
    conversation: txResult.conversation,
    sendResult: txResult.sendResult
  };
}

async function recordInboundSms({
  tenant,
  to,
  from,
  text,
  mediaCount = 0,
  eventKey = null
}) {
  const accountId = String(tenant?.accountId || '');
  const convoKey = buildConvoKey(to, from);
  return withTransaction(pool, async (db) => {
    let conversation = await ensureConversationRecord(db, accountId, to, from);
    if (String(conversation?.status || '').toLowerCase() === 'closed') {
      conversation = {
        ...conversation,
        status: 'new',
        stage: 'ask_service',
        bookingTime: null,
        bookingEndTime: null,
        closedAt: null,
        fields: {},
        leadData: {},
        flow: {
          flowId: null,
          ruleId: null,
          stepId: null,
          status: 'idle',
          startedAt: null,
          updatedAt: null,
          lastAutoSentAt: null,
          lockUntil: null
        },
        audit: [{
          ts: Date.now(),
          type: 'lifecycle_reset',
          meta: { source: 'inbound_sms_after_closed' }
        }]
      };
    }

    const now = Date.now();
    const idempotencyKey = deriveIdempotencyKey({ eventKey });
    const message = newMessage('inbound', String(to), String(from), String(text || ''), {
      status: 'received',
      mediaCount: Number(mediaCount || 0),
      idempotencyKey,
      meta: {
        mediaCount: Number(mediaCount || 0),
        idempotencyKey
      }
    });
    const persisted = await insertIdempotent(db, accountId, convoKey, {
      id: message.id,
      direction: 'inbound',
      body: message.body,
      status: 'received',
      idempotencyKey,
      retryCount: 0,
      lastAttemptAt: now,
      updatedAt: now,
      createdAt: now,
      lastStatusEventAt: now,
      payload: buildMessagePayloadProjection({
        meta: message?.meta
      }),
      to: String(to),
      from: String(from)
    });

    const updatedConversation = {
      ...conversation,
      lastActivityAt: now,
      updatedAt: now,
      accountId,
      messages: [...(Array.isArray(conversation.messages) ? conversation.messages : []), persisted]
    };
    const compliance = processInboundCompliance({
      data: loadData(),
      tenant,
      conversation: updatedConversation,
      from: String(from),
      text: String(text || ''),
      sendMessageFn: async (payload) => appendOutboundMessage({
        tenant,
        to,
        from,
        text: payload.text,
        source: payload.source,
        bypassCompliance: true,
        meta: payload.meta || {},
        requireExisting: true
      })
    });
    await updateByConvoKey(db, accountId, convoKey, {
      to,
      from,
      status: updatedConversation.status,
      stage: updatedConversation.stage,
      flow: updatedConversation.flow,
      audit: updatedConversation.audit,
      leadData: updatedConversation.leadData,
      fields: updatedConversation.fields,
      bookingTime: updatedConversation.bookingTime,
      bookingEndTime: updatedConversation.bookingEndTime,
      amount: updatedConversation.amount,
      paymentStatus: updatedConversation.paymentStatus,
      closedAt: updatedConversation.closedAt,
      lastActivityAt: updatedConversation.lastActivityAt,
      updatedAt: updatedConversation.updatedAt,
      payload: buildConversationPayloadProjection(updatedConversation)
    });
    return {
      conversation: normalizeConversationForRead(await getByConvoKey(db, accountId, convoKey)),
      compliance,
      duplicateBlocked: Boolean(idempotencyKey && persisted.id !== message.id)
    };
  });
}

async function deleteMessageById({ tenant, to, from, messageId }) {
  const accountId = String(tenant?.accountId || '');
  const convoKey = buildConvoKey(to, from);
  return withTransaction(pool, async (db) => {
    const conversation = await getByConvoKey(db, accountId, convoKey);
    if (!conversation) return null;
    const deleted = await deleteById(db, accountId, convoKey, messageId);
    if (!deleted) {
      const err = new Error('Message not found');
      err.status = 404;
      throw err;
    }
    const updatedMessages = await listByConversation(db, accountId, convoKey);
    const nextConversation = {
      ...conversation,
      messages: updatedMessages,
      lastActivityAt: Date.now(),
      updatedAt: Date.now(),
      audit: [...(Array.isArray(conversation.audit) ? conversation.audit : []), {
        ts: Date.now(),
        type: 'message_deleted',
        meta: { messageId: String(messageId), source: 'dashboard' }
      }]
    };
    await updateByConvoKey(db, accountId, convoKey, {
      status: nextConversation.status,
      stage: nextConversation.stage,
      flow: nextConversation.flow,
      audit: nextConversation.audit,
      leadData: nextConversation.leadData,
      fields: nextConversation.fields,
      bookingTime: nextConversation.bookingTime,
      bookingEndTime: nextConversation.bookingEndTime,
      amount: nextConversation.amount,
      paymentStatus: nextConversation.paymentStatus,
      closedAt: nextConversation.closedAt,
      lastActivityAt: nextConversation.lastActivityAt,
      updatedAt: nextConversation.updatedAt,
      payload: buildConversationPayloadProjection(nextConversation)
    });
    return normalizeConversationForRead(await getByConvoKey(db, accountId, convoKey));
  });
}

async function deleteMessageByIndex({ tenant, to, from, index }) {
  const accountId = String(tenant?.accountId || '');
  const convoKey = buildConvoKey(to, from);
  const messages = await listByConversation(pool, accountId, convoKey);
  if (!Number.isFinite(Number(index)) || Number(index) < 0 || Number(index) >= messages.length) {
    const err = new Error('Message index out of range');
    err.status = 400;
    throw err;
  }
  return deleteMessageById({
    tenant,
    to,
    from,
    messageId: messages[Number(index)].id
  });
}

async function reconcileSnapshotMessagesToDb({ accountId = null, logger = console } = {}) {
  const data = loadData();
  const conversations = Object.entries(data.conversations || {})
    .filter(([, conversation]) => !accountId || String(conversation?.accountId || '') === String(accountId))
    .sort(([left], [right]) => String(left).localeCompare(String(right)));
  for (const [convoKey, conversation] of conversations) {
    const currentAccountId = String(conversation?.accountId || '');
    await withTransaction(pool, async (db) => {
      await ensureConversationRecord(db, currentAccountId, conversation?.to, conversation?.from, conversation);
      const seenFingerprints = new Set();
      const list = Array.isArray(conversation?.messages) ? conversation.messages : [];
      for (let i = 0; i < list.length; i += 1) {
        const item = list[i] || {};
        const fingerprint = crypto
          .createHash('sha1')
          .update(JSON.stringify([
            currentAccountId,
            convoKey,
            Number(item?.ts || 0),
            String(item?.dir || item?.direction || ''),
            String(item?.text || item?.body || ''),
            i
          ]))
          .digest('hex');
        if (seenFingerprints.has(fingerprint)) {
          logger.warn?.(`[messaging-backfill] collapsed duplicate message fingerprint for ${convoKey}`);
          continue;
        }
        seenFingerprints.add(fingerprint);
        const deterministicId = String(item?.id || `msg_${String(i).padStart(6, '0')}_${fingerprint.slice(0, 18)}`);
        await insertIdempotent(db, currentAccountId, convoKey, {
          id: deterministicId,
          direction: String(item?.direction || (String(item?.dir || '') === 'in' ? 'inbound' : 'outbound')),
          body: String(item?.body || item?.text || ''),
          status: String(item?.status || 'sent'),
          idempotencyKey: item?.idempotencyKey || item?.meta?.idempotencyKey || null,
          retryCount: Number(item?.retryCount || 0),
          lastAttemptAt: item?.lastAttemptAt || null,
          updatedAt: item?.updatedAt || item?.ts || Date.now(),
          createdAt: item?.createdAt || item?.ts || Date.now(),
          to: item?.to || conversation?.to,
          from: item?.from || conversation?.from,
          payload: buildMessagePayloadProjection({
            source: item?.source || item?.meta?.source || null,
            meta: item?.meta,
            providerMeta: item?.providerMeta,
            attachments: item?.attachments
          })
        });
      }
    });
  }
}

module.exports = {
  buildConvoKey,
  normalizeConversationForRead,
  loadConversationPreferred,
  ensureConversationRecord,
  deriveIdempotencyKey,
  listByConversation,
  insertIdempotent,
  updateStatusById,
  deleteById,
  pool,
  withTransaction,
  loadData,
  enforceOutboundCompliance,
  newMessage,
  processInboundCompliance,
  twilioReadyForSend,
  sendTwilioMessageForTenant,
  updateByConvoKey,
  getByConvoKey,
  createIfMissing,
  logMessagingCore,
  sendExistingOutboundMessage,
  setMessageTransportForTests,
  resetMessageTransportForTests,
  appendOutboundMessage,
  recordInboundSms,
  deleteMessageById,
  deleteMessageByIndex,
  reconcileSnapshotMessagesToDb
};
