const {
  updateConversation,
  getConversationById,
  loadData,
  flushDataNow
} = require('../store/dataStore');
const { USE_DB_CONVERSATIONS, USE_DB_MESSAGES } = require('../config/runtime');
const {
  attemptOutboundMessage,
  newMessage,
  processInboundCompliance
} = require('./complianceService');
const {
  appendOutboundMessage,
  recordInboundSms: recordInboundSmsDb,
  deleteMessageById,
  deleteMessageByIndex,
  reconcileSnapshotMessagesToDb
} = require('./messagesService');
const {
  recordMissedCall: recordMissedCallDb,
  recordBookingSync: recordBookingSyncDb,
  updateConversationStatus: updateConversationStatusDb,
  mutateFlowConversation,
  reconcileSnapshotConversationsToDb
} = require('./conversationsService');

function logBoundary(level, fields = {}) {
  const line = {
    level,
    entity: String(fields.entity || 'messaging_boundary'),
    service: 'messagingBoundaryService',
    operation: String(fields.operation || 'unknown'),
    accountId: fields.accountId ? String(fields.accountId) : null,
    convoKey: fields.convoKey ? String(fields.convoKey) : null,
    to: fields.to ? String(fields.to) : null,
    from: fields.from ? String(fields.from) : null,
    route: fields.route ? String(fields.route) : null,
    requestId: fields.requestId ? String(fields.requestId) : null,
    errorType: fields.errorType ? String(fields.errorType) : null,
    message: fields.message ? String(fields.message) : null
  };
  const fn = level === 'error' ? console.error : console.warn;
  fn(JSON.stringify(line));
}

function dbMessagingEnabled() {
  return USE_DB_CONVERSATIONS === true || USE_DB_MESSAGES === true;
}

function buildConvoKey(to, from) {
  return `${String(to || '')}__${String(from || '')}`;
}

function normalizeTenantInput(tenant, fallback = {}) {
  const accountId = String(tenant?.accountId || fallback.accountId || '').trim();
  const to = String(tenant?.to || fallback.to || '').trim();
  if (!accountId || !to) return null;
  return { accountId, to };
}

function getExistingConversation(accountId, to, from) {
  return getConversationById(String(accountId || ''), buildConvoKey(to, from))?.conversation || null;
}

function deriveBoundaryIdempotencyKey(input = {}) {
  if (input?.idempotencyKey) return String(input.idempotencyKey);
  if (input?.eventKey) return String(input.eventKey);
  if (input?.messageSid) return `sms:${String(input.messageSid)}`;
  if (input?.callSid) return `call:${String(input.callSid)}`;
  if (input?.bookingId) return `booking:${String(input.bookingId)}`;
  const meta = input?.meta && typeof input.meta === 'object' ? input.meta : {};
  if (meta.outboundCorrelationId) return String(meta.outboundCorrelationId);
  if (meta.actionId) return `action:${String(meta.actionId)}`;
  if (meta.runId && meta.stepId) return `run:${String(meta.runId)}:${String(meta.stepId)}`;
  if (meta.correlationId) return `corr:${String(meta.correlationId)}`;
  return null;
}

function findMessageByIdempotencyKey(conversation, idempotencyKey) {
  if (!idempotencyKey) return null;
  const messages = Array.isArray(conversation?.messages) ? conversation.messages : [];
  return messages.find((message) => {
    const meta = message?.meta && typeof message.meta === 'object' ? message.meta : {};
    return String(message?.idempotencyKey || meta.idempotencyKey || '') === String(idempotencyKey);
  }) || null;
}

function nextOrderingSequence(conversation) {
  conversation.messages = Array.isArray(conversation.messages) ? conversation.messages : [];
  let current = Number(conversation._messageOrderingSequence || 0);
  if (!current) {
    current = conversation.messages.reduce((max, message) => {
      const meta = message?.meta && typeof message.meta === 'object' ? message.meta : {};
      const seq = Number(meta.orderingSequence || 0);
      return seq > max ? seq : max;
    }, 0);
  }
  current += 1;
  conversation._messageOrderingSequence = current;
  return current;
}

function warnOnBackdatedMessage(conversation, message, context = {}) {
  const messages = Array.isArray(conversation?.messages) ? conversation.messages : [];
  if (!messages.length) return;
  const previous = messages[messages.length - 1];
  if (Number(previous?.ts || 0) <= Number(message?.ts || 0)) return;
  logBoundary('warn', {
    entity: 'message',
    operation: context.operation || 'append',
    accountId: conversation?.accountId || context.accountId || '',
    convoKey: buildConvoKey(conversation?.to || context.to || '', conversation?.from || context.from || ''),
    to: conversation?.to || context.to || '',
    from: conversation?.from || context.from || '',
    route: context.route,
    requestId: context.requestId,
    errorType: 'messaging_boundary_ordering_warning',
    message: 'Attempted to append a backdated message; preserving append order.'
  });
}

async function mutateConversationLegacy({
  tenant,
  to,
  from,
  operation,
  route = null,
  requestId = null,
  requireExisting = false,
  mutate
}) {
  const resolvedTenant = normalizeTenantInput(tenant, { to });
  if (!resolvedTenant) {
    logBoundary('warn', {
      operation,
      to,
      from,
      route,
      requestId,
      errorType: 'messaging_boundary_tenant_ambiguous',
      message: 'Missing tenant context for legacy messaging write.'
    });
    return null;
  }

  if (requireExisting && !getExistingConversation(resolvedTenant.accountId, resolvedTenant.to, from)) {
    return null;
  }

  try {
    return await updateConversation(String(resolvedTenant.to), String(from), async (conversation, data) => {
      if (conversation.accountId && String(conversation.accountId) !== String(resolvedTenant.accountId)) {
        const err = new Error('Conversation does not belong to tenant.');
        logBoundary('warn', {
          operation,
          entity: 'conversation',
          accountId: resolvedTenant.accountId,
          convoKey: buildConvoKey(resolvedTenant.to, from),
          to: resolvedTenant.to,
          from,
          route,
          requestId,
          errorType: 'messaging_boundary_tenant_ambiguous',
          message: err.message
        });
        throw err;
      }
      if (conversation.to && String(conversation.to) !== String(resolvedTenant.to)) {
        const err = new Error('Conversation phone does not belong to tenant.');
        logBoundary('warn', {
          operation,
          entity: 'conversation',
          accountId: resolvedTenant.accountId,
          convoKey: buildConvoKey(resolvedTenant.to, from),
          to: resolvedTenant.to,
          from,
          route,
          requestId,
          errorType: 'messaging_boundary_tenant_ambiguous',
          message: err.message
        });
        throw err;
      }
      conversation.accountId = resolvedTenant.accountId;
      conversation.id = buildConvoKey(resolvedTenant.to, from);
      conversation.to = String(conversation.to || resolvedTenant.to);
      conversation.from = String(conversation.from || from || '');
      conversation.messages = Array.isArray(conversation.messages) ? conversation.messages : [];
      conversation.audit = Array.isArray(conversation.audit) ? conversation.audit : [];
      return mutate(conversation, data, resolvedTenant);
    }, resolvedTenant.accountId);
  } catch (err) {
    if (/does not belong to tenant/i.test(String(err?.message || ''))) {
      return null;
    }
    logBoundary('error', {
      operation,
      entity: 'messaging_boundary',
      accountId: resolvedTenant.accountId,
      convoKey: buildConvoKey(resolvedTenant.to, from),
      to: resolvedTenant.to,
      from,
      route,
      requestId,
      errorType: 'messaging_boundary_sync_failed',
      message: err?.message || 'Legacy messaging write failed.'
    });
    throw err;
  }
}

function applyLifecycleReset(conversation, { to, from, accountId, source }) {
  conversation.accountId = accountId;
  conversation.id = buildConvoKey(to, from);
  conversation.to = String(to);
  conversation.from = String(from);
  conversation.status = 'new';
  conversation.stage = 'ask_service';
  conversation.closedAt = null;
  conversation.bookingTime = null;
  conversation.bookingEndTime = null;
  conversation.messages = [];
  conversation.fields = {};
  conversation.leadData = {};
  conversation.audit = [{
    ts: Date.now(),
    type: 'lifecycle_reset',
    meta: { source: String(source || 'inbound_after_closed') }
  }];
  conversation.flow = {
    flowId: null,
    ruleId: null,
    stepId: null,
    status: 'idle',
    startedAt: null,
    updatedAt: null,
    lastAutoSentAt: null,
    lockUntil: null
  };
}

async function recordOutboundAttempt({
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
  afterSuccess = null
}) {
  if (dbMessagingEnabled()) {
    return appendOutboundMessage({
      tenant,
      to,
      from,
      text,
      source,
      consentConfirmed,
      consentSource,
      transactional,
      bypassCompliance,
      meta,
      route,
      requestId,
      requireExisting,
      afterSuccess
    });
  }
  const idempotencyKey = deriveBoundaryIdempotencyKey({ meta, idempotencyKey: meta?.idempotencyKey });
  let sendResult = { ok: false, error: { code: 'unknown', message: 'Conversation not found.' } };
  const conversation = await mutateConversationLegacy({
    tenant,
    to,
    from,
    operation: 'recordOutboundAttempt',
    route,
    requestId,
    requireExisting,
    mutate(conversation, data, resolvedTenant) {
      const duplicate = findMessageByIdempotencyKey(conversation, idempotencyKey);
      if (duplicate) {
        logBoundary('warn', {
          entity: 'message',
          operation: 'recordOutboundAttempt',
          accountId: resolvedTenant.accountId,
          convoKey: buildConvoKey(resolvedTenant.to, from),
          to: resolvedTenant.to,
          from,
          route,
          requestId,
          errorType: 'messaging_boundary_duplicate_blocked',
          message: 'Duplicate outbound append blocked by boundary idempotency.'
        });
        sendResult = { ok: true, duplicate: true, message: duplicate };
        return;
      }

      const orderingSequence = nextOrderingSequence(conversation);
      sendResult = attemptOutboundMessage({
        data,
        tenant: resolvedTenant,
        conversation,
        from: String(from),
        text: String(text || ''),
        source,
        transactional,
        bypassCompliance,
        consentConfirmed,
        consentSource,
        meta: {
          ...(meta && typeof meta === 'object' ? meta : {}),
          idempotencyKey,
          orderingSequence
        }
      });
      if (!sendResult.ok) return;
      if (typeof afterSuccess === 'function') {
        afterSuccess(conversation, sendResult.message);
      }
    }
  });

  return { conversation, sendResult };
}

async function recordInboundSms({
  tenant,
  to,
  from,
  text,
  mediaCount = 0,
  eventKey = null,
  route = null,
  requestId = null
}) {
  if (dbMessagingEnabled()) {
    return recordInboundSmsDb({
      tenant,
      to,
      from,
      text,
      mediaCount,
      eventKey,
      route,
      requestId
    });
  }
  let compliance = null;
  let duplicateBlocked = false;
  const conversation = await mutateConversationLegacy({
    tenant,
    to,
    from,
    operation: 'recordInboundSms',
    route,
    requestId,
    requireExisting: false,
    mutate(conversation, data, resolvedTenant) {
      if (String(conversation?.status || '').toLowerCase() === 'closed') {
        applyLifecycleReset(conversation, {
          to: resolvedTenant.to,
          from,
          accountId: resolvedTenant.accountId,
          source: 'inbound_sms_after_closed'
        });
      }

      const idempotencyKey = deriveBoundaryIdempotencyKey({ eventKey });
      const duplicate = findMessageByIdempotencyKey(conversation, idempotencyKey);
      if (duplicate) {
        duplicateBlocked = true;
        logBoundary('warn', {
          entity: 'message',
          operation: 'recordInboundSms',
          accountId: resolvedTenant.accountId,
          convoKey: buildConvoKey(resolvedTenant.to, from),
          to: resolvedTenant.to,
          from,
          route,
          requestId,
          errorType: 'messaging_boundary_duplicate_blocked',
          message: 'Duplicate inbound SMS append blocked by boundary idempotency.'
        });
        return;
      }

      const message = newMessage('inbound', String(resolvedTenant.to), String(from), String(text || ''), {
        status: 'received',
        mediaCount: Number(mediaCount || 0),
        idempotencyKey,
        meta: {
          mediaCount: Number(mediaCount || 0),
          idempotencyKey,
          orderingSequence: nextOrderingSequence(conversation)
        }
      });
      warnOnBackdatedMessage(conversation, message, { operation: 'recordInboundSms', route, requestId });
      conversation.messages.push(message);
      conversation.lastActivityAt = Date.now();
      compliance = processInboundCompliance({
        data,
        tenant: resolvedTenant,
        conversation,
        from: String(from),
        text: String(text || '')
      });
    }
  });

  return { conversation, compliance, duplicateBlocked };
}

async function recordMissedCall({
  tenant,
  to,
  from,
  eventKey = null,
  route = null,
  requestId = null
}) {
  if (dbMessagingEnabled()) {
    const conversation = await recordMissedCallDb({
      tenant,
      to,
      from,
      eventKey,
      route,
      requestId
    });
    return { conversation, duplicateBlocked: false };
  }
  let duplicateBlocked = false;
  const conversation = await mutateConversationLegacy({
    tenant,
    to,
    from,
    operation: 'recordMissedCall',
    route,
    requestId,
    requireExisting: false,
    mutate(conversation, _data, resolvedTenant) {
      if (String(conversation?.status || '').toLowerCase() === 'closed') {
        applyLifecycleReset(conversation, {
          to: resolvedTenant.to,
          from,
          accountId: resolvedTenant.accountId,
          source: 'missed_call_after_closed'
        });
      }

      const idempotencyKey = deriveBoundaryIdempotencyKey({ eventKey });
      const alreadyMarked = conversation.audit.some((entry) =>
        String(entry?.type || '') === 'missed_call' &&
        String(entry?.meta?.idempotencyKey || '') === String(idempotencyKey || '')
      );
      if (idempotencyKey && alreadyMarked) {
        duplicateBlocked = true;
        logBoundary('warn', {
          entity: 'conversation',
          operation: 'recordMissedCall',
          accountId: resolvedTenant.accountId,
          convoKey: buildConvoKey(resolvedTenant.to, from),
          to: resolvedTenant.to,
          from,
          route,
          requestId,
          errorType: 'messaging_boundary_duplicate_blocked',
          message: 'Duplicate missed-call mutation blocked by boundary idempotency.'
        });
        return;
      }

      conversation.lastActivityAt = Date.now();
      conversation.status = 'active';
      conversation.audit.push({
        ts: Date.now(),
        type: 'missed_call',
        meta: {
          source: 'webhook_missed_call',
          idempotencyKey: idempotencyKey || null
        }
      });
    }
  });

  return { conversation, duplicateBlocked };
}

async function recordBookingSync({
  tenant,
  to,
  from,
  bookingStart,
  bookingEnd,
  bookingId = '',
  source,
  status = 'booked',
  service = '',
  serviceRequired = '',
  servicesList = [],
  servicesSummary = '',
  customerName = '',
  customerPhone = '',
  customerEmail = '',
  vehicle = '',
  amount = null,
  notes = '',
  appendMessage = false,
  messageText = '',
  patchConversation = null,
  route = null,
  requestId = null
}) {
  if (dbMessagingEnabled()) {
    const conversation = await recordBookingSyncDb({
      tenant,
      to,
      from,
      bookingStart,
      bookingEnd,
      bookingId,
      source,
      status,
      service,
      serviceRequired,
      servicesList,
      servicesSummary,
      customerName,
      customerPhone,
      customerEmail,
      vehicle,
      amount,
      notes,
      appendMessage,
      messageText,
      patchConversation,
      route,
      requestId
    });
    return conversation;
  }
  let duplicateBlocked = false;
  const conversation = await mutateConversationLegacy({
    tenant,
    to,
    from,
    operation: 'recordBookingSync',
    route,
    requestId,
    requireExisting: false,
    mutate(conversation, _data, resolvedTenant) {
      const idempotencyKey = deriveBoundaryIdempotencyKey({ bookingId, eventKey: bookingId ? `booking:${bookingId}` : null });
      const shouldSkipMessage = appendMessage && idempotencyKey && findMessageByIdempotencyKey(conversation, idempotencyKey);
      if (shouldSkipMessage) {
        duplicateBlocked = true;
        logBoundary('warn', {
          entity: 'message',
          operation: 'recordBookingSync',
          accountId: resolvedTenant.accountId,
          convoKey: buildConvoKey(resolvedTenant.to, from),
          to: resolvedTenant.to,
          from,
          route,
          requestId,
          errorType: 'messaging_boundary_duplicate_blocked',
          message: 'Duplicate booking message append blocked by boundary idempotency.'
        });
      }

      conversation.status = String(status || 'booked');
      conversation.bookingTime = Number(bookingStart || Date.now());
      conversation.bookingEndTime = Number(bookingEnd || (conversation.bookingTime + (60 * 60 * 1000)));
      conversation.closedAt = null;
      conversation.leadData = conversation.leadData && typeof conversation.leadData === 'object' ? conversation.leadData : {};
      conversation.leadData.intent = String(service || conversation.leadData.intent || '').trim();
      if (serviceRequired) conversation.leadData.request = String(serviceRequired);
      if (notes) conversation.leadData.notes = String(notes);
      if (customerName) conversation.leadData.customer_name = String(customerName).trim();
      if (customerPhone) conversation.leadData.customer_phone = String(customerPhone).trim();
      if (customerEmail) conversation.leadData.customer_email = String(customerEmail).trim();
      if (serviceRequired) conversation.leadData.service_required = String(serviceRequired);
      if (Array.isArray(servicesList) && servicesList.length) conversation.leadData.services_list = servicesList.slice(0, 8);
      if (servicesSummary) conversation.leadData.services_summary = String(servicesSummary);
      if (bookingId) conversation.leadData.booking_id = String(bookingId);
      conversation.leadData.booking_time = conversation.bookingTime;
      conversation.leadData.booking_end_time = conversation.bookingEndTime;
      if (vehicle) conversation.leadData.vehicle = String(vehicle).trim();
      if (amount != null && Number.isFinite(Number(amount)) && Number(amount) > 0) {
        conversation.amount = Number(amount);
        conversation.leadData.amount = Number(amount);
        conversation.leadData.booking_amount = Number(amount);
      }
      conversation.lastActivityAt = Date.now();
      if (typeof patchConversation === 'function') {
        patchConversation(conversation);
      }
      conversation.audit.push({
        ts: Date.now(),
        type: 'status_change',
        meta: { status: String(status || 'booked'), source: String(source || 'booking_sync') }
      });

      if (appendMessage && !shouldSkipMessage) {
        const message = newMessage('inbound', String(resolvedTenant.to), String(from), String(messageText || ''), {
          status: 'received',
          source: String(source || 'booking_sync'),
          bookingId: bookingId ? String(bookingId) : '',
          idempotencyKey,
          meta: {
            source: String(source || 'booking_sync'),
            bookingId: bookingId ? String(bookingId) : '',
            idempotencyKey,
            orderingSequence: nextOrderingSequence(conversation),
            bookingTime: conversation.bookingTime,
            bookingEndTime: conversation.bookingEndTime
          }
        });
        warnOnBackdatedMessage(conversation, message, { operation: 'recordBookingSync', route, requestId });
        conversation.messages.push(message);
      }
    }
  });

  return { conversation, duplicateBlocked };
}

async function updateConversationStatusLegacy({
  tenant,
  to,
  from,
  status,
  bookingTime = null,
  bookingEndTime = null,
  amount = null,
  source = 'legacy_status_update',
  patch = null,
  route = null,
  requestId = null,
  requireExisting = true
}) {
  if (dbMessagingEnabled()) {
    return updateConversationStatusDb({
      tenant,
      to,
      from,
      status,
      bookingTime,
      bookingEndTime,
      amount,
      source,
      patch,
      route,
      requestId,
      requireExisting
    });
  }
  return mutateConversationLegacy({
    tenant,
    to,
    from,
    operation: 'updateConversationStatusLegacy',
    route,
    requestId,
    requireExisting,
    mutate(conversation) {
      const normalizedStatus = String(status || '').toLowerCase();
      conversation.status = normalizedStatus;
      if (normalizedStatus === 'booked') {
        conversation.bookingTime = Number(bookingTime || Date.now());
        conversation.bookingEndTime = Number(bookingEndTime || (conversation.bookingTime + (60 * 60 * 1000)));
        conversation.closedAt = null;
        conversation.leadData = conversation.leadData && typeof conversation.leadData === 'object' ? conversation.leadData : {};
        conversation.leadData.booking_time = conversation.bookingTime;
        conversation.leadData.booking_end_time = conversation.bookingEndTime;
        if (Number.isFinite(Number(amount)) && Number(amount) > 0) {
          conversation.amount = Number(amount);
          conversation.leadData.amount = Number(amount);
        }
      }
      if (normalizedStatus === 'closed') {
        conversation.closedAt = Date.now();
      }
      conversation.lastActivityAt = Date.now();
      if (typeof patch === 'function') patch(conversation);
      conversation.audit.push({
        ts: Date.now(),
        type: 'status_change',
        meta: { status: conversation.status, source: String(source || 'legacy_status_update') }
      });
    }
  });
}

async function deleteConversationMessageLegacy({
  tenant,
  to,
  from,
  index,
  route = null,
  requestId = null
}) {
  if (dbMessagingEnabled()) {
    return deleteMessageByIndex({ tenant, to, from, index });
  }
  return mutateConversationLegacy({
    tenant,
    to,
    from,
    operation: 'deleteConversationMessageLegacy',
    route,
    requestId,
    requireExisting: true,
    mutate(conversation) {
      conversation.messages = Array.isArray(conversation.messages) ? conversation.messages : [];
      if (index >= conversation.messages.length) {
        throw new Error('Message index out of range');
      }
      conversation.messages.splice(index, 1);
      conversation.lastActivityAt = Date.now();
      conversation.audit.push({
        ts: Date.now(),
        type: 'message_deleted',
        meta: { index: Number(index), source: 'dashboard' }
      });
    }
  });
}

async function deleteConversationMessageById({
  tenant,
  to,
  from,
  messageId,
  route = null,
  requestId = null
}) {
  if (dbMessagingEnabled()) {
    return deleteMessageById({ tenant, to, from, messageId });
  }
  return mutateConversationLegacy({
    tenant,
    to,
    from,
    operation: 'deleteConversationMessageById',
    route,
    requestId,
    requireExisting: true,
    mutate(conversation) {
      conversation.messages = Array.isArray(conversation.messages) ? conversation.messages : [];
      const index = conversation.messages.findIndex((message) => String(message?.id || '') === String(messageId || ''));
      if (index < 0) {
        throw new Error('Message not found');
      }
      conversation.messages.splice(index, 1);
      conversation.lastActivityAt = Date.now();
      conversation.audit.push({
        ts: Date.now(),
        type: 'message_deleted',
        meta: { messageId: String(messageId), source: 'dashboard' }
      });
    }
  });
}

async function startFlowLegacy({
  tenant,
  to,
  from,
  flowId,
  ruleId = null,
  route = null,
  requestId = null,
  executeInitialStep
}) {
  if (dbMessagingEnabled()) {
    return mutateFlowConversation({
      tenant,
      to,
      from,
      requireExisting: false,
      mutate: async (conversation) => {
        conversation.flow = {
          flowId,
          ruleId: ruleId || null,
          stepId: 'start',
          status: 'active',
          startedAt: Date.now(),
          updatedAt: Date.now(),
          history: ['start'],
          data: {}
        };
        conversation.audit = Array.isArray(conversation.audit) ? conversation.audit : [];
        conversation.audit.push({
          ts: Date.now(),
          type: 'flow_started',
          meta: { flowId, ruleId }
        });
        if (typeof executeInitialStep === 'function') {
          await executeInitialStep(conversation);
        }
      }
    });
  }
  return mutateConversationLegacy({
    tenant,
    to,
    from,
    operation: 'startFlowLegacy',
    route,
    requestId,
    requireExisting: false,
    mutate: async (conversation) => {
      conversation.flow = {
        flowId,
        ruleId: ruleId || null,
        stepId: 'start',
        status: 'active',
        startedAt: Date.now(),
        updatedAt: Date.now(),
        history: ['start'],
        data: {}
      };
      conversation.audit.push({
        ts: Date.now(),
        type: 'flow_started',
        meta: { flowId, ruleId }
      });
      if (typeof executeInitialStep === 'function') {
        await executeInitialStep(conversation);
      }
    }
  });
}

async function advanceFlowLegacy({
  tenant,
  to,
  from,
  text,
  route = null,
  requestId = null,
  advance
}) {
  if (dbMessagingEnabled()) {
    return mutateFlowConversation({
      tenant,
      to,
      from,
      requireExisting: true,
      mutate: async (conversation) => {
        if (typeof advance === 'function') {
          await advance(conversation);
        }
        conversation.audit = Array.isArray(conversation.audit) ? conversation.audit : [];
        if (conversation.flow && conversation.flow.status === 'active') {
          conversation.flow.updatedAt = Date.now();
        }
        conversation.audit.push({
          ts: Date.now(),
          type: 'flow_advanced',
          meta: { stepId: conversation?.flow?.stepId || '', text: String(text || '') }
        });
      }
    });
  }
  return mutateConversationLegacy({
    tenant,
    to,
    from,
    operation: 'advanceFlowLegacy',
    route,
    requestId,
    requireExisting: true,
    mutate: async (conversation, data, resolvedTenant) => {
      if (String(conversation.accountId || '') !== String(resolvedTenant.accountId)) return;
      if (typeof advance === 'function') {
        await advance(conversation, data);
      }
      if (conversation.flow && conversation.flow.status === 'active') {
        conversation.flow.updatedAt = Date.now();
      }
      conversation.audit.push({
        ts: Date.now(),
        type: 'flow_advanced',
        meta: { stepId: conversation?.flow?.stepId || '', text: String(text || '') }
      });
    }
  });
}

async function flushLegacyMessagingState() {
  if (dbMessagingEnabled()) {
    throw new Error('messaging_core_snapshot_write_blocked');
  }
  await flushDataNow();
}

async function reconcileSnapshotMessagingToDb(options = {}) {
  if (dbMessagingEnabled()) {
    const err = new Error('messaging_snapshot_shutdown');
    err.code = 'messaging_snapshot_shutdown';
    throw err;
  }
  await reconcileSnapshotConversationsToDb(options);
  await reconcileSnapshotMessagesToDb(options);
}

module.exports = {
  applyLifecycleReset,
  buildConvoKey,
  flushLegacyMessagingState,
  recordOutboundAttempt,
  recordInboundSms,
  recordMissedCall,
  recordBookingSync,
  updateConversationStatusLegacy,
  deleteConversationMessageLegacy,
  deleteConversationMessageById,
  startFlowLegacy,
  advanceFlowLegacy
  ,
  reconcileSnapshotMessagingToDb
};
