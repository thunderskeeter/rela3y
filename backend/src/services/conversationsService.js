const { pool } = require('../db/pool');
const { withTransaction } = require('../db/withTransaction');
const { USE_DB_CONVERSATIONS, USE_DB_MESSAGES } = require('../config/runtime');
const { loadData, getConversationById, getConversations } = require('../store/dataStore');
const { verifyParity, stableNormalize } = require('./migrationParityService');
const {
  listByTenant,
  getByConvoKey,
  createIfMissing,
  updateByConvoKey
} = require('../repositories/conversationsRepo');
const { insertIdempotent } = require('../repositories/messagesRepo');
const { newMessage } = require('./complianceService');
const {
  buildConversationPayloadProjection,
  buildMessagePayloadProjection
} = require('./messagingPayloadService');

function buildConvoKey(to, from) {
  return `${String(to || '')}__${String(from || '')}`;
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
    createdAt: Number(conversation?.createdAt || 0),
    updatedAt: Number(conversation?.updatedAt || 0),
    flow: conversation?.flow || null,
    audit: Array.isArray(conversation?.audit) ? conversation.audit : [],
    leadData: conversation?.leadData || {},
    messages: (Array.isArray(conversation?.messages) ? conversation.messages : []).map((message) => ({
      id: String(message?.id || ''),
      status: String(message?.status || ''),
      text: String(message?.text || message?.body || ''),
      ts: Number(message?.ts || 0)
    }))
  })));
}

function getSnapshotConversation(accountId, convoKey) {
  return normalizeConversationForRead(getConversationById(accountId, convoKey)?.conversation || null);
}

async function getDbConversation(accountId, convoKey, meta = {}) {
  return normalizeConversationForRead(await getByConvoKey(meta?.db || pool, accountId, convoKey));
}

async function listConversationsForTenant(accountId, meta = {}) {
  const oldFactory = async () => getConversations(accountId).map(({ id, conversation }) => normalizeConversationForRead({ ...conversation, id }));
  const newFactory = async () => (await listByTenant(meta?.db || pool, accountId)).map(normalizeConversationForRead);
  if (USE_DB_CONVERSATIONS || USE_DB_MESSAGES) {
    return newFactory();
  }
  return oldFactory();
}

async function getConversationDetail(accountId, convoKey, meta = {}) {
  if (USE_DB_CONVERSATIONS || USE_DB_MESSAGES) {
    return getDbConversation(accountId, convoKey, meta);
  }
  return getSnapshotConversation(accountId, convoKey);
}

async function listThreads(accountId, meta = {}) {
  const conversations = await listConversationsForTenant(accountId, meta);
  return conversations.map((c) => ({
    from: c.from,
    to: c.to,
    lastText: c.messages?.length ? c.messages[c.messages.length - 1].text : '',
    stage: c.stage || '',
    status: c.status || '',
    accountId: c.accountId || ''
  }));
}

async function recordMissedCall({ tenant, to, from, eventKey = null }) {
  const accountId = String(tenant?.accountId || '');
  const convoKey = buildConvoKey(to, from);
  return withTransaction(pool, async (db) => {
    let conversation = await getByConvoKey(db, accountId, convoKey);
    if (!conversation) {
      conversation = await createIfMissing(db, accountId, {
        convoKey,
        to,
        from,
        status: 'active',
        stage: 'ask_service',
        audit: [],
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
        payload: buildConversationPayloadProjection({
          audit: [],
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
          }
        })
      });
    }
    const audit = Array.isArray(conversation.audit) ? [...conversation.audit] : [];
    const alreadyMarked = audit.some((entry) =>
      String(entry?.type || '') === 'missed_call' &&
      String(entry?.meta?.idempotencyKey || '') === String(eventKey || '')
    );
    if (!alreadyMarked) {
      audit.push({
        ts: Date.now(),
        type: 'missed_call',
        meta: {
          source: 'webhook_missed_call',
          idempotencyKey: eventKey ? String(eventKey) : null
        }
      });
    }
    const next = {
      ...conversation,
      status: 'active',
      lastActivityAt: Date.now(),
      updatedAt: Date.now(),
      audit
    };
    await updateByConvoKey(db, accountId, convoKey, {
      to,
      from,
      status: next.status,
      stage: next.stage,
      flow: next.flow,
      audit: next.audit,
      leadData: next.leadData,
      fields: next.fields,
      bookingTime: next.bookingTime,
      bookingEndTime: next.bookingEndTime,
      amount: next.amount,
      paymentStatus: next.paymentStatus,
      closedAt: next.closedAt,
      lastActivityAt: next.lastActivityAt,
      updatedAt: next.updatedAt,
      payload: buildConversationPayloadProjection(next)
    });
    return normalizeConversationForRead(await getByConvoKey(db, accountId, convoKey));
  });
}

async function recordBookingSync(input = {}) {
  const accountId = String(input?.tenant?.accountId || '');
  const to = String(input?.to || '');
  const from = String(input?.from || '');
  const convoKey = buildConvoKey(to, from);
  return withTransaction(pool, async (db) => {
    let conversation = await getByConvoKey(db, accountId, convoKey);
    if (!conversation) {
      conversation = await createIfMissing(db, accountId, {
        convoKey,
        to,
        from,
        status: String(input?.status || 'booked'),
        stage: 'booked',
        audit: [],
        leadData: {},
        payload: buildConversationPayloadProjection({
          audit: [],
          leadData: {}
        })
      });
    }
    const leadData = conversation.leadData && typeof conversation.leadData === 'object' ? { ...conversation.leadData } : {};
    if (input?.service) leadData.intent = String(input.service);
    if (input?.serviceRequired) {
      leadData.request = String(input.serviceRequired);
      leadData.service_required = String(input.serviceRequired);
    }
    if (input?.notes) leadData.notes = String(input.notes);
    if (input?.customerName) leadData.customer_name = String(input.customerName);
    if (input?.customerPhone) leadData.customer_phone = String(input.customerPhone);
    if (input?.customerEmail) leadData.customer_email = String(input.customerEmail);
    if (Array.isArray(input?.servicesList) && input.servicesList.length) leadData.services_list = input.servicesList.slice(0, 8);
    if (input?.servicesSummary) leadData.services_summary = String(input.servicesSummary);
    if (input?.bookingId) leadData.booking_id = String(input.bookingId);
    if (input?.vehicle) leadData.vehicle = String(input.vehicle);
    if (input?.amount != null && Number.isFinite(Number(input.amount))) {
      leadData.amount = Number(input.amount);
      leadData.booking_amount = Number(input.amount);
    }
    const audit = [...(Array.isArray(conversation.audit) ? conversation.audit : []), {
      ts: Date.now(),
      type: 'status_change',
      meta: { status: String(input?.status || 'booked'), source: String(input?.source || 'booking_sync') }
    }];
    const next = {
      ...conversation,
      status: String(input?.status || 'booked'),
      stage: String(input?.status || 'booked'),
      bookingTime: Number(input?.bookingStart || Date.now()),
      bookingEndTime: Number(input?.bookingEnd || (Date.now() + (60 * 60 * 1000))),
      amount: input?.amount != null && Number.isFinite(Number(input.amount)) ? Number(input.amount) : conversation.amount,
      leadData,
      audit,
      lastActivityAt: Date.now(),
      updatedAt: Date.now()
    };
    if (typeof input?.patchConversation === 'function') {
      input.patchConversation(next);
    }
    await updateByConvoKey(db, accountId, convoKey, {
      to,
      from,
      status: next.status,
      stage: next.stage,
      flow: next.flow,
      audit: next.audit,
      leadData: next.leadData,
      fields: next.fields,
      bookingTime: next.bookingTime,
      bookingEndTime: next.bookingEndTime,
      amount: next.amount,
      paymentStatus: next.paymentStatus,
      closedAt: next.closedAt,
      lastActivityAt: next.lastActivityAt,
      updatedAt: next.updatedAt,
      payload: buildConversationPayloadProjection(next)
    });
    if (input?.appendMessage) {
      const message = newMessage('inbound', to, from, String(input?.messageText || ''), {
        status: 'received',
        idempotencyKey: input?.bookingId ? `booking:${String(input.bookingId)}` : null
      });
      await insertIdempotent(db, accountId, convoKey, {
        id: message.id,
        direction: 'inbound',
        body: message.body,
        status: 'received',
        idempotencyKey: input?.bookingId ? `booking:${String(input.bookingId)}` : null,
        createdAt: message.ts,
        updatedAt: message.ts,
        to,
        from,
        payload: buildMessagePayloadProjection({
          meta: {
            ...(message?.meta || {}),
            source: String(input?.source || 'booking_sync'),
            bookingId: input?.bookingId ? String(input.bookingId) : ''
          }
        })
      });
    }
    return normalizeConversationForRead(await getByConvoKey(db, accountId, convoKey));
  });
}

async function updateConversationStatus({
  tenant,
  to,
  from,
  status,
  bookingTime = null,
  bookingEndTime = null,
  amount = null,
  source = 'legacy_status_update',
  patch = null,
  requireExisting = true
}) {
  const accountId = String(tenant?.accountId || '');
  const convoKey = buildConvoKey(to, from);
  return withTransaction(pool, async (db) => {
    let conversation = await getByConvoKey(db, accountId, convoKey);
    if (!conversation && requireExisting) return null;
    conversation = conversation || await createIfMissing(db, accountId, {
      convoKey,
      to,
      from,
      status: String(status || 'new'),
      stage: 'ask_service',
      audit: [],
      leadData: {},
      payload: buildConversationPayloadProjection({ audit: [], leadData: {} })
    });
    const normalizedStatus = String(status || '').toLowerCase();
    const leadData = conversation.leadData && typeof conversation.leadData === 'object' ? { ...conversation.leadData } : {};
    const next = {
      ...conversation,
      status: normalizedStatus,
      lastActivityAt: Date.now(),
      updatedAt: Date.now(),
      audit: [...(Array.isArray(conversation.audit) ? conversation.audit : []), {
        ts: Date.now(),
        type: 'status_change',
        meta: { status: normalizedStatus, source: String(source || 'legacy_status_update') }
      }],
      leadData
    };
    if (normalizedStatus === 'booked') {
      next.bookingTime = Number(bookingTime || Date.now());
      next.bookingEndTime = Number(bookingEndTime || (next.bookingTime + (60 * 60 * 1000)));
      next.stage = 'booked';
      leadData.booking_time = next.bookingTime;
      leadData.booking_end_time = next.bookingEndTime;
      if (Number.isFinite(Number(amount)) && Number(amount) > 0) {
        next.amount = Number(amount);
        leadData.amount = Number(amount);
      }
    }
    if (normalizedStatus === 'closed') {
      next.closedAt = Date.now();
    }
    if (typeof patch === 'function') patch(next);
    await updateByConvoKey(db, accountId, convoKey, {
      to,
      from,
      status: next.status,
      stage: next.stage,
      flow: next.flow,
      audit: next.audit,
      leadData: next.leadData,
      fields: next.fields,
      bookingTime: next.bookingTime,
      bookingEndTime: next.bookingEndTime,
      amount: next.amount,
      paymentStatus: next.paymentStatus,
      closedAt: next.closedAt,
      lastActivityAt: next.lastActivityAt,
      updatedAt: next.updatedAt,
      payload: buildConversationPayloadProjection(next)
    });
    return normalizeConversationForRead(await getByConvoKey(db, accountId, convoKey));
  });
}

async function mutateFlowConversation({ tenant, to, from, requireExisting = false, mutate }) {
  const accountId = String(tenant?.accountId || '');
  const convoKey = buildConvoKey(to, from);
  return withTransaction(pool, async (db) => {
    let conversation = await getByConvoKey(db, accountId, convoKey);
    if (!conversation && requireExisting) return null;
    conversation = conversation || await createIfMissing(db, accountId, {
      convoKey,
      to,
      from,
      audit: [],
      leadData: {},
      payload: buildConversationPayloadProjection({ audit: [], leadData: {} })
    });
    const working = normalizeConversationForRead(conversation);
    if (typeof mutate === 'function') {
      await mutate(working);
    }
    working.lastActivityAt = working.lastActivityAt || Date.now();
    working.updatedAt = Date.now();
    await updateByConvoKey(db, accountId, convoKey, {
      to,
      from,
      status: working.status,
      stage: working.stage,
      flow: working.flow,
      audit: working.audit,
      leadData: working.leadData,
      fields: working.fields,
      bookingTime: working.bookingTime,
      bookingEndTime: working.bookingEndTime,
      amount: working.amount,
      paymentStatus: working.paymentStatus,
      closedAt: working.closedAt,
      lastActivityAt: working.lastActivityAt,
      updatedAt: working.updatedAt,
      payload: buildConversationPayloadProjection(working)
    });
    return normalizeConversationForRead(await getByConvoKey(db, accountId, convoKey));
  });
}

async function reconcileSnapshotConversationsToDb({ accountId = null } = {}) {
  const data = loadData();
  const entries = Object.entries(data.conversations || {})
    .filter(([, conversation]) => !accountId || String(conversation?.accountId || '') === String(accountId))
    .sort(([left], [right]) => String(left).localeCompare(String(right)));
  for (const [convoKey, conversation] of entries) {
    await withTransaction(pool, async (db) => {
      await createIfMissing(db, String(conversation?.accountId || ''), {
        convoKey,
        to: conversation?.to,
        from: conversation?.from,
        status: conversation?.status,
        stage: conversation?.stage,
        createdAt: conversation?.createdAt || Date.now(),
        updatedAt: conversation?.updatedAt || Date.now(),
        lastActivityAt: conversation?.lastActivityAt || null,
        flow: conversation?.flow || null,
        audit: Array.isArray(conversation?.audit) ? conversation.audit : [],
        leadData: conversation?.leadData || {},
        fields: conversation?.fields || {},
        bookingTime: conversation?.bookingTime || null,
        bookingEndTime: conversation?.bookingEndTime || null,
        amount: conversation?.amount ?? null,
        paymentStatus: conversation?.paymentStatus || null,
        closedAt: conversation?.closedAt || null,
        payload: buildConversationPayloadProjection(conversation || {})
      });
    });
  }
}

module.exports = {
  listConversationsForTenant,
  getConversationDetail,
  listThreads,
  recordMissedCall,
  recordBookingSync,
  updateConversationStatus,
  mutateFlowConversation,
  reconcileSnapshotConversationsToDb,
  normalizeConversationForRead
};
