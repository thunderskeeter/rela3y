process.env.USE_DB_CONVERSATIONS = 'true';
process.env.USE_DB_MESSAGES = 'true';
process.env.ENABLE_PARITY_CHECKS = 'false';
process.env.DISABLE_SNAPSHOT_PERSISTENCE = 'true';

const assert = require('node:assert/strict');
const {
  request,
  initApp,
  seedBaseline,
  seedDbTenants,
  login,
  shutdown,
  ACCOUNT_A_TO,
  ACCOUNT_B_TO,
  OWNER_EMAIL,
  OWNER_PASSWORD
} = require('./_shared');
const { loadData, saveDataDebounced } = require('../src/store/dataStore');
const { pool } = require('../src/db/pool');
const { ensureSchedulingConfig, listAvailability } = require('../src/services/publicBookingService');
const { getConversationDetail } = require('../src/services/conversationsService');
const { startFlow } = require('../src/services/flowEngine');
const { createIfMissing } = require('../src/repositories/conversationsRepo');
const { insertIdempotent } = require('../src/repositories/messagesRepo');

const ACCOUNT_A_ID = 'acct_10000000001';
const ACCOUNT_B_ID = 'acct_10000000002';

function convoKey(to, from) {
  return `${to}__${from}`;
}

function unique(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

async function seedCoreFixtures() {
  const now = Date.now();
  const convoA = convoKey(ACCOUNT_A_TO, '+18145550101');
  await createIfMissing(pool, ACCOUNT_A_ID, {
    convoKey: convoA,
    to: ACCOUNT_A_TO,
    from: '+18145550101',
    status: 'new',
    stage: 'ask_service',
    leadData: { request: 'wash' },
    audit: [],
    createdAt: now - 8000,
    updatedAt: now - 5000,
    lastActivityAt: now - 5000,
    payload: {}
  });
  await insertIdempotent(pool, ACCOUNT_A_ID, convoA, {
    id: unique('msg_seed_a_in'),
    direction: 'inbound',
    body: 'Initial inbound',
    status: 'received',
    to: ACCOUNT_A_TO,
    from: '+18145550101',
    createdAt: now - 5000,
    updatedAt: now - 5000,
    payload: {}
  });
  await insertIdempotent(pool, ACCOUNT_A_ID, convoA, {
    id: unique('msg_seed_a_out'),
    direction: 'outbound',
    body: 'Initial reply',
    status: 'sent',
    to: ACCOUNT_A_TO,
    from: '+18145550101',
    createdAt: now - 5000,
    updatedAt: now - 5000,
    payload: {}
  });

  const convoB = convoKey(ACCOUNT_B_TO, '+18145550901');
  await createIfMissing(pool, ACCOUNT_B_ID, {
    convoKey: convoB,
    to: ACCOUNT_B_TO,
    from: '+18145550901',
    status: 'new',
    stage: 'ask_service',
    audit: [],
    leadData: {},
    createdAt: now - 7000,
    updatedAt: now - 4000,
    lastActivityAt: now - 4000,
    payload: {}
  });
  await insertIdempotent(pool, ACCOUNT_B_ID, convoB, {
    id: unique('msg_seed_b_in'),
    direction: 'inbound',
    body: 'Tenant B seed',
    status: 'received',
    to: ACCOUNT_B_TO,
    from: '+18145550901',
    createdAt: now - 4000,
    updatedAt: now - 4000,
    payload: {}
  });
}

async function setupBookingWorkspace(token) {
  const data = loadData();
  const account = data?.accounts?.[ACCOUNT_A_TO];
  ensureSchedulingConfig(account);
  account.workspace = account.workspace || {};
  account.workspace.timezone = 'America/New_York';
  account.workspace.businessHours = {
    mon: [{ start: '09:00', end: '17:00' }],
    tue: [{ start: '09:00', end: '17:00' }],
    wed: [{ start: '09:00', end: '17:00' }],
    thu: [{ start: '09:00', end: '17:00' }],
    fri: [{ start: '09:00', end: '17:00' }],
    sat: [],
    sun: []
  };
  account.scheduling.publicToken = token;
  account.scheduling.mode = 'internal';
  account.scheduling.slotIntervalMin = 30;
  account.scheduling.leadTimeMin = 0;
  account.scheduling.bufferMin = 0;
  account.scheduling.maxBookingsPerDay = 0;
  account.internalBookings = [];
  account.calendarEvents = [];
  saveDataDebounced(data);
  const availability = listAvailability(account, { days: 7, durationMin: 60 });
  const day = availability.days.find((entry) => Array.isArray(entry?.slots) && entry.slots.length > 0);
  return {
    token,
    slot: {
      start: Number(day.slots[0].start),
      end: Number(day.slots[0].end)
    }
  };
}

async function resetMessagingDb() {
  await pool.query('DELETE FROM messages');
  await pool.query('DELETE FROM audit_logs WHERE entity_type = $1 OR entity_type = $2', ['conversation', 'integration']);
  await pool.query('DELETE FROM conversations');
}

async function run() {
  const app = await initApp();
  try {
    console.log('progress: baseline');
    await seedBaseline();
    console.log('progress: reset-db');
    await resetMessagingDb();
    console.log('progress: seed-tenants');
    await seedDbTenants();
    console.log('progress: seed-core');
    await seedCoreFixtures();

    const agent = request.agent(app);
    console.log('progress: login');
    const csrf = await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });
    const id = convoKey(ACCOUNT_A_TO, '+18145550101');

    console.log('progress: list');
    const listRes = await agent.get(`/api/conversations?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(listRes.statusCode, 200);
    assert.equal(Array.isArray(listRes.body?.conversations), true);
    assert.equal(listRes.body.conversations.some((item) => item.id === id), true);
    assert.equal(listRes.body.conversations.some((item) => item.id === convoKey(ACCOUNT_B_TO, '+18145550901')), false);

    const detailRes = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(detailRes.statusCode, 200);
    assert.equal(detailRes.body?.conversation?.messages?.length, 2);
    assert.equal(detailRes.body.conversation.messages.every((message) => typeof message.id === 'string' && message.id.length > 0), true);
    assert.equal(detailRes.body.conversation.messages[0].text, 'Initial inbound');
    assert.equal(detailRes.body.conversation.messages[1].text, 'Initial reply');

    const directRes = await agent.get(`/api/conversation?to=${encodeURIComponent(ACCOUNT_A_TO)}&from=${encodeURIComponent('+18145550101')}`);
    assert.equal(directRes.statusCode, 200);
    assert.equal(directRes.body.conversation.messages.every((message) => typeof message.id === 'string' && message.id.length > 0), true);

    const threadRes = await agent.get(`/api/threads?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(threadRes.statusCode, 200);
    assert.equal(threadRes.body.threads.some((thread) => thread.from === '+18145550101'), true);

    assert.equal(loadData().conversations[id], undefined);
    const sendRes = await agent
      .post(`/api/conversations/${encodeURIComponent(id)}/send?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ text: 'DB-native dashboard reply' });
    assert.equal(sendRes.statusCode, 200);
    assert.equal(sendRes.body?.ok, true);
    assert.equal(sendRes.body.conversation.messages.every((message) => typeof message.id === 'string' && message.id.length > 0), true);
    assert.equal(loadData().conversations[id], undefined);
    const afterSend = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(afterSend.body.conversation.messages.some((m) => m.text === 'DB-native dashboard reply'), true);
    const afterSnapshotClear = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(afterSnapshotClear.statusCode, 200);
    assert.equal(afterSnapshotClear.body.conversation.messages.some((m) => m.text === 'DB-native dashboard reply'), true);

    const statusRes = await agent
      .post(`/api/conversations/${encodeURIComponent(id)}/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ status: 'booked', bookingTime: Date.now() + 3600000, bookingEndTime: Date.now() + 7200000, amount: 250 });
    assert.equal(statusRes.statusCode, 200);
    assert.equal(statusRes.body.conversation.status, 'booked');
    assert.equal(statusRes.body.conversation.messages.every((message) => typeof message.id === 'string' && message.id.length > 0), true);
    const afterStatus = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(afterStatus.body.conversation.status, 'booked');

    const messageIdToDelete = afterStatus.body.conversation.messages[0].id;
    const deleteByIdRes = await agent
      .delete(`/api/conversations/${encodeURIComponent(id)}/messages/by-id/${encodeURIComponent(messageIdToDelete)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf);
    assert.equal(deleteByIdRes.statusCode, 200);
    assert.equal(deleteByIdRes.body.conversation.messages.some((message) => message.id === messageIdToDelete), false);
    const afterDeleteById = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(afterDeleteById.body.conversation.messages.some((message) => message.id === messageIdToDelete), false);

    const deleteRes = await agent
      .delete(`/api/conversations/${encodeURIComponent(id)}/messages/0?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf);
    assert.equal(deleteRes.statusCode, 200);
    const afterDelete = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(afterDelete.body.conversation.messages[0].text, 'DB-native dashboard reply');

    const concurrentId = convoKey(ACCOUNT_A_TO, '+18145550102');
    await createIfMissing(pool, ACCOUNT_A_ID, {
      convoKey: concurrentId,
      to: ACCOUNT_A_TO,
      from: '+18145550102',
      status: 'new',
      stage: 'ask_service',
      audit: [],
      leadData: {},
      createdAt: Date.now() - 3000,
      updatedAt: Date.now() - 1000,
      lastActivityAt: Date.now() - 1000,
      payload: {}
    });
    await insertIdempotent(pool, ACCOUNT_A_ID, concurrentId, {
      id: 'seed_first',
      direction: 'inbound',
      body: 'Concurrent first',
      status: 'received',
      to: ACCOUNT_A_TO,
      from: '+18145550102',
      createdAt: Date.now() - 2000,
      updatedAt: Date.now() - 2000,
      payload: {}
    });
    await insertIdempotent(pool, ACCOUNT_A_ID, concurrentId, {
      id: 'seed_second',
      direction: 'inbound',
      body: 'Concurrent second',
      status: 'received',
      to: ACCOUNT_A_TO,
      from: '+18145550102',
      createdAt: Date.now() - 1000,
      updatedAt: Date.now() - 1000,
      payload: {}
    });
    const concurrentBefore = await agent.get(`/api/conversations/${encodeURIComponent(concurrentId)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(concurrentBefore.statusCode, 200);
    const protectedMessageId = concurrentBefore.body.conversation.messages[1].id;
    const concurrentDelete = agent
      .delete(`/api/conversations/${encodeURIComponent(concurrentId)}/messages/by-id/${encodeURIComponent(protectedMessageId)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf);
    const concurrentInsert = agent
      .post(`/api/conversations/${encodeURIComponent(concurrentId)}/send?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ text: 'Concurrent outbound append' });
    const [concurrentDeleteRes, concurrentSendRes] = await Promise.all([concurrentDelete, concurrentInsert]);
    assert.equal(concurrentDeleteRes.statusCode, 200);
    assert.equal(concurrentSendRes.statusCode, 200);
    const concurrentAfter = await agent.get(`/api/conversations/${encodeURIComponent(concurrentId)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(concurrentAfter.statusCode, 200);
    assert.equal(concurrentAfter.body.conversation.messages.some((message) => message.id === protectedMessageId), false);
    assert.equal(concurrentAfter.body.conversation.messages.some((message) => message.text === 'Concurrent outbound append'), true);

    const wrongTenantRes = await agent
      .post(`/api/conversations/${encodeURIComponent(convoKey(ACCOUNT_B_TO, '+18145550901'))}/send?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ text: 'wrong tenant' });
    assert.equal(wrongTenantRes.statusCode, 404);

    const wrongTenantDeleteRes = await agent
      .delete(`/api/conversations/${encodeURIComponent(convoKey(ACCOUNT_B_TO, '+18145550901'))}/messages/by-id/${encodeURIComponent('nope')}?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf);
    assert.equal(wrongTenantDeleteRes.statusCode, 404);

    const smsSid = unique('SM_DB_CORE');
    const smsRes = await request(app)
      .post('/webhooks/sms')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send({
        From: '+18145550155',
        To: ACCOUNT_A_TO,
        Body: 'Inbound through DB core',
        MessageSid: smsSid
      });
    assert.equal(smsRes.statusCode, 200);
    const smsDup = await request(app)
      .post('/webhooks/sms')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send({
        From: '+18145550155',
        To: ACCOUNT_A_TO,
        Body: 'Inbound through DB core',
        MessageSid: smsSid
      });
    assert.equal(smsDup.statusCode, 200);
    assert.equal(smsDup.body?.duplicate, true);
    const inboundDetail = await agent.get(`/api/conversations/${encodeURIComponent(convoKey(ACCOUNT_A_TO, '+18145550155'))}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(inboundDetail.statusCode, 200);
    assert.equal(
      inboundDetail.body.conversation.messages.filter((message) => message.text === 'Inbound through DB core').length,
      1
    );

    const missedCall = await request(app)
      .post('/webhooks/missed-call')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send({ From: '+18145550177', To: ACCOUNT_A_TO, CallSid: unique('CA_DB_CORE') });
    assert.equal(missedCall.statusCode, 200);
    const missedDetail = await agent.get(`/api/conversations/${encodeURIComponent(convoKey(ACCOUNT_A_TO, '+18145550177'))}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(missedDetail.statusCode, 200);
    assert.equal(missedDetail.body.conversation.status, 'active');

    const { token, slot } = await setupBookingWorkspace(unique('booking_db_core'));
    const bookingRes = await request(app)
      .post(`/api/public/booking/${encodeURIComponent(token)}/book`)
      .send({
        customerName: 'DB Booker',
        customerPhone: '+18145550188',
        customerEmail: 'dbbooker@example.com',
        serviceName: 'Full Detail',
        notes: 'DB booking sync',
        start: slot.start,
        end: slot.end
      });
    assert.equal(bookingRes.statusCode, 200);
    assert.equal(bookingRes.body?.ok, true);
    const bookingDetail = await agent.get(`/api/conversations/${encodeURIComponent(convoKey(ACCOUNT_A_TO, '+18145550188'))}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(bookingDetail.statusCode, 200);
    assert.equal(bookingDetail.body.conversation.status, 'booked');

    const tenant = { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO };
    const flowConvoKey = convoKey(ACCOUNT_A_TO, '+18145550199');
    await createIfMissing(pool, ACCOUNT_A_ID, {
      convoKey: flowConvoKey,
      to: ACCOUNT_A_TO,
      from: '+18145550199',
      status: 'new',
      stage: 'ask_service',
      audit: [],
      leadData: {},
      createdAt: Date.now() - 2000,
      updatedAt: Date.now() - 1000,
      lastActivityAt: Date.now() - 1000,
      payload: {}
    });
    await insertIdempotent(pool, ACCOUNT_A_ID, flowConvoKey, {
      id: unique('flow_seed_msg'),
      direction: 'inbound',
      body: 'Need tint',
      status: 'received',
      to: ACCOUNT_A_TO,
      from: '+18145550199',
      createdAt: Date.now() - 1000,
      updatedAt: Date.now() - 1000,
      payload: {}
    });
    await startFlow({ tenant, to: ACCOUNT_A_TO, from: '+18145550199', flowId: 'detailing_missed_call_v1' });
    const flowConversation = await getConversationDetail(ACCOUNT_A_ID, flowConvoKey);
    assert.equal(Boolean(flowConversation?.flow), true);
    assert.equal(Array.isArray(flowConversation?.audit), true);

    console.log('progress: done');
    console.log('[tests] messaging core DB integration checks passed');
  } finally {
    await shutdown();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
