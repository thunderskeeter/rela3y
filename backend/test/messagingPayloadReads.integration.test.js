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
const { pool } = require('../src/db/pool');
const { createIfMissing } = require('../src/repositories/conversationsRepo');
const { insertIdempotent } = require('../src/repositories/messagesRepo');

const ACCOUNT_A_ID = 'acct_10000000001';
const ACCOUNT_B_ID = 'acct_10000000002';

function convoKey(to, from) {
  return `${to}__${from}`;
}

async function resetDb() {
  await pool.query('DELETE FROM webhook_receipts');
  await pool.query('DELETE FROM messages');
  await pool.query('DELETE FROM conversations');
}

async function seedConversation({ accountId, to, from, status = 'new', stage = 'ask_service', payload = {} }) {
  return createIfMissing(pool, accountId, {
    convoKey: convoKey(to, from),
    to,
    from,
    status,
    stage,
    createdAt: Date.now() - 2000,
    updatedAt: Date.now() - 1000,
    lastActivityAt: Date.now() - 1000,
    payload: {
      id: convoKey(to, from),
      convoKey: convoKey(to, from),
      to,
      from,
      status,
      stage,
      accountId,
      messages: [],
      audit: [],
      leadData: {},
      ...payload
    }
  });
}

async function seedMessage({
  accountId,
  to,
  from,
  id,
  body,
  status,
  retryCount = 0,
  lastAttemptAt = null,
  failureCode = null,
  failureReason = null,
  nextRetryAt = null,
  providerMessageId = null,
  payload = {}
}) {
  const createdAt = Date.now() - 1000;
  return insertIdempotent(pool, accountId, convoKey(to, from), {
    id,
    direction: 'outbound',
    body,
    status,
    retryCount,
    lastAttemptAt,
    failureCode,
    failureReason,
    nextRetryAt,
    providerMessageId,
    createdAt,
    updatedAt: createdAt + 10,
    payload: {
      id,
      convoKey: convoKey(to, from),
      conversationId: convoKey(to, from),
      direction: 'outbound',
      dir: 'out',
      body,
      text: body,
      status,
      ...payload
    }
  });
}

async function run() {
  const app = await initApp();
  try {
    await seedBaseline();
    await resetDb();
    await seedDbTenants();

    const agent = request.agent(app);
    const csrf = await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });

    {
      const from = '+18145550601';
      const id = convoKey(ACCOUNT_A_TO, from);
      await seedConversation({
        accountId: ACCOUNT_A_ID,
        to: ACCOUNT_A_TO,
        from,
        status: 'new',
        stage: 'ask_service',
        payload: {
          status: 'closed',
          stage: 'closed',
          leadData: 'bad',
          audit: 'bad'
        }
      });
      await seedMessage({
        accountId: ACCOUNT_A_ID,
        to: ACCOUNT_A_TO,
        from,
        id: 'msg_payload_read_001',
        body: 'Canonical payload-retired body',
        status: 'failed',
        retryCount: 2,
        lastAttemptAt: Date.now() - 60_000,
        failureCode: 'TWILIO_DOWN',
        failureReason: 'Carrier rejected send',
        nextRetryAt: Date.now() + 300_000,
        providerMessageId: 'SM_payload_read_001',
        payload: {
          id: 'wrong_id',
          text: 'Wrong payload body',
          status: 'delivered',
          retryCount: 99,
          failureCode: 'WRONG',
          failureReason: 'WRONG',
          nextRetryAt: 1,
          providerMessageId: 'SM_wrong_payload',
          providerMeta: { sid: 'SM_wrong_payload', deliveryStatus: 'delivered' }
        }
      });

      const detail = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(detail.statusCode, 200);
      assert.equal(detail.body.conversation.status, 'new');
      assert.equal(detail.body.conversation.stage, 'ask_service');
      assert.deepEqual(detail.body.conversation.leadData, {});
      assert.deepEqual(detail.body.conversation.audit, []);
      assert.equal(detail.body.conversation.messages.length, 1);
      const message = detail.body.conversation.messages[0];
      assert.equal(message.id, 'msg_payload_read_001');
      assert.equal(message.text, 'Canonical payload-retired body');
      assert.equal(message.status, 'failed');
      assert.equal(message.retryCount, 2);
      assert.equal(message.failureCode, 'TWILIO_DOWN');
      assert.equal(message.failureReason, 'Carrier rejected send');
      assert.equal(message.providerMessageId, 'SM_payload_read_001');
      assert.equal(message.providerMeta.sid, 'SM_payload_read_001');
      assert.equal(message.providerMeta.deliveryStatus, 'failed');

      const direct = await agent.get(`/api/conversation?to=${encodeURIComponent(ACCOUNT_A_TO)}&from=${encodeURIComponent(from)}`);
      assert.equal(direct.statusCode, 200);
      assert.equal(direct.body.conversation.messages[0].id, 'msg_payload_read_001');
      assert.equal(direct.body.conversation.messages[0].text, 'Canonical payload-retired body');
    }

    {
      const from = '+18145550602';
      const id = convoKey(ACCOUNT_A_TO, from);
      await seedConversation({
        accountId: ACCOUNT_A_ID,
        to: ACCOUNT_A_TO,
        from,
        status: 'new',
        stage: 'ask_service'
      });
      await seedMessage({
        accountId: ACCOUNT_A_ID,
        to: ACCOUNT_A_TO,
        from,
        id: 'msg_payload_read_002',
        body: 'Canonical malformed payload body',
        status: 'sent',
        providerMessageId: 'SM_payload_read_002'
      });
      await pool.query(
        'UPDATE conversations SET payload = $3::jsonb WHERE tenant_id = $1 AND convo_key = $2',
        [ACCOUNT_A_ID, id, 'null']
      );
      await pool.query(
        'UPDATE messages SET payload = $3::jsonb WHERE tenant_id = $1 AND id = $2',
        [ACCOUNT_A_ID, 'msg_payload_read_002', '"broken-payload"']
      );

      const detail = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(detail.statusCode, 200);
      assert.equal(detail.body.conversation.id, id);
      assert.equal(detail.body.conversation.status, 'new');
      assert.deepEqual(detail.body.conversation.audit, []);
      assert.deepEqual(detail.body.conversation.leadData, {});
      assert.equal(detail.body.conversation.flow, null);
      assert.equal(detail.body.conversation.messages[0].id, 'msg_payload_read_002');
      assert.equal(detail.body.conversation.messages[0].text, 'Canonical malformed payload body');
      assert.equal(detail.body.conversation.messages[0].providerMessageId, 'SM_payload_read_002');
    }

    {
      const from = '+18145550603';
      const id = convoKey(ACCOUNT_A_TO, from);
      await seedConversation({
        accountId: ACCOUNT_A_ID,
        to: ACCOUNT_A_TO,
        from,
        status: 'new',
        stage: 'ask_service'
      });
      await seedMessage({
        accountId: ACCOUNT_A_ID,
        to: ACCOUNT_A_TO,
        from,
        id: 'msg_payload_read_003',
        body: 'Canonical callback body',
        status: 'sent',
        providerMessageId: 'SM_payload_read_003'
      });
      await pool.query(
        'UPDATE messages SET payload = $3::jsonb WHERE tenant_id = $1 AND id = $2',
        [ACCOUNT_A_ID, 'msg_payload_read_003', '{"providerMeta":{"sid":"SM_wrong_sid","deliveryStatus":"failed"},"status":"failed"}']
      );

      const callback = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: 'SM_payload_read_003',
          MessageStatus: 'delivered',
          To: from,
          From: ACCOUNT_A_TO
        });
      assert.equal(callback.statusCode, 200);
      const detail = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(detail.statusCode, 200);
      assert.equal(detail.body.conversation.messages[0].status, 'delivered');
      assert.equal(detail.body.conversation.messages[0].providerMessageId, 'SM_payload_read_003');
      assert.equal(detail.body.conversation.messages[0].providerMeta.sid, 'SM_payload_read_003');
      assert.equal(detail.body.conversation.messages[0].providerMeta.deliveryStatus, 'delivered');
    }

    {
      const from = '+18145550911';
      const id = convoKey(ACCOUNT_B_TO, from);
      await seedConversation({
        accountId: ACCOUNT_B_ID,
        to: ACCOUNT_B_TO,
        from,
        status: 'new',
        stage: 'ask_service',
        payload: { leadData: 'bad', audit: 'bad' }
      });
      await seedMessage({
        accountId: ACCOUNT_B_ID,
        to: ACCOUNT_B_TO,
        from,
        id: 'msg_payload_read_tenant_b',
        body: 'Tenant B canonical body',
        status: 'sent',
        providerMessageId: 'SM_payload_read_b'
      });
      await pool.query(
        'UPDATE messages SET payload = $3::jsonb WHERE tenant_id = $1 AND id = $2',
        [ACCOUNT_B_ID, 'msg_payload_read_tenant_b', 'null']
      );
      const blocked = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(blocked.statusCode, 404);
    }

    console.log('[tests] messaging payload read retirement checks passed');
  } finally {
    await shutdown();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
