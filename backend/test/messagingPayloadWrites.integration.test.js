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
  OWNER_EMAIL,
  OWNER_PASSWORD
} = require('./_shared');
const { pool } = require('../src/db/pool');
const { createIfMissing } = require('../src/repositories/conversationsRepo');
const { insertIdempotent } = require('../src/repositories/messagesRepo');
const { updateDevSettings } = require('../src/store/dataStore');
const {
  appendOutboundMessage,
  setMessageTransportForTests,
  resetMessageTransportForTests
} = require('../src/services/messagesService');

const ACCOUNT_A_ID = 'acct_10000000001';

function convoKey(from) {
  return `${ACCOUNT_A_TO}__${from}`;
}

async function resetDb() {
  await pool.query('DELETE FROM webhook_receipts');
  await pool.query('DELETE FROM messages');
  await pool.query('DELETE FROM conversations');
}

async function ensureConversation(from) {
  const key = convoKey(from);
  await createIfMissing(pool, ACCOUNT_A_ID, {
    convoKey: key,
    to: ACCOUNT_A_TO,
    from,
    status: 'new',
    stage: 'ask_service',
    createdAt: Date.now(),
    updatedAt: Date.now(),
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
    audit: [],
    leadData: {},
    payload: {}
  });
  return key;
}

async function getMessagePayload(messageId) {
  const result = await pool.query('SELECT payload FROM messages WHERE tenant_id = $1 AND id = $2', [ACCOUNT_A_ID, messageId]);
  return result.rows[0]?.payload || null;
}

async function getConversationPayload(conversationId) {
  const result = await pool.query('SELECT payload FROM conversations WHERE tenant_id = $1 AND convo_key = $2', [ACCOUNT_A_ID, conversationId]);
  return result.rows[0]?.payload || null;
}

async function seedLegacyMessage(from) {
  const key = convoKey(from);
  await insertIdempotent(pool, ACCOUNT_A_ID, key, {
    id: 'legacy_full_payload_message',
    direction: 'outbound',
    body: 'Legacy full payload body',
    status: 'sent',
    providerMessageId: 'SM_legacy_payload',
    createdAt: Date.now() - 5000,
    updatedAt: Date.now() - 4000,
    payload: {
      id: 'legacy_full_payload_message',
      convoKey: key,
      conversationId: key,
      accountId: ACCOUNT_A_ID,
      direction: 'outbound',
      dir: 'out',
      body: 'Legacy full payload body',
      text: 'Legacy full payload body',
      status: 'sent',
      retryCount: 0,
      providerMessageId: 'SM_legacy_payload',
      providerMeta: {
        provider: 'twilio',
        sid: 'SM_legacy_payload',
        deliveryStatus: 'sent'
      },
      meta: {
        source: 'legacy_seed'
      }
    }
  });
}

async function run() {
  const app = await initApp();
  try {
    await seedBaseline();
    await resetDb();
    await seedDbTenants();
    updateDevSettings({ enabled: true, autoCreateTenants: true, verboseTenantLogs: false, simulateOutbound: false });

    const agent = request.agent(app);
    await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });

    {
      const from = '+18145550701';
      const id = await ensureConversation(from);
      setMessageTransportForTests(async () => ({
        sid: 'SM_payload_write_outbound',
        status: 'sent',
        to: from,
        from: ACCOUNT_A_TO,
        messagingServiceSid: ''
      }));
      const out = await appendOutboundMessage({
        tenant: { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO },
        to: ACCOUNT_A_TO,
        from,
        text: 'Minimal outbound payload',
        source: 'payload_write_test',
        requireExisting: true,
        waitForTransport: true
      });
      const messageId = out.sendResult.message.id;
      const payload = await getMessagePayload(messageId);
      assert.equal(typeof payload, 'object');
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'id'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'text'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'body'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'status'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'conversationId'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'convoKey'), false);
      assert.equal(payload.source, undefined);
      assert.equal(payload.meta.source, 'payload_write_test');
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'providerMessageId'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'providerMeta'), false);

      const conversationPayload = await getConversationPayload(id);
      assert.equal(Object.prototype.hasOwnProperty.call(conversationPayload, 'id'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(conversationPayload, 'to'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(conversationPayload, 'from'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(conversationPayload, 'status'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(conversationPayload, 'messages'), false);
      assert.deepEqual(conversationPayload, {});

      const detail = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(detail.statusCode, 200);
      assert.equal(detail.body.conversation.messages[0].text, 'Minimal outbound payload');
      assert.equal(detail.body.conversation.messages[0].providerMessageId, 'SM_payload_write_outbound');
    }

    {
      const from = '+18145550702';
      const id = await ensureConversation(from);
      const inbound = await request(app)
        .post('/webhooks/sms')
        .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
        .send({
          From: from,
          To: ACCOUNT_A_TO,
          Body: 'Minimal inbound payload',
          MessageSid: 'SM_payload_write_inbound'
        });
      assert.equal(inbound.statusCode, 200);
      const rows = await pool.query(
        'SELECT id, payload FROM messages WHERE tenant_id = $1 AND conversation_id = $2 ORDER BY created_at ASC, id ASC',
        [ACCOUNT_A_ID, id]
      );
      assert.equal(rows.rowCount >= 1, true);
      const payload = rows.rows[0].payload;
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'id'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'text'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'body'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'status'), false);
      assert.equal(typeof payload.meta, 'object');
      assert.equal(payload.meta.mediaCount, 0);
      assert.equal(Object.prototype.hasOwnProperty.call(payload, 'providerMeta'), false);

      const detail = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(detail.statusCode, 200);
      assert.equal(detail.body.conversation.messages[0].text, 'Minimal inbound payload');
    }

    {
      const from = '+18145550703';
      const id = await ensureConversation(from);
      await seedLegacyMessage(from);
      setMessageTransportForTests(async () => ({
        sid: 'SM_payload_write_mixed',
        status: 'sent',
        to: from,
        from: ACCOUNT_A_TO,
        messagingServiceSid: ''
      }));
      await appendOutboundMessage({
        tenant: { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO },
        to: ACCOUNT_A_TO,
        from,
        text: 'Minimal mixed payload',
        source: 'payload_write_test',
        requireExisting: true,
        waitForTransport: true
      });
      const detail = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(detail.statusCode, 200);
      assert.equal(detail.body.conversation.messages.length, 2);
      assert.equal(detail.body.conversation.messages.some((message) => message.text === 'Legacy full payload body'), true);
      assert.equal(detail.body.conversation.messages.some((message) => message.text === 'Minimal mixed payload'), true);
    }

    console.log('[tests] messaging payload write reduction checks passed');
  } finally {
    resetMessageTransportForTests();
    await shutdown();
  }
}

run().catch((err) => {
  resetMessageTransportForTests();
  console.error(err);
  process.exit(1);
});
