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
const { getById } = require('../src/repositories/messagesRepo');
const { appendOutboundMessage, setMessageTransportForTests, resetMessageTransportForTests } = require('../src/services/messagesService');
const { runRetryBatch } = require('../src/services/messageRetryService');

const ACCOUNT_A_ID = 'acct_10000000001';

function convoKey(from) {
  return `${ACCOUNT_A_TO}__${from}`;
}

function tenant() {
  return { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO };
}

async function resetDb() {
  await pool.query('DELETE FROM webhook_receipts');
  await pool.query('DELETE FROM messages');
  await pool.query('DELETE FROM conversations');
}

async function ensureConversation(from, overrides = {}) {
  const key = convoKey(from);
  await createIfMissing(pool, ACCOUNT_A_ID, {
    convoKey: key,
    to: ACCOUNT_A_TO,
    from,
    status: overrides.status || 'new',
    stage: overrides.stage || 'ask_service',
    flow: overrides.flow,
    audit: overrides.audit,
    leadData: overrides.leadData,
    fields: overrides.fields,
    bookingTime: overrides.bookingTime,
    bookingEndTime: overrides.bookingEndTime,
    amount: overrides.amount,
    paymentStatus: overrides.paymentStatus,
    closedAt: overrides.closedAt,
    createdAt: overrides.createdAt || Date.now(),
    updatedAt: overrides.updatedAt || Date.now(),
    payload: overrides.payload || {}
  });
  return key;
}

async function fetchMessageRow(messageId) {
  const result = await pool.query(
    'SELECT id, to_number, from_number, provider_message_id, payload FROM messages WHERE tenant_id = $1 AND id = $2',
    [ACCOUNT_A_ID, messageId]
  );
  return result.rows[0] || null;
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
      const from = '+18145550801';
      const id = await ensureConversation(from, {
        flow: { status: 'idle' },
        audit: [],
        leadData: {}
      });
      setMessageTransportForTests(async () => ({
        sid: 'SM_canonical_send_001',
        status: 'sent',
        to: from,
        from: ACCOUNT_A_TO,
        messagingServiceSid: ''
      }));
      const result = await appendOutboundMessage({
        tenant: tenant(),
        to: ACCOUNT_A_TO,
        from,
        text: 'Canonical to/from outbound',
        source: 'canonical_fields_test',
        requireExisting: true,
        waitForTransport: true
      });
      assert.equal(result.sendResult.ok, true);
      const messageId = result.sendResult.message.id;
      const row = await fetchMessageRow(messageId);
      assert.equal(row.to_number, ACCOUNT_A_TO);
      assert.equal(row.from_number, from);
      assert.equal(Object.prototype.hasOwnProperty.call(row.payload || {}, 'to'), false);
      assert.equal(Object.prototype.hasOwnProperty.call(row.payload || {}, 'from'), false);

      const detail = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(detail.statusCode, 200);
      const message = detail.body.conversation.messages.find((item) => item.id === messageId);
      assert.equal(message.text, 'Canonical to/from outbound');
      assert.equal(message.to, ACCOUNT_A_TO);
      assert.equal(message.from, from);
    }

    {
      const from = '+18145550802';
      await ensureConversation(from, {
        flow: { status: 'idle' },
        audit: [],
        leadData: {}
      });
      setMessageTransportForTests(async () => {
        const err = new Error('retry canonical seed fail');
        err.code = 'RETRY_CANONICAL_FAIL';
        throw err;
      });
      const initial = await appendOutboundMessage({
        tenant: tenant(),
        to: ACCOUNT_A_TO,
        from,
        text: 'Retry canonical to/from',
        source: 'canonical_fields_test',
        requireExisting: true,
        waitForTransport: true
      });
      const messageId = initial.sendResult.message.id;
      await pool.query('UPDATE messages SET payload = $3::jsonb WHERE tenant_id = $1 AND id = $2', [
        ACCOUNT_A_ID,
        messageId,
        {}
      ]);
      await pool.query(
        'UPDATE messages SET status = $3, next_retry_at = NOW() - INTERVAL \'1 second\', updated_at = NOW() - INTERVAL \'1 second\' WHERE tenant_id = $1 AND id = $2',
        [ACCOUNT_A_ID, messageId, 'failed']
      );
      setMessageTransportForTests(async () => ({
        sid: 'SM_canonical_retry_001',
        status: 'sent',
        to: from,
        from: ACCOUNT_A_TO,
        messagingServiceSid: ''
      }));
      const retry = await runRetryBatch({ limit: 10, now: Date.now() });
      assert.equal(retry.claimedCount, 1);
      const afterRetry = await getById(pool, ACCOUNT_A_ID, messageId);
      assert.equal(afterRetry.status, 'sent');
      assert.equal(afterRetry.to, ACCOUNT_A_TO);
      assert.equal(afterRetry.from, from);
      assert.equal(afterRetry.providerMessageId, 'SM_canonical_retry_001');

      await pool.query('UPDATE messages SET payload = $3::jsonb WHERE tenant_id = $1 AND id = $2', [
        ACCOUNT_A_ID,
        messageId,
        {}
      ]);
      const callback = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: 'SM_canonical_retry_001',
          MessageStatus: 'delivered',
          To: from,
          From: ACCOUNT_A_TO
        });
      assert.equal(callback.statusCode, 200);
      const delivered = await getById(pool, ACCOUNT_A_ID, messageId);
      assert.equal(delivered.status, 'delivered');
      assert.equal(delivered.id, messageId);
    }

    {
      const newFrom = '+18145550803';
      const legacyFrom = '+18145550804';
      const bookingTime = Date.now() + 3_600_000;
      const bookingEndTime = bookingTime + 3_600_000;
      const canonicalId = await ensureConversation(newFrom, {
        status: 'booked',
        stage: 'booked',
        flow: { status: 'running', flowId: 'canonical_flow', updatedAt: bookingTime },
        audit: [{ ts: bookingTime - 5000, type: 'status_change', meta: { status: 'booked' } }],
        leadData: { request: 'paint correction', booking_id: 'bk_001' },
        fields: { location: 'Garage A' },
        bookingTime,
        bookingEndTime,
        amount: 275,
        paymentStatus: 'paid',
        closedAt: bookingEndTime + 1000,
        payload: {}
      });
      const legacyId = await ensureConversation(legacyFrom, {
        status: 'booked',
        stage: 'booked',
        flow: { status: 'running', flowId: 'legacy_flow', updatedAt: bookingTime },
        audit: [{ ts: bookingTime - 4000, type: 'status_change', meta: { status: 'booked' } }],
        leadData: { request: 'paint correction', booking_id: 'bk_legacy' },
        fields: { location: 'Garage B' },
        bookingTime,
        bookingEndTime,
        amount: 275,
        paymentStatus: 'paid',
        closedAt: bookingEndTime + 2000,
        payload: {
          flow: { status: 'running', flowId: 'legacy_flow', updatedAt: bookingTime },
          audit: [{ ts: bookingTime - 4000, type: 'status_change', meta: { status: 'booked' } }],
          leadData: { request: 'paint correction', booking_id: 'bk_legacy' },
          fields: { location: 'Garage B' },
          bookingTime,
          bookingEndTime,
          amount: 275,
          paymentStatus: 'paid',
          closedAt: bookingEndTime + 2000
        }
      });
      await pool.query(
        `UPDATE conversations
         SET flow_state = '{}'::jsonb,
             audit_entries = '[]'::jsonb,
             lead_data = '{}'::jsonb,
             fields_data = '{}'::jsonb,
             booking_time = NULL,
             booking_end_time = NULL,
             amount_value = NULL,
             payment_status = NULL,
             closed_at = NULL
         WHERE tenant_id = $1 AND convo_key = $2`,
        [ACCOUNT_A_ID, legacyId]
      );

      const canonicalDetail = await agent.get(`/api/conversations/${encodeURIComponent(canonicalId)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      const legacyDetail = await agent.get(`/api/conversations/${encodeURIComponent(legacyId)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(canonicalDetail.statusCode, 200);
      assert.equal(legacyDetail.statusCode, 200);
      assert.equal(canonicalDetail.body.conversation.flow.flowId, 'canonical_flow');
      assert.equal(legacyDetail.body.conversation.flow.flowId, 'legacy_flow');
      assert.equal(canonicalDetail.body.conversation.audit.length, 1);
      assert.equal(legacyDetail.body.conversation.audit.length, 1);
      assert.equal(canonicalDetail.body.conversation.leadData.request, 'paint correction');
      assert.equal(legacyDetail.body.conversation.leadData.request, 'paint correction');
      assert.equal(canonicalDetail.body.conversation.bookingTime, bookingTime);
      assert.equal(legacyDetail.body.conversation.bookingTime, bookingTime);
      assert.equal(canonicalDetail.body.conversation.paymentStatus, 'paid');
      assert.equal(legacyDetail.body.conversation.paymentStatus, 'paid');

      const list = await agent.get(`/api/conversations?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(list.statusCode, 200);
      const canonicalListItem = list.body.conversations.find((item) => item.id === canonicalId);
      const legacyListItem = list.body.conversations.find((item) => item.id === legacyId);
      assert.equal(canonicalListItem.paymentStatus, 'paid');
      assert.equal(legacyListItem.paymentStatus, 'paid');
      assert.equal(canonicalListItem.bookingTime, bookingTime);
      assert.equal(legacyListItem.bookingTime, bookingTime);
    }

    {
      const wrongTenantDelete = await agent
        .delete(`/api/conversations/${encodeURIComponent(convoKey('+18145550803'))}/messages/by-id/${encodeURIComponent('missing_message')}?to=${encodeURIComponent('+10000000002')}`)
        .set('x-csrf-token', csrf);
      assert.equal(wrongTenantDelete.statusCode, 404);
    }

    console.log('[tests] messaging canonical field expansion checks passed');
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
