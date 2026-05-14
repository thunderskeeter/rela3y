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
  OWNER_PASSWORD,
  SUPERADMIN_EMAIL,
  SUPERADMIN_PASSWORD
} = require('./_shared');
const { pool } = require('../src/db/pool');
const { createIfMissing } = require('../src/repositories/conversationsRepo');
const { getById } = require('../src/repositories/messagesRepo');
const { appendOutboundMessage, setMessageTransportForTests, resetMessageTransportForTests } = require('../src/services/messagesService');
const { runRetryBatch } = require('../src/services/messageRetryService');
const { updateDevSettings } = require('../src/store/dataStore');

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
    payload: {}
  });
  return key;
}

function tenant() {
  return { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO };
}

async function run() {
  const app = await initApp();
  try {
    await seedBaseline();
    await resetDb();
    await seedDbTenants();
    updateDevSettings({ enabled: true, autoCreateTenants: true, verboseTenantLogs: false, simulateOutbound: false });

    const agent = request.agent(app);
    const csrf = await login(agent, { email: SUPERADMIN_EMAIL, password: SUPERADMIN_PASSWORD });
    agent.jar.setCookie(`csrfToken=${csrf}`, 'http://127.0.0.1');

    {
      const from = '+18145551201';
      const id = await ensureConversation(from);
      setMessageTransportForTests(async () => ({
        sid: 'SM_hardening_payload_001',
        status: 'sent',
        to: from,
        from: ACCOUNT_A_TO,
        messagingServiceSid: ''
      }));
      const out = await appendOutboundMessage({
        tenant: tenant(),
        to: ACCOUNT_A_TO,
        from,
        text: 'Payload minimized',
        source: 'hardening_test',
        requireExisting: true,
        waitForTransport: true
      });
      const row = await pool.query('SELECT payload FROM messages WHERE tenant_id = $1 AND id = $2', [ACCOUNT_A_ID, out.sendResult.message.id]);
      assert.equal(row.rows[0].payload.source, undefined);
      assert.equal(row.rows[0].payload.meta.source, 'hardening_test');
      assert.equal(row.rows[0].payload.providerMeta, undefined);

      const detail = await agent.get(`/api/conversations/${encodeURIComponent(id)}?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
      assert.equal(detail.statusCode, 200);
      assert.equal(detail.body.conversation.messages[0].providerMeta.sid, 'SM_hardening_payload_001');
    }

    {
      await resetDb();
      const from = '+18145551202';
      await ensureConversation(from);
      setMessageTransportForTests(async () => {
        const err = new Error('seed send fail');
        err.code = 'SEED_SEND_FAIL';
        throw err;
      });
      const initial = await appendOutboundMessage({
        tenant: tenant(),
        to: ACCOUNT_A_TO,
        from,
        text: 'Send retry callback flow',
        source: 'hardening_test',
        requireExisting: true,
        waitForTransport: true
      });
      let stored = await getById(pool, ACCOUNT_A_ID, initial.sendResult.message.id);
      assert.equal(stored.status, 'failed');
      assert.equal(Boolean(stored.queuedAt), true);
      assert.equal(Boolean(stored.failedAt), true);
      assert.equal(Boolean(stored.lastStatusEventAt), true);

      setMessageTransportForTests(async () => ({
        sid: 'SM_hardening_retry_001',
        status: 'sent',
        to: from,
        from: ACCOUNT_A_TO,
        messagingServiceSid: ''
      }));
      const retry = await runRetryBatch({ limit: 10, now: Number(stored.nextRetryAt) + 1 });
      assert.equal(retry.claimedCount, 1);
      stored = await getById(pool, ACCOUNT_A_ID, stored.id);
      assert.equal(stored.status, 'sent');
      assert.equal(stored.providerMessageId, 'SM_hardening_retry_001');
      assert.equal(Boolean(stored.sentAt), true);

      const delivered = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: 'SM_hardening_retry_001',
          MessageStatus: 'delivered',
          To: from,
          From: ACCOUNT_A_TO
        });
      assert.equal(delivered.statusCode, 200);

      const duplicate = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: 'SM_hardening_retry_001',
          MessageStatus: 'delivered',
          To: from,
          From: ACCOUNT_A_TO
        });
      assert.equal(duplicate.statusCode, 200);
      assert.equal(duplicate.body.duplicate, true);

      stored = await getById(pool, ACCOUNT_A_ID, stored.id);
      assert.equal(stored.status, 'delivered');
      assert.equal(Boolean(stored.deliveredAt), true);
      assert.equal(Boolean(stored.firstProviderCallbackAt), true);

      const retryAgain = await runRetryBatch({ limit: 10, now: Date.now() + (24 * 60 * 60 * 1000) });
      assert.equal(retryAgain.claimedCount, 0);

      const analytics = await agent.get(`/api/admin/developer/messaging/${encodeURIComponent(ACCOUNT_A_ID)}/analytics?range=30d`);
      assert.equal(analytics.statusCode, 200);
      assert.equal(analytics.body.analytics.outboundTotal >= 1, true);
      assert.equal(analytics.body.analytics.sentCount >= 1, true);
      assert.equal(analytics.body.analytics.deliveredCount >= 1, true);

      const messageAdmin = await agent.get(`/api/admin/developer/messaging/${encodeURIComponent(ACCOUNT_A_ID)}/messages/${encodeURIComponent(stored.id)}`);
      assert.equal(messageAdmin.statusCode, 200);
      assert.equal(messageAdmin.body.message.id, stored.id);
      assert.equal(messageAdmin.body.message.status, 'delivered');
      const blockedMessageAdmin = await agent.get(`/api/admin/developer/messaging/${encodeURIComponent('acct_missing')}/messages/${encodeURIComponent(stored.id)}`);
      assert.equal(blockedMessageAdmin.statusCode, 404);

      const convoByKey = await agent.get(`/api/admin/developer/messaging/${encodeURIComponent(ACCOUNT_A_ID)}/conversations/by-key/${encodeURIComponent(convoKey(from))}`);
      assert.equal(convoByKey.statusCode, 200);
      assert.equal(convoByKey.body.conversation.convoKey, convoKey(from));
      const blockedConvoByKey = await agent.get(`/api/admin/developer/messaging/${encodeURIComponent('acct_missing')}/conversations/by-key/${encodeURIComponent(convoKey(from))}`);
      assert.equal(blockedConvoByKey.statusCode, 404);
      const rowId = convoByKey.body.conversation.rowId;
      assert.equal(typeof rowId, 'string');
      assert.equal(rowId.length > 0, true);

      const convoByRow = await agent.get(`/api/admin/developer/messaging/${encodeURIComponent(ACCOUNT_A_ID)}/conversations/by-row/${encodeURIComponent(rowId)}`);
      assert.equal(convoByRow.statusCode, 200);
      assert.equal(convoByRow.body.conversation.rowId, rowId);
      const blockedConvoByRow = await agent.get(`/api/admin/developer/messaging/${encodeURIComponent('acct_missing')}/conversations/by-row/${encodeURIComponent(rowId)}`);
      assert.equal(blockedConvoByRow.statusCode, 404);

      const failures = await agent.get(`/api/admin/developer/messaging/${encodeURIComponent(ACCOUNT_A_ID)}/failures?limit=10`);
      assert.equal(failures.statusCode, 200);
      assert.equal(Array.isArray(failures.body.failures), true);
    }

    console.log('[tests] messaging hardening checks passed');
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
