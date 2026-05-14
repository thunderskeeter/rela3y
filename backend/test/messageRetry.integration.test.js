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
  shutdown,
  ACCOUNT_A_TO
} = require('./_shared');
const { pool } = require('../src/db/pool');
const { createIfMissing } = require('../src/repositories/conversationsRepo');
const { getById, listByConversation, updateStatusById } = require('../src/repositories/messagesRepo');
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
    payload: {
      id: key,
      convoKey: key,
      to: ACCOUNT_A_TO,
      from,
      status: 'new',
      stage: 'ask_service',
      accountId: ACCOUNT_A_ID,
      messages: [],
      audit: [],
      leadData: {}
    }
  });
  return key;
}

function tenant() {
  return { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO };
}

async function firstMessageForConversation(from) {
  const items = await listByConversation(pool, ACCOUNT_A_ID, convoKey(from));
  return items[0] || null;
}

async function run() {
  const app = await initApp();
  try {
    await seedBaseline();
    await resetDb();
    await seedDbTenants();
    updateDevSettings({ enabled: true, autoCreateTenants: true, verboseTenantLogs: false, simulateOutbound: false });

    {
      await resetDb();
      const from = '+18145550401';
      await ensureConversation(from);
      setMessageTransportForTests(async () => {
        const err = new Error('Twilio transport down');
        err.code = 'TWILIO_DOWN';
        throw err;
      });
      const out = await appendOutboundMessage({
        tenant: tenant(),
        to: ACCOUNT_A_TO,
        from,
        text: 'Retry me later',
        source: 'retry_test',
        requireExisting: true,
        waitForTransport: true
      });
      assert.equal(out.sendResult.ok, true);
      const stored = await firstMessageForConversation(from);
      assert.equal(stored.status, 'failed');
      assert.equal(stored.retryCount, 1);
      assert.equal(stored.failureCode, 'TWILIO_DOWN');
      assert.match(stored.failureReason, /Twilio transport down/);
      assert.equal(Boolean(stored.nextRetryAt), true);
    }

    {
      await resetDb();
      const from = '+18145550402';
      await ensureConversation(from);
      setMessageTransportForTests(async () => ({
        sid: 'SM_retry_success_001',
        status: 'sent',
        to: from,
        from: ACCOUNT_A_TO,
        messagingServiceSid: ''
      }));
      const initial = await appendOutboundMessage({
        tenant: tenant(),
        to: ACCOUNT_A_TO,
        from,
        text: 'Fail then retry',
        source: 'retry_test',
        requireExisting: true,
        waitForTransport: true
      });
      let stored = await getById(pool, ACCOUNT_A_ID, initial.sendResult.message.id);
      assert.equal(stored.status, 'sent');

      await updateStatusById(pool, ACCOUNT_A_ID, stored.id, {
        status: 'failed',
        retryCount: 1,
        lastAttemptAt: Date.now() - 60_000,
        updatedAt: Date.now() - 60_000,
        failureCode: 'TWILIO_DOWN',
        failureReason: 'seed failure',
        nextRetryAt: Date.now() - 1_000,
        providerMessageId: 'SM_retry_old_001',
        payload: { ...stored, failureCode: 'TWILIO_DOWN', failureReason: 'seed failure', nextRetryAt: Date.now() - 1_000 }
      });

      setMessageTransportForTests(async () => ({
        sid: 'SM_retry_success_002',
        status: 'sent',
        to: from,
        from: ACCOUNT_A_TO,
        messagingServiceSid: ''
      }));
      const result = await runRetryBatch({ limit: 10, now: Date.now() });
      assert.equal(result.claimedCount >= 1, true);
      stored = await getById(pool, ACCOUNT_A_ID, stored.id);
      assert.equal(stored.status, 'sent');
      assert.equal(stored.id, initial.sendResult.message.id);
      assert.equal(stored.providerMessageId, 'SM_retry_success_002');
      assert.equal(stored.nextRetryAt, null);
      assert.equal((await listByConversation(pool, ACCOUNT_A_ID, convoKey(from))).length, 1);
    }

    {
      await resetDb();
      const from = '+18145550403';
      await ensureConversation(from);
      setMessageTransportForTests(async () => {
        const err = new Error('Still failing');
        err.code = 'TWILIO_STILL_FAILING';
        throw err;
      });
      const initial = await appendOutboundMessage({
        tenant: tenant(),
        to: ACCOUNT_A_TO,
        from,
        text: 'Exhaust retries',
        source: 'retry_test',
        requireExisting: true,
        waitForTransport: true
      });
      let stored = await getById(pool, ACCOUNT_A_ID, initial.sendResult.message.id);
      assert.equal(stored.retryCount, 1);

      await runRetryBatch({ limit: 10, now: Number(stored.nextRetryAt) + 1 });
      stored = await getById(pool, ACCOUNT_A_ID, stored.id);
      assert.equal(stored.status, 'failed');
      assert.equal(stored.retryCount, 2);
      assert.equal(Boolean(stored.nextRetryAt), true);

      await runRetryBatch({ limit: 10, now: Number(stored.nextRetryAt) + 1 });
      stored = await getById(pool, ACCOUNT_A_ID, stored.id);
      assert.equal(stored.status, 'failed');
      assert.equal(stored.retryCount, 3);
      assert.equal(stored.nextRetryAt, null);
    }

    {
      await resetDb();
      const from = '+18145550404';
      await ensureConversation(from);
      setMessageTransportForTests(async () => {
        const err = new Error('Pickup seed failure');
        err.code = 'SEED_FAIL';
        throw err;
      });
      const initial = await appendOutboundMessage({
        tenant: tenant(),
        to: ACCOUNT_A_TO,
        from,
        text: 'Concurrent retry',
        source: 'retry_test',
        requireExisting: true,
        waitForTransport: true
      });
      let sendCount = 0;
      setMessageTransportForTests(async () => {
        sendCount += 1;
        await new Promise((resolve) => setTimeout(resolve, 50));
        return {
          sid: 'SM_retry_concurrent_001',
          status: 'sent',
          to: from,
          from: ACCOUNT_A_TO,
          messagingServiceSid: ''
        };
      });
      const stored = await getById(pool, ACCOUNT_A_ID, initial.sendResult.message.id);
      const now = Number(stored.nextRetryAt) + 1;
      const [a, b] = await Promise.all([
        runRetryBatch({ limit: 1, now }),
        runRetryBatch({ limit: 1, now })
      ]);
      assert.equal(a.claimedCount + b.claimedCount, 1);
      assert.equal(sendCount, 1);
    }

    {
      await resetDb();
      const from = '+18145550405';
      await ensureConversation(from);
      setMessageTransportForTests(async () => {
        const err = new Error('Callback retry seed fail');
        err.code = 'CALLBACK_SEED_FAIL';
        throw err;
      });
      const initial = await appendOutboundMessage({
        tenant: tenant(),
        to: ACCOUNT_A_TO,
        from,
        text: 'Retry then callback',
        source: 'retry_test',
        requireExisting: true,
        waitForTransport: true
      });
      setMessageTransportForTests(async () => ({
        sid: 'SM_retry_callback_001',
        status: 'sent',
        to: from,
        from: ACCOUNT_A_TO,
        messagingServiceSid: ''
      }));
      const failed = await getById(pool, ACCOUNT_A_ID, initial.sendResult.message.id);
      await runRetryBatch({ limit: 10, now: Number(failed.nextRetryAt) + 1 });
      let stored = await getById(pool, ACCOUNT_A_ID, failed.id);
      assert.equal(stored.providerMessageId, 'SM_retry_callback_001');

      const callback = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: 'SM_retry_callback_001',
          MessageStatus: 'delivered',
          To: from,
          From: ACCOUNT_A_TO
        });
      assert.equal(callback.statusCode, 200);
      stored = await getById(pool, ACCOUNT_A_ID, failed.id);
      assert.equal(stored.status, 'delivered');
      assert.equal(stored.id, failed.id);
    }

    console.log('[tests] message retry checks passed');
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
