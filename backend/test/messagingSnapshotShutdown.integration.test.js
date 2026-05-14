process.env.USE_DB_CONVERSATIONS = 'true';
process.env.USE_DB_MESSAGES = 'true';
process.env.ENABLE_PARITY_CHECKS = 'false';
process.env.DISABLE_SNAPSHOT_PERSISTENCE = 'false';

const assert = require('node:assert/strict');
const {
  initApp,
  seedBaseline,
  seedDbTenants,
  shutdown,
  ACCOUNT_A_TO
} = require('./_shared');
const {
  loadData,
  saveDataDebounced,
  flushDataNow,
  updateConversation,
  appendMessage,
  deleteConversation
} = require('../src/store/dataStore');
const { pool } = require('../src/db/pool');
const { createIfMissing } = require('../src/repositories/conversationsRepo');
const { insertIdempotent } = require('../src/repositories/messagesRepo');
const { appendOutboundMessage, setMessageTransportForTests, resetMessageTransportForTests } = require('../src/services/messagesService');
const { runRetryBatch } = require('../src/services/messageRetryService');
const { reconcileSnapshotMessagingToDb, flushLegacyMessagingState } = require('../src/services/messagingBoundaryService');
const { processTwilioStatusCallback } = require('../src/services/providerCallbackService');

const ACCOUNT_A_ID = 'acct_10000000001';

function convoKey(from) {
  return `${ACCOUNT_A_TO}__${from}`;
}

async function resetDb() {
  await pool.query('DELETE FROM webhook_receipts');
  await pool.query('DELETE FROM messages');
  await pool.query('DELETE FROM conversations');
  await pool.query('UPDATE app_state SET snapshot = $1::jsonb, updated_at = NOW() WHERE id = 1', [JSON.stringify({ conversations: {} })]);
}

async function run() {
  await initApp();
  try {
    await seedBaseline();
    await resetDb();
    await seedDbTenants();

    const from = '+18145550991';
    const id = convoKey(from);
    await createIfMissing(pool, ACCOUNT_A_ID, {
      convoKey: id,
      to: ACCOUNT_A_TO,
      from,
      status: 'new',
      stage: 'ask_service',
      audit: [],
      leadData: {},
      payload: {}
    });
    await insertIdempotent(pool, ACCOUNT_A_ID, id, {
      id: 'shutdown_seed_message',
      direction: 'outbound',
      body: 'Seed before shutdown flush',
      status: 'failed',
      retryCount: 1,
      nextRetryAt: Date.now() - 1000,
      to: ACCOUNT_A_TO,
      from,
      payload: {}
    });

    setMessageTransportForTests(async () => ({
      sid: 'SM_shutdown_retry_001',
      status: 'sent',
      to: from,
      from: ACCOUNT_A_TO,
      messagingServiceSid: ''
    }));
    const retry = await runRetryBatch({ limit: 5, now: Date.now() });
    assert.equal(retry.claimedCount, 1);

    const callback = await processTwilioStatusCallback({
      tenant: { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO },
      providerMessageId: 'SM_shutdown_retry_001',
      providerStatus: 'delivered',
      eventId: 'shutdown_callback_event_001',
      rawPayload: { test: true }
    });
    assert.equal(callback.ok, true);

    const send = await appendOutboundMessage({
      tenant: { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO },
      to: ACCOUNT_A_TO,
      from: '+18145550992',
      text: 'Snapshot shutdown send',
      source: 'snapshot_shutdown_test',
      requireExisting: false,
      waitForTransport: true
    });
    assert.equal(send.sendResult.ok, true);

    const data = loadData();
    assert.equal(Object.keys(data.conversations || {}).length, 0);
    data.accounts[ACCOUNT_A_TO].workspace = data.accounts[ACCOUNT_A_TO].workspace || {};
    data.accounts[ACCOUNT_A_TO].workspace.identity = data.accounts[ACCOUNT_A_TO].workspace.identity || {};
    data.accounts[ACCOUNT_A_TO].workspace.identity.businessName = 'Snapshot Shutdown Check';
    saveDataDebounced(data);
    await flushDataNow();

    const snapshot = await pool.query('SELECT snapshot FROM app_state WHERE id = 1');
    assert.equal(snapshot.rowCount, 1);
    assert.deepEqual(snapshot.rows[0].snapshot?.conversations || {}, {});

    const liveCounts = await pool.query(
      'SELECT (SELECT COUNT(*)::int FROM conversations WHERE tenant_id = $1) AS conversations, (SELECT COUNT(*)::int FROM messages WHERE tenant_id = $1) AS messages',
      [ACCOUNT_A_ID]
    );
    assert.equal(liveCounts.rows[0].conversations >= 2, true);
    assert.equal(liveCounts.rows[0].messages >= 2, true);

    assert.throws(() => updateConversation(ACCOUNT_A_TO, from, () => {}), /snapshot_write_blocked/);
    assert.throws(() => appendMessage(ACCOUNT_A_ID, id, { id: 'bad', body: 'bad' }), /snapshot_write_blocked/);
    assert.throws(() => deleteConversation(ACCOUNT_A_TO, from, ACCOUNT_A_ID), /snapshot_write_blocked/);
    await assert.rejects(() => flushLegacyMessagingState(), /messaging_core_snapshot_write_blocked/);
    await assert.rejects(() => reconcileSnapshotMessagingToDb(), /messaging_snapshot_shutdown/);

    console.log('[tests] messaging snapshot shutdown checks passed');
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
