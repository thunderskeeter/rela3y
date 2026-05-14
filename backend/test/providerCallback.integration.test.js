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
const { createIfMissing, getByConvoKey } = require('../src/repositories/conversationsRepo');
const { insertIdempotent, getByProviderMessageId } = require('../src/repositories/messagesRepo');

const ACCOUNT_A_ID = 'acct_10000000001';

function convoKey(from) {
  return `${ACCOUNT_A_TO}__${from}`;
}

async function resetDb() {
  await pool.query('DELETE FROM webhook_receipts');
  await pool.query('DELETE FROM messages');
  await pool.query('DELETE FROM conversations');
}

async function seedOutboundMessage(from, providerMessageId) {
  const key = convoKey(from);
  await createIfMissing(pool, ACCOUNT_A_ID, {
    convoKey: key,
    to: ACCOUNT_A_TO,
    from,
    status: 'new',
    stage: 'ask_service',
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
  const now = Date.now();
  return insertIdempotent(pool, ACCOUNT_A_ID, key, {
    id: `msg_${providerMessageId}`,
    direction: 'outbound',
    body: 'Provider callback seed',
    status: 'sent',
    providerMessageId,
    updatedAt: now,
    createdAt: now - 1000,
    payload: {
      id: `msg_${providerMessageId}`,
      conversationId: key,
      convoKey: key,
      direction: 'outbound',
      dir: 'out',
      body: 'Provider callback seed',
      text: 'Provider callback seed',
      status: 'sent',
      providerMessageId,
      providerMeta: {
        provider: 'twilio',
        sid: providerMessageId,
        deliveryStatus: 'sent'
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

    {
      const providerMessageId = 'SM_provider_callback_001';
      await seedOutboundMessage('+18145550301', providerMessageId);

      const res = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: providerMessageId,
          MessageStatus: 'delivered',
          To: '+18145550301',
          From: ACCOUNT_A_TO
        });

      assert.equal(res.statusCode, 200);
      assert.equal(res.body.ok, true);
      const stored = await getByProviderMessageId(pool, ACCOUNT_A_ID, providerMessageId);
      assert.equal(stored.status, 'delivered');
      assert.equal(stored.providerMessageId, providerMessageId);
    }

    {
      const providerMessageId = 'SM_provider_callback_002';
      await seedOutboundMessage('+18145550302', providerMessageId);

      const first = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: providerMessageId,
          MessageStatus: 'delivered',
          To: '+18145550302',
          From: ACCOUNT_A_TO
        });
      assert.equal(first.statusCode, 200);

      const duplicate = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: providerMessageId,
          MessageStatus: 'delivered',
          To: '+18145550302',
          From: ACCOUNT_A_TO
        });
      assert.equal(duplicate.statusCode, 200);
      assert.equal(duplicate.body.duplicate, true);
    }

    {
      const unmatched = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: 'SM_provider_callback_missing',
          MessageStatus: 'delivered',
          To: '+18145550303',
          From: ACCOUNT_A_TO
        });
      assert.equal(unmatched.statusCode, 200);
      assert.equal(unmatched.body.unmatched, true);
    }

    {
      const providerMessageId = 'SM_provider_callback_003';
      await seedOutboundMessage('+18145550304', providerMessageId);
      const delivered = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: providerMessageId,
          MessageStatus: 'delivered',
          To: '+18145550304',
          From: ACCOUNT_A_TO
        });
      assert.equal(delivered.statusCode, 200);

      const blocked = await request(app)
        .post(`/webhooks/twilio/status?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: providerMessageId,
          MessageStatus: 'sent',
          To: '+18145550304',
          From: ACCOUNT_A_TO
        });
      assert.equal(blocked.statusCode, 200);
      assert.equal(blocked.body.blocked, true);
      const stored = await getByProviderMessageId(pool, ACCOUNT_A_ID, providerMessageId);
      assert.equal(stored.status, 'delivered');
    }

    {
      const providerMessageId = 'SM_provider_callback_004';
      await seedOutboundMessage('+18145550305', providerMessageId);
      const res = await request(app)
        .post('/webhooks/twilio/status')
        .set('x-dev-webhook-secret', 'test-webhook-secret')
        .send({
          MessageSid: providerMessageId,
          MessageStatus: 'delivered',
          To: '+18145550305',
          From: ACCOUNT_A_TO
        });
      assert.equal(res.statusCode, 403);
    }

    const convo = await getByConvoKey(pool, ACCOUNT_A_ID, convoKey('+18145550301'));
    assert.equal(Array.isArray(convo.messages), true);
    assert.equal(convo.messages.some((item) => item.providerMessageId === 'SM_provider_callback_001'), true);

    console.log('[tests] provider callback normalization checks passed');
  } finally {
    await shutdown();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
