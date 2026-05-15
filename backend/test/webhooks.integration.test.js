const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const {
  request,
  initApp,
  seedBaseline,
  shutdown,
  ACCOUNT_A_TO
} = require('./_shared');

function stripeSig(payload, secret) {
  const ts = Math.floor(Date.now() / 1000);
  const signed = `${ts}.${payload}`;
  const digest = crypto.createHmac('sha256', String(secret)).update(signed, 'utf8').digest('hex');
  return `t=${ts},v1=${digest}`;
}

async function run() {
  const app = await initApp();

  await seedBaseline();
  {
    const payload = {
      id: 'evt_replay_1',
      type: 'lead_lost',
      from: '+18145550123',
      To: ACCOUNT_A_TO,
      data: {}
    };
    const first = await request(app)
      .post('/webhooks/event')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send(payload);
    assert.equal(first.statusCode, 200);
    assert.equal(first.body?.ok, true);

    const second = await request(app)
      .post('/webhooks/event')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send(payload);
    assert.equal(second.statusCode, 200);
    assert.equal(second.body?.duplicate, true);
  }

  await seedBaseline();
  {
    const unsupported = await request(app)
      .post('/webhooks/event')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send({
        id: 'evt_bad_type_1',
        type: 'drop_database_now',
        from: '+18145550124',
        To: ACCOUNT_A_TO,
        data: {}
      });
    assert.equal(unsupported.statusCode, 400);
    assert.equal(unsupported.body?.error, 'Unsupported event type');
  }

  await seedBaseline();
  {
    const missingSig = await request(app)
      .post('/webhooks/sms')
      .send({
        From: '+18145550188',
        To: ACCOUNT_A_TO,
        Body: 'hello'
      });
    assert.equal(missingSig.statusCode, 403);

    const smsPayload = {
      From: '+18145550125',
      To: ACCOUNT_A_TO,
      Body: 'Need pricing'
    };
    const smsFirst = await request(app)
      .post('/webhooks/sms')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send(smsPayload);
    assert.equal(smsFirst.statusCode, 200);
    assert.equal(smsFirst.body?.ok, true);
    const smsSecond = await request(app)
      .post('/webhooks/sms')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send(smsPayload);
    assert.equal(smsSecond.statusCode, 200);
    assert.equal(smsSecond.body?.duplicate, true);

    const mmsPayload = {
      From: '+18145550127',
      To: ACCOUNT_A_TO,
      NumMedia: '2'
    };
    const mmsOnly = await request(app)
      .post('/webhooks/sms')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send(mmsPayload);
    assert.equal(mmsOnly.statusCode, 200);
    assert.equal(mmsOnly.body?.ok, true);

    const missedCallPayload = {
      From: '+18145550126',
      To: ACCOUNT_A_TO
    };
    const callFirst = await request(app)
      .post('/webhooks/missed-call')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send(missedCallPayload);
    assert.equal(callFirst.statusCode, 200);
    assert.equal(callFirst.body?.ok, true);
    const callSecond = await request(app)
      .post('/webhooks/missed-call')
      .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
      .send(missedCallPayload);
    assert.equal(callSecond.statusCode, 200);
    assert.equal(callSecond.body?.duplicate, true);
  }

  await seedBaseline();
  {
    let saw429 = false;
    const attempts = process.env.DEV_MODE === 'true' ? 320 : 80;
    for (let i = 0; i < attempts; i += 1) {
      const res = await request(app)
        .post('/webhooks/event')
        .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
        .set('x-forwarded-for', '10.9.9.9')
        .send({
          id: `evt_rate_${i}`,
          type: 'unsupported_rate_limit_probe',
          from: '+18145550999',
          To: ACCOUNT_A_TO,
          data: {}
        });
      if (res.statusCode === 429) {
        saw429 = true;
        break;
      }
    }
    assert.equal(saw429, true);
  }

  await seedBaseline();
  {
    const body = JSON.stringify({ id: 'evt_sig_1', type: 'invoice.paid', data: { object: {} } });
    const sig = 't=123,v1=deadbeef';
    const badSig = await request(app)
      .post(`/webhooks/stripe?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('stripe-signature', sig)
      .set('content-type', 'application/json')
      .send(body);
    assert.equal(badSig.statusCode, 400);
  }

  await seedBaseline();
  {
    const body = JSON.stringify({
      id: 'evt_dup_1',
      type: 'invoice.payment_failed',
      data: { object: { id: 'in_dup_1', number: 'INV-DUP-1', status: 'past_due', created: Math.floor(Date.now() / 1000) } }
    });
    const sig = stripeSig(body, 'whsec_test_local');
    const first = await request(app)
      .post(`/webhooks/stripe?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('stripe-signature', sig)
      .set('content-type', 'application/json')
      .send(body);
    assert.equal(first.statusCode, 200);
    assert.equal(first.body?.ok, true);
    const second = await request(app)
      .post(`/webhooks/stripe?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('stripe-signature', sig)
      .set('content-type', 'application/json')
      .send(body);
    assert.equal(second.statusCode, 200);
    assert.equal(second.body?.duplicate, true);
  }

  await seedBaseline();
  {
    const { loadData, saveDataDebounced, flushDataNow } = require('../src/store/dataStore');
    const data = loadData();
    data.dev = data.dev && typeof data.dev === 'object' ? data.dev : {};
    data.dev.platformBillingStripe = {
      enabled: true,
      secretKey: 'sk_test_platformwebhook',
      publishableKey: '',
      webhookSecret: 'whsec_platform_local',
      accountId: 'acct_platform',
      accountEmail: '',
      accountDisplayName: '',
      connectedAt: Date.now(),
      lastTestedAt: null,
      lastStatus: null,
      lastError: null
    };
    const account = data.accounts?.[ACCOUNT_A_TO];
    assert.ok(account, 'expected account');
    account.billing = account.billing && typeof account.billing === 'object' ? account.billing : {};
    account.billing.pendingCheckout = {
      sessionId: 'cs_platform_1',
      planKey: 'growth',
      cadence: 'annual',
      createdAt: Date.now()
    };
    saveDataDebounced(data);
    await flushDataNow();

    const body = JSON.stringify({
      id: 'evt_platform_checkout_1',
      type: 'checkout.session.completed',
      data: {
        object: {
          id: 'cs_platform_1',
          customer: 'cus_platform_1',
          subscription: 'sub_platform_1',
          payment_status: 'paid',
          metadata: {
            accountId: String(account.accountId || account.id || ''),
            to: ACCOUNT_A_TO,
            planKey: 'growth',
            cadence: 'annual'
          }
        }
      }
    });
    const sig = stripeSig(body, 'whsec_platform_local');
    const res = await request(app)
      .post('/webhooks/stripe/platform')
      .set('stripe-signature', sig)
      .set('content-type', 'application/json')
      .send(body);
    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    const updated = loadData().accounts?.[ACCOUNT_A_TO];
    assert.equal(String(updated?.billing?.platformStripeCustomerId || ''), 'cus_platform_1');
    assert.equal(String(updated?.billing?.platformStripeSubscriptionId || ''), 'sub_platform_1');
    assert.equal(String(updated?.billing?.plan?.key || ''), 'growth');
    assert.equal(String(updated?.billing?.plan?.interval || ''), 'year');
    assert.equal(String(updated?.billing?.plan?.status || ''), 'active');
  }

  console.log('[tests] webhooks integration checks passed');
}

run()
  .then(async () => {
    await shutdown();
  })
  .catch(async (err) => {
    console.error('[tests] failure:', err?.stack || err?.message || err);
    try { await shutdown(); } catch {}
    process.exit(1);
  });
