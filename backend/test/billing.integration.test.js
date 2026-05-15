const assert = require('node:assert/strict');
const {
  request,
  initApp,
  seedBaseline,
  login,
  shutdown,
  ACCOUNT_A_TO,
  OWNER_EMAIL,
  OWNER_PASSWORD
} = require('./_shared');

const { loadData, saveDataDebounced, flushDataNow } = require('../src/store/dataStore');
const { applyStripeWebhookEventForTo } = require('../src/services/stripeIntegrationService');
const { canAccountAccessProduct } = require('../src/services/billingPolicyService');

async function run() {
  const app = await initApp();

  await seedBaseline();
  {
    const agent = request.agent(app);
    const csrf = await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });

    const trial = await agent
      .post(`/api/billing/trial/start?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ days: 14 });
    assert.equal(trial.statusCode, 200);
    assert.equal(String(trial.body?.plan?.status || ''), 'trialing');
    assert.ok(Number(trial.body?.plan?.trialEndsAt || 0) > Date.now());

    const up = await agent
      .patch(`/api/billing/plan?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ planKey: 'growth' });
    assert.equal(up.statusCode, 200);
    assert.equal(String(up.body?.plan?.key || ''), 'growth');

    const down = await agent
      .patch(`/api/billing/plan?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ planKey: 'starter' });
    assert.equal(down.statusCode, 200);
    assert.equal(String(down.body?.plan?.key || ''), 'starter');
  }

  await seedBaseline();
  {
    const data = loadData();
    const account = data.accounts?.[ACCOUNT_A_TO];
    assert.ok(account, 'expected account');
    account.billing = account.billing && typeof account.billing === 'object' ? account.billing : {};
    account.billing.isLive = true;
    account.billing.provider = 'stripe';
    account.billing.plan = account.billing.plan && typeof account.billing.plan === 'object' ? account.billing.plan : {};
    account.billing.plan.status = 'active';
    account.billing.dunning = {
      enabled: true,
      maxAttempts: 2,
      attempts: 0,
      retryCadenceHours: [1, 1]
    };
    saveDataDebounced(data);
    await flushDataNow();

    applyStripeWebhookEventForTo(ACCOUNT_A_TO, {
      id: 'evt_fail_1',
      type: 'invoice.payment_failed',
      data: { object: { id: 'in_fail_1', number: 'INV-FAIL-1', status: 'past_due', amount_due: 12900, created: Math.floor(Date.now() / 1000) } }
    });
    let updated = loadData().accounts?.[ACCOUNT_A_TO];
    assert.equal(String(updated?.billing?.plan?.status || ''), 'past_due');

    applyStripeWebhookEventForTo(ACCOUNT_A_TO, {
      id: 'evt_fail_2',
      type: 'invoice.payment_failed',
      data: { object: { id: 'in_fail_2', number: 'INV-FAIL-2', status: 'unpaid', amount_due: 12900, created: Math.floor(Date.now() / 1000) } }
    });
    updated = loadData().accounts?.[ACCOUNT_A_TO];
    assert.equal(String(updated?.billing?.plan?.status || ''), 'unpaid');
    assert.ok(Number(updated?.billing?.dunning?.lockedAt || 0) > 0);

    assert.equal(canAccountAccessProduct(updated, Date.now()), false);

    const agent = request.agent(app);
    const denied = await agent
      .post('/api/auth/login')
      .send({ email: OWNER_EMAIL, password: OWNER_PASSWORD, useCookie: true });
    if (String(process.env.DEV_MODE || '').toLowerCase() === 'true') {
      assert.equal(denied.statusCode, 200);
    } else {
      assert.equal(denied.statusCode, 403);
      assert.equal(String(denied.body?.reason || ''), 'payment_unpaid');
    }

    const invoices = Array.isArray(updated?.billing?.invoices) ? updated.billing.invoices : [];
    assert.ok(invoices.some((x) => String(x?.id || '') === 'in_fail_1'));
    assert.ok(invoices.some((x) => String(x?.id || '') === 'in_fail_2'));
  }

  await seedBaseline();
  {
    const data = loadData();
    const account = data.accounts?.[ACCOUNT_A_TO];
    assert.ok(account, 'expected account');
    account.customerBilling = {
      invoices: [{
        id: 'cinv_test_paid',
        invoiceNumber: 'INV-CUST-1',
        conversationId: `${ACCOUNT_A_TO}__+18145550123`,
        amountCents: 25000,
        paymentStatus: 'open',
        payment: {
          provider: 'stripe_checkout',
          checkoutSessionId: 'cs_test_old',
          status: 'open',
          url: 'https://checkout.stripe.com/c/pay/test'
        }
      }]
    };
    saveDataDebounced(data);
    await flushDataNow();

    applyStripeWebhookEventForTo(ACCOUNT_A_TO, {
      id: 'evt_checkout_paid_1',
      type: 'checkout.session.completed',
      data: {
        object: {
          id: 'cs_test_paid',
          status: 'complete',
          payment_status: 'paid',
          payment_intent: 'pi_test_paid',
          metadata: {
            invoiceId: 'cinv_test_paid',
            accountId: String(account.accountId || account.id || ''),
            to: ACCOUNT_A_TO
          }
        }
      }
    });

    const updated = loadData().accounts?.[ACCOUNT_A_TO];
    const invoice = updated?.customerBilling?.invoices?.find((x) => String(x?.id || '') === 'cinv_test_paid');
    assert.equal(String(invoice?.paymentStatus || ''), 'paid');
    assert.equal(String(invoice?.payment?.status || ''), 'paid');
    assert.equal(String(invoice?.payment?.paymentIntentId || ''), 'pi_test_paid');
  }

  console.log('[tests] billing integration checks passed');
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
