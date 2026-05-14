const assert = require('node:assert/strict');
const {
  request,
  initApp,
  seedBaseline,
  login,
  shutdown,
  ACCOUNT_A_TO,
  ACCOUNT_B_TO,
  OWNER_EMAIL,
  OWNER_PASSWORD
} = require('./_shared');

async function run() {
  const app = await initApp();

  await seedBaseline();
  {
    const agent = request.agent(app);
    const csrf = await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });

    const billingOther = await agent
      .get(`/api/billing/summary?to=${encodeURIComponent(ACCOUNT_B_TO)}`);
    assert.equal(billingOther.statusCode, 404);

    const convosOther = await agent
      .get(`/api/conversations?to=${encodeURIComponent(ACCOUNT_B_TO)}`);
    assert.equal(convosOther.statusCode, 404);

    const analyticsOther = await agent
      .get(`/api/analytics/summary?to=${encodeURIComponent(ACCOUNT_B_TO)}`);
    assert.equal(analyticsOther.statusCode, 404);

    const notificationsOther = await agent
      .get(`/api/notifications/settings?to=${encodeURIComponent(ACCOUNT_B_TO)}`);
    assert.equal(notificationsOther.statusCode, 404);

    const writeOther = await agent
      .patch(`/api/billing/details?to=${encodeURIComponent(ACCOUNT_B_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ billingEmail: 'blocked@example.com' });
    assert.equal(writeOther.statusCode, 404);

    const allowedSelf = await agent
      .get(`/api/billing/summary?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(allowedSelf.statusCode, 200);
  }

  console.log('[tests] tenant isolation integration checks passed');
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

