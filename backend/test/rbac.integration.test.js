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
  OWNER_PASSWORD,
  SUPERADMIN_EMAIL,
  SUPERADMIN_PASSWORD
} = require('./_shared');

async function run() {
  const app = await initApp();

  await seedBaseline();
  {
    const agent = request.agent(app);
    const csrf = await login(agent, { email: SUPERADMIN_EMAIL, password: SUPERADMIN_PASSWORD });

    const missingCsrf = await agent
      .patch(`/api/dev/settings?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .send({ verboseTenantLogs: true });
    assert.equal(missingCsrf.statusCode, 403);

    const withCsrf = await agent
      .patch(`/api/dev/settings?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ verboseTenantLogs: true });
    assert.equal(withCsrf.statusCode, 200);
    assert.equal(withCsrf.body?.ok, true);
  }

  await seedBaseline();
  {
    const agent = request.agent(app);
    const csrf = await login(agent, { email: SUPERADMIN_EMAIL, password: SUPERADMIN_PASSWORD });

    const missingCsrf = await agent
      .post('/api/admin/accounts')
      .send({ to: ACCOUNT_A_TO });
    assert.equal(missingCsrf.statusCode, 403);

    const badAdminPayload = await agent
      .post('/api/admin/accounts')
      .set('x-csrf-token', csrf)
      .send({ to: 'not-a-number' });
    assert.equal(badAdminPayload.statusCode, 400);
  }

  await seedBaseline();
  {
    const agent = request.agent(app);
    const csrf = await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });

    const badRules = await agent
      .post(`/api/rules?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ rules: 'nope' });
    assert.equal(badRules.statusCode, 400);

    const badNotifications = await agent
      .get(`/api/notifications/log?to=${encodeURIComponent(ACCOUNT_A_TO)}&limit=9999`);
    assert.ok([400, 403].includes(badNotifications.statusCode));

    const badAgentStart = await agent
      .post(`/api/agent/start?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({ opportunityId: 'opp_1', mode: 'INVALID_MODE' });
    assert.ok([400, 403].includes(badAgentStart.statusCode));

    const badAdminStripeTestBody = await agent
      .post('/api/admin/developer/platform-billing/stripe/test')
      .set('x-csrf-token', csrf)
      .send({ unexpected: true });
    assert.ok([400, 403].includes(badAdminStripeTestBody.statusCode));
  }

  await seedBaseline();
  {
    const agent = request.agent(app);
    await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });
    const res = await agent.get(`/api/account?to=${encodeURIComponent(ACCOUNT_B_TO)}`);
    assert.equal(res.statusCode, 404);
  }

  console.log('[tests] rbac integration checks passed');
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
