const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
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

async function runPhase1() {
  const marker = `tenant-audit-${Date.now()}`;
  const app = await initApp();
  await seedBaseline();

  const agent = request.agent(app);
  const csrf = await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });

  const saveRes = await agent
    .patch(`/api/account/workspace?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
    .set('x-csrf-token', csrf)
    .send({ workspace: { identity: { businessName: marker } } });
  assert.equal(saveRes.statusCode, 200, `phase1 save failed: ${saveRes.statusCode}`);

  await shutdown();

  const child = spawnSync(
    process.execPath,
    [path.resolve(__filename), '--phase2', marker],
    { stdio: 'inherit', env: process.env }
  );
  if (child.status !== 0) {
    throw new Error(`phase2 failed with exit code ${child.status}`);
  }

  console.log('[tests] tenant leak audit passed');
}

async function runPhase2(marker) {
  const app = await initApp();
  const agent = request.agent(app);
  await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });

  // Persistence check after fresh process startup.
  const accountA = await agent.get(`/api/account?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
  assert.equal(accountA.statusCode, 200, `phase2 account A read failed: ${accountA.statusCode}`);
  assert.equal(
    String(accountA.body?.account?.workspace?.identity?.businessName || ''),
    String(marker),
    'persisted marker mismatch after restart'
  );

  // Tenant isolation matrix for key dashboard APIs.
  const checks = [
    { name: 'account', method: 'get', path: `/api/account?to=${encodeURIComponent(ACCOUNT_B_TO)}` },
    { name: 'conversations', method: 'get', path: `/api/conversations?to=${encodeURIComponent(ACCOUNT_B_TO)}` },
    { name: 'contacts', method: 'get', path: `/api/contacts?to=${encodeURIComponent(ACCOUNT_B_TO)}` },
    { name: 'analytics_summary', method: 'get', path: `/api/analytics/summary?to=${encodeURIComponent(ACCOUNT_B_TO)}` },
    { name: 'analytics_funnel', method: 'get', path: `/api/analytics/funnel?to=${encodeURIComponent(ACCOUNT_B_TO)}` },
    { name: 'billing_summary', method: 'get', path: `/api/billing/summary?to=${encodeURIComponent(ACCOUNT_B_TO)}` },
    { name: 'notifications_settings', method: 'get', path: `/api/notifications/settings?to=${encodeURIComponent(ACCOUNT_B_TO)}` },
    { name: 'rules', method: 'get', path: `/api/rules?to=${encodeURIComponent(ACCOUNT_B_TO)}` },
    { name: 'flows', method: 'get', path: `/api/flows?to=${encodeURIComponent(ACCOUNT_B_TO)}` }
  ];

  for (const check of checks) {
    const res = await agent[check.method](check.path);
    assert.equal(
      res.statusCode,
      404,
      `tenant isolation failed for ${check.name}: expected 404, got ${res.statusCode}`
    );
  }

  await shutdown();
}

const isPhase2 = process.argv[2] === '--phase2';
const markerArg = process.argv[3];

(isPhase2 ? runPhase2(markerArg) : runPhase1())
  .catch(async (err) => {
    console.error('[tests] tenant leak audit failed:', err?.stack || err?.message || err);
    try { await shutdown(); } catch {}
    process.exit(1);
  });

