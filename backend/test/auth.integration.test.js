const assert = require('node:assert/strict');
const {
  request,
  initApp,
  seedBaseline,
  shutdown
} = require('./_shared');

async function run() {
  const app = await initApp();

  {
    const ready = await request(app).get('/ready');
    assert.equal(ready.statusCode, 200);
    assert.equal(ready.body?.ok, true);
  }

  await seedBaseline();
  {
    const badPayload = await request(app)
      .post('/api/auth/login')
      .set('x-forwarded-for', '10.2.0.1')
      .send({});
    assert.equal(badPayload.statusCode, 400);

    let saw429 = false;
    const attempts = process.env.DEV_MODE === 'true' ? 340 : 20;
    for (let i = 0; i < attempts; i += 1) {
      const res = await request(app)
        .post('/api/auth/login')
        .set('x-forwarded-for', '10.2.0.2')
        .send({ email: 'nobody@example.com', password: 'wrong' });
      if (res.statusCode === 429) {
        saw429 = true;
        break;
      }
    }
    assert.equal(saw429, true);
  }

  console.log('[tests] auth integration checks passed');
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
