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
const { loadData, saveDataDebounced, flushDataNow } = require('../src/store/dataStore');
const { pool } = require('../src/db/pool');
const { getByPhone, listByTenant } = require('../src/repositories/contactsRepo');

const ACCOUNT_A_ID = 'acct_10000000001';
const ACCOUNT_B_ID = 'acct_10000000002';

async function seedContacts() {
  const data = loadData();
  data.contacts = {
    [`${ACCOUNT_A_TO}__+18145550100`]: {
      id: `${ACCOUNT_A_TO}__+18145550100`,
      accountId: ACCOUNT_A_ID,
      to: ACCOUNT_A_TO,
      phone: '+18145550100',
      name: 'Taylor Customer',
      tags: ['booked'],
      lifecycle: { leadStatus: 'booked' },
      summary: { notes: 'VIP detail' },
      createdAt: Date.now() - 5_000,
      updatedAt: Date.now() - 2_000
    },
    [`${ACCOUNT_A_TO}__+18145550101`]: {
      id: `${ACCOUNT_A_TO}__+18145550101`,
      accountId: ACCOUNT_A_ID,
      to: ACCOUNT_A_TO,
      phone: '+18145550101',
      name: '',
      tags: [],
      lifecycle: { leadStatus: 'new' },
      summary: { notes: 'Should be filtered unless includeUnqualified=true' },
      createdAt: Date.now() - 4_000,
      updatedAt: Date.now() - 1_000
    },
    [`${ACCOUNT_B_TO}__+18145550999`]: {
      id: `${ACCOUNT_B_TO}__+18145550999`,
      accountId: ACCOUNT_B_ID,
      to: ACCOUNT_B_TO,
      phone: '+18145550999',
      name: 'Other Tenant',
      tags: ['booked'],
      lifecycle: { leadStatus: 'booked' },
      summary: { notes: 'Must never leak' },
      createdAt: Date.now() - 4_000,
      updatedAt: Date.now() - 1_000
    }
  };
  saveDataDebounced(data);
  await flushDataNow();
}

function snapshotContact(to, phone) {
  const data = loadData();
  return data?.contacts?.[`${to}__${phone}`] || null;
}

async function assertDbAndSnapshotMatch(accountId, to, phone) {
  const dbContact = await getByPhone(pool, accountId, phone);
  const snapshot = snapshotContact(to, phone);
  assert.equal(Boolean(dbContact), true);
  assert.equal(Boolean(snapshot), true);
  assert.equal(String(dbContact.phone), String(snapshot.phone));
  assert.equal(String(dbContact.name || ''), String(snapshot.name || ''));
  assert.equal(String(dbContact.accountId), String(snapshot.accountId));
  assert.equal(String(dbContact.to || ''), String(snapshot.to || ''));
}

async function run() {
  const app = await initApp();

  await seedBaseline();
  await seedContacts();

  const agent = request.agent(app);
  const csrf = await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });

  {
    const filtered = await agent
      .get(`/api/contacts?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(filtered.statusCode, 200);
    assert.equal(Array.isArray(filtered.body?.contacts), true);
    assert.equal(filtered.body.contacts.length, 1);
    assert.equal(filtered.body.contacts[0]?.name, 'Taylor Customer');
    assert.equal(filtered.body.contacts[0]?.summary?.notes, 'VIP detail');
  }

  {
    const created = await agent
      .post(`/api/contacts?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({
        contact: {
          phone: '814-555-0199',
          name: 'Fresh Lead'
        }
      });
    assert.equal(created.statusCode, 200);
    assert.equal(created.body?.ok, true);
    assert.equal(created.body?.contact?.phone, '+18145550199');
    assert.equal(created.body?.contact?.name, 'Fresh Lead');

    const listed = await agent
      .get(`/api/contacts?to=${encodeURIComponent(ACCOUNT_A_TO)}&includeUnqualified=true`);
    assert.equal(listed.statusCode, 200);
    const createdContact = listed.body.contacts.find((contact) => String(contact?.phone || '') === '+18145550199');
    assert.equal(Boolean(createdContact), true);

    await assertDbAndSnapshotMatch(ACCOUNT_A_ID, ACCOUNT_A_TO, '+18145550199');
  }

  {
    const updated = await agent
      .post(`/api/contacts?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({
        contact: {
          phone: '+18145550100',
          name: 'Taylor Updated',
          tags: ['booked', 'vip']
        }
      });
    assert.equal(updated.statusCode, 200);
    assert.equal(updated.body?.contact?.name, 'Taylor Updated');

    const dbContact = await getByPhone(pool, ACCOUNT_A_ID, '+18145550100');
    assert.equal(dbContact?.name, 'Taylor Updated');
    assert.deepEqual(dbContact?.tags || [], ['booked', 'vip']);
    await assertDbAndSnapshotMatch(ACCOUNT_A_ID, ACCOUNT_A_TO, '+18145550100');
  }

  {
    const imported = await agent
      .post(`/api/contacts/import?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({
        contacts: [
          { phone: '+18145550100', name: 'Should Not Override Richer Existing Name' },
          { phone: '+18145550101', name: 'Filled From Import' },
          { phone: '+18145550155', name: 'Imported New' },
          { phone: '+18145550156', name: 'Imported New Two' }
        ]
      });
    assert.equal(imported.statusCode, 200);
    assert.equal(imported.body?.imported, 2);
    assert.equal(imported.body?.skipped, 2);
    assert.equal(imported.body?.total, 4);

    const importedRetry = await agent
      .post(`/api/contacts/import?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
      .set('x-csrf-token', csrf)
      .send({
        contacts: [
          { phone: '+18145550100', name: 'Should Not Override Richer Existing Name' },
          { phone: '+18145550101', name: 'Filled From Import' },
          { phone: '+18145550155', name: 'Imported New' },
          { phone: '+18145550156', name: 'Imported New Two' }
        ]
      });
    assert.equal(importedRetry.statusCode, 200);
    assert.equal(importedRetry.body?.imported, 0);
    assert.equal(importedRetry.body?.skipped, 4);
    assert.equal(importedRetry.body?.total, 4);

    const dbContacts = await listByTenant(pool, ACCOUNT_A_ID);
    const phones = dbContacts.map((contact) => String(contact.phone));
    assert.equal(phones.filter((phone) => phone === '+18145550155').length, 1);
    assert.equal(phones.filter((phone) => phone === '+18145550156').length, 1);

    const importedExisting = await getByPhone(pool, ACCOUNT_A_ID, '+18145550101');
    assert.equal(importedExisting?.name, 'Filled From Import');
    const richerExisting = await getByPhone(pool, ACCOUNT_A_ID, '+18145550100');
    assert.equal(richerExisting?.name, 'Taylor Updated');
    await assertDbAndSnapshotMatch(ACCOUNT_A_ID, ACCOUNT_A_TO, '+18145550101');
    await assertDbAndSnapshotMatch(ACCOUNT_A_ID, ACCOUNT_A_TO, '+18145550100');
    await assertDbAndSnapshotMatch(ACCOUNT_A_ID, ACCOUNT_A_TO, '+18145550155');
    await assertDbAndSnapshotMatch(ACCOUNT_A_ID, ACCOUNT_A_TO, '+18145550156');
  }

  {
    const crossTenantCreate = await agent
      .post(`/api/contacts?to=${encodeURIComponent(ACCOUNT_B_TO)}`)
      .set('x-csrf-token', csrf)
      .send({
        contact: {
          phone: '+18145550888',
          name: 'Blocked Cross Tenant'
        }
      });
    assert.equal(crossTenantCreate.statusCode, 404);

    const crossTenantImport = await agent
      .post(`/api/contacts/import?to=${encodeURIComponent(ACCOUNT_B_TO)}`)
      .set('x-csrf-token', csrf)
      .send({
        contacts: [{ phone: '+18145550889', name: 'Blocked Import' }]
      });
    assert.equal(crossTenantImport.statusCode, 404);
  }

  console.log('[tests] contacts integration checks passed');
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
