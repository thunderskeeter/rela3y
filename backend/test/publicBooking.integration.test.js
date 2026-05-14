const assert = require('node:assert/strict');
const {
  request,
  initApp,
  seedBaseline,
  shutdown,
  ACCOUNT_A_TO
} = require('./_shared');

const { loadData, saveDataDebounced, flushDataNow } = require('../src/store/dataStore');
const { ensureSchedulingConfig, listAvailability } = require('../src/services/publicBookingService');

async function setupBookingWorkspace(token) {
  const data = loadData();
  const account = data?.accounts?.[ACCOUNT_A_TO];
  assert.ok(account, 'Expected baseline account to exist');
  ensureSchedulingConfig(account);
  account.workspace = account.workspace || {};
  account.workspace.timezone = 'America/New_York';
  account.workspace.businessHours = {
    mon: [{ start: '09:00', end: '17:00' }],
    tue: [{ start: '09:00', end: '17:00' }],
    wed: [{ start: '09:00', end: '17:00' }],
    thu: [{ start: '09:00', end: '17:00' }],
    fri: [{ start: '09:00', end: '17:00' }],
    sat: [],
    sun: []
  };
  account.scheduling.publicToken = String(token || 'public_booking_test_token');
  account.scheduling.mode = 'internal';
  account.scheduling.slotIntervalMin = 30;
  account.scheduling.leadTimeMin = 0;
  account.scheduling.bufferMin = 0;
  account.scheduling.maxBookingsPerDay = 0;
  account.internalBookings = [];
  account.calendarEvents = [];
  saveDataDebounced(data);
  await flushDataNow();
  // Use a wider window so this stays deterministic across weekday/weekend runtime.
  const availability = listAvailability(account, { days: 7, durationMin: 60 });
  const day = (availability.days || []).find((d) => Array.isArray(d?.slots) && d.slots.length > 0);
  assert.ok(day, 'Expected at least one open day');
  const slot = day.slots[0];
  assert.ok(slot, 'Expected at least one slot');
  return {
    token: account.scheduling.publicToken,
    slot: { start: Number(slot.start), end: Number(slot.end) },
    timezone: String(availability.timezone || '')
  };
}

async function run() {
  const app = await initApp();

  await seedBaseline();
  {
    const { token, slot, timezone } = await setupBookingWorkspace('public_booking_reliability');
    const config = await request(app).get(`/api/public/booking/${encodeURIComponent(token)}/config`);
    assert.equal(config.statusCode, 200);
    assert.equal(config.body?.ok, true);
    assert.equal(String(config.body?.timezone || ''), timezone);

    const invalid = await request(app)
      .post(`/api/public/booking/${encodeURIComponent(token)}/book`)
      .send({
        customerName: 'Slot Drift',
        customerPhone: '+18145550111',
        customerEmail: 'slot-drift@example.com',
        serviceName: 'Detail',
        notes: '',
        start: slot.start + (5 * 60 * 1000),
        end: slot.end + (5 * 60 * 1000)
      });
    assert.equal(invalid.statusCode, 409);
    assert.equal(String(invalid.body?.code || ''), 'invalid_slot');
    assert.ok(Array.isArray(invalid.body?.nextAvailable), 'Expected nextAvailable list for invalid slot');

    const first = await request(app)
      .post(`/api/public/booking/${encodeURIComponent(token)}/book`)
      .send({
        customerName: 'Jane Primary',
        customerPhone: '+18145550101',
        customerEmail: 'jane@example.com',
        serviceName: 'Full Detail',
        notes: 'test booking',
        start: slot.start,
        end: slot.end
      });
    assert.equal(first.statusCode, 200);
    assert.equal(first.body?.ok, true);
    const bookingId = String(first.body?.booking?.id || '');
    assert.ok(bookingId.startsWith('bk_'));

    const duplicate = await request(app)
      .post(`/api/public/booking/${encodeURIComponent(token)}/book`)
      .send({
        customerName: 'Jane Primary',
        customerPhone: '+18145550101',
        customerEmail: 'jane@example.com',
        serviceName: 'Full Detail',
        notes: 'test booking',
        start: slot.start,
        end: slot.end
      });
    assert.equal(duplicate.statusCode, 200);
    assert.equal(duplicate.body?.ok, true);
    assert.equal(duplicate.body?.duplicate, true);
    assert.equal(String(duplicate.body?.booking?.id || ''), bookingId);

    const conflict = await request(app)
      .post(`/api/public/booking/${encodeURIComponent(token)}/book`)
      .send({
        customerName: 'John Conflict',
        customerPhone: '+18145550199',
        customerEmail: 'john@example.com',
        serviceName: 'Interior',
        notes: 'same slot',
        start: slot.start,
        end: slot.end
      });
    assert.equal(conflict.statusCode, 409);
    assert.equal(String(conflict.body?.code || ''), 'slot_conflict');
    assert.ok(Array.isArray(conflict.body?.nextAvailable), 'Expected nextAvailable list for slot conflict');
  }

  console.log('[tests] public booking reliability checks passed');
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
