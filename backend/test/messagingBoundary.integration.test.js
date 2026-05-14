process.env.USE_DB_OPPORTUNITIES = 'true';
process.env.USE_DB_ACTIONS = 'true';

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
const { ensureSchedulingConfig, listAvailability } = require('../src/services/publicBookingService');
const {
  recordInboundSms,
  recordMissedCall,
  recordOutboundAttempt,
  updateConversationStatusLegacy
} = require('../src/services/messagingBoundaryService');
const { persistSnapshotOpportunity, getOpportunityTimeline } = require('../src/services/opportunitiesService');

const ACCOUNT_A_ID = 'acct_10000000001';
const ACCOUNT_B_ID = 'acct_10000000002';

function convoKey(to, from) {
  return `${to}__${from}`;
}

function uniqueKey(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

async function seedMessagingFixtures() {
  const now = Date.now();
  const data = loadData();
  data.conversations = {
    [convoKey(ACCOUNT_A_TO, '+18145550101')]: {
      id: convoKey(ACCOUNT_A_TO, '+18145550101'),
      to: ACCOUNT_A_TO,
      from: '+18145550101',
      accountId: ACCOUNT_A_ID,
      status: 'new',
      stage: 'ask_service',
      messages: [],
      audit: [],
      leadData: {},
      createdAt: now - 10_000,
      updatedAt: now - 10_000,
      lastActivityAt: now - 10_000
    },
    [convoKey(ACCOUNT_B_TO, '+18145550901')]: {
      id: convoKey(ACCOUNT_B_TO, '+18145550901'),
      to: ACCOUNT_B_TO,
      from: '+18145550901',
      accountId: ACCOUNT_B_ID,
      status: 'new',
      stage: 'ask_service',
      messages: [],
      audit: [],
      leadData: {},
      createdAt: now - 10_000,
      updatedAt: now - 10_000,
      lastActivityAt: now - 10_000
    }
  };
  data.leadEvents = [];
  data.revenueEvents = [];
  data.actions = [];
  saveDataDebounced(data);
  await flushDataNow();

  await persistSnapshotOpportunity(ACCOUNT_A_ID, {
    id: 'opp_msg_boundary_a',
    accountId: ACCOUNT_A_ID,
    convoKey: convoKey(ACCOUNT_A_TO, '+18145550101'),
    contactId: 'contact_boundary_a',
    status: 'open',
    stage: 'NEW',
    riskScore: 10,
    stageHistory: [],
    riskHistory: [],
    actionLogIds: [],
    createdAt: now - 9_000,
    updatedAt: now - 9_000,
    metadata: {}
  }, { operation: 'messaging_boundary_seed' });
}

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
  account.scheduling.publicToken = String(token || 'messaging_boundary_booking');
  account.scheduling.mode = 'internal';
  account.scheduling.slotIntervalMin = 30;
  account.scheduling.leadTimeMin = 0;
  account.scheduling.bufferMin = 0;
  account.scheduling.maxBookingsPerDay = 0;
  account.internalBookings = [];
  account.calendarEvents = [];
  saveDataDebounced(data);
  await flushDataNow();
  const availability = listAvailability(account, { days: 7, durationMin: 60 });
  const day = (availability.days || []).find((entry) => Array.isArray(entry?.slots) && entry.slots.length > 0);
  assert.ok(day, 'Expected at least one open day');
  const slot = day.slots[0];
  return {
    token: account.scheduling.publicToken,
    start: Number(slot.start),
    end: Number(slot.end)
  };
}

async function testInboundSmsRouteAndTimeline(app) {
  await seedBaseline();
  await seedMessagingFixtures();
  const messageSid = uniqueKey('SM_BOUNDARY');

  const smsRes = await request(app)
    .post('/webhooks/sms')
    .set('x-dev-webhook-secret', process.env.WEBHOOK_DEV_SECRET)
    .send({
      From: '+18145550101',
      To: ACCOUNT_A_TO,
      Body: 'Need help with a quote',
      MessageSid: messageSid
    });
  assert.equal(smsRes.statusCode, 200);
  assert.equal(smsRes.body?.ok, true);

  const convo = loadData().conversations[convoKey(ACCOUNT_A_TO, '+18145550101')];
  assert.ok(convo, 'Expected inbound SMS conversation to exist');
  assert.equal(convo.messages.length, 1);
  assert.equal(convo.messages[0]?.text, 'Need help with a quote');

  const leadEvents = loadData().leadEvents.filter((event) => String(event?.convoKey || '') === convoKey(ACCOUNT_A_TO, '+18145550101'));
  assert.equal(leadEvents.length > 0, true);
}

async function testBoundaryIdempotencyAndOrdering() {
  await seedBaseline();
  await seedMessagingFixtures();

  const tenant = { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO };
  await recordInboundSms({
    tenant,
    to: ACCOUNT_A_TO,
    from: '+18145550101',
    text: 'first inbound',
    eventKey: 'sms:boundary:duplicate',
    route: 'test'
  });
  await recordInboundSms({
    tenant,
    to: ACCOUNT_A_TO,
    from: '+18145550101',
    text: 'first inbound',
    eventKey: 'sms:boundary:duplicate',
    route: 'test'
  });

  const fixedNow = Date.now();
  const originalNow = Date.now;
  Date.now = () => fixedNow;
  try {
    await recordOutboundAttempt({
      tenant,
      to: ACCOUNT_A_TO,
      from: '+18145550101',
      text: 'outbound one',
      source: 'test_outbound',
      meta: { actionId: 'action_msg_boundary_1' },
      route: 'test',
      requireExisting: true
    });
    await recordOutboundAttempt({
      tenant,
      to: ACCOUNT_A_TO,
      from: '+18145550101',
      text: 'outbound one',
      source: 'test_outbound',
      meta: { actionId: 'action_msg_boundary_1' },
      route: 'test',
      requireExisting: true
    });
  } finally {
    Date.now = originalNow;
  }

  const convo = loadData().conversations[convoKey(ACCOUNT_A_TO, '+18145550101')];
  assert.equal(convo.messages.length, 2);
  assert.equal(convo.messages[0].text, 'first inbound');
  assert.equal(convo.messages[1].text, 'outbound one');
  const firstSequence = Number(convo.messages[0]?.meta?.orderingSequence || convo.messages[0]?.orderingSequence || 0);
  const secondSequence = Number(convo.messages[1]?.meta?.orderingSequence || convo.messages[1]?.orderingSequence || 0);
  assert.equal(firstSequence < secondSequence, true);
}

async function testMissedCallBoundaryIdempotency() {
  await seedBaseline();
  await seedMessagingFixtures();

  const tenant = { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO };
  await recordMissedCall({
    tenant,
    to: ACCOUNT_A_TO,
    from: '+18145550101',
    eventKey: 'call:boundary:1',
    route: 'test'
  });
  await recordMissedCall({
    tenant,
    to: ACCOUNT_A_TO,
    from: '+18145550101',
    eventKey: 'call:boundary:1',
    route: 'test'
  });

  const convo = loadData().conversations[convoKey(ACCOUNT_A_TO, '+18145550101')];
  const missedCallAudits = convo.audit.filter((entry) => entry.type === 'missed_call');
  assert.equal(missedCallAudits.length, 1);
  assert.equal(convo.status, 'active');
}

async function testManualSendAndTenantSafety(app) {
  await seedBaseline();
  await seedMessagingFixtures();

  const agent = request.agent(app);
  const csrf = await login(agent, { email: OWNER_EMAIL, password: OWNER_PASSWORD });

  const sendRes = await agent
    .post(`/api/conversations/${encodeURIComponent(convoKey(ACCOUNT_A_TO, '+18145550101'))}/send?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
    .set('x-csrf-token', csrf)
    .send({ text: 'Manual follow-up from dashboard' });
  assert.equal(sendRes.statusCode, 200);
  assert.equal(sendRes.body?.ok, true);

  const convo = loadData().conversations[convoKey(ACCOUNT_A_TO, '+18145550101')];
  assert.equal(convo.messages.some((message) => message.text === 'Manual follow-up from dashboard'), true);

  const wrongTenant = await agent
    .post(`/api/conversations/${encodeURIComponent(convoKey(ACCOUNT_B_TO, '+18145550901'))}/send?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
    .set('x-csrf-token', csrf)
    .send({ text: 'Should not cross tenants' });
  assert.equal(wrongTenant.statusCode, 404);

  const crossTenantMutation = await updateConversationStatusLegacy({
    tenant: { accountId: ACCOUNT_A_ID, to: ACCOUNT_B_TO },
    to: ACCOUNT_B_TO,
    from: '+18145550901',
    status: 'closed',
    source: 'cross_tenant_test',
    route: 'test',
    requireExisting: true
  });
  assert.equal(crossTenantMutation, null);
  assert.equal(loadData().conversations[convoKey(ACCOUNT_B_TO, '+18145550901')].status, 'new');
}

async function testPublicBookingBoundary(app) {
  await seedBaseline();
  const { token, start, end } = await setupBookingWorkspace(uniqueKey('messaging_boundary_booking'));

  const bookingRes = await request(app)
    .post(`/api/public/booking/${encodeURIComponent(token)}/book`)
    .send({
      customerName: 'Boundary Booker',
      customerPhone: '+18145550133',
      customerEmail: 'boundary@example.com',
      serviceName: 'Full Detail',
      notes: 'Needs pickup',
      start,
      end
    });
  assert.equal(bookingRes.statusCode, 200);
  assert.equal(bookingRes.body?.ok, true);

  const convo = loadData().conversations[convoKey(ACCOUNT_A_TO, '+18145550133')];
  assert.ok(convo, 'Expected booking sync conversation to exist');
  assert.equal(convo.status, 'booked');
  assert.equal(convo.messages.length, 1);
  assert.equal(String(convo.messages[0]?.meta?.source || ''), 'public_booking');
}

(async () => {
  const app = await initApp();
  try {
    await testInboundSmsRouteAndTimeline(app);
    await testBoundaryIdempotencyAndOrdering();
    await testMissedCallBoundaryIdempotency();
    await testManualSendAndTenantSafety(app);
    await testPublicBookingBoundary(app);
    console.log('[tests] messaging boundary integration checks passed');
  } finally {
    await shutdown();
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
