process.env.ENABLE_PARITY_CHECKS = 'true';
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
  SUPERADMIN_EMAIL,
  SUPERADMIN_PASSWORD
} = require('./_shared');
const { loadData, saveDataDebounced, flushDataNow } = require('../src/store/dataStore');
const { pool } = require('../src/db/pool');
const { withQueryCount } = require('../src/db/queryInstrumentation');
const { handleSignal } = require('../src/services/revenueOrchestrator');
const { getFunnelMetrics, getOpportunityTimeline, persistSnapshotOpportunity } = require('../src/services/opportunitiesService');
const {
  getActivityFeed,
  logActionStartWrite,
  attachActionToOpportunityWrite,
  hasSuccessfulActionByIdempotencyWrite
} = require('../src/services/actionsService');

const ACCOUNT_A_ID = 'acct_10000000001';
const ACCOUNT_B_ID = 'acct_10000000002';

async function countRows(sql, params = []) {
  const result = await pool.query(sql, params);
  return Number(result.rows[0]?.count || 0);
}

async function seedWriteState() {
  const now = Date.now();
  const data = loadData();
  data.conversations = {
    [`${ACCOUNT_A_TO}__+18145550101`]: {
      id: `${ACCOUNT_A_TO}__+18145550101`,
      to: ACCOUNT_A_TO,
      from: '+18145550101',
      accountId: ACCOUNT_A_ID,
      status: 'active',
      stage: 'ask_service',
      messages: [],
      audit: [],
      createdAt: now - 10_000,
      updatedAt: now - 8_000,
      lastActivityAt: now - 8_000
    },
    [`${ACCOUNT_B_TO}__+18145550901`]: {
      id: `${ACCOUNT_B_TO}__+18145550901`,
      to: ACCOUNT_B_TO,
      from: '+18145550901',
      accountId: ACCOUNT_B_ID,
      status: 'active',
      stage: 'ask_service',
      messages: [],
      audit: [],
      createdAt: now - 10_000,
      updatedAt: now - 8_000,
      lastActivityAt: now - 8_000
    }
  };
  data.revenueOpportunities = [
    {
      id: 'opp_pause_a',
      accountId: ACCOUNT_A_ID,
      convoKey: `${ACCOUNT_A_TO}__+18145550101`,
      contactId: 'contact_pause_a',
      status: 'open',
      stage: 'CONTACTED',
      riskScore: 25,
      stageHistory: [],
      riskHistory: [],
      actionLogIds: [],
      stopAutomation: false,
      createdAt: now - 10_000,
      updatedAt: now - 9_000,
      metadata: {}
    },
    {
      id: 'opp_pause_b',
      accountId: ACCOUNT_B_ID,
      convoKey: `${ACCOUNT_B_TO}__+18145550901`,
      contactId: 'contact_pause_b',
      status: 'open',
      stage: 'CONTACTED',
      riskScore: 20,
      stageHistory: [],
      riskHistory: [],
      actionLogIds: [],
      stopAutomation: false,
      createdAt: now - 10_000,
      updatedAt: now - 9_000,
      metadata: {}
    }
  ];
  data.actions = [];
  data.leadEvents = [];
  saveDataDebounced(data);
  await flushDataNow();
}

async function testSignalCreatesOpportunityAndFeedsReads() {
  const leadEvent = {
    id: 'evt_write_signal_1',
    accountId: ACCOUNT_A_ID,
    convoKey: `${ACCOUNT_A_TO}__+18145550177`,
    channel: 'sms',
    type: 'missed_call',
    ts: Date.now() - 5_000,
    payload: { source: 'test_signal' }
  };
  const data = loadData();
  data.leadEvents.push(leadEvent);
  data.conversations[leadEvent.convoKey] = {
    id: leadEvent.convoKey,
    to: ACCOUNT_A_TO,
    from: '+18145550177',
    accountId: ACCOUNT_A_ID,
    status: 'active',
    stage: 'ask_service',
    messages: [],
    audit: [],
    createdAt: Date.now() - 6_000,
    updatedAt: Date.now() - 5_500,
    lastActivityAt: Date.now() - 5_500
  };
  saveDataDebounced(data);
  await flushDataNow();

  await handleSignal(ACCOUNT_A_ID, leadEvent);

  const oppRow = await pool.query('SELECT id, tenant_id FROM opportunities WHERE tenant_id = $1 AND payload->>\'convoKey\' = $2', [ACCOUNT_A_ID, leadEvent.convoKey]);
  assert.equal(oppRow.rowCount, 1);

  const funnel = await getFunnelMetrics(ACCOUNT_A_ID, { identifiers: { route: 'writes/funnel' } });
  assert.equal(funnel.leadsCreated >= 2, true);

  const timeline = await getOpportunityTimeline(ACCOUNT_A_ID, oppRow.rows[0].id, { identifiers: { route: 'writes/timeline' } });
  assert.equal(timeline.opportunityId, oppRow.rows[0].id);
}

async function testSignalSkipActionIdempotencyAndFeedConsistency() {
  const signalId = 'evt_skip_idem_1';
  const first = await logActionStartWrite({
    accountId: ACCOUNT_A_ID,
    opportunityId: 'opp_pause_a',
    convoKey: `${ACCOUNT_A_TO}__+18145550101`,
    actionType: 'do_nothing',
    payload: { skippedReason: 'no_action_required', signalId }
  });
  const second = await logActionStartWrite({
    accountId: ACCOUNT_A_ID,
    opportunityId: 'opp_pause_a',
    convoKey: `${ACCOUNT_A_TO}__+18145550101`,
    actionType: 'do_nothing',
    payload: { skippedReason: 'no_action_required', signalId }
  });
  assert.equal(first.id, second.id);

  const actionCount = await countRows('SELECT COUNT(*)::int AS count FROM actions WHERE tenant_id = $1 AND payload->\'payload\'->>\'signalId\' = $2', [
    ACCOUNT_A_ID,
    signalId
  ]);
  assert.equal(actionCount, 1);

  const feed = await getActivityFeed(ACCOUNT_A_ID, 20, { identifiers: { route: 'writes/activity' } });
  assert.equal(feed.items.some((item) => String(item?.payload?.signalId || item?.payload?.payload?.signalId || '') === signalId), true);
}

async function testPauseResumeRouteAndTenantIsolation(app) {
  const agent = request.agent(app);
  const csrfToken = await login(agent, { email: SUPERADMIN_EMAIL, password: SUPERADMIN_PASSWORD });

  const pauseRes = await agent
    .post(`/api/agent/opportunity/opp_pause_a/pause?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
    .set('x-csrf-token', csrfToken)
    .send({});
  assert.equal(pauseRes.statusCode, 200);
  assert.deepEqual(pauseRes.body, { ok: true, opportunityId: 'opp_pause_a', stopAutomation: true });

  const paused = await pool.query('SELECT payload FROM opportunities WHERE tenant_id = $1 AND id = $2', [ACCOUNT_A_ID, 'opp_pause_a']);
  assert.equal(paused.rows[0].payload.stopAutomation, true);

  const crossTenant = await agent
    .post(`/api/agent/opportunity/opp_pause_b/pause?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
    .set('x-csrf-token', csrfToken)
    .send({});
  assert.equal(crossTenant.statusCode, 404);

  const resumeRes = await agent
    .post(`/api/agent/opportunity/opp_pause_a/resume?to=${encodeURIComponent(ACCOUNT_A_TO)}`)
    .set('x-csrf-token', csrfToken)
    .send({});
  assert.equal(resumeRes.statusCode, 200);
  assert.equal(resumeRes.body.ok, true);
}

async function testActionIdempotencyAndTenantAttachIsolation() {
  const first = await logActionStartWrite({
    accountId: ACCOUNT_A_ID,
    opportunityId: 'opp_pause_a',
    convoKey: `${ACCOUNT_A_TO}__+18145550101`,
    runId: 'run_write_1',
    stepId: 'step_1',
    actionType: 'send_message',
    payload: { messageText: 'hello', runId: 'run_write_1' }
  });
  const second = await logActionStartWrite({
    accountId: ACCOUNT_A_ID,
    opportunityId: 'opp_pause_a',
    convoKey: `${ACCOUNT_A_TO}__+18145550101`,
    runId: 'run_write_1',
    stepId: 'step_1',
    actionType: 'send_message',
    payload: { messageText: 'hello', runId: 'run_write_1' }
  });
  assert.equal(first.id, second.id);

  const successful = await hasSuccessfulActionByIdempotencyWrite(ACCOUNT_A_ID, `run:${ACCOUNT_A_ID}:run_write_1:step_1:send_message`);
  assert.equal(successful, false);

  const runActionCount = await countRows('SELECT COUNT(*)::int AS count FROM actions WHERE tenant_id = $1 AND idempotency_key = $2', [
    ACCOUNT_A_ID,
    `run:${ACCOUNT_A_ID}:run_write_1:step_1:send_message`
  ]);
  assert.equal(runActionCount, 1);

  const attachResult = await attachActionToOpportunityWrite(ACCOUNT_A_ID, 'opp_pause_b', first.id);
  assert.equal(attachResult, null);
}

async function testWriteQueryDiscipline() {
  const writeOpportunity = loadData().revenueOpportunities.find((item) => item.id === 'opp_pause_a');
  writeOpportunity.riskScore = 42;
  writeOpportunity.updatedAt = Date.now();

  const opportunityQueries = await withQueryCount(pool, async (db) =>
    persistSnapshotOpportunity(ACCOUNT_A_ID, writeOpportunity, { operation: 'query_count_write', db })
  );
  assert.equal(opportunityQueries.count <= 6, true);

  const actionQueries = await withQueryCount(pool, async (db) =>
    logActionStartWrite({
      accountId: ACCOUNT_A_ID,
      opportunityId: 'opp_pause_a',
      convoKey: `${ACCOUNT_A_TO}__+18145550101`,
      actionType: 'create_alert',
      payload: { alertId: 'alert_query_count' }
    }, { db })
  );
  assert.equal(actionQueries.count, 1);
}

(async () => {
  const app = await initApp();
  try {
    await seedBaseline();
    await seedWriteState();
    await testSignalCreatesOpportunityAndFeedsReads();
    await testSignalSkipActionIdempotencyAndFeedConsistency();
    await testPauseResumeRouteAndTenantIsolation(app);
    await testActionIdempotencyAndTenantAttachIsolation();
    await testWriteQueryDiscipline();
    console.log('[tests] opportunities/actions writes integration checks passed');
  } finally {
    await shutdown();
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
