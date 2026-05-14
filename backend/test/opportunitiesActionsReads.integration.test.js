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
const { getFunnelMetrics, getOpportunityTimeline } = require('../src/services/opportunitiesService');
const { getActivityFeed, getPlaybookPerformance } = require('../src/services/actionsService');

const ACCOUNT_A_ID = 'acct_10000000001';
const ACCOUNT_B_ID = 'acct_10000000002';
const DAY_MS = 24 * 60 * 60 * 1000;

function seedAnalyticsState() {
  const now = Date.now();
  const base = now - (2 * DAY_MS);
  const data = loadData();

  data.conversations = {
    [`${ACCOUNT_A_TO}__+18145550111`]: {
      to: ACCOUNT_A_TO,
      from: '+18145550111',
      accountId: ACCOUNT_A_ID,
      status: 'booked',
      stage: 'booked',
      messages: [],
      audit: [],
      createdAt: base + 500,
      updatedAt: base + 4000,
      lastActivityAt: base + 6000
    },
    [`${ACCOUNT_A_TO}__+18145550112`]: {
      to: ACCOUNT_A_TO,
      from: '+18145550112',
      accountId: ACCOUNT_A_ID,
      status: 'new',
      stage: 'ask_service',
      messages: [],
      audit: [],
      createdAt: base + 1000,
      updatedAt: base + 4500,
      lastActivityAt: base + 4500
    },
    [`${ACCOUNT_A_TO}__+18145550113`]: {
      to: ACCOUNT_A_TO,
      from: '+18145550113',
      accountId: ACCOUNT_A_ID,
      status: 'open',
      stage: 'detect_intent',
      messages: [],
      audit: [],
      createdAt: base + 1200,
      updatedAt: base + 8000,
      lastActivityAt: base + 8000
    },
    [`${ACCOUNT_B_TO}__+18145550999`]: {
      to: ACCOUNT_B_TO,
      from: '+18145550999',
      accountId: ACCOUNT_B_ID,
      status: 'booked',
      stage: 'closed',
      messages: [],
      audit: [],
      createdAt: base + 700,
      updatedAt: base + 4700,
      lastActivityAt: base + 4700
    }
  };

  data.revenueOpportunities = [
    {
      id: 'opp_a_1',
      accountId: ACCOUNT_A_ID,
      convoKey: `${ACCOUNT_A_TO}__+18145550111`,
      contactId: 'contact_a_1',
      status: 'recovered',
      stage: 'booked',
      confidence: 0.85,
      riskScore: 40,
      estimatedValueCents: 25000,
      stageHistory: [{ stage: 'NEW', ts: base + 500 }],
      riskHistory: [{ riskScore: 10, ts: base + 500 }, { riskScore: 40, ts: base + 3000 }],
      createdAt: base + 500,
      updatedAt: base + 4000
    },
    {
      id: 'opp_a_2',
      accountId: ACCOUNT_A_ID,
      convoKey: `${ACCOUNT_A_TO}__+18145550112`,
      contactId: 'contact_a_2',
      status: 'open',
      stage: 'ask_service',
      confidence: 0.2,
      riskScore: 30,
      stageHistory: [{ stage: 'NEW', ts: base + 1000 }],
      riskHistory: [{ riskScore: 30, ts: base + 1000 }],
      createdAt: base + 1000,
      updatedAt: base + 4500
    },
    {
      id: 'opp_a_3',
      accountId: ACCOUNT_A_ID,
      convoKey: `${ACCOUNT_A_TO}__+18145550113`,
      contactId: 'contact_a_3',
      status: 'open',
      stage: 'detect_intent',
      confidence: 0.6,
      riskScore: 35,
      stageHistory: [{ stage: 'NEW', ts: base + 1200 }, { stage: 'detect_intent', ts: base + 8000 }],
      riskHistory: [{ riskScore: 15, ts: base + 1200 }, { riskScore: 35, ts: base + 8000 }],
      createdAt: base + 1200,
      updatedAt: base + 8000
    },
    {
      id: 'opp_b_1',
      accountId: ACCOUNT_B_ID,
      convoKey: `${ACCOUNT_B_TO}__+18145550999`,
      contactId: 'contact_b_1',
      status: 'won',
      stage: 'closed',
      confidence: 0.9,
      riskScore: 55,
      createdAt: base + 700,
      updatedAt: base + 4700
    }
  ];

  data.leadEvents = [
    {
      id: 'lead_a1_inbound',
      accountId: ACCOUNT_A_ID,
      convoKey: `${ACCOUNT_A_TO}__+18145550111`,
      type: 'inbound_message',
      channel: 'sms',
      ts: base + 1100,
      payload: { text: 'Need a booking' }
    },
    {
      id: 'lead_a1_completed',
      accountId: ACCOUNT_A_ID,
      convoKey: `${ACCOUNT_A_TO}__+18145550111`,
      type: 'booking_completed',
      channel: 'sms',
      ts: base + 6000,
      payload: { text: 'Booked' }
    },
    {
      id: 'lead_a2_inbound',
      accountId: ACCOUNT_A_ID,
      convoKey: `${ACCOUNT_A_TO}__+18145550112`,
      type: 'inbound_message',
      channel: 'sms',
      ts: base + 2100,
      payload: { text: 'Can you call me back?' }
    },
    {
      id: 'lead_b1_inbound',
      accountId: ACCOUNT_B_ID,
      convoKey: `${ACCOUNT_B_TO}__+18145550999`,
      type: 'inbound_message',
      channel: 'sms',
      ts: base + 3100,
      payload: { text: 'Other tenant' }
    },
    {
      id: 'lead_a3_same_ts_b',
      accountId: ACCOUNT_A_ID,
      convoKey: `${ACCOUNT_A_TO}__+18145550113`,
      type: 'inbound_message',
      channel: 'sms',
      ts: base + 8000,
      payload: { text: 'Same ts inbound B' }
    },
    {
      id: 'lead_a3_same_ts_a',
      accountId: ACCOUNT_A_ID,
      convoKey: `${ACCOUNT_A_TO}__+18145550113`,
      type: 'booking_completed',
      channel: 'sms',
      ts: base + 8000,
      payload: { text: 'Same ts inbound A' }
    }
  ];

  data.revenueEvents = [
    {
      id: 'rev_a_quote_started',
      business_id: ACCOUNT_A_ID,
      revenue_event_type: 'quote_started',
      status: 'open',
      contact_id: 'contact_a_1',
      created_at: base + 7000,
      metadata_json: { convoKey: `${ACCOUNT_A_TO}__+18145550111` }
    },
    {
      id: 'rev_a_quote_ready',
      business_id: ACCOUNT_A_ID,
      revenue_event_type: 'quote_ready',
      status: 'open',
      contact_id: 'contact_a_1',
      created_at: base + 7100,
      metadata_json: { convoKey: `${ACCOUNT_A_TO}__+18145550111` }
    },
    {
      id: 'rev_a_quote_shown',
      business_id: ACCOUNT_A_ID,
      revenue_event_type: 'quote_shown',
      status: 'open',
      contact_id: 'contact_a_1',
      created_at: base + 7200,
      metadata_json: { convoKey: `${ACCOUNT_A_TO}__+18145550111` }
    },
    {
      id: 'rev_a_quote_accepted',
      business_id: ACCOUNT_A_ID,
      revenue_event_type: 'quote_accepted',
      status: 'won',
      contact_id: 'contact_a_1',
      created_at: base + 7300,
      metadata_json: { convoKey: `${ACCOUNT_A_TO}__+18145550111` }
    },
    {
      id: 'rev_a_timeline',
      business_id: ACCOUNT_A_ID,
      revenue_event_type: 'appointment_booked',
      status: 'won',
      contact_id: 'contact_a_1',
      related_lead_event_id: 'lead_a1_inbound',
      created_at: base + 5000,
      metadata_json: { convoKey: `${ACCOUNT_A_TO}__+18145550111` }
    },
    {
      id: 'rev_a3_same_ts',
      business_id: ACCOUNT_A_ID,
      revenue_event_type: 'quote_ready',
      status: 'open',
      contact_id: 'contact_a_3',
      related_lead_event_id: 'lead_a3_same_ts_a',
      created_at: base + 8000,
      metadata_json: { convoKey: `${ACCOUNT_A_TO}__+18145550113` }
    }
  ];

  data.actions = [
    {
      id: 'act_a_1',
      accountId: ACCOUNT_A_ID,
      opportunityId: 'opp_a_1',
      convoKey: `${ACCOUNT_A_TO}__+18145550111`,
      ts: base + 2000,
      actionType: 'send_sms',
      runId: 'run_a_1',
      correlationId: 'corr_a_1',
      channel: 'sms',
      payload: { messageText: 'First follow-up', runId: 'run_a_1' },
      outcome: { status: 'sent', error: null },
      justification: { riskScore: 40, reasons: ['no_response'] }
    },
    {
      id: 'act_a_2',
      accountId: ACCOUNT_A_ID,
      opportunityId: 'opp_a_1',
      convoKey: `${ACCOUNT_A_TO}__+18145550111`,
      ts: base + 3000,
      actionType: 'send_sms',
      runId: 'run_a_1',
      correlationId: 'corr_a_1',
      channel: 'sms',
      payload: { messageText: 'Second follow-up', runId: 'run_a_1' },
      outcome: { status: 'skipped', error: null },
      justification: { riskScore: 40, reasons: ['cooldown'] }
    },
    {
      id: 'act_a_3',
      accountId: ACCOUNT_A_ID,
      opportunityId: 'opp_a_2',
      convoKey: `${ACCOUNT_A_TO}__+18145550112`,
      ts: base + 4000,
      actionType: 'email_quote',
      runId: 'run_a_2',
      correlationId: 'corr_a_2',
      channel: 'email',
      payload: { messageText: 'Quote sent', runId: 'run_a_2' },
      outcome: { status: 'failed', error: 'bounce' },
      justification: { riskScore: 30, reasons: ['stale_lead'] }
    },
    {
      id: 'act_a_4',
      accountId: ACCOUNT_A_ID,
      opportunityId: 'opp_a_2',
      convoKey: `${ACCOUNT_A_TO}__+18145550112`,
      ts: base + 4500,
      actionType: 'send_sms',
      runId: null,
      correlationId: 'corr_a_3',
      channel: 'sms',
      payload: { messageText: 'Newest action' },
      outcome: { status: 'sent', error: null },
      justification: { riskScore: 20, reasons: ['manual_review'] }
    },
    {
      id: 'act_a_6',
      accountId: ACCOUNT_A_ID,
      opportunityId: 'opp_a_3',
      convoKey: `${ACCOUNT_A_TO}__+18145550113`,
      ts: base + 8000,
      actionType: 'send_sms',
      runId: 'run_a_3',
      correlationId: 'corr_same_ts_z',
      channel: 'sms',
      payload: { messageText: 'Same ts action Z', runId: 'run_a_3' },
      outcome: { status: 'sent', error: null },
      justification: { riskScore: 35, reasons: ['timing_test'] }
    },
    {
      id: 'act_a_5',
      accountId: ACCOUNT_A_ID,
      opportunityId: 'opp_a_3',
      convoKey: `${ACCOUNT_A_TO}__+18145550113`,
      ts: base + 8000,
      actionType: 'send_sms',
      runId: 'run_a_3',
      correlationId: 'corr_same_ts_a',
      channel: 'sms',
      payload: { messageText: 'Same ts action A', runId: 'run_a_3' },
      outcome: { status: 'skipped', error: null },
      justification: { riskScore: 35, reasons: ['timing_test'] }
    },
    {
      id: 'act_b_1',
      accountId: ACCOUNT_B_ID,
      opportunityId: 'opp_b_1',
      convoKey: `${ACCOUNT_B_TO}__+18145550999`,
      ts: base + 9000,
      actionType: 'send_sms',
      runId: 'run_b_1',
      correlationId: 'corr_b_1',
      channel: 'sms',
      payload: { messageText: 'Other tenant', runId: 'run_b_1' },
      outcome: { status: 'sent', error: null },
      justification: { riskScore: 55, reasons: ['other_tenant'] }
    }
  ];

  data.agentRuns = [
    {
      id: 'run_a_1',
      accountId: ACCOUNT_A_ID,
      opportunityId: 'opp_a_1',
      createdAt: base + 1000,
      status: 'COMPLETED',
      stepState: { completedSteps: ['step_1', 'step_2'] },
      plan: { playbookId: 'playbook_alpha' }
    },
    {
      id: 'run_a_2',
      accountId: ACCOUNT_A_ID,
      opportunityId: 'opp_a_2',
      createdAt: base + 2500,
      status: 'FAILED',
      stepState: { completedSteps: ['step_1'] },
      plan: { playbookId: 'playbook_beta' }
    },
    {
      id: 'run_b_1',
      accountId: ACCOUNT_B_ID,
      opportunityId: 'opp_b_1',
      createdAt: base + 1500,
      status: 'COMPLETED',
      stepState: { completedSteps: ['step_1'] },
      plan: { playbookId: 'playbook_other' }
    },
    {
      id: 'run_a_3',
      accountId: ACCOUNT_A_ID,
      opportunityId: 'opp_a_3',
      createdAt: base + 7900,
      status: 'COMPLETED',
      stepState: { completedSteps: ['step_1', 'step_2', 'step_3'] },
      plan: { playbookId: 'playbook_alpha' }
    }
  ];

  saveDataDebounced(data);
}

async function run() {
  const app = await initApp();
  await seedBaseline();
  seedAnalyticsState();
  await flushDataNow();

  const agent = request.agent(app);
  await login(agent, { email: SUPERADMIN_EMAIL, password: SUPERADMIN_PASSWORD });

  {
    const res = await agent.get(`/api/analytics/funnel?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(res.statusCode, 200);
    assert.deepEqual(res.body, {
      leadsCreated: 3,
      replied: 3,
      qualified: 3,
      booked: 1,
      completed: 2,
      quoteStarted: 1,
      quoteReady: 2,
      quoteShown: 1,
      quoteAccepted: 1,
      startedToReadyDropoffPct: -100,
      readyToShownDropoffPct: 50,
      shownToAcceptedDropoffPct: 0
    });
  }

  {
    const res = await agent.get(`/api/analytics/activity-feed?to=${encodeURIComponent(ACCOUNT_A_TO)}&limit=6`);
    assert.equal(res.statusCode, 200);
    assert.equal(Array.isArray(res.body?.items), true);
    assert.equal(Array.isArray(res.body?.groups), true);
    assert.deepEqual(res.body.items.map((item) => item.id), ['act_a_6', 'act_a_5', 'act_a_4', 'act_a_3', 'act_a_2', 'act_a_1']);
    assert.deepEqual(res.body.groups.map((group) => group.correlationId), ['corr_same_ts_a', 'corr_same_ts_z', 'corr_a_3', 'corr_a_2', 'corr_a_1']);
    assert.deepEqual(res.body.groups.find((group) => group.correlationId === 'corr_a_1').actions.map((item) => item.id), ['act_a_1', 'act_a_2']);
    assert.deepEqual(res.body.groups.find((group) => group.correlationId === 'corr_same_ts_a').actions.map((item) => item.id), ['act_a_5']);
    assert.deepEqual(res.body.items.map((item) => item.ts), res.body.items.map((item) => item.ts).slice().sort((a, b) => b - a));
  }

  {
    const res = await agent.get(`/api/analytics/opportunity/opp_a_1/timeline?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.opportunityId, 'opp_a_1');
    assert.equal(res.body?.convoKey, `${ACCOUNT_A_TO}__+18145550111`);
    assert.deepEqual(res.body?.stageHistory, [{ stage: 'NEW', ts: res.body.stageHistory[0].ts }]);
    assert.deepEqual(res.body?.riskHistory.map((item) => item.riskScore), [10, 40]);
    assert.deepEqual(res.body.timeline.map((item) => item.id), [
      'lead_a1_inbound',
      'act_a_1',
      'act_a_2',
      'rev_a_timeline',
      'lead_a1_completed',
      'rev_a_quote_started',
      'rev_a_quote_ready',
      'rev_a_quote_shown',
      'rev_a_quote_accepted'
    ]);
    assert.deepEqual([...res.body.timeline].map((item) => item.ts), [...res.body.timeline].map((item) => item.ts).slice().sort((a, b) => a - b));
  }

  {
    const res = await agent.get(`/api/analytics/opportunity/opp_b_1/timeline?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(res.statusCode, 404);
  }

  {
    const res = await agent.get(`/api/analytics/opportunity/opp_a_3/timeline?to=${encodeURIComponent(ACCOUNT_A_TO)}`);
    assert.equal(res.statusCode, 200);
    assert.deepEqual(res.body.timeline.map((item) => item.id), [
      'act_a_5',
      'act_a_6',
      'lead_a3_same_ts_a',
      'lead_a3_same_ts_b',
      'rev_a3_same_ts'
    ]);
    assert.deepEqual(res.body.timeline.map((item) => item.ts), res.body.timeline.map((item) => item.ts).slice().sort((a, b) => a - b));
  }

  {
    const res = await agent.get(`/api/analytics/agent-metrics?to=${encodeURIComponent(ACCOUNT_A_TO)}&range=30d`);
    assert.equal(res.statusCode, 200);
    assert.deepEqual(res.body, {
      rangeDays: 30,
      runsStarted: 3,
      runsCompleted: 2,
      runsFailed: 1,
      averageStepsToRecovery: 2.5,
      recoveryRate: 0.333,
      avgMinutesToFirstResponseAfterRun: 0
    });
  }

  {
    const res = await agent.get(`/api/analytics/playbook-performance?to=${encodeURIComponent(ACCOUNT_A_TO)}&range=30d`);
    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.rangeDays, 30);
    assert.deepEqual(res.body.items, [
      {
        playbookId: 'playbook_alpha',
        runs: 2,
        completed: 2,
        failed: 0,
        byStepType: {
          SEND_SMS: { sent: 2, failed: 0, skipped: 2 }
        },
        completionRate: 1
      },
      {
        playbookId: 'playbook_beta',
        runs: 1,
        completed: 0,
        failed: 1,
        byStepType: {
          EMAIL_QUOTE: { sent: 0, failed: 1, skipped: 0 }
        },
        completionRate: 0
      }
    ]);
  }

  {
    const { count, result } = await withQueryCount(pool, (db) =>
      getFunnelMetrics(ACCOUNT_A_ID, {
        db,
        identifiers: { route: 'test/funnel' }
      })
    );
    assert.equal(count, 1);
    assert.equal(result?.leadsCreated, 3);
  }

  {
    const { count, result } = await withQueryCount(pool, (db) =>
      getOpportunityTimeline(ACCOUNT_A_ID, 'opp_a_1', {
        db,
        identifiers: { route: 'test/timeline' }
      })
    );
    assert.equal(count, 2);
    assert.equal(result?.opportunityId, 'opp_a_1');
  }

  {
    const { count, result } = await withQueryCount(pool, (db) =>
      getActivityFeed(ACCOUNT_A_ID, 6, {
        db,
        identifiers: { route: 'test/activity-feed', limit: 6 }
      })
    );
    assert.equal(count, 1);
    assert.deepEqual(result.items.map((item) => item.id), ['act_a_6', 'act_a_5', 'act_a_4', 'act_a_3', 'act_a_2', 'act_a_1']);
  }

  {
    const { count, result } = await withQueryCount(pool, (db) =>
      getPlaybookPerformance(ACCOUNT_A_ID, '30d', {
        db,
        identifiers: { route: 'test/playbook-performance', rangeDays: '30d' }
      })
    );
    assert.equal(count, 1);
    assert.equal(result.items[0]?.playbookId, 'playbook_alpha');
  }

  console.log('[tests] opportunities/actions reads integration checks passed');
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
