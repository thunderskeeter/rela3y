process.env.ENABLE_PARITY_CHECKS = 'true';
process.env.USE_DB_OPPORTUNITIES = 'true';
process.env.USE_DB_ACTIONS = 'true';

const assert = require('node:assert/strict');
const {
  initApp,
  seedBaseline,
  shutdown,
  ACCOUNT_A_TO,
  ACCOUNT_B_TO
} = require('./_shared');
const { loadData, saveDataDebounced, flushDataNow } = require('../src/store/dataStore');
const { pool } = require('../src/db/pool');
const { withQueryCount } = require('../src/db/queryInstrumentation');
const { listByTenant, getById } = require('../src/repositories/opportunitiesRepo');
const { listRecentByTenant, listByOpportunity, listByRunIds } = require('../src/repositories/actionsRepo');
const { getFunnelMetrics, getOpportunityTimeline } = require('../src/services/opportunitiesService');
const { getActivityFeed, getPlaybookPerformance } = require('../src/services/actionsService');

const ACCOUNT_A_ID = 'acct_10000000001';
const ACCOUNT_B_ID = 'acct_10000000002';
const DAY_MS = 24 * 60 * 60 * 1000;

function seedAuditState() {
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
      id: 'rev_a_timeline',
      business_id: ACCOUNT_A_ID,
      revenue_event_type: 'appointment_booked',
      status: 'won',
      contact_id: 'contact_a_1',
      related_lead_event_id: 'lead_a1_inbound',
      created_at: base + 5000,
      metadata_json: { convoKey: `${ACCOUNT_A_TO}__+18145550111` }
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
      id: 'act_b_1',
      accountId: ACCOUNT_B_ID,
      opportunityId: 'opp_b_1',
      convoKey: `${ACCOUNT_B_TO}__+18145550999`,
      ts: base + 9000,
      actionType: 'send_sms',
      runId: 'run_shared',
      correlationId: 'corr_b_1',
      channel: 'sms',
      payload: { messageText: 'Other tenant', runId: 'run_shared' },
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
      id: 'run_shared',
      accountId: ACCOUNT_B_ID,
      opportunityId: 'opp_b_1',
      createdAt: base + 1500,
      status: 'COMPLETED',
      stepState: { completedSteps: ['step_1'] },
      plan: { playbookId: 'playbook_other' }
    }
  ];

  saveDataDebounced(data);
}

async function expectNoParityMismatch(label, fn, verdicts) {
  await fn();
  verdicts.push(`${label}: clean`);
}

async function expectParityMismatch(label, fn, verdicts) {
  let threw = false;
  try {
    await fn();
  } catch (err) {
    threw = /Parity mismatch/i.test(String(err?.message || ''));
  }
  if (!threw) {
    throw new Error(`too aggressive: ${label}`);
  }
  verdicts.push(`${label}: noisy but explainable`);
}

async function resetSeed() {
  await seedBaseline();
  seedAuditState();
  await flushDataNow();
}

async function run() {
  await initApp();
  const verdicts = [];

  await resetSeed();

  {
    const opportunities = await listByTenant(pool, ACCOUNT_A_ID);
    assert.equal(opportunities.length, 2);
    assert.equal(opportunities.every((row) => row.accountId === ACCOUNT_A_ID), true);

    const opportunity = await getById(pool, ACCOUNT_A_ID, 'opp_b_1');
    assert.equal(opportunity, null);
  }

  {
    const actions = await listRecentByTenant(pool, ACCOUNT_A_ID, 10);
    assert.equal(actions.length, 3);
    assert.equal(actions.every((row) => row.accountId === ACCOUNT_A_ID), true);

    const byOpportunity = await listByOpportunity(pool, ACCOUNT_A_ID, 'opp_a_1');
    assert.deepEqual(byOpportunity.map((row) => row.id), ['act_a_2', 'act_a_1']);

    const byRunIds = await listByRunIds(pool, ACCOUNT_A_ID, ['run_a_1', 'run_shared']);
    assert.deepEqual(byRunIds.map((row) => row.id), ['act_a_2', 'act_a_1']);
  }

  {
    const { count } = await withQueryCount(pool, (db) =>
      getFunnelMetrics(ACCOUNT_A_ID, { db, identifiers: { route: 'audit/funnel' } })
    );
    assert.equal(count, 1);
  }

  {
    const { count } = await withQueryCount(pool, (db) =>
      getActivityFeed(ACCOUNT_A_ID, 10, { db, identifiers: { route: 'audit/activity-feed', limit: 10 } })
    );
    assert.equal(count, 1);
  }

  {
    const { count } = await withQueryCount(pool, (db) =>
      getPlaybookPerformance(ACCOUNT_A_ID, '30d', { db, identifiers: { route: 'audit/playbook-performance' } })
    );
    assert.equal(count, 1);
  }

  await expectNoParityMismatch('funnel baseline parity', () =>
    getFunnelMetrics(ACCOUNT_A_ID, { identifiers: { route: 'audit/funnel/baseline' } }), verdicts
  );

  await expectNoParityMismatch('activity-feed baseline parity', () =>
    getActivityFeed(ACCOUNT_A_ID, 10, { identifiers: { route: 'audit/activity-feed/baseline', limit: 10 } }), verdicts
  );

  await expectNoParityMismatch('timeline baseline parity', () =>
    getOpportunityTimeline(ACCOUNT_A_ID, 'opp_a_1', { identifiers: { route: 'audit/timeline/baseline', opportunityId: 'opp_a_1' } }), verdicts
  );

  await resetSeed();
  await pool.query(
    `
      UPDATE opportunities
      SET payload = jsonb_set(payload, '{riskScore}', '999'::jsonb, true)
      WHERE tenant_id = $1 AND id = $2
    `,
    [ACCOUNT_A_ID, 'opp_a_2']
  );
  await expectNoParityMismatch('funnel canonical overlay benign divergence', () =>
    getFunnelMetrics(ACCOUNT_A_ID, { identifiers: { route: 'audit/funnel/benign' } }), verdicts
  );

  await resetSeed();
  await pool.query(
    `
      UPDATE opportunities
      SET payload = payload - 'contactId'
      WHERE tenant_id = $1 AND id = $2
    `,
    [ACCOUNT_A_ID, 'opp_a_1']
  );
  await expectParityMismatch('timeline missing contactId divergence', () =>
    getOpportunityTimeline(ACCOUNT_A_ID, 'opp_a_1', { identifiers: { route: 'audit/timeline/contactId', opportunityId: 'opp_a_1' } }), verdicts
  );

  await resetSeed();
  await pool.query(
    `
      UPDATE actions
      SET payload = jsonb_set(payload, '{correlationId}', '\"corr_drift\"'::jsonb, true)
      WHERE tenant_id = $1 AND id = $2
    `,
    [ACCOUNT_A_ID, 'act_a_1']
  );
  await expectParityMismatch('activity-feed grouping divergence', () =>
    getActivityFeed(ACCOUNT_A_ID, 10, { identifiers: { route: 'audit/activity-feed/grouping', limit: 10 } }), verdicts
  );

  await resetSeed();
  await pool.query(
    `
      UPDATE opportunities
      SET risk_score = 5
      WHERE tenant_id = $1 AND id = $2
    `,
    [ACCOUNT_A_ID, 'opp_a_2']
  );
  await expectParityMismatch('funnel semantic divergence', () =>
    getFunnelMetrics(ACCOUNT_A_ID, { identifiers: { route: 'audit/funnel/semantic' } }), verdicts
  );

  await resetSeed();
  await pool.query(
    `
      DELETE FROM actions
      WHERE tenant_id = $1 AND id = $2
    `,
    [ACCOUNT_A_ID, 'act_a_2']
  );
  await expectParityMismatch('timeline membership divergence', () =>
    getOpportunityTimeline(ACCOUNT_A_ID, 'opp_a_1', { identifiers: { route: 'audit/timeline/membership', opportunityId: 'opp_a_1' } }), verdicts
  );

  console.log('[parity-audit] verdicts');
  for (const verdict of verdicts) {
    console.log(`[parity-audit] ${verdict}`);
  }
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
