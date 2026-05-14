process.env.USE_DB_CONVERSATIONS = 'true';
process.env.USE_DB_MESSAGES = 'true';
process.env.ENABLE_PARITY_CHECKS = 'false';
process.env.DISABLE_SNAPSHOT_PERSISTENCE = 'true';

const assert = require('node:assert/strict');
const {
  initApp,
  seedBaseline,
  seedDbTenants,
  shutdown,
  ACCOUNT_A_TO
} = require('./_shared');
const { loadData, saveDataDebounced } = require('../src/store/dataStore');
const { pool } = require('../src/db/pool');
const { getConversationDetail } = require('../src/services/conversationsService');
const { evaluateTrigger } = require('../src/services/automationEngine');
const { executeRevenueAction } = require('../src/services/actionExecutor');
const { executeStep } = require('../src/services/agentEngine');
const { persistSnapshotOpportunity } = require('../src/services/opportunitiesService');
const { createIfMissing } = require('../src/repositories/conversationsRepo');
const { insertIdempotent } = require('../src/repositories/messagesRepo');

const ACCOUNT_A_ID = 'acct_10000000001';

function convoKey(to, from) {
  return `${to}__${from}`;
}

function unique(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

async function resetMessagingAndRevenueDb() {
  await pool.query('DELETE FROM actions');
  await pool.query('DELETE FROM opportunities');
  await pool.query('DELETE FROM messages');
  await pool.query('DELETE FROM conversations');
}

function resetSnapshotMessagingState() {
  const data = loadData();
  data.conversations = {};
  data.revenueOpportunities = [];
  data.agentRuns = [];
  data.rules = [];
  saveDataDebounced(data);
}

async function seedConversation(from, messageText = 'Seed inbound') {
  const id = convoKey(ACCOUNT_A_TO, from);
  const now = Date.now();
  await createIfMissing(pool, ACCOUNT_A_ID, {
    convoKey: id,
    to: ACCOUNT_A_TO,
    from,
    status: 'new',
    stage: 'ask_service',
    audit: [],
    leadData: {},
    createdAt: now - 2000,
    updatedAt: now - 1000,
    lastActivityAt: now - 1000,
    payload: {}
  });
  await insertIdempotent(pool, ACCOUNT_A_ID, id, {
    id: unique('seed_msg'),
    direction: 'inbound',
    body: messageText,
    status: 'received',
    to: ACCOUNT_A_TO,
    from,
    createdAt: now - 1000,
    updatedAt: now - 1000,
    payload: {}
  });
  return id;
}

async function seedOpportunity(id, convoKeyValue, overrides = {}) {
  const now = Date.now();
  const base = {
    id,
    accountId: ACCOUNT_A_ID,
    convoKey: convoKeyValue,
    stage: 'NEW',
    riskScore: 25,
    contactId: unique('contact'),
    createdAt: now - 5000,
    updatedAt: now - 1000,
    actionLogIds: [],
    metadata: {}
  };
  const opportunity = { ...base, ...overrides };
  const data = loadData();
  data.revenueOpportunities = Array.isArray(data.revenueOpportunities) ? data.revenueOpportunities : [];
  data.revenueOpportunities.push(opportunity);
  saveDataDebounced(data);
  await persistSnapshotOpportunity(ACCOUNT_A_ID, opportunity, { operation: 'test_seed' });
  return opportunity;
}

async function run() {
  await initApp();
  try {
    await seedBaseline();
    await resetMessagingAndRevenueDb();
    await seedDbTenants();

    {
      resetSnapshotMessagingState();
      const from = '+18145550201';
      const id = await seedConversation(from, 'Automation seed');
      assert.equal(loadData().conversations[id], undefined);
      const data = loadData();
      data.rules = [{
        id: unique('rule'),
        accountId: ACCOUNT_A_ID,
        trigger: 'missed_call',
        enabled: true,
        delayMinutes: 0,
        template: 'Automation DB-native send',
        templateId: unique('tpl'),
        category: 'lead_response'
      }];
      saveDataDebounced(data);

      const results = await evaluateTrigger('missed_call', {
        tenant: { accountId: ACCOUNT_A_ID, to: ACCOUNT_A_TO },
        to: ACCOUNT_A_TO,
        from,
        eventData: {}
      });
      assert.equal(results.some((item) => item.action === 'sent'), true);
      assert.equal(loadData().conversations[id], undefined);
      const detail = await getConversationDetail(ACCOUNT_A_ID, id);
      assert.equal(detail.messages.some((item) => item.text === 'Automation DB-native send'), true);
    }

    {
      resetSnapshotMessagingState();
      const from = '+18145550202';
      const id = await seedConversation(from, 'Action seed');
      const opportunity = await seedOpportunity(unique('opp_action'), id);
      assert.equal(loadData().conversations[id], undefined);

      const result = await executeRevenueAction(
        ACCOUNT_A_ID,
        { id: unique('lead_evt'), convoKey: id },
        opportunity,
        {
          nextAction: 'send_message',
          messageText: 'Action executor DB-native send',
          recommendedPack: 'recovery_pack',
          policy: { dailyFollowupCapPerLead: 2, minCooldownMinutes: 30 }
        },
        { trigger: 'signal_ingest' }
      );
      assert.equal(result.ok, true);
      assert.equal(loadData().conversations[id], undefined);
      const detail = await getConversationDetail(ACCOUNT_A_ID, id);
      assert.equal(detail.messages.some((item) => item.text === 'Action executor DB-native send'), true);
    }

    {
      resetSnapshotMessagingState();
      const from = '+18145550203';
      const id = await seedConversation(from, 'Agent seed');
      const opportunity = await seedOpportunity(unique('opp_agent'), id, {
        metadata: { lastSignalType: 'inbound_message' }
      });
      const now = Date.now();
      const run = {
        id: unique('run_agent'),
        accountId: ACCOUNT_A_ID,
        opportunityId: opportunity.id,
        createdAt: now,
        updatedAt: now,
        status: 'PLANNED',
        mode: 'AUTO',
        trigger: 'manual_user_start',
        correlationId: unique('corr'),
        plan: {
          playbookId: 'home_services',
          steps: [{
            stepId: 's1',
            type: 'SEND_MESSAGE',
            when: { kind: 'NOW' },
            payload: { messageText: 'Agent engine DB-native send' },
            guardrails: { requiresReview: false, maxAttempts: 1, cooldownMinutes: 30 },
            successCriteria: { kind: 'INBOUND_REPLY' },
            failureCriteria: { kind: 'NO_REPLY_WITHIN', minutes: 15 }
          }]
        },
        stepState: {
          currentStepId: null,
          completedSteps: [],
          scheduledStepJobs: []
        },
        lastError: null
      };
      const data = loadData();
      data.agentRuns = Array.isArray(data.agentRuns) ? data.agentRuns : [];
      data.agentRuns.push(run);
      const oppRef = data.revenueOpportunities.find((item) => String(item?.id || '') === String(opportunity.id));
      oppRef.agentState = oppRef.agentState || {};
      oppRef.agentState.activeRunId = run.id;
      oppRef.agentState.lastRunId = run.id;
      saveDataDebounced(data);

      assert.equal(loadData().conversations[id], undefined);
      const result = await executeStep(ACCOUNT_A_ID, run.id, 's1', {
        trigger: 'manual_user_start',
        reviewApproved: true,
        forceNow: true,
        skipSchedule: true
      });
      assert.equal(result.ok, true);
      assert.equal(loadData().conversations[id], undefined);
      const detail = await getConversationDetail(ACCOUNT_A_ID, id);
      assert.equal(detail.messages.some((item) => item.text === 'Agent engine DB-native send'), true);
    }

    console.log('[tests] messaging async writer cutover checks passed');
  } finally {
    await shutdown();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
