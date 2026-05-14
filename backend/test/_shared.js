const request = require('supertest');
require('dotenv').config({ path: '.env' });

process.env.NODE_ENV = process.env.NODE_ENV || 'test';
process.env.DEV_MODE = process.env.DEV_MODE || 'true';
process.env.WEBHOOK_DEV_SECRET = process.env.WEBHOOK_DEV_SECRET || 'test-webhook-secret';
process.env.AUTH_SECRET = process.env.AUTH_SECRET || 'test-auth-secret';
if (process.env.DATABASE_URL) {
  const u = new URL(process.env.DATABASE_URL);
  const base = String(u.pathname || '').replace(/^\/+/, '') || 'relay_dashboard';
  u.pathname = `/${base.endsWith('_test') ? base : `${base}_test`}`;
  process.env.DATABASE_URL = u.toString();
} else {
  process.env.DATABASE_URL = 'postgres://postgres:postgres@127.0.0.1:5432/relay_dashboard_test';
}

const { runMigrations } = require('../src/db/migrate');
const { createApp } = require('../src/app');
const { initDataStore, loadData, saveDataDebounced, flushDataNow, shutdownDataStore, ensureAccountForTo } = require('../src/store/dataStore');
const { pool } = require('../src/db/pool');
const { hashPassword } = require('../src/utils/auth');

const ACCOUNT_A_TO = '+10000000001';
const ACCOUNT_B_TO = '+10000000002';
const ACCOUNT_A_ID = 'acct_10000000001';
const ACCOUNT_B_ID = 'acct_10000000002';

const OWNER_EMAIL = 'owner-a@example.com';
const OWNER_PASSWORD = 'owner-pass-123';
const SUPERADMIN_EMAIL = 'superadmin@example.com';
const SUPERADMIN_PASSWORD = 'superadmin-pass-123';
let loginIpCounter = 10;

function snapshotPersistenceDisabled() {
  const raw = String(process.env.DISABLE_SNAPSHOT_PERSISTENCE || '').trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on';
}

function resetData() {
  const data = loadData();
  data.conversations = {};
  data.rules = [];
  data.accounts = {};
  data.flows = {};
  data.contacts = {};
  data.scheduledJobs = [];
  data.vipList = {};
  data.users = [];
  data.sessions = {};
  data.dev = null;
  data.leadEvents = [];
  data.revenueOpportunities = [];
  data.leadIntelligence = {};
  data.alerts = [];
  data.actions = [];
  data.optimizationEvents = [];
  data.agentRuns = [];
  data.reviewQueue = [];
  data.revenueEvents = [];
  data.workspaceRequests = [];
  data.webhookReceipts = {};
  return data;
}

async function seedBaseline() {
  const data = resetData();
  ensureAccountForTo(data, ACCOUNT_A_TO, { autoCreate: true });
  ensureAccountForTo(data, ACCOUNT_B_TO, { autoCreate: true });
  data.accounts[ACCOUNT_A_TO].id = ACCOUNT_A_ID;
  data.accounts[ACCOUNT_A_TO].accountId = ACCOUNT_A_ID;
  data.accounts[ACCOUNT_A_TO].businessName = 'Account A';
  data.accounts[ACCOUNT_A_TO].integrations = data.accounts[ACCOUNT_A_TO].integrations || {};
  data.accounts[ACCOUNT_A_TO].integrations.stripe = {
    enabled: true,
    webhookSecret: 'whsec_test_local'
  };
  data.accounts[ACCOUNT_B_TO].id = ACCOUNT_B_ID;
  data.accounts[ACCOUNT_B_TO].accountId = ACCOUNT_B_ID;
  data.accounts[ACCOUNT_B_TO].businessName = 'Account B';
  data.users.push({
    id: 'user_owner_a',
    email: OWNER_EMAIL,
    passwordHash: hashPassword(OWNER_PASSWORD),
    role: 'owner',
    accountIds: [ACCOUNT_A_ID],
    disabled: false
  });
  data.users.push({
    id: 'user_superadmin',
    email: SUPERADMIN_EMAIL,
    passwordHash: hashPassword(SUPERADMIN_PASSWORD),
    role: 'superadmin',
    accountIds: [],
    disabled: false
  });
  saveDataDebounced(data);
  if (!snapshotPersistenceDisabled()) {
    await flushDataNow();
  }
}

async function login(agent, { email, password }) {
  loginIpCounter += 1;
  const res = await agent
    .post('/api/auth/login')
    .set('x-forwarded-for', `10.1.0.${loginIpCounter}`)
    .send({ email, password, useCookie: true });
  if (res.statusCode !== 200) {
    throw new Error(`Login failed (${res.statusCode}): ${JSON.stringify(res.body || {})}`);
  }
  return res.body.csrfToken;
}

async function initApp() {
  await runMigrations();
  await initDataStore();
  return createApp();
}

async function seedDbTenants() {
  await pool.query(
    `
      INSERT INTO tenants (id, to_number, business_name, workspace, settings, defaults, compliance, billing, integrations)
      VALUES
        ($1,$2,$3,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb),
        ($4,$5,$6,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb)
      ON CONFLICT (id)
      DO UPDATE SET
        to_number = EXCLUDED.to_number,
        business_name = EXCLUDED.business_name,
        updated_at = NOW()
    `,
    [
      ACCOUNT_A_ID, ACCOUNT_A_TO, 'Account A',
      ACCOUNT_B_ID, ACCOUNT_B_TO, 'Account B'
    ]
  );
}

async function shutdown() {
  await shutdownDataStore();
  await pool.end();
}

module.exports = {
  request,
  initApp,
  seedBaseline,
  seedDbTenants,
  login,
  shutdown,
  ACCOUNT_A_TO,
  ACCOUNT_B_TO,
  OWNER_EMAIL,
  OWNER_PASSWORD,
  SUPERADMIN_EMAIL,
  SUPERADMIN_PASSWORD
};
