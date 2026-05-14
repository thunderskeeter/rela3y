// Bootstrap for Relay Dashboard backend
// Keep this file tiny. Most logic lives in ./src
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });
const { validateRuntimeConfig } = require('./src/config/runtime');

validateRuntimeConfig();

const { createApp } = require('./src/app');
const { runMigrations } = require('./src/db/migrate');
const { backfillTenantAccountIds } = require('./src/services/tenantMigration');
const { ensureDefaultSuperadminUser } = require('./src/utils/auth');
const { initFlows } = require('./src/store/initFlows');
const { DEV_MODE } = require('./src/config/runtime');
const { initDataStore, flushDataNow, loadData, saveDataDebounced } = require('./src/store/dataStore');
const { initScheduler } = require('./src/services/scheduler');
const { initComplianceRetentionJob } = require('./src/services/complianceService');

function parseBool(value, fallback = false) {
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) return fallback;
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
  if (['0', 'false', 'no', 'off'].includes(raw)) return false;
  return fallback;
}

async function bootstrap() {
  await runMigrations();
  await initDataStore();

  if (DEV_MODE === true && parseBool(process.env.RESET_CONVERSATIONS_ON_BOOT, false)) {
    const data = loadData();
    const removedConversations = Object.keys(data?.conversations || {}).length;
    const removedOpportunities = Array.isArray(data?.revenueOpportunities)
      ? data.revenueOpportunities.filter((opp) => String(opp?.convoKey || '').trim()).length
      : 0;

    data.conversations = {};
    if (Array.isArray(data.revenueOpportunities)) {
      data.revenueOpportunities = data.revenueOpportunities.filter((opp) => !String(opp?.convoKey || '').trim());
    }
    if (Array.isArray(data.leadEvents)) {
      data.leadEvents = data.leadEvents.filter((event) => !String(event?.convoKey || '').trim());
    }
    if (Array.isArray(data.actions)) {
      data.actions = data.actions.filter((action) => !String(action?.convoKey || action?.conversationId || '').trim());
    }
    saveDataDebounced(data);
    await flushDataNow();
    console.log(`[bootstrap] reset conversations on boot (removed conversations=${removedConversations}, related opportunities=${removedOpportunities})`);
  }

  // Ensure tenant/account IDs are present before any route/service logic runs
  backfillTenantAccountIds();
  ensureDefaultSuperadminUser();

  // Initialize flow templates on startup
  initFlows();
  await flushDataNow();

  // Initialize the automation scheduler (delayed follow-ups, reminders, win-back)
  initScheduler();
  initComplianceRetentionJob();

  const PORT = process.env.PORT || 3001;
  const app = createApp({
    // Serve the frontend (static) from ../frontend
    staticDir: path.join(__dirname, '..', 'frontend'),
  });

  app.listen(PORT, () => {
    console.log(`Relay backend listening on http://127.0.0.1:${PORT}`);
  });
}

bootstrap().catch((err) => {
  console.error('[bootstrap] failed:', err?.stack || err?.message || err);
  process.exit(1);
});
