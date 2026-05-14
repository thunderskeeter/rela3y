const assert = require('node:assert/strict');
const {
  initApp,
  seedBaseline,
  shutdown
} = require('./_shared');

const { emitAccountEvent } = require('../src/services/notificationService');
const { getNotificationLog, setNotificationSettings } = require('../src/store/dataStore');
const ACCOUNT_A_ID = 'acct_10000000001';

async function run() {
  await initApp();

  await seedBaseline();
  {
    setNotificationSettings(ACCOUNT_A_ID, {
      channels: { email: true, sms: false, desktop: false },
      triggers: {
        highValueLead: true,
        noResponse: true,
        failedWebhook: true,
        failedAutomation: true
      }
    });

    const hv = emitAccountEvent(ACCOUNT_A_ID, {
      type: 'high_value_lead',
      from: '+18145550131',
      conversationId: '+10000000001__+18145550131'
    });
    assert.equal(hv.ok, true);

    const nr = emitAccountEvent(ACCOUNT_A_ID, {
      type: 'no_response',
      from: '+18145550132',
      conversationId: '+10000000001__+18145550132'
    });
    assert.equal(nr.ok, true);

    const fw = emitAccountEvent(ACCOUNT_A_ID, {
      type: 'failed_webhook',
      from: '+18145550133',
      conversationId: '+10000000001__+18145550133'
    });
    assert.equal(fw.ok, true);

    const fa = emitAccountEvent(ACCOUNT_A_ID, {
      type: 'failed_automation',
      from: '+18145550134',
      conversationId: '+10000000001__+18145550134'
    });
    assert.equal(fa.ok, true);

    const logs = getNotificationLog(ACCOUNT_A_ID, 50);
    const sentTypes = new Set(logs.filter((x) => x.status === 'sent').map((x) => String(x.eventType || '')));
    assert.equal(sentTypes.has('high_value_lead'), true);
    assert.equal(sentTypes.has('no_response'), true);
    assert.equal(sentTypes.has('failed_webhook'), true);
    assert.equal(sentTypes.has('failed_automation'), true);
  }

  await seedBaseline();
  {
    setNotificationSettings(ACCOUNT_A_ID, {
      channels: { email: true, sms: false, desktop: false },
      triggers: { highValueLead: false }
    });
    const hvBlocked = emitAccountEvent(ACCOUNT_A_ID, {
      type: 'high_value_lead',
      from: '+18145550199',
      conversationId: '+10000000001__+18145550199'
    });
    assert.equal(hvBlocked.ok, true);
    const logs = getNotificationLog(ACCOUNT_A_ID, 5);
    assert.equal(String(logs[0]?.eventType || ''), 'high_value_lead');
    assert.equal(String(logs[0]?.status || ''), 'blocked');
    assert.equal(String(logs[0]?.reason || ''), 'trigger_disabled');
  }

  console.log('[tests] notifications integration checks passed');
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
