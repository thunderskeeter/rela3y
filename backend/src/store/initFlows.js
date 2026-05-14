const { loadData, getFlows, flushDataNow } = require('./dataStore');

function initFlows() {
  try {
    const data = loadData();
    const accounts = Object.values(data?.accounts || {});
    if (!accounts.length) {
      console.log('[flows] no accounts found; skipping account flow initialization');
      return;
    }

    let initializedForAccounts = 0;
    for (const account of accounts) {
      const accountId = String(account?.accountId || account?.id || '').trim();
      if (!accountId) continue;
      getFlows(accountId);
      initializedForAccounts += 1;
    }

    flushDataNow();
    console.log(`[flows] ensured template flows for ${initializedForAccounts} account(s)`);
  } catch (err) {
    console.error('[flows] initialization error:', err);
  }
}

module.exports = { initFlows };
