const {
  loadData,
  saveDataDebounced,
  ensureAccountForTo,
  inferAccountIdForTo,
  accountIdFromTo,
  flowStorageKey
} = require('../store/dataStore');
const { DEV_MODE } = require('../config/runtime');

const DEFAULT_DEV_TO = '+10000000000';

function deriveToFromConvoKey(key) {
  const [to] = String(key || '').split('__');
  return to || null;
}

function deriveToFromContactKey(key) {
  const [to] = String(key || '').split('__');
  return to || null;
}

function markOrphan(record, kind, key, counters) {
  if (!record || typeof record !== 'object') return false;
  if (record.orphaned === true) return false;
  record.orphaned = true;
  record.orphanedAt = Date.now();
  record.orphanReason = 'missing_resolvable_tenant';
  if (counters) counters.orphaned += 1;
  console.warn(`[tenant-migration] ${kind} ${key} marked orphaned (prod mode)`);
  return true;
}

function assignOrMark(record, accountId, kind, key, counters) {
  if (!record || typeof record !== 'object') return false;
  if (accountId) {
    record.accountId = accountId;
    delete record.orphaned;
    delete record.orphanedAt;
    delete record.orphanReason;
    if (counters) counters.assigned += 1;
    return true;
  }
  if (DEV_MODE) {
    return false;
  }
  return markOrphan(record, kind, key, counters);
}

function migrateNamespacedFlows(data, counters) {
  data.flows = data.flows && typeof data.flows === 'object' && !Array.isArray(data.flows) ? data.flows : {};
  const next = {};
  let moved = 0;
  let collisions = 0;

  for (const [storageKey, flow] of Object.entries(data.flows || {})) {
    if (!flow || typeof flow !== 'object') continue;
    const inferredId = String(flow.id || storageKey.split('__').slice(1).join('__') || storageKey).trim();
    const accountId = String(flow.accountId || '').trim();
    if (!accountId || !inferredId) {
      next[storageKey] = flow;
      continue;
    }
    const targetKey = flowStorageKey(accountId, inferredId);
    if (!targetKey) {
      next[storageKey] = flow;
      continue;
    }
    flow.id = inferredId;
    flow.accountId = accountId;
    if (Object.prototype.hasOwnProperty.call(next, targetKey)) {
      collisions += 1;
      console.warn(`[tenant-migration] flow key collision for ${targetKey}; keeping first entry`);
      continue;
    }
    next[targetKey] = flow;
    if (targetKey !== storageKey) moved += 1;
  }

  data.flows = next;
  if (counters) {
    counters.flowsMoved += moved;
    counters.flowCollisions += collisions;
  }
  return moved > 0 || collisions > 0;
}

function backfillTenantAccountIds() {
  const data = loadData();
  let changed = false;
  const counts = {
    assigned: 0,
    orphaned: 0,
    flowsMoved: 0,
    flowCollisions: 0
  };

  // Ensure accounts have stable ids
  for (const [to, account] of Object.entries(data.accounts || {})) {
    if (!account || typeof account !== 'object') continue;
    const ensured = ensureAccountForTo(data, to, { autoCreate: true });
    if (ensured && (!account.id || !account.accountId)) changed = true;
  }

  const defaultAccount = ensureAccountForTo(data, DEFAULT_DEV_TO, { autoCreate: DEV_MODE === true });
  if (defaultAccount && (!defaultAccount.id || !defaultAccount.accountId)) changed = true;
  const defaultAccountId = defaultAccount ? String(defaultAccount.id || defaultAccount.accountId) : accountIdFromTo(DEFAULT_DEV_TO);

  // Conversations
  for (const [key, convo] of Object.entries(data.conversations || {})) {
    if (!convo || convo.accountId) continue;
    const to = String(convo.to || deriveToFromConvoKey(key) || '').trim();
    let accountId = to ? inferAccountIdForTo(data, to, { autoCreate: DEV_MODE }) : null;
    if (!accountId) {
      if (DEV_MODE) {
        accountId = defaultAccountId;
        console.warn(`[tenant-migration] Conversation ${key} missing resolvable tenant, assigned ${accountId}`);
      }
    }
    if (assignOrMark(convo, accountId, 'Conversation', key, counts)) changed = true;
  }

  // Contacts
  for (const [key, contact] of Object.entries(data.contacts || {})) {
    if (!contact || contact.accountId) continue;
    const to = String(contact.to || deriveToFromContactKey(key) || '').trim();
    let accountId = to ? inferAccountIdForTo(data, to, { autoCreate: DEV_MODE }) : null;
    if (!accountId) {
      if (DEV_MODE) {
        accountId = defaultAccountId;
        console.warn(`[tenant-migration] Contact ${key} missing resolvable tenant, assigned ${accountId}`);
      }
    }
    if (assignOrMark(contact, accountId, 'Contact', key, counts)) changed = true;
  }

  // Flows
  for (const [flowId, flow] of Object.entries(data.flows || {})) {
    if (!flow || flow.accountId) continue;
    let accountId = null;
    if (flow.to) accountId = inferAccountIdForTo(data, flow.to, { autoCreate: DEV_MODE });
    if (!accountId) {
      if (DEV_MODE) {
        accountId = defaultAccountId;
        console.warn(`[tenant-migration] Flow ${flowId} missing resolvable tenant, assigned ${accountId}`);
      }
    }
    if (assignOrMark(flow, accountId, 'Flow', flowId, counts)) changed = true;
  }

  // Rules in keyed buckets by "to"
  if (data.rules && !Array.isArray(data.rules) && typeof data.rules === 'object') {
    for (const [to, rules] of Object.entries(data.rules)) {
      const inferred = inferAccountIdForTo(data, to, { autoCreate: DEV_MODE });
      const accountId = inferred || (DEV_MODE ? defaultAccountId : null);
      for (const rule of Array.isArray(rules) ? rules : []) {
        if (!rule || rule.accountId) continue;
        if (assignOrMark(rule, accountId, 'Rule', `${to}::${rule.id || 'unknown'}`, counts)) changed = true;
      }
    }
  }

  // VIP entries in keyed buckets by "to"
  if (data.vipList && typeof data.vipList === 'object') {
    for (const [to, entries] of Object.entries(data.vipList)) {
      const inferred = inferAccountIdForTo(data, to, { autoCreate: DEV_MODE });
      const accountId = inferred || (DEV_MODE ? defaultAccountId : null);
      for (const entry of Array.isArray(entries) ? entries : []) {
        if (!entry || entry.accountId) continue;
        if (assignOrMark(entry, accountId, 'VIP', `${to}::${entry.phone || 'unknown'}`, counts)) changed = true;
      }
    }
  }

  // Scheduled jobs
  for (const job of Array.isArray(data.scheduledJobs) ? data.scheduledJobs : []) {
    if (!job || job.accountId) continue;
    const to = String(job.to || '').trim();
    let accountId = to ? inferAccountIdForTo(data, to, { autoCreate: DEV_MODE }) : null;
    if (!accountId && DEV_MODE) accountId = defaultAccountId;
    if (assignOrMark(job, accountId, 'ScheduledJob', job.id || 'unknown', counts)) changed = true;
  }

  if (migrateNamespacedFlows(data, counts)) changed = true;

  if (changed) {
    saveDataDebounced(data);
    console.log(`[tenant-migration] accountId backfill complete: assigned=${counts.assigned} orphaned=${counts.orphaned} flowsMoved=${counts.flowsMoved} flowCollisions=${counts.flowCollisions}`);
  } else {
    console.log('[tenant-migration] no accountId backfill needed');
  }
}

module.exports = {
  backfillTenantAccountIds
};
