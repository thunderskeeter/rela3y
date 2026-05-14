// DEPRECATED: runtime snapshot persistence layer.
// Keep only until all request/background paths are repository-backed.
const { pool } = require('./pool');
const { USE_DB_CONVERSATIONS, USE_DB_MESSAGES } = require('../config/runtime');

function dbMessagingEnabled() {
  return USE_DB_CONVERSATIONS === true || USE_DB_MESSAGES === true;
}

function stripMessagingFromSnapshot(snapshot) {
  const base = snapshot && typeof snapshot === 'object' ? { ...snapshot } : emptyState();
  base.conversations = {};
  return base;
}

function emptyState() {
  return {
    conversations: {},
    rules: [],
    accounts: {},
    flows: {},
    contacts: {},
    scheduledJobs: [],
    vipList: {},
    users: [],
    sessions: {},
    dev: null,
    leadEvents: [],
    revenueOpportunities: [],
    leadIntelligence: {},
    alerts: [],
    actions: [],
    optimizationEvents: [],
    agentRuns: [],
    reviewQueue: [],
    revenueEvents: [],
    workspaceRequests: []
  };
}

async function loadStateSnapshot() {
  const result = await pool.query('SELECT snapshot FROM app_state WHERE id = 1');
  if (!result.rowCount) return emptyState();
  const snapshot = result.rows[0]?.snapshot;
  if (!snapshot || typeof snapshot !== 'object') return emptyState();
  return dbMessagingEnabled() ? stripMessagingFromSnapshot(snapshot) : snapshot;
}

function accountIdFromAccount(account, fallbackTo) {
  const id = String(account?.accountId || account?.id || '').trim();
  if (id) return id;
  const digits = String(fallbackTo || '').replace(/[^\d]/g, '');
  return `acct_${digits || 'unknown'}`;
}

function ts(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return null;
  return new Date(n);
}

async function persistStateSnapshot(snapshot) {
  const persistedSnapshot = dbMessagingEnabled() ? stripMessagingFromSnapshot(snapshot) : snapshot;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `
      INSERT INTO app_state (id, snapshot, updated_at)
      VALUES (1, $1::jsonb, NOW())
      ON CONFLICT (id)
      DO UPDATE SET snapshot = EXCLUDED.snapshot, updated_at = NOW();
      `,
      [persistedSnapshot]
    );

    const accounts = Object.entries(persistedSnapshot?.accounts || {});
    const tenantIdByTo = new Map();
    for (const [to, account] of accounts) {
      const tenantId = accountIdFromAccount(account, to);
      tenantIdByTo.set(String(to), tenantId);
      await client.query(
        `
        INSERT INTO tenants (
          id, to_number, business_name, workspace, settings, defaults, compliance, billing, integrations, updated_at
        )
        VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,NOW())
        ON CONFLICT (id)
        DO UPDATE SET
          to_number = EXCLUDED.to_number,
          business_name = EXCLUDED.business_name,
          workspace = EXCLUDED.workspace,
          settings = EXCLUDED.settings,
          defaults = EXCLUDED.defaults,
          compliance = EXCLUDED.compliance,
          billing = EXCLUDED.billing,
          integrations = EXCLUDED.integrations,
          updated_at = NOW();
        `,
        [
          tenantId,
          String(to),
          String(account?.businessName || account?.workspace?.identity?.businessName || ''),
          account?.workspace || {},
          account?.settings || {},
          account?.defaults || {},
          account?.compliance || {},
          account?.billing || {},
          account?.integrations || {}
        ]
      );
    }

    await client.query('DELETE FROM user_tenants');
    for (const user of Array.isArray(persistedSnapshot?.users) ? persistedSnapshot.users : []) {
      const userId = String(user?.id || '').trim();
      const roleId = String(user?.role || 'readonly').trim().toLowerCase() || 'readonly';
      if (!userId) continue;
      await client.query(
        `
        INSERT INTO users (id, email, password_hash, role_id, disabled, created_at, last_login_at)
        VALUES ($1,$2,$3,$4,$5,COALESCE($6,NOW()),$7)
        ON CONFLICT (id)
        DO UPDATE SET
          email = EXCLUDED.email,
          password_hash = EXCLUDED.password_hash,
          role_id = EXCLUDED.role_id,
          disabled = EXCLUDED.disabled,
          last_login_at = EXCLUDED.last_login_at;
        `,
        [
          userId,
          String(user?.email || '').toLowerCase(),
          String(user?.passwordHash || ''),
          roleId,
          user?.disabled === true,
          ts(user?.createdAt),
          ts(user?.lastLoginAt)
        ]
      );
      for (const accountId of Array.isArray(user?.accountIds) ? user.accountIds : []) {
        const tid = String(accountId || '').trim();
        if (!tid) continue;
        await client.query(
          `INSERT INTO user_tenants (user_id, tenant_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
          [userId, tid]
        );
      }
    }

    const truncateTargets = dbMessagingEnabled()
      ? 'scheduled_jobs, contacts, opportunities, actions, audit_logs, billing_events'
      : 'messages, scheduled_jobs, contacts, conversations, opportunities, actions, audit_logs, billing_events';
    await client.query(`TRUNCATE ${truncateTargets} RESTART IDENTITY CASCADE`);

    for (const [key, contact] of Object.entries(persistedSnapshot?.contacts || {})) {
      const to = String(contact?.to || '').trim() || String(key).split('__')[0] || '';
      const tenantId = String(contact?.accountId || tenantIdByTo.get(to) || '').trim();
      if (!tenantId) continue;
      await client.query(
        `
        INSERT INTO contacts (id, tenant_id, phone, name, tags, payload, created_at, updated_at)
        VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,COALESCE($7,NOW()),COALESCE($8,NOW()))
        `,
        [
          String(key),
          tenantId,
          String(contact?.phone || ''),
          String(contact?.name || ''),
          JSON.stringify(Array.isArray(contact?.tags) ? contact.tags : []),
          contact || {},
          ts(contact?.createdAt),
          ts(contact?.updatedAt)
        ]
      );
    }

    if (!dbMessagingEnabled()) {
      for (const [id, convo] of Object.entries(persistedSnapshot?.conversations || {})) {
        const to = String(convo?.to || '').trim();
        const tenantId = String(convo?.accountId || tenantIdByTo.get(to) || '').trim();
        if (!tenantId) continue;
        await client.query(
          `
          INSERT INTO conversations (
            id, tenant_id, to_number, from_number, status, stage, payload, created_at, updated_at, last_activity_at, convo_key
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,COALESCE($8,NOW()),COALESCE($9,NOW()),$10,$11)
          `,
          [
            String(id),
            tenantId,
            to,
            String(convo?.from || ''),
            String(convo?.status || 'new'),
            String(convo?.stage || 'ask_service'),
            convo || {},
            ts(convo?.createdAt),
            ts(convo?.updatedAt),
            ts(convo?.lastActivityAt),
            String(id)
          ]
        );

        for (const auditEntry of Array.isArray(convo?.audit) ? convo.audit : []) {
          await client.query(
            `
            INSERT INTO audit_logs (tenant_id, actor_user_id, event_type, entity_type, entity_id, payload, created_at)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb,COALESCE($7,NOW()))
            `,
            [
              tenantId,
              auditEntry?.actorUserId ? String(auditEntry.actorUserId) : null,
              String(auditEntry?.type || 'conversation.audit'),
              'conversation',
              String(id),
              auditEntry || {},
              ts(auditEntry?.ts)
            ]
          );
        }
      }
    }

    for (const opp of Array.isArray(persistedSnapshot?.revenueOpportunities) ? persistedSnapshot.revenueOpportunities : []) {
      const tenantId = String(opp?.accountId || '').trim();
      const oppId = String(opp?.id || '').trim();
      if (!tenantId || !oppId) continue;
      const convoId = String(opp?.convoKey || '').trim() || null;
      await client.query(
        `
        INSERT INTO opportunities (id, tenant_id, conversation_id, stage, risk_score, payload, created_at, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6::jsonb,COALESCE($7,NOW()),COALESCE($8,NOW()))
        `,
        [
          oppId,
          tenantId,
          convoId,
          String(opp?.stage || 'NEW'),
          Number(opp?.riskScore || 0),
          opp || {},
          ts(opp?.createdAt),
          ts(opp?.updatedAt || opp?.lastActivityAt)
        ]
      );
    }

    const seenActionIds = new Set();
    const seenTenantIdempotency = new Set();
    for (const action of Array.isArray(persistedSnapshot?.actions) ? persistedSnapshot.actions : []) {
      const tenantId = String(action?.accountId || '').trim();
      const actionId = String(action?.id || '').trim();
      if (!tenantId || !actionId) continue;
      if (seenActionIds.has(actionId)) continue;
      const idemKey = action?.idempotencyKey ? String(action.idempotencyKey).trim() : '';
      const scopedIdem = idemKey ? `${tenantId}__${idemKey}` : '';
      if (scopedIdem && seenTenantIdempotency.has(scopedIdem)) continue;
      seenActionIds.add(actionId);
      if (scopedIdem) seenTenantIdempotency.add(scopedIdem);
      const status = String(action?.outcome?.status || 'pending');
      const convoId = String(action?.convoKey || '').trim() || null;
      await client.query(
        `
        INSERT INTO actions (
          id, tenant_id, opportunity_id, conversation_id, action_type, status, idempotency_key, payload, created_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,COALESCE($9,NOW()))
        ON CONFLICT (id) DO NOTHING
        `,
        [
          actionId,
          tenantId,
          action?.opportunityId ? String(action.opportunityId) : null,
          convoId,
          String(action?.actionType || 'unknown_action'),
          status,
          idemKey || null,
          action || {},
          ts(action?.ts)
        ]
      );
    }

    for (const account of Object.values(persistedSnapshot?.accounts || {})) {
      const tenantId = String(account?.accountId || account?.id || '').trim();
      if (!tenantId) continue;
      const events = Array.isArray(account?.billing?.activity) ? account.billing.activity : [];
      for (const event of events) {
        const eventId = String(event?.id || '').trim() || null;
        await client.query(
          `
          INSERT INTO billing_events (id, tenant_id, provider, event_type, external_event_id, payload, created_at)
          VALUES ($1,$2,$3,$4,$5,$6::jsonb,COALESCE($7,NOW()))
          ON CONFLICT (id) DO NOTHING
          `,
          [
            eventId || `be_${tenantId}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            tenantId,
            String(account?.billing?.provider || 'stripe'),
            String(event?.type || 'billing_activity'),
            event?.externalEventId ? String(event.externalEventId) : null,
            event || {},
            ts(event?.ts)
          ]
        );
      }
      const integrationLogs = Array.isArray(account?.integrationLogs) ? account.integrationLogs : [];
      for (const log of integrationLogs) {
        await client.query(
          `
          INSERT INTO audit_logs (tenant_id, actor_user_id, event_type, entity_type, entity_id, payload, created_at)
          VALUES ($1,$2,$3,$4,$5,$6::jsonb,COALESCE($7,NOW()))
          `,
          [
            tenantId,
            null,
            String(log?.type || 'integration.log'),
            'integration',
            String(log?.id || ''),
            log || {},
            ts(log?.ts)
          ]
        );
      }
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  emptyState,
  loadStateSnapshot,
  persistStateSnapshot
};

