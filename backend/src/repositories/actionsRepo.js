function mapActionRow(row) {
  const payload = row?.payload && typeof row.payload === 'object' ? { ...row.payload } : {};
  const createdAt = row?.created_at ? new Date(row.created_at).getTime() : null;
  return {
    ...payload,
    id: String(row?.id || payload.id || ''),
    accountId: String(row?.tenant_id || payload.accountId || payload.tenantId || ''),
    tenantId: String(row?.tenant_id || payload.tenantId || payload.accountId || ''),
    opportunityId: row?.opportunity_id ? String(row.opportunity_id) : (payload.opportunityId ? String(payload.opportunityId) : null),
    convoKey: row?.conversation_id ? String(row.conversation_id) : String(payload.convoKey || payload.conversationId || ''),
    conversationId: row?.conversation_id ? String(row.conversation_id) : String(payload.conversationId || payload.convoKey || ''),
    actionType: String(row?.action_type || payload.actionType || 'unknown_action'),
    status: String(row?.status || payload.status || payload?.outcome?.status || 'pending'),
    idempotencyKey: row?.idempotency_key ? String(row.idempotency_key) : (payload.idempotencyKey ? String(payload.idempotencyKey) : null),
    ts: payload.ts ? Number(payload.ts) : createdAt,
    createdAt
  };
}

function normalizeActionRows(rows) {
  return Array.isArray(rows) ? rows.map(mapActionRow) : [];
}

function toDbTimestamp(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return null;
  return new Date(n);
}

async function getRowById(db, accountId, actionId) {
  const result = await db.query(
    `
      SELECT id, tenant_id, opportunity_id, conversation_id, action_type, status, idempotency_key, payload, created_at
      FROM actions
      WHERE tenant_id = $1 AND id = $2
      LIMIT 1
    `,
    [String(accountId), String(actionId)]
  );
  return result.rowCount ? result.rows[0] : null;
}

async function listRecentByTenant(db, accountId, limit) {
  const result = await db.query(
    `
      SELECT id, tenant_id, opportunity_id, conversation_id, action_type, status, idempotency_key, payload, created_at
      FROM actions
      WHERE tenant_id = $1
      ORDER BY created_at DESC, id DESC
      LIMIT $2
    `,
    [String(accountId), Number(limit)]
  );
  return normalizeActionRows(result.rows);
}

async function listByOpportunity(db, accountId, opportunityId) {
  const result = await db.query(
    `
      SELECT id, tenant_id, opportunity_id, conversation_id, action_type, status, idempotency_key, payload, created_at
      FROM actions
      WHERE tenant_id = $1 AND opportunity_id = $2
      ORDER BY created_at DESC, id DESC
    `,
    [String(accountId), String(opportunityId)]
  );
  return normalizeActionRows(result.rows);
}

async function listByRunIds(db, accountId, runIds) {
  const scopedRunIds = Array.from(new Set((Array.isArray(runIds) ? runIds : []).map((runId) => String(runId || '').trim()).filter(Boolean)));
  if (!scopedRunIds.length) return [];
  const result = await db.query(
    `
      SELECT id, tenant_id, opportunity_id, conversation_id, action_type, status, idempotency_key, payload, created_at
      FROM actions
      WHERE tenant_id = $1
        AND COALESCE(payload->>'runId', '') = ANY($2::text[])
      ORDER BY created_at DESC, id DESC
    `,
    [String(accountId), scopedRunIds]
  );
  return normalizeActionRows(result.rows);
}

async function create(db, accountId, input) {
  const payload = input?.payload && typeof input.payload === 'object' ? { ...input.payload } : {};
  const params = [
    String(input?.id || payload.id || ''),
    String(accountId),
    input?.opportunityId ? String(input.opportunityId) : (payload.opportunityId ? String(payload.opportunityId) : null),
    null,
    String(input?.actionType || payload.actionType || 'unknown_action'),
    String(input?.status || payload.status || payload?.outcome?.status || 'pending'),
    input?.idempotencyKey ? String(input.idempotencyKey) : (payload.idempotencyKey ? String(payload.idempotencyKey) : null),
    payload,
    toDbTimestamp(input?.createdAt ?? payload.createdAt ?? payload.ts ?? Date.now())
  ];

  const hasIdempotency = Boolean(params[6]);
  const sql = hasIdempotency
    ? `
      INSERT INTO actions (id, tenant_id, opportunity_id, conversation_id, action_type, status, idempotency_key, payload, created_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,COALESCE($9, NOW()))
      ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL
      DO UPDATE SET tenant_id = actions.tenant_id
      RETURNING id, tenant_id, opportunity_id, conversation_id, action_type, status, idempotency_key, payload, created_at
    `
    : `
      INSERT INTO actions (id, tenant_id, opportunity_id, conversation_id, action_type, status, idempotency_key, payload, created_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,COALESCE($9, NOW()))
      RETURNING id, tenant_id, opportunity_id, conversation_id, action_type, status, idempotency_key, payload, created_at
    `;
  const result = await db.query(sql, params);
  return mapActionRow(result.rows[0]);
}

async function updateById(db, accountId, actionId, patch = {}) {
  const currentRow = await getRowById(db, accountId, actionId);
  if (!currentRow) return null;
  const currentPayload = currentRow.payload && typeof currentRow.payload === 'object' ? { ...currentRow.payload } : {};
  const payloadPatch = patch?.payload && typeof patch.payload === 'object' ? patch.payload : {};
  const mergedPayload = { ...currentPayload, ...payloadPatch };
  const status = Object.prototype.hasOwnProperty.call(patch, 'status')
    ? String(patch.status || 'pending')
    : String(currentRow.status || mergedPayload.status || mergedPayload?.outcome?.status || 'pending');
  const result = await db.query(
    `
      UPDATE actions
      SET
        status = $3,
        payload = $4::jsonb
      WHERE tenant_id = $1 AND id = $2
      RETURNING id, tenant_id, opportunity_id, conversation_id, action_type, status, idempotency_key, payload, created_at
    `,
    [String(accountId), String(actionId), status, mergedPayload]
  );
  return result.rowCount ? mapActionRow(result.rows[0]) : null;
}

async function getById(db, accountId, actionId) {
  const row = await getRowById(db, accountId, actionId);
  return row ? mapActionRow(row) : null;
}

async function hasSuccessfulByIdempotency(db, accountId, idempotencyKey) {
  const key = String(idempotencyKey || '').trim();
  if (!key) return false;
  const result = await db.query(
    `
      SELECT 1
      FROM actions
      WHERE tenant_id = $1
        AND idempotency_key = $2
        AND status = 'sent'
      LIMIT 1
    `,
    [String(accountId), key]
  );
  return result.rowCount > 0;
}

module.exports = {
  listRecentByTenant,
  listByOpportunity,
  listByRunIds,
  create,
  updateById,
  getById,
  hasSuccessfulByIdempotency,
  mapActionRow
};
