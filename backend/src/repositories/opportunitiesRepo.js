function mapOpportunityRow(row) {
  const payload = row?.payload && typeof row.payload === 'object' ? { ...row.payload } : {};
  return {
    ...payload,
    id: String(row?.id || payload.id || ''),
    accountId: String(row?.tenant_id || payload.accountId || payload.tenantId || ''),
    tenantId: String(row?.tenant_id || payload.tenantId || payload.accountId || ''),
    convoKey: row?.conversation_id ? String(row.conversation_id) : String(payload.convoKey || payload.conversationId || ''),
    conversationId: row?.conversation_id ? String(row.conversation_id) : String(payload.conversationId || payload.convoKey || ''),
    stage: String(row?.stage || payload.stage || 'NEW'),
    riskScore: Number(row?.risk_score ?? payload.riskScore ?? 0),
    createdAt: payload.createdAt ? Number(payload.createdAt) : (row?.created_at ? new Date(row.created_at).getTime() : null),
    updatedAt: payload.updatedAt ? Number(payload.updatedAt) : (row?.updated_at ? new Date(row.updated_at).getTime() : null)
  };
}

function normalizeOpportunityRows(rows) {
  return Array.isArray(rows) ? rows.map(mapOpportunityRow) : [];
}

function toDbTimestamp(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return null;
  return new Date(n);
}

async function getRowById(db, accountId, opportunityId) {
  const result = await db.query(
    `
      SELECT id, tenant_id, conversation_id, stage, risk_score, payload, created_at, updated_at
      FROM opportunities
      WHERE tenant_id = $1 AND id = $2
      LIMIT 1
    `,
    [String(accountId), String(opportunityId)]
  );
  return result.rowCount ? result.rows[0] : null;
}

async function listByTenant(db, accountId) {
  const result = await db.query(
    `
      SELECT id, tenant_id, conversation_id, stage, risk_score, payload, created_at, updated_at
      FROM opportunities
      WHERE tenant_id = $1
      ORDER BY updated_at DESC, id ASC
    `,
    [String(accountId)]
  );
  return normalizeOpportunityRows(result.rows);
}

async function getById(db, accountId, opportunityId) {
  const row = await getRowById(db, accountId, opportunityId);
  return row ? mapOpportunityRow(row) : null;
}

async function create(db, accountId, input) {
  const payload = input?.payload && typeof input.payload === 'object' ? { ...input.payload } : {};
  const tenantId = String(accountId);
  const opportunityId = String(input?.id || payload.id || '');
  const stage = String(input?.stage || payload.stage || 'NEW');
  const riskScore = Number(input?.riskScore ?? payload.riskScore ?? 0);
  const createdAt = toDbTimestamp(input?.createdAt ?? payload.createdAt ?? Date.now());
  const updatedAt = toDbTimestamp(input?.updatedAt ?? payload.updatedAt ?? Date.now());
  const result = await db.query(
    `
      INSERT INTO opportunities (id, tenant_id, conversation_id, stage, risk_score, payload, created_at, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6::jsonb,COALESCE($7, NOW()),COALESCE($8, NOW()))
      RETURNING id, tenant_id, conversation_id, stage, risk_score, payload, created_at, updated_at
    `,
    [
      opportunityId,
      tenantId,
      null,
      stage,
      riskScore,
      payload,
      createdAt,
      updatedAt
    ]
  );
  return mapOpportunityRow(result.rows[0]);
}

async function updateById(db, accountId, opportunityId, patch = {}) {
  const currentRow = await getRowById(db, accountId, opportunityId);
  if (!currentRow) return null;

  const currentPayload = currentRow.payload && typeof currentRow.payload === 'object' ? { ...currentRow.payload } : {};
  const payloadPatch = patch?.payload && typeof patch.payload === 'object' ? patch.payload : {};
  const mergedPayload = { ...currentPayload, ...payloadPatch };
  const stage = Object.prototype.hasOwnProperty.call(patch, 'stage')
    ? String(patch.stage || 'NEW')
    : String(currentRow.stage || mergedPayload.stage || 'NEW');
  const riskScore = Object.prototype.hasOwnProperty.call(patch, 'riskScore')
    ? Number(patch.riskScore ?? 0)
    : Number(currentRow.risk_score ?? mergedPayload.riskScore ?? 0);
  const updatedAt = toDbTimestamp(patch?.updatedAt ?? mergedPayload.updatedAt ?? Date.now());

  const result = await db.query(
    `
      UPDATE opportunities
      SET
        conversation_id = $3,
        stage = $4,
        risk_score = $5,
        payload = $6::jsonb,
        updated_at = COALESCE($7, NOW())
      WHERE tenant_id = $1 AND id = $2
      RETURNING id, tenant_id, conversation_id, stage, risk_score, payload, created_at, updated_at
    `,
    [
      String(accountId),
      String(opportunityId),
      null,
      stage,
      riskScore,
      mergedPayload,
      updatedAt
    ]
  );
  return result.rowCount ? mapOpportunityRow(result.rows[0]) : null;
}

module.exports = {
  listByTenant,
  getById,
  create,
  updateById,
  mapOpportunityRow
};
