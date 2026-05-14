function toTimestampExpression(paramIndex) {
  return `TO_TIMESTAMP($${paramIndex}::double precision / 1000.0)`;
}

function mapContactRow(row) {
  const payload = row?.payload && typeof row.payload === 'object' ? { ...row.payload } : {};
  return {
    ...payload,
    id: String(payload.id || row?.id || ''),
    accountId: String(payload.accountId || row?.tenant_id || ''),
    tenantId: String(row?.tenant_id || payload.tenantId || payload.accountId || ''),
    phone: String(payload.phone || row?.phone || ''),
    name: String(payload.name || row?.name || ''),
    tags: Array.isArray(payload.tags) ? payload.tags : (Array.isArray(row?.tags) ? row.tags : []),
    createdAt: payload.createdAt ? Number(payload.createdAt) : (row?.created_at ? new Date(row.created_at).getTime() : null),
    updatedAt: payload.updatedAt ? Number(payload.updatedAt) : (row?.updated_at ? new Date(row.updated_at).getTime() : null)
  };
}

function normalizeContactRows(rows) {
  return Array.isArray(rows) ? rows.map(mapContactRow) : [];
}

async function listByTenant(db, accountId) {
  const result = await db.query(
    `
      SELECT id, tenant_id, phone, name, tags, payload, created_at, updated_at
      FROM contacts
      WHERE tenant_id = $1
      ORDER BY updated_at DESC, id ASC
    `,
    [String(accountId)]
  );
  return normalizeContactRows(result.rows);
}

async function listByPhones(db, accountId, phones) {
  const normalizedPhones = Array.from(new Set((Array.isArray(phones) ? phones : []).map((phone) => String(phone || '').trim()).filter(Boolean)));
  if (!normalizedPhones.length) return [];
  const result = await db.query(
    `
      SELECT id, tenant_id, phone, name, tags, payload, created_at, updated_at
      FROM contacts
      WHERE tenant_id = $1
        AND phone = ANY($2::text[])
      ORDER BY updated_at DESC, id ASC
    `,
    [String(accountId), normalizedPhones]
  );
  return normalizeContactRows(result.rows);
}

async function getByPhone(db, accountId, phone) {
  const result = await db.query(
    `
      SELECT id, tenant_id, phone, name, tags, payload, created_at, updated_at
      FROM contacts
      WHERE tenant_id = $1 AND phone = $2
      LIMIT 1
    `,
    [String(accountId), String(phone)]
  );
  return result.rowCount ? mapContactRow(result.rows[0]) : null;
}

async function upsertByPhone(db, accountId, input) {
  const result = await db.query(
    `
      INSERT INTO contacts (
        id, tenant_id, phone, name, tags, payload, created_at, updated_at
      )
      VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, ${toTimestampExpression(7)}, ${toTimestampExpression(8)})
      ON CONFLICT (tenant_id, phone)
      DO UPDATE SET
        name = EXCLUDED.name,
        tags = EXCLUDED.tags,
        payload = EXCLUDED.payload,
        updated_at = EXCLUDED.updated_at
      RETURNING id, tenant_id, phone, name, tags, payload, created_at, updated_at
    `,
    [
      String(input?.id || ''),
      String(accountId),
      String(input?.phone || ''),
      String(input?.name || ''),
      JSON.stringify(Array.isArray(input?.tags) ? input.tags : []),
      JSON.stringify(input?.payload || {}),
      Number(input?.createdAt || Date.now()),
      Number(input?.updatedAt || Date.now())
    ]
  );
  return result.rowCount ? mapContactRow(result.rows[0]) : null;
}

async function bulkUpsertByPhone(db, accountId, items) {
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) return [];

  const valuesSql = [];
  const params = [];
  for (let index = 0; index < rows.length; index += 1) {
    const item = rows[index] || {};
    const base = (index * 8) + 1;
    valuesSql.push(
      `($${base}, $${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}::jsonb, $${base + 5}::jsonb, ${toTimestampExpression(base + 6)}, ${toTimestampExpression(base + 7)})`
    );
    params.push(
      String(item?.id || ''),
      String(accountId),
      String(item?.phone || ''),
      String(item?.name || ''),
      JSON.stringify(Array.isArray(item?.tags) ? item.tags : []),
      JSON.stringify(item?.payload || {}),
      Number(item?.createdAt || Date.now()),
      Number(item?.updatedAt || Date.now())
    );
  }

  const result = await db.query(
    `
      INSERT INTO contacts (
        id, tenant_id, phone, name, tags, payload, created_at, updated_at
      )
      VALUES ${valuesSql.join(', ')}
      ON CONFLICT (tenant_id, phone)
      DO UPDATE SET
        name = EXCLUDED.name,
        tags = EXCLUDED.tags,
        payload = EXCLUDED.payload,
        updated_at = EXCLUDED.updated_at
      RETURNING id, tenant_id, phone, name, tags, payload, created_at, updated_at
    `,
    params
  );
  return normalizeContactRows(result.rows);
}

module.exports = {
  listByTenant,
  listByPhones,
  getByPhone,
  upsertByPhone,
  bulkUpsertByPhone,
  mapContactRow
};
