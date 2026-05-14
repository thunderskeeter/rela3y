const { mapMessageRow } = require('./messagesRepo');

function toDbTimestamp(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return null;
  return new Date(n);
}

function asObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? { ...value } : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function pickTimestampMillis(primary, fallback) {
  return toMillis(primary) ?? toFiniteOrNull(fallback);
}

function toMillis(value) {
  if (!value) return null;
  const date = new Date(value);
  const time = date.getTime();
  return Number.isFinite(time) ? time : null;
}

function toFiniteOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toJsonbValue(value, fallback) {
  const resolved = value == null ? fallback : value;
  return JSON.stringify(resolved);
}

function mapConversationRow(row) {
  const payload = asObject(row?.payload);
  const messages = Array.isArray(row?.messages_json) ? row.messages_json.map(mapMessageRow) : [];
  const createdAt = toMillis(row?.created_at);
  const updatedAt = toMillis(row?.updated_at);
  const lastActivityAt = toMillis(row?.last_activity_at);
  const canonicalFlow = asObject(row?.flow_state);
  const canonicalAudit = asArray(row?.audit_entries);
  const canonicalLeadData = asObject(row?.lead_data);
  const canonicalFields = asObject(row?.fields_data);
  return {
    ...payload,
    id: String(row?.convo_key || row?.id || ''),
    convoKey: String(row?.convo_key || row?.id || ''),
    rowId: row?.row_id ? String(row.row_id) : '',
    accountId: String(row?.tenant_id || ''),
    tenantId: String(row?.tenant_id || ''),
    to: String(row?.to_number || ''),
    from: String(row?.from_number || ''),
    status: String(row?.status || 'new'),
    stage: String(row?.stage || 'ask_service'),
    createdAt,
    updatedAt,
    lastActivityAt,
    audit: canonicalAudit,
    leadData: canonicalLeadData,
    flow: Object.keys(canonicalFlow).length ? canonicalFlow : null,
    fields: canonicalFields,
    bookingTime: pickTimestampMillis(row?.booking_time, null),
    bookingEndTime: pickTimestampMillis(row?.booking_end_time, null),
    amount: toFiniteOrNull(row?.amount_value),
    bookingAmount: toFiniteOrNull(row?.amount_value),
    resolvedAmount: toFiniteOrNull(row?.amount_value),
    resolvedAmountCents: row?.amount_value != null ? Math.round(Number(row.amount_value) * 100) : null,
    closedAt: pickTimestampMillis(row?.closed_at, null),
    paymentStatus: row?.payment_status ? String(row.payment_status) : '',
    messages
  };
}

function conversationSelectSql(whereClause) {
  return `
    SELECT
      c.id,
      c.row_id,
      c.convo_key,
      c.tenant_id,
      c.to_number,
      c.from_number,
      c.status,
      c.stage,
      c.payload,
      c.flow_state,
      c.audit_entries,
      c.lead_data,
      c.fields_data,
      c.booking_time,
      c.booking_end_time,
      c.amount_value,
      c.payment_status,
      c.closed_at,
      c.created_at,
      c.updated_at,
      c.last_activity_at,
      COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'id', m.id,
            'tenant_id', m.tenant_id,
            'conversation_id', m.conversation_id,
            'direction', m.direction,
            'body', m.body,
            'status', m.status,
            'idempotency_key', m.idempotency_key,
            'retry_count', m.retry_count,
            'last_attempt_at', m.last_attempt_at,
            'updated_at', m.updated_at,
            'failure_code', m.failure_code,
            'failure_reason', m.failure_reason,
            'next_retry_at', m.next_retry_at,
            'provider_message_id', m.provider_message_id,
            'queued_at', m.queued_at,
            'sent_at', m.sent_at,
            'delivered_at', m.delivered_at,
            'failed_at', m.failed_at,
            'first_provider_callback_at', m.first_provider_callback_at,
            'last_status_event_at', m.last_status_event_at,
            'to_number', m.to_number,
            'from_number', m.from_number,
            'payload', m.payload,
            'created_at', m.created_at
          )
          ORDER BY m.created_at ASC, m.id ASC
        ) FILTER (WHERE m.id IS NOT NULL),
        '[]'::jsonb
      ) AS messages_json
    FROM conversations c
    LEFT JOIN messages m
      ON m.tenant_id = c.tenant_id
     AND m.conversation_id = c.id
    ${whereClause}
    GROUP BY
      c.id, c.row_id, c.convo_key, c.tenant_id, c.to_number, c.from_number,
      c.status, c.stage, c.payload, c.flow_state, c.audit_entries, c.lead_data, c.fields_data,
      c.booking_time, c.booking_end_time, c.amount_value, c.payment_status, c.closed_at,
      c.created_at, c.updated_at, c.last_activity_at
  `;
}

async function listByTenant(db, accountId) {
  const result = await db.query(
    `
      ${conversationSelectSql('WHERE c.tenant_id = $1')}
      ORDER BY COALESCE(c.last_activity_at, c.updated_at) DESC, c.id ASC
    `,
    [String(accountId)]
  );
  return result.rows.map(mapConversationRow);
}

async function getByConvoKey(db, accountId, convoKey) {
  const result = await db.query(
    `
      ${conversationSelectSql('WHERE c.tenant_id = $1 AND c.convo_key = $2')}
      LIMIT 1
    `,
    [String(accountId), String(convoKey)]
  );
  return result.rowCount ? mapConversationRow(result.rows[0]) : null;
}

async function getByRowId(db, accountId, rowId) {
  const result = await db.query(
    `
      ${conversationSelectSql('WHERE c.tenant_id = $1 AND c.row_id = $2')}
      LIMIT 1
    `,
    [String(accountId), String(rowId)]
  );
  return result.rowCount ? mapConversationRow(result.rows[0]) : null;
}

async function createIfMissing(db, accountId, input = {}) {
  const payload = input?.payload && typeof input.payload === 'object' ? { ...input.payload } : {};
  const convoKey = String(input?.convoKey || payload.convoKey || payload.id || '');
  const result = await db.query(
    `
      INSERT INTO conversations (
        id, tenant_id, to_number, from_number, status, stage, payload,
        flow_state, audit_entries, lead_data, fields_data, booking_time, booking_end_time,
        amount_value, payment_status, closed_at, created_at, updated_at, last_activity_at, convo_key
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb,$12,$13,$14,$15,$16,
        COALESCE($17, NOW()),
        COALESCE($18, NOW()),
        $19,
        $20
      )
      ON CONFLICT (id)
      DO UPDATE SET tenant_id = conversations.tenant_id
      RETURNING id, row_id, convo_key, tenant_id, to_number, from_number, status, stage,
                payload, flow_state, audit_entries, lead_data, fields_data, booking_time, booking_end_time,
                amount_value, payment_status, closed_at, created_at, updated_at, last_activity_at
    `,
    [
      convoKey,
      String(accountId),
      String(input?.to || payload.to || ''),
      String(input?.from || payload.from || ''),
      String(input?.status || payload.status || 'new'),
      String(input?.stage || payload.stage || 'ask_service'),
      toJsonbValue(payload, {}),
      toJsonbValue(asObject(input?.flow ?? payload?.flow), {}),
      toJsonbValue(asArray(input?.audit ?? payload?.audit), []),
      toJsonbValue(asObject(input?.leadData ?? payload?.leadData), {}),
      toJsonbValue(asObject(input?.fields ?? payload?.fields), {}),
      toDbTimestamp(input?.bookingTime ?? payload?.bookingTime ?? null),
      toDbTimestamp(input?.bookingEndTime ?? payload?.bookingEndTime ?? null),
      input?.amount ?? payload?.amount ?? null,
      input?.paymentStatus ? String(input.paymentStatus) : (payload?.paymentStatus ? String(payload.paymentStatus) : null),
      toDbTimestamp(input?.closedAt ?? payload?.closedAt ?? null),
      toDbTimestamp(input?.createdAt ?? payload?.createdAt ?? Date.now()),
      toDbTimestamp(input?.updatedAt ?? payload?.updatedAt ?? Date.now()),
      toDbTimestamp(input?.lastActivityAt ?? payload?.lastActivityAt ?? null),
      convoKey
    ]
  );
  return mapConversationRow({ ...result.rows[0], messages_json: [] });
}

async function updateByConvoKey(db, accountId, convoKey, patch = {}) {
  const currentResult = await db.query(
    `
      SELECT id, row_id, convo_key, tenant_id, to_number, from_number, status, stage,
             payload, flow_state, audit_entries, lead_data, fields_data, booking_time, booking_end_time,
             amount_value, payment_status, closed_at, created_at, updated_at, last_activity_at
      FROM conversations
      WHERE tenant_id = $1 AND convo_key = $2
      LIMIT 1
    `,
    [String(accountId), String(convoKey)]
  );
  if (!currentResult.rowCount) return null;
  const currentRow = currentResult.rows[0];
  const payloadPatch = patch?.payload && typeof patch.payload === 'object' ? patch.payload : {};
  const mergedPayload = {
    ...asObject(currentRow?.payload),
    ...payloadPatch
  };
  const result = await db.query(
    `
      UPDATE conversations
      SET
        to_number = COALESCE($3, to_number),
        from_number = COALESCE($4, from_number),
        status = COALESCE($5, status),
        stage = COALESCE($6, stage),
        payload = $7::jsonb,
        flow_state = COALESCE($8::jsonb, flow_state),
        audit_entries = COALESCE($9::jsonb, audit_entries),
        lead_data = COALESCE($10::jsonb, lead_data),
        fields_data = COALESCE($11::jsonb, fields_data),
        booking_time = COALESCE($12, booking_time),
        booking_end_time = COALESCE($13, booking_end_time),
        amount_value = COALESCE($14, amount_value),
        payment_status = COALESCE($15, payment_status),
        closed_at = COALESCE($16, closed_at),
        updated_at = COALESCE($17, NOW()),
        last_activity_at = COALESCE($18, last_activity_at)
      WHERE tenant_id = $1 AND convo_key = $2
      RETURNING id, row_id, convo_key, tenant_id, to_number, from_number, status, stage,
                payload, flow_state, audit_entries, lead_data, fields_data, booking_time, booking_end_time,
                amount_value, payment_status, closed_at, created_at, updated_at, last_activity_at
    `,
    [
      String(accountId),
      String(convoKey),
      patch?.to ? String(patch.to) : null,
      patch?.from ? String(patch.from) : null,
      patch?.status ? String(patch.status) : null,
      patch?.stage ? String(patch.stage) : null,
      toJsonbValue(mergedPayload, {}),
      patch?.flow && typeof patch.flow === 'object' ? toJsonbValue(patch.flow, {}) : null,
      Array.isArray(patch?.audit) ? toJsonbValue(patch.audit, []) : null,
      patch?.leadData && typeof patch.leadData === 'object' ? toJsonbValue(patch.leadData, {}) : null,
      patch?.fields && typeof patch.fields === 'object' ? toJsonbValue(patch.fields, {}) : null,
      toDbTimestamp(patch?.bookingTime ?? null),
      toDbTimestamp(patch?.bookingEndTime ?? null),
      patch?.amount != null ? Number(patch.amount) : null,
      patch?.paymentStatus ? String(patch.paymentStatus) : null,
      toDbTimestamp(patch?.closedAt ?? null),
      toDbTimestamp(patch?.updatedAt ?? Date.now()),
      toDbTimestamp(patch?.lastActivityAt ?? null)
    ]
  );
  return result.rowCount ? mapConversationRow({ ...result.rows[0], messages_json: [] }) : null;
}

async function deleteByConvoKey(db, accountId, convoKey) {
  const result = await db.query(
    `
      DELETE FROM conversations
      WHERE tenant_id = $1 AND convo_key = $2
      RETURNING id, row_id, convo_key, tenant_id, to_number, from_number, status, stage,
                payload, flow_state, audit_entries, lead_data, fields_data, booking_time, booking_end_time,
                amount_value, payment_status, closed_at, created_at, updated_at, last_activity_at
    `,
    [String(accountId), String(convoKey)]
  );
  return result.rowCount ? mapConversationRow({ ...result.rows[0], messages_json: [] }) : null;
}

module.exports = {
  listByTenant,
  getByConvoKey,
  getByRowId,
  createIfMissing,
  updateByConvoKey,
  deleteByConvoKey,
  mapConversationRow
};
