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

function toMillis(value) {
  if (!value) return null;
  const date = new Date(value);
  const time = date.getTime();
  return Number.isFinite(time) ? time : null;
}

function toPositiveNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function buildProviderMeta(canonical = {}) {
  const merged = {
    provider: canonical.provider || null,
    sid: canonical.providerMessageId || '',
    deliveryStatus: canonical.status || '',
    errorCode: canonical.failureCode || '',
    errorMessage: canonical.failureReason || ''
  };
  return Object.values(merged).some((value) => value !== null && value !== '')
    ? merged
    : null;
}

function mapMessageRow(row) {
  const payload = asObject(row?.payload);
  const createdAt = toMillis(row?.created_at);
  const updatedAt = toMillis(row?.updated_at) ?? createdAt;
  const direction = String(row?.direction || 'outbound');
  const text = String(row?.body ?? '');
  const status = String(row?.status || 'sent');
  const providerMessageId = row?.provider_message_id ? String(row.provider_message_id) : '';
  const failureCode = row?.failure_code ? String(row.failure_code) : '';
  const failureReason = row?.failure_reason ? String(row.failure_reason) : '';
  const canonical = {
    providerMessageId,
    status,
    failureCode,
    failureReason,
    provider: providerMessageId ? 'twilio' : null
  };
  return {
    ...payload,
    id: String(row?.id || ''),
    accountId: String(row?.tenant_id || ''),
    tenantId: String(row?.tenant_id || ''),
    conversationId: String(row?.conversation_id || ''),
    convoKey: String(row?.conversation_id || ''),
    to: String(row?.to_number || ''),
    from: String(row?.from_number || ''),
    direction,
    dir: direction === 'outbound' ? 'out' : (direction === 'inbound' ? 'in' : ''),
    body: text,
    text,
    status,
    idempotencyKey: row?.idempotency_key ? String(row.idempotency_key) : '',
    retryCount: toPositiveNumber(row?.retry_count, 0),
    lastAttemptAt: toMillis(row?.last_attempt_at),
    failureCode,
    failureReason,
    nextRetryAt: toMillis(row?.next_retry_at),
    providerMessageId,
    source: String(payload?.meta?.source || payload?.source || ''),
    providerMeta: buildProviderMeta(canonical),
    meta: asObject(payload?.meta),
    attachments: asArray(payload?.attachments),
    ts: createdAt,
    createdAt,
    updatedAt,
    queuedAt: toMillis(row?.queued_at),
    sentAt: toMillis(row?.sent_at),
    deliveredAt: toMillis(row?.delivered_at),
    failedAt: toMillis(row?.failed_at),
    firstProviderCallbackAt: toMillis(row?.first_provider_callback_at),
    lastStatusEventAt: toMillis(row?.last_status_event_at)
  };
}

async function listByConversation(db, accountId, convoKey, pagination = {}) {
  const limit = Math.max(1, Math.min(500, Number(pagination?.limit || 500)));
  const result = await db.query(
    `
      SELECT id, tenant_id, conversation_id, direction, body, status, idempotency_key,
             retry_count, last_attempt_at, updated_at, failure_code, failure_reason, next_retry_at, provider_message_id,
             queued_at, sent_at, delivered_at, failed_at, first_provider_callback_at, last_status_event_at,
             to_number, from_number, payload, created_at
      FROM messages
      WHERE tenant_id = $1 AND conversation_id = $2
      ORDER BY created_at ASC, id ASC
      LIMIT $3
    `,
    [String(accountId), String(convoKey), limit]
  );
  return result.rows.map(mapMessageRow);
}

async function insertIdempotent(db, accountId, convoKey, input = {}) {
  const payload = input?.payload && typeof input.payload === 'object' ? { ...input.payload } : {};
  const params = [
    String(input?.id || ''),
    String(accountId),
    String(convoKey),
    String(input?.direction || 'outbound'),
    String(input?.body ?? ''),
    String(input?.status || 'sent'),
    input?.idempotencyKey ? String(input.idempotencyKey) : null,
    Number.isFinite(Number(input?.retryCount)) ? Number(input.retryCount) : 0,
    toDbTimestamp(input?.lastAttemptAt ?? null),
    toDbTimestamp(input?.updatedAt ?? input?.createdAt ?? Date.now()),
    input?.providerMessageId ? String(input.providerMessageId) : null,
    input?.failureCode ? String(input.failureCode) : null,
    input?.failureReason ? String(input.failureReason) : null,
    toDbTimestamp(input?.nextRetryAt ?? null),
    input?.to ? String(input.to) : null,
    input?.from ? String(input.from) : null,
    payload,
    toDbTimestamp(input?.createdAt ?? Date.now()),
    toDbTimestamp(input?.queuedAt ?? null),
    toDbTimestamp(input?.sentAt ?? null),
    toDbTimestamp(input?.deliveredAt ?? null),
    toDbTimestamp(input?.failedAt ?? null),
    toDbTimestamp(input?.firstProviderCallbackAt ?? null),
    toDbTimestamp(input?.lastStatusEventAt ?? input?.updatedAt ?? input?.createdAt ?? Date.now())
  ];

  const hasIdempotency = Boolean(params[6]);
  const sql = hasIdempotency
    ? `
      INSERT INTO messages (
        id, tenant_id, conversation_id, direction, body, status, idempotency_key,
        retry_count, last_attempt_at, updated_at, provider_message_id, failure_code, failure_reason, next_retry_at,
        to_number, from_number, payload, created_at,
        queued_at, sent_at, delivered_at, failed_at, first_provider_callback_at, last_status_event_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,COALESCE($10, NOW()),$11,$12,$13,$14,$15,$16,$17::jsonb,COALESCE($18, NOW()),$19,$20,$21,$22,$23,$24)
      ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL
      DO UPDATE SET tenant_id = messages.tenant_id
      RETURNING id, tenant_id, conversation_id, direction, body, status, idempotency_key,
                retry_count, last_attempt_at, updated_at, failure_code, failure_reason, next_retry_at, provider_message_id,
                queued_at, sent_at, delivered_at, failed_at, first_provider_callback_at, last_status_event_at,
                to_number, from_number, payload, created_at
    `
    : `
      INSERT INTO messages (
        id, tenant_id, conversation_id, direction, body, status, idempotency_key,
        retry_count, last_attempt_at, updated_at, provider_message_id, failure_code, failure_reason, next_retry_at,
        to_number, from_number, payload, created_at,
        queued_at, sent_at, delivered_at, failed_at, first_provider_callback_at, last_status_event_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,COALESCE($10, NOW()),$11,$12,$13,$14,$15,$16,$17::jsonb,COALESCE($18, NOW()),$19,$20,$21,$22,$23,$24)
      ON CONFLICT (id)
      DO UPDATE SET tenant_id = messages.tenant_id
      RETURNING id, tenant_id, conversation_id, direction, body, status, idempotency_key,
                retry_count, last_attempt_at, updated_at, failure_code, failure_reason, next_retry_at, provider_message_id,
                queued_at, sent_at, delivered_at, failed_at, first_provider_callback_at, last_status_event_at,
                to_number, from_number, payload, created_at
    `;
  const result = await db.query(sql, params);
  return mapMessageRow(result.rows[0]);
}

async function getById(db, accountId, messageId) {
  const result = await db.query(
    `
      SELECT id, tenant_id, conversation_id, direction, body, status, idempotency_key,
             retry_count, last_attempt_at, updated_at, failure_code, failure_reason, next_retry_at,
             provider_message_id, queued_at, sent_at, delivered_at, failed_at, first_provider_callback_at, last_status_event_at,
             to_number, from_number, payload, created_at
      FROM messages
      WHERE tenant_id = $1 AND id = $2
      LIMIT 1
    `,
    [String(accountId), String(messageId)]
  );
  return result.rowCount ? mapMessageRow(result.rows[0]) : null;
}

async function updateStatusById(db, accountId, messageId, patch = {}) {
  const result = await db.query(
    `
      UPDATE messages
      SET
        status = COALESCE($3, status),
        retry_count = COALESCE($4, retry_count),
        last_attempt_at = COALESCE($5, last_attempt_at),
        updated_at = COALESCE($6, NOW()),
        provider_message_id = COALESCE($7, provider_message_id),
        failure_code = $8,
        failure_reason = $9,
        next_retry_at = $10,
        to_number = COALESCE($11, to_number),
        from_number = COALESCE($12, from_number),
        payload = COALESCE($13::jsonb, payload),
        queued_at = COALESCE($14, queued_at),
        sent_at = COALESCE($15, sent_at),
        delivered_at = COALESCE($16, delivered_at),
        failed_at = COALESCE($17, failed_at),
        first_provider_callback_at = COALESCE($18, first_provider_callback_at),
        last_status_event_at = COALESCE($19, last_status_event_at)
      WHERE tenant_id = $1 AND id = $2
      RETURNING id, tenant_id, conversation_id, direction, body, status, idempotency_key,
                retry_count, last_attempt_at, updated_at, failure_code, failure_reason, next_retry_at, provider_message_id,
                queued_at, sent_at, delivered_at, failed_at, first_provider_callback_at, last_status_event_at,
                to_number, from_number, payload, created_at
    `,
    [
      String(accountId),
      String(messageId),
      patch?.status ? String(patch.status) : null,
      Number.isFinite(Number(patch?.retryCount)) ? Number(patch.retryCount) : null,
      toDbTimestamp(patch?.lastAttemptAt ?? null),
      toDbTimestamp(patch?.updatedAt ?? Date.now()),
      patch?.providerMessageId ? String(patch.providerMessageId) : null,
      patch?.failureCode == null ? null : String(patch.failureCode),
      patch?.failureReason == null ? null : String(patch.failureReason),
      toDbTimestamp(patch?.nextRetryAt ?? null),
      patch?.to ? String(patch.to) : null,
      patch?.from ? String(patch.from) : null,
      patch?.payload && typeof patch.payload === 'object' ? patch.payload : null,
      toDbTimestamp(patch?.queuedAt ?? null),
      toDbTimestamp(patch?.sentAt ?? null),
      toDbTimestamp(patch?.deliveredAt ?? null),
      toDbTimestamp(patch?.failedAt ?? null),
      toDbTimestamp(patch?.firstProviderCallbackAt ?? null),
      toDbTimestamp(patch?.lastStatusEventAt ?? null)
    ]
  );
  return result.rowCount ? mapMessageRow(result.rows[0]) : null;
}

async function getByProviderMessageId(db, accountId, providerMessageId) {
  const result = await db.query(
    `
      SELECT id, tenant_id, conversation_id, direction, body, status, idempotency_key,
             retry_count, last_attempt_at, updated_at, failure_code, failure_reason, next_retry_at, provider_message_id,
             queued_at, sent_at, delivered_at, failed_at, first_provider_callback_at, last_status_event_at,
             to_number, from_number, payload, created_at
      FROM messages
      WHERE tenant_id = $1
        AND provider_message_id = $2
      ORDER BY created_at DESC, id DESC
      LIMIT 1
    `,
    [String(accountId), String(providerMessageId)]
  );
  return result.rowCount ? mapMessageRow(result.rows[0]) : null;
}

async function claimRetryableMessages(db, { limit = 25, now = Date.now(), maxRetryCount = 3 } = {}) {
  const result = await db.query(
    `
      WITH candidates AS (
        SELECT m.id, m.tenant_id
        FROM messages m
        WHERE m.status IN ('failed', 'undelivered')
          AND m.next_retry_at IS NOT NULL
          AND m.next_retry_at <= COALESCE($1, NOW())
          AND m.retry_count < $2
        ORDER BY m.next_retry_at ASC, m.updated_at ASC, m.id ASC
        LIMIT $3
        FOR UPDATE SKIP LOCKED
      )
      UPDATE messages AS m
      SET
        status = 'sending',
        last_attempt_at = COALESCE($1, NOW()),
        updated_at = COALESCE($1, NOW())
      FROM candidates
      WHERE m.id = candidates.id
        AND m.tenant_id = candidates.tenant_id
      RETURNING m.id, m.tenant_id, m.conversation_id, m.direction, m.body, m.status, m.idempotency_key,
                m.retry_count, m.last_attempt_at, m.updated_at, m.failure_code, m.failure_reason, m.next_retry_at,
                m.provider_message_id, m.queued_at, m.sent_at, m.delivered_at, m.failed_at, m.first_provider_callback_at, m.last_status_event_at,
                m.to_number, m.from_number, m.payload, m.created_at
    `,
    [toDbTimestamp(now), Number(maxRetryCount), Math.max(1, Math.min(250, Number(limit || 25)))]
  );
  return result.rows.map(mapMessageRow);
}

async function deleteById(db, accountId, convoKey, messageId) {
  const result = await db.query(
    `
      DELETE FROM messages
      WHERE tenant_id = $1 AND conversation_id = $2 AND id = $3
      RETURNING id, tenant_id, conversation_id, direction, body, status, idempotency_key,
                retry_count, last_attempt_at, updated_at, failure_code, failure_reason, next_retry_at, provider_message_id,
                queued_at, sent_at, delivered_at, failed_at, first_provider_callback_at, last_status_event_at,
                to_number, from_number, payload, created_at
    `,
    [String(accountId), String(convoKey), String(messageId)]
  );
  return result.rowCount ? mapMessageRow(result.rows[0]) : null;
}

async function listRecentFailures(db, accountId, { limit = 25 } = {}) {
  const result = await db.query(
    `
      SELECT id, tenant_id, conversation_id, direction, body, status, idempotency_key,
             retry_count, last_attempt_at, updated_at, failure_code, failure_reason, next_retry_at, provider_message_id,
             queued_at, sent_at, delivered_at, failed_at, first_provider_callback_at, last_status_event_at,
             to_number, from_number, payload, created_at
      FROM messages
      WHERE tenant_id = $1
        AND status IN ('failed', 'undelivered')
      ORDER BY last_status_event_at DESC NULLS LAST, id DESC
      LIMIT $2
    `,
    [String(accountId), Math.max(1, Math.min(250, Number(limit || 25)))]
  );
  return result.rows.map(mapMessageRow);
}

async function getDeliveryAnalyticsSummary(db, accountId, { rangeStart = null } = {}) {
  const result = await db.query(
    `
      SELECT
        COUNT(*) FILTER (
          WHERE ($2::timestamptz IS NULL OR queued_at >= $2)
        )::int AS outbound_total,
        COUNT(*) FILTER (
          WHERE sent_at IS NOT NULL
            AND ($2::timestamptz IS NULL OR queued_at >= $2)
        )::int AS sent_count,
        COUNT(*) FILTER (
          WHERE delivered_at IS NOT NULL
            AND ($2::timestamptz IS NULL OR queued_at >= $2)
        )::int AS delivered_count,
        COUNT(*) FILTER (
          WHERE status = 'failed'
            AND ($2::timestamptz IS NULL OR queued_at >= $2)
        )::int AS failed_count,
        COUNT(*) FILTER (
          WHERE status = 'undelivered'
            AND ($2::timestamptz IS NULL OR queued_at >= $2)
        )::int AS undelivered_count,
        COUNT(*) FILTER (
          WHERE retry_count > 0
            AND status IN ('sent', 'delivered')
            AND ($2::timestamptz IS NULL OR queued_at >= $2)
        )::int AS retry_success_count,
        COUNT(*) FILTER (
          WHERE status IN ('failed', 'undelivered')
            AND next_retry_at IS NOT NULL
            AND next_retry_at <= NOW()
        )::int AS retry_queue_size,
        AVG(EXTRACT(EPOCH FROM (sent_at - queued_at)) * 1000.0) FILTER (
          WHERE queued_at IS NOT NULL
            AND sent_at IS NOT NULL
            AND ($2::timestamptz IS NULL OR queued_at >= $2)
        ) AS avg_time_to_sent_ms,
        AVG(EXTRACT(EPOCH FROM (delivered_at - queued_at)) * 1000.0) FILTER (
          WHERE queued_at IS NOT NULL
            AND delivered_at IS NOT NULL
            AND ($2::timestamptz IS NULL OR queued_at >= $2)
        ) AS avg_time_to_delivered_ms,
        percentile_cont(0.95) WITHIN GROUP (
          ORDER BY EXTRACT(EPOCH FROM (delivered_at - queued_at)) * 1000.0
        ) FILTER (
          WHERE queued_at IS NOT NULL
            AND delivered_at IS NOT NULL
            AND ($2::timestamptz IS NULL OR queued_at >= $2)
        ) AS p95_time_to_delivered_ms
      FROM messages
      WHERE tenant_id = $1
        AND direction = 'outbound'
    `,
    [String(accountId), toDbTimestamp(rangeStart)]
  );
  const row = result.rows[0] || {};
  return {
    accountId: String(accountId),
    rangeStart: rangeStart ? Number(rangeStart) : null,
    outboundTotal: Number(row.outbound_total || 0),
    sentCount: Number(row.sent_count || 0),
    deliveredCount: Number(row.delivered_count || 0),
    failedCount: Number(row.failed_count || 0),
    undeliveredCount: Number(row.undelivered_count || 0),
    retrySuccessCount: Number(row.retry_success_count || 0),
    retryQueueSize: Number(row.retry_queue_size || 0),
    avgTimeToSentMs: toPositiveNumber(row.avg_time_to_sent_ms, null),
    avgTimeToDeliveredMs: toPositiveNumber(row.avg_time_to_delivered_ms, null),
    p95TimeToDeliveredMs: toPositiveNumber(row.p95_time_to_delivered_ms, null)
  };
}

module.exports = {
  listByConversation,
  insertIdempotent,
  getById,
  updateStatusById,
  getByProviderMessageId,
  claimRetryableMessages,
  deleteById,
  listRecentFailures,
  getDeliveryAnalyticsSummary,
  mapMessageRow
};
