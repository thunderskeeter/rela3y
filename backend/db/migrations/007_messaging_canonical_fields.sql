ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS to_number TEXT NULL;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS from_number TEXT NULL;

UPDATE messages m
SET
  to_number = COALESCE(m.to_number, c.to_number),
  from_number = COALESCE(m.from_number, c.from_number)
FROM conversations c
WHERE c.tenant_id = m.tenant_id
  AND c.id = m.conversation_id
  AND (m.to_number IS NULL OR m.from_number IS NULL);

ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS flow_state JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS audit_entries JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS lead_data JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS fields_data JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS booking_time TIMESTAMPTZ NULL;

ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS booking_end_time TIMESTAMPTZ NULL;

ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS amount_value NUMERIC(12,2) NULL;

ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS payment_status TEXT NULL;

ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ NULL;

UPDATE conversations
SET
  flow_state = CASE
    WHEN jsonb_typeof(payload->'flow') = 'object' THEN payload->'flow'
    ELSE flow_state
  END,
  audit_entries = CASE
    WHEN jsonb_typeof(payload->'audit') = 'array' THEN payload->'audit'
    ELSE audit_entries
  END,
  lead_data = CASE
    WHEN jsonb_typeof(payload->'leadData') = 'object' THEN payload->'leadData'
    ELSE lead_data
  END,
  fields_data = CASE
    WHEN jsonb_typeof(payload->'fields') = 'object' THEN payload->'fields'
    ELSE fields_data
  END,
  booking_time = COALESCE(
    booking_time,
    CASE
      WHEN jsonb_typeof(payload->'bookingTime') IN ('number','string') THEN to_timestamp((payload->>'bookingTime')::double precision / 1000.0)
      ELSE NULL
    END
  ),
  booking_end_time = COALESCE(
    booking_end_time,
    CASE
      WHEN jsonb_typeof(payload->'bookingEndTime') IN ('number','string') THEN to_timestamp((payload->>'bookingEndTime')::double precision / 1000.0)
      ELSE NULL
    END
  ),
  amount_value = COALESCE(
    amount_value,
    CASE
      WHEN jsonb_typeof(payload->'amount') IN ('number','string') THEN NULLIF(payload->>'amount', '')::numeric
      ELSE NULL
    END
  ),
  payment_status = COALESCE(payment_status, NULLIF(payload->>'paymentStatus', '')),
  closed_at = COALESCE(
    closed_at,
    CASE
      WHEN jsonb_typeof(payload->'closedAt') IN ('number','string') THEN to_timestamp((payload->>'closedAt')::double precision / 1000.0)
      ELSE NULL
    END
  );

CREATE INDEX IF NOT EXISTS idx_messages_tenant_to_from_created
  ON messages(tenant_id, to_number, from_number, created_at ASC, id ASC);

CREATE INDEX IF NOT EXISTS idx_conversations_tenant_booking_time
  ON conversations(tenant_id, booking_time DESC)
  WHERE booking_time IS NOT NULL;
