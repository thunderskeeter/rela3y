ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS failure_code TEXT NULL;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS failure_reason TEXT NULL;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_messages_retry_pickup
  ON messages(tenant_id, status, next_retry_at, retry_count, updated_at)
  WHERE status IN ('failed', 'undelivered');
