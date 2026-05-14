ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS queued_at TIMESTAMPTZ NULL;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ NULL;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ NULL;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ NULL;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS first_provider_callback_at TIMESTAMPTZ NULL;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS last_status_event_at TIMESTAMPTZ NULL;

UPDATE messages
SET
  queued_at = COALESCE(queued_at, created_at),
  sent_at = CASE
    WHEN sent_at IS NOT NULL THEN sent_at
    WHEN status IN ('sent', 'delivered') THEN COALESCE(updated_at, created_at)
    ELSE NULL
  END,
  delivered_at = CASE
    WHEN delivered_at IS NOT NULL THEN delivered_at
    WHEN status = 'delivered' THEN COALESCE(updated_at, created_at)
    ELSE NULL
  END,
  failed_at = CASE
    WHEN failed_at IS NOT NULL THEN failed_at
    WHEN status IN ('failed', 'undelivered') THEN COALESCE(updated_at, created_at)
    ELSE NULL
  END,
  last_status_event_at = COALESCE(last_status_event_at, updated_at, created_at);

-- Supports tenant-scoped delivery analytics summary over recent outbound lifecycle windows:
-- WHERE tenant_id = $1 AND direction = 'outbound' AND queued_at >= $2
CREATE INDEX IF NOT EXISTS idx_messages_tenant_outbound_queued_window
  ON messages(tenant_id, queued_at DESC, id DESC)
  WHERE direction = 'outbound' AND queued_at IS NOT NULL;

-- Supports tenant-scoped delivery analytics over delivered rows:
-- WHERE tenant_id = $1 AND direction = 'outbound' AND delivered_at >= $2
CREATE INDEX IF NOT EXISTS idx_messages_tenant_outbound_delivered_window
  ON messages(tenant_id, delivered_at DESC, id DESC)
  WHERE direction = 'outbound' AND delivered_at IS NOT NULL;

-- Supports recent failed outbound debug/admin reads:
-- WHERE tenant_id = $1 AND status IN ('failed','undelivered') ORDER BY last_status_event_at DESC, id DESC LIMIT $2
CREATE INDEX IF NOT EXISTS idx_messages_tenant_recent_failures
  ON messages(tenant_id, last_status_event_at DESC, id DESC)
  WHERE status IN ('failed', 'undelivered');
