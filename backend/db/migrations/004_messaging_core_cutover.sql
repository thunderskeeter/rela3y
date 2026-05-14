ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS row_id UUID NOT NULL DEFAULT gen_random_uuid();

ALTER TABLE conversations
  ADD COLUMN IF NOT EXISTS convo_key TEXT;

UPDATE conversations
SET convo_key = id
WHERE convo_key IS NULL OR convo_key = '';

ALTER TABLE conversations
  ALTER COLUMN convo_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_row_id
  ON conversations(row_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_tenant_convo_key
  ON conversations(tenant_id, convo_key);

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS idempotency_key TEXT NULL;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'sent';

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ NULL;

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_tenant_idempotency
  ON messages(tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_conversations_tenant_last_activity
  ON conversations(tenant_id, COALESCE(last_activity_at, updated_at) DESC, convo_key ASC);
