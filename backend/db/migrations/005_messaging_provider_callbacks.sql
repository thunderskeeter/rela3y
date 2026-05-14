ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS provider_message_id TEXT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_tenant_provider_message
  ON messages(tenant_id, provider_message_id)
  WHERE provider_message_id IS NOT NULL;
