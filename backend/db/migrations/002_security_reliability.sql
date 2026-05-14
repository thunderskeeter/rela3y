CREATE TABLE IF NOT EXISTS webhook_receipts (
  account_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  event_id TEXT NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (account_id, provider, event_id)
);

CREATE INDEX IF NOT EXISTS idx_webhook_receipts_seen_at
  ON webhook_receipts(first_seen_at DESC);

CREATE TABLE IF NOT EXISTS rate_limit_counters (
  bucket_key TEXT PRIMARY KEY,
  count BIGINT NOT NULL,
  reset_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rate_limit_counters_reset_at
  ON rate_limit_counters(reset_at);
