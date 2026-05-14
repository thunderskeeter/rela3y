# PR-7 Messaging Cutover

Last updated: 2026-04-06

## Completed In This Branch

### PR-8 messaging hardening preflight
- Remaining compatibility-only message payload fields at PR-8 start:
  - `meta`
  - `providerMeta`
  - `attachments`
  - `source`
- Message lifecycle transition logic at PR-8 start was still split across:
  - `backend/src/services/messagesService.js`
  - `backend/src/services/providerCallbackService.js`
  - `backend/src/services/messageRetryPolicy.js`
- Existing developer/admin surface to extend:
  - `backend/src/routes/admin.routes.js`

### PR-8 cleanup inspection from current repo state
- The branch already contains the main PR-8 hardening primitives:
  - shared status policy in `backend/src/services/messageStatusPolicy.js`
  - canonical lifecycle timestamps and SQL-backed analytics reads in `backend/src/repositories/messagesRepo.js`
  - DB-native messaging debug endpoints in `backend/src/routes/admin.routes.js`
- Remaining core messaging payload reads are now compatibility-only output in `backend/src/repositories/messagesRepo.js`:
  - `meta`
  - `attachments`
  - compatibility-derived `source`
- `providerMeta` is now derived from canonical provider/status/failure columns only and no longer merges legacy payload state
- Remaining payload-derived correctness defects discovered outside the core messaging path:
  - `backend/src/routes/analytics.routes.js`
  - `backend/src/services/customerInvoiceService.js`
  - `backend/src/services/scheduler.js`
- Conversation repo payload-backed compatibility input remains under review, but active conversation/message correctness no longer depends on payload or snapshot state.

### Async/orchestrator messaging writers moved to DB-backed services
- `backend/src/services/automationEngine.js`
  - `evaluateTrigger()` and `sendAutomationMessage()` now route outbound messaging through `messagesService.appendOutboundMessage()`.
- `backend/src/services/actionExecutor.js`
  - `executeRevenueAction()` now routes the outbound send branch through `messagesService.appendOutboundMessage()`.
- `backend/src/services/agentEngine.js`
  - `executeStep()` now routes `SEND_MESSAGE` through `messagesService.appendOutboundMessage()`.
- `backend/src/services/scheduler.js`
  - scheduled automation sends and periodic automation scans now call the async DB-backed `sendAutomationMessage()` path correctly.

### Fail-fast messaging write guards added
- `backend/src/store/dataStore.js`
  - messaging mutation entrypoints throw `snapshot_write_blocked` in DB messaging mode.
- `backend/src/services/complianceService.js`
  - `attemptOutboundMessage()` throws `legacy_messaging_writer_blocked` in DB messaging mode so it cannot silently persist messaging state.

### Provider callback normalization added
- `backend/src/services/providerCallbackService.js`
  - Twilio message status callbacks now resolve messages through canonical `provider_message_id`
  - duplicate callbacks are suppressed through `webhook_receipts`
  - unmatched callbacks and regressive status transitions are logged and blocked centrally
- `backend/src/routes/webhooks.routes.js`
  - `POST /webhooks/twilio/status` is the single callback ingress for DB-native message lifecycle updates
- `backend/db/migrations/005_messaging_provider_callbacks.sql`
  - adds canonical `messages.provider_message_id`

### Retry lifecycle hardening added
- `backend/db/migrations/006_messaging_retry_support.sql`
  - adds canonical `failure_code`, `failure_reason`, and `next_retry_at`
- `backend/src/repositories/messagesRepo.js`
  - adds SQL-safe retry row claiming with `FOR UPDATE SKIP LOCKED`
- `backend/src/services/messagesService.js`
  - first-send and retry attempts now share the same outbound transport/update helper
- `backend/src/services/messageRetryService.js`
  - adds the DB-native retry worker path
  - retries update the same message row instead of appending a new one

### Message-ID-first mutation added
- `backend/src/routes/messages.routes.js`
  - adds `DELETE /api/conversations/:id/messages/by-id/:messageId`
  - keeps `DELETE /api/conversations/:id/messages/:index` only as a deprecated compatibility shim
- `backend/src/services/messagesService.js`
  - makes delete-by-message-id the primary DB-native mutation path
  - reduces delete-by-index to ordered ID resolution plus delegation
- `backend/src/services/messagingBoundaryService.js`
  - adds a DB-native `deleteConversationMessageById()` boundary entrypoint

### Payload-read retirement added
- `backend/src/repositories/messagesRepo.js`
  - active messaging reads no longer fall back to payload for message identity, body, status, retry, or provider fields
  - canonical `provider_message_id` is now the only provider callback lookup key
- `backend/src/repositories/conversationsRepo.js`
  - canonical conversation identity/status/timestamps now override payload consistently
  - malformed or null payloads now default safely for `audit`, `leadData`, `flow`, and `fields`
- `backend/src/services/messagesService.js`
  - compatibility `providerMeta` output is now built from canonical message state, not `message.payload`
- `backend/src/services/providerCallbackService.js`
  - callback updates no longer re-read provider metadata from payload

### Payload-write reduction added
- `backend/src/services/messagingPayloadService.js`
  - centralizes minimal message and conversation payload projections
- `backend/src/services/messagesService.js`
  - new message rows now write compact compatibility payloads instead of full message blobs
  - outbound status/retry updates keep payload limited to provider/retry compatibility fields plus explicit metadata
- `backend/src/services/conversationsService.js`
  - conversation payload writes now keep only non-canonical metadata such as `flow`, `audit`, `leadData`, `fields`, and booking/payment fields that still lack dedicated columns
- `backend/src/repositories/conversationsRepo.js`
  - conversation payload merging now uses the stored payload JSON directly instead of re-expanding the whole mapped conversation row back into payload

### Canonical field expansion added
- `backend/db/migrations/007_messaging_canonical_fields.sql`
  - adds canonical message addressing columns `to_number` / `from_number`
  - adds canonical conversation lifecycle/metadata columns for `flow`, `audit`, `leadData`, `fields`, booking timing, amount, payment status, and close time
- `backend/src/repositories/messagesRepo.js`
  - message reads now surface canonical `to` / `from`
  - retry/provider/delete reads all preserve canonical addressing on the same row
- `backend/src/repositories/conversationsRepo.js`
  - conversation reads now prefer canonical lifecycle/metadata columns and only fall back to payload for legacy rows
- `backend/src/services/messagesService.js`
  - outbound send and retry flows now persist and reuse canonical message addressing instead of payload `to` / `from`
- `backend/src/services/conversationsService.js`
  - conversation write paths now persist canonical flow/audit/lead-data/booking/payment fields alongside compatibility payload writes
- `backend/src/services/messagingPayloadService.js`
  - message payload projection no longer writes `to` / `from`; those values are fully canonical now

### Final payload policy and snapshot shutdown added
- `backend/src/services/messagingPayloadService.js`
  - conversation payload writes are now fully suppressed for new DB-native rows
  - message payload is retained only for compatibility fields: `meta`, `providerMeta`, `attachments`, and `source`
- `backend/src/store/dataStore.js`
  - DB messaging mode now strips `dataCache.conversations` on load/save so snapshot conversations cannot act as messaging truth
  - snapshot messaging accessors now return empty/null and messaging mutations still fail fast
- `backend/src/db/stateRepository.js`
  - DB messaging mode now strips conversations from `app_state.snapshot`
  - snapshot persistence no longer truncates or rewrites live `conversations` / `messages` tables
- `backend/src/services/messagingBoundaryService.js`
  - snapshot reconciliation is now blocked in DB messaging mode

### PR-8 messaging hardening added
- `backend/db/migrations/008_messaging_hardening.sql`
  - adds canonical lifecycle timing columns for queue/send/delivery/failure tracking
  - adds only query-backed indexes for outbound analytics windows and recent failure reads
- `backend/src/services/messageStatusPolicy.js`
  - defines the shared message status state machine for send/retry/callback flows
- `backend/src/services/messagingAnalyticsService.js`
  - derives delivery analytics and debug reads from canonical SQL only
- `backend/src/routes/admin.routes.js`
  - adds developer messaging debug endpoints backed by DB-native repos/services only
- `backend/src/repositories/messagesRepo.js`
  - removes payload fallback for canonical message writes
  - adds canonical lifecycle timestamp mapping plus analytics/admin queries
- `backend/src/repositories/conversationsRepo.js`
  - removes payload fallback from active conversation read correctness
- PR-8 cleanup then removes the remaining direct `m.payload.*` business reads from downstream analytics, invoice, and scheduler helpers, while keeping broader non-messaging route modernization explicitly out of scope for the messaging cutover itself.

## Verified Surfaces

### Targeted async writer verification
- `backend/test/messagingAsyncWriters.integration.test.js`
  - automation-triggered outbound send appends a DB message and leaves snapshot conversation messages unchanged
  - action-triggered outbound send appends a DB message and leaves snapshot conversation messages unchanged
  - agent-triggered outbound send appends a DB message and leaves snapshot conversation messages unchanged

### Core request-path regression retained
- `backend/test/messagingCoreDb.integration.test.js`
  - request-path messaging still works in DB mode
  - conversation reads remain DB-native after clearing in-memory snapshot conversation state
  - message-bearing reads expose stable `message.id`
  - delete-by-message-id works
  - delete-by-index still works only as an ID-resolution shim
  - concurrent inserts do not break ID-based delete correctness

### Provider callback verification
- `backend/test/providerCallback.integration.test.js`
  - delivered callback updates canonical DB message status
  - duplicate callback is ignored
  - unmatched callback is visible but non-fatal
  - regressive callback is blocked
  - webhook tenant resolution fails closed when the signed tenant selector is missing

### Retry verification
- `backend/test/messageRetry.integration.test.js`
  - first-send failure becomes retryable
  - retry success updates the same message row
  - repeated failure exhausts retry cap
  - concurrent worker pickup does not double-send
  - provider callback after retry still updates the same message row

### Payload-read verification
- `backend/test/messagingPayloadReads.integration.test.js`
  - stale payload does not override canonical message/conversation read state
  - null or malformed payload does not break conversation/message reads
  - provider and retry fields still read correctly from canonical columns
  - tenant safety still holds with corrupted payload rows

### Payload-write verification
- `backend/test/messagingPayloadWrites.integration.test.js`
  - new outbound and inbound messages persist minimal payload projections
  - legacy full-payload rows still read correctly beside new minimal rows
  - conversation payload writes no longer mirror canonical identity/status/message arrays
  - provider and retry lifecycle flows remain compatible with reduced payload writes

### Canonical-field verification
- `backend/test/messagingCanonicalFields.integration.test.js`
  - outbound send works with canonical `messages.to_number` / `messages.from_number` and no payload addressing
  - retry works after stripping message payload addressing
  - provider callback still resolves the same row after retry without payload addressing
  - conversation metadata reads remain correct for both canonical new rows and legacy payload-backed rows
  - mixed old/new conversation rows behave consistently

### Snapshot-shutdown verification
- `backend/test/messagingSnapshotShutdown.integration.test.js`
  - messaging still works with snapshot persistence enabled
  - `app_state.snapshot` no longer stores conversation data in DB messaging mode
  - snapshot flushes do not truncate live message/conversation tables
  - legacy snapshot messaging mutation/reconciliation entrypoints fail fast

## Remaining PR-7 Work

### Still not fully finished
- canonical conversation linkage expansion in downstream domains

### Guarded legacy fallback still present
- `backend/src/services/complianceService.js`
  - `attemptOutboundMessage()` still exists but hard-fails in DB messaging mode
- `backend/src/services/messagingBoundaryService.js`
  - legacy snapshot fallback branches still exist for non-DB mode only
- `backend/src/routes/dev.routes.js`
  - dev-only messaging mutation helpers remain snapshot-based and deferred
- `backend/src/services/complianceService.js`
  - retention purge remains outside the DB-native messaging cutover

## Final Payload Policy

### Retained temporarily
- Message `payload.meta`
- Message `payload.providerMeta`
- Message `payload.attachments`
- Message `payload.source`

These remain only for compatibility output and adjacent non-core consumers.

### Removed from new writes
- Conversation payload for all DB-native write paths
- Message payload `to`, `from`, `status`, `body`, `text`, `conversationId`, `convoKey`, retry fields, and provider identity fields

### Authoritative sources
- Message identity, addressing, lifecycle, retry, and provider state: canonical columns
- Conversation flow/audit/lead/booking/payment state: canonical columns / JSONB columns

## Writer Audit Snapshot

### Migrated in PR-6
- dashboard send routes
- inbound SMS webhook
- missed-call webhook
- booking sync request paths
- conversation status update
- delete-by-index compatibility path
- flow start/advance request-path conversation persistence

### Migrated in current PR-7 work
- automation-triggered outbound messaging
- action-triggered outbound messaging
- agent-triggered outbound messaging
- scheduler-triggered automation sends via the shared automation service path

### Remaining non-DB messaging paths
- guarded legacy fallback helpers only
- dev-only mutation routes
- retention purge
