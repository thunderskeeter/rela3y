# PR-8 Messaging Hardening

Last updated: 2026-04-06

## Global Invariant
- All messaging reads, writes, analytics, and admin/debug operations must derive solely from canonical DB state.
- Payload and snapshot must never be fallback truth.
- If any code path requires payload or snapshot for messaging correctness, that is a defect.

## What Already Exists In This Branch
- Shared status policy is already implemented in `backend/src/services/messageStatusPolicy.js`.
- Canonical lifecycle timing fields and SQL-backed analytics queries are already implemented in `backend/src/repositories/messagesRepo.js`.
- DB-native developer/admin messaging tooling already exists in `backend/src/routes/admin.routes.js`.
- Snapshot messaging remains shut down in DB mode.
- Message payload is already non-authoritative and conversation payload is already suppressed for new DB-native rows.

## Current Payload Surface

### Message payload still written for new rows
- `backend/src/services/messagingPayloadService.js`
  - writes compatibility-only message payload fields

### Message payload still read in active messaging mapping
- `backend/src/repositories/messagesRepo.js`
  - `meta`
  - `attachments`
  - compatibility-derived `source`

### Retained compatibility-only message payload fields
- `meta`
  - retained because adjacent routes and services still use message metadata such as booking/payment hints and source tags
  - dependent code paths currently include:
    - `backend/src/routes/messages.routes.js`
    - `backend/src/routes/analytics.routes.js`
    - `backend/src/services/customerInvoiceService.js`
    - `backend/src/services/scheduler.js`
- `attachments`
  - retained because active message-bearing responses still expose attachments for compatibility output
  - dependent code paths currently include:
    - `backend/src/repositories/messagesRepo.js`
    - `backend/src/services/messagesService.js`
    - `backend/src/services/messagingPayloadService.js`
- `source`
  - retained only as compatibility output derived from `payload.meta.source` or legacy `payload.source`
  - dependent code paths currently include:
    - `backend/src/repositories/messagesRepo.js`
    - `backend/src/services/messagesService.js`

### Final payload policy after PR-8 cleanup
- Canonical message truth is now fully column-driven for:
  - identity
  - addressing
  - status
  - provider state
  - retry state
  - lifecycle timestamps
- Message payload is compatibility-only for:
  - `meta`
  - `attachments`
  - compatibility `source`
- `providerMeta` remains part of response output, but is now derived from canonical DB columns only.
- Snapshot never participates in payload fallback or message correctness.

### Removed from payload-derived truth during PR-8 cleanup
- `providerMeta`
  - no longer reads legacy payload `providerMeta`
  - response `providerMeta` is now derived from canonical provider/status/failure columns only

### Routes that still expose compatibility fields
- `backend/src/routes/messages.routes.js`
  - conversation detail / message-bearing responses still expose mapped message compatibility fields:
    - `meta`
    - `providerMeta`
    - `attachments`
    - `source`
- `backend/src/routes/admin.routes.js`
  - message lookup and conversation debug reads can still include those same compatibility fields through `messagesRepo.mapMessageRow()`

## Current Status Surface

### Current message statuses
- `queued`
- `sending`
- `sent`
- `delivered`
- `failed`
- `undelivered`
- `received`
- `simulated`

### Real transition sites
- `backend/src/services/messagesService.js`
  - first-send / outbound mutation path
- `backend/src/services/messageRetryService.js`
  - retry claim / resend path
- `backend/src/services/providerCallbackService.js`
  - callback normalization and lifecycle updates

### Shared state machine source of truth
- `backend/src/services/messageStatusPolicy.js`
  - allowed statuses
  - callback normalization
  - retryable statuses
  - blocked transition fields

### Enforced transition rules
- outbound send:
  - `queued -> sending -> sent`
- provider callback:
  - `sent|sending -> delivered|failed|undelivered`
- retry re-entry:
  - `failed|undelivered -> sending`
- terminal:
  - `delivered`
  - `received`
  - `simulated`
- blocked transitions are logged with:
  - `message_id`
  - `previous_status`
  - `attempted_status`
  - `source`

## Lifecycle Timing Surface

### Canonical lifecycle timestamp fields already present
- `queued_at`
- `sent_at`
- `delivered_at`
- `failed_at`
- `first_provider_callback_at`
- `last_status_event_at`

### Remaining lifecycle hardening question
- downstream business analytics/services must not reconstruct messaging correctness from message payload fields when canonical lifecycle and conversation fields already exist

## Current Index Surface

### Message indexes already present
- tenant + idempotency key
- tenant + provider message ID
- retry pickup
- tenant + `to_number` + `from_number` + `created_at`
- PR-8 analytics/failure indexes from `backend/db/migrations/008_messaging_hardening.sql`
  - outbound queued window
  - outbound delivered window
  - recent failures

### Query/index alignment verified in PR-8 cleanup
- provider lookup query:
  - `getByProviderMessageId()`
  - supported by tenant + `provider_message_id`
- retry claim query:
  - `claimRetryableMessages()`
  - supported by retry pickup index added in PR-7 retry hardening
- recent failures query:
  - `listRecentFailures()`
  - now ordered by `last_status_event_at DESC, id DESC` to match the PR-8 recent-failures index
- analytics summary query:
  - `getDeliveryAnalyticsSummary()`
  - now scans only outbound rows for the tenant and matches the queued/delivered analytics window indexes conceptually
- On the tiny test fixture, `EXPLAIN` still chooses sequential scans because the table cardinality is trivial. The query shapes themselves now align with the intended indexes.

## Current Admin/Debug Surface
- `backend/src/routes/admin.routes.js`
  - analytics summary
  - recent failed outbound messages
  - conversation by `convoKey`
  - conversation by `row_id`
  - message by `message.id`

### Admin/debug guarantees
- DB-native only
- explicit account existence checks on every messaging debug endpoint
- no raw payload JSON exposed
- compatibility fields may still appear through mapped message output, but canonical DB state remains the source of truth

## Classified Remaining Payload Reads

### Acceptable compatibility-only reads
- `backend/src/repositories/messagesRepo.js`
  - `meta`
  - `attachments`
  - compatibility `source`

These are response-shape compatibility reads only. They must not affect identity, addressing, status, retry state, provider state, analytics, or admin/debug correctness.

### Remaining correctness-path defects to remove in PR-8 cleanup
- `backend/src/routes/analytics.routes.js`
  - removed direct `m.payload.*` reads for booking/payment-derived message behavior
  - broader route modernization is still separate because the route itself still uses non-messaging snapshot/business data helpers
- `backend/src/services/customerInvoiceService.js`
  - removed direct `m.payload.*` reads for invoice amount/payment-derived message behavior
  - remaining work in this area is broader customer-billing modernization, not messaging payload correctness
- `backend/src/services/scheduler.js`
  - removed direct `m.payload.bookingTime` fallback during booked-conversation timing resolution

## Verified Regression Surfaces
- `backend/test/messagingPayloadReads.integration.test.js`
- `backend/test/messagingPayloadWrites.integration.test.js`
- `backend/test/providerCallback.integration.test.js`
- `backend/test/messageRetry.integration.test.js`
- `backend/test/messagingHardening.integration.test.js`
- `backend/test/messagingCoreDb.integration.test.js`
- `backend/test/messagingAsyncWriters.integration.test.js`

### Needs review but not yet classified as a defect
- `backend/src/repositories/conversationsRepo.js`
  - create/update paths still accept payload-backed compatibility input for conversation metadata writes
  - this is only acceptable if payload is treated as caller input and canonical columns remain authoritative after write

## Execution Notes For Remaining Chunks
- Chunk 2 should finalize which message payload fields remain as compatibility output and remove any canonical fallback hidden inside provider/source derivation.
- Chunk 3 should verify that all real status mutations are already routed through `messageStatusPolicy.js`, and tighten only if a bypass is found.
- Chunk 4 should remove the remaining downstream `m.payload.*` correctness reads in analytics, invoicing, and scheduler paths or explicitly document any out-of-scope non-messaging business logic.
- Chunk 5 should verify that existing PR-8 indexes match the actual query shapes already in code.
- Chunk 6 should verify that admin/debug responses remain canonical-first and account-scoped.
