# Relay Snapshot Migration Status

Last updated: 2026-04-06

## PR-8 Messaging Hardening State
- PR-8 is a cleanup-and-verification pass, not a fresh migration. The branch already contains:
  - shared message status policy in `backend/src/services/messageStatusPolicy.js`
  - canonical delivery lifecycle timestamps and SQL-backed analytics queries in `backend/src/repositories/messagesRepo.js`
  - developer/admin messaging tooling in `backend/src/routes/admin.routes.js`
- Remaining active message payload compatibility fields are currently:
  - `meta`
  - `attachments`
  - `source`
- Remaining core messaging compatibility reads are currently confined to:
  - `backend/src/repositories/messagesRepo.js`
- PR-8 payload cleanup now derives `providerMeta` from canonical message columns only; legacy payload `providerMeta` no longer participates in active message reads.
- Remaining payload-derived correctness defects discovered during PR-8 inspection:
  - direct `m.payload.*` reads in `backend/src/routes/analytics.routes.js`, `backend/src/services/customerInvoiceService.js`, and `backend/src/services/scheduler.js` have now been removed
  - broader non-messaging business routes in those files still need their own DB-native modernization and are not counted as core messaging invariant regressions once payload truth is removed
- Existing message lifecycle transition code sites:
  - `backend/src/services/messagesService.js`
  - `backend/src/services/providerCallbackService.js`
  - `backend/src/services/messageRetryService.js`
- Existing message indexes:
  - tenant/idempotency
  - tenant/provider_message_id
  - retry pickup
  - tenant/to_number/from_number/created_at
  - PR-8 queued/delivered/failure analytics indexes from `backend/db/migrations/008_messaging_hardening.sql`
- Existing internal developer/admin surface:
  - `backend/src/routes/admin.routes.js`

## Foundation
- Inventory: In progress
- Repository conventions: Done
- Transaction helper: Done
- Parity helper: Done
- Query instrumentation helper: Done
- Flags wired: Done

## Audit Logs
- Schema: Existing
- Reads: Not started
- Parity: Not started
- Writes: Not started
- Snapshot imports removed: No
- Tests: Not started
- Flag removed: N/A

## Billing Events
- Schema: Existing
- Reads: Not started
- Parity: Not started
- Writes: Not started
- Snapshot imports removed: No
- Tests: Not started
- Flag removed: N/A

## Webhook Receipts
- Schema: Existing
- Reads: Done
- Parity: N/A
- Writes: Done
- Snapshot imports removed: Yes
- Tests: Existing coverage retained
- Flag removed: N/A

## Contacts
- Schema: Existing
- Reads: Done
- Parity: In progress
- Writes: Done (existing upsert/import contract only)
- Snapshot imports removed: Partial
- Tests: Done
- Flag removed: No

## Conversations
- Schema: Existing
- Reads: Partial
- Parity: In progress
- Writes: In progress
- Snapshot imports removed: Partial
- Tests: Done
- Flag removed: No
Write isolation notes:
- PR-5 centralizes the remaining production-critical legacy conversation write edges behind `messagingBoundaryService`, including inbound SMS, missed-call state, manual send/status/delete flows, public-booking conversation sync, and flow start/advance wrappers.
- PR-6 adds `conversationsRepo` and `conversationsService`, and cuts the main request-path conversation reads in `messages.routes` over to DB-backed services behind `USE_DB_CONVERSATIONS` / `USE_DB_MESSAGES`.
- PR-6 also cuts the main request-path conversation writes over for dashboard status changes, missed-call state, booking sync, and flow start/advance persistence through DB-backed services.
- PR-7 now routes the remaining high-volume async/orchestrator messaging conversation mutations in `automationEngine`, `actionExecutor`, and `agentEngine` through DB-backed messaging services, with snapshot mutation blocked in DB messaging mode.
- PR-7 also introduces a DB-native Twilio status callback route/service backed by canonical `provider_message_id`, with duplicate/unmatched/regressive callback handling centralized in `providerCallbackService`.
- Deferred conversation write edges now primarily remain in legacy fallback-only paths (`messagingBoundaryService` non-DB mode, `complianceService` fallback auto-replies when no DB-native send function is provided), retention purge, and dev-only mutation routes.

## Messages
- Schema: In progress
- Reads: Partial
- Parity: In progress
- Writes: In progress
- Snapshot imports removed: Partial
- Tests: Done
- Flag removed: No
Write isolation notes:
- PR-6 adds `messagesRepo` and `messagesService`, and cuts the main request-path message reads and writes over for dashboard send, inbound SMS, delete-by-index compatibility, and booking-sync message append behind `USE_DB_MESSAGES`.
- PR-6 preserves deterministic message ordering in SQL with `ORDER BY created_at ASC, id ASC`, keeps delete-by-index as a compatibility shim only, and adds deterministic snapshot-to-DB reconciliation for seeded snapshot conversations/messages.
- Narrow route-shaped parity now exists for conversation list/detail reads, but parity remains disabled during DB-write integration tests because DB-backed request writes intentionally do not keep snapshot messaging state in sync.
- PR-7 now routes automation-triggered, action-triggered, and agent-triggered outbound messaging through `messagesService.appendOutboundMessage()`, and adds fail-fast guards so direct snapshot messaging mutation throws in DB messaging mode.
- PR-7 also centralizes provider delivery/status updates through `providerCallbackService`, adds canonical `messages.provider_message_id`, and keeps duplicate/unmatched/out-of-order callback handling observable and tenant-scoped.
- PR-7 retry hardening now adds canonical retry scheduling fields, SQL-safe retry claims, and a DB-native retry worker path that reuses the same outbound send/update service for first-send and retry attempts.
- PR-7 message-ID cleanup now makes delete-by-message-id the primary mutation path, keeps delete-by-index only as a deprecated compatibility shim, and verifies message IDs across the main message-bearing read surfaces.
- PR-7 payload-read retirement now removes payload fallback for active message identity/status/provider/retry reads, makes canonical DB columns authoritative in `messagesRepo` and `conversationsRepo`, and verifies that stale/null/malformed messaging payloads do not break DB-native conversation/message reads.
- PR-7 payload-write reduction now stops writing full message payload blobs for new DB-native messaging rows, reduces conversation payload writes to non-canonical metadata only, and keeps payload as a minimal compatibility projection rather than an active data mirror.
- PR-7 canonical field expansion now adds canonical `messages.to_number` / `messages.from_number`, canonical conversation flow/audit/lead-data/booking metadata columns, and updates send/retry/read flows so active messaging correctness no longer depends on payload addressing or payload-only conversation lifecycle fields.
- PR-7 final payload policy now keeps message payload only as a compatibility artifact for `meta`, `providerMeta`, `attachments`, and `source`; conversation payload is no longer written for new DB-native rows.
- PR-7 messaging snapshot shutdown now strips conversations from in-memory/app-state snapshots in DB messaging mode, prevents snapshot persistence from truncating live `conversations` / `messages` tables, and blocks legacy snapshot reconciliation/mutation entrypoints.
- PR-8 payload finalization now removes payload fallback for canonical message writes, stops writing provider lifecycle mirrors into new message payloads, and keeps only explicitly justified compatibility payload fields.
- PR-8 status hardening now centralizes lifecycle transitions in a shared message status policy used by send, retry, and callback paths.
- PR-8 delivery analytics now records canonical lifecycle timestamps and derives tenant-scoped messaging SLA metrics from SQL only.
- PR-8 admin/debug tooling now exposes DB-native developer reads for message by ID, conversation by key/row ID, recent failures, and delivery analytics.
- PR-8 follow-up inspection confirms the remaining invariant violations are downstream payload-derived business reads in analytics, invoice generation, and scheduler booking-time resolution, not the core DB-native messaging lifecycle itself.
- PR-8 cleanup now removes the remaining direct `m.payload.*` reads from analytics, invoice generation, and scheduler booking-time resolution; any remaining modernization work in those files is broader non-messaging business logic rather than messaging payload truth.
- Deferred message write edges now primarily remain in guarded legacy fallback paths (`complianceService.attemptOutboundMessage()` when invoked outside the DB-native service path, `messagingBoundaryService` non-DB fallback), retention purge, and dev-only mutation routes. Canonical linkage expansion remains the next major cleanup item; messaging snapshot persistence is now shut down for DB messaging mode.

## Opportunities
- Schema: Existing
- Reads: Partial
- Parity: In progress
- Writes: Partial
- Snapshot imports removed: Partial
- Tests: Done
- Flag removed: No
Parity audit notes:
- `timeline missing contactId divergence`: expected during audit only when the DB row payload is intentionally stripped of `contactId` while snapshot state still has it. This is expected because timeline revenue-event composition still depends on mixed-source opportunity/contact linkage. It becomes suspicious if it appears without an intentional DB mutation or starts showing up in baseline parity runs.
- `funnel semantic divergence`: expected during audit only when the DB canonical `risk_score` is intentionally changed away from the snapshot value. This is expected because the audit is proving that funnel parity does not hide qualification-count drift behind normalization. It becomes suspicious if baseline funnel parity starts logging this mismatch or if a payload-only change begins affecting the public funnel response.

## Actions
- Schema: Existing
- Reads: Partial
- Parity: In progress
- Writes: Partial
- Snapshot imports removed: Partial
- Tests: Done
- Flag removed: No
Parity audit notes:
- `activity-feed grouping divergence`: expected during audit only when a DB action payload is intentionally mutated to a different `correlationId` than the snapshot action. This is expected because the audit is proving that grouping drift remains visible after normalization. It becomes suspicious if it appears in baseline parity runs or after a rollout that is supposed to keep grouping keys stable across snapshot and DB representations.
- `timeline membership divergence`: expected during audit only when a DB action row is intentionally removed while the snapshot action is still present. This is expected because the audit is proving that mixed-source timeline event membership changes are not normalized away. It becomes suspicious if it appears without an intentional fixture mutation, because that would indicate dropped relational reads or snapshot/DB drift in migrated routes.
Write migration notes:
- PR-4 moves the main revenue orchestrator, action execution, passive monitoring, agent pause/resume, and agent-engine action logging paths to repository-backed writes with snapshot kept as a compatibility mirror.
- Remaining snapshot-backed write edges are still present in deferred conversation/message and dev-only flows, including sync-only helper paths that run through `complianceService` / `attemptOutboundMessage()` and dev mutation routes.

## Scheduler
- Schema: In progress
- Reads: Not started
- Parity: N/A
- Writes: Not started
- Snapshot imports removed: No
- Tests: Not started
- Flag removed: No
