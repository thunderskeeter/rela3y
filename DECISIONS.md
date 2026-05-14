# Decision Log

Governance enforcement: all decisions are constrained by [AGENTS.md](./AGENTS.md) and must pass hard gates in [PRODUCTION_READINESS_SCORE.md](./PRODUCTION_READINESS_SCORE.md).

## D-001
- Decision statement: Use a snapshot-in-Postgres state model with an in-memory store facade.
- Where in code: `backend/src/store/dataStore.js`, `backend/src/db/stateRepository.js`, `backend/db/migrations/001_phase1_foundation.sql`.
- Why: Keeps existing domain behavior stable while moving persistence from flat files to Postgres-backed snapshots.
- Tradeoffs: Fast migration path, but coarse-grained persistence and limited row-level transactional granularity.
- Revisit when: Normalized relational write paths for core entities are ready for cutover.

## D-002
- Decision statement: Run scheduler on a 60-second tick and run PRM every 5 ticks.
- Where in code: `backend/src/services/scheduler.js`, `backend/src/services/passiveRevenueMonitoring.js`.
- Why: Predictable low-complexity cadence with enough frequency for near-real-time follow-up.
- Tradeoffs: In-process scheduler is single-instance biased and not ideal for horizontal scale.
- Revisit when: Introducing distributed worker/queue infrastructure.

## D-003
- Decision statement: Enforce webhook idempotency with provider event receipts, preferring Postgres and falling back to memory.
- Where in code: `backend/src/services/webhookIdempotencyService.js`, `backend/src/routes/webhooks.routes.js`, `backend/db/migrations/002_security_reliability.sql`.
- Why: Prevent duplicate side effects from webhook retries/replays.
- Tradeoffs: Memory fallback is less durable than DB receipts during DB outages.
- Revisit when: Durable queue + retry infrastructure is adopted for all inbound events.

## D-004
- Decision statement: Normalize inbound activity to canonical signal types before decisioning.
- Where in code: `backend/src/services/signalService.js`, `backend/src/services/revenueIntelligenceService.js`, `backend/src/services/revenueOrchestrator.js`.
- Why: Reduces channel-specific branching and allows deterministic policy evaluation.
- Tradeoffs: Canonical map must be maintained as new channels/events are introduced.
- Revisit when: Signal taxonomy changes or new high-volume channels are added.

## D-005
- Decision statement: Decision engine must output structured ActionPlan objects consumed by executors.
- Where in code: `backend/src/services/decisionEngine.js`, `backend/src/services/aiDecisionEngine.js`, `backend/src/services/actionExecutor.js`.
- Why: Clear contract between reasoning and execution improves testability and auditability.
- Tradeoffs: Requires version discipline when ActionPlan shape evolves.
- Revisit when: ActionPlan schema versioning needs backward compatibility guarantees.

## D-006
- Decision statement: Opportunity lifecycle transitions are deterministic and stage changes are audited.
- Where in code: `backend/src/services/opportunityLifecycle.js`, `backend/src/services/revenueIntelligenceService.js`, `backend/src/services/actionLogger.js`.
- Why: Predictable lifecycle behavior and traceable stage history for ops/reporting.
- Tradeoffs: Rule-based transitions may underfit edge cases without iterative tuning.
- Revisit when: Lifecycle false-positive/false-negative rates warrant updated transition logic.

## D-007
- Decision statement: Enforce quiet hours and daily automation/follow-up caps before outbound actions.
- Where in code: `backend/src/services/revenueOrchestrator.js`, `backend/src/services/complianceService.js`, `backend/src/store/dataStore.js`.
- Why: Compliance and customer experience safeguards are mandatory for autonomous messaging.
- Tradeoffs: Conservative throttling can reduce short-term conversion in some segments.
- Revisit when: Tenant-level policy tuning and experimentation framework matures.

## D-008
- Decision statement: Keep frontend static until PMF while backend capability stabilizes.
- Where in code: `frontend/index.html`, `frontend/js/app.js`, `backend/src/app.js` (static serving).
- Why: Minimizes product surface-area churn while core revenue/reliability engine hardens.
- Tradeoffs: Limited UX state sophistication versus SPA architecture.
- Revisit when: PMF confirmed and frontend complexity justifies framework/state migration.
