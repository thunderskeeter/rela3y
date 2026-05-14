# Ranked Next 10 Tasks

Scope: Ranked by impact on reliability, monetization safety, and production readiness.

1. Add deterministic quote-to-book funnel event chain
- Why: Current analytics has funnel and revenue events, but quote-specific lifecycle instrumentation is incomplete.
- Do:
  - Emit explicit events: `quote_started`, `quote_ready`, `quote_shown`, `quote_accepted`.
  - Persist with tenant/account scope and correlation id.
  - Extend revenue overview/funnel endpoints to include quote transitions.
- Target files:
  - `backend/src/services/flowEngine.js`
  - `backend/src/services/revenueIntelligenceService.js`
  - `backend/src/routes/analytics.routes.js`

2. Add intake and deterministic pricing integration tests
- Why: New intake and tint logic needs regression safety.
- Do:
  - Add tests for missing-field loops and `ready_to_quote` gates.
  - Add pricing snapshot tests for tint replacement rules.
  - Add negative tests for malformed pricing config.
- Target files:
  - `backend/test/quote.intake.integration.test.js` (new)
  - `backend/test/pricing.deterministic.integration.test.js` (new)
  - `backend/src/services/flowEngine.js`

3. Replace interval scheduler with Redis/BullMQ worker
- Why: Current scheduler is setInterval-based and documented as lightweight.
- Do:
  - Introduce BullMQ queue + worker for delayed actions.
  - Keep idempotency keys and tenant scope assertions.
  - Migrate `scheduledJobs` semantics to queue-backed execution.
- Target files:
  - `backend/src/services/scheduler.js`
  - `backend/src/services/actionExecutor.js`
  - `backend/src/services/agentEngine.js`

4. Expand route-level RBAC audit and enforcement
- Why: Tenant scoping is strong, but privileged actions need explicit and testable RBAC consistency.
- Do:
  - Audit all `/api/*` routes for role checks on privileged ops.
  - Add middleware coverage for admin/superadmin-only actions.
  - Add endpoint matrix test for allow/deny cases.
- Target files:
  - `backend/src/routes/*.routes.js`
  - `backend/src/utils/authMiddleware.js`
  - `backend/test/rbac.integration.test.js` (new)

5. Add webhook replay/tamper test suite
- Why: Signature checks and idempotency exist; dedicated regression tests are needed.
- Do:
  - Test valid signature pass, invalid signature reject.
  - Test duplicate webhook idempotent handling.
  - Test replay window behavior and expected response codes.
- Target files:
  - `backend/src/routes/webhooks.routes.js`
  - `backend/src/services/webhookIdempotencyService.js`
  - `backend/test/webhooks.security.integration.test.js` (new)

6. Enforce feature gating parity (frontend + backend)
- Why: Plan enforcement must be impossible to bypass via direct API.
- Do:
  - Map each gated feature to backend guard and frontend lock state.
  - Return machine-readable denial reasons from API.
  - Render lock reason consistently in UI.
- Target files:
  - `backend/src/routes/billing.routes.js`
  - `backend/src/routes/account.routes.js`
  - `frontend/js/app.js`

7. Add weekly owner digest job (tenant scoped)
- Why: Operations visibility and motivation loop.
- Do:
  - Generate weekly summary payload (recovered revenue, converted missed calls, bookings, response SLA).
  - Persist audit event and schedule/send via notification service.
  - Add on/off setting in account notifications.
- Target files:
  - `backend/src/services/notificationService.js`
  - `backend/src/services/passiveRevenueMonitoring.js`
  - `backend/src/routes/account.routes.js`

8. Add migration rollback checks to CI
- Why: Production durability requires reproducible up/down migration confidence.
- Do:
  - CI step: migrate up, smoke query, migrate down, migrate up.
  - Fail pipeline on rollback inconsistency.
  - Document commands in runbook.
- Target files:
  - `.github/workflows/ci.yml`
  - `backend/src/db/migrate.js`
  - `RUNBOOK.md`

9. Add conversion drop-off alerting
- Why: Early warning for quote->book regression directly impacts revenue.
- Do:
  - Add threshold config (per tenant).
  - Trigger alert when drop-off worsens over rolling windows.
  - Surface alert in topbar notifications and revenue board.
- Target files:
  - `backend/src/routes/analytics.routes.js`
  - `backend/src/services/notificationService.js`
  - `frontend/js/app.js`

10. Normalize and harden home/revenue KPI metric definitions
- Why: KPI consistency across pages and topbar avoids trust erosion.
- Do:
  - Define canonical metric definitions in one shared helper.
  - Remove page-to-page drift in recovered/booked/conversion calculations.
  - Add tests for metric helper output.
- Target files:
  - `frontend/js/app.js`
  - `backend/src/routes/analytics.routes.js`
  - `backend/test/analytics.metrics.integration.test.js` (new)

