# RELAY PRODUCTION ROADMAP
Goal: Transform Relay from advanced prototype to production-grade, scalable, secure SaaS that can be safely sold.

Execution is divided into controlled phases.
No phase may be skipped.
Each phase must pass its exit criteria before moving forward.

--------------------------------------------------
PHASE 0 – FOUNDATION AUDIT
--------------------------------------------------

Objective:
Establish a clear understanding of current architecture and production gaps.

Owner:
CTO Agent

Deliverables:
- Full architecture audit
- Production readiness score (0–100)
- List of critical risks
- Data persistence review
- Auth flow review
- Tenant isolation review

Exit Criteria:
- Written audit document
- Identified top 10 production blockers

--------------------------------------------------
PHASE 1 – DATA LAYER REBUILD
--------------------------------------------------

Objective:
Replace JSON storage with production-grade relational database.

Owner:
Database Architect

Tasks:
- Design PostgreSQL schema
- Create migration system
- Implement connection pooling
- Add tenant isolation enforcement
- Replace dataStore.js logic

Must Include:
- Foreign keys
- Indexing strategy
- Transaction support
- Audit log persistence

Exit Criteria:
- No flat file persistence
- All services use database layer
- Migration scripts reproducible

--------------------------------------------------
PHASE 2 – SECURITY HARDENING
--------------------------------------------------

Objective:
Make Relay safe to sell.

Owner:
Security Engineer

Tasks:
- Remove secrets from repo
- Rotate API keys
- Add request rate limiting
- Add input validation layer
- Enforce RBAC everywhere
- Add CSRF protection
- Validate all webhook signatures
- Implement idempotency keys

Must Produce:
- Threat model
- Security checklist
- Attack surface diagram

Exit Criteria:
- No unauthenticated data exposure
- Tenant isolation tested
- Webhooks tamper-resistant

--------------------------------------------------
PHASE 3 – BACKEND STABILIZATION
--------------------------------------------------

Objective:
Ensure deterministic behavior at scale.

Owner:
Backend Systems Engineer

Tasks:
- Convert lifecycle logic to event-driven architecture
- Introduce job queue (Redis + BullMQ)
- Implement retry logic
- Add structured logging
- Add failure monitoring
- Remove race conditions

Exit Criteria:
- No background logic tied to in-memory timers
- All async operations retry-safe
- Logs structured and queryable

--------------------------------------------------
PHASE 4 – TESTING & VALIDATION
--------------------------------------------------

Objective:
Eliminate silent failure risk.

Owner:
QA Automation Engineer

Tasks:
- Add integration tests (Supertest)
- Add tenant isolation tests
- Add webhook signature tests
- Add lifecycle regression suite
- Add CI test runner

Exit Criteria:
- Core flows covered by automated tests
- CI fails on regression
- Deployment blocked if tests fail

--------------------------------------------------
PHASE 5 – FRONTEND PROFESSIONALIZATION
--------------------------------------------------

Objective:
Make Relay feel like a real SaaS product.

Owner:
Frontend Architect + UX Specialist

Tasks:
- Unify authentication system
- Remove demo-only logic
- Improve loading states
- Improve error handling
- Add onboarding flow
- Add ROI dashboard highlights
- Add revenue insight panels

Exit Criteria:
- Consistent auth model
- No broken loading states
- Clear value proposition on dashboard

--------------------------------------------------
PHASE 6 – BILLING & MONETIZATION
--------------------------------------------------

Objective:
Enable revenue.

Owner:
Billing Engineer

Tasks:
- Integrate Stripe
- Add subscription tiers
- Add feature gating middleware
- Add usage tracking
- Add billing event logging

Exit Criteria:
- Plans enforceable
- Upgrades/downgrades supported
- Webhook billing reconciliation implemented

--------------------------------------------------
PHASE 7 – INFRASTRUCTURE & DEPLOYMENT
--------------------------------------------------

Objective:
Prepare for real-world scale.

Owner:
DevOps Engineer

Tasks:
- Dockerize backend
- Add environment config separation
- Add CI/CD pipeline
- Add health checks
- Add centralized logging
- Add monitoring (Sentry)
- Define horizontal scaling plan

Exit Criteria:
- Deployment reproducible
- Logs centralized
- Zero-downtime deploy possible

--------------------------------------------------
PHASE 8 – SCALE READINESS REVIEW
--------------------------------------------------

Objective:
Final production audit before selling.

Owner:
CTO Agent

Checklist:
- Can 100 tenants run safely?
- Can webhook storms be handled?
- Can a malicious tenant break isolation?
- Can background workers recover from crash?
- Can new engineers onboard in <1 week?

Exit Criteria:
- Production readiness score > 85/100
- No critical open security issues
- Infrastructure documented

--------------------------------------------------
RULES OF EXECUTION
--------------------------------------------------

- No new features until Phase 3 is complete.
- No monetization until Phase 2 is complete.
- No marketing until Phase 6 is complete.
- Each phase must produce written documentation.
- Every major change must consider scalability and security impact.

--------------------------------------------------
PHASE 9 - CONVERSION + OPERATIONS HARDENING
--------------------------------------------------

Objective:
Turn the current system into a measurable, reliable revenue engine with defensible business metrics and enforceable operations.

Owner:
CTO Agent coordinating Backend, QA, Security, Frontend, and Billing roles.

Workstreams:

1) Conversion tracking end-to-end
- Track each lead through: source -> intent -> quote_shown -> booking_created -> revenue_closed.
- Add funnel drop-off visibility and alerting for step regressions.
- Add source-level performance views for owner decisions.

Exit Criteria:
- Every booked/won record has attributable source and intent.
- Funnel stages visible in dashboard with drop-off percentages.
- Automated alert generated when quote->book conversion drops below threshold.

2) Quote-to-booking reliability
- Add deterministic tests for intake completeness and ready_to_quote gating.
- Add deterministic pricing snapshot tests for key services (detailing/tint).
- Add config validation so missing pricing config cannot silently produce bad quotes.

Exit Criteria:
- Test suite covers intake gate + pricing outputs.
- Invalid pricing config fails fast with explicit errors.
- No quote sent without required intake fields for that service.

3) Billing + plan enforcement polish
- Audit and unify feature gating across frontend routes and backend endpoints.
- Add explicit lock-state reasons for blocked features.
- Ensure billing state changes reconcile quickly and predictably.

Exit Criteria:
- No gated feature accessible by URL or direct API call when blocked.
- Locked UI states display reason and required plan.
- Billing state changes reflected in UI and API within one refresh cycle.

4) Ops visibility for clients
- Add "Today's wins" module and weekly owner digest pipeline.
- Surface response SLA, recovered calls, booked jobs, and recovered revenue in one view.
- Ensure digest reflects tenant-only scoped data.

Exit Criteria:
- Daily dashboard summary visible in home/revenue surfaces.
- Weekly digest event is generated and auditable per tenant.
- Metrics in digest match analytics API values.

5) Data durability + migration hardening
- Complete migration away from legacy flat-file assumptions in runtime paths.
- Add migration rollback drills in CI and runbook.
- Add durability checks for restart/recovery scenarios.

Exit Criteria:
- Critical runtime paths do not depend on JSON file semantics.
- CI verifies migrate up/down for core migrations.
- Restart simulation confirms no data loss for opportunities/actions/jobs.

6) Security hardening pass
- Enforce RBAC and tenant checks on all protected endpoints.
- Add webhook signature + replay + abuse tests.
- Expand rate-limit coverage for high-risk routes.

Exit Criteria:
- Route audit passes with zero unauthorized cross-tenant access paths.
- Webhook tamper and replay tests pass.
- Rate-limit protections present on auth, webhook, and abuse-prone APIs.
