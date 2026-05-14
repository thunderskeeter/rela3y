# PRODUCTION READINESS SCORING SYSTEM (PRS)
Relay must be evaluated against this PRS on every meaningful change.

## How to use (mandatory)
For every PR / change set:
1) Fill out the PRS checklist below.
2) Compute the score (0–100).
3) If any HARD GATE fails, the PR is blocked until fixed.
4) Include the final PRS report in the PR description.

---

# HARD GATES (PASS/FAIL) — PR BLOCKERS
If ANY of these fail: **DO NOT MERGE / DO NOT SHIP.**

## G1 — Secrets Safety
- [ ] No `.env` or secrets committed or bundled.
- [ ] No API keys/tokens in source, logs, sample payloads, screenshots, or test fixtures.
- [ ] Config uses environment variables + documented `.env.example`.

**Fail examples:** `.env` in repo, leaked Twilio token, Stripe secret in logs.

## G2 — Tenant Isolation
- [ ] Every read/write path is scoped by tenant/account and enforced server-side.
- [ ] No client-controlled tenant context without server verification.
- [ ] Cross-tenant access is impossible via ID guessing.

**Fail examples:** `GET /api/contacts?id=...` returns data from another tenant.

## G3 — Auth & Authorization Consistency
- [ ] One auth strategy is used end-to-end (cookie sessions OR bearer tokens), documented.
- [ ] Authorization is checked on all protected routes.
- [ ] Privileged actions require RBAC checks.

**Fail examples:** frontend uses localStorage demo tokens while backend expects cookie sessions.

## G4 — Webhook Security & Idempotency
- [ ] Webhooks are signature-verified (Twilio/Stripe/etc) using official methods.
- [ ] Webhooks are idempotent (dedupe key / event id stored).
- [ ] Replays do not mutate state twice.

**Fail examples:** duplicate Stripe event creates two subscriptions.

## G5 — Persistence Is Production-Grade
- [ ] No flat-file persistence for production data (no JSON store as primary DB).
- [ ] Writes are transactional where required.
- [ ] Schema migrations exist and are reproducible.

**Fail examples:** data stored in `data/*.json` on the app server.

## G6 — Observability Minimum
- [ ] Errors are not swallowed; they are logged with context.
- [ ] There is a consistent request correlation id (or equivalent).
- [ ] Critical paths emit structured logs.

**Fail examples:** silent catch blocks; “it failed but no logs”.

---

# SCORING (0–100)
Each category is scored 0–10. Multiply by weight and sum.

**Formula:** Total = Σ (CategoryScore/10 * CategoryWeight)

## Category Weights
1) Security & Secrets Hygiene — weight 20
2) Multi-tenant Isolation — weight 20
3) Data Layer & Migrations — weight 15
4) Reliability & Idempotency — weight 15
5) Auth/RBAC Correctness — weight 10
6) Observability & Debuggability — weight 8
7) Test Coverage & Regression Safety — weight 7
8) Performance & Scalability Basics — weight 3
9) Frontend Product Quality — weight 2

Total weight = 100

---

# 1) SECURITY & SECRETS HYGIENE (0–10) [weight 20]
Score guidance:
- 0–2: secrets in repo, no validation, unsafe defaults
- 3–5: basics present but missing validation/rate-limits/log redaction
- 6–8: strong baseline, inputs validated, rate limiting, minimal attack surface
- 9–10: threat model + continuous checks + secure defaults everywhere

Checklist:
- [ ] `.env` not tracked; `.env.example` present
- [ ] Keys rotated and stored only in env/secret manager
- [ ] Input validation exists on all write endpoints (zod/joi/etc)
- [ ] Rate limiting exists on auth + webhooks + abuse-prone endpoints
- [ ] Logs redact secrets/PII where appropriate

Notes / evidence:

Score (0–10):

---

# 2) MULTI-TENANT ISOLATION (0–10) [weight 20]
Checklist:
- [ ] Tenant is derived server-side (session/token → tenant)
- [ ] Every query has tenant scope condition
- [ ] No “admin bypass” without explicit role
- [ ] Tests exist for cross-tenant access attempts

Notes / evidence:

Score (0–10):

---

# 3) DATA LAYER & MIGRATIONS (0–10) [weight 15]
Checklist:
- [ ] PostgreSQL (or equivalent) is the source of truth
- [ ] Migrations exist, can rebuild from zero
- [ ] Connection pooling configured
- [ ] Constraints + indexes defined
- [ ] Transactions used for multi-step writes
- [ ] Background jobs do not depend on in-memory state

Notes / evidence:

Score (0–10):

---

# 4) RELIABILITY & IDEMPOTENCY (0–10) [weight 15]
Checklist:
- [ ] Webhooks + external calls are idempotent
- [ ] Retry strategy exists with backoff
- [ ] Job queue exists for background tasks (BullMQ/Redis etc) OR explicit reasoning why not needed
- [ ] Failure modes are tracked and visible (dead-letter / failed jobs)
- [ ] No duplicate side-effects on retries

Notes / evidence:

Score (0–10):

---

# 5) AUTH / RBAC CORRECTNESS (0–10) [weight 10]
Checklist:
- [ ] Single auth scheme and documented
- [ ] Cookies: CSRF strategy defined OR bearer tokens: storage strategy justified
- [ ] RBAC middleware enforced on privileged endpoints
- [ ] Session/token expiry and refresh behavior defined
- [ ] Password handling uses modern KDF (scrypt/argon2/bcrypt) with sane params

Notes / evidence:

Score (0–10):

---

# 6) OBSERVABILITY & DEBUGGABILITY (0–10) [weight 8]
Checklist:
- [ ] Structured logs (JSON or consistent format) include request id, tenant id, user id (if safe)
- [ ] Errors have stack traces (server-side)
- [ ] Health endpoint exists (and checks DB connectivity)
- [ ] Monitoring hooks exist (Sentry or equivalent) OR clear plan

Notes / evidence:

Score (0–10):

---

# 7) TEST COVERAGE & REGRESSION SAFETY (0–10) [weight 7]
Checklist:
- [ ] Integration tests for core flows (auth, tenant isolation, contacts/messages, webhooks)
- [ ] Webhook signature tests
- [ ] CI runs tests and fails build on regression
- [ ] “Golden path” demo script exists and is repeatable

Notes / evidence:

Score (0–10):

---

# 8) PERFORMANCE & SCALABILITY BASICS (0–10) [weight 3]
Checklist:
- [ ] No N+1 patterns on hot endpoints (basic review)
- [ ] Pagination on list endpoints
- [ ] Reasonable indexing for search/list
- [ ] Background work off the request path where needed

Notes / evidence:

Score (0–10):

---

# 9) FRONTEND PRODUCT QUALITY (0–10) [weight 2]
Checklist:
- [ ] Consistent auth UX (no demo auth leakage)
- [ ] Loading states + error states exist for core pages
- [ ] No console spam; errors are user-safe
- [ ] Key flows are easy: onboarding, connect number, view leads, respond, report

Notes / evidence:

Score (0–10):

---

# PRS RESULT
## HARD GATES
G1 Secrets Safety: PASS / FAIL
G2 Tenant Isolation: PASS / FAIL
G3 Auth/RBAC Consistency: PASS / FAIL
G4 Webhook Security & Idempotency: PASS / FAIL
G5 Persistence Production-Grade: PASS / FAIL
G6 Observability Minimum: PASS / FAIL

## Score Calculation
Security (20):      ( /10) =>
Tenant (20):        ( /10) =>
Data (15):          ( /10) =>
Reliability (15):   ( /10) =>
Auth/RBAC (10):     ( /10) =>
Observability (8):  ( /10) =>
Tests (7):          ( /10) =>
Perf (3):           ( /10) =>
Frontend (2):       ( /10) =>

TOTAL PRS (0–100):

## Interpretation
- 0–49: Prototype / unsafe to sell
- 50–69: Small pilot only (friendly customers, limited liability)
- 70–84: Sellable v1 with guardrails (single-region, modest scale)
- 85–94: Production-ready SaaS baseline
- 95–100: Strongly production-hardened

## Required next actions (if score < 85)
List the top 5 items that most increase score fast:
1)
2)
3)
4)
5)