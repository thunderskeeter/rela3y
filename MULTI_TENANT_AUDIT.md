# Multi-Tenant Safety Audit

## Executive summary
Status: **FAIL -> CONDITIONAL PASS (after fixes in this audit)**

Before fixes, one webhook tenant-resolution flaw could mis-route signed webhook callbacks. After fixes, tenant isolation enforcement is materially stronger across API, store access, scheduler execution, and webhook handling. Remaining risk is primarily architectural (single shared snapshot store) rather than an active route-level bypass.

## Canonical tenant boundary contract
1. Every authenticated API request must resolve `tenant.accountId` + `tenant.to` before domain handlers.
2. Every read/write touching tenant data must be scoped by `tenant.accountId` (or deterministic token->tenant mapping for public endpoints).
3. Admin-only paths may bypass tenant scoping, but only under explicit superadmin middleware and admin routes.
4. Webhooks must resolve tenant deterministically from trusted selector inputs and validate signatures before business actions.
5. Idempotency keys must be tenant-scoped.

## Risk table
| ID | Severity | Area | Status | Summary |
| --- | --- | --- | --- | --- |
| MT-001 | High | Webhooks | Fixed | Webhook tenant selector prioritized body `To` over signed query fallback, risking wrong tenant routing on voice callback variants. |
| MT-002 | Medium | Scheduler/jobs | Fixed | No explicit runtime tenant-ownership assertion before scheduled job execution. |
| MT-003 | Low | Logs/audits | Fixed | Request/error logs included full URL with query params (possible phone/tenant selector leakage in shared logs). |
| MT-004 | Medium | Data layer architecture | Open | Core runtime uses a global JSON snapshot (`app_state.snapshot`) with in-memory filtering; correctness depends on caller discipline. |

## Phase 0 discovery
- Entrypoint: [backend/server.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/server.js)
- App composition + middleware chain: [backend/src/app.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/app.js)
- Tenant resolver middleware: [backend/src/utils/accountContext.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/accountContext.js)
- Auth + account access checks: [backend/src/utils/authMiddleware.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/authMiddleware.js)
- Store pattern: [backend/src/store/dataStore.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/store/dataStore.js)
- Snapshot persistence: [backend/src/db/stateRepository.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/db/stateRepository.js)

Tenant ID resolution evidence:
- API tenant middleware mount: [backend/src/app.js:79](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/app.js:79)
- Account access guard mount: [backend/src/app.js:80](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/app.js:80)
- Webhook tenant resolver: [backend/src/utils/accountContext.js:89](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/accountContext.js:89)

## Phase 2 route audit table
Legend: `Auth?` = session/sig gating, `Tenant resolved?` = tenant context present, `Role enforced?` = explicit role checks, `Tenant scoped reads/writes?` = account-scoped data paths.

| Route | Auth? | Tenant resolved? | Role enforced? | Tenant scoped reads/writes? | Notes |
| --- | --- | --- | --- | --- | --- |
| `/api/auth/login` | No | No | No | N/A | Session bootstrap only. |
| `/api/auth/logout` | Session optional | No | No | N/A | Clears cookies/session token. |
| `/api/auth/me` | Yes | No | User | N/A | Identity endpoint only. |
| `/api/admin/*` | Yes | Explicit bypass | superadmin | Admin scope | Mounted at [app.js:75](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/app.js:75). |
| `/api/account*` | Yes | Yes | account access | Yes | Uses `req.tenant` and account-scoped reads/writes. |
| `/api/billing*` | Yes | Yes | account access | Yes | `getTenantAccount` validates `to/accountId` match. |
| `/api/contacts*` | Yes | Yes | account access | Yes | Scoped via `tenant.accountId`; conflict checks block cross-account overwrite. |
| `/api/messages*` (`threads`,`conversations`,`send`,`status`) | Yes | Yes | account access | Yes | IDs are `to__from`; route validates tenant `to`. |
| `/api/rules*` + `/api/vip*` | Yes | Yes | account access | Yes | Stored per tenant number + accountId filtering. |
| `/api/flows*` + `/api/outcome-packs*` | Yes | Yes | account access | Yes | Flow storage key is account-scoped. |
| `/api/notifications*` | Yes | Yes | account access | Yes | Settings/log are account-scoped. |
| `/api/analytics*` + `/api/automation/run-recommended` | Yes | Yes | account access | Yes | Query and aggregation scoped by accountId. |
| `/api/agent*` | Yes | Yes | account access | Yes | Opportunity/run lookups require accountId match. |
| `/api/onboarding*` | Yes | Yes | account access | Yes | Setup/options/workspace requests scoped to tenant. |
| `/api/integrations*` | Yes | Yes | owner/admin | Yes | Router-level role middleware + tenant checks. |
| `/api/dev*` | Yes | Yes | superadmin | Yes | Router-level superadmin guard. |
| `/api/public/booking/:token/*` | Public token | Token->tenant | No | Yes | Deterministic token-to-account mapping. |
| `/oauth/calendar/:provider/callback` | OAuth state | State->tenant | No | Yes | Callback binds to stored tenant in OAuth state. |
| `/webhooks/stripe` | Signature | Query `to` + per-tenant secret | No | Yes | Idempotency claim per tenant key. |
| `/webhooks/sms|missed-call|voice/*|event` | Signature/dev secret | `requireTenantForWebhook` | No | Yes | Idempotency claims include tenant key. |

## Findings (with evidence, risk, recommendation)

### MT-001 (High, Fixed): webhook tenant selector precedence could mis-route voice callback tenant
- Evidence:
  - Selector implementation: [backend/src/utils/accountContext.js:89-95](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/accountContext.js:89)
  - Previously used `bodyTo || queryTo`.
- Risk:
  - Voice callback payloads can include `To` values that are not tenant numbers in all Twilio callback variants. Prior precedence allowed accidental mis-resolution and wrong tenant binding before signature check path completion.
- Fix recommendation:
  - Prefer signed query `to` when present for webhook tenant selector; fallback to body `To`.
- Applied:
  - Changed precedence to `queryTo || bodyTo` and updated comment.

### MT-002 (Medium, Fixed): no explicit runtime tenant-scope assertions in critical execution paths
- Evidence:
  - Scheduler executes due jobs from shared queue: [backend/src/services/scheduler.js:84-150](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/services/scheduler.js:84)
  - Missing generic assert helper pre-fix.
- Risk:
  - If malformed job data appears, execution could proceed deeper before failing implicitly.
- Fix recommendation:
  - Add explicit tenant assertions (`assertTenantScope`) before execution and guard checks.
- Applied:
  - Added helper + assertions in scheduler and webhook conversation sync path.

### MT-003 (Low, Fixed): request/error logs included query parameters (PII/selector leakage risk)
- Evidence:
  - Request log path field: [backend/src/utils/requestLogger.js:20](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/requestLogger.js:20)
  - Error log path field: [backend/src/app.js:120](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/app.js:120)
- Risk:
  - Query values can include phone numbers / tenant selectors (`to`, `accountId`) in central logs.
- Fix recommendation:
  - Strip query strings from logged paths.
- Applied:
  - Added `stripQuery()` in both log points.

### MT-004 (Medium, Open): single global snapshot store remains a systemic tenant-isolation footgun
- Evidence:
  - Global snapshot read: [backend/src/db/stateRepository.js:29](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/db/stateRepository.js:29)
  - Runtime cache hydration from shared snapshot: [backend/src/store/dataStore.js:163](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/store/dataStore.js:163)
- Risk:
  - Any future missed filter can expose cross-tenant records because all tenant data is loaded together.
- Fix recommendation:
  - Move runtime read paths to normalized tenant-scoped DB tables for critical entities first (conversations, contacts, opportunities, actions), then retire snapshot-first read logic.
- Status:
  - Not changed in this audit (non-surgical architectural migration).

## Data-layer verification notes
Scoped function evidence:
- Contacts scoped: [dataStore.js:521-524](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/store/dataStore.js:521)
- Conversations scoped: [dataStore.js:528-531](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/store/dataStore.js:528)
- Conversation by ID scoped: [dataStore.js:535](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/store/dataStore.js:535)
- Rules scoped: [dataStore.js:559](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/store/dataStore.js:559)
- Flows scoped: [dataStore.js:542](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/store/dataStore.js:542)
- Cross-tenant contact upsert blocked: [dataStore.js:584](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/store/dataStore.js:584)

## Webhooks + idempotency verification
- Webhook tenant binding middleware: [accountContext.js:202](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/accountContext.js:202)
- Signature enforcement: [accountContext.js:226](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/accountContext.js:226)
- Idempotency persistence key includes tenant (`account_id`): [webhookIdempotencyService.js:16-21](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/services/webhookIdempotencyService.js:16)

## Scheduler/jobs verification
- Due job fetch from shared queue: [scheduler.js:89](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/services/scheduler.js:89)
- Added tenant-scope assertion before execution: [scheduler.js:149](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/services/scheduler.js:149)
- Added tenant-scope assertion in guard: [scheduler.js:205](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/services/scheduler.js:205)

## Frontend fetch audit
- UI appends tenant selectors (`to/accountId`) in request helper, but server still enforces `requireAccountAccess`:
  - UI tenant query helper: [frontend/js/app.js:12730-12738](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/frontend/js/app.js:12730)
  - Backend account access enforcement: [backend/src/utils/authMiddleware.js:61-67](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/authMiddleware.js:61)
- Conclusion:
  - Client can choose selector inputs, but unauthorized account traversal is rejected server-side.

## Fixes applied (with diffs)

### 1) Add tenant guard helpers (`requireTenant(req)`, `assertTenantScope`)
```diff
--- a/backend/src/utils/tenant.js
+++ b/backend/src/utils/tenant.js
@@
-  requireTenant,
+  requireTenant: requireTenantMiddleware,
@@
+function requireTenant(req) {
+  const accountId = String(req?.tenant?.accountId || '').trim();
+  const to = String(req?.tenant?.to || '').trim();
+  if (!accountId || !to) {
+    const err = new Error('Missing tenant context on request');
+    err.status = 400;
+    err.code = 'TENANT_REQUIRED';
+    throw err;
+  }
+  return req.tenant;
+}
+
+function assertTenantScope(expectedTenantId, actualTenantId, { entity = 'entity', status = 403 } = {}) {
+  ...
+}
@@
+  requireTenantMiddleware,
+  assertTenantScope,
```

### 2) Harden webhook tenant selector precedence
```diff
--- a/backend/src/utils/accountContext.js
+++ b/backend/src/utils/accountContext.js
@@
-  // Prefer body To (Twilio standard); allow signed query `to` fallback ...
+  // Prefer signed query `to` when present ...
@@
-  const value = bodyTo || queryTo;
+  const value = queryTo || bodyTo;
```

### 3) Enforce tenant helper in webhook handlers + scope assertion
```diff
--- a/backend/src/routes/webhooks.routes.js
+++ b/backend/src/routes/webhooks.routes.js
@@
+const { requireTenant, assertTenantScope } = require('../utils/tenant');
@@
-const tenant = req.tenant;
+const tenant = requireTenant(req);
@@
+if (convo?.accountId) {
+  assertTenantScope(tenant.accountId, convo.accountId, { entity: 'webhook missed-call conversation' });
+}
```

### 4) Add scheduler tenant assertions
```diff
--- a/backend/src/services/scheduler.js
+++ b/backend/src/services/scheduler.js
@@
+const { assertTenantScope } = require('../utils/tenant');
@@
+if (conversation?.accountId) {
+  assertTenantScope(job.accountId, conversation.accountId, { entity: 'scheduled job conversation' });
+}
@@
+if (conversation?.accountId) {
+  assertTenantScope(job.accountId, conversation.accountId, { entity: 'scheduled job guard conversation' });
+}
```

### 5) Strip query params from request/error logs
```diff
--- a/backend/src/utils/requestLogger.js
+++ b/backend/src/utils/requestLogger.js
@@
+function stripQuery(url) { ... }
@@
-      path: req.originalUrl || req.url,
+      path: stripQuery(req.originalUrl || req.url),
```

```diff
--- a/backend/src/app.js
+++ b/backend/src/app.js
@@
+function stripQuery(url) { ... }
@@
-      path: req?.originalUrl || req?.url || '',
+      path: stripQuery(req?.originalUrl || req?.url || ''),
```

## Modified files
- [backend/src/utils/tenant.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/tenant.js)
- [backend/src/utils/accountContext.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/accountContext.js)
- [backend/src/routes/webhooks.routes.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/routes/webhooks.routes.js)
- [backend/src/services/scheduler.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/services/scheduler.js)
- [backend/src/utils/requestLogger.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/requestLogger.js)
- [backend/src/app.js](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/app.js)

## Remaining TODOs
1. Replace shared snapshot read path (`app_state.snapshot`) with tenant-scoped table reads in runtime services.
2. Add automated regression tests for webhook tenant resolution precedence (`query to` vs `body To`) and scheduler scope assertions.
3. Add central log redaction policy for phone fields in service-level `console.log` calls.

## Unsafe query lint notes
Avoid these patterns in future code:
- `find(... id === req.params.id)` without `accountId` predicate.
- `data.<collection>` scans without `.filter(accountId)` for tenant data.
- Trusting `tenantId/accountId` from body/query without `canUserAccessAccount` validation.
- Logging full URLs or payloads containing phone/account selectors.

## Final verification checklist (proof)
- `node --check` passed for all modified JS files.
- Webhook tenant selector now prefers signed query fallback ([accountContext.js:95](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/accountContext.js:95)).
- Scheduler enforces tenant assertion before execution ([scheduler.js:149](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/services/scheduler.js:149)).
- Request/error path logging strips query strings ([requestLogger.js:20](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/utils/requestLogger.js:20), [app.js:120](/C:/Users/justi/OneDrive/Desktop/relay_dashboard_company/backend/src/app.js:120)).

## Ready to sell?
Verdict: **Not fully ready yet**.

Conditionally sellable only if:
1. Open architectural item MT-004 is accepted as bounded risk for current stage.
2. Test plan in `MULTI_TENANT_TEST_PLAN.md` is executed and signed off.
3. Follow-up migration plan from snapshot-first to tenant-scoped DB reads is scheduled and tracked.

Current finding counts:
- High: 1 (fixed)
- Medium: 2 (1 fixed, 1 open)
- Low: 1 (fixed)
