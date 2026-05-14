# Multi-Tenant Test Plan

## Manual role matrix
| Role | Expected tenant access | Must fail on cross-tenant selector? | Key screens |
| --- | --- | --- | --- |
| Superadmin | Admin routes + selectable tenant workspaces | N/A for `/api/admin/*`; yes for tenant APIs when selector unauthorized | Admin, Integrations, Billing, Dev |
| Owner | Assigned tenant(s) only | Yes (`404 Not found`) | Messages, Contacts, Analytics, Billing |
| Admin | Assigned tenant(s) only | Yes (`404 Not found`) | Settings, Integrations, Team |
| Agent | Assigned tenant(s) only with limited features | Yes (`404 Not found`) | Agent queue, conversations |
| Readonly/member | Assigned tenant(s) read-focused | Yes (`404 Not found`) | Dashboard, analytics read paths |

## API test cases (existing routes)
Prereqs:
- Backend running on `http://127.0.0.1:3001`
- Valid session cookie (`relay_sid`) and csrf (`relay_csrf`) for authenticated tests

### 1) Auth baseline
```bash
curl -i -X POST http://127.0.0.1:3001/api/auth/login \
  -H "Content-Type: application/json" \
  --data '{"email":"owner@example.com","password":"***"}'
```
Expected: `200`, sets `relay_sid` + `relay_csrf` cookies.

### 2) Tenant-scoped read succeeds for authorized tenant
```bash
curl -i "http://127.0.0.1:3001/api/conversations?accountId=<authorized_account_id>" \
  -H "x-csrf-token: <csrf>" \
  -H "Cookie: relay_sid=<sid>; relay_csrf=<csrf>"
```
Expected: `200`, only conversations for that account.

### 3) Cross-tenant read is blocked
```bash
curl -i "http://127.0.0.1:3001/api/conversations?accountId=<different_account_id>" \
  -H "x-csrf-token: <csrf>" \
  -H "Cookie: relay_sid=<sid>; relay_csrf=<csrf>"
```
Expected: `404` via account access guard.

### 4) ID route cannot pivot tenants
```bash
curl -i "http://127.0.0.1:3001/api/conversations/%2B15550001111__%2B15550002222?accountId=<authorized_account_id>" \
  -H "x-csrf-token: <csrf>" \
  -H "Cookie: relay_sid=<sid>; relay_csrf=<csrf>"
```
Expected: `404` unless conversation belongs to active tenant and `to` matches tenant number.

### 5) Webhook dedupe is tenant-scoped
```bash
curl -i -X POST "http://127.0.0.1:3001/webhooks/event?to=%2B15550001111" \
  -H "Content-Type: application/json" \
  -H "x-dev-webhook-secret: <dev_secret>" \
  --data '{"id":"evt_same_id","type":"lead_lost","from":"+15550009999","data":{}}'

curl -i -X POST "http://127.0.0.1:3001/webhooks/event?to=%2B15550001111" \
  -H "Content-Type: application/json" \
  -H "x-dev-webhook-secret: <dev_secret>" \
  --data '{"id":"evt_same_id","type":"lead_lost","from":"+15550009999","data":{}}'
```
Expected: second call returns `duplicate: true`.

## Webhook simulation steps
Use existing dev endpoints only.

1. Simulate missed call for active tenant:
```bash
curl -i -X POST "http://127.0.0.1:3001/api/dev/revenue/simulate?accountId=<authorized_account_id>" \
  -H "Content-Type: application/json" \
  -H "x-csrf-token: <csrf>" \
  -H "Cookie: relay_sid=<sid>; relay_csrf=<csrf>" \
  --data '{"scenario":"missed_call_business_hours","from":"+15550009999"}'
```
2. Simulate inbound SMS and confirm automation + scoped conversation updates:
```bash
curl -i -X POST "http://127.0.0.1:3001/api/dev/revenue/simulate?accountId=<authorized_account_id>" \
  -H "Content-Type: application/json" \
  -H "x-csrf-token: <csrf>" \
  -H "Cookie: relay_sid=<sid>; relay_csrf=<csrf>" \
  --data '{"scenario":"inbound_sms_reply","from":"+15550009999","text":"Need quote"}'
```
3. Re-run with a different tenant selector not in user account list.
Expected: `404`.

## Scheduler verification steps
1. Trigger a scenario that schedules automation (`silent_lead`):
```bash
curl -i -X POST "http://127.0.0.1:3001/api/dev/revenue/simulate?accountId=<authorized_account_id>" \
  -H "Content-Type: application/json" \
  -H "x-csrf-token: <csrf>" \
  -H "Cookie: relay_sid=<sid>; relay_csrf=<csrf>" \
  --data '{"scenario":"silent_lead","from":"+15550009999"}'
```
2. Wait for scheduler tick (~60s) and inspect logs.
Expected logs:
- Scheduled execution entries for the same tenant only.
- No tenant-scope mismatch errors for normal flow.
3. Negative check: manually tamper a scheduled job `accountId` in store (dev only) and wait tick.
Expected:
- Job is failed/cancelled by scope assertion path, not executed against another tenant.

## Analytics aggregation checks
1. Query `/api/analytics/revenue-overview?accountId=<tenantA>` and store totals.
2. Query `/api/analytics/revenue-overview?accountId=<tenantB>` as same user if authorized for both.
Expected: disjoint totals; no bleed across tenants.
3. Query tenantB as tenantA-only user.
Expected: `404`.

## Logs/audit checks
1. Hit a route with query params (`?to=...&accountId=...`).
2. Confirm request log path does not include query string.
Expected: path logged as route path only (query stripped).

## Regression checklist
- [ ] `node --check` passes on modified files.
- [ ] `/api/*` unauthorized tenant selectors return `404`.
- [ ] Webhook signature + tenant resolution still accepts valid payloads.
- [ ] Duplicate webhook IDs are deduped per tenant.
- [ ] Scheduler executes only account-matching jobs.
- [ ] Analytics endpoints remain account-scoped.
- [ ] Request/error logs omit query strings.
- [ ] No new dependency introduced.
