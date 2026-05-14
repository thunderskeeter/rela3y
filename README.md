# Revenue Recovery Operating System

Revenue Recovery OS (AI Receptionist / AI Revenue Employee) is a Node/Express backend that serves a static frontend and exposes tenant-scoped APIs designed for recovering missed revenue, managing opportunities, scheduling, billing, and integrations.

## Requirements

- Node.js 20+
- npm 10+

## Local Development

1. Copy `backend/.env.example` to `backend/.env`.
2. Fill required values (`AUTH_SECRET` at minimum).
3. Install dependencies:
   - `npm --prefix backend install`
4. Run DB migrations:
   - `npm --prefix backend run migrate`
4. Start server:
   - `npm --prefix backend run dev`
5. Open:
   - `http://127.0.0.1:3001`

## Production Environment Variables

- `NODE_ENV=production`
- `PORT=3001` (or platform-assigned port)
- `DEV_MODE=false`
- `DATABASE_URL=postgres://...`
- `AUTH_SECRET=<long-random-secret>`
- `APP_PUBLIC_BASE_URL=https://your-app-domain.com`
- `COOKIE_SECURE=true`
- `COOKIE_SAMESITE=Lax` (or `None` if cross-site cookies are required with HTTPS)
- `CORS_ORIGINS=https://your-app-domain.com`
- `USE_DB_CONTACTS=true`
- `USE_DB_CONVERSATIONS=true`
- `USE_DB_MESSAGES=true`

If you use integrations, also set the related keys from `backend/.env.example`.

## Render

This repo includes `render.yaml` for a single Docker web service plus Render PostgreSQL. Use `/ready` as the health check. Set `AUTH_SECRET`, `APP_PUBLIC_BASE_URL`, and `CORS_ORIGINS` in Render before first deploy; add `CAL_OAUTH_REDIRECT_BASE` and `WEBHOOK_PUBLIC_BASE` only after calendar or webhook integrations are live.

## Docker

Build:

```bash
docker build -t relay-dashboard .
```

Run:

```bash
docker run --rm -p 3001:3001 --env-file backend/.env relay-dashboard
```

PostgreSQL must be reachable by `DATABASE_URL` before backend startup.

## CI

GitHub Actions workflow at `.github/workflows/ci.yml` performs install + syntax checks for backend and frontend JavaScript.
It also runs PostgreSQL-backed migrations and security integration tests.

## Revenue Recovery OS (PRM) Notes

- PRM loop runs inside `backend/src/services/scheduler.js` every 5 scheduler ticks (about every 5 minutes).
- PRM scans open/at-risk opportunities, recomputes deterministic risk, and triggers orchestrated recovery when risk crosses threshold.
- Deterministic risk rules (MVP) are implemented in `backend/src/services/revenueIntelligenceService.js`:
  - `no_response_30m` (+40)
  - `missed_call_no_reply_45m` (+35)
  - `after_hours_lead` (+20)
  - `negative_sentiment` (+20)
  - `high_urgency` (+25)
- Quiet hours and opt-out compliance are respected before outbound sends.

### Tenant Settings Added

Per-account settings now include:

- `account.settings.featureFlags`
  - `enableOptimization` (default `false`)
  - `enableAIMessageVariants` (default `false`)
  - `enableMoneyProjections` (default `false`)
- `account.settings.policies`
  - `dailyFollowupCapPerLead` (default `2`)
  - `minCooldownMinutes` (default `30`)
  - `quietHours` (`startHour`, `endHour`, `timezone`)
  - `maxAutomationsPerOpportunityPerDay` (default `4`)

### Lifecycle + Audit

- Opportunities now track deterministic lifecycle stages:
  - `NEW`, `CONTACTED`, `ENGAGED`, `QUALIFIED`, `BOOKED`, `WON`, `LOST`, `STALE`
- Every stage change is persisted in `stageHistory`.
- Automated actions are stored in `actions[]` with:
  - action payload
  - deterministic justification
  - outcome status (`pending`, `sent`, `failed`, `skipped`)
- Optimization runs are stored in `optimizationEvents[]`.

### Local Simulation (no Twilio required)

Use superadmin auth and call:

- `POST /api/dev/revenue/simulate` with `{"scenario":"missed_call_business_hours","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"inbound_sms_reply","from":"+18145550199","text":"Yes, still interested"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"after_hours_missed_call","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"silent_lead","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"reactivation"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"prm_cooldown","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"daily_cap","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"quiet_hours_schedule","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"stage_transitions","from":"+18145550199"}`
- `POST /api/dev/revenue/run-optimization` with `{}`

Traceability is persisted in:

- `leadEvents[]` -> normalized signal history
- `revenueOpportunities[]` -> opportunity/risk state
- scheduled followups in `scheduledJobs[]`
- alerts in `alerts[]`
- action audit in `actions[]`
- optimization change log in `optimizationEvents[]`

### Verification Endpoints

- `GET /api/analytics/activity-feed?limit=50`
- `GET /api/analytics/optimization-log?limit=50`
- `GET /api/analytics/revenue-overview`
- `GET /api/analytics/at-risk`

## Autonomous Revenue Agent Mode

Agent mode adds deterministic plan-driven automation (`plan -> execute -> verify -> adapt`) with optional review queue approvals.

### Agent Data Collections

- `agentRuns[]`:
  - stores run plan, status, step state, trigger, mode, and correlation id.
- `reviewQueue[]`:
  - stores pending approval items for review-required steps.
- `actions[]`:
  - now includes `runId`, `stepId`, `correlationId`, `idempotencyKey`, and `dryRun` where applicable.

### Agent Settings

Per-account defaults now include:

- `account.settings.featureFlags.enableAgentMode` (default `true`)
- `account.settings.playbookOverrides` (tenant-level industry playbook overrides)

### Reliability Guardrails

- Opportunity lock (`agentState.lockedUntil`, `agentState.lockOwner`) prevents double-run collisions.
- Step action idempotency enforced via `idempotencyKey`.
- Scheduled jobs for agent steps persist in `scheduledJobs[]` with run metadata (`runId`, `stepId`, `correlationId`).
- Replay dry-run supported via dev endpoint and logs auditable `skipped` actions (`dry_run_would_send`).

### Agent Endpoints

- `POST /api/agent/start` body: `{ "opportunityId":"...", "mode":"AUTO|REVIEW_REQUIRED|MANUAL" }`
- `GET /api/agent/opportunity/:id/run`
- `POST /api/agent/opportunity/:id/pause`
- `POST /api/agent/opportunity/:id/resume`
- `POST /api/agent/run/:id/cancel`
- `GET /api/agent/review-queue?limit=50`
- `POST /api/agent/review-queue/:id/approve`
- `POST /api/agent/review-queue/:id/reject`
- `GET /api/analytics/agent-metrics?range=30d`
- `GET /api/analytics/playbook-performance?range=30d`

### Agent Dev Scenarios

Use superadmin auth:

- `POST /api/dev/revenue/simulate` with `{"scenario":"agent_run_missed_call_home_services","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"agent_run_after_hours_medspa","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"agent_review_required_escalation","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"agent_lock_contention","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"agent_idempotency_double_fire","from":"+18145550199"}`
- `POST /api/dev/revenue/simulate` with `{"scenario":"agent_replay_dry_run","from":"+18145550199"}`
- `POST /api/dev/agent/replay-run` with `{"runId":"<existingRunId>","dryRun":true}`

## Signal Intelligence System

- **Signal canonicalization** lives in `backend/src/services/signalService.js`. All incoming events (missed calls, chats, IG DMs, abandoned quotes, booking threats, etc.) are normalized into the canonical signal types (`inbound_call_missed`, `inbound_message_received`, `web_form_submitted`, `ig_dm_received`, `chat_unanswered`, `quote_abandoned`, `appointment_no_show_risk`, `lead_stalled`) before being stored in `leadEvents[]`.
- **Decision engine** (`backend/src/services/decisionEngine.js`) wraps `aiDecisionEngine.js`, injects business context/policies, and produces structured `ActionPlan` objects describing what to do now, what to do later, and which signal triggered it.
- **Action executor** (`backend/src/services/actionExecutor.js`) handles sending SMS, scheduling follow-ups, and logging outcomes. Every action logs the originating signal ID, decision plan, outcome status, and compliance metadata inside `actions[]`, so you can always answer “which signal → which plan → which action → which outcome.”
- **Monitoring/PRM loop** continues evaluating leads via `revenueOrchestrator.handleSignal`, re-running the decision pipeline when the periodic scanner or follow-up jobs detect stalled opportunities.

## Workspace Provisioning

- **Request workspace** flow (`POST /api/onboarding/workspace-request`) captures email, business name, industry, and revenue priorities. Requests persist in the new `workspaceRequests[]` collection and power the templated onboarding (industry playbooks, finance defaults).
- **Admin review** is available via `GET /api/onboarding/workspace-requests` (scoped to the tenant) so operations/CSA teams can track incoming demand and approve/provision accounts before any user logs in.

## Outcome Pack Onboarding

- `GET /api/outcome-packs` now returns the curated packs (Recover Missed Calls, After-Hours Receptionist, Lead Qualification + Booking, Review Capture + Auto Reply, Reactivation Campaign) with their signal triggers, deterministic AI prompts, follow-up cadence, and live metrics (`signalsCaptured`, `recoveredValueCents`, `atRiskValueCents`).
- `POST /api/outcome-packs/:id/enable` / `.../disable` toggles the pack, updates tenant settings, and mirrors the toggle on the underlying flow templates so the pack can be turned on without opening the automation builder.
- `GET /api/onboarding/options` exposes the outcome packs, onboarding progress, and default industry/avg ticket values.
- `POST /api/onboarding/setup` is the lightweight wizard payload: `{ businessType, outcomePacks, avgTicketValueCents, bookingUrl, phoneConnected, calendarConnected, goLive }`. It writes industry metadata, finance defaults, selected pack map, and onboarding status to `account.settings`.
- Tenant settings now persist `account.settings.outcomePacks` (per-pack enabled state) and `account.settings.onboarding` (wizard state, selected packs, phone/calendar flags). The Revenue UI uses these to surface the outcome pack selector and onboarding card so owners can go live in <3 minutes without touching flows.

## Security Notes

- Do not commit `backend/.env`.
- Keep `DEV_MODE=false` in production.
- Keep `ALLOW_DEFAULT_SUPERADMIN=false` in production.
- Rotate secrets before public launch.
- Auth strategy is cookie-session only (no bearer-token auth path).
- Cookie-auth mutating requests now require CSRF:
  - send `x-csrf-token` with the token returned at login (`csrfToken`) or from `relay_csrf` cookie.
- Backend emits structured request logs with `requestId` and returns `x-request-id` response header.
- Readiness probe: `GET /ready` validates database connectivity.
