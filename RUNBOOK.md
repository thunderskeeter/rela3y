# Runbook

Governance enforcement: operate under [AGENTS.md](./AGENTS.md) and verify release readiness using [PRODUCTION_READINESS_SCORE.md](./PRODUCTION_READINESS_SCORE.md) before shipping.

## Prereqs
- Node.js 20+
- npm 10+
- PostgreSQL 14+ reachable from `DATABASE_URL`
- Docker (optional, for local Postgres)

## First-Time Setup
1. Copy env template: `Copy-Item backend/.env.example backend/.env`
2. Set required values in `backend/.env` (minimum: `AUTH_SECRET`, `DATABASE_URL`).
3. Install deps: `npm --prefix backend install`
4. Run migrations: `npm --prefix backend run migrate`

## Start PostgreSQL
- Docker option:
  - `docker run --name relay-postgres -e POSTGRES_USER=relay -e POSTGRES_PASSWORD=relay -e POSTGRES_DB=relay_dashboard -p 5432:5432 -d postgres:16`
  - Example `DATABASE_URL=postgres://relay:relay@127.0.0.1:5432/relay_dashboard`
- Local service option:
  - Start your local PostgreSQL service (Windows Services/Homebrew/systemctl).
  - Create DB/user and set `DATABASE_URL` in `backend/.env`.

## Migrations
- Run: `npm --prefix backend run migrate`
- Files: `backend/db/migrations/*.sql`
- Runner: `backend/src/db/migrate.js`

## Run Backend
- Dev (nodemon): `npm --prefix backend run dev`
- Production start command: `npm --prefix backend run start`
- Base URL: `http://127.0.0.1:3001`

## Test Without Twilio (Dev Simulation)
Authenticate as superadmin, then call:
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
- Agent scenarios:
  - `POST /api/dev/revenue/simulate` with `{"scenario":"agent_run_missed_call_home_services","from":"+18145550199"}`
  - `POST /api/dev/revenue/simulate` with `{"scenario":"agent_run_after_hours_medspa","from":"+18145550199"}`
  - `POST /api/dev/revenue/simulate` with `{"scenario":"agent_review_required_escalation","from":"+18145550199"}`
  - `POST /api/dev/revenue/simulate` with `{"scenario":"agent_lock_contention","from":"+18145550199"}`
  - `POST /api/dev/revenue/simulate` with `{"scenario":"agent_idempotency_double_fire","from":"+18145550199"}`
  - `POST /api/dev/revenue/simulate` with `{"scenario":"agent_replay_dry_run","from":"+18145550199"}`
  - `POST /api/dev/agent/replay-run` with `{"runId":"<existingRunId>","dryRun":true}`

## Where to Debug
- App wiring/middleware: `backend/src/app.js`
- Scheduler + PRM cadence: `backend/src/services/scheduler.js`, `backend/src/services/passiveRevenueMonitoring.js`
- Webhooks + dedupe: `backend/src/routes/webhooks.routes.js`, `backend/src/services/webhookIdempotencyService.js`
- Signal/decision/action path: `backend/src/services/signalService.js`, `backend/src/services/decisionEngine.js`, `backend/src/services/actionExecutor.js`, `backend/src/services/revenueOrchestrator.js`
- Lifecycle + audit: `backend/src/services/opportunityLifecycle.js`, `backend/src/services/actionLogger.js`
- Persistence: `backend/src/store/dataStore.js`, `backend/src/db/stateRepository.js`, `backend/src/db/pool.js`
- Security integration tests: `backend/test/security.integration.test.js`

## Repo Hygiene
- `node_modules` must never be committed (`.gitignore` already includes `node_modules/` and `backend/node_modules/`).
- Never commit `backend/.env`; keep secrets only in env/secret manager.
- Keep `.env.example` updated when env contracts change.
