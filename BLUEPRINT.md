# One Page Blueprint

Governance enforcement: follow [AGENTS.md](./AGENTS.md) role constraints and block merges on [PRODUCTION_READINESS_SCORE.md](./PRODUCTION_READINESS_SCORE.md) hard gates.

## System Mindmap
```mermaid
mindmap
  root((Relay Revenue Recovery OS))
    Ingress
      Twilio Webhooks
        /webhooks/missed-call
        /webhooks/sms
        /webhooks/event
      Dev Simulator
        /api/dev/revenue/simulate
        /api/dev/revenue/run-optimization
    Core Runtime
      Express App
        app.js route + middleware wiring
      Scheduler
        60s tick
        PRM every 5 ticks (~5 min)
        hourly scans every 60 ticks
    Intelligence + Decisions
      Canonical Signals
        signalService normalizeSignalType
      Opportunity + Risk
        revenueIntelligenceService
        opportunityLifecycle stage transitions
      ActionPlan
        decisionEngine wraps aiDecisionEngine
    Execution
      actionExecutor
        outbound SMS
        followup scheduling
      complianceService
        quiet hours + caps + opt-out guardrails
      webhookIdempotencyService
        receipts (Postgres + memory fallback)
    Persistence
      dataStore cache/orchestration
      stateRepository snapshot in Postgres
      db migrations
    UI
      static frontend
        frontend/index.html
        frontend/js/app.js
```

## Missed Call -> SMS -> AI -> Booking
```mermaid
sequenceDiagram
  participant Twilio as Twilio/Webhook Source
  participant WH as webhooks.routes.js
  participant RI as revenueIntelligenceService
  participant OR as revenueOrchestrator
  participant DE as decisionEngine/aiDecisionEngine
  participant EX as actionExecutor
  participant ST as dataStore/stateRepository

  Twilio->>WH: POST /webhooks/missed-call (CallSid)
  WH->>WH: claimWebhookEvent(twilio_call, CallSid)
  WH->>RI: createLeadEvent(missed_call/after_hours_inquiry)
  WH->>OR: handleSignal(accountId, leadEvent)
  OR->>DE: createActionPlan(...)
  DE-->>OR: ActionPlan(nextAction, messageText, followups, policy)
  OR->>EX: executeRevenueAction(...)
  EX->>ST: write actions/opportunity updates + schedule followup
  EX-->>Twilio: outbound SMS (via compliance/send path)

  Twilio->>WH: POST /webhooks/sms (MessageSid + reply)
  WH->>WH: claimWebhookEvent(twilio_sms, MessageSid)
  WH->>RI: createLeadEvent(inbound_message)
  WH->>OR: handleSignal(...) re-evaluate stage/risk
  OR->>ST: persist updated opportunity/lifecycle

  Twilio->>WH: POST /webhooks/event type=booking_created
  WH->>RI: createLeadEvent(booking_created)
  WH->>OR: handleSignal(...) => BOOKED/WON path
  OR->>ST: persist stageHistory + action audit
```

## Status Snapshot
- Done
  - Tenant-scoped Express API + static frontend wiring (`backend/src/app.js`, `frontend/index.html`).
  - Snapshot-backed Postgres persistence via in-memory store + repository (`backend/src/store/dataStore.js`, `backend/src/db/stateRepository.js`).
  - Deterministic signal -> plan -> action pipeline with lifecycle + audit (`backend/src/services/signalService.js`, `backend/src/services/decisionEngine.js`, `backend/src/services/actionExecutor.js`, `backend/src/services/opportunityLifecycle.js`).
  - Scheduler live at 60s tick; PRM loop runs every 5 ticks (`backend/src/services/scheduler.js`, `backend/src/services/passiveRevenueMonitoring.js`).
- Next
  - Continue migration from snapshot model toward fully normalized relational writes for the target entities listed in `AGENTS.md`.
  - Move in-process scheduler/work to external queue workers for horizontal scaling.
  - Expand integration test coverage beyond current security suite (`backend/test/security.integration.test.js`).
- Broken/Painful
  - In-process `setInterval` scheduler and in-memory hot state are restart-sensitive and not queue-backed.
  - Snapshot persistence centralizes writes in one large document, making partial transactional updates harder.
  - Frontend is intentionally static pre-PMF, so rich state/error handling is limited versus full SPA standards.
