# Relay Threat Model (Phase 2)

## Scope
- Backend API (`/api/*`)
- Webhooks (`/webhooks/*`, `/webhooks/stripe`)
- Tenant-scoped persistence and auth/session flows
- Integration credentials (Twilio/Stripe/Calendar)

## Primary assets
- Tenant conversation/contact/opportunity data
- Auth/session tokens
- Integration secrets (API keys, webhook tokens)
- Billing and webhook event integrity

## Trust boundaries
- Browser client -> API
- Third-party webhook providers -> webhook endpoints
- Backend -> PostgreSQL
- Admin/superadmin controls -> tenant-level configuration

## Key threats and mitigations
1. Tenant data leakage
- Threat: cross-tenant ID or selector abuse.
- Mitigation: `requireTenant` + `requireAccountAccess` enforced server-side before tenant routes.

2. Auth bypass / privilege escalation
- Threat: low-privilege user invokes privileged integration/admin mutations.
- Mitigation: `requireRole` gating on privileged routes (`admin`, `dev`, `integrations` write paths).

3. CSRF on cookie-authenticated requests
- Threat: browser submits mutating request with ambient session cookie.
- Mitigation: double-submit CSRF token (`relay_csrf` cookie + `x-csrf-token`) on mutating `/api` requests when not using bearer auth.

4. Injection / malformed payload abuse
- Threat: malformed request body leads to unsafe state mutations.
- Mitigation: Zod validation on auth, webhook, dev, and integration mutation payloads.

5. Replay attacks on webhooks
- Threat: duplicate Stripe/Twilio webhook delivery mutates state multiple times.
- Mitigation: signature verification plus idempotency receipts (`webhookReceipts`) keyed by provider/event-id.

6. Credential stuffing / brute force / request flooding
- Threat: repeated login attempts or endpoint flooding.
- Mitigation: endpoint rate limiting on `/api/auth`, `/api` traffic, and webhooks.

## Residual risks
- In-memory rate limits are per-process; distributed environments require shared limiter storage.
- CSRF token strategy assumes browser sends explicit `x-csrf-token` for cookie auth clients.
- Additional validation coverage is still needed for all remaining write endpoints.
