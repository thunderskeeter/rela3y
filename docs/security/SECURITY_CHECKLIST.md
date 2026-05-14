# Phase 2 Security Checklist

## Secrets and config
- [x] `.env` ignored by git.
- [x] Docker context excludes `backend/.env` and `backend/data`.
- [x] Runtime uses env vars for auth/webhook/integration credentials.

## Request hardening
- [x] Rate limiting: auth endpoints.
- [x] Rate limiting: webhook endpoints.
- [x] Rate limiting: API traffic baseline.
- [x] Security headers baseline applied.

## Validation
- [x] Auth login payload validation.
- [x] Webhook payload validation (SMS/call/event).
- [x] Integration mutation payload validation.
- [x] Dev mutation payload validation.

## AuthN/AuthZ
- [x] `requireAuth` on protected API.
- [x] `requireAccountAccess` for tenant-scoped routes.
- [x] `requireRole` on admin/dev/integration write controls.
- [x] CSRF protection for mutating cookie-auth requests.

## Webhook controls
- [x] Signature verification (Stripe/Twilio).
- [x] Stripe signature timestamp tolerance.
- [x] Webhook idempotency receipts (Stripe + Twilio event IDs where provided).

## Follow-up items
- [ ] Expand Zod coverage to every remaining write route.
- [ ] Move rate limiter/idempotency storage to shared datastore for multi-instance deployments.
- [ ] Add automated integration tests for CSRF and webhook replay protections.
