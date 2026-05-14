# Attack Surface Diagram (Text)

## External entry points
1. Browser/Web client
- `GET /`
- `GET /book/:token`
- `GET/POST/... /api/*`

2. Webhook providers
- `POST /webhooks/sms`
- `POST /webhooks/missed-call`
- `POST /webhooks/voice/incoming`
- `POST /webhooks/voice/dial-result`
- `POST /webhooks/event`
- `POST /webhooks/stripe?to=...`

## Internal security controls by boundary
- Edge:
  - CORS allowlist (production)
  - Rate limits (`/api/auth`, `/api`, `/webhooks`)
  - Security headers
- Auth boundary:
  - Session/bearer auth resolver
  - `requireAuth`
  - `requireRole`
  - `requireAccountAccess`
  - CSRF for mutating cookie-auth requests
- Tenant boundary:
  - `requireTenant`
  - `requireTenantForWebhook` (To-number + signature verification)
- Integrity boundary:
  - Webhook idempotency receipts
  - Signature verification and timestamp checks
- Data boundary:
  - PostgreSQL-backed store
  - Tenant foreign keys in normalized tables

## High-risk operations
- Integration credential updates (`/api/integrations/*`)
- Admin platform configuration (`/api/admin/*`)
- Dev simulator controls (`/api/dev/*`)
- Webhook processing endpoints

## Required monitoring focus
- 401/403 spikes by endpoint
- 429 rate-limit spikes
- Webhook duplicate and signature-failure counts
- Cross-tenant access denied events
