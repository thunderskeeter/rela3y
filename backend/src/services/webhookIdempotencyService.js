const { pool } = require('../db/pool');
const { claim } = require('../repositories/webhookReceiptsRepo');

async function claimWebhookEventDb(accountId, provider, eventId) {
  return claim(pool, accountId, provider, eventId);
}

async function claimWebhookEvent(accountId, provider, eventId) {
  const aid = String(accountId || '').trim();
  const p = String(provider || '').trim().toLowerCase();
  const eid = String(eventId || '').trim();
  if (!aid || !p || !eid) return { ok: false, reason: 'missing_key' };
  return claimWebhookEventDb(aid, p, eid);
}

module.exports = {
  claimWebhookEvent
};
