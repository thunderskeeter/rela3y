const crypto = require('crypto');
const { getNotificationSettings, appendNotificationLog } = require('../store/dataStore');
const { loadData, getAccountById } = require('../store/dataStore');

const dedupeCache = new Map(); // accountId -> Map(eventId -> ts)

function toHmMinutes(hhmm, fallback) {
  const raw = String(hhmm || '');
  const m = raw.match(/^([01]\d|2[0-3]):([0-5]\d)$/);
  if (!m) return fallback;
  return (Number(m[1]) * 60) + Number(m[2]);
}

function inQuietHours(quietHours) {
  if (!quietHours?.enabled) return false;
  const start = toHmMinutes(quietHours.start, 21 * 60);
  const end = toHmMinutes(quietHours.end, 8 * 60);
  const tz = String(quietHours.timezone || 'America/New_York');
  let hour = 0;
  let minute = 0;
  try {
    const dt = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      hour12: false,
      hour: '2-digit',
      minute: '2-digit'
    }).formatToParts(new Date());
    hour = Number(dt.find((p) => p.type === 'hour')?.value || 0);
    minute = Number(dt.find((p) => p.type === 'minute')?.value || 0);
  } catch {
    const now = new Date();
    hour = now.getHours();
    minute = now.getMinutes();
  }
  const current = (hour * 60) + minute;
  if (start === end) return true;
  if (start < end) return current >= start && current < end;
  return current >= start || current < end; // overnight window
}

function triggerEnabled(settings, eventType) {
  const map = {
    vip_message: 'vipMessage',
    missed_call: 'missedCall',
    new_booking: 'newBooking',
    high_value_lead: 'highValueLead',
    no_response: 'noResponse',
    failed_webhook: 'failedWebhook',
    failed_automation: 'failedAutomation'
  };
  const key = map[String(eventType || '').toLowerCase()];
  if (!key) return true;
  return settings?.triggers?.[key] !== false;
}

function shouldNotify(settings, event) {
  if (!triggerEnabled(settings, event?.type)) {
    return { ok: false, reason: 'trigger_disabled' };
  }
  if (inQuietHours(settings?.quietHours)) {
    return { ok: false, reason: 'quiet_hours' };
  }
  const hasChannel = settings?.channels?.email || settings?.channels?.sms || settings?.channels?.desktop;
  if (!hasChannel) {
    return { ok: false, reason: 'no_channels_enabled' };
  }
  return { ok: true };
}

function eventIdentity(tenant, event, dedupeMinutes) {
  const minuteBucket = Math.floor(Date.now() / (Math.max(1, dedupeMinutes) * 60 * 1000));
  const base = [
    String(event?.type || 'unknown'),
    String(event?.conversationId || ''),
    String(event?.from || ''),
    String(event?.to || tenant?.to || ''),
    String(minuteBucket)
  ].join('|');
  return crypto.createHash('sha1').update(base).digest('hex');
}

function cleanupDedupe(map, ttlMs) {
  const cutoff = Date.now() - ttlMs;
  for (const [k, ts] of map.entries()) {
    if (ts < cutoff) map.delete(k);
  }
}

function deliver(event, settings) {
  const channels = [];
  if (settings?.channels?.email) channels.push('email');
  if (settings?.channels?.sms) channels.push('sms');
  if (settings?.channels?.desktop) channels.push('desktop');
  const preferred = String(event?.meta?.preferredChannel || '').trim().toLowerCase();
  const selected = preferred ? channels.filter((ch) => ch === preferred) : channels;
  return selected.map((channel) => ({
    channel,
    status: 'sent',
    reason: null,
    eventType: String(event?.type || '')
  }));
}

function emitEvent(tenant, event) {
  if (!tenant?.accountId || !tenant?.to) {
    return { ok: false, reason: 'missing_tenant' };
  }
  const settings = getNotificationSettings(tenant.accountId);
  const should = shouldNotify(settings, event);
  const eventType = String(event?.type || 'unknown');
  const dedupeMinutes = Number(settings?.dedupeMinutes || 10);
  const eventId = eventIdentity(tenant, event, dedupeMinutes);
  const ttlMs = Math.max(1, dedupeMinutes) * 60 * 1000;

  const cacheForTenant = dedupeCache.get(tenant.accountId) || new Map();
  dedupeCache.set(tenant.accountId, cacheForTenant);
  cleanupDedupe(cacheForTenant, ttlMs);

  if (cacheForTenant.has(eventId)) {
    appendNotificationLog(tenant.accountId, {
      ts: Date.now(),
      eventType,
      channel: 'system',
      status: 'deduped',
      reason: 'duplicate_event',
      eventId
    });
    return { ok: true, deduped: true, eventId };
  }
  cacheForTenant.set(eventId, Date.now());

  if (!should.ok) {
    appendNotificationLog(tenant.accountId, {
      ts: Date.now(),
      eventType,
      channel: 'system',
      status: 'blocked',
      reason: should.reason,
      eventId
    });
    return { ok: true, blocked: true, reason: should.reason, eventId };
  }

  const results = deliver(event, settings);
  if (!results.length) {
    appendNotificationLog(tenant.accountId, {
      ts: Date.now(),
      eventType,
      channel: 'system',
      status: 'blocked',
      reason: 'preferred_channel_unavailable',
      eventId
    });
    return { ok: true, blocked: true, reason: 'preferred_channel_unavailable', eventId };
  }
  for (const r of results) {
    appendNotificationLog(tenant.accountId, {
      ts: Date.now(),
      eventType,
      channel: r.channel,
      status: r.status,
      reason: r.reason,
      eventId
    });
  }

  return { ok: true, sent: results.length, eventId, results };
}

function emitAccountEvent(accountId, event) {
  const aid = String(accountId || '').trim();
  if (!aid) return { ok: false, reason: 'missing_account' };
  const data = loadData();
  const accountRef = getAccountById(data, aid);
  const to = String(accountRef?.to || accountRef?.account?.to || '').trim();
  if (!to) return { ok: false, reason: 'missing_tenant_to' };
  return emitEvent({ accountId: aid, to }, event);
}

module.exports = {
  emitEvent,
  emitAccountEvent,
  shouldNotify,
  deliver
};
