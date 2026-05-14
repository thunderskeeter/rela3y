const { loadData, saveDataDebounced, ensureAccountForTo, inferAccountIdForTo, getDevSettings } = require('../store/dataStore');
const crypto = require('crypto');
const { USE_DB_CONVERSATIONS, USE_DB_MESSAGES } = require('../config/runtime');
const {
  twilioReadyForSend,
  sendTwilioMessageForTenant
} = require('./twilioIntegrationService');

const E164_REGEX = /^\+[1-9]\d{1,14}$/;

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function deepMerge(base, patch) {
  if (!isPlainObject(base) || !isPlainObject(patch)) return patch;
  const merged = { ...base };
  for (const [k, v] of Object.entries(patch)) {
    if (isPlainObject(v) && isPlainObject(base[k])) merged[k] = deepMerge(base[k], v);
    else merged[k] = v;
  }
  return merged;
}

function isPlainObject(v) {
  return v && typeof v === 'object' && !Array.isArray(v);
}

function getDefaultComplianceConfig() {
  return {
    stopKeywords: ['STOP', 'UNSUBSCRIBE', 'CANCEL', 'END', 'QUIT'],
    helpKeywords: ['HELP', 'INFO'],
    stopBehavior: {
      enabled: true,
      autoReply: true,
      autoReplyText: "You're opted out. Reply START to resubscribe.",
      notifyOwnerOnOptOut: true,
      resubscribeAutoReply: true,
      resubscribeAutoReplyText: "You're resubscribed."
    },
    optOut: {
      enforce: true,
      allowTransactional: false,
      resubscribeKeywords: ['START', 'UNSTOP', 'YES'],
      storeAsTag: 'DNR'
    },
    consent: {
      requireForOutbound: false,
      consentTag: 'CONSENT',
      consentCheckboxText: 'I confirm I have consent to text this contact.',
      consentSourceOptions: ['verbal', 'form', 'existing_customer', 'other']
    },
    retention: {
      enabled: true,
      messageLogDays: 90,
      purgeOnSchedule: true,
      lastPurgeAt: null
    },
    audit: {
      enabled: true,
      logOptOutEvents: true,
      logBlockedSends: true
    }
  };
}

function normalizeTenant(data, tenant) {
  if (!tenant) return null;
  if (typeof tenant === 'string') {
    const to = String(tenant).trim();
    if (!to) return null;
    const account = ensureAccountForTo(data, to, { autoCreate: true });
    return { to, accountId: String(account.id || account.accountId) };
  }
  const to = String(tenant.to || '').trim();
  const accountId = String(tenant.accountId || '').trim();
  if (to && !accountId) {
    const inferred = inferAccountIdForTo(data, to, { autoCreate: true });
    return inferred ? { to, accountId: inferred } : null;
  }
  if (!to || !accountId) return null;
  return { to, accountId };
}

function ensureTenantCompliance(data, tenant) {
  const t = normalizeTenant(data, tenant);
  if (!t) throw new Error('Missing tenant');
  const account = ensureAccountForTo(data, t.to, { autoCreate: true });
  account.id = account.id || t.accountId;
  account.accountId = account.accountId || account.id;
  account.compliance = deepMerge(getDefaultComplianceConfig(), account.compliance || {});
  return account.compliance;
}

function normalizePhone(raw) {
  if (!raw) return null;
  let digits = String(raw).replace(/[^\d+]/g, '');
  if (!digits) return null;
  if (!digits.startsWith('+')) {
    if (digits.length === 10) digits = '+1' + digits;
    else if (digits.length === 11 && digits.startsWith('1')) digits = '+' + digits;
    else digits = '+' + digits;
  }
  return digits;
}

function ensureContactFields(contact) {
  contact.flags = contact.flags || {};
  contact.summary = contact.summary || {};
  contact.lifecycle = contact.lifecycle || {};
  contact.tags = Array.isArray(contact.tags) ? contact.tags : [];
  if (typeof contact.optedOut !== 'boolean') contact.optedOut = false;
  if (contact.optedOutAt === undefined) contact.optedOutAt = null;
  if (contact.consent === undefined) contact.consent = false;
  if (contact.consentSource === undefined) contact.consentSource = null;
  if (contact.dnrSource === undefined) contact.dnrSource = null;
  if (contact.flags.doNotAutoReply === undefined) contact.flags.doNotAutoReply = false;
  return contact;
}

function ensureContactRecord(data, tenant, from) {
  const t = normalizeTenant(data, tenant);
  if (!t) throw new Error('Missing tenant');
  const to = t.to;
  data.contacts = data.contacts || {};
  const normalized = normalizePhone(from) || String(from);
  const targetNorm = normalizePhone(normalized) || normalized;

  let matchKey = null;
  for (const [key, c] of Object.entries(data.contacts)) {
    if (!key.startsWith(`${to}__`)) continue;
    if (c?.accountId && String(c.accountId) !== String(t.accountId)) continue;
    const cNorm = normalizePhone(c?.phone || '');
    if (cNorm && cNorm === targetNorm) {
      matchKey = key;
      break;
    }
  }

  if (!matchKey) {
    matchKey = `${to}__${targetNorm}`;
    data.contacts[matchKey] = {
      accountId: t.accountId,
      phone: targetNorm,
      name: '',
      flags: { vip: false, doNotAutoReply: false },
      summary: { notes: '' },
      lifecycle: { leadStatus: 'new' },
      tags: [],
      optedOut: false,
      optedOutAt: null,
      consent: false,
      consentSource: null,
      dnrSource: null,
      createdAt: Date.now(),
      updatedAt: Date.now()
    };
  }

  const contact = ensureContactFields(data.contacts[matchKey]);
  contact.accountId = contact.accountId || t.accountId;
  contact.phone = normalizePhone(contact.phone) || targetNorm;
  contact.updatedAt = Date.now();
  return contact;
}

function normalizeBody(text) {
  return String(text || '').trim().replace(/\s+/g, ' ').toUpperCase();
}

function isStopKeyword(normalizedBody, keywords) {
  const list = Array.isArray(keywords) ? keywords : [];
  return list.some((kw) => {
    const k = String(kw || '').trim().toUpperCase();
    return k && (normalizedBody === k || normalizedBody.startsWith(`${k} `));
  });
}

function isExactKeyword(normalizedBody, keywords) {
  const list = Array.isArray(keywords) ? keywords : [];
  return list.some((kw) => normalizedBody === String(kw || '').trim().toUpperCase());
}

function isStop(normalizedBody, keywords) {
  return isStopKeyword(normalizedBody, keywords);
}

function isStart(normalizedBody, keywords) {
  return isExactKeyword(normalizedBody, keywords);
}

function newMessage(direction, to, from, body, extra = {}) {
  const now = Date.now();
  const outbound = direction === 'outbound';
  return {
    id: (typeof crypto !== 'undefined' && crypto.randomUUID) ? crypto.randomUUID() : `${now}_${Math.random().toString(16).slice(2)}`,
    direction,
    to: String(to),
    from: String(from),
    body: String(body || ''),
    ts: now,
    status: extra.status || (outbound ? 'queued' : 'received'),
    providerMeta: extra.providerMeta || null,
    dir: outbound ? 'out' : 'in',
    text: String(body || ''),
    ...extra
  };
}

function addAudit(conversation, type, meta) {
  conversation.audit = conversation.audit || [];
  conversation.audit.push({ ts: Date.now(), type, meta: meta || {} });
}

function enforceOutboundCompliance({ data, tenant, conversation, from, consentConfirmed, consentSource, transactional }) {
  const compliance = ensureTenantCompliance(data, tenant);
  const contact = ensureContactRecord(data, tenant, from);
  ensureContactFields(contact);

  if (compliance.optOut?.enforce && contact.optedOut) {
    const allowTxn = compliance.optOut?.allowTransactional === true && transactional === true;
    if (!allowTxn) {
      if (compliance.audit?.enabled && compliance.audit?.logBlockedSends) {
        addAudit(conversation, 'compliance_blocked_send', {
          code: 'OPTED_OUT',
          source: 'outbound_enforcement'
        });
      }
      return {
        ok: false,
        error: { code: 'OPTED_OUT', message: 'Contact has opted out (STOP). Sending is blocked.' }
      };
    }
  }

  if (compliance.consent?.requireForOutbound) {
    if (contact.consent !== true) {
      if (consentConfirmed === true) {
        contact.consent = true;
        contact.consentSource = consentSource || 'other';
      } else {
        if (compliance.audit?.enabled && compliance.audit?.logBlockedSends) {
          addAudit(conversation, 'compliance_blocked_send', {
            code: 'NO_CONSENT',
            source: 'outbound_enforcement'
          });
        }
        return {
          ok: false,
          error: { code: 'NO_CONSENT', message: 'No consent on file. Mark consent to send.' }
        };
      }
    }
  }

  return { ok: true, compliance, contact };
}

function dbMessagingEnabled() {
  return USE_DB_CONVERSATIONS === true || USE_DB_MESSAGES === true;
}

function logLegacyMessagingWriteBlocked(fields = {}) {
  console.error(JSON.stringify({
    level: 'error',
    entity: 'messaging_core',
    service: 'complianceService',
    operation: 'attemptOutboundMessage',
    accountId: fields.accountId ? String(fields.accountId) : null,
    conversationId: fields.conversationId ? String(fields.conversationId) : null,
    errorType: 'legacy_messaging_writer_blocked',
    message: 'Legacy snapshot-backed outbound persistence is blocked in DB messaging mode.'
  }));
}

function attemptOutboundMessage({
  data,
  tenant,
  conversation,
  from,
  text,
  source,
  transactional = false,
  bypassCompliance = false,
  consentConfirmed = false,
  consentSource = null,
  meta = {}
}) {
  if (dbMessagingEnabled()) {
    logLegacyMessagingWriteBlocked({
      accountId: tenant?.accountId || conversation?.accountId || '',
      conversationId: conversation?.id || conversation?.convoKey || `${conversation?.to || tenant?.to || ''}__${from || conversation?.from || ''}`
    });
    const err = new Error('legacy_messaging_writer_blocked');
    err.code = 'legacy_messaging_writer_blocked';
    throw err;
  }
  const resolvedTenant = normalizeTenant(data, tenant);
  if (!resolvedTenant) {
    return { ok: false, error: { code: 'TENANT_REQUIRED', message: 'Tenant context is required for outbound sends.' } };
  }
  if (!conversation || typeof conversation !== 'object') {
    return { ok: false, error: { code: 'CONVERSATION_REQUIRED', message: 'Conversation is required for outbound sends.' } };
  }
  if (conversation.accountId && String(conversation.accountId) !== String(resolvedTenant.accountId)) {
    if (getDevSettings().verboseTenantLogs === true) {
      console.warn(`[tenant] cross-tenant access blocked: convoAccountId=${conversation.accountId} tenantAccountId=${resolvedTenant.accountId}`);
    }
    return { ok: false, error: { code: 'CROSS_TENANT_CONVERSATION', message: 'Conversation does not belong to the active tenant.' } };
  }
  if (conversation.to && String(conversation.to) !== String(resolvedTenant.to)) {
    if (getDevSettings().verboseTenantLogs === true) {
      console.warn(`[tenant] cross-tenant access blocked: convoTo=${conversation.to} tenantTo=${resolvedTenant.to}`);
    }
    return { ok: false, error: { code: 'CROSS_TENANT_CONVERSATION', message: 'Conversation phone does not belong to the active tenant.' } };
  }
  conversation.accountId = resolvedTenant.accountId;
  conversation.to = String(conversation.to || resolvedTenant.to);
  conversation.from = String(conversation.from || from || '');

  let enforcement = { ok: true };
  if (!bypassCompliance) {
    enforcement = enforceOutboundCompliance({
      data,
      tenant: resolvedTenant,
      conversation,
      from: String(from || conversation.from || ''),
      consentConfirmed,
      consentSource,
      transactional
    });
    if (!enforcement.ok) return enforcement;
  }

  const dev = getDevSettings();
  const simulateOutbound = dev?.enabled === true && dev?.simulateOutbound === true;
  if (!simulateOutbound) {
    const ready = twilioReadyForSend(resolvedTenant);
    if (!ready.ok) {
      return { ok: false, error: { code: 'TWILIO_NOT_CONFIGURED', message: ready.reason } };
    }
  }

  conversation.messages = conversation.messages || [];
  const msg = newMessage('outbound', resolvedTenant.to, String(from || conversation.from || ''), text, {
    ...meta,
    status: simulateOutbound ? 'simulated' : (meta?.status || 'queued'),
    providerMeta: simulateOutbound ? { provider: 'simulator' } : null,
    accountId: resolvedTenant.accountId
  });
  conversation.messages.push(msg);
  conversation.lastActivityAt = Date.now();

  if (!simulateOutbound) {
    const destination = String(from || conversation.from || '').trim();
    const body = String(text || '');
    sendTwilioMessageForTenant(resolvedTenant, { to: destination, body })
      .then((sent) => {
        msg.status = 'sent';
        msg.providerMeta = {
          provider: 'twilio',
          sid: String(sent?.sid || ''),
          deliveryStatus: String(sent?.status || ''),
          to: String(sent?.to || ''),
          from: String(sent?.from || ''),
          messagingServiceSid: String(sent?.messagingServiceSid || '')
        };
        saveDataDebounced(data);
      })
      .catch((err) => {
        msg.status = 'failed';
        msg.providerMeta = {
          provider: 'twilio',
          error: String(err?.message || 'Twilio send failed')
        };
        addAudit(conversation, 'outbound_delivery_failed', {
          source: source || 'unknown',
          reason: String(err?.message || 'Twilio send failed')
        });
        saveDataDebounced(data);
      });
  }

  try {
    const {
      createLeadEvent,
      upsertRevenueOpportunityFromEvent,
      evaluateOpportunity
    } = require('./revenueIntelligenceService');
    const convoKey = `${String(conversation.to || resolvedTenant.to)}__${String(from || conversation.from || '')}`;
    const evt = createLeadEvent(resolvedTenant.accountId, {
      convoKey,
      contactId: null,
      channel: 'sms',
      type: 'outbound_message',
      payload: {
        source: String(source || 'outbound'),
        text: String(text || ''),
        transactional: transactional === true,
        auto: meta?.auto === true
      }
    });
    const opp = upsertRevenueOpportunityFromEvent(resolvedTenant.accountId, evt);
    if (opp?.id) evaluateOpportunity(resolvedTenant.accountId, opp.id);
  } catch {}

  return { ok: true, message: msg };
}

function applyOptOutTag(contact, tag) {
  if (!tag) return;
  contact.tags = Array.isArray(contact.tags) ? contact.tags : [];
  if (!contact.tags.includes(tag)) contact.tags.push(tag);
  contact.flags = contact.flags || {};
  contact.flags.doNotAutoReply = true;
  contact.dnrSource = 'optout';
}

function clearOptOutTagIfOwned(contact, tag) {
  if (contact.dnrSource !== 'optout') return;
  contact.flags = contact.flags || {};
  contact.flags.doNotAutoReply = false;
  if (tag && Array.isArray(contact.tags)) {
    contact.tags = contact.tags.filter((t) => t !== tag);
  }
  contact.dnrSource = null;
}

function processInboundCompliance({ data, tenant, conversation, from, text, sendMessageFn = null }) {
  const resolvedTenant = normalizeTenant(data, tenant);
  if (!resolvedTenant) return { handled: false };
  const compliance = ensureTenantCompliance(data, resolvedTenant);
  const contact = ensureContactRecord(data, resolvedTenant, from);
  const to = resolvedTenant.to;
  const norm = normalizeBody(text);
  const now = Date.now();

  if (compliance.stopBehavior?.enabled && isStopKeyword(norm, compliance.stopKeywords)) {
    const alreadyOptedOut = contact.optedOut === true;
    if (!alreadyOptedOut) {
      contact.optedOut = true;
      contact.optedOutAt = now;
      applyOptOutTag(contact, compliance.optOut?.storeAsTag || 'DNR');
      if (conversation.flow?.status === 'active') {
        conversation.flow.status = 'paused_opted_out';
      }
      if (compliance.audit?.enabled && compliance.audit?.logOptOutEvents) {
        addAudit(conversation, 'compliance_opt_out', { from, to, keyword: norm.split(' ')[0] });
      }
    }

    let autoReplySent = false;
    if (!alreadyOptedOut && compliance.stopBehavior?.autoReply && compliance.stopBehavior?.autoReplyText) {
      const payload = {
        data,
        tenant: resolvedTenant,
        conversation,
        from,
        text: compliance.stopBehavior.autoReplyText,
        source: 'compliance_stop_auto_reply',
        bypassCompliance: true,
        meta: { auto: true, system: true, status: 'sent' }
      };
      if (typeof sendMessageFn === 'function') {
        autoReplySent = true;
        void sendMessageFn(payload).catch(() => {});
      } else {
        const sent = attemptOutboundMessage(payload);
        autoReplySent = sent.ok;
      }
    }

    return { handled: true, type: 'opt_out', alreadyOptedOut, autoReplySent };
  }

  if (isExactKeyword(norm, compliance.optOut?.resubscribeKeywords || [])) {
    if (contact.optedOut) {
      contact.optedOut = false;
      contact.optedOutAt = null;
      clearOptOutTagIfOwned(contact, compliance.optOut?.storeAsTag || 'DNR');
      if (conversation.flow?.status === 'paused_opted_out') {
        conversation.flow.status = 'idle';
      }
      if (compliance.audit?.enabled && compliance.audit?.logOptOutEvents) {
        addAudit(conversation, 'compliance_resubscribe', { from, to, keyword: norm });
      }
      if (compliance.stopBehavior?.resubscribeAutoReply) {
        const payload = {
          data,
          tenant: resolvedTenant,
          conversation,
          from,
          text: compliance.stopBehavior?.resubscribeAutoReplyText || "You're resubscribed.",
          source: 'compliance_resubscribe_auto_reply',
          bypassCompliance: true,
          meta: { auto: true, system: true, status: 'sent' }
        };
        if (typeof sendMessageFn === 'function') {
          void sendMessageFn(payload).catch(() => {});
        } else {
          attemptOutboundMessage(payload);
        }
      }
      return { handled: true, type: 'resubscribe' };
    }
    return { handled: true, type: 'resubscribe_noop' };
  }

  return { handled: false };
}

function validateCompliancePatch(nextCompliance) {
  const errors = [];

  const days = Number(nextCompliance?.retention?.messageLogDays);
  if (!Number.isFinite(days) || days < 7 || days > 365) {
    errors.push('retention.messageLogDays must be between 7 and 365');
  }

  const allNumbers = []
    .concat(nextCompliance?.stopKeywords || [])
    .concat(nextCompliance?.helpKeywords || [])
    .concat(nextCompliance?.optOut?.resubscribeKeywords || []);
  if (allNumbers.some((x) => typeof x !== 'string' || !x.trim())) {
    errors.push('keyword lists must contain non-empty strings');
  }

  if (typeof nextCompliance?.stopBehavior?.autoReplyText !== 'string' || !nextCompliance.stopBehavior.autoReplyText.trim()) {
    errors.push('stopBehavior.autoReplyText is required');
  }

  if (!Array.isArray(nextCompliance?.consent?.consentSourceOptions) || nextCompliance.consent.consentSourceOptions.length === 0) {
    errors.push('consent.consentSourceOptions must be a non-empty array');
  }

  return errors;
}

function purgeMessagesForTenant(data, to, days) {
  const cutoff = Date.now() - (days * 24 * 60 * 60 * 1000);
  let removed = 0;
  for (const convo of Object.values(data.conversations || {})) {
    if (String(convo.to) !== String(to) || !Array.isArray(convo.messages)) continue;
    const before = convo.messages.length;
    convo.messages = convo.messages.filter((m) => {
      const ts = Number(m?.ts || 0);
      if (!ts) return true;
      return ts >= cutoff;
    });
    removed += (before - convo.messages.length);
  }
  return removed;
}

function runComplianceRetentionPurge({ to = null, force = false } = {}) {
  const data = loadData();
  data.accounts = data.accounts || {};
  let totalRemoved = 0;
  const perTenant = [];

  const targets = to
    ? [String(to)]
    : Object.keys(data.accounts);

  for (const tenantTo of targets) {
    const compliance = ensureTenantCompliance(data, tenantTo);
    const retention = compliance.retention || {};
    if (!retention.enabled) continue;
    if (!force && retention.purgeOnSchedule !== true) continue;
    const days = Number(retention.messageLogDays || 90);
    const removed = purgeMessagesForTenant(data, tenantTo, days);
    totalRemoved += removed;
    retention.lastPurgeAt = Date.now();
    perTenant.push({ to: tenantTo, removed, days });
  }

  if (perTenant.length > 0) saveDataDebounced(data);
  return { ok: true, totalRemoved, tenants: perTenant };
}

let retentionTimer = null;
function initComplianceRetentionJob() {
  runComplianceRetentionPurge();
  if (retentionTimer) clearInterval(retentionTimer);
  retentionTimer = setInterval(() => {
    runComplianceRetentionPurge();
  }, 24 * 60 * 60 * 1000);
}

module.exports = {
  E164_REGEX,
  deepMerge,
  getDefaultComplianceConfig,
  ensureTenantCompliance,
  ensureContactFields,
  ensureContactRecord,
  normalizeBody,
  isStop,
  isStart,
  isStopKeyword,
  isExactKeyword,
  newMessage,
  enforceOutboundCompliance,
  attemptOutboundMessage,
  processInboundCompliance,
  validateCompliancePatch,
  runComplianceRetentionPurge,
  initComplianceRetentionJob
};
