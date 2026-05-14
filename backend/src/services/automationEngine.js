const { loadData, getConversationById } = require('../store/dataStore');
const { emitEvent } = require('./notificationService');
const { appendOutboundMessage } = require('./messagesService');
const { DEV_MODE } = require('../config/runtime');

function debugLog(...args) {
  if (DEV_MODE === true) console.log(...args);
}

// ─── Automation Rules Engine ───────────────────────────────────────────
// Evaluates the 15 automation templates against triggers.
// Coexists with flowEngine.js — suppresses rules when a multi-step flow is active.

/**
 * Main entry: evaluate all enabled rules for a trigger type.
 * Called from webhooks (missed_call, inbound_sms) and the event endpoint.
 *
 * @param {string} triggerType - 'missed_call' | 'inbound_sms' | 'no_response' |
 *        'booking_created' | 'booking_reminder' | 'service_completed' | 'quote_sent' |
 *        'lead_lost' | 'inactive_customer' | 'seasonal'
 * @param {object} ctx - { to, from, eventData? }
 * @returns {Array} actions taken
 */
async function evaluateTrigger(triggerType, { tenant, to, from, eventData }) {
  if (!tenant || !tenant.accountId || !tenant.to) return [];
  const tenantTo = String(to || tenant.to);
  if (tenantTo !== String(tenant.to)) return [];
  const data = loadData();
  const actions = [];

  // Load rules for this business number
  const rules = getRulesForNumber(data, tenantTo, tenant.accountId);
  const matching = rules.filter((r) => r.trigger === triggerType && r.enabled);
  if (matching.length === 0) return actions;

  const key = `${tenantTo}__${from}`;
  const conversation = getConversationById(tenant.accountId, key)?.conversation || null;
  if (conversation && String(conversation.accountId || '') !== String(tenant.accountId)) return actions;

  if (triggerType === 'booking_created') {
    emitEvent(tenant, { type: 'new_booking', to: tenantTo, from: String(from || ''), conversationId: key });
  }
  if (triggerType === 'no_response') {
    emitEvent(tenant, {
      type: 'no_response',
      to: tenantTo,
      from: String(from || ''),
      conversationId: key,
      meta: { source: 'automation_trigger' }
    });
  }

  for (const rule of matching) {
    if (!passesConditions(rule, { tenant, to: tenantTo, from, conversation, data, triggerType })) {
      continue;
    }

    const delayMinutes = Number(rule.delayMinutes || 0);

    if (delayMinutes === 0) {
      const text = replaceTemplateVars(rule.template, { to: tenantTo, from, conversation, data, eventData });
      const sent = await sendAutomationMessage(tenant, tenantTo, from, rule, text);
      if (sent.ok) {
        actions.push({ ruleId: rule.id, templateId: rule.templateId, action: 'sent', text });
      } else {
        actions.push({ ruleId: rule.id, templateId: rule.templateId, action: 'blocked', error: sent.error });
        emitEvent(tenant, {
          type: 'failed_automation',
          to: tenantTo,
          from: String(from || ''),
          conversationId: key,
          meta: {
            ruleId: rule.id,
            templateId: rule.templateId,
            trigger: rule.trigger,
            errorCode: String(sent?.error?.code || sent?.error || 'send_failed')
          }
        });
      }
      continue;
    }

    if (delayMinutes > 0) {
      const { scheduleJob } = require('./scheduler');
      const job = scheduleJob(rule, { tenant, to: tenantTo, from });
      if (job) {
        actions.push({ ruleId: rule.id, templateId: rule.templateId, action: 'scheduled', scheduledFor: job.scheduledFor });
      } else {
        emitEvent(tenant, {
          type: 'failed_automation',
          to: tenantTo,
          from: String(from || ''),
          conversationId: key,
          meta: {
            ruleId: rule.id,
            templateId: rule.templateId,
            trigger: rule.trigger,
            errorCode: 'schedule_failed'
          }
        });
      }
      continue;
    }

    if (eventData?.bookingTime) {
      const { scheduleJob } = require('./scheduler');
      const scheduledFor = Number(eventData.bookingTime) + (delayMinutes * 60 * 1000);
      if (scheduledFor > Date.now()) {
        const job = scheduleJob(rule, { tenant, to: tenantTo, from, overrideScheduledFor: scheduledFor });
        if (job) {
          actions.push({ ruleId: rule.id, templateId: rule.templateId, action: 'scheduled', scheduledFor: job.scheduledFor });
        } else {
          emitEvent(tenant, {
            type: 'failed_automation',
            to: tenantTo,
            from: String(from || ''),
            conversationId: key,
            meta: {
              ruleId: rule.id,
              templateId: rule.templateId,
              trigger: rule.trigger,
              errorCode: 'schedule_failed'
            }
          });
        }
      }
    }
  }

  if (actions.some((a) => a.action === 'sent')) {
    scheduleNoResponseFollowUps(tenant, tenantTo, from, data);
  }

  return actions;
}

/**
 * Schedule no_response follow-up rules after an auto-message was sent.
 */
function scheduleNoResponseFollowUps(tenant, to, from, data) {
  const rules = getRulesForNumber(data, to, tenant.accountId);
  const noResponseRules = rules.filter(r => r.trigger === 'no_response' && r.enabled && (r.delayMinutes || 0) > 0);

  if (noResponseRules.length === 0) return;

  const { scheduleJob } = require('./scheduler');
  for (const rule of noResponseRules) {
    scheduleJob(rule, { tenant, to, from });
  }
}

/**
 * Check all conditions for a rule.
 */
function passesConditions(rule, { tenant, to, from, conversation, data, triggerType }) {
  // 1. VIP / do-not-auto-reply check
  if (isVip(tenant, to, from, data)) {
    debugLog(`🚫 VIP suppressed: ${from}`);
    return false;
  }

  // 2. Active flow check (suppress automations if flow engine is handling this conversation)
  //    Exception: booking and postService automations fire regardless
  const skipIfFlowActive = !['booking', 'postService'].includes(rule.category);
  if (skipIfFlowActive && conversation?.flow?.status === 'active') {
    debugLog(`🚫 Active flow suppressed: ${rule.templateId}`);
    return false;
  }

  // 3. firstTimeOnly: only fire if this is the first interaction
  if (rule.firstTimeOnly) {
    const inboundCount = (conversation?.messages || []).filter(m => m.dir === 'in').length;
    const outboundAutoCount = (conversation?.messages || []).filter(m => m.dir === 'out' && m.auto).length;
    // For missed_call: no prior messages at all means first time
    // For inbound_sms: the current message would be the first inbound
    if (triggerType === 'missed_call' && (conversation?.messages?.length || 0) > 0) {
      return false;
    }
    if (triggerType === 'inbound_sms' && inboundCount > 1) {
      return false;
    }
  }

  // 4. Business hours check
  if (rule.businessHoursOnly && !isWithinBusinessHours(to, data)) {
    debugLog(`🚫 Outside business hours: ${rule.templateId}`);
    return false;
  }

  // 5. After hours check
  if (rule.afterHoursOnly && isWithinBusinessHours(to, data)) {
    debugLog(`🚫 Within business hours (after-hours rule): ${rule.templateId}`);
    return false;
  }

  // 6. Duplicate prevention: don't fire same templateId twice for same conversation
  const alreadyFired = (conversation?.audit || []).some(
    a => a.type === 'automation_fired' && a.meta?.templateId === rule.templateId
  );
  if (alreadyFired && rule.firstTimeOnly) {
    debugLog(`🚫 Already fired: ${rule.templateId}`);
    return false;
  }

  return true;
}

/**
 * Check if a phone number is on the VIP/do-not-auto-reply list.
 */
function isVip(tenant, to, from, data) {
  const vipList = (data.vipList?.[to] || []).filter((v) => String(v.accountId || tenant.accountId) === String(tenant.accountId));
  // Normalize phone for comparison (strip non-digits)
  const normalizePhone = (p) => String(p).replace(/\D/g, '');
  const fromNorm = normalizePhone(from);

  for (const vip of vipList) {
    const vipPhone = normalizePhone(vip.phone || '');
    if (vipPhone && vipPhone === fromNorm && vip.neverAutoReply) {
      return true;
    }
  }
  return false;
}

/**
 * Check if current time is within business hours for a business number.
 */
function isWithinBusinessHours(to, data) {
  const account = data?.accounts?.[to];
  const hours = account?.businessHours;

  const now = new Date();
  const day = now.getDay(); // 0=Sun, 6=Sat
  const currentMinutes = now.getHours() * 60 + now.getMinutes();

  if (!hours) {
    // Defaults: Mon-Fri 8am-6pm, Sat 9am-2pm, Sun closed
    if (day === 0) return false;
    if (day === 6) return currentMinutes >= 540 && currentMinutes < 840;
    return currentMinutes >= 480 && currentMinutes < 1080;
  }

  const dayNames = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
  const todayConfig = hours[dayNames[day]];
  if (!todayConfig || !todayConfig.open) return false;

  return currentMinutes >= todayConfig.start && currentMinutes < todayConfig.end;
}

/**
 * Replace template variables in a message.
 */
function replaceTemplateVars(template, { to, from, conversation, data, eventData }) {
  const account = data?.accounts?.[to] || {};
  const flowData = conversation?.flow?.data || {};

  return template
    .replace(/\[Business Name\]/g, account.businessName || account.name || 'Our Business')
    .replace(/\{\{service\}\}/g, eventData?.service || flowData.primary_intent || flowData.service || 'your service')
    .replace(/\{\{date\}\}/g, eventData?.date || flowData.date || '[date]')
    .replace(/\{\{time\}\}/g, eventData?.time || flowData.time || '[time]')
    .replace(/\{\{vehicle\}\}/g, flowData.vehicle || 'your vehicle')
    .replace(/\[Review Link\]/g, account.reviewLink || '[Review Link]')
    .replace(/\[Your Address\]/g, account.address || '[Address]');
}

/**
 * Send an automation message by appending to the conversation.
 */
async function sendAutomationMessage(tenant, to, from, rule, text) {
  const { sendResult } = await appendOutboundMessage({
    tenant,
    to,
    from,
    text,
    source: 'automation_rule',
    transactional: ['booking', 'postService'].includes(rule?.category),
    requireExisting: false,
    meta: {
      auto: true,
      status: 'sent',
      ruleId: rule.id,
      templateId: rule.templateId
    },
    afterSuccess(conversation) {
      conversation.lastActivityAt = Date.now();
      conversation.audit = Array.isArray(conversation.audit) ? conversation.audit : [];
      conversation.audit.push({
        ts: Date.now(),
        type: 'automation_fired',
        meta: {
          ruleId: rule.id,
          templateId: rule.templateId,
          trigger: rule.trigger,
          category: rule.category
        }
      });
    }
  });
  return sendResult;
}

/**
 * Get rules for a specific business number.
 * Handles migration from old flat array format.
 */
function getRulesForNumber(data, to, accountId) {
  if (!data.rules) return [];
  // Old format: flat array
  if (Array.isArray(data.rules)) return data.rules.filter((r) => String(r.accountId || '') === String(accountId));
  // New format: keyed by number
  return (data.rules[to] || []).filter((r) => String(r.accountId || '') === String(accountId));
}

async function runRecommendedAutomation(accountId, opportunityId) {
  const { runRecommendedAction } = require('./revenueOrchestrator');
  return runRecommendedAction(accountId, opportunityId);
}

module.exports = {
  evaluateTrigger,
  isWithinBusinessHours,
  replaceTemplateVars,
  sendAutomationMessage,
  getRulesForNumber,
  scheduleNoResponseFollowUps,
  runRecommendedAutomation
};
