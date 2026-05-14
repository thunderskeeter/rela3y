const { loadData, saveDataDebounced, getConversationById } = require('../store/dataStore');
const { sendAutomationMessage, replaceTemplateVars, getRulesForNumber } = require('./automationEngine');
const { runScheduledCalendarIcsSyncs, runScheduledCalendarTwoWaySyncs } = require('./calendarIcsService');
const { runPassiveRevenueMonitoring, runReactivationScan, runPerformanceAlerts } = require('./passiveRevenueMonitoring');
const { runWeeklyOwnerDigest } = require('./opsVisibilityService');
const { optimizeOutcomePacks } = require('./optimizationService');
const { generateId } = require('../utils/id');
const { assertTenantScope } = require('../utils/tenant');
const { syncInvoiceLifecycleForConversation } = require('./customerInvoiceService');
const { DEV_MODE } = require('../config/runtime');

function debugLog(...args) {
  if (DEV_MODE === true) console.log(...args);
}

// ─── Scheduler ─────────────────────────────────────────────────────────
// Lightweight setInterval-based scheduler for delayed automations.
// Jobs persist to data.scheduledJobs in the JSON file so they survive restarts.

let schedulerInterval = null;
let tickCount = 0;

function isWithinQuietHours(account, ts = Date.now()) {
  const quiet = account?.settings?.notifications?.quietHours;
  if (!quiet || quiet.enabled !== true) return false;
  const m1 = String(quiet.start || '').match(/^(\d{2}):(\d{2})$/);
  const m2 = String(quiet.end || '').match(/^(\d{2}):(\d{2})$/);
  if (!m1 || !m2) return false;
  const mins = new Date(ts).getHours() * 60 + new Date(ts).getMinutes();
  const s = Number(m1[1]) * 60 + Number(m1[2]);
  const e = Number(m2[1]) * 60 + Number(m2[2]);
  if (s < e) return mins >= s && mins < e;
  return mins >= s || mins < e;
}

function normalizeTimezone(value) {
  const tz = String(value || '').trim();
  return tz || 'America/New_York';
}

function dayKeyInTimezone(ms, timezone) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: normalizeTimezone(timezone),
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).format(new Date(ms));
}

function hourInTimezone(ms, timezone) {
  const raw = new Intl.DateTimeFormat('en-US', {
    timeZone: normalizeTimezone(timezone),
    hour: '2-digit',
    hour12: false
  }).format(new Date(ms));
  const n = Number(raw);
  return Number.isFinite(n) ? n : 0;
}

function resolveConversationBookingTime(convo) {
  const candidates = [
    convo?.bookingTime,
    convo?.leadData?.booking_time,
    convo?.leadData?.bookingTime
  ];
  for (const value of candidates) {
    const ms = Number(value || 0);
    if (Number.isFinite(ms) && ms > 0) return ms;
  }
  if (Array.isArray(convo?.messages)) {
    for (let i = convo.messages.length - 1; i >= 0; i--) {
      const m = convo.messages[i];
      const ms = Number(m?.meta?.bookingTime || m?.bookingTime || 0);
      if (Number.isFinite(ms) && ms > 0) return ms;
    }
  }
  return 0;
}

function closeBookedConversationsAtNoon() {
  const data = loadData();
  const now = Date.now();
  const conversations = Object.entries(data.conversations || {});
  let closed = 0;

  for (const [key, convo] of conversations) {
    if (!convo || String(convo?.status || '').toLowerCase() !== 'booked') continue;
    const bookingMs = resolveConversationBookingTime(convo);
    if (!Number.isFinite(bookingMs) || bookingMs <= 0) continue;

    const to = String(convo?.to || '').trim();
    const account = data?.accounts?.[to] || {};
    const timezone = normalizeTimezone(account?.workspace?.timezone);
    const bookingDay = dayKeyInTimezone(bookingMs, timezone);
    const nowDay = dayKeyInTimezone(now, timezone);
    const nowHour = hourInTimezone(now, timezone);

    const shouldClose = nowDay > bookingDay || (nowDay === bookingDay && nowHour >= 12);
    if (!shouldClose) continue;

    convo.status = 'closed';
    convo.closedAt = now;
    convo.lastActivityAt = now;
    convo.audit = Array.isArray(convo.audit) ? convo.audit : [];
    convo.audit.push({
      ts: now,
      type: 'status_change',
      meta: { status: 'closed', source: 'scheduler_noon_close', bookingTime: bookingMs }
    });
    try {
      const from = String(convo?.from || '').trim();
      const accountId = String(convo?.accountId || '').trim();
      if (accountId && to && from) {
        syncInvoiceLifecycleForConversation({
          accountId,
          to,
          from,
          lifecycleStatus: 'close'
        });
      }
    } catch (err) {
      console.error('scheduler invoice lifecycle sync failed:', err?.message || err);
    }
    closed++;
  }

  if (closed > 0) {
    saveDataDebounced(data);
    debugLog(`🕛 Auto-closed ${closed} booked conversations at noon threshold`);
  }
}

/**
 * Initialize the scheduler. Call once at server startup.
 */
function initScheduler() {
  debugLog('⏰ Scheduler initialized');

  // Process any overdue jobs immediately
  tickScheduler();

  // Check every 60 seconds
  schedulerInterval = setInterval(tickScheduler, 60 * 1000);
}

/**
 * Stop the scheduler (for testing/cleanup).
 */
function stopScheduler() {
  if (schedulerInterval) {
    clearInterval(schedulerInterval);
    schedulerInterval = null;
  }
}

/**
 * Main tick: process due jobs and run periodic scans.
 */
function tickScheduler() {
  tickCount++;

  // Process due scheduled jobs every tick (every 60s)
  processDueJobs();
  closeBookedConversationsAtNoon();
  runScheduledCalendarIcsSyncs();
  runScheduledCalendarTwoWaySyncs();

  if (tickCount % 5 === 0) {
    runPassiveRevenueMonitoring().catch((err) => {
      console.error('PRM tick failed:', err?.message || err);
    });
  }

  // Periodic scans every 60 ticks (~1 hour)
  if (tickCount % 60 === 0) {
    void scanForInactiveCustomers();
    void scanForSeasonalReminders();
    void scanForLostLeads();
    runReactivationScan().catch((err) => console.error('Reactivation scan failed:', err?.message || err));
    runPerformanceAlerts();
    runWeeklyOwnerDigest();
    optimizeOutcomePacks().catch((err) => console.error('Optimization loop failed:', err?.message || err));
    cleanupOldJobs();
  }
}

/**
 * Process all jobs that are due (scheduledFor <= now).
 */
function processDueJobs() {
  const data = loadData();
  if (!data.scheduledJobs || data.scheduledJobs.length === 0) return;

  const now = Date.now();
  const dueJobs = data.scheduledJobs.filter(j => j.status === 'pending' && j.scheduledFor <= now);

  if (dueJobs.length === 0) return;

  let processed = 0;
  for (const job of dueJobs) {
    try {
      // Re-validate before executing
      const executionCheck = shouldExecuteJob(job, data);
      if (executionCheck === 'defer_quiet_hours') {
        job.scheduledFor = Date.now() + (15 * 60 * 1000);
        job.deferReason = 'quiet_hours';
        processed++;
        continue;
      }
      if (!executionCheck) {
        job.status = 'cancelled';
        job.cancelledAt = now;
        job.cancelReason = 'condition_check_failed';
        processed++;
        continue;
      }

      // Execute: send the automation message
      if (String(job.source || '') === 'agent_engine') {
        const runId = String(job?.eventData?.runId || '');
        const stepId = String(job?.eventData?.stepId || '');
        if (!runId || !stepId) {
          job.status = 'failed';
          job.error = 'agent_job_missing_run_step';
          processed++;
          continue;
        }
        const { executeStep } = require('./agentEngine');
        const result = executeStep(job.accountId, runId, stepId, {
          trigger: 'prm_tick',
          reviewApproved: false,
          forceNow: true
        });
        if (result && typeof result.then === 'function') {
          result.then(() => {
            job.status = 'completed';
            job.executedAt = now;
            saveDataDebounced(data);
          }).catch((err) => {
            job.status = 'failed';
            job.error = err?.message || 'agent_step_execution_failed';
            saveDataDebounced(data);
          });
        } else {
          job.status = 'completed';
          job.executedAt = now;
        }
        processed++;
        continue;
      }

      // Execute: send the automation message
      const conversation = getConversationById(job.accountId, `${job.to}__${job.from}`)?.conversation;
      if (conversation?.accountId) {
        assertTenantScope(job.accountId, conversation.accountId, { entity: 'scheduled job conversation' });
      }
      const text = replaceTemplateVars(
        job.ruleSnapshot.template,
        { to: job.to, from: job.from, conversation, data, eventData: job.eventData }
      );

      const tenant = { accountId: job.accountId, to: job.to };
      const sent = sendAutomationMessage(tenant, job.to, job.from, job.ruleSnapshot, text);
      if (sent && typeof sent.then === 'function') {
        sent.then((result) => {
          if (!result?.ok) {
            job.status = 'cancelled';
            job.cancelledAt = now;
            job.cancelReason = `compliance_blocked_${result?.error?.code || 'UNKNOWN'}`;
            if (job?.eventData?.actionId) {
              try {
                const { markActionOutcomeById } = require('./revenueOrchestrator');
                void markActionOutcomeById(job.accountId, job.eventData.actionId, { status: 'failed', error: job.cancelReason });
              } catch {}
            }
            saveDataDebounced(data);
            return;
          }
          job.status = 'completed';
          job.executedAt = now;
          if (job?.eventData?.actionId) {
            try {
              const { markActionOutcomeById } = require('./revenueOrchestrator');
              void markActionOutcomeById(job.accountId, job.eventData.actionId, { status: 'sent' });
            } catch {}
          }
          saveDataDebounced(data);
        }).catch((err) => {
          job.status = 'failed';
          job.error = err?.message || 'scheduled_send_failed';
          saveDataDebounced(data);
        });
        processed++;
        continue;
      }

      if (!sent.ok) {
        job.status = 'cancelled';
        job.cancelledAt = now;
        job.cancelReason = `compliance_blocked_${sent.error?.code || 'UNKNOWN'}`;
        if (job?.eventData?.actionId) {
          try {
            const { markActionOutcomeById } = require('./revenueOrchestrator');
            void markActionOutcomeById(job.accountId, job.eventData.actionId, { status: 'failed', error: job.cancelReason });
          } catch {}
        }
        processed++;
        continue;
      }

      job.status = 'completed';
      job.executedAt = now;
      if (job?.eventData?.actionId) {
        try {
          const { markActionOutcomeById } = require('./revenueOrchestrator');
          void markActionOutcomeById(job.accountId, job.eventData.actionId, { status: 'sent' });
        } catch {}
      }
      processed++;
      debugLog(`✅ Scheduled job executed: ${job.templateId} → ${job.to}__${job.from}`);
    } catch (err) {
      console.error(`❌ Error executing scheduled job ${job.id}:`, err.message);
      job.status = 'failed';
      job.error = err.message;
      processed++;
    }
  }

  if (processed > 0) {
    saveDataDebounced(data);
  }
}

/**
 * Validate whether a scheduled job should still execute.
 */
function shouldExecuteJob(job, data) {
  if (String(job.source || '') === 'agent_engine') {
    return true;
  }
  const key = `${job.to}__${job.from}`;
  const conversation = getConversationById(job.accountId, key)?.conversation;
  if (conversation?.accountId) {
    assertTenantScope(job.accountId, conversation.accountId, { entity: 'scheduled job guard conversation' });
  }
  if (!conversation || String(conversation.accountId || '') !== String(job.accountId || '')) {
    return false;
  }

  // For no_response jobs: check if customer replied since job was created
  if (job.trigger === 'no_response') {
    const messages = conversation?.messages || [];
    const customerRepliedSince = messages.some(
      m => m.dir === 'in' && m.ts > job.createdAt
    );
    if (customerRepliedSince) {
      debugLog(`🚫 Customer replied, cancelling no_response job: ${job.templateId}`);
      return false;
    }
  }

  // For lost lead jobs: check conversation status is still lost/cold
  if (job.trigger === 'lead_lost') {
    const status = (conversation?.status || '').toLowerCase();
    if (status !== 'lost' && status !== 'cold') {
      return false;
    }
  }

  // Check if rule is still enabled
  if (String(job.source || '') !== 'revenue_orchestrator') {
    const currentRules = getRulesForNumber(data, job.to, job.accountId);
    const rule = currentRules.find(r => r.templateId === job.templateId);
    if (!rule || !rule.enabled) {
      debugLog(`🚫 Rule disabled, cancelling job: ${job.templateId}`);
      return false;
    }
  }
  if (String(job.source || '') === 'revenue_orchestrator') {
    const account = data?.accounts?.[String(job.to || '')] || null;
    if (isWithinQuietHours(account, Date.now())) {
      return 'defer_quiet_hours';
    }
  }

  // Don't send if conversation has active flow (unless booking/postService)
  const skipIfFlowActive = !['booking', 'postService'].includes(job.ruleSnapshot?.category);
  if (skipIfFlowActive && conversation?.flow?.status === 'active') {
    return false;
  }

  return true;
}

/**
 * Schedule a delayed automation job.
 */
function scheduleJob(rule, { tenant, to, from, overrideScheduledFor, eventData, source = 'automation', conversationId = null }) {
  if (!tenant || !tenant.accountId || !tenant.to) return null;
  if (String(to) !== String(tenant.to)) return null;
  const data = loadData();
  data.scheduledJobs = data.scheduledJobs || [];

  const delayMs = (rule.delayMinutes || 0) * 60 * 1000;
  const scheduledFor = overrideScheduledFor || (Date.now() + delayMs);

  // Prevent duplicate: same templateId + same conversation + still pending
  const existing = data.scheduledJobs.find(j =>
    j.accountId === tenant.accountId &&
    j.to === to && j.from === from &&
    j.templateId === rule.templateId &&
    j.status === 'pending'
  );
  if (existing) {
    debugLog(`⏰ Already scheduled: ${rule.templateId} for ${to}__${from}`);
    return existing;
  }

  const job = {
    id: generateId(),
    ruleId: rule.id,
    templateId: rule.templateId,
    accountId: tenant.accountId,
    to,
    from,
    trigger: rule.trigger,
    category: rule.category,
    scheduledFor,
    createdAt: Date.now(),
    source: String(source || 'automation'),
    conversationId: conversationId ? String(conversationId) : `${to}__${from}`,
    status: 'pending',
    eventData: eventData || null,
    ruleSnapshot: {
      id: rule.id,
      templateId: rule.templateId,
      name: rule.name,
      template: rule.template,
      trigger: rule.trigger,
      category: rule.category
    }
  };

  data.scheduledJobs.push(job);
  saveDataDebounced(data);

  const delayStr = overrideScheduledFor
    ? `at ${new Date(scheduledFor).toISOString()}`
    : `in ${rule.delayMinutes}min`;
  debugLog(`⏰ Scheduled: ${rule.templateId} for ${to}__${from} ${delayStr}`);
  return job;
}

/**
 * Cancel pending jobs for a conversation (e.g. when customer replies).
 * Only cancels no_response and lead_lost triggers by default.
 */
function cancelJobsForConvo(to, from, accountId, reason) {
  const data = loadData();
  if (!data.scheduledJobs || data.scheduledJobs.length === 0) return;

  const cancellableTriggers = ['no_response', 'lead_lost'];
  let cancelled = 0;

  for (const job of data.scheduledJobs) {
    if (
      job.to === to &&
      job.from === from &&
      String(job.accountId || '') === String(accountId || '') &&
      job.status === 'pending'
    ) {
      if (cancellableTriggers.includes(job.trigger)) {
        job.status = 'cancelled';
        job.cancelledAt = Date.now();
        job.cancelReason = reason || 'customer_replied';
        cancelled++;
      }
    }
  }

  if (cancelled > 0) {
    saveDataDebounced(data);
    debugLog(`🚫 Cancelled ${cancelled} scheduled jobs for ${to}__${from} (${reason || 'customer_replied'})`);
  }
}

// ─── Periodic scans ────────────────────────────────────────────────────

/**
 * Scan for inactive customers (no activity in 30+ days) and fire win-back.
 */
async function scanForInactiveCustomers() {
  const data = loadData();
  const THIRTY_DAYS = 30 * 24 * 60 * 60 * 1000;
  const now = Date.now();

  const conversations = Object.entries(data.conversations || {});
  for (const [key, convo] of conversations) {
    const lastActivity = convo.lastActivityAt || 0;
    if (now - lastActivity < THIRTY_DAYS) continue;

    const to = convo.to;
    const from = convo.from;
    const accountId = convo.accountId;
    if (!to || !from || !accountId) continue;

    // Check if inactive-30-days rule is enabled for this number
    const rules = getRulesForNumber(data, to, accountId);
    const rule = rules.find(r => r.templateId === 'inactive-30-days' && r.enabled);
    if (!rule) continue;

    // Check if we already sent this (check audit)
    const alreadySent = (convo.audit || []).some(
      a => a.type === 'automation_fired' && a.meta?.templateId === 'inactive-30-days'
    );
    if (alreadySent) continue;

    // Fire it
    const text = replaceTemplateVars(rule.template, { to, from, conversation: convo, data });
    await sendAutomationMessage({ accountId, to }, to, from, rule, text);
    debugLog(`👋 Win-back sent: ${to}__${from} (inactive ${Math.round((now - lastActivity) / (24*60*60*1000))} days)`);
  }
}

/**
 * Scan for seasonal reminder opportunities.
 */
async function scanForSeasonalReminders() {
  const data = loadData();
  const now = new Date();
  const month = now.getMonth(); // 0-11

  // Determine season
  let season;
  if (month >= 2 && month <= 4) season = 'spring';
  else if (month >= 5 && month <= 7) season = 'summer';
  else if (month >= 8 && month <= 10) season = 'fall';
  else season = 'winter';

  // Only send during first 14 days of the season
  const seasonStartMonth = { spring: 2, summer: 5, fall: 8, winter: 11 };
  const startMonth = seasonStartMonth[season];
  if (now.getMonth() !== startMonth && now.getMonth() !== (startMonth === 11 ? 0 : startMonth)) return;
  if (now.getDate() > 14) return;

  const currentSeasonKey = `${season}-${now.getFullYear()}`;

  const conversations = Object.entries(data.conversations || {});
  for (const [key, convo] of conversations) {
    const to = convo.to;
    const from = convo.from;
    const accountId = convo.accountId;
    if (!to || !from || !accountId) continue;

    // Check if rule is enabled
    const rules = getRulesForNumber(data, to, accountId);
    const rule = rules.find(r => r.templateId === 'seasonal-reminder' && r.enabled);
    if (!rule) continue;

    // Check if already sent this season
    const alreadySent = (convo.audit || []).some(
      a => a.type === 'automation_fired' && a.meta?.templateId === 'seasonal-reminder' && a.meta?.season === currentSeasonKey
    );
    if (alreadySent) continue;

    // Must have had some prior interaction (don't spam unknown numbers)
    if (!convo.messages || convo.messages.length === 0) continue;

    const text = replaceTemplateVars(rule.template, { to, from, conversation: convo, data });
    await sendAutomationMessage({ accountId, to }, to, from, { ...rule, _seasonKey: currentSeasonKey }, text);

    // Add season key to audit for dedup
    const audit = (data.conversations[key]?.audit || []);
    const lastAudit = audit[audit.length - 1];
    if (lastAudit && lastAudit.type === 'automation_fired' && lastAudit.meta?.templateId === 'seasonal-reminder') {
      lastAudit.meta.season = currentSeasonKey;
    }

    debugLog(`🍂 Seasonal reminder sent: ${to}__${from} (${season})`);
  }
}

/**
 * Scan for lost leads that should get recovery messages.
 */
async function scanForLostLeads() {
  const data = loadData();
  const SEVEN_DAYS = 7 * 24 * 60 * 60 * 1000;
  const now = Date.now();

  const conversations = Object.entries(data.conversations || {});
  for (const [key, convo] of conversations) {
    const status = (convo.status || '').toLowerCase();
    if (status !== 'lost' && status !== 'cold') continue;

    const to = convo.to;
    const from = convo.from;
    const accountId = convo.accountId;
    if (!to || !from || !accountId) continue;

    // Check when status was set to lost (use last status change in audit or lastActivityAt)
    const lostEvent = [...(convo.audit || [])].reverse().find(
      a => a.type === 'status_changed' && (a.meta?.newStatus === 'lost' || a.meta?.newStatus === 'cold')
    );
    const lostAt = lostEvent?.ts || convo.lastActivityAt || 0;
    if (now - lostAt < SEVEN_DAYS) continue;

    // Check if rule is enabled
    const rules = getRulesForNumber(data, to, accountId);
    const rule = rules.find(r => r.templateId === 'lost-lead-recovery' && r.enabled);
    if (!rule) continue;

    // Check if already sent
    const alreadySent = (convo.audit || []).some(
      a => a.type === 'automation_fired' && a.meta?.templateId === 'lost-lead-recovery'
    );
    if (alreadySent) continue;

    const text = replaceTemplateVars(rule.template, { to, from, conversation: convo, data });
    await sendAutomationMessage({ accountId, to }, to, from, rule, text);
    debugLog(`🔄 Lost lead recovery sent: ${to}__${from}`);
  }
}

/**
 * Clean up completed/cancelled/failed jobs older than 7 days.
 */
function cleanupOldJobs() {
  const data = loadData();
  if (!data.scheduledJobs) return;

  const SEVEN_DAYS = 7 * 24 * 60 * 60 * 1000;
  const cutoff = Date.now() - SEVEN_DAYS;
  const before = data.scheduledJobs.length;

  data.scheduledJobs = data.scheduledJobs.filter(j =>
    j.status === 'pending' || (j.executedAt || j.cancelledAt || j.createdAt) > cutoff
  );

  const removed = before - data.scheduledJobs.length;
  if (removed > 0) {
    saveDataDebounced(data);
    debugLog(`🧹 Cleaned up ${removed} old scheduled jobs`);
  }
}

module.exports = {
  initScheduler,
  stopScheduler,
  scheduleJob,
  cancelJobsForConvo,
  tickScheduler,
  scanForInactiveCustomers,
  scanForSeasonalReminders,
  scanForLostLeads
};
