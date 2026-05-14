const { loadData, getAccountById } = require('../store/dataStore');
const { getPlaybookForAccount, getVariants } = require('./industryPlaybooks');
const { getFeatureFlags } = require('./revenueIntelligenceService');

const DAY_KEYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

function normalizeBusinessContext(accountId) {
  const data = loadData();
  const accountRef = getAccountById(data, accountId);
  const account = accountRef?.account || {};
  const workspace = account.workspace || {};
  const profile = account.businessProfile || {};
  return {
    businessName: String(workspace?.identity?.businessName || account.businessName || 'Your business').trim(),
    businessType: profile.businessType || workspace?.identity?.industry || 'local_service',
    businessHours: profile.businessHours || workspace.businessHours || {},
    bookingUrl: profile.bookingUrl || account.bookingUrl || '',
    toneStyle: profile.toneStyle || workspace?.settings?.tonePreference || 'friendly_professional',
    services: Array.isArray(profile.services) ? profile.services : [],
    escalationRules: profile.escalationRules && typeof profile.escalationRules === 'object'
      ? { ...profile.escalationRules }
      : {}
  };
}

function getPhoneFromConvo(convoKey) {
  if (!convoKey || typeof convoKey !== 'string') return null;
  const parts = convoKey.split('__');
  return parts[1] || null;
}

function buildContactHistory(accountId, leadEvent, opportunity) {
  const data = loadData();
  const phone = String(leadEvent?.contactId || opportunity?.contactId || getPhoneFromConvo(leadEvent?.convoKey) || '').trim();
  const contacts = Object.values(data.contacts || {}).filter((c) => String(c?.accountId || '') === String(accountId));
  const contact = contacts.find((c) => String(c?.phone || '') === phone) || null;
  const flags = contact?.flags || {};
  const revenueEvents = (data.revenueEvents || []).filter((e) =>
    String(e?.business_id || e?.accountId || '') === String(accountId)
  );
  const contactEvents = string => revenueEvents.filter((e) => String(e?.contact_id || '') === String(string));
  const relevantEvents = contactEvents(phone);
  const bookingTypes = new Set(['booking_created', 'appointment_booked', 'opportunity_recovered', 'sale_closed']);
  const bookingEvents = relevantEvents.filter((e) => bookingTypes.has(String(e?.revenue_event_type || '').toLowerCase()));
  const lastBookingAt = bookingEvents.reduce((acc, evt) => Math.max(acc, Number(evt?.created_at || evt?.ts || evt?.updated_at || 0)), 0);
  const spends = bookingEvents
    .map((evt) => Number(evt?.estimated_value_cents || 0))
    .filter((n) => Number.isFinite(n) && n > 0);
  const avgSpendCents = spends.length ? Math.round(spends.reduce((sum, n) => sum + n, 0) / spends.length) : null;
  return {
    phone,
    name: contact?.name || '',
    firstName: contact?.name ? contact.name.split(' ')[0] : '',
    vip: Boolean(flags.vip || contact?.tags?.includes?.('vip')),
    doNotContact: Boolean(contact?.optedOut === true || flags.doNotAutoReply === true || flags.doNotContact === true),
    optedOut: Boolean(contact?.optedOut === true),
    lastBookingAt,
    pastBookingsCount: bookingEvents.length,
    avgSpendCents,
    tags: Array.isArray(contact?.tags) ? contact.tags : [],
    serviceType: opportunity?.metadata?.serviceType || leadEvent?.payload?.service || leadEvent?.payload?.serviceType || ''
  };
}

function matchesUrgentKeywords(text, keywords) {
  if (!text) return false;
  const clean = String(text).toLowerCase();
  return (Array.isArray(keywords) ? keywords : []).some((word) => {
    const key = String(word || '').toLowerCase();
    return key && clean.includes(key);
  });
}

function parseTimeWindow(value) {
  if (!value || typeof value !== 'string') return null;
  const pieces = value.split(':').map((x) => Number(x));
  if (pieces.length < 2 || !Number.isFinite(pieces[0]) || !Number.isFinite(pieces[1])) return null;
  return (Math.max(0, Math.min(23, pieces[0])) * 60) + Math.max(0, Math.min(59, pieces[1]));
}

function isWithinBusinessHours(hours = {}, ts = Date.now()) {
  if (!hours || typeof hours !== 'object') return false;
  const day = DAY_KEYS[new Date(ts).getDay()];
  const windows = Array.isArray(hours[day]) ? hours[day] : [];
  if (!windows.length) return false;
  const now = new Date(ts);
  const minutes = now.getHours() * 60 + now.getMinutes();
  for (const window of windows) {
    const start = parseTimeWindow(window?.start);
    const end = parseTimeWindow(window?.end);
    if (start == null || end == null) continue;
    if (start <= end) {
      if (minutes >= start && minutes < end) return true;
    } else {
      if (minutes >= start || minutes < end) return true;
    }
  }
  return false;
}

function findNextOpenWindow(hours = {}, ts = Date.now()) {
  const startDate = new Date(ts);
  for (let offset = 0; offset < 7; offset += 1) {
    const candidate = new Date(startDate);
    candidate.setDate(startDate.getDate() + offset);
    const day = DAY_KEYS[candidate.getDay()];
    const windows = Array.isArray(hours[day]) ? hours[day] : [];
    for (const window of windows) {
      const start = parseTimeWindow(window?.start);
      if (start == null) continue;
      const open = new Date(candidate);
      open.setHours(0, 0, 0, 0);
      open.setMinutes(start);
      if (open.getTime() > ts) return open;
    }
  }
  return null;
}

function formatNextOpenWindow(date) {
  if (!date) return null;
  const day = DAY_KEYS[date.getDay()];
  const hours = `${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
  return `${day.charAt(0).toUpperCase() + day.slice(1)} at ${hours}`;
}

function buildAfterHoursMessage(profile, nextOpen) {
  const nextText = nextOpen ? ` We'll reply ${formatNextOpenWindow(nextOpen)}.` : ' We will reply when we reopen.';
  return `Thanks for reaching out to ${profile.businessName}.${nextText}`;
}

function renderTemplate(template, vars = {}, allowed = []) {
  if (!template) return '';
  const allowList = Array.isArray(allowed) && allowed.length ? allowed : null;
  const raw = String(template || '');
  return raw.replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (match, key) => {
    const field = String(key || '');
    if (allowList && !allowList.includes(field)) return '';
    const value = vars[field];
    return value == null ? '' : String(value);
  }).replace(/\s{2,}/g, ' ').trim();
}

function chooseVariant(playbook, stepType, profile, contactHistory, flags = {}) {
  const variants = getVariants(playbook, stepType);
  if (!variants.length) return null;
  let pool = variants;
  if (flags.enableAIMessageVariants === true) {
    const tone = String(profile.toneStyle || '').split('_')[0].toLowerCase();
    const match = variants.find((v) => String(v?.style || '').toLowerCase() === tone);
    if (match) pool = [match];
  }
  const variant = pool[0];
  const vars = {
    businessName: profile.businessName,
    bookingLink: profile.bookingUrl,
    firstName: contactHistory.firstName || 'there',
    serviceType: contactHistory.serviceType || ''
  };
  const text = renderTemplate(variant.textTemplate, vars, variant.allowedPlaceholders || []);
  return {
    id: String(variant.id || ''),
    text: text || '',
    style: String(variant.style || 'direct')
  };
}

function buildFallbackMessage(profile, contactHistory, intent) {
  const greeting = contactHistory.firstName ? `${contactHistory.firstName}, ` : '';
  if (intent === 'book') {
    return `${greeting}Thanks for reaching out to ${profile.businessName}. Reply with the service you're after and we will lock in a slot.`;
  }
  if (intent === 'quote') {
    return `${greeting}We can craft your quote—just share the service details and location.`;
  }
  if (intent === 'emergency') {
    return `${greeting}${profile.businessName} sees this is urgent. Reply CALL if you need immediate help.`;
  }
  return `${greeting}Thanks for reaching out to ${profile.businessName}. Reply with a bit more info and we will help right away.`;
}

function buildBookingSlots(profile) {
  if (!profile.bookingUrl) return [];
  return [{
    type: 'booking_link',
    label: 'Book online',
    url: profile.bookingUrl
  }];
}

function buildFollowupSchedule(playbook) {
  const baseDelays = Array.isArray(playbook?.cadenceProfile?.followupMinutes)
    ? playbook.cadenceProfile.followupMinutes.map((delay) => Math.max(15, Number(delay) || 30))
    : [30, 120];
  const maxFollowups = Math.max(1, Number(playbook?.cadenceProfile?.maxFollowups || 2));
  return {
    delays: baseDelays,
    maxFollowups
  };
}

function buildFollowups(profile, schedule, contactHistory) {
  const followups = [];
  const delays = schedule.delays.slice(0, schedule.maxFollowups);
  const lastDelay = delays.length ? delays[delays.length - 1] : 45;
  for (let idx = 0; idx < schedule.maxFollowups; idx += 1) {
    const delay = delays[idx] ?? lastDelay;
    const friendlyName = contactHistory.firstName || 'there';
    const message = idx === 0
      ? `Just checking in from ${profile.businessName}, ${friendlyName}. Still happy to help whenever you're ready.`
      : `Still here for you, ${friendlyName}. Reply when you're free and we'll take care of it.`;
    followups.push({
      delayMinutes: delay,
      messageText: message,
      condition: 'no_reply'
    });
  }
  return followups;
}

function detectRecommendedPack(signalType) {
  const map = {
    missed_call: 'recover_missed_calls',
    after_hours_inquiry: 'after_hours_receptionist',
    lead_stalled: 'reactivation_campaign',
    form_submit: 'lead_qualification_booking',
    inbound_message: 'lead_qualification_booking',
    booking_created: 'recover_missed_calls'
  };
  return map[String(signalType || '').toLowerCase()] || 'lead_qualification_booking';
}

function inferIntent(leadEvent, opportunity, contactHistory) {
  const payload = leadEvent?.payload || {};
  const explicit = String(payload.intent || opportunity?.metadata?.intent || '').toLowerCase();
  if (['book', 'quote', 'emergency', 'info', 'general_question'].includes(explicit)) return explicit === 'info' ? 'general_question' : explicit;
  const text = String(payload.text || payload.body || '').toLowerCase();
  if (/\b(emergency|urgent|asap|immediately)\b/.test(text)) return 'emergency';
  if (/\b(book|schedule|appointment|meet)\b/.test(text)) return 'book';
  if (/\b(quote|estimate|pricing|cost)\b/.test(text)) return 'quote';
  if (contactHistory.pastBookingsCount >= 1 && text.includes('again')) return 'book';
  if (text) return 'general_question';
  return 'unknown';
}

function determineUrgency(opportunity, contactHistory, leadEvent, profile) {
  const risk = Number(opportunity?.riskScore || 0);
  const urgentText = matchesUrgentKeywords(String(leadEvent?.payload?.text || ''), profile.escalationRules.urgentKeywords || []);
  if (contactHistory.vip || urgentText || risk >= 70) return 'high';
  if (risk >= 40 || contactHistory.pastBookingsCount >= 3) return 'medium';
  return 'low';
}

function buildEscalationMessage(profile, contactHistory, reason) {
  if (reason === 'vip_priority') {
    return `${contactHistory.firstName || 'This lead'} is VIP flagged and needs a human response ASAP.`;
  }
  if (reason === 'urgent_keyword') {
    return `Urgent signal captured—please call ${profile.businessName} or the lead directly as soon as possible.`;
  }
  return `Lead requires manual follow-up. Please review the conversation and take over.`;
}

function decideActionPlan(accountId, { leadEvent = {}, opportunity = {} } = {}) {
  const profile = normalizeBusinessContext(accountId);
  const contactHistory = buildContactHistory(accountId, leadEvent, opportunity);
  const intent = inferIntent(leadEvent, opportunity, contactHistory);
  const urgency = determineUrgency(opportunity, contactHistory, leadEvent, profile);
  const afterHours = !isWithinBusinessHours(profile.businessHours, Number(leadEvent?.ts || opportunity?.metadata?.lastSignalTs || Date.now()));
  const urgentKeywordsHit = matchesUrgentKeywords(String(leadEvent?.payload?.text || ''), profile.escalationRules.urgentKeywords || []);
  const escalateVIP = profile.escalationRules.escalateOnVIP === true && contactHistory.vip;
  const escalate = (escalateVIP && Number(opportunity?.riskScore || 0) >= 50) || urgentKeywordsHit;
  const nextAction = contactHistory.doNotContact || contactHistory.optedOut || opportunity?.stopAutomation ? 'do_nothing'
    : (escalate ? 'escalate' : 'send_message');
  const accountRef = getAccountById(loadData(), accountId);
  const flags = getFeatureFlags(accountRef);
  const playbook = getPlaybookForAccount(accountId);
  const variant = chooseVariant(playbook, 'SEND_MESSAGE', profile, contactHistory, flags);
  let messageText = variant?.text || buildFallbackMessage(profile, contactHistory, intent);
  let messageTemplateId = variant?.id || null;
  let escalationReason = null;
  if (nextAction === 'escalate') {
    escalationReason = urgentKeywordsHit ? 'urgent_keyword' : (escalateVIP ? 'vip_priority' : 'manual_review');
    messageText = buildEscalationMessage(profile, contactHistory, escalationReason);
    messageTemplateId = `escalate_${escalationReason}`;
  } else if (nextAction === 'send_message' && afterHours) {
    const nextOpen = findNextOpenWindow(profile.businessHours, Number(leadEvent?.ts || Date.now()));
    const suffix = nextOpen ? ` We'll reply ${formatNextOpenWindow(nextOpen)}.` : ' We will reply when we reopen.';
    messageText = `${messageText}${suffix}`.trim();
  }
  const bookingSlots = buildBookingSlots(profile);
  const followupSchedule = buildFollowupSchedule(playbook);
  const followups = nextAction === 'send_message' ? buildFollowups(profile, followupSchedule, contactHistory) : [];
  const tagsToApply = [
    `intent:${intent}`,
    `urgency:${urgency}`,
    ...(contactHistory.vip ? ['vip'] : []),
    `signal:${String(leadEvent?.type || 'lead_stalled')}`
  ];
  const plan = {
    intent,
    urgency,
    nextAction,
    messageTemplateId,
    messageText: nextAction === 'do_nothing' ? '' : messageText,
    followupSchedule,
    followups,
    bookingSlots,
    recommendedPack: detectRecommendedPack(String(leadEvent?.type || 'lead_stalled')),
    tagsToApply,
    afterHours,
    escalation: escalationReason ? { reason: escalationReason } : null
  };
  return plan;
}

module.exports = {
  decideActionPlan,
  normalizeBusinessContext
};
