const { loadData, saveDataDebounced } = require('../store/dataStore');
const { generateId } = require('../utils/id');

const DEFAULT_CONFIDENCE = 0.6;

function ensureRevenueEvents(data) {
  data.revenueEvents = Array.isArray(data.revenueEvents) ? data.revenueEvents : [];
}

function logRevenueEvent(accountId, input = {}) {
  if (!accountId) throw new Error('accountId is required');
  const data = loadData();
  ensureRevenueEvents(data);
  const event = {
    id: generateId(),
    business_id: String(accountId),
    contact_id: input.contactId ? String(input.contactId) : null,
    related_lead_event_id: input.relatedLeadEventId ? String(input.relatedLeadEventId) : null,
    revenue_event_type: String(input.revenueEventType || 'opportunity_created'),
    estimated_value_cents: Number.isFinite(Number(input.estimatedValueCents)) ? Number(input.estimatedValueCents) : null,
    confidence: Number.isFinite(Number(input.confidence)) ? Math.min(1, Math.max(0, Number(input.confidence))) : DEFAULT_CONFIDENCE,
    status: String(input.status || 'open'),
    created_at: Date.now(),
    metadata_json: input.metadata && typeof input.metadata === 'object' ? { ...input.metadata } : {}
  };
  data.revenueEvents.push(event);
  saveDataDebounced(data);
  return event;
}

function getRevenueEventsForAccount(accountId) {
  const data = loadData();
  return (data.revenueEvents || []).filter((e) => String(e?.business_id || '') === String(accountId));
}

function getRevenueSettings(accountId) {
  const data = loadData();
  const account = (data.accounts || {})[Object.keys(data.accounts || {}).find((k) => String(data.accounts[k]?.accountId || data.accounts[k]?.id || '') === String(accountId))] || null;
  const settings = account?.settings?.finance || {};
  return {
    averageTicketValueCents: Number(settings.averageTicketValueCents || 0),
    conversionRateBaseline: Math.max(0, Math.min(1, Number(settings.conversionRateBaseline || 0))),
    valueByServiceType: settings.valueByServiceType && typeof settings.valueByServiceType === 'object' ? settings.valueByServiceType : {}
  };
}

module.exports = {
  logRevenueEvent,
  getRevenueEventsForAccount,
  getRevenueSettings
};
