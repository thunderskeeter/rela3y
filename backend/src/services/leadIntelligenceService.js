const { loadData } = require('../store/dataStore');
const { updateIntelligence, scopedIntelligenceKey } = require('./revenueIntelligenceService');
const { normalizeSignalType } = require('./signalService');

const URGENT_KEYWORDS = ['urgent', 'emergency', 'asap', 'now', 'immediately', 'help', 'priority'];
const PRICE_KEYWORDS = ['price', 'cost', 'quote', 'estimate', 'how much', 'pricing'];
const NEGATIVE_KEYWORDS = ['not happy', 'bad', 'terrible', 'disappointed', 'hate', 'don\'t'];
const POSITIVE_KEYWORDS = ['great', 'love', 'awesome', 'perfect', 'thank you', 'thanks'];

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function deriveIntent(text = '') {
  const normalized = String(text || '').toLowerCase();
  if (/\b(emergency|urgent)\b/.test(normalized)) return 'emergency';
  if (/\b(book|schedule|appointment|meet)\b/.test(normalized)) return 'book';
  if (/\b(quote|estimate|pricing|price|cost)\b/.test(normalized)) return 'quote';
  if (normalized) return 'general_question';
  return 'unknown';
}

function scoreUrgency(text = '', score = 0) {
  const normalized = String(text || '').toLowerCase();
  let next = score;
  if (URGENT_KEYWORDS.some((term) => normalized.includes(term))) next += 30;
  if (PRICE_KEYWORDS.some((term) => normalized.includes(term))) next += 10;
  return clamp(next, 0, 100);
}

function scoreSentiment(text = '', existing = 0) {
  const normalized = String(text || '').toLowerCase();
  let delta = 0;
  if (NEGATIVE_KEYWORDS.some((term) => normalized.includes(term))) delta -= 25;
  if (POSITIVE_KEYWORDS.some((term) => normalized.includes(term))) delta += 20;
  return clamp(existing + delta, -100, 100);
}

function scoreLeadQuality({ hasContact = false, pastBookings = 0, leadAgeMinutes = 0 }) {
  let score = 50;
  if (hasContact) score += 15;
  if (pastBookings >= 1) score += 20;
  if (leadAgeMinutes <= 60) score += 10;
  if (leadAgeMinutes > 1440) score -= 10;
  return clamp(score, 0, 100);
}

function getKeyFromEvent(event) {
  if (!event) return '';
  if (event.contactId) return String(event.contactId);
  if (event.convoKey) return String(event.convoKey);
  return String(event.payload?.contactId || '');
}

async function analyzeLeadEvent(accountId, event) {
  if (!accountId || !event) return null;
  const key = getKeyFromEvent(event);
  if (!key) return null;
  const data = loadData();
  const existing = data?.leadIntelligence?.[scopedIntelligenceKey(accountId, key)] || {};
  const msg = String(event?.payload?.text || event?.payload?.body || event?.payload?.message || '').trim();
  const intent = deriveIntent(msg);
  const urgencyScore = scoreUrgency(msg, Number(existing?.urgencyScore || 0));
  const sentimentScore = scoreSentiment(msg, Number(existing?.sentimentScore || 0));
  const leadQualityScore = scoreLeadQuality({
    hasContact: Boolean(event?.contactId),
    pastBookings: Number(existing?.pastBookings || 0),
    leadAgeMinutes: Math.floor(((Date.now() - Number(event?.ts || Date.now())) || 0) / 60000)
  });
  const payload = {
    intent,
    urgencyScore,
    sentimentScore,
    leadQualityScore,
    lastAnalyzedAt: Date.now(),
    notes: [
      `signal:${normalizeSignalType(event?.type || '')}`,
      ...(existing?.notes || []).slice(0, 5)
    ]
  };
  return updateIntelligence(accountId, key, payload);
}

module.exports = {
  analyzeLeadEvent,
  deriveIntent,
  scoreUrgency,
  scoreSentiment,
  scoreLeadQuality
};
