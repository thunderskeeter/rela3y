const CANONICAL_SIGNALS = [
  'inbound_call_missed',
  'inbound_message_received',
  'web_form_submitted',
  'ig_dm_received',
  'chat_unanswered',
  'quote_abandoned',
  'appointment_no_show_risk',
  'lead_stalled'
];

function normalizeSignalType(rawType, payload = {}) {
  const type = String(rawType || '').trim().toLowerCase().replace(/\s+/g, '_');
  if (CANONICAL_SIGNALS.includes(type)) return type;

  if (type.includes('missed') && type.includes('call')) return 'inbound_call_missed';
  if (type.includes('inbound') && type.includes('sms')) return 'inbound_message_received';
  if (type.includes('form')) return 'web_form_submitted';
  if (type.includes('ig') || type.includes('instagram')) return 'ig_dm_received';
  if (type.includes('chat')) return payload.chatStatus === 'unanswered' ? 'chat_unanswered' : 'lead_stalled';
  if ((type.includes('quote') || type.includes('abandoned')) && payload.status === 'abandoned') return 'quote_abandoned';
  if (type.includes('appointment') && payload?.risk === 'no_show') return 'appointment_no_show_risk';
  if (payload?.stalled === true) return 'lead_stalled';
  return 'lead_stalled';
}

function isCanonical(type) {
  return CANONICAL_SIGNALS.includes(String(type || '').trim().toLowerCase());
}

module.exports = {
  CANONICAL_SIGNALS,
  normalizeSignalType,
  isCanonical
};
