const STAGES = ['NEW', 'CONTACTED', 'ENGAGED', 'QUALIFIED', 'BOOKED', 'WON', 'LOST', 'STALE'];
const STALE_DAYS = 7;
const DAY_MS = 24 * 60 * 60 * 1000;

function normalizeStage(stage) {
  const s = String(stage || '').toUpperCase();
  return STAGES.includes(s) ? s : 'NEW';
}

function hasQualifiedSignal(recentEvents = []) {
  for (const event of recentEvents) {
    if (!event || typeof event !== 'object') continue;
    if (event.payload?.qualified === true) return true;
    const text = String(event.payload?.text || event.payload?.body || '').toLowerCase();
    if (/\b(quote approved|ready to book|book now|schedule now|yes lets do it|let's do it)\b/.test(text)) {
      return true;
    }
  }
  return false;
}

function deriveStage(opportunity, recentEvents = []) {
  const current = normalizeStage(opportunity?.stage);
  const status = String(opportunity?.status || '').toLowerCase();
  if (['won', 'closed'].includes(status)) return 'WON';
  if (status === 'lost') return 'LOST';

  const lastType = String(recentEvents[recentEvents.length - 1]?.type || '').toLowerCase();
  if (lastType === 'booking_completed') return 'WON';
  if (lastType === 'booking_created') return 'BOOKED';
  if (lastType === 'outbound_message' && current === 'NEW') return 'CONTACTED';
  if (lastType === 'inbound_message' && ['NEW', 'CONTACTED'].includes(current)) return 'ENGAGED';
  if (hasQualifiedSignal(recentEvents) && ['NEW', 'CONTACTED', 'ENGAGED'].includes(current)) return 'QUALIFIED';

  const lastInboundAt = Number(opportunity?.lastInboundAt || 0);
  const lastActivityAt = Number(opportunity?.lastActivityAt || 0);
  const anchor = Math.max(lastInboundAt, lastActivityAt);
  if (anchor > 0 && (Date.now() - anchor) >= (STALE_DAYS * DAY_MS) && !['BOOKED', 'WON', 'LOST'].includes(current)) {
    return 'STALE';
  }

  return current;
}

function transitionStage(opportunity, toStage, reason = '') {
  const from = normalizeStage(opportunity?.stage);
  const to = normalizeStage(toStage);
  if (!opportunity || typeof opportunity !== 'object') return opportunity;
  if (from === to) return opportunity;
  opportunity.stage = to;
  opportunity.stageHistory = Array.isArray(opportunity.stageHistory) ? opportunity.stageHistory : [];
  opportunity.stageHistory.push({
    ts: Date.now(),
    from,
    to,
    reason: String(reason || 'transition')
  });
  opportunity.stageHistory = opportunity.stageHistory.slice(-200);
  return opportunity;
}

function updateActivityTimestamps(opportunity, leadEvent) {
  if (!opportunity || typeof opportunity !== 'object') return opportunity;
  const ts = Number(leadEvent?.ts || Date.now());
  const type = String(leadEvent?.type || '').toLowerCase();
  opportunity.lastActivityAt = ts;
  if (type === 'inbound_message' || type === 'form_submit' || type === 'after_hours_inquiry') {
    opportunity.lastInboundAt = ts;
  }
  if (type === 'outbound_message') {
    opportunity.lastOutboundAt = ts;
  }
  return opportunity;
}

module.exports = {
  STAGES,
  normalizeStage,
  deriveStage,
  transitionStage,
  updateActivityTimestamps
};
