const DAY_MS = 24 * 60 * 60 * 1000;

const ALLOWED_STATUSES = new Set(['active', 'trialing', 'past_due', 'unpaid', 'canceled']);

function normalizeBillingStatus(status) {
  const v = String(status || '').trim().toLowerCase();
  return ALLOWED_STATUSES.has(v) ? v : 'active';
}

function ensureBillingShape(account = {}) {
  account.billing = account.billing && typeof account.billing === 'object' ? account.billing : {};
  account.billing.plan = account.billing.plan && typeof account.billing.plan === 'object' ? account.billing.plan : {};
  account.billing.dunning = account.billing.dunning && typeof account.billing.dunning === 'object'
    ? account.billing.dunning
    : {};
  const dunning = account.billing.dunning;
  dunning.enabled = dunning.enabled !== false;
  dunning.maxAttempts = Number.isFinite(Number(dunning.maxAttempts)) ? Math.max(1, Math.min(12, Number(dunning.maxAttempts))) : 4;
  dunning.attempts = Number.isFinite(Number(dunning.attempts)) ? Math.max(0, Number(dunning.attempts)) : 0;
  dunning.retryCadenceHours = Array.isArray(dunning.retryCadenceHours) && dunning.retryCadenceHours.length
    ? dunning.retryCadenceHours.map((x) => Math.max(1, Math.min(240, Number(x) || 24)))
    : [24, 48, 72, 96];
  dunning.lastFailedAt = Number(dunning.lastFailedAt || 0) || null;
  dunning.nextRetryAt = Number(dunning.nextRetryAt || 0) || null;
  dunning.graceEndsAt = Number(dunning.graceEndsAt || 0) || null;
  dunning.lockedAt = Number(dunning.lockedAt || 0) || null;
  dunning.lastFailureReason = String(dunning.lastFailureReason || '').trim() || null;
  account.billing.plan.status = normalizeBillingStatus(account.billing.plan.status);
  return account.billing;
}

function computeBillingLockState(account = {}, now = Date.now()) {
  const billing = ensureBillingShape(account);
  const plan = billing.plan || {};
  const dunning = billing.dunning || {};
  const status = normalizeBillingStatus(plan.status);
  const trialEndsAt = Number(plan.trialEndsAt || 0) || null;
  const lockAt = Number(dunning.lockedAt || 0) || null;

  if (status === 'active') return { locked: false, reason: null };
  if (status === 'trialing') {
    if (trialEndsAt && now > trialEndsAt) {
      if (lockAt && now >= lockAt) return { locked: true, reason: 'trial_expired' };
      return { locked: false, reason: 'trial_expired_grace' };
    }
    return { locked: false, reason: null };
  }
  if (status === 'past_due') {
    if (lockAt && now >= lockAt) return { locked: true, reason: 'payment_past_due' };
    return { locked: false, reason: 'payment_past_due' };
  }
  if (status === 'unpaid') return { locked: true, reason: 'payment_unpaid' };
  if (status === 'canceled') return { locked: true, reason: 'subscription_canceled' };
  return { locked: false, reason: null };
}

function canAccountAccessProduct(account = {}, now = Date.now()) {
  const billing = ensureBillingShape(account);
  const status = normalizeBillingStatus(billing?.plan?.status);
  if (billing.isLive !== true) return false;
  const lock = computeBillingLockState(account, now);
  if (lock.locked) return false;
  return status === 'active' || status === 'trialing' || status === 'past_due';
}

function recordPaymentFailure(account = {}, { now = Date.now(), reason = 'payment_failed' } = {}) {
  const billing = ensureBillingShape(account);
  const dunning = billing.dunning;
  dunning.attempts = Number(dunning.attempts || 0) + 1;
  dunning.lastFailedAt = now;
  dunning.lastFailureReason = String(reason || 'payment_failed');
  if (!dunning.graceEndsAt) dunning.graceEndsAt = now + (7 * DAY_MS);
  const cadence = dunning.retryCadenceHours;
  const stepIdx = Math.max(0, Math.min(cadence.length - 1, dunning.attempts - 1));
  dunning.nextRetryAt = now + (Math.max(1, Number(cadence[stepIdx] || 24)) * 60 * 60 * 1000);

  if (dunning.attempts >= Number(dunning.maxAttempts || 4)) {
    billing.plan.status = 'unpaid';
    dunning.lockedAt = now;
  } else {
    billing.plan.status = 'past_due';
    dunning.lockedAt = dunning.graceEndsAt;
  }
  return billing;
}

function recordPaymentSuccess(account = {}, { now = Date.now() } = {}) {
  const billing = ensureBillingShape(account);
  const dunning = billing.dunning;
  dunning.attempts = 0;
  dunning.lastFailedAt = null;
  dunning.nextRetryAt = null;
  dunning.graceEndsAt = null;
  dunning.lockedAt = null;
  dunning.lastFailureReason = null;
  billing.plan.status = 'active';
  if (!billing.plan.nextBillingAt || Number(billing.plan.nextBillingAt) < now) {
    billing.plan.nextBillingAt = now + (30 * DAY_MS);
  }
  return billing;
}

function startTrial(account = {}, { now = Date.now(), days = 14 } = {}) {
  const billing = ensureBillingShape(account);
  const n = Math.max(1, Math.min(30, Number(days) || 14));
  billing.plan.status = 'trialing';
  billing.plan.trialEndsAt = now + (n * DAY_MS);
  billing.plan.nextBillingAt = billing.plan.trialEndsAt;
  billing.dunning.lockedAt = billing.plan.trialEndsAt + (3 * DAY_MS);
  return billing;
}

module.exports = {
  normalizeBillingStatus,
  ensureBillingShape,
  computeBillingLockState,
  canAccountAccessProduct,
  recordPaymentFailure,
  recordPaymentSuccess,
  startTrial
};

