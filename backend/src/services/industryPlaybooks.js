const { loadData, getAccountById } = require('../store/dataStore');

const BASE_PLAYBOOKS = {
  home_services: {
    id: 'home_services',
    name: 'Home Services',
    industries: ['plumber', 'hvac', 'electric', 'home services'],
    goalsBySignal: {
      missed_call: 'recover_missed_call',
      inbound_message: 'qualify_and_book',
      after_hours_inquiry: 'after_hours_capture',
      lead_stalled: 'revive_stale_lead'
    },
    cadenceProfile: { followupMinutes: [15, 120], maxFollowups: 2 },
    qualifyingQuestions: ['Is this an emergency?', 'What service do you need?', 'What zip code are you in?'],
    variants: {
      SEND_MESSAGE: [
        { id: 'hs_send_friendly', style: 'friendly', textTemplate: 'Hi {{firstName}}, this is {{businessName}}. Sorry we missed your call. Is this urgent or should we schedule service?', allowedPlaceholders: ['firstName', 'businessName'] },
        { id: 'hs_send_direct', style: 'direct', textTemplate: '{{businessName}} here. We missed your call. Reply EMERGENCY or SCHEDULE and we will help now.', allowedPlaceholders: ['businessName'] }
      ],
      ASK_QUALIFYING: [
        { id: 'hs_qualify_short', style: 'short', textTemplate: 'Quick details so we can route correctly: service type and zip code?', allowedPlaceholders: [] }
      ],
      OFFER_BOOKING: [
        { id: 'hs_book_direct', style: 'direct', textTemplate: 'Book your preferred time here: {{bookingLink}}', allowedPlaceholders: ['bookingLink'] }
      ]
    }
  },
  auto_services: {
    id: 'auto_services',
    name: 'Auto Services',
    industries: ['auto', 'detailing', 'repair'],
    goalsBySignal: {
      missed_call: 'recover_missed_call',
      inbound_message: 'quote_then_book',
      after_hours_inquiry: 'after_hours_capture',
      lead_stalled: 'revive_stale_lead'
    },
    cadenceProfile: { followupMinutes: [20, 180], maxFollowups: 2 },
    qualifyingQuestions: ['What vehicle year/make/model?', 'Which service package?', 'Preferred day?'],
    variants: {
      SEND_MESSAGE: [
        { id: 'auto_send_friendly', style: 'friendly', textTemplate: 'Hey {{firstName}}, {{businessName}} here. Sorry we missed you. Want a quick quote or booking link?', allowedPlaceholders: ['firstName', 'businessName'] }
      ],
      ASK_QUALIFYING: [
        { id: 'auto_qualify_direct', style: 'direct', textTemplate: 'Send vehicle year/make/model + service needed and we will price it fast.', allowedPlaceholders: [] }
      ],
      OFFER_BOOKING: [
        { id: 'auto_book_short', style: 'short', textTemplate: 'Reserve your slot: {{bookingLink}}', allowedPlaceholders: ['bookingLink'] }
      ]
    }
  },
  med_spa_beauty: {
    id: 'med_spa_beauty',
    name: 'Med Spa / Beauty',
    industries: ['med spa', 'beauty', 'aesthetics'],
    goalsBySignal: {
      missed_call: 'recover_missed_call',
      inbound_message: 'consult_then_book',
      after_hours_inquiry: 'after_hours_capture',
      lead_stalled: 'revive_stale_lead'
    },
    cadenceProfile: { followupMinutes: [30, 240], maxFollowups: 2 },
    qualifyingQuestions: ['Which treatment are you interested in?', 'Is this your first visit?', 'What day works best?'],
    variants: {
      SEND_MESSAGE: [
        { id: 'spa_send_friendly', style: 'friendly', textTemplate: 'Thanks for reaching out to {{businessName}}. We missed your call and can help with a quick consult + booking.', allowedPlaceholders: ['businessName'] }
      ],
      ASK_QUALIFYING: [
        { id: 'spa_qualify_short', style: 'short', textTemplate: 'What treatment are you interested in and when would you like to come in?', allowedPlaceholders: [] }
      ],
      OFFER_BOOKING: [
        { id: 'spa_book_friendly', style: 'friendly', textTemplate: 'You can pick a time here anytime: {{bookingLink}}', allowedPlaceholders: ['bookingLink'] }
      ]
    }
  },
  real_estate: {
    id: 'real_estate',
    name: 'Real Estate',
    industries: ['real estate', 'realtor', 'agent'],
    goalsBySignal: {
      missed_call: 'recover_missed_call',
      inbound_message: 'qualify_buyer_seller',
      after_hours_inquiry: 'after_hours_capture',
      lead_stalled: 'revive_stale_lead'
    },
    cadenceProfile: { followupMinutes: [30, 360], maxFollowups: 2 },
    qualifyingQuestions: ['Are you buying or selling?', 'Target area?', 'Timeline to move?'],
    variants: {
      SEND_MESSAGE: [
        { id: 're_send_direct', style: 'direct', textTemplate: '{{businessName}} team here. Sorry we missed you. Are you buying or selling right now?', allowedPlaceholders: ['businessName'] }
      ],
      ASK_QUALIFYING: [
        { id: 're_qualify_friendly', style: 'friendly', textTemplate: 'Share target area + timeline and we will line up best next steps.', allowedPlaceholders: [] }
      ],
      OFFER_BOOKING: [
        { id: 're_book_direct', style: 'direct', textTemplate: 'Book a call here: {{bookingLink}}', allowedPlaceholders: ['bookingLink'] }
      ]
    }
  }
};

function detectIndustry(account) {
  const raw = String(
    account?.workspace?.identity?.industry
    || account?.workspace?.identity?.businessType
    || account?.businessType
    || account?.industry
    || ''
  ).toLowerCase();
  for (const [id, pb] of Object.entries(BASE_PLAYBOOKS)) {
    if ((pb.industries || []).some((tag) => raw.includes(String(tag).toLowerCase()))) {
      return id;
    }
  }
  return 'home_services';
}

function deepMerge(base, patch) {
  if (!patch || typeof patch !== 'object' || Array.isArray(patch)) return base;
  if (!base || typeof base !== 'object' || Array.isArray(base)) return patch;
  const out = { ...base };
  for (const [k, v] of Object.entries(patch)) {
    if (v && typeof v === 'object' && !Array.isArray(v) && out[k] && typeof out[k] === 'object' && !Array.isArray(out[k])) {
      out[k] = deepMerge(out[k], v);
    } else {
      out[k] = v;
    }
  }
  return out;
}

function getPlaybookForAccount(accountId) {
  const data = loadData();
  const ref = getAccountById(data, accountId);
  const account = ref?.account || {};
  const id = detectIndustry(account);
  const base = BASE_PLAYBOOKS[id] || BASE_PLAYBOOKS.home_services;
  const overrides = account?.settings?.playbookOverrides?.[id] || {};
  return deepMerge(base, overrides);
}

function goalForSignal(playbook, signalType) {
  return String(playbook?.goalsBySignal?.[String(signalType || '')] || 'qualify_and_book');
}

function buildGoalSteps(playbook, goal, { requiresReview = false } = {}) {
  const delays = Array.isArray(playbook?.cadenceProfile?.followupMinutes) ? playbook.cadenceProfile.followupMinutes : [20, 120];
  const firstDelay = Math.max(15, Number(delays[0] || 15));
  const secondDelay = Math.max(60, Number(delays[1] || 120));
  const questions = Array.isArray(playbook?.qualifyingQuestions) ? playbook.qualifyingQuestions.slice(0, 3) : [];

  if (goal === 'after_hours_capture') {
    return [
      {
        stepId: 's1',
        type: 'SEND_MESSAGE',
        when: { kind: 'NOW' },
        payload: {},
        guardrails: { requiresReview, maxAttempts: 1, cooldownMinutes: 30 },
        successCriteria: { kind: 'INBOUND_REPLY' },
        failureCriteria: { kind: 'NO_REPLY_WITHIN', minutes: firstDelay }
      },
      {
        stepId: 's2',
        type: 'SCHEDULE_FOLLOWUP',
        when: { kind: 'AT_NEXT_OPEN' },
        payload: {},
        guardrails: { requiresReview: false, maxAttempts: 1, cooldownMinutes: 30 },
        successCriteria: { kind: 'INBOUND_REPLY' },
        failureCriteria: { kind: 'NO_REPLY_WITHIN', minutes: secondDelay }
      },
      {
        stepId: 's3',
        type: 'CREATE_ALERT',
        when: { kind: 'AFTER_MINUTES', minutes: secondDelay },
        payload: { alertMessage: 'Manual follow-up recommended for after-hours lead.' },
        guardrails: { requiresReview: true, maxAttempts: 1, cooldownMinutes: 30 },
        successCriteria: { kind: 'HUMAN_CONFIRMED' },
        failureCriteria: { kind: 'NO_REPLY_WITHIN', minutes: secondDelay }
      }
    ];
  }

  return [
    {
      stepId: 's1',
      type: 'SEND_MESSAGE',
      when: { kind: 'NOW' },
      payload: {},
      guardrails: { requiresReview, maxAttempts: 1, cooldownMinutes: 30 },
      successCriteria: { kind: 'INBOUND_REPLY' },
      failureCriteria: { kind: 'NO_REPLY_WITHIN', minutes: firstDelay }
    },
    {
      stepId: 's2',
      type: 'ASK_QUALIFYING',
      when: { kind: 'AFTER_MINUTES', minutes: firstDelay },
      payload: { questions },
      guardrails: { requiresReview: false, maxAttempts: 2, cooldownMinutes: 30 },
      successCriteria: { kind: 'INBOUND_REPLY' },
      failureCriteria: { kind: 'NO_REPLY_WITHIN', minutes: secondDelay }
    },
    {
      stepId: 's3',
      type: 'OFFER_BOOKING',
      when: { kind: 'AFTER_MINUTES', minutes: secondDelay },
      payload: {},
      guardrails: { requiresReview: false, maxAttempts: 1, cooldownMinutes: 30 },
      successCriteria: { kind: 'BOOKING_CREATED' },
      failureCriteria: { kind: 'NO_REPLY_WITHIN', minutes: secondDelay }
    },
    {
      stepId: 's4',
      type: 'CREATE_ALERT',
      when: { kind: 'AFTER_MINUTES', minutes: secondDelay },
      payload: { alertMessage: 'Automation paused: repeated no-response; check lead manually.' },
      guardrails: { requiresReview: true, maxAttempts: 1, cooldownMinutes: 30 },
      successCriteria: { kind: 'HUMAN_CONFIRMED' },
      failureCriteria: { kind: 'NO_REPLY_WITHIN', minutes: secondDelay }
    }
  ];
}

function getVariants(playbook, stepType) {
  const list = playbook?.variants?.[String(stepType || '')];
  return Array.isArray(list) ? list : [];
}

module.exports = {
  BASE_PLAYBOOKS,
  getPlaybookForAccount,
  goalForSignal,
  buildGoalSteps,
  getVariants,
  detectIndustry
};
