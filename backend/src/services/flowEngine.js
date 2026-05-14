const { loadData, saveDataDebounced, getFlowInData } = require('../store/dataStore');
const Anthropic = require('@anthropic-ai/sdk');
const { recordOutboundAttempt } = require('./messagingBoundaryService');
const { pushBookingToConnectedCalendars } = require('./calendarIcsService');
const { ensureSchedulingConfig, publicBookingUrlForAccount } = require('./publicBookingService');
const { logRevenueEvent } = require('./revenueEventService');
const { startFlowLegacy, advanceFlowLegacy } = require('./messagingBoundaryService');
const { DEV_MODE } = require('../config/runtime');

function debugLog(...args) {
  if (DEV_MODE === true) console.log(...args);
}

// Initialize Claude (only if API key is set)
let anthropic = null;
if (process.env.ANTHROPIC_API_KEY) {
  anthropic = new Anthropic({
    apiKey: process.env.ANTHROPIC_API_KEY
  });
  debugLog('✅ Claude API initialized for intent detection');
}


// Advanced Flow Engine - Multi-step conversations with AI intent and branching

/**
 * Start a flow for a conversation
 */
async function startFlow({ tenant, to, from, flowId, ruleId }) {
  if (!tenant || !tenant.accountId || !tenant.to) return null;
  if (String(to) !== String(tenant.to)) return null;
  const data = loadData();
  const flow = getFlowInData(data, tenant.accountId, flowId);

  if (!flow || !flow.enabled || String(flow.accountId || '') !== String(tenant.accountId)) {
    debugLog(`Flow ${flowId} not found or disabled`);
    return null;
  }

  return startFlowLegacy({
    tenant,
    to,
    from,
    flowId,
    ruleId,
    route: 'flowEngine.startFlow',
    executeInitialStep: async (conversation) => {
      const firstStep = flow.steps['start'];
      if (firstStep) {
        await executeStep(conversation, flow, firstStep);
      }
    }
  });
}

/**
 * Advance flow based on incoming message
 */
async function advanceFlow({ tenant, to, from, text }) {
  if (!tenant || !tenant.accountId || !tenant.to) return null;
  if (String(to) !== String(tenant.to)) return null;
  const data = loadData();

  return advanceFlowLegacy({
    tenant,
    to,
    from,
    text,
    route: 'flowEngine.advanceFlow',
    advance: async (conversation) => {
      if (String(conversation.accountId || '') !== String(tenant.accountId)) return;
      if (!conversation.flow || conversation.flow.status !== 'active') {
        return;
      }

      const flow = getFlowInData(data, tenant.accountId, conversation.flow.flowId);
      if (!flow || String(flow.accountId || '') !== String(tenant.accountId)) {
        debugLog(`Flow ${conversation.flow.flowId} not found`);
        return;
      }

      const currentStep = flow.steps[conversation.flow.stepId];
      if (!currentStep) {
        debugLog(`Step ${conversation.flow.stepId} not found`);
        return;
      }

      if (currentStep.type === 'wait_for_reply') {
        await handleWaitForReply(conversation, flow, currentStep, text);
      }
    }
  });
}

/**
 * Step: AI Intent (Multi) - Detects ALL matching intents
 */
async function executeAIIntentMulti(conversation, flow, step) {
  const lastMessage = getLastCustomerMessage(conversation);
  if (!lastMessage) {
    debugLog('❌ No customer message to analyze');
    return;
  }

  debugLog(`🎯 Multi-intent detection: "${lastMessage.text}"`);
  const detectedIntents = detectAllIntents(lastMessage.text, step.intents);
  debugLog(`✅ Detected intents:`, detectedIntents);

  conversation.flow.data.intents = detectedIntents;
  conversation.flow.data.primary_intent = detectedIntents[0];

  if (detectedIntents.includes('escalate') && step.onEscalate) {
    await moveToStep(conversation, flow, step.onEscalate);
  } else if (detectedIntents.length === 1 && detectedIntents[0] === 'other' && step.onUnknown) {
    await moveToStep(conversation, flow, step.onUnknown);
  } else if (step.next) {
    await moveToStep(conversation, flow, step.next);
  }
}


/**
 * Step: AI Intent Detection using Claude
 * This is the ONLY step that uses AI
 */
async function executeAIIntentClaude(conversation, flow, step) {
  debugLog(`🤖 Using Claude API for intent detection`);
  
  const lastMessage = getLastCustomerMessage(conversation);
  if (!lastMessage) {
    debugLog('❌ No message to analyze');
    return;
  }

  const userMessage = lastMessage.text;
  debugLog(`📝 Analyzing: "${userMessage}"`);

  // If Claude not configured, fall back to keyword matching
  if (!anthropic) {
    debugLog('⚠️  Claude API not configured, using keyword fallback');
    const fallbackIntents = detectAllIntents(userMessage, step.intents);
    conversation.flow.data.intents = fallbackIntents;
    conversation.flow.data.primary_intent = fallbackIntents[0];

    if (fallbackIntents.includes('escalate') && step.onEscalate) {
      await moveToStep(conversation, flow, step.onEscalate);
    } else if (fallbackIntents.length === 1 && fallbackIntents[0] === 'other' && step.onUnknown) {
      await moveToStep(conversation, flow, step.onUnknown);
    } else if (step.next) {
      await moveToStep(conversation, flow, step.next);
    }
    return;
  }

  try {
    const intentEntries = Object.entries(step?.intents || {}).filter(([k]) => k !== 'other');
    const intentLines = intentEntries.map(([intentId, cfg]) => {
      const kws = Array.isArray(cfg?.keywords) ? cfg.keywords.filter(Boolean).slice(0, 8) : [];
      const hint = kws.length ? ` (keywords: ${kws.join(', ')})` : '';
      return `- ${intentId}${hint}`;
    }).join('\n');
    const domainContext = String(step?.aiContext || 'a local service business').trim();

    const response = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 120,
      temperature: 0,
      messages: [{
        role: 'user',
        content: `You are analyzing customer requests for ${domainContext}.

Customer message: "${userMessage}"

Available service IDs (return ONLY these IDs when applicable):
${intentLines}

Special intent:
- escalate: customer asks for a human, manager, callback, or is frustrated/confused

Instructions:
1. If escalation intent is present, return ["escalate"].
2. Otherwise, return all matching service IDs in a JSON array.
3. If none match, return ["other"].
4. Output JSON array only.

Response:`
      }]
    });

    const aiResponse = response.content[0].text.trim();
    debugLog(`🤖 Claude response: ${aiResponse}`);

    // Parse JSON response
    let intents;
    try {
      intents = JSON.parse(aiResponse);
    } catch (parseErr) {
      // If JSON parse fails, try to extract array from text
      const match = aiResponse.match(/\[.*?\]/);
      if (match) {
        intents = JSON.parse(match[0]);
      } else {
        debugLog('⚠️  Could not parse AI response, using keyword fallback');
        intents = detectAllIntents(userMessage, step.intents);
      }
    }

    debugLog(`✅ Detected intents:`, intents);

    conversation.flow.data.intents = intents;
    conversation.flow.data.primary_intent = intents[0];

    // Route escalation/unknown intents to special steps if defined
    if (intents.includes('escalate') && step.onEscalate) {
      await moveToStep(conversation, flow, step.onEscalate);
      return;
    }
    if (intents.length === 1 && intents[0] === 'other' && step.onUnknown) {
      await moveToStep(conversation, flow, step.onUnknown);
      return;
    }

    if (step.next) {
      await moveToStep(conversation, flow, step.next);
    }

  } catch (err) {
    console.error('❌ Claude API error:', err.message);

    // Fall back to keyword matching
    debugLog('⚠️  Falling back to keyword matching');
    const fallbackIntents = detectAllIntents(userMessage, step.intents);
    conversation.flow.data.intents = fallbackIntents;
    conversation.flow.data.primary_intent = fallbackIntents[0];

    if (fallbackIntents.includes('escalate') && step.onEscalate) {
      await moveToStep(conversation, flow, step.onEscalate);
    } else if (fallbackIntents.length === 1 && fallbackIntents[0] === 'other' && step.onUnknown) {
      await moveToStep(conversation, flow, step.onUnknown);
    } else if (step.next) {
      await moveToStep(conversation, flow, step.next);
    }
  }
}

// Keywords that indicate the customer wants to talk to a human
const ESCALATION_KEYWORDS = [
  'speak to', 'talk to', 'real person', 'human', 'representative',
  'manager', 'employee', 'agent', 'someone', 'operator', 'owner',
  'call me', 'phone call', 'stop', 'quit', 'unsubscribe',
  'leave me alone', 'stop texting', 'not a bot', 'real human',
  'actual person', 'live person', 'frustrated', 'this is dumb',
  'wtf', 'what the', 'are you a bot', 'is this a bot',
  'i don\'t understand', 'doesn\'t make sense', 'makes no sense'
];

/**
 * Detect ALL intents in a message (fallback method)
 */
function detectAllIntents(text, intents) {
  const lowerText = text.toLowerCase();

  // Check for escalation first (highest priority)
  for (const phrase of ESCALATION_KEYWORDS) {
    if (lowerText.includes(phrase)) {
      return ['escalate'];
    }
  }

  const detected = [];

  for (const [intentName, intentConfig] of Object.entries(intents)) {
    if (intentName === 'other') continue;

    const keywords = intentConfig.keywords || [];
    for (const keyword of keywords) {
      if (lowerText.includes(keyword.toLowerCase())) {
        detected.push(intentName);
        break;
      }
    }
  }

  return detected.length > 0 ? detected : ['other'];
}
/**
 * Execute a step
 */
async function executeStep(conversation, flow, step) {
  switch (step.type) {
    case 'send_message':
      return await executeSendMessage(conversation, flow, step);
    case 'wait_for_reply':
      return executeWaitForReply(conversation, flow, step);
    case 'ai_intent':
      return await executeAIIntent(conversation, flow, step);
    case 'ai_intent_claude':
      return await executeAIIntentClaude(conversation, flow, step);
    case 'branch':
      return await executeBranch(conversation, flow, step);
    case 'collect_data':
      return await executeCollectData(conversation, flow, step);
    case 'smart_validate':
      return await executeSmartValidate(conversation, flow, step);
    case 'calculate_pricing':
      return await executeCalculatePricing(conversation, flow, step);
    case 'send_booking_link':
      return await executeSendBookingLink(conversation, flow, step);
    case 'build_services_summary':
      return await executeBuildServicesSummary(conversation, flow, step);
    case 'ask_quote_missing':
      return await executeAskQuoteMissing(conversation, flow, step);
    case 'collect_quote_missing':
      return await executeCollectQuoteMissing(conversation, flow, step);
    case 'validate_vehicle_claude':
      return await executeValidateVehicleClaude(conversation, flow, step);
    case 'end_flow':
      return executeEndFlow(conversation, flow, step);
    default:
      debugLog(`Unknown step type: ${step.type}`);
  }
}

/**
 * Step: Send Message
 */
async function executeSendMessage(conversation, flow, step) {
  let text = replaceVariables(step.text, conversation, flow);
  if (String(step?.trackRevenueEvent || '') === 'quote_shown' && conversation?.flow?.data?.quote_required === true) {
    text += `\n\n${buildQuoteRequiredNotice(conversation)}`;
  }
  const tenant = { accountId: conversation.accountId, to: conversation.to };
  const { sendResult } = await recordOutboundAttempt({
    tenant,
    to: conversation.to,
    from: conversation.from,
    text,
    source: 'flow_engine',
    requireExisting: true,
    meta: {
      auto: true,
      status: 'sent',
      flowStep: conversation.flow.stepId
    }
  });
  if (!sendResult?.ok) return;

  if (step?.trackRevenueEvent) {
    emitQuoteLifecycleEvent(conversation, flow, step, step.trackRevenueEvent);
  }

  // Move to next step
  if (step.next) {
    await moveToStep(conversation, flow, step.next);
  }
}

/**
 * Step: Wait for Reply
 */
function executeWaitForReply(conversation, flow, step) {
  // Just marks conversation as waiting
  // When message comes in, handleWaitForReply processes it
  conversation.flow.waiting = true;
  conversation.flow.waitingSince = Date.now();
}

/**
 * Handle reply when in wait_for_reply step
 */
async function handleWaitForReply(conversation, flow, step, text) {
  // Save data if specified
  if (step.saveAs) {
    conversation.flow.data[step.saveAs] = text;
  }

  conversation.flow.waiting = false;

  // Move to next step
  if (step.next) {
    await moveToStep(conversation, flow, step.next);
  }
}

/**
 * Step: AI Intent Detection
 */
async function executeAIIntent(conversation, flow, step) {
  debugLog(`🎯 executeAIIntent called`);
  debugLog(`📋 Conversation has ${conversation.messages?.length || 0} messages`);

  const lastMessage = getLastCustomerMessage(conversation);
  if (!lastMessage) {
    debugLog('❌ No customer message to analyze intent');
    debugLog('📝 Messages:', JSON.stringify(conversation.messages?.map(m => ({ dir: m.dir, text: m.text })) || []));
    return;
  }

  debugLog(`📨 Last customer message: "${lastMessage.text}"`);
  const detectedIntent = detectIntent(lastMessage.text, step.intents);
  debugLog(`✅ Intent detected: ${detectedIntent}`);
  conversation.flow.data.intent = detectedIntent;

  // Move to next step
  if (step.next) {
    debugLog(`➡️  Moving to next step: ${step.next}`);
    await moveToStep(conversation, flow, step.next);
  }
}

/**
 * Detect intent from text using keyword matching
 */
function detectIntent(text, intents) {
  const lowerText = text.toLowerCase();
  
  for (const [intentName, intentConfig] of Object.entries(intents)) {
    if (intentName === 'other') continue;  // Skip 'other' for now
    
    const keywords = intentConfig.keywords || [];
    for (const keyword of keywords) {
      if (lowerText.includes(keyword.toLowerCase())) {
        return intentName;
      }
    }
  }

  return 'other';  // Default fallback
}

/**
 * Step: Branch
 */
async function executeBranch(conversation, flow, step) {
  for (const condition of step.conditions || []) {
    if (evaluateCondition(condition.if, conversation)) {
      if (step?.trackQuoteAccepted === true && String(condition?.next || '') === 'send_booking') {
        emitQuoteLifecycleEvent(conversation, flow, step, 'quote_accepted', {
          matchedCondition: String(condition.if || '')
        });
      }
      await moveToStep(conversation, flow, condition.next);
      return;
    }
  }

  // No condition matched, use default
  if (step.default) {
    if (step?.trackQuoteAccepted === true && String(step.default || '') === 'send_booking') {
      emitQuoteLifecycleEvent(conversation, flow, step, 'quote_accepted', {
        matchedCondition: 'default'
      });
    }
    await moveToStep(conversation, flow, step.default);
  }
}

/**
 * Evaluate a condition
 */
function evaluateCondition(conditionStr, conversation) {
  // Simple condition parser
  // Example: "primary_intent == ceramic"
  const parts = conditionStr.split(/\s*(==|!=|contains)\s*/);
  if (parts.length < 3) return false;

  const [field, operator, value] = parts;
  const actualValue = conversation.flow.data[field];

  // Handle array values (e.g. intents array from Claude)
  if (Array.isArray(actualValue)) {
    switch (operator) {
      case '==':
        return actualValue.includes(value);
      case '!=':
        return !actualValue.includes(value);
      case 'contains':
        return actualValue.some(v => String(v).toLowerCase().includes(String(value).toLowerCase()));
      default:
        return false;
    }
  }

  switch (operator) {
    case '==':
      return String(actualValue) === String(value);
    case '!=':
      return String(actualValue) !== String(value);
    case 'contains':
      return String(actualValue).toLowerCase().includes(String(value).toLowerCase());
    default:
      return false;
  }
}

/**
 * Step: Collect Data
 */
async function executeCollectData(conversation, flow, step) {
  const lastMessage = getLastCustomerMessage(conversation);
  if (lastMessage && step.saveAs) {
    conversation.flow.data[step.saveAs] = lastMessage.text;
  }

  if (step.next) {
    await moveToStep(conversation, flow, step.next);
  }
}

/**
 * Step: Send Booking Link
 */
async function executeSendBookingLink(conversation, flow, step) {
  const dataForAccount = loadData();
  const account = dataForAccount?.accounts?.[String(conversation?.to || '')] || null;
  const scheduling = ensureSchedulingConfig(account || {});
  if (account) saveDataDebounced(dataForAccount);
  const internalUrl = account ? publicBookingUrlForAccount(account) : '';
  const mode = String(scheduling?.mode || '').toLowerCase();
  const bookingUrl = (mode === 'internal' && internalUrl)
    ? internalUrl
    : (conversation.bookingUrl || scheduling?.url || flow.bookingUrl || "https://calendly.com/your-business");
  const text = `${step.text || "Book your appointment:"}\n${bookingUrl}`;
  const tenant = { accountId: conversation.accountId, to: conversation.to };
  const { sendResult } = await recordOutboundAttempt({
    tenant,
    to: conversation.to,
    from: conversation.from,
    text,
    source: 'flow_engine_booking',
    transactional: true,
    requireExisting: true,
    meta: {
      auto: true,
      status: 'sent',
      flowStep: conversation.flow.stepId
    }
  });
  if (!sendResult?.ok) return;

  if (step.next) {
    await moveToStep(conversation, flow, step.next);
  }
}

/**
 * Step: End Flow
 */
function executeEndFlow(conversation, flow, step) {
  conversation.flow.status = 'completed';
  conversation.flow.completedAt = Date.now();

  if (step.updateStatus) {
    conversation.status = step.updateStatus;
    conversation.lastActivityAt = Date.now();
    conversation.audit = conversation.audit || [];
    conversation.audit.push({
      ts: Date.now(),
      type: 'status_change',
      meta: { status: step.updateStatus }
    });

    if (String(step.updateStatus).toLowerCase() === 'booked' && conversation?.to && conversation?.from && conversation?.accountId) {
      const tenant = {
        accountId: String(conversation.accountId),
        to: String(conversation.to)
      };
      const vehicle = String(conversation?.flow?.data?.vehicle || '').trim();
      const title = vehicle ? `Booked Appointment · ${vehicle}` : 'Booked Appointment';
      pushBookingToConnectedCalendars(tenant, {
        remoteId: `${conversation.to}__${conversation.from}__flow_booked`,
        title,
        start: Date.now(),
        end: Date.now() + (60 * 60 * 1000),
        location: ''
      });
    }
  }

  conversation.audit = conversation.audit || [];
  conversation.audit.push({ 
    ts: Date.now(), 
    type: 'flow_completed', 
    meta: { flowId: conversation.flow.flowId } 
  });
}

/**
 * Move to next step in flow
 */
async function moveToStep(conversation, flow, stepId) {
  const nextStep = flow.steps[stepId];
  if (!nextStep) {
    debugLog(`Step ${stepId} not found`);
    return;
  }

  conversation.flow.stepId = stepId;
  conversation.flow.history = conversation.flow.history || [];
  conversation.flow.history.push(stepId);

  // Execute the new step
  await executeStep(conversation, flow, nextStep);
}

// Service pricing catalog for building multi-service summaries
const SERVICE_CATALOG = {
  full:             { name: 'Full Detail (Interior + Exterior)', price: '$200-300', hoursMin: 3,  hoursMax: 4  },
  interior:         { name: 'Interior Detail',                   price: '$100-150', hoursMin: 2,  hoursMax: 2  },
  exterior:         { name: 'Exterior Wash & Wax',               price: '$80-120',  hoursMin: 1,  hoursMax: 2  },
  ceramic:          { name: 'Ceramic Coating',                    price: '$500-800', hoursMin: 8,  hoursMax: 16 },
  tint:             { name: 'Window Tint',                        price: '$200-400', hoursMin: 2,  hoursMax: 4  },
  headlight:        { name: 'Headlight Restoration (pair)',       price: '$80-160',  hoursMin: 1,  hoursMax: 2  },
  paint_correction: { name: 'Paint Correction',                   price: '$300-600', hoursMin: 4,  hoursMax: 8  },
  ppf:              { name: 'Paint Protection Film (PPF)',         price: '$1200-2000', hoursMin: 8, hoursMax: 16 }
};

const DEFAULT_PAINT_SCOPES = {
  spot: { name: 'Paint Correction (spot/panel scratch)', price: '$120-260', hoursMin: 1, hoursMax: 3 },
  standard: { name: 'Paint Correction (single panel typical)', price: '$220-450', hoursMin: 2, hoursMax: 5 },
  large: { name: 'Paint Correction (multi-panel)', price: '$450-900', hoursMin: 6, hoursMax: 12 }
};

const SCOPE_KEYWORD_HINTS = {
  spot: ['spot', 'small', 'minor', 'single area', 'single spot', 'inch', 'door panel'],
  standard: ['standard', 'typical', 'normal', 'single panel'],
  large: ['large', 'multi', 'multiple panels', 'whole', 'entire', 'severe', 'deep'],
  basic: ['basic', 'quick', 'simple'],
  premium: ['premium', 'deluxe', 'deep clean', 'full glam'],
  partial: ['partial', 'half head', 'touch-up'],
  full: ['full', 'all over', 'full head'],
  one_year: ['1 year', 'one year', 'entry'],
  two_year: ['2 year', 'two year'],
  five_year: ['5 year', 'five year', 'pro', 'premium'],
  light: ['light', 'slight'],
  moderate: ['moderate', 'medium'],
  heavy: ['heavy', 'severe', 'oxidized', 'yellow'],
  front_two: ['front two', '2 front'],
  rear_two: ['rear two', '2 rear', 'rear doors', 'back two'],
  back_window: ['back window', 'rear glass', 'rear windshield'],
  side_set_four: ['four windows', '4 windows', 'all side windows', 'side set'],
  full_sides_plus_back: ['all sides and rear', 'sides plus rear', 'full side plus back'],
  windshield_full: ['full windshield', 'entire windshield'],
  windshield_strip: ['windshield strip', 'brow', 'sun strip'],
  sunroof: ['sunroof', 'moonroof'],
  remove_old_tint: ['remove old tint', 'old tint removal', 'strip tint', 'remove tint'],
  adhesive_cleanup: ['adhesive cleanup', 'glue cleanup', 'glue removal'],
  four_windows: ['four windows', '4 windows', 'full side'],
  full_vehicle: ['full vehicle', 'all windows', 'whole car']
};

const DEFAULT_SERVICE_KEYWORDS = {
  full: ['full detail', 'full service', 'complete detail', 'interior and exterior'],
  interior: ['interior', 'inside', 'vacuum', 'seats', 'carpet'],
  exterior: ['exterior', 'outside', 'wash', 'wax'],
  ceramic: ['ceramic', 'coating'],
  tint: ['tint', 'window tint'],
  headlight: ['headlight', 'headlights', 'foggy lights'],
  paint_correction: ['paint correction', 'scratch', 'swirl', 'buff'],
  ppf: ['ppf', 'paint protection film', 'clear bra']
};

function getPricingConfigForConversation(conversation) {
  try {
    const to = String(conversation?.to || '').trim();
    if (!to) return {};
    const flowId = String(conversation?.flow?.flowId || '').trim();
    const data = loadData();
    const byFlow = data?.accounts?.[to]?.workspace?.pricingByFlow;
    const flowCfg = flowId && byFlow && typeof byFlow === 'object' ? byFlow[flowId] : null;
    if (flowCfg && typeof flowCfg === 'object') return flowCfg;
    const cfg = data?.accounts?.[to]?.workspace?.pricing;
    return cfg && typeof cfg === 'object' ? cfg : {};
  } catch {
    return {};
  }
}

function getServiceCatalogForConversation(conversation) {
  const pricing = getPricingConfigForConversation(conversation);
  const overrides = pricing?.services && typeof pricing.services === 'object' ? pricing.services : {};
  const catalog = { ...SERVICE_CATALOG };
  for (const [id, base] of Object.entries(SERVICE_CATALOG)) {
    const override = overrides?.[id];
    if (override && typeof override === 'object') {
      catalog[id] = { ...base, ...override };
    }
  }
  for (const [id, spec] of Object.entries(overrides)) {
    if (!id || !spec || typeof spec !== 'object') continue;
    if (!catalog[id]) {
      catalog[id] = {
        name: String(spec.name || id),
        price: String(spec.price || '$0-0'),
        hoursMin: Number(spec.hoursMin || 1),
        hoursMax: Number(spec.hoursMax || spec.hoursMin || 1)
      };
    }
  }
  return catalog;
}

function parsePriceRange(priceText) {
  const m = String(priceText || '').match(/\$(\d+)\s*-\s*(\d+)/);
  if (!m) return { min: 0, max: 0 };
  return { min: Number(m[1] || 0), max: Number(m[2] || 0) };
}

function normalizeServiceSpec(raw, fallback = {}) {
  const base = { ...(fallback || {}), ...(raw || {}) };
  if (!base.price) {
    const min = Number(base.priceMin || 0);
    const max = Number(base.priceMax || 0);
    if (min > 0 || max > 0) base.price = `$${min}-${max || min}`;
  }
  return base;
}

/**
 * Format a time range in hours into a human-friendly string
 */
function formatTimeRange(minHours, maxHours) {
  if (maxHours <= 8) {
    // Show in hours
    if (minHours === maxHours) return `${minHours} hours`;
    return `${minHours}-${maxHours} hours`;
  }
  // Convert to days (8 hours = 1 working day)
  const minDays = Math.ceil(minHours / 8);
  const maxDays = Math.ceil(maxHours / 8);
  if (minDays === maxDays) return `${minDays} day${minDays > 1 ? 's' : ''}`;
  return `${minDays}-${maxDays} days`;
}

function detectPaintScope(contextText) {
  const t = String(contextText || '').toLowerCase();
  const hasInches = /(\d{1,2})\s*(inch|inches|in)\b/.test(t);
  const smallHints = ['small', 'minor', 'light scratch', 'spot', 'door panel', 'single area', 'single spot'];
  const largeHints = ['full panel', 'multiple panels', 'multi panel', 'whole side', 'entire side', 'deep scratch', 'severe'];

  if (largeHints.some((k) => t.includes(k))) return 'large';
  if (hasInches || smallHints.some((k) => t.includes(k))) return 'spot';
  return 'standard';
}

function detectScopeByKeywords(contextText, scopeKeys = []) {
  const t = String(contextText || '').toLowerCase();
  if (!scopeKeys.length) return '';
  for (const key of scopeKeys) {
    const hints = SCOPE_KEYWORD_HINTS[key] || [String(key || '').replace(/_/g, ' ')];
    if (hints.some((h) => t.includes(String(h).toLowerCase()))) {
      return key;
    }
  }
  return '';
}

function resolveScopeMapForService(serviceId, pricingConfig = {}) {
  const scoped = pricingConfig?.serviceScopes && typeof pricingConfig.serviceScopes === 'object'
    ? pricingConfig.serviceScopes[serviceId]
    : null;
  if (scoped && typeof scoped === 'object' && Object.keys(scoped).length > 0) {
    return scoped;
  }
  if (serviceId === 'paint_correction') {
    const configuredPaint = pricingConfig?.paintScopes && typeof pricingConfig.paintScopes === 'object'
      ? pricingConfig.paintScopes
      : {};
    return { ...DEFAULT_PAINT_SCOPES, ...configuredPaint };
  }
  return null;
}

function serviceOverride(serviceId, contextText, pricingConfig = {}) {
  const scopeMap = resolveScopeMapForService(serviceId, pricingConfig);
  if (!scopeMap) return null;
  const keys = Object.keys(scopeMap);
  if (!keys.length) return null;
  let detected = detectScopeByKeywords(contextText, keys);
  if (!detected && serviceId === 'paint_correction') {
    detected = detectPaintScope(contextText);
  }
  if (!detected || !scopeMap[detected]) {
    if (scopeMap.standard) detected = 'standard';
    else if (scopeMap.basic) detected = 'basic';
    else detected = keys[0];
  }
  return scopeMap[detected] || null;
}

function toMoneyRange(minValue, maxValue) {
  const min = Math.max(0, Math.round(Number(minValue || 0)));
  const max = Math.max(min, Math.round(Number(maxValue || min)));
  return `$${min}-${max}`;
}

function toNumberOr(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function tintPricingRules(pricingConfig = {}) {
  const cfg = pricingConfig?.tintReplacementRules && typeof pricingConfig.tintReplacementRules === 'object'
    ? pricingConfig.tintReplacementRules
    : {};
  return {
    basePerRearWindow: toNumberOr(cfg.basePerRearWindow, 85),
    filmTypeMultiplier: {
      dyed: toNumberOr(cfg?.filmTypeMultiplier?.dyed, 1),
      carbon: toNumberOr(cfg?.filmTypeMultiplier?.carbon, 1.2),
      ceramic: toNumberOr(cfg?.filmTypeMultiplier?.ceramic, 1.5)
    },
    removalAddOnPerWindow: {
      clean_peel: toNumberOr(cfg?.removalAddOnPerWindow?.clean_peel, 10),
      adhesive_cleanup: toNumberOr(cfg?.removalAddOnPerWindow?.adhesive_cleanup, 30)
    },
    tintPercentAddOnPerWindow: {
      5: toNumberOr(cfg?.tintPercentAddOnPerWindow?.[5], 10),
      15: toNumberOr(cfg?.tintPercentAddOnPerWindow?.[15], 5),
      20: toNumberOr(cfg?.tintPercentAddOnPerWindow?.[20], 0),
      35: toNumberOr(cfg?.tintPercentAddOnPerWindow?.[35], 0)
    },
    defaultRearWindowCount: Math.max(1, toNumberOr(cfg.defaultRearWindowCount, 2)),
    hoursPerRearWindow: Math.max(0.1, toNumberOr(cfg.hoursPerRearWindow, 0.8)),
    removalHoursPerWindow: {
      clean_peel: Math.max(0, toNumberOr(cfg?.removalHoursPerWindow?.clean_peel, 0.1)),
      adhesive_cleanup: Math.max(0, toNumberOr(cfg?.removalHoursPerWindow?.adhesive_cleanup, 0.35))
    }
  };
}

function toLabel(value, fallback = 'unknown') {
  const v = String(value || '').trim();
  return v || fallback;
}

function normalizeTintIntake(raw = {}) {
  const film = String(raw.tint_film_type || '').toLowerCase();
  const filmType = ['dyed', 'carbon', 'ceramic'].includes(film) ? film : 'dyed';
  const removalRaw = String(raw.tint_removal_state || '').toLowerCase();
  const removalState = removalRaw === 'adhesive_cleanup' ? 'adhesive_cleanup' : 'clean_peel';
  const pct = Number(raw.tint_percent);
  const tintPercent = Number.isFinite(pct) ? pct : null;
  const countRaw = Number(raw.tint_rear_windows_count);
  const rearWindowCount = Number.isFinite(countRaw) && countRaw >= 1 ? Math.round(countRaw) : null;
  return { filmType, removalState, tintPercent, rearWindowCount };
}

function computeTintReplacementLineItem(serviceId, intake = {}, pricingConfig = {}, fallbackSpec = {}) {
  if (serviceId !== 'tint') return null;
  const rules = tintPricingRules(pricingConfig);
  const normalized = normalizeTintIntake(intake);
  const rearWindows = normalized.rearWindowCount || rules.defaultRearWindowCount;
  const filmMultiplier = rules.filmTypeMultiplier[normalized.filmType] || 1;
  const removalAddOn = rules.removalAddOnPerWindow[normalized.removalState] || 0;
  const tintPctAddOn = rules.tintPercentAddOnPerWindow[normalized.tintPercent] || 0;

  const perWindow = (rules.basePerRearWindow * filmMultiplier) + removalAddOn + tintPctAddOn;
  const total = perWindow * rearWindows;
  const hours = (rules.hoursPerRearWindow + (rules.removalHoursPerWindow[normalized.removalState] || 0)) * rearWindows;
  const filmLabel = toLabel(normalized.filmType, 'dyed');
  const tintPctLabel = normalized.tintPercent == null ? 'unspecified' : `${normalized.tintPercent}%`;
  const removalLabel = normalized.removalState === 'adhesive_cleanup' ? 'adhesive cleanup' : 'clean peel';

  return {
    name: `Tint Replacement (${rearWindows} rear windows, ${filmLabel}, ${tintPctLabel}, ${removalLabel})`,
    price: toMoneyRange(total, total),
    hoursMin: Number(hours.toFixed(1)),
    hoursMax: Number(hours.toFixed(1)),
    details: {
      rearWindows,
      filmType: normalized.filmType,
      tintPercent: normalized.tintPercent,
      removalState: normalized.removalState
    },
    fallbackSpec
  };
}

function buildEstimateFromServices(intents, contextText = '', options = {}) {
  const catalog = options?.catalog && typeof options.catalog === 'object' ? options.catalog : SERVICE_CATALOG;
  const pricingConfig = options?.pricingConfig && typeof options.pricingConfig === 'object' ? options.pricingConfig : {};
  const intake = options?.intake && typeof options.intake === 'object' ? options.intake : {};
  const services = (Array.isArray(intents) ? intents : []).filter((id) => catalog[id]);
  if (!services.length) {
    return {
      services: [],
      servicesSummary: '',
      totalPrice: '$0-0',
      totalTime: '0 hours'
    };
  }

  let totalMinPrice = 0;
  let totalMaxPrice = 0;
  let totalMinHours = 0;
  let totalMaxHours = 0;

  const lines = services.map((id) => {
    const tintLine = computeTintReplacementLineItem(id, intake, pricingConfig, catalog[id]);
    const override = tintLine || serviceOverride(id, contextText, pricingConfig);
    const svc = normalizeServiceSpec(override || catalog[id], SERVICE_CATALOG[id]);
    const range = parsePriceRange(svc.price);
    totalMinPrice += range.min;
    totalMaxPrice += range.max;
    totalMinHours += Number(svc.hoursMin || 0);
    totalMaxHours += Number(svc.hoursMax || 0);
    return `- ${svc.name}: ${svc.price}`;
  });

  return {
    services,
    servicesSummary: lines.join('\n'),
    totalPrice: `$${totalMinPrice}-${totalMaxPrice}`,
    totalTime: formatTimeRange(totalMinHours, totalMaxHours)
  };
}

function uniqueList(items) {
  const out = [];
  const seen = new Set();
  for (const item of Array.isArray(items) ? items : []) {
    const k = String(item || '').trim();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(k);
  }
  return out;
}

function collectRecentInboundText(conversation, maxItems = 12) {
  const msgs = Array.isArray(conversation?.messages) ? conversation.messages : [];
  const inbound = [];
  for (let i = msgs.length - 1; i >= 0; i -= 1) {
    if (String(msgs[i]?.dir || '') !== 'in') continue;
    const t = String(msgs[i]?.text || '').trim();
    if (!t) continue;
    inbound.push(t);
    if (inbound.length >= maxItems) break;
  }
  return inbound.reverse();
}

function getIntentConfigFromFlow(flow) {
  const steps = flow?.steps && typeof flow.steps === 'object' ? Object.values(flow.steps) : [];
  const intentStep = steps.find((s) => s && (s.type === 'ai_intent_claude' || s.type === 'ai_intent') && s.intents);
  return intentStep?.intents && typeof intentStep.intents === 'object' ? intentStep.intents : {};
}

function inferServicesFromCatalogText(catalog, contextText) {
  const text = String(contextText || '').toLowerCase();
  if (!text) return [];
  const scores = {};
  for (const [id, spec] of Object.entries(catalog || {})) {
    const wordsFromName = String(spec?.name || '')
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((w) => w && w.length >= 4);
    const idWords = String(id || '').toLowerCase().split('_').filter((w) => w && w.length >= 3);
    const seeded = DEFAULT_SERVICE_KEYWORDS[id] || [];
    const keywords = uniqueList([...seeded, ...idWords, ...wordsFromName]);
    let score = 0;
    for (const kw of keywords) {
      if (!kw) continue;
      if (text.includes(kw)) score += (kw.includes(' ') ? 3 : 1);
    }
    if (score > 0) scores[id] = score;
  }
  return Object.entries(scores)
    .sort((a, b) => b[1] - a[1])
    .map(([id]) => id);
}

function inferServicesForConversation(conversation, flow, catalog) {
  const flowData = conversation?.flow?.data || {};
  const explicit = uniqueList(flowData.intents).filter((id) => catalog[id]);
  if (explicit.length) return explicit;

  const intentsConfig = getIntentConfigFromFlow(flow);
  const inboundTexts = collectRecentInboundText(conversation, 12);
  const fromIntents = [];
  for (const msgText of inboundTexts) {
    const detected = detectAllIntents(msgText, intentsConfig)
      .filter((x) => x && x !== 'other' && x !== 'escalate')
      .filter((x) => catalog[x]);
    fromIntents.push(...detected);
  }
  const inferredFromIntents = uniqueList(fromIntents);
  if (inferredFromIntents.length) return inferredFromIntents;

  const contextText = [
    ...inboundTexts,
    String(flowData.scope_details || ''),
    String(flowData.paint_scope || ''),
    String(flowData.vehicle_notes || '')
  ].filter(Boolean).join(' ');
  const fromCatalog = inferServicesFromCatalogText(catalog, contextText).filter((id) => catalog[id]);
  return uniqueList(fromCatalog);
}

function applyEstimateToConversation(conversation, services, estimate, catalog) {
  conversation.flow = conversation.flow || {};
  conversation.flow.data = conversation.flow.data || {};
  conversation.flow.data.intents = services;
  conversation.flow.data.primary_intent = services[0] || conversation.flow.data.primary_intent || '';
  conversation.flow.data.services_summary = estimate.servicesSummary;
  conversation.flow.data.services_list = services;
  conversation.flow.data.total_price = estimate.totalPrice;
  conversation.flow.data.total_time = estimate.totalTime;

  const names = services.map((id) => String(catalog?.[id]?.name || id)).filter(Boolean);
  const summaryCompact = names.join(', ');
  conversation.leadData = conversation.leadData && typeof conversation.leadData === 'object' ? conversation.leadData : {};
  conversation.leadData.services_list = services;
  conversation.leadData.services_summary = estimate.servicesSummary;
  conversation.leadData.intent = services[0] || conversation.leadData.intent || '';
  const intake = conversation?.flow?.data?.intake;
  if (intake && typeof intake === 'object') {
    conversation.leadData.answers_collected = { ...intake };
  }
  if (summaryCompact) {
    conversation.leadData.request = summaryCompact;
    conversation.leadData.service_required = summaryCompact;
  }
}

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

function getGuardrailThreshold(pricingConfig = {}) {
  const raw = Number(pricingConfig?.quoteGuardrails?.minConfidence);
  if (Number.isFinite(raw) && raw >= 0 && raw <= 1) return raw;
  return 0.7;
}

function computeQuoteGuardrails(conversation, services, estimate, pricingConfig = {}, catalog = {}) {
  const intake = getIntakeState(conversation);
  const required = requiredIntakeFields(services, intake);
  const missing = required.filter((field) => intake[field] === undefined || intake[field] === null || intake[field] === '');
  const reasons = [];

  if (!Array.isArray(services) || services.length === 0) {
    reasons.push('no_service_detected');
  }
  if (missing.length > 0) {
    reasons.push(`missing_required_fields:${missing.join(',')}`);
  }

  const unpricedServices = [];
  const unresolvedConfigured = [];
  const scoped = pricingConfig?.serviceScopes && typeof pricingConfig.serviceScopes === 'object'
    ? pricingConfig.serviceScopes
    : {};
  const configuredServices = pricingConfig?.services && typeof pricingConfig.services === 'object'
    ? pricingConfig.services
    : {};
  const enforceConfiguredPricing = pricingConfig?.quoteGuardrails?.requireConfiguredPrice === true;

  for (const id of Array.isArray(services) ? services : []) {
    const spec = normalizeServiceSpec(catalog?.[id] || {}, SERVICE_CATALOG[id] || {});
    const range = parsePriceRange(spec?.price);
    if (!range.min && !range.max) {
      unpricedServices.push(id);
    }
    const hasScoped = scoped?.[id] && typeof scoped[id] === 'object' && Object.keys(scoped[id]).length > 0;
    const hasConfiguredDirect = configuredServices?.[id] && typeof configuredServices[id] === 'object';
    if (enforceConfiguredPricing && !hasConfiguredDirect && !hasScoped && id !== 'tint') {
      unresolvedConfigured.push(id);
    }
  }

  if (unpricedServices.length > 0) {
    reasons.push(`unpriced_services:${unpricedServices.join(',')}`);
  }
  if (unresolvedConfigured.length > 0) {
    reasons.push(`missing_configured_price:${unresolvedConfigured.join(',')}`);
  }

  const recentInbound = collectRecentInboundText(conversation, 6).join(' ').trim();
  const hasScopeSignal = Boolean(
    String(conversation?.flow?.data?.scope_details || '').trim() ||
    String(conversation?.flow?.data?.paint_scope || '').trim() ||
    recentInbound.length > 0
  );
  if (!required.length && !hasScopeSignal && services.length > 0) {
    reasons.push('low_context_for_scope');
  }
  if (requiresPhotoAssessment(services, intake) && String(intake?.photo_assessment_path || '') !== 'photos') {
    reasons.push('photo_or_range_required');
  }
  if (String(intake?.photo_assessment_path || '') === 'range') {
    reasons.push('range_only_quote');
  }
  if (String(intake?.photo_assessment_path || '') === 'photos' && intake?.photo_upload_confirmed !== true) {
    reasons.push('photos_pending');
  }

  let confidence = 0.95;
  if (services.length === 0) confidence -= 0.8;
  if (required.length > 0) {
    confidence -= (missing.length / required.length) * 0.5;
  } else if (!hasScopeSignal && services.length > 0) {
    confidence -= 0.25;
  }
  if (unpricedServices.length > 0) confidence -= 0.45;
  if (unresolvedConfigured.length > 0) confidence -= 0.3;
  if (String(estimate?.totalPrice || '') === '$0-0') confidence -= 0.45;
  if (String(intake?.photo_assessment_path || '') === 'range') confidence -= 0.2;
  if (String(intake?.photo_assessment_path || '') === 'photos' && intake?.photo_upload_confirmed !== true) confidence -= 0.3;
  confidence = clamp01(Number(confidence.toFixed(2)));

  const threshold = getGuardrailThreshold(pricingConfig);
  const quoteRequired = reasons.length > 0 || confidence < threshold;

  return {
    quoteConfidence: confidence,
    quoteRequired,
    reasons,
    threshold,
    missingRequiredFields: missing
  };
}

function applyQuoteGuardrailsToConversation(conversation, guardrails = {}) {
  conversation.flow = conversation.flow || {};
  conversation.flow.data = conversation.flow.data || {};
  conversation.flow.data.quote_confidence = Number(guardrails.quoteConfidence || 0);
  conversation.flow.data.quote_required = guardrails.quoteRequired === true;
  conversation.flow.data.quote_guardrail_reasons = Array.isArray(guardrails.reasons) ? guardrails.reasons : [];
  conversation.flow.data.quote_confidence_threshold = Number(guardrails.threshold || 0);
  conversation.flow.data.quote_missing_required_fields = Array.isArray(guardrails.missingRequiredFields)
    ? guardrails.missingRequiredFields
    : [];
  conversation.leadData = conversation.leadData && typeof conversation.leadData === 'object' ? conversation.leadData : {};
  conversation.leadData.quote_confidence = conversation.flow.data.quote_confidence;
  conversation.leadData.quote_required = conversation.flow.data.quote_required;
  conversation.leadData.quote_guardrail_reasons = [...conversation.flow.data.quote_guardrail_reasons];
  conversation.leadData.quote_missing_required_fields = Array.isArray(conversation.flow.data.quote_missing_required_fields)
    ? [...conversation.flow.data.quote_missing_required_fields]
    : [];
}

function parseEstimatedValueFromPriceRange(value) {
  const text = String(value || '').trim();
  if (!text) return null;
  const normalized = text.replace(/\s+/g, '');
  const rangeMatch = normalized.match(/\$?([\d,]+)-\$?([\d,]+)/);
  if (rangeMatch) {
    const min = Number(String(rangeMatch[1] || '').replace(/,/g, ''));
    const max = Number(String(rangeMatch[2] || '').replace(/,/g, ''));
    if (Number.isFinite(min) && Number.isFinite(max) && min >= 0 && max >= min) {
      return Math.round(((min + max) / 2) * 100);
    }
  }
  const singleMatch = normalized.match(/\$?([\d,]+)/);
  if (singleMatch) {
    const n = Number(String(singleMatch[1] || '').replace(/,/g, ''));
    if (Number.isFinite(n) && n >= 0) return Math.round(n * 100);
  }
  return null;
}

function getQuoteTrackingState(conversation) {
  conversation.flow = conversation.flow || {};
  conversation.flow.data = conversation.flow.data || {};
  conversation.flow.data.quote_tracking = (
    conversation.flow.data.quote_tracking &&
    typeof conversation.flow.data.quote_tracking === 'object'
  ) ? conversation.flow.data.quote_tracking : {};
  const tracking = conversation.flow.data.quote_tracking;
  tracking.events = tracking.events && typeof tracking.events === 'object' ? tracking.events : {};
  tracking.eventTimestamps = tracking.eventTimestamps && typeof tracking.eventTimestamps === 'object'
    ? tracking.eventTimestamps
    : {};
  return tracking;
}

function buildQuoteEventMetadata(conversation, flow, step, extra = {}) {
  const intents = Array.isArray(conversation?.flow?.data?.intents) ? conversation.flow.data.intents : [];
  const servicesList = Array.isArray(conversation?.flow?.data?.services_list)
    ? conversation.flow.data.services_list
    : intents;
  return {
    source: 'flow_engine',
    flowId: String(conversation?.flow?.flowId || flow?.id || ''),
    flowStepId: String(step?.id || conversation?.flow?.stepId || ''),
    convoKey: `${String(conversation?.to || '')}__${String(conversation?.from || '')}`,
    to: String(conversation?.to || ''),
    from: String(conversation?.from || ''),
    primaryIntent: String(conversation?.flow?.data?.primary_intent || ''),
    intents,
    servicesList,
    readyToQuote: Boolean(conversation?.flow?.data?.ready_to_quote),
    quoteRequired: Boolean(conversation?.flow?.data?.quote_required),
    quoteConfidence: Number(conversation?.flow?.data?.quote_confidence || 0),
    quoteGuardrailReasons: Array.isArray(conversation?.flow?.data?.quote_guardrail_reasons)
      ? conversation.flow.data.quote_guardrail_reasons
      : [],
    ...extra
  };
}

function emitQuoteLifecycleEvent(conversation, flow, step, eventType, extraMetadata = {}) {
  const accountId = String(conversation?.accountId || '').trim();
  if (!accountId) return false;
  const type = String(eventType || '').trim();
  if (!type) return false;

  const tracking = getQuoteTrackingState(conversation);
  if (tracking.events[type]) return false;

  try {
    const estimatedValueCents = parseEstimatedValueFromPriceRange(conversation?.flow?.data?.total_price);
    logRevenueEvent(accountId, {
      contactId: conversation?.from ? String(conversation.from) : null,
      revenueEventType: type,
      estimatedValueCents,
      confidence: 0.75,
      status: type === 'quote_accepted' ? 'won' : 'open',
      metadata: buildQuoteEventMetadata(conversation, flow, step, extraMetadata)
    });
    tracking.events[type] = true;
    tracking.eventTimestamps[type] = Date.now();
    return true;
  } catch (err) {
    debugLog(`Quote lifecycle event failed (${type}): ${err?.message || err}`);
    return false;
  }
}

const DETAILING_CORE_SERVICES = new Set(['full', 'interior', 'exterior']);
const DETAILING_REQUIRED_FIELDS = [
  'vehicle_size',
  'condition_level',
  'pet_hair',
  'stains_odor'
];
const DETAILING_TINT_REQUIRED_FIELDS = [
  'tint_rear_windows_count',
  'tint_film_type',
  'tint_percent',
  'tint_removal_state'
];
const PHOTO_REQUIRED_SERVICES = new Set(['paint_correction', 'ppf', 'ceramic']);

const INTAKE_QUESTIONS = {
  vehicle_size: 'What vehicle size is it: sedan, SUV, truck, or van?',
  condition_level: 'How is the condition level: light, medium, or heavy?',
  pet_hair: 'Any pet hair: yes or no?',
  stains_odor: 'Any stains or odor: yes or no?',
  tint_rear_windows_count: 'How many rear windows need tint replacement?',
  tint_film_type: 'What tint film type do you want: dyed, carbon, or ceramic?',
  tint_percent: 'What tint percentage do you want (for example 5, 15, 20, or 35)?',
  tint_removal_state: 'For the old tint, is it a clean peel or adhesive/glue cleanup needed?',
  photo_assessment_path: 'For exact pricing, choose one: upload 2 photos of the condition/affected area, or continue with an estimate range only.',
  photo_upload_confirmed: 'Reply "photos sent" once you upload the photos, or reply "range only" to continue without photos.'
};

function getIntakeState(conversation) {
  conversation.flow = conversation.flow || {};
  conversation.flow.data = conversation.flow.data || {};
  conversation.flow.data.intake = conversation.flow.data.intake && typeof conversation.flow.data.intake === 'object'
    ? conversation.flow.data.intake
    : {};
  return conversation.flow.data.intake;
}

function parseYesNo(text) {
  const t = String(text || '').toLowerCase();
  if (!t) return null;
  if (/\b(yes|y|yeah|yep|true)\b/.test(t)) return true;
  if (/\b(no|n|nope|false)\b/.test(t)) return false;
  return null;
}

function parseTintPercent(text) {
  const m = String(text || '').match(/(?:^|[^0-9])(5|15|20|25|30|35|40|45|50|70)\s*%?/i);
  return m ? Number(m[1]) : null;
}

function parseRearWindowCount(text) {
  const t = String(text || '').toLowerCase();
  const m = t.match(/(\d{1,2})\s*(?:rear\s*)?(?:window|windows)\b/);
  if (m) return Number(m[1]);
  const any = t.match(/\b(\d{1,2})\b/);
  if (any) {
    const n = Number(any[1]);
    if (n >= 1 && n <= 8) return n;
  }
  if (t.includes('two')) return 2;
  if (t.includes('one')) return 1;
  if (t.includes('three')) return 3;
  if (t.includes('four')) return 4;
  return null;
}

function parseFieldValue(field, text) {
  const t = String(text || '').toLowerCase();
  if (!t) return null;
  if (field === 'vehicle_size') {
    if (t.includes('sedan') || t.includes('coupe') || t.includes('hatchback')) return 'sedan';
    if (t.includes('suv')) return 'suv';
    if (t.includes('truck')) return 'truck';
    if (t.includes('van') || t.includes('minivan')) return 'van';
    return null;
  }
  if (field === 'condition_level') {
    if (/\blight\b/.test(t)) return 'light';
    if (/\bmedium\b/.test(t)) return 'medium';
    if (/\bheavy\b/.test(t)) return 'heavy';
    return null;
  }
  if (field === 'pet_hair' || field === 'stains_odor') {
    return parseYesNo(t);
  }
  if (field === 'tint_film_type') {
    if (t.includes('ceramic')) return 'ceramic';
    if (t.includes('carbon')) return 'carbon';
    if (t.includes('dyed')) return 'dyed';
    return null;
  }
  if (field === 'tint_percent') return parseTintPercent(t);
  if (field === 'tint_rear_windows_count') return parseRearWindowCount(t);
  if (field === 'tint_removal_state') {
    if (t.includes('glue') || t.includes('adhesive') || t.includes('residue') || t.includes('sticky')) return 'adhesive_cleanup';
    if (t.includes('clean peel') || t.includes('clean')) return 'clean_peel';
    if (t.includes('ripped') || t.includes('rip')) return 'adhesive_cleanup';
    return null;
  }
  if (field === 'photo_assessment_path') {
    if (/\b(photo|photos|pic|pics|picture|upload|mms)\b/.test(t)) return 'photos';
    if (/\b(range|estimate only|range only|no photo|no photos|without photo|without photos|can'?t upload|cant upload)\b/.test(t)) return 'range';
    return null;
  }
  if (field === 'photo_upload_confirmed') {
    if (/\b(photo sent|photos sent|uploaded|upload done|sent photo|sent photos|done)\b/.test(t)) return true;
    if (/\b(range only|estimate only|skip photos|no photos)\b/.test(t)) return false;
    return parseYesNo(t);
  }
  return null;
}

function extractIntakeAnswers(text) {
  const out = {};
  for (const field of Object.keys(INTAKE_QUESTIONS)) {
    const v = parseFieldValue(field, text);
    if (v !== null && v !== undefined) out[field] = v;
  }
  return out;
}

function requiresPhotoAssessment(services, intake = {}) {
  const ids = new Set(Array.isArray(services) ? services : []);
  if ([...ids].some((id) => PHOTO_REQUIRED_SERVICES.has(id))) return true;
  if (ids.has('tint') && String(intake?.tint_removal_state || '') === 'adhesive_cleanup') return true;
  if (String(intake?.condition_level || '') === 'heavy') return true;
  if (intake?.pet_hair === true && intake?.stains_odor === true) return true;
  return false;
}

function requiredIntakeFields(services, intake = {}) {
  const ids = new Set(Array.isArray(services) ? services : []);
  const required = [];
  const add = (field) => { if (!required.includes(field)) required.push(field); };
  if ([...ids].some((id) => DETAILING_CORE_SERVICES.has(id))) {
    for (const field of DETAILING_REQUIRED_FIELDS) add(field);
  }
  if (ids.has('tint')) {
    for (const field of DETAILING_TINT_REQUIRED_FIELDS) add(field);
  }
  if (requiresPhotoAssessment(services, intake)) {
    add('photo_assessment_path');
    if (String(intake?.photo_assessment_path || '') === 'photos') {
      add('photo_upload_confirmed');
    }
  }
  return required;
}

function refreshQuoteReadiness(conversation, services) {
  const intake = getIntakeState(conversation);
  const required = requiredIntakeFields(services, intake);
  const missing = required.filter((field) => intake[field] === undefined || intake[field] === null || intake[field] === '');
  conversation.flow.data.required_fields = required;
  conversation.flow.data.missing_fields = missing;
  conversation.flow.data.ready_to_quote = missing.length === 0;
  if (missing.length > 0) {
    conversation.flow.data.pending_intake_field = missing[0];
  } else {
    conversation.flow.data.pending_intake_field = '';
  }
  return missing;
}

async function executeAskQuoteMissing(conversation, flow, step) {
  const catalog = getServiceCatalogForConversation(conversation);
  const services = inferServicesForConversation(conversation, flow, catalog);
  if (!services.length) {
    if (step.onUnknown) return moveToStep(conversation, flow, step.onUnknown);
    return;
  }

  const intake = getIntakeState(conversation);
  for (const msg of collectRecentInboundText(conversation, 12)) {
    const parsed = extractIntakeAnswers(msg);
    for (const [field, value] of Object.entries(parsed)) {
      if (intake[field] === undefined || intake[field] === null || intake[field] === '') {
        intake[field] = value;
      }
    }
  }

  const missing = refreshQuoteReadiness(conversation, services);
  if (!missing.length) {
    emitQuoteLifecycleEvent(conversation, flow, step, 'quote_ready', {
      requiredFieldCount: Array.isArray(conversation?.flow?.data?.required_fields) ? conversation.flow.data.required_fields.length : 0
    });
    const readyStep = step.onReady || step.nextReady || step.next;
    if (readyStep) {
      await moveToStep(conversation, flow, readyStep);
    }
    return;
  }

  const nextField = missing[0];
  const prefix = String(step?.prefixText || 'Got it. I just need a few details before I show your exact price.').trim();
  const question = INTAKE_QUESTIONS[nextField] || 'Could you share one more detail for your quote?';
  const text = `${prefix}\n\n${question}`;

  const tenant = { accountId: conversation.accountId, to: conversation.to };
  const { sendResult } = await recordOutboundAttempt({
    tenant,
    to: conversation.to,
    from: conversation.from,
    text,
    source: 'flow_engine_intake_prompt',
    requireExisting: true,
    meta: {
      auto: true,
      status: 'sent',
      flowStep: conversation.flow.stepId
    }
  });
  if (!sendResult?.ok) return;

  if (step.next) {
    await moveToStep(conversation, flow, step.next);
  }
}

async function executeCollectQuoteMissing(conversation, flow, step) {
  const catalog = getServiceCatalogForConversation(conversation);
  const services = inferServicesForConversation(conversation, flow, catalog);
  const intake = getIntakeState(conversation);
  const last = getLastCustomerMessage(conversation);
  const text = String(last?.text || '');

  const pending = String(conversation?.flow?.data?.pending_intake_field || '');
  if (pending) {
    const pendingValue = parseFieldValue(pending, text);
    if (pendingValue !== null && pendingValue !== undefined) {
      intake[pending] = pendingValue;
    }
  }
  const parsed = extractIntakeAnswers(text);
  for (const [field, value] of Object.entries(parsed)) {
    intake[field] = value;
  }

  const missing = refreshQuoteReadiness(conversation, services);
  if (!missing.length) {
    emitQuoteLifecycleEvent(conversation, flow, step, 'quote_ready', {
      requiredFieldCount: Array.isArray(conversation?.flow?.data?.required_fields) ? conversation.flow.data.required_fields.length : 0
    });
    const readyStep = step.onReady || step.nextReady || step.next;
    if (readyStep) {
      await moveToStep(conversation, flow, readyStep);
    }
    return;
  }

  const missingStep = step.onMissing || step.next;
  if (missingStep) {
    await moveToStep(conversation, flow, missingStep);
  }
}

/**
 * Step: Build Services Summary - dynamically lists ALL detected services with pricing
 */
async function executeBuildServicesSummary(conversation, flow, step) {
  const catalog = getServiceCatalogForConversation(conversation);
  const intents = inferServicesForConversation(conversation, flow, catalog);
  debugLog('Building services summary for intents:', intents);
  const pricingConfig = getPricingConfigForConversation(conversation);
  const contextText = [
    ...collectRecentInboundText(conversation, 12),
    String(conversation?.flow?.data?.scope_details || ''),
    String(conversation?.flow?.data?.paint_scope || ''),
    String(getLastCustomerMessage(conversation)?.text || '')
  ].filter(Boolean).join(' ');
  const estimate = buildEstimateFromServices(
    intents,
    contextText,
    { pricingConfig, catalog, intake: getIntakeState(conversation) }
  );
  const services = estimate.services;

  let text;
  const followupPrompt = String(step?.followupPrompt || 'Tell me a bit more so I can tighten this quote.').trim();
  const showEstimateBeforeIntake = step?.showEstimateBeforeIntake !== false;
  if (services.length === 0) {
    if (step.onUnknown) {
      await moveToStep(conversation, flow, step.onUnknown);
      return;
    }
    text = "I'd love to help! Could you let me know which service you're interested in?";
  } else if (!showEstimateBeforeIntake) {
    text = `Great, I can help with that.\n\nDetected services:\n${estimate.servicesSummary}\n\n${followupPrompt}`;
  } else {
    text = `Great, here's what I recommend:\n\n${estimate.servicesSummary}\n\nEstimated total: ${estimate.totalPrice}\nEstimated time: ${estimate.totalTime}\n\n${followupPrompt}`;
  }

  applyEstimateToConversation(conversation, services, estimate, catalog);
  const guardrails = computeQuoteGuardrails(conversation, services, estimate, pricingConfig, catalog);
  applyQuoteGuardrailsToConversation(conversation, guardrails);
  emitQuoteLifecycleEvent(conversation, flow, step, 'quote_started', {
    serviceCount: services.length,
    quoteRequired: guardrails.quoteRequired,
    quoteConfidence: guardrails.quoteConfidence
  });

  const tenant = { accountId: conversation.accountId, to: conversation.to };
  const { sendResult } = await recordOutboundAttempt({
    tenant,
    to: conversation.to,
    from: conversation.from,
    text,
    source: 'flow_engine_summary',
    requireExisting: true,
    meta: {
      auto: true,
      status: 'sent',
      flowStep: conversation.flow.stepId
    }
  });
  if (!sendResult?.ok) return;

  if (step.next) {
    await moveToStep(conversation, flow, step.next);
  }
}

/**
 * Step: Validate Vehicle using Claude - ensures customer provides Year, Make, Model
 */
async function executeValidateVehicleClaude(conversation, flow, step) {
  const lastMessage = getLastCustomerMessage(conversation);
  if (!lastMessage) {
    debugLog('❌ No message to validate');
    return;
  }

  const userText = lastMessage.text;
  debugLog(`🚗 Validating vehicle: "${userText}"`);

  // If Claude not configured, fall back to regex validation
  if (!anthropic) {
    debugLog('⚠️  Claude not configured, using regex vehicle validation');
    const vehiclePattern = /(19|20)\d{2}\s+\w+\s+\w+/i;
    const isValid = vehiclePattern.test(userText);
    if (isValid && step.saveAs) {
      conversation.flow.data[step.saveAs] = userText;
    }
    if (isValid && step.onSuccess) {
      await moveToStep(conversation, flow, step.onSuccess);
    } else if (!isValid && step.onFailure) {
      await moveToStep(conversation, flow, step.onFailure);
    }
    return;
  }

  try {
    const response = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 150,
      temperature: 0,
      messages: [{
        role: 'user',
        content: `The customer was asked what vehicle they have for auto detailing services.
Their response: "${userText}"

Determine if this is a valid vehicle description. We need at least a Year and Make. Model is preferred but not required if clearly implied.

Valid examples: "2020 Ford F-150", "2018 Toyota Camry", "22 Honda Civic", "my 2019 BMW X5", "2015 Chevy Silverado"
Too vague: "Ford", "truck", "my car", "SUV", "Honda", "a blue one"

Respond with ONLY a JSON object (no markdown):
- If valid: {"valid": true, "formatted": "2020 Ford F-150"}
- If too vague: {"valid": false, "reason": "brief friendly reason"}`
      }]
    });

    const aiResponse = response.content[0].text.trim();
    debugLog(`🚗 Claude vehicle response: ${aiResponse}`);

    let result;
    try {
      result = JSON.parse(aiResponse);
    } catch (parseErr) {
      // Try to extract JSON from response
      const match = aiResponse.match(/\{.*\}/s);
      if (match) {
        result = JSON.parse(match[0]);
      } else {
        debugLog('⚠️  Could not parse vehicle validation, using regex fallback');
        const vehiclePattern = /(19|20)\d{2}\s+\w+\s+\w+/i;
        result = { valid: vehiclePattern.test(userText) };
        if (result.valid) result.formatted = userText;
      }
    }

    if (result.valid) {
      debugLog(`✅ Valid vehicle: ${result.formatted}`);
      if (step.saveAs) {
        conversation.flow.data[step.saveAs] = result.formatted || userText;
      }
      if (step.onSuccess) {
        await moveToStep(conversation, flow, step.onSuccess);
      }
    } else {
      debugLog(`❌ Invalid vehicle: ${result.reason || 'too vague'}`);
      if (step.onFailure) {
        await moveToStep(conversation, flow, step.onFailure);
      }
    }

  } catch (err) {
    console.error('❌ Claude vehicle validation error:', err.message);
    // Fall back to regex
    const vehiclePattern = /(19|20)\d{2}\s+\w+\s+\w+/i;
    const isValid = vehiclePattern.test(userText);
    if (isValid && step.saveAs) {
      conversation.flow.data[step.saveAs] = userText;
    }
    if (isValid && step.onSuccess) {
      await moveToStep(conversation, flow, step.onSuccess);
    } else if (!isValid && step.onFailure) {
      await moveToStep(conversation, flow, step.onFailure);
    }
  }
}

/**
 * Get last customer message
 */
function getLastCustomerMessage(conversation) {
  const messages = conversation.messages || [];
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].dir === 'in') {
      return messages[i];
    }
  }
  return null;
}

/**
 * Replace variables in text
 */
function replaceVariables(text, conversation, flow) {
  // Look up business name: account settings > flow template > fallback
  let businessName = flow.businessName || 'Our Business';
  try {
    const data = loadData();
    const to = conversation.to;
    if (to && data.accounts?.[to]?.businessName) {
      businessName = data.accounts[to].businessName;
    }
  } catch {}

  return text
    .replace(/\{business_name\}/g, businessName)
    .replace(/\{customer_name\}/g, conversation.flow?.data?.name || 'there')
    .replace(/\{vehicle\}/g, conversation.flow?.data?.vehicle || 'your vehicle')
    .replace(/\{services_summary\}/g, conversation.flow?.data?.services_summary || 'Service estimate unavailable')
    .replace(/\{total_price\}/g, conversation.flow?.data?.total_price || '$0-0')
    .replace(/\{total_time\}/g, conversation.flow?.data?.total_time || 'TBD')
    .replace(/\{quote_confidence_pct\}/g, String(Math.round(Number(conversation?.flow?.data?.quote_confidence || 0) * 100)))
    .replace(/\{quote_required\}/g, conversation?.flow?.data?.quote_required ? 'yes' : 'no');
}

function buildQuoteRequiredNotice(conversation) {
  const reasons = Array.isArray(conversation?.flow?.data?.quote_guardrail_reasons)
    ? conversation.flow.data.quote_guardrail_reasons
    : [];
  const confidencePct = Math.round(Number(conversation?.flow?.data?.quote_confidence || 0) * 100);
  if (reasons.includes('photos_pending')) {
    return `FINAL QUOTE REQUIRED: We still need your photos for exact pricing. Current estimate confidence: ${confidencePct}%.`;
  }
  if (reasons.includes('range_only_quote')) {
    return `FINAL QUOTE REQUIRED: This is a range estimate only without photos. Final price is confirmed after inspection. Confidence: ${confidencePct}%.`;
  }
  return `FINAL QUOTE REQUIRED: This estimate is not final until inspection/remaining details are confirmed. Confidence: ${confidencePct}%.`;
}

function extractVehicleAndNotes(text) {
  const raw = String(text || '').trim();
  const vehicleMatch = raw.match(/\b(19|20)\d{2}\s+[A-Za-z0-9-]+\s+[A-Za-z0-9-]+(?:\s+[A-Za-z0-9-]+)?/i);
  if (!vehicleMatch) return { vehicle: raw, notes: '' };
  const vehicle = String(vehicleMatch[0] || '').trim();
  const notes = raw.replace(vehicleMatch[0], '').replace(/^[,\s.-]+/, '').trim();
  return { vehicle, notes };
}

/**
 * Step: Smart Validate (stub - uses basic validation for now)
 */
async function executeSmartValidate(conversation, flow, step) {
  debugLog(`🔍 executeSmartValidate: ${step.validationType || 'default'}`);
  const lastMessage = getLastCustomerMessage(conversation);
  if (!lastMessage) {
    debugLog('❌ No message to validate');
    return;
  }

  const validationType = step.validationType || 'default';
  const isValid = validateInput(lastMessage.text, validationType);
  if (isValid && step.saveAs) {
    if (validationType === 'vehicle') {
      const parsed = extractVehicleAndNotes(lastMessage.text);
      conversation.flow.data[step.saveAs] = parsed.vehicle || lastMessage.text;
      if (parsed.notes) {
        conversation.flow.data.vehicle_notes = parsed.notes;
      }
    } else {
      conversation.flow.data[step.saveAs] = lastMessage.text;
    }
  }

  if (isValid && step.onSuccess) {
    await moveToStep(conversation, flow, step.onSuccess);
  } else if (!isValid && step.onFailure) {
    await moveToStep(conversation, flow, step.onFailure);
  } else if (step.next) {
    await moveToStep(conversation, flow, step.next);
  }
}

/**
 * Step: Calculate Pricing (stub - passes through to next step)
 */
async function executeCalculatePricing(conversation, flow, step) {
  debugLog('executeCalculatePricing called');
  const catalog = getServiceCatalogForConversation(conversation);
  const intents = inferServicesForConversation(conversation, flow, catalog);
  const intake = conversation?.flow?.data?.intake && typeof conversation.flow.data.intake === 'object'
    ? conversation.flow.data.intake
    : {};
  const pricingContext = [
    String(conversation?.flow?.data?.vehicle || ''),
    String(conversation?.flow?.data?.paint_scope || ''),
    String(conversation?.flow?.data?.scope_details || ''),
    String(intake.vehicle_size || ''),
    String(intake.condition_level || ''),
    String(intake.tint_film_type || ''),
    intake.tint_percent != null ? `${intake.tint_percent}%` : '',
    intake.tint_rear_windows_count != null ? `${intake.tint_rear_windows_count} rear windows` : '',
    String(intake.tint_removal_state || ''),
    ...collectRecentInboundText(conversation, 12)
  ].join(' ');
  const pricingConfig = getPricingConfigForConversation(conversation);
  const estimate = buildEstimateFromServices(intents, pricingContext, { pricingConfig, catalog, intake });
  applyEstimateToConversation(conversation, estimate.services, estimate, catalog);
  const guardrails = computeQuoteGuardrails(conversation, estimate.services, estimate, pricingConfig, catalog);
  applyQuoteGuardrailsToConversation(conversation, guardrails);
  if (step.next) {
    await moveToStep(conversation, flow, step.next);
  }
}

async function executeValidateInput(conversation, flow, step) {
  debugLog(`🔍 executeValidateInput: ${step.validationType}`);

  const lastMessage = getLastCustomerMessage(conversation);
  if (!lastMessage) {
    debugLog('❌ No message to validate');
    return;
  }

  debugLog(`📝 Validating: "${lastMessage.text}"`);
  const isValid = validateInput(lastMessage.text, step.validationType);
  debugLog(`${isValid ? '✅' : '❌'} Validation ${isValid ? 'passed' : 'failed'}`);

  if (isValid) {
    // Save if specified
    if (step.saveAs) {
      conversation.flow.data[step.saveAs] = lastMessage.text;
      debugLog(`💾 Saved as: ${step.saveAs}`);
    }

    // Move to success path
    if (step.onSuccess) {
      debugLog(`➡️  Moving to: ${step.onSuccess}`);
      await moveToStep(conversation, flow, step.onSuccess);
    }
  } else {
    // Move to failure/retry path
    if (step.onFailure) {
      debugLog(`⚠️  Moving to failure path: ${step.onFailure}`);
      await moveToStep(conversation, flow, step.onFailure);
    }
  }
}

/**
 * Validate input based on type
 */
function validateInput(text, validationType) {
  switch (validationType) {
    case 'vehicle':
      // Check for year (4 digits), make, model
      // Examples: "2020 Ford F-150", "2018 Toyota Camry"
      const vehiclePattern = /(19|20)\d{2}\s+\w+\s+\w+/i;
      return vehiclePattern.test(text);
    
    case 'phone':
      const phonePattern = /\d{10,}/;
      return phonePattern.test(text.replace(/\D/g, ''));
    
    case 'email':
      const emailPattern = /\S+@\S+\.\S+/;
      return emailPattern.test(text);
    
    default:
      return true;
  }
}

module.exports = { 
  startFlow, 
  advanceFlow 
};
