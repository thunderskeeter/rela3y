const DETAILING_MISSED_CALL_FLOW = {
  id: "detailing_missed_call_v1",
  name: "Auto Detailing: Missed Call → Booking",
  accountId: "acct_detailer",
  industry: "auto_detailing",
  trigger: "missed_call",
  enabled: true,
  businessName: "Mike's Detailing",
  bookingUrl: "https://calendly.com/mikes-detailing",
  steps: {
    // STEP 1: Fixed message (100% system)
    start: {
      type: "send_message",
      text: "Hi! We missed your call at {business_name}. What can I help you with today?",
      next: "wait_for_service"
    },
    
    // STEP 2: Wait for reply
    wait_for_service: {
      type: "wait_for_reply",
      next: "detect_intent_ai"
    },
    
    // STEP 3: AI analyzes intent (20% AI - ONLY AI STEP)
    detect_intent_ai: {
      type: "ai_intent_claude",
      intents: {
        interior: { keywords: ["interior", "inside", "vacuum"] },
        exterior: { keywords: ["exterior", "outside", "wash", "wax"] },
        full: { keywords: ["full", "detail", "both", "complete"] },
        ceramic: { keywords: ["ceramic", "coating", "protection"] },
        tint: { keywords: ["tint", "window"] },
        headlight: { keywords: ["headlight", "headlights", "foggy lights", "oxidized lights"] },
        paint_correction: { keywords: ["paint", "scratch", "buff"] },
        ppf: { keywords: ["ppf", "paint protection film", "clear bra"] },
        other: { keywords: [] }
      },
      next: "build_services_summary",
      onEscalate: "escalate_to_human",
      onUnknown: "unknown_request"
    },

    build_services_summary: {
      type: "build_services_summary",
      showEstimateBeforeIntake: false,
      followupPrompt: "I can price this exactly after a few quick details.",
      next: "ask_quote_missing"
    },

    ask_quote_missing: {
      type: "ask_quote_missing",
      prefixText: "I can get you an exact quote. Quick intake:",
      next: "wait_for_quote_details",
      onReady: "route_quote_details",
      onUnknown: "unknown_request"
    },

    wait_for_quote_details: {
      type: "wait_for_reply",
      saveAs: "quote_detail_answer",
      next: "collect_quote_missing"
    },

    collect_quote_missing: {
      type: "collect_quote_missing",
      onMissing: "ask_quote_missing",
      onReady: "route_quote_details"
    },

    // ESCALATION: Customer wants to talk to a real person
    escalate_to_human: {
      type: "send_message",
      text: "No problem at all! We'll notify {business_name} and have them reach out to you directly as soon as possible. Hang tight!",
      next: "end_escalated"
    },

    // UNKNOWN: Bot can't understand the request
    unknown_request: {
      type: "send_message",
      text: "I'm sorry, we're having trouble understanding your request. We'll notify {business_name} and have them reach back out to you ASAP!",
      next: "end_escalated"
    },

    end_escalated: {
      type: "end_flow",
      updateStatus: "needs_callback"
    },
    
    // Legacy vehicle path retained for compatibility (not used in default path)
    ask_vehicle: {
      type: "send_message",
      text: "Great! What vehicle do you have? Please include Year, Make, and Model (e.g., '2020 Ford F-150')",
      next: "wait_for_vehicle"
    },
    
    // STEP 5: Wait for vehicle
    wait_for_vehicle: {
      type: "wait_for_reply",
      next: "validate_vehicle_smart"
    },
    
    // STEP 6: Validate vehicle (100% system)
    validate_vehicle_smart: {
      type: "smart_validate",
      validationType: "vehicle",
      saveAs: "vehicle",
      onSuccess: "route_quote_details",
      onFailure: "ask_vehicle_again"
    },

    route_quote_details: {
      type: "branch",
      conditions: [
        { if: "intents contains paint_correction", next: "ask_paint_scope" }
      ],
      default: "calculate_pricing"
    },

    ask_paint_scope: {
      type: "send_message",
      text: "Got it. For scratch work, is this a small spot, one full panel, or multiple panels? Also about how long/wide is the area?",
      next: "wait_for_paint_scope"
    },

    wait_for_paint_scope: {
      type: "wait_for_reply",
      saveAs: "paint_scope",
      next: "calculate_pricing"
    },
    
    ask_vehicle_again: {
      type: "send_message",
      text: "I need your vehicle's Year, Make, and Model (e.g., '2020 Ford F-150'). What is it?",
      next: "wait_for_vehicle"
    },
    
    // STEP 7: Calculate pricing (100% system)
    calculate_pricing: {
      type: "calculate_pricing",
      next: "show_pricing"
    },
    
    // STEP 8: Show pricing (100% system)
    show_pricing: {
      type: "send_message",
      text: "Perfect. For your {vehicle}, here is a ballpark estimate:\n\n{services_summary}\n\nEstimated total: {total_price}\nEstimated time: {total_time}\n\nIf you want, I can send the booking link and we can confirm final pricing after inspection.",
      trackRevenueEvent: "quote_shown",
      next: "wait_for_approval"
    },
    
    // STEP 9: Wait for approval
    wait_for_approval: {
      type: "wait_for_reply",
      saveAs: "approval",
      next: "check_approval"
    },
    
    // STEP 10: Check approval (100% system)
    check_approval: {
      type: "branch",
      trackQuoteAccepted: true,
      conditions: [
      { if: "approval contains yes", next: "confirm_slot" },
      { if: "approval contains yeah", next: "confirm_slot" },
      { if: "approval contains ok", next: "confirm_slot" },
      { if: "approval contains sure", next: "confirm_slot" },
      { if: "approval contains perfect", next: "confirm_slot" },
      { if: "approval contains works", next: "confirm_slot" },
        { if: "approval contains price", next: "show_pricing" },
        { if: "approval contains pricing", next: "show_pricing" },
        { if: "approval contains cost", next: "show_pricing" },
        { if: "approval contains how much", next: "show_pricing" }
      ],
      default: "handle_objection"
    },
    
    handle_objection: {
      type: "send_message",
      text: "No problem. If you want, I can have {business_name} call you to go over options.",
      next: "end_callback"
    },
    
    // STEP 11: Send booking (100% system)
    confirm_slot: {
      type: "send_message",
      text: "I just verified the schedule and your requested slot looks open. I'll send the link so you can lock it in before someone else grabs it.",
      next: "send_booking"
    },
    send_booking: {
      type: "send_booking_link",
      text: "Awesome! Here's the booking link — it will include the summary of the services we discussed so you don't need to repeat it when you confirm.",
      next: "end"
    },
    
    end: {
      type: "end_flow",
      updateStatus: "booked"
    },
    end_callback: {
      type: "end_flow",
      updateStatus: "needs_callback"
    }
  }
};

// Export templates for seeding + lookup.
// Keep this stable because initFlows expects FLOW_TEMPLATES.<industry> keys.
const FLOW_TEMPLATES = {
  detailing: DETAILING_MISSED_CALL_FLOW,
};

const OUTCOME_PACKS = {
  recover_missed_calls: {
    id: 'recover_missed_calls',
    name: 'Recover Missed Calls',
    description: 'Capture every missed call and move leads toward booking with a friendly, confident assistant.',
    defaultEnabled: true,
    signals: ['missed_call'],
    aiPrompt: 'Missed call lead—send a friendly intro, ask intent, push to booking.',
    tone: 'friendly',
    actionPlan: {
      messageTone: 'friendly_professional',
      followupCadence: { initialDelayMinutes: 15, repeatMinutes: 90, maxFollowups: 2 },
      tags: ['pack:recover_missed_calls', 'signal:missed_call']
    },
    followupCadence: { initialDelayMinutes: 15, repeatMinutes: 90, maxFollowups: 2 },
    metrics: [
      { key: 'signalsCaptured', label: 'Signals captured' },
      { key: 'recoveredValueCents', label: 'Recovered value' },
      { key: 'atRiskValueCents', label: 'At-risk value' }
    ],
    flowTemplateIds: ['detailing_missed_call_v1']
  },
  after_hours_receptionist: {
    id: 'after_hours_receptionist',
    name: 'After-Hours Receptionist',
    description: 'Keeps leads warm outside business hours and schedules follow-up right when doors open.',
    defaultEnabled: false,
    signals: ['after_hours_inquiry', 'missed_call'],
    aiPrompt: 'After-hours reception—acknowledge, set expectation, schedule next open slot.',
    tone: 'professional',
    actionPlan: {
      messageTone: 'calm',
      followupCadence: { initialDelayMinutes: 0, repeatMinutes: 120, maxFollowups: 2 },
      tags: ['pack:after_hours_receptionist', 'quiet_hours']
    },
    followupCadence: { initialDelayMinutes: 5, repeatMinutes: 120, maxFollowups: 2 },
    metrics: [
      { key: 'signalsCaptured', label: 'Signals captured' },
      { key: 'recoveredValueCents', label: 'Recovered value' },
      { key: 'atRiskValueCents', label: 'At-risk value' }
    ],
    flowTemplateIds: ['detailing_missed_call_v1']
  },
  lead_qualification_booking: {
    id: 'lead_qualification_booking',
    name: 'Lead Qualification + Booking',
    description: 'Qualifies intent, gathers details, and nudges leads into the calendar.',
    defaultEnabled: true,
    signals: ['inbound_message', 'form_submit'],
    aiPrompt: 'Qualify the ask quickly, answer objections, and offer a booking link.',
    tone: 'direct',
    actionPlan: {
      messageTone: 'direct',
      followupCadence: { initialDelayMinutes: 30, repeatMinutes: 120, maxFollowups: 3 },
      tags: ['pack:lead_qualification_booking', 'intent:book']
    },
    followupCadence: { initialDelayMinutes: 30, repeatMinutes: 120, maxFollowups: 3 },
    metrics: [
      { key: 'signalsCaptured', label: 'Signals captured' },
      { key: 'recoveredValueCents', label: 'Recovered value' },
      { key: 'atRiskValueCents', label: 'At-risk value' }
    ],
    flowTemplateIds: ['detailing_missed_call_v1']
  },
  review_capture_auto_reply: {
    id: 'review_capture_auto_reply',
    name: 'Review Capture + Auto Reply',
    description: 'Replies to reviews and feedback with empathetic messaging while triaging sentiment.',
    defaultEnabled: false,
    signals: ['inbound_review', 'feedback_received'],
    aiPrompt: 'Review or feedback hit—acknowledge, offer help, and flag if escalation needed.',
    tone: 'empathetic',
    actionPlan: {
      messageTone: 'empathetic',
      followupCadence: { initialDelayMinutes: 10, repeatMinutes: 180, maxFollowups: 1 },
      tags: ['pack:review_capture_auto_reply', 'signal:review']
    },
    followupCadence: { initialDelayMinutes: 10, repeatMinutes: 180, maxFollowups: 1 },
    metrics: [
      { key: 'signalsCaptured', label: 'Signals captured' },
      { key: 'recoveredValueCents', label: 'Recovered value' },
      { key: 'atRiskValueCents', label: 'At-risk value' }
    ],
    flowTemplateIds: ['detailing_missed_call_v1']
  },
  reactivation_campaign: {
    id: 'reactivation_campaign',
    name: 'Reactivation Campaign',
    description: 'Re-engages idle customers with structured reminders and value-focused notes.',
    defaultEnabled: false,
    signals: ['lead_stalled', 'inactive_customer'],
    aiPrompt: 'Stale lead—highlight past value, offer quick re-booking, share incentives.',
    tone: 'friendly',
    actionPlan: {
      messageTone: 'friendly',
      followupCadence: { initialDelayMinutes: 60, repeatMinutes: 240, maxFollowups: 2 },
      tags: ['pack:reactivation_campaign', 'signal:stalled']
    },
    followupCadence: { initialDelayMinutes: 60, repeatMinutes: 240, maxFollowups: 2 },
    metrics: [
      { key: 'signalsCaptured', label: 'Signals captured' },
      { key: 'recoveredValueCents', label: 'Recovered value' },
      { key: 'atRiskValueCents', label: 'At-risk value' }
    ],
    flowTemplateIds: ['detailing_missed_call_v1']
  }
};

module.exports = {
  FLOW_TEMPLATES,
  OUTCOME_PACKS,
  DETAILING_MISSED_CALL_FLOW,
};
