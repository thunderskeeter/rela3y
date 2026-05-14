const express = require('express');
const crypto = require('crypto');
const { loadData, saveDataDebounced, getFlows } = require('../store/dataStore');
const { hashPassword, normalizeRole, normalizeEmail, sanitizeUser, destroySessionsForUser } = require('../utils/auth');
const { generateId } = require('../utils/id');
const { ensureSchedulingConfig, publicBookingUrlForAccount } = require('../services/publicBookingService');
const { z, validateBody, validateParams } = require('../utils/validate');
const {
  getDefaultComplianceConfig,
  runComplianceRetentionPurge,
  validateCompliancePatch
} = require('../services/complianceService');

const accountRouter = express.Router();

const DAY_KEYS = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
const E164_REGEX = /^\+[1-9]\d{1,14}$/;
const HHMM_REGEX = /^([01]\d|2[0-3]):([0-5]\d)$/;
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const LOGO_DATA_URL_REGEX = /^data:(image\/(?:png|jpeg|jpg|webp|gif));base64,([A-Za-z0-9+/=]+)$/i;
const MAX_LOGO_BYTES = 750 * 1024;
const MIN_LOGO_WIDTH = 96;
const MIN_LOGO_HEIGHT = 96;
const MAX_LOGO_WIDTH = 2000;
const MAX_LOGO_HEIGHT = 2000;
const MIN_LOGO_ASPECT = 0.5; // width / height
const MAX_LOGO_ASPECT = 6;   // width / height

const compliancePatchSchema = z.object({}).passthrough();
const noBodySchema = z.object({}).strict().optional().default({});
const workspacePatchSchema = z.object({
  workspace: z.object({}).passthrough().optional(),
  defaults: z.object({}).passthrough().optional()
}).refine((v) => Object.keys(v).length > 0, { message: 'At least one field must be provided' });
const businessNameSchema = z.object({
  businessName: z.string().trim().max(120).optional().default('')
});
const schedulingSchema = z.object({
  scheduling: z.object({
    mode: z.enum(['internal', 'link', 'manual']).optional(),
    url: z.string().trim().url().max(2048).optional().or(z.literal('')),
    label: z.string().trim().max(120).optional(),
    instructions: z.string().trim().max(500).optional(),
    slotIntervalMin: z.coerce.number().int().min(5).max(240).optional(),
    leadTimeMin: z.coerce.number().int().min(0).max(10080).optional(),
    bufferMin: z.coerce.number().int().min(0).max(1440).optional(),
    maxBookingsPerDay: z.coerce.number().int().min(0).max(1000).optional()
  })
});
const bookingSchema = z.object({
  bookingUrl: z.string().trim().url().max(2048).optional().or(z.literal(''))
});
const logoUploadSchema = z.object({
  dataUrl: z.string().trim().min(30).max(1_500_000),
  fileName: z.string().trim().max(160).optional().default('')
});
const billingDetailsPatchSchema = z.object({
  companyName: z.string().trim().max(160).optional(),
  billingEmail: z.string().trim().email().max(254).optional(),
  addressLine1: z.string().trim().max(160).optional(),
  addressLine2: z.string().trim().max(160).optional(),
  city: z.string().trim().max(120).optional(),
  state: z.string().trim().max(120).optional(),
  postalCode: z.string().trim().max(32).optional(),
  country: z.string().trim().max(64).optional(),
  taxId: z.string().trim().max(64).optional()
});
const userIdParamSchema = z.object({
  userId: z.string().trim().min(1).max(80)
});
const resetPasswordSchema = z.object({
  password: z.string().min(8).max(200)
});
const teamSecurityPatchSchema = z.object({
  enforceMfa: z.boolean().optional(),
  sessionTimeout: z.string().trim().min(1).max(16).optional(),
  ipAllowlist: z.array(z.string().trim().min(1).max(64)).optional(),
  allowedDomains: z.array(z.string().trim().min(1).max(120)).optional(),
  allowPersonalApiKeys: z.boolean().optional()
}).refine((value) => Object.keys(value).length > 0, {
  message: 'At least one field must be provided'
});
const createWorkspaceUserSchema = z.object({
  name: z.string().trim().min(1).max(120),
  email: z.string().trim().email().max(254),
  role: z.string().trim().min(1).max(32)
});
const updateWorkspaceUserSchema = z.object({
  role: z.string().trim().min(1).max(32).optional(),
  disabled: z.boolean().optional()
}).refine((value) => Object.keys(value).length > 0, {
  message: 'At least one field must be provided'
});
const inviteCreateSchema = z.object({
  email: z.string().trim().email().max(254),
  role: z.string().trim().min(1).max(32),
  name: z.string().trim().max(120).optional().default(''),
  expiresIn: z.enum(['24h', '72h', '7d']).optional().default('72h')
});
const inviteBulkSchema = z.object({
  emails: z.array(z.string().trim().email().max(254)).min(1).max(200),
  role: z.string().trim().min(1).max(32),
  expiresIn: z.enum(['24h', '72h', '7d']).optional().default('72h')
});
const inviteLinkSchema = z.object({
  role: z.string().trim().min(1).max(32).optional().default('agent'),
  expiresIn: z.enum(['24h', '72h', '7d']).optional().default('72h')
});
const adminPasscodeVerifySchema = z.object({
  passcode: z.string().trim().min(4).max(32)
});
const adminPasscodeSetSchema = z.object({
  newPasscode: z.string().trim().min(4).max(32),
  currentPasscode: z.string().trim().min(4).max(32).optional()
});

function generateTempPassword() {
  const lower = 'abcdefghijkmnpqrstuvwxyz';
  const upper = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
  const digits = '23456789';
  const symbols = '!@#$%&*_-+=';
  const all = `${lower}${upper}${digits}${symbols}`;
  const pick = (chars) => chars[Math.floor(Math.random() * chars.length)];
  const chars = [pick(lower), pick(upper), pick(digits), pick(symbols)];
  while (chars.length < 14) chars.push(pick(all));
  for (let i = chars.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    const t = chars[i];
    chars[i] = chars[j];
    chars[j] = t;
  }
  return chars.join('');
}

function defaultTeamSecuritySettings() {
  return {
    enforceMfa: false,
    sessionTimeout: '8h',
    ipAllowlist: [],
    allowedDomains: [],
    allowPersonalApiKeys: false,
    updatedAt: 0
  };
}

function normalizeTeamSecuritySettings(input) {
  const src = input && typeof input === 'object' ? input : {};
  const allowedTimeouts = new Set(['15m', '30m', '1h', '8h', '24h']);
  const sessionTimeoutRaw = String(src.sessionTimeout || '8h').trim();
  return {
    enforceMfa: src.enforceMfa === true,
    sessionTimeout: allowedTimeouts.has(sessionTimeoutRaw) ? sessionTimeoutRaw : '8h',
    ipAllowlist: Array.isArray(src.ipAllowlist)
      ? [...new Set(src.ipAllowlist.map((x) => String(x || '').trim()).filter(Boolean))]
      : [],
    allowedDomains: Array.isArray(src.allowedDomains)
      ? [...new Set(src.allowedDomains.map((x) => String(x || '').trim().toLowerCase()).filter(Boolean))]
      : [],
    allowPersonalApiKeys: src.allowPersonalApiKeys === true,
    updatedAt: Number(src.updatedAt || 0) || 0
  };
}

function ttlFromInviteExpiry(expiresIn) {
  const key = String(expiresIn || '72h').trim().toLowerCase();
  if (key === '24h') return 24 * 60 * 60 * 1000;
  if (key === '7d') return 7 * 24 * 60 * 60 * 1000;
  return 72 * 60 * 60 * 1000;
}

function tokenHash(rawToken) {
  return crypto.createHash('sha256').update(String(rawToken || ''), 'utf8').digest('hex');
}

function issueInviteToken() {
  return crypto.randomBytes(24).toString('base64url');
}

function hashAdminPasscode(passcode) {
  return crypto.createHash('sha256').update(String(passcode || ''), 'utf8').digest('hex');
}

function isValidAdminPasscode(passcode) {
  return /^\d{4,12}$/.test(String(passcode || '').trim());
}

function safeEqualHex(a, b) {
  const ah = String(a || '').trim();
  const bh = String(b || '').trim();
  if (!ah || !bh) return false;
  try {
    const ab = Buffer.from(ah, 'hex');
    const bb = Buffer.from(bh, 'hex');
    if (ab.length !== bb.length || ab.length === 0) return false;
    return crypto.timingSafeEqual(ab, bb);
  } catch {
    return false;
  }
}

function ensureAdminAccessConfig(account) {
  account.workspace = account.workspace && typeof account.workspace === 'object' ? account.workspace : {};
  account.workspace.adminAccess = account.workspace.adminAccess && typeof account.workspace.adminAccess === 'object'
    ? account.workspace.adminAccess
    : {};
  if (typeof account.workspace.adminAccess.passcodeHash !== 'string') {
    account.workspace.adminAccess.passcodeHash = '';
  }
  account.workspace.adminAccess.updatedAt = Number(account.workspace.adminAccess.updatedAt || 0) || 0;
  account.workspace.adminAccess.updatedByUserId = String(account.workspace.adminAccess.updatedByUserId || '');
  return account.workspace.adminAccess;
}

function ensureWorkspaceInvitations(account) {
  account.workspace = account.workspace && typeof account.workspace === 'object' ? account.workspace : {};
  if (!Array.isArray(account.workspace.invitations)) account.workspace.invitations = [];
  return account.workspace.invitations;
}

function defaultWorkspace(primaryNumber = '+10000000000') {
  return {
    identity: {
      businessName: '',
      businessEmail: '',
      businessPhone: '',
      industry: '',
      logoUrl: ''
    },
    timezone: 'America/New_York',
    settings: {
      avgTicketValueCents: 0,
      tonePreference: 'friendly_professional',
      featureFlags: {
        enableOptimization: false,
        enableAIMessageVariants: false,
        enableMoneyProjections: false
      },
      policies: {
        dailyFollowupCapPerLead: 2,
        minCooldownMinutes: 30,
        quietHours: { startHour: 20, endHour: 8, timezone: 'America/New_York' },
        maxAutomationsPerOpportunityPerDay: 4
      }
    },
    phoneNumbers: [
      { number: primaryNumber, label: 'Primary', isPrimary: true }
    ],
    businessHours: {
      mon: [{ start: '09:00', end: '17:00' }],
      tue: [{ start: '09:00', end: '17:00' }],
      wed: [{ start: '09:00', end: '17:00' }],
      thu: [{ start: '09:00', end: '17:00' }],
      fri: [{ start: '09:00', end: '17:00' }],
      sat: [],
      sun: []
    },
    pricing: {
      services: {
        full: { name: 'Full Detail (Interior + Exterior)', price: '$200-300', hoursMin: 3, hoursMax: 4 },
        interior: { name: 'Interior Detail', price: '$100-150', hoursMin: 2, hoursMax: 2 },
        exterior: { name: 'Exterior Wash & Wax', price: '$80-120', hoursMin: 1, hoursMax: 2 },
        ceramic: { name: 'Ceramic Coating', price: '$500-800', hoursMin: 8, hoursMax: 16 },
        tint: { name: 'Window Tint', price: '$200-400', hoursMin: 2, hoursMax: 4 },
        headlight: { name: 'Headlight Restoration (pair)', price: '$80-160', hoursMin: 1, hoursMax: 2 },
        paint_correction: { name: 'Paint Correction', price: '$300-600', hoursMin: 4, hoursMax: 8 },
        ppf: { name: 'Paint Protection Film (PPF)', price: '$1200-2000', hoursMin: 8, hoursMax: 16 }
      },
      paintScopes: {
        spot: { name: 'Paint Correction (spot/panel scratch)', price: '$120-260', hoursMin: 1, hoursMax: 3 },
        standard: { name: 'Paint Correction (single panel typical)', price: '$220-450', hoursMin: 2, hoursMax: 5 },
        large: { name: 'Paint Correction (multi-panel)', price: '$450-900', hoursMin: 6, hoursMax: 12 }
      },
      serviceScopes: {
        full: {
          basic: { name: 'Full Detail (basic package)', price: '$180-260', hoursMin: 2.5, hoursMax: 3.5 },
          standard: { name: 'Full Detail (standard package)', price: '$240-360', hoursMin: 3.5, hoursMax: 5 },
          premium: { name: 'Full Detail (premium package)', price: '$340-520', hoursMin: 5, hoursMax: 8 }
        },
        interior: {
          light: { name: 'Light (quick refresh)', price: '$90-140', hoursMin: 1.25, hoursMax: 2.25 },
          pet_hair: { name: 'Pet hair removal', price: '$140-230', hoursMin: 2, hoursMax: 3.5 },
          stains_odor: { name: 'Stains / odor treatment', price: '$160-280', hoursMin: 2.25, hoursMax: 4 },
          heavy: { name: 'Heavy soil + deep clean', price: '$220-360', hoursMin: 3, hoursMax: 5.5 }
        },
        exterior: {
          basic: { name: 'Exterior Wash (basic)', price: '$60-95', hoursMin: 0.75, hoursMax: 1.5 },
          standard: { name: 'Exterior Wash & Wax', price: '$90-140', hoursMin: 1, hoursMax: 2 },
          premium: { name: 'Exterior Decon + Protection', price: '$150-260', hoursMin: 2, hoursMax: 4 }
        },
        ceramic: {
          one_year: { name: 'Ceramic Coating (1-year)', price: '$350-550', hoursMin: 5, hoursMax: 8 },
          two_year: { name: 'Ceramic Coating (2-year)', price: '$550-850', hoursMin: 8, hoursMax: 14 },
          five_year: { name: 'Ceramic Coating (5-year)', price: '$900-1600', hoursMin: 14, hoursMax: 24 }
        },
        tint: {
          front_two: { name: 'Front 2 windows', price: '$120-180', hoursMin: 1.25, hoursMax: 2 },
          rear_two: { name: 'Rear 2 windows', price: '$120-180', hoursMin: 1.25, hoursMax: 2 },
          back_window: { name: 'Back windshield (rear glass)', price: '$140-240', hoursMin: 1.5, hoursMax: 2.5 },
          side_set_four: { name: '4 side windows', price: '$220-320', hoursMin: 2.25, hoursMax: 3.5 },
          full_sides_plus_back: { name: 'All sides + rear glass', price: '$320-460', hoursMin: 3, hoursMax: 5 },
          windshield_full: { name: 'Full front windshield', price: '$180-320', hoursMin: 1.5, hoursMax: 3 },
          windshield_strip: { name: 'Windshield brow/strip', price: '$40-90', hoursMin: 0.5, hoursMax: 1 },
          sunroof: { name: 'Sunroof tint', price: '$60-140', hoursMin: 0.75, hoursMax: 1.5 },
          remove_old_tint: { name: 'Old tint removal', price: '$120-260', hoursMin: 1.5, hoursMax: 3.5 },
          adhesive_cleanup: { name: 'Adhesive cleanup / glue removal', price: '$60-180', hoursMin: 0.75, hoursMax: 2.5 }
        },
        headlight: {
          light: { name: 'Headlight Restoration (light oxidation)', price: '$70-120', hoursMin: 0.75, hoursMax: 1.5 },
          moderate: { name: 'Headlight Restoration (moderate oxidation)', price: '$110-180', hoursMin: 1, hoursMax: 2.5 },
          heavy: { name: 'Headlight Restoration (heavy oxidation)', price: '$160-280', hoursMin: 2, hoursMax: 4 }
        },
        paint_correction: {
          spot: { name: 'Paint Correction (spot/panel scratch)', price: '$120-260', hoursMin: 1, hoursMax: 3 },
          standard: { name: 'Paint Correction (single panel typical)', price: '$220-450', hoursMin: 2, hoursMax: 5 },
          large: { name: 'Paint Correction (multi-panel)', price: '$450-900', hoursMin: 6, hoursMax: 12 }
        },
        ppf: {
          partial: { name: 'PPF (partial front)', price: '$600-1100', hoursMin: 5, hoursMax: 9 },
          full: { name: 'PPF (full front)', price: '$1200-2200', hoursMin: 8, hoursMax: 16 },
          full_vehicle: { name: 'PPF (full vehicle)', price: '$3500-7000', hoursMin: 24, hoursMax: 48 }
        }
      }
    },
    pricingByFlow: {
      detailing_missed_call_v1: {
        services: {
          full: { name: 'Full Detail (Interior + Exterior)', price: '$200-300', hoursMin: 3, hoursMax: 4 },
          interior: { name: 'Interior Detail', price: '$100-150', hoursMin: 2, hoursMax: 2 },
          exterior: { name: 'Exterior Wash & Wax', price: '$80-120', hoursMin: 1, hoursMax: 2 },
          ceramic: { name: 'Ceramic Coating', price: '$500-800', hoursMin: 8, hoursMax: 16 },
          tint: { name: 'Window Tint', price: '$200-400', hoursMin: 2, hoursMax: 4 },
          headlight: { name: 'Headlight Restoration (pair)', price: '$80-160', hoursMin: 1, hoursMax: 2 },
          paint_correction: { name: 'Paint Correction', price: '$300-600', hoursMin: 4, hoursMax: 8 },
          ppf: { name: 'Paint Protection Film (PPF)', price: '$1200-2000', hoursMin: 8, hoursMax: 16 }
        },
        serviceScopes: {
          full: {
            basic: { name: 'Full Detail (basic package)', price: '$180-260', hoursMin: 2.5, hoursMax: 3.5 },
            standard: { name: 'Full Detail (standard package)', price: '$240-360', hoursMin: 3.5, hoursMax: 5 },
            premium: { name: 'Full Detail (premium package)', price: '$340-520', hoursMin: 5, hoursMax: 8 }
          },
          interior: {
            light: { name: 'Light (quick refresh)', price: '$90-140', hoursMin: 1.25, hoursMax: 2.25 },
            pet_hair: { name: 'Pet hair removal', price: '$140-230', hoursMin: 2, hoursMax: 3.5 },
            stains_odor: { name: 'Stains / odor treatment', price: '$160-280', hoursMin: 2.25, hoursMax: 4 },
            heavy: { name: 'Heavy soil + deep clean', price: '$220-360', hoursMin: 3, hoursMax: 5.5 }
          },
          exterior: {
            basic: { name: 'Exterior Wash (basic)', price: '$60-95', hoursMin: 0.75, hoursMax: 1.5 },
            standard: { name: 'Exterior Wash & Wax', price: '$90-140', hoursMin: 1, hoursMax: 2 },
            premium: { name: 'Exterior Decon + Protection', price: '$150-260', hoursMin: 2, hoursMax: 4 }
          },
          ceramic: {
            one_year: { name: 'Ceramic Coating (1-year)', price: '$350-550', hoursMin: 5, hoursMax: 8 },
            two_year: { name: 'Ceramic Coating (2-year)', price: '$550-850', hoursMin: 8, hoursMax: 14 },
            five_year: { name: 'Ceramic Coating (5-year)', price: '$900-1600', hoursMin: 14, hoursMax: 24 }
          },
          tint: {
            front_two: { name: 'Front 2 windows', price: '$120-180', hoursMin: 1.25, hoursMax: 2 },
            rear_two: { name: 'Rear 2 windows', price: '$120-180', hoursMin: 1.25, hoursMax: 2 },
            back_window: { name: 'Back windshield (rear glass)', price: '$140-240', hoursMin: 1.5, hoursMax: 2.5 },
            side_set_four: { name: '4 side windows', price: '$220-320', hoursMin: 2.25, hoursMax: 3.5 },
            full_sides_plus_back: { name: 'All sides + rear glass', price: '$320-460', hoursMin: 3, hoursMax: 5 },
            windshield_full: { name: 'Full front windshield', price: '$180-320', hoursMin: 1.5, hoursMax: 3 },
            windshield_strip: { name: 'Windshield brow/strip', price: '$40-90', hoursMin: 0.5, hoursMax: 1 },
            sunroof: { name: 'Sunroof tint', price: '$60-140', hoursMin: 0.75, hoursMax: 1.5 },
            remove_old_tint: { name: 'Old tint removal', price: '$120-260', hoursMin: 1.5, hoursMax: 3.5 },
            adhesive_cleanup: { name: 'Adhesive cleanup / glue removal', price: '$60-180', hoursMin: 0.75, hoursMax: 2.5 }
          },
          headlight: {
            light: { name: 'Headlight Restoration (light oxidation)', price: '$70-120', hoursMin: 0.75, hoursMax: 1.5 },
            moderate: { name: 'Headlight Restoration (moderate oxidation)', price: '$110-180', hoursMin: 1, hoursMax: 2.5 },
            heavy: { name: 'Headlight Restoration (heavy oxidation)', price: '$160-280', hoursMin: 2, hoursMax: 4 }
          },
          paint_correction: {
            spot: { name: 'Paint Correction (spot/panel scratch)', price: '$120-260', hoursMin: 1, hoursMax: 3 },
            standard: { name: 'Paint Correction (single panel typical)', price: '$220-450', hoursMin: 2, hoursMax: 5 },
            large: { name: 'Paint Correction (multi-panel)', price: '$450-900', hoursMin: 6, hoursMax: 12 }
          },
          ppf: {
            partial: { name: 'PPF (partial front)', price: '$600-1100', hoursMin: 5, hoursMax: 9 },
            full: { name: 'PPF (full front)', price: '$1200-2200', hoursMin: 8, hoursMax: 16 },
            full_vehicle: { name: 'PPF (full vehicle)', price: '$3500-7000', hoursMin: 24, hoursMax: 48 }
          }
        }
      },
    }
  };
}

function defaultAccountDefaults() {
  return {
    defaultFlowId: ''
  };
}

function defaultAccountCompliance() {
  return getDefaultComplianceConfig();
}

function defaultAccountBilling() {
  const now = Date.now();
  return {
    provider: 'demo',
    isLive: false,
    plan: {
      key: 'pro',
      name: 'Pro',
      priceMonthly: 129,
      interval: 'month',
      status: 'active',
      trialEndsAt: null,
      nextBillingAt: now + (1000 * 60 * 60 * 24 * 18),
      endsAt: null,
      seats: { used: 4, total: 10 }
    },
    usage: {
      cycleResetsAt: now + (1000 * 60 * 60 * 24 * 18),
      messagesSent: { used: 12480, limit: 20000 },
      automationsRun: { used: 3870, limit: 10000 },
      activeConversations: { used: 196, limit: 500 }
    },
    paymentMethod: {
      brand: 'Visa',
      last4: '4242',
      expMonth: 12,
      expYear: 2027
    },
    details: {
      companyName: '',
      billingEmail: '',
      addressLine1: '',
      addressLine2: '',
      city: '',
      state: '',
      postalCode: '',
      country: 'US',
      taxId: ''
    },
    invoices: [
      { id: 'inv_demo_001', number: 'INV-1001', date: now - (1000 * 60 * 60 * 24 * 3), amount: 12900, status: 'paid', pdfUrl: null },
      { id: 'inv_demo_000', number: 'INV-1000', date: now - (1000 * 60 * 60 * 24 * 33), amount: 12900, status: 'paid', pdfUrl: null },
      { id: 'inv_demo_099', number: 'INV-0999', date: now - (1000 * 60 * 60 * 24 * 62), amount: 12900, status: 'refunded', pdfUrl: null }
    ],
    activity: [
      { id: 'ba_001', ts: now - (1000 * 60 * 40), type: 'invoice_paid', message: 'Invoice INV-1001 paid' },
      { id: 'ba_002', ts: now - (1000 * 60 * 60 * 2), type: 'payment_method_updated', message: 'Payment method updated' },
      { id: 'ba_003', ts: now - (1000 * 60 * 60 * 24), type: 'plan_changed', message: 'Plan changed to Pro' },
      { id: 'ba_004', ts: now - (1000 * 60 * 60 * 24 * 14), type: 'invoice_paid', message: 'Invoice INV-1000 paid' },
      { id: 'ba_005', ts: now - (1000 * 60 * 60 * 24 * 33), type: 'invoice_refunded', message: 'Invoice INV-0999 refunded' }
    ],
    portalUrl: null,
    platformStripeCustomerId: '',
    updatedAt: now
  };
}

function defaultAccountIntegrations() {
  return {
    stripe: {
      enabled: false,
      secretKey: '',
      publishableKey: '',
      webhookSecret: '',
      customerId: '',
      accountId: '',
      accountEmail: '',
      accountDisplayName: '',
      connectedAt: null,
      lastTestedAt: null,
      lastStatus: null,
      lastError: null
    },
    twilio: {
      enabled: false,
      accountSid: '',
      apiKeySid: '',
      apiKeySecret: '',
      messagingServiceSid: '',
      phoneNumber: '',
      webhookAuthToken: '',
      connectedAt: null,
      lastTestedAt: null,
      lastStatus: null,
      lastError: null
    },
    calendarIcs: {
      enabled: false,
      provider: 'other',
      url: '',
      privacyMode: true,
      syncMinutes: 60,
      lastSyncedAt: null,
      lastSyncStatus: null,
      lastSyncError: null,
      importedCountLast: 0,
      lastSyncAttemptAt: null
    },
    calendarProviders: {
      google: {
        enabled: false,
        mode: 'two_way',
        provider: 'google',
        calendarId: 'primary',
        syncMinutes: 15,
        connectedAt: null,
        lastSyncedAt: null,
        lastSyncStatus: null,
        lastSyncError: null,
        importedCountLast: 0,
        pushedCountLast: 0,
        lastSyncAttemptAt: null
      },
      outlook: {
        enabled: false,
        mode: 'two_way',
        provider: 'outlook',
        calendarId: 'primary',
        syncMinutes: 15,
        connectedAt: null,
        lastSyncedAt: null,
        lastSyncStatus: null,
        lastSyncError: null,
        importedCountLast: 0,
        pushedCountLast: 0,
        lastSyncAttemptAt: null
      }
    }
  };
}

function defaultRevenueSettings() {
  return {
    featureFlags: {
      enableOptimization: false,
      enableAIMessageVariants: false,
      enableMoneyProjections: false,
      enableAgentMode: true
    },
    policies: {
      dailyFollowupCapPerLead: 2,
      minCooldownMinutes: 30,
      quietHours: { startHour: 20, endHour: 8, timezone: 'America/New_York' },
      maxAutomationsPerOpportunityPerDay: 4
    },
    playbookOverrides: {},
    outcomePacks: {},
    onboarding: {
      stage: 'welcome',
      completed: false,
      selectedPacks: []
    }
  };
}

function isPlainObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value);
}

function deepMerge(base, patch) {
  if (!isPlainObject(base) || !isPlainObject(patch)) return patch;
  const merged = { ...base };
  for (const [key, value] of Object.entries(patch)) {
    if (isPlainObject(value) && isPlainObject(base[key])) {
      merged[key] = deepMerge(base[key], value);
    } else {
      merged[key] = value;
    }
  }
  return merged;
}

function toMinutes(hhmm) {
  const [h, m] = hhmm.split(':').map(Number);
  return h * 60 + m;
}

function isValidIanaTimezone(timezone) {
  if (!timezone || typeof timezone !== 'string') return false;
  try {
    Intl.DateTimeFormat('en-US', { timeZone: timezone }).format(new Date());
    return true;
  } catch {
    return false;
  }
}

function validateBusinessHours(hours) {
  if (!isPlainObject(hours)) return 'businessHours must be an object';

  for (const day of DAY_KEYS) {
    const slots = hours[day];
    if (!Array.isArray(slots)) return `businessHours.${day} must be an array`;

    const normalized = [];
    for (let i = 0; i < slots.length; i += 1) {
      const slot = slots[i];
      const start = String(slot?.start || '');
      const end = String(slot?.end || '');

      if (!HHMM_REGEX.test(start) || !HHMM_REGEX.test(end)) {
        return `businessHours.${day}[${i}] must use HH:MM`;
      }
      if (toMinutes(start) >= toMinutes(end)) {
        return `businessHours.${day}[${i}] must have start < end`;
      }
      normalized.push({ startMin: toMinutes(start), endMin: toMinutes(end) });
    }

    normalized.sort((a, b) => a.startMin - b.startMin);
    for (let i = 1; i < normalized.length; i += 1) {
      if (normalized[i - 1].endMin > normalized[i].startMin) {
        return `businessHours.${day} has overlapping slots`;
      }
    }
  }

  return null;
}

function validateWorkspaceAndDefaults(data, account, mergedWorkspace, mergedDefaults, workspacePatch = null) {
  const businessName = String(mergedWorkspace?.identity?.businessName || '').trim();
  if (businessName && (businessName.length < 2 || businessName.length > 60)) {
    return 'businessName must be 2-60 characters';
  }
  const businessEmail = String(mergedWorkspace?.identity?.businessEmail || '').trim();
  if (businessEmail && !EMAIL_REGEX.test(businessEmail)) {
    return 'businessEmail must be a valid email address';
  }
  const businessPhone = String(mergedWorkspace?.identity?.businessPhone || '').trim();
  if (businessPhone.length > 40) {
    return 'businessPhone must be 40 characters or fewer';
  }
  if (!isValidIanaTimezone(mergedWorkspace?.timezone)) {
    return 'timezone must be a valid IANA timezone string';
  }

  const hoursErr = validateBusinessHours(mergedWorkspace?.businessHours);
  if (hoursErr) return hoursErr;

  const numbers = Array.isArray(mergedWorkspace?.phoneNumbers) ? mergedWorkspace.phoneNumbers : [];
  if (!numbers.length) return 'phoneNumbers must include at least one number';

  let primaryCount = 0;
  const seen = new Set();
  for (let i = 0; i < numbers.length; i += 1) {
    const num = String(numbers[i]?.number || '').trim();
    if (!E164_REGEX.test(num)) return `phoneNumbers[${i}].number must be E.164 format`;
    if (seen.has(num)) return 'phoneNumbers must be unique';
    seen.add(num);
    if (numbers[i]?.isPrimary === true) primaryCount += 1;
  }
  if (primaryCount !== 1) return 'phoneNumbers must include exactly one primary number';

  const flowId = String(mergedDefaults?.defaultFlowId || '').trim();
  if (flowId) {
    const hasFlow = getFlows(account.accountId).some((f) => String(f.id) === flowId);
    if (!hasFlow) {
      return 'defaultFlowId must exist in flows';
    }
  }

  // Keep legacy top-level fields backward compatible
  account.businessName = businessName;
  return null;
}

function normalizePhoneNumbersForCompatibility(numbers, fallbackNumber) {
  const list = Array.isArray(numbers) ? numbers : [];
  const normalized = list
    .map((n) => ({
      number: String(n?.number || '').trim(),
      label: String(n?.label || '').trim() || 'Number',
      isPrimary: n?.isPrimary === true
    }))
    .filter((n) => n.number);

  if (!normalized.length) {
    return [{ number: fallbackNumber, label: 'Primary', isPrimary: true }];
  }

  if (!normalized.some((n) => n.isPrimary)) {
    normalized[0].isPrimary = true;
  }
  if (normalized.filter((n) => n.isPrimary).length > 1) {
    let foundPrimary = false;
    normalized.forEach((n) => {
      if (n.isPrimary && !foundPrimary) {
        foundPrimary = true;
      } else {
        n.isPrimary = false;
      }
    });
  }

  return normalized;
}

function ensureAccount(data, to) {
  if (!data.accounts) data.accounts = {};
  if (!data.accounts[to]) {
    data.accounts[to] = {
      to,
      businessName: '',
      scheduling: { mode: 'manual', url: '', label: 'Book a time', instructions: '' },
      bookingUrl: '',
      workspace: defaultWorkspace(to),
      defaults: defaultAccountDefaults(),
      compliance: defaultAccountCompliance(),
      billing: defaultAccountBilling(),
      integrations: defaultAccountIntegrations(),
      integrationLogs: [],
      calendarEvents: []
    };
  }

  const account = data.accounts[to];
  if (!account.id) account.id = account.accountId || `acct_${String(to).replace(/[^\d]/g, '') || 'unknown'}`;
  if (!account.accountId) account.accountId = account.id;
  account.workspace = deepMerge(defaultWorkspace(to), account.workspace || {});
  account.settings = deepMerge(defaultRevenueSettings(), account.settings || {});
  account.workspace.settings = deepMerge(defaultWorkspace(to).settings || {}, account.workspace.settings || {});
  ensureSchedulingConfig(account);
  account.defaults = deepMerge(defaultAccountDefaults(), account.defaults || {});
  account.compliance = deepMerge(defaultAccountCompliance(), account.compliance || {});
  account.billing = deepMerge(defaultAccountBilling(), account.billing || {});
  account.integrations = deepMerge(defaultAccountIntegrations(), account.integrations || {});
  if (!Array.isArray(account.integrationLogs)) account.integrationLogs = [];
  if (!Array.isArray(account.calendarEvents)) account.calendarEvents = [];
  account.workspace.phoneNumbers = normalizePhoneNumbersForCompatibility(account.workspace.phoneNumbers, to);

  // Keep legacy businessName and identity.businessName in sync
  if (!account.workspace.identity) account.workspace.identity = {};
  const topLevelName = String(account.businessName || '').trim();
  const identityName = String(account.workspace.identity.businessName || '').trim();
  if (topLevelName && !identityName) {
    account.workspace.identity.businessName = topLevelName;
  } else if (identityName && !topLevelName) {
    account.businessName = identityName;
  }

  return account;
}

function normalizeBillingStatus(status) {
  const v = String(status || '').toLowerCase();
  const allowed = ['active', 'trialing', 'past_due', 'unpaid', 'canceled'];
  return allowed.includes(v) ? v : 'active';
}

function readPngDimensions(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 24) return null;
  const sig = buffer.slice(0, 8);
  const pngSig = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
  if (!sig.equals(pngSig)) return null;
  const width = buffer.readUInt32BE(16);
  const height = buffer.readUInt32BE(20);
  if (!width || !height) return null;
  return { width, height };
}

function readGifDimensions(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 10) return null;
  const header = buffer.slice(0, 6).toString('ascii');
  if (header !== 'GIF87a' && header !== 'GIF89a') return null;
  const width = buffer.readUInt16LE(6);
  const height = buffer.readUInt16LE(8);
  if (!width || !height) return null;
  return { width, height };
}

function readJpegDimensions(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 4) return null;
  if (buffer[0] !== 0xff || buffer[1] !== 0xd8) return null;
  let i = 2;
  while (i + 9 < buffer.length) {
    if (buffer[i] !== 0xff) {
      i += 1;
      continue;
    }
    const marker = buffer[i + 1];
    if (marker === 0xd9 || marker === 0xda) break;
    const len = buffer.readUInt16BE(i + 2);
    if (len < 2 || i + 2 + len > buffer.length) break;
    const isSof =
      marker === 0xc0 || marker === 0xc1 || marker === 0xc2 || marker === 0xc3 ||
      marker === 0xc5 || marker === 0xc6 || marker === 0xc7 ||
      marker === 0xc9 || marker === 0xca || marker === 0xcb ||
      marker === 0xcd || marker === 0xce || marker === 0xcf;
    if (isSof) {
      const height = buffer.readUInt16BE(i + 5);
      const width = buffer.readUInt16BE(i + 7);
      if (!width || !height) return null;
      return { width, height };
    }
    i += 2 + len;
  }
  return null;
}

function readWebpDimensions(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 30) return null;
  if (buffer.slice(0, 4).toString('ascii') !== 'RIFF') return null;
  if (buffer.slice(8, 12).toString('ascii') !== 'WEBP') return null;
  const chunk = buffer.slice(12, 16).toString('ascii');
  if (chunk === 'VP8 ') {
    if (buffer.length < 30) return null;
    const width = buffer.readUInt16LE(26) & 0x3fff;
    const height = buffer.readUInt16LE(28) & 0x3fff;
    if (!width || !height) return null;
    return { width, height };
  }
  if (chunk === 'VP8L') {
    if (buffer.length < 25) return null;
    const b0 = buffer[21];
    const b1 = buffer[22];
    const b2 = buffer[23];
    const b3 = buffer[24];
    const width = 1 + (((b1 & 0x3f) << 8) | b0);
    const height = 1 + (((b3 & 0x0f) << 10) | (b2 << 2) | ((b1 & 0xc0) >> 6));
    if (!width || !height) return null;
    return { width, height };
  }
  if (chunk === 'VP8X') {
    if (buffer.length < 30) return null;
    const width = 1 + buffer.readUIntLE(24, 3);
    const height = 1 + buffer.readUIntLE(27, 3);
    if (!width || !height) return null;
    return { width, height };
  }
  return null;
}

function readLogoDimensionsByMime(buffer, mimeType) {
  const mt = String(mimeType || '').toLowerCase();
  if (mt === 'image/png') return readPngDimensions(buffer);
  if (mt === 'image/gif') return readGifDimensions(buffer);
  if (mt === 'image/jpeg' || mt === 'image/jpg') return readJpegDimensions(buffer);
  if (mt === 'image/webp') return readWebpDimensions(buffer);
  return null;
}

function normalizeInvoiceStatus(status) {
  const v = String(status || '').toLowerCase();
  const allowed = ['paid', 'open', 'past_due', 'refunded'];
  return allowed.includes(v) ? v : 'open';
}

function normalizeBillingDetails(details) {
  const d = details && typeof details === 'object' ? details : {};
  return {
    companyName: String(d.companyName || '').trim(),
    billingEmail: String(d.billingEmail || '').trim(),
    addressLine1: String(d.addressLine1 || '').trim(),
    addressLine2: String(d.addressLine2 || '').trim(),
    city: String(d.city || '').trim(),
    state: String(d.state || '').trim(),
    postalCode: String(d.postalCode || '').trim(),
    country: String(d.country || 'US').trim(),
    taxId: String(d.taxId || '').trim()
  };
}

function canManageWorkspaceUsers(user) {
  const role = normalizeRole(user?.role);
  return role === 'owner' || role === 'admin' || role === 'superadmin';
}

function canAssignWorkspaceRole(actorRole, targetRole) {
  const actor = normalizeRole(actorRole);
  const target = normalizeRole(targetRole);
  if (!actor || !target || target === 'superadmin') return false;
  if (actor === 'superadmin' || actor === 'owner') return true;
  if (actor === 'admin') return target === 'agent' || target === 'readonly';
  return false;
}

function canManageTargetWorkspaceUser(actorRole, targetRole) {
  const actor = normalizeRole(actorRole);
  const target = normalizeRole(targetRole);
  if (!actor || !target || target === 'superadmin') return false;
  if (actor === 'superadmin' || actor === 'owner') return true;
  if (actor === 'admin') return target === 'agent' || target === 'readonly';
  return false;
}

function countEnabledOwnersInAccount(users, accountId) {
  return users.filter((u) => {
    const role = normalizeRole(u?.role);
    if (role !== 'owner') return false;
    if (u?.disabled === true) return false;
    return Array.isArray(u?.accountIds) && u.accountIds.map(String).includes(String(accountId));
  }).length;
}

// Read account settings for a given business number
accountRouter.get('/account', (req, res) => {
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  account.scheduling.publicUrl = publicBookingUrlForAccount(account);
  saveDataDebounced(data);
  res.json({ account });
});

// Patch compliance for a given tenant
accountRouter.patch('/account/compliance', validateBody(compliancePatchSchema), (req, res) => {
  const to = req.tenant.to;

  const patch = req.body || {};

  const data = loadData();
  const account = ensureAccount(data, to);

  const merged = deepMerge(account.compliance || {}, patch);
  const errs = validateCompliancePatch(merged);
  if (errs.length) return res.status(400).json({ error: errs.join('; ') });

  account.compliance = merged;
  saveDataDebounced(data);
  res.json({ ok: true, compliance: account.compliance });
});

// Run compliance retention purge now (single tenant)
accountRouter.post('/account/compliance/purge-now', validateBody(noBodySchema), (req, res) => {
  const to = req.tenant.to;
  const result = runComplianceRetentionPurge({ to, force: true });
  res.json({ ok: true, result });
});

// Patch workspace/defaults for a given business number
accountRouter.patch('/account/workspace', validateBody(workspacePatchSchema), (req, res) => {
  const to = req.tenant.to;

  const patch = req.body || {};

  const data = loadData();
  const account = ensureAccount(data, to);

  const workspacePatch = isPlainObject(patch.workspace) ? patch.workspace : {};
  const defaultsPatch = isPlainObject(patch.defaults) ? patch.defaults : {};

  const mergedWorkspace = deepMerge(account.workspace || {}, workspacePatch);
  const mergedDefaults = deepMerge(account.defaults || {}, defaultsPatch);

  const err = validateWorkspaceAndDefaults(data, account, mergedWorkspace, mergedDefaults, workspacePatch);
  if (err) return res.status(400).json({ error: err });

  account.workspace = mergedWorkspace;
  account.defaults = mergedDefaults;
  account.businessName = String(account.workspace?.identity?.businessName || '').trim();

  saveDataDebounced(data);
  res.json({ ok: true, account });
});

accountRouter.post('/account/logo', validateBody(logoUploadSchema), (req, res) => {
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const dataUrl = String(req?.body?.dataUrl || '').trim();
  const fileName = String(req?.body?.fileName || '').trim().slice(0, 160);
  const match = dataUrl.match(LOGO_DATA_URL_REGEX);
  if (!match) return res.status(400).json({ error: 'Logo must be PNG, JPEG, WEBP, or GIF data URL' });
  const mimeType = String(match[1] || '').toLowerCase();
  const base64 = String(match[2] || '');
  let buffer = null;
  try {
    buffer = Buffer.from(base64, 'base64');
  } catch {
    return res.status(400).json({ error: 'Invalid logo file encoding' });
  }
  if (!buffer || !buffer.length) return res.status(400).json({ error: 'Logo file is empty' });
  if (buffer.length > MAX_LOGO_BYTES) return res.status(400).json({ error: 'Logo file must be 750KB or smaller' });
  const dims = readLogoDimensionsByMime(buffer, mimeType);
  if (!dims?.width || !dims?.height) return res.status(400).json({ error: 'Unable to read image dimensions' });
  if (dims.width < MIN_LOGO_WIDTH || dims.height < MIN_LOGO_HEIGHT) {
    return res.status(400).json({ error: `Logo dimensions must be at least ${MIN_LOGO_WIDTH}x${MIN_LOGO_HEIGHT}` });
  }
  if (dims.width > MAX_LOGO_WIDTH || dims.height > MAX_LOGO_HEIGHT) {
    return res.status(400).json({ error: `Logo dimensions must be <= ${MAX_LOGO_WIDTH}x${MAX_LOGO_HEIGHT}` });
  }
  const aspect = Number(dims.width) / Number(dims.height);
  if (!Number.isFinite(aspect) || aspect < MIN_LOGO_ASPECT || aspect > MAX_LOGO_ASPECT) {
    return res.status(400).json({ error: `Logo aspect ratio must be between ${MIN_LOGO_ASPECT}:1 and ${MAX_LOGO_ASPECT}:1` });
  }
  account.workspace = account.workspace && typeof account.workspace === 'object' ? account.workspace : {};
  account.workspace.identity = account.workspace.identity && typeof account.workspace.identity === 'object'
    ? account.workspace.identity
    : {};
  account.workspace.identity.logoAsset = {
    mimeType,
    dataBase64: buffer.toString('base64'),
    bytes: buffer.length,
    width: dims.width,
    height: dims.height,
    fileName,
    uploadedAt: Date.now()
  };
  account.workspace.identity.logoUrl = `/api/account/logo?to=${encodeURIComponent(String(to))}&v=${Date.now()}`;
  saveDataDebounced(data);
  return res.json({
    ok: true,
    logoUrl: account.workspace.identity.logoUrl,
    bytes: buffer.length,
    mimeType,
    width: dims.width,
    height: dims.height
  });
});

accountRouter.delete('/account/logo', validateBody(noBodySchema), (req, res) => {
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  account.workspace = account.workspace && typeof account.workspace === 'object' ? account.workspace : {};
  account.workspace.identity = account.workspace.identity && typeof account.workspace.identity === 'object'
    ? account.workspace.identity
    : {};
  account.workspace.identity.logoAsset = null;
  account.workspace.identity.logoUrl = '';
  saveDataDebounced(data);
  return res.json({ ok: true });
});

accountRouter.get('/account/logo', (req, res) => {
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const asset = account?.workspace?.identity?.logoAsset;
  const mimeType = String(asset?.mimeType || '').trim().toLowerCase();
  const dataBase64 = String(asset?.dataBase64 || '').trim();
  if (!mimeType || !dataBase64) return res.status(404).json({ error: 'Logo not found' });
  let buffer = null;
  try {
    buffer = Buffer.from(dataBase64, 'base64');
  } catch {
    return res.status(500).json({ error: 'Corrupt logo data' });
  }
  if (!buffer || !buffer.length) return res.status(404).json({ error: 'Logo not found' });
  res.setHeader('Content-Type', mimeType);
  res.setHeader('Content-Length', String(buffer.length));
  res.setHeader('Cache-Control', 'private, max-age=300');
  return res.send(buffer);
});

// Save business name
accountRouter.post('/account/business-name', validateBody(businessNameSchema), (req, res) => {
  const { businessName } = req.body || {};
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  account.businessName = String(businessName || '').trim();
  account.workspace.identity.businessName = account.businessName;

  // Also update the flow template's businessName so {business_name} resolves correctly
  for (const flow of getFlows(account.accountId)) {
    if (account.businessName) flow.businessName = account.businessName;
  }

  saveDataDebounced(data);
  res.json({ ok: true, account });
});

// New scheduling shape (preferred)
accountRouter.post('/account/scheduling', validateBody(schedulingSchema), (req, res) => {
  const { scheduling } = req.body || {};
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const existing = ensureSchedulingConfig(account) || {};
  const modeRaw = String(scheduling?.mode || '').toLowerCase();
  const requestedMode = (modeRaw === 'internal' || modeRaw === 'link' || modeRaw === 'manual') ? modeRaw : existing.mode;
  const normalizedUrl = String(scheduling?.url || '').trim();
  let mode = requestedMode || 'manual';
  if (normalizedUrl) mode = 'link';
  else if (mode === 'link') mode = 'internal';
  account.scheduling = {
    ...existing,
    mode,
    url: normalizedUrl,
    label: scheduling?.label || 'Book a time',
    instructions: scheduling?.instructions || '',
    slotIntervalMin: Number(scheduling?.slotIntervalMin || existing.slotIntervalMin || 30),
    leadTimeMin: Number(scheduling?.leadTimeMin || existing.leadTimeMin || 60),
    bufferMin: Number(scheduling?.bufferMin || existing.bufferMin || 0),
    maxBookingsPerDay: Number(scheduling?.maxBookingsPerDay || existing.maxBookingsPerDay || 0)
  };
  account.scheduling.publicUrl = publicBookingUrlForAccount(account);
  saveDataDebounced(data);
  res.json({ ok: true, account });
});

// Legacy booking url (fallback)
accountRouter.post('/account/booking', validateBody(bookingSchema), (req, res) => {
  const { bookingUrl } = req.body || {};
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  account.bookingUrl = bookingUrl ? String(bookingUrl).trim() : '';
  const existing = ensureSchedulingConfig(account) || {};
  // keep scheduling in sync
  account.scheduling = {
    ...existing,
    mode: account.bookingUrl ? 'link' : 'internal',
    url: account.bookingUrl,
    label: 'Book a time',
    instructions: ''
  };
  account.scheduling.publicUrl = publicBookingUrlForAccount(account);
  saveDataDebounced(data);
  res.json({ ok: true, account });
});

accountRouter.get('/account/admin-access', (req, res) => {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const adminAccess = ensureAdminAccessConfig(account);
  saveDataDebounced(data);
  return res.json({
    ok: true,
    configured: Boolean(adminAccess.passcodeHash),
    updatedAt: Number(adminAccess.updatedAt || 0)
  });
});

accountRouter.post('/account/admin-access/verify', validateBody(adminPasscodeVerifySchema), (req, res) => {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  const passcode = String(req?.body?.passcode || '').trim();
  if (!isValidAdminPasscode(passcode)) {
    return res.status(400).json({ error: 'Passcode must be 4-12 digits' });
  }
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const adminAccess = ensureAdminAccessConfig(account);
  if (!adminAccess.passcodeHash) {
    return res.status(400).json({ error: 'Admin passcode is not configured' });
  }
  const ok = safeEqualHex(hashAdminPasscode(passcode), adminAccess.passcodeHash);
  if (!ok) return res.status(403).json({ error: 'Invalid admin passcode' });
  return res.json({ ok: true, unlocked: true });
});

accountRouter.put('/account/admin-access/passcode', validateBody(adminPasscodeSetSchema), (req, res) => {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  const newPasscode = String(req?.body?.newPasscode || '').trim();
  const currentPasscode = String(req?.body?.currentPasscode || '').trim();
  if (!isValidAdminPasscode(newPasscode)) {
    return res.status(400).json({ error: 'Passcode must be 4-12 digits' });
  }

  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const adminAccess = ensureAdminAccessConfig(account);
  if (adminAccess.passcodeHash) {
    if (!isValidAdminPasscode(currentPasscode)) {
      return res.status(400).json({ error: 'Current passcode is required' });
    }
    const currentOk = safeEqualHex(hashAdminPasscode(currentPasscode), adminAccess.passcodeHash);
    if (!currentOk) return res.status(403).json({ error: 'Current passcode is invalid' });
  }
  adminAccess.passcodeHash = hashAdminPasscode(newPasscode);
  adminAccess.updatedAt = Date.now();
  adminAccess.updatedByUserId = String(req?.user?.id || '');
  account.workspace.adminAccess = adminAccess;
  saveDataDebounced(data);
  return res.json({
    ok: true,
    configured: true,
    updatedAt: adminAccess.updatedAt
  });
});

// Billing summary (tenant scoped)
accountRouter.get('/billing/summary', (req, res) => {
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const billing = account.billing || defaultAccountBilling();

  const plan = billing.plan || {};
  const status = normalizeBillingStatus(plan.status);
  const now = Date.now();
  const daysLeft = status === 'trialing' && plan.trialEndsAt
    ? Math.max(0, Math.ceil((Number(plan.trialEndsAt) - now) / (1000 * 60 * 60 * 24)))
    : null;

  saveDataDebounced(data);
  res.json({
    demoMode: billing.isLive !== true,
    accountId: String(account.accountId || account.id || ''),
    billing: {
      provider: billing.provider || 'demo',
      isLive: billing.isLive === true,
      plan: {
        key: String(plan.key || 'pro'),
        name: String(plan.name || 'Pro'),
        priceMonthly: Number(plan.priceMonthly || 0),
        interval: String(plan.interval || 'month'),
        status,
        trialEndsAt: plan.trialEndsAt ? Number(plan.trialEndsAt) : null,
        trialDaysLeft: daysLeft,
        nextBillingAt: plan.nextBillingAt ? Number(plan.nextBillingAt) : null,
        endsAt: plan.endsAt ? Number(plan.endsAt) : null,
        seats: {
          used: Number(plan?.seats?.used || 0),
          total: Number(plan?.seats?.total || 0)
        }
      },
      usage: billing.usage || {},
      paymentMethod: billing.paymentMethod || null,
      details: normalizeBillingDetails(billing.details),
      activity: Array.isArray(billing.activity) ? billing.activity.slice(0, 5) : [],
      updatedAt: Number(billing.updatedAt || now)
    }
  });
});

// Billing invoices (tenant scoped)
accountRouter.get('/billing/invoices', (req, res) => {
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const billing = account.billing || defaultAccountBilling();
  const invoices = (Array.isArray(billing.invoices) ? billing.invoices : []).map((inv) => ({
    id: String(inv.id || ''),
    number: String(inv.number || ''),
    date: Number(inv.date || Date.now()),
    amount: Number(inv.amount || 0),
    status: normalizeInvoiceStatus(inv.status),
    pdfUrl: inv.pdfUrl ? String(inv.pdfUrl) : null
  }));
  saveDataDebounced(data);
  res.json({
    demoMode: billing.isLive !== true,
    invoices,
    total: invoices.length
  });
});

// Billing portal URL (tenant scoped)
accountRouter.get('/billing/portal', (req, res) => {
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const billing = account.billing || defaultAccountBilling();
  const url = billing.isLive === true && billing.portalUrl ? String(billing.portalUrl) : null;
  saveDataDebounced(data);
  res.json({
    demoMode: billing.isLive !== true,
    url,
    message: url ? 'ok' : 'Billing Portal available after connecting Stripe'
  });
});

// Update billing details (tenant scoped)
accountRouter.patch('/billing/details', validateBody(billingDetailsPatchSchema), (req, res) => {
  const to = req.tenant.to;
  const patch = req.body || {};

  const data = loadData();
  const account = ensureAccount(data, String(to));
  const billing = account.billing || defaultAccountBilling();

  const nextDetails = normalizeBillingDetails({
    ...(billing.details || {}),
    ...patch
  });

  account.billing = {
    ...billing,
    details: nextDetails,
    updatedAt: Date.now(),
    activity: [
      {
        id: `ba_${Date.now()}`,
        ts: Date.now(),
        type: 'settings_changed',
        message: 'Billing details updated'
      },
      ...(Array.isArray(billing.activity) ? billing.activity : [])
    ].slice(0, 20)
  };

  saveDataDebounced(data);
  res.json({
    ok: true,
    demoMode: account.billing.isLive !== true,
    details: account.billing.details,
    updatedAt: account.billing.updatedAt
  });
});

// List users in current workspace account (owner/admin/superadmin only)
accountRouter.get('/account/users', (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });
  const data = loadData();
  const users = Array.isArray(data.users) ? data.users : [];
  const scoped = users
    .filter((u) => {
      const role = normalizeRole(u?.role);
      if (role === 'superadmin') return false;
      return Array.isArray(u?.accountIds) && u.accountIds.map(String).includes(accountId);
    })
    .map((u) => sanitizeUser(u))
    .filter(Boolean);
  return res.json({ ok: true, users: scoped });
});

// Create user in current workspace account (owner/admin/superadmin only)
accountRouter.post('/account/users', validateBody(createWorkspaceUserSchema), (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });

  const email = normalizeEmail(req?.body?.email);
  const name = String(req?.body?.name || '').trim();
  const role = normalizeRole(req?.body?.role);
  if (!email) return res.status(400).json({ error: 'email is required' });
  if (!name) return res.status(400).json({ error: 'name is required' });
  if (!role || role === 'superadmin') return res.status(400).json({ error: 'invalid role' });
  const actorRole = normalizeRole(req?.user?.role);
  if (!canAssignWorkspaceRole(actorRole, role)) {
    return res.status(403).json({ error: 'Insufficient permissions for requested role' });
  }

  const data = loadData();
  if (!Array.isArray(data.users)) data.users = [];
  const users = data.users;
  const existing = users.find((u) => normalizeEmail(u?.email) === email);
  if (existing) {
    return res.status(409).json({ error: 'User with this email already exists' });
  }

  const temporaryPassword = generateTempPassword();
  const nextUser = {
    id: generateId(),
    name,
    email,
    passwordHash: hashPassword(temporaryPassword),
    role,
    accountIds: [accountId],
    createdAt: Date.now(),
    lastLoginAt: null,
    disabled: false
  };
  users.push(nextUser);
  saveDataDebounced(data);
  return res.status(201).json({ ok: true, user: sanitizeUser(nextUser), temporaryPassword });
});

// Update user role/disabled in current workspace account
accountRouter.patch('/account/users/:userId', validateParams(userIdParamSchema), validateBody(updateWorkspaceUserSchema), (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });
  const userId = String(req.params?.userId || '').trim();
  if (!userId) return res.status(400).json({ error: 'userId is required' });

  const data = loadData();
  const users = Array.isArray(data.users) ? data.users : [];
  const target = users.find((u) => String(u?.id || '') === userId);
  if (!target) return res.status(404).json({ error: 'User not found' });
  const belongsToAccount = Array.isArray(target.accountIds) && target.accountIds.map(String).includes(accountId);
  if (!belongsToAccount) return res.status(404).json({ error: 'User not found' });

  const actorRole = normalizeRole(req?.user?.role);
  const targetRole = normalizeRole(target?.role);
  if (!canManageTargetWorkspaceUser(actorRole, targetRole)) {
    return res.status(403).json({ error: 'Insufficient permissions for target user' });
  }

  if (Object.prototype.hasOwnProperty.call(req.body || {}, 'role')) {
    const nextRole = normalizeRole(req?.body?.role);
    if (!nextRole || nextRole === 'superadmin') return res.status(400).json({ error: 'invalid role' });
    if (!canAssignWorkspaceRole(actorRole, nextRole)) {
      return res.status(403).json({ error: 'Insufficient permissions for requested role' });
    }
    if (targetRole === 'owner' && nextRole !== 'owner' && countEnabledOwnersInAccount(users, accountId) <= 1) {
      return res.status(400).json({ error: 'At least one enabled owner is required' });
    }
    target.role = nextRole;
  }

  if (Object.prototype.hasOwnProperty.call(req.body || {}, 'disabled')) {
    const nextDisabled = req?.body?.disabled === true;
    if (nextDisabled && String(target?.id || '') === String(req?.user?.id || '')) {
      return res.status(400).json({ error: 'You cannot disable your own account' });
    }
    if (targetRole === 'owner' && nextDisabled && countEnabledOwnersInAccount(users, accountId) <= 1) {
      return res.status(400).json({ error: 'At least one enabled owner is required' });
    }
    target.disabled = nextDisabled;
    if (nextDisabled) destroySessionsForUser(target.id);
  }

  saveDataDebounced(data);
  return res.json({ ok: true, user: sanitizeUser(target) });
});

// Remove user from current workspace account
accountRouter.delete('/account/users/:userId', validateParams(userIdParamSchema), (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });
  const userId = String(req.params?.userId || '').trim();
  if (!userId) return res.status(400).json({ error: 'userId is required' });
  if (String(userId) === String(req?.user?.id || '')) {
    return res.status(400).json({ error: 'You cannot remove your own account' });
  }

  const data = loadData();
  const users = Array.isArray(data.users) ? data.users : [];
  const idx = users.findIndex((u) => String(u?.id || '') === userId);
  if (idx < 0) return res.status(404).json({ error: 'User not found' });
  const target = users[idx];
  const belongsToAccount = Array.isArray(target.accountIds) && target.accountIds.map(String).includes(accountId);
  if (!belongsToAccount) return res.status(404).json({ error: 'User not found' });

  const actorRole = normalizeRole(req?.user?.role);
  const targetRole = normalizeRole(target?.role);
  if (!canManageTargetWorkspaceUser(actorRole, targetRole)) {
    return res.status(403).json({ error: 'Insufficient permissions for target user' });
  }
  if (targetRole === 'owner' && countEnabledOwnersInAccount(users, accountId) <= 1) {
    return res.status(400).json({ error: 'At least one enabled owner is required' });
  }

  users.splice(idx, 1);
  destroySessionsForUser(userId);
  saveDataDebounced(data);
  return res.json({ ok: true, removedUserId: userId });
});

// Create a one-time invitation token for a specific email
accountRouter.post('/account/invitations', validateBody(inviteCreateSchema), (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });

  const actorRole = normalizeRole(req?.user?.role);
  const role = normalizeRole(req?.body?.role);
  const email = normalizeEmail(req?.body?.email);
  const name = String(req?.body?.name || '').trim();
  const expiresIn = String(req?.body?.expiresIn || '72h');
  if (!role || !canAssignWorkspaceRole(actorRole, role)) return res.status(403).json({ error: 'Insufficient permissions for requested role' });
  if (!email) return res.status(400).json({ error: 'email is required' });

  const data = loadData();
  const users = Array.isArray(data.users) ? data.users : [];
  if (users.some((u) => normalizeEmail(u?.email) === email)) {
    return res.status(409).json({ error: 'User with this email already exists' });
  }
  const account = ensureAccount(data, String(req.tenant.to));
  const invites = ensureWorkspaceInvitations(account);
  const now = Date.now();
  const token = issueInviteToken();
  invites.unshift({
    id: generateId(),
    tokenHash: tokenHash(token),
    accountId,
    email,
    name,
    role,
    status: 'pending',
    createdAt: now,
    expiresAt: now + ttlFromInviteExpiry(expiresIn),
    createdByUserId: String(req?.user?.id || ''),
    acceptedAt: null,
    acceptedUserId: null
  });
  account.workspace.invitations = invites.slice(0, 1000);
  saveDataDebounced(data);
  return res.status(201).json({
    ok: true,
    invitation: {
      email,
      role,
      expiresAt: now + ttlFromInviteExpiry(expiresIn),
      acceptPath: `/?inviteToken=${encodeURIComponent(token)}`
    }
  });
});

// Create invitation tokens in bulk
accountRouter.post('/account/invitations/bulk', validateBody(inviteBulkSchema), (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });
  const actorRole = normalizeRole(req?.user?.role);
  const role = normalizeRole(req?.body?.role);
  if (!role || !canAssignWorkspaceRole(actorRole, role)) return res.status(403).json({ error: 'Insufficient permissions for requested role' });

  const emails = Array.isArray(req?.body?.emails) ? req.body.emails.map((x) => normalizeEmail(x)).filter(Boolean) : [];
  const expiresIn = String(req?.body?.expiresIn || '72h');
  const ttl = ttlFromInviteExpiry(expiresIn);
  const now = Date.now();
  const data = loadData();
  const users = Array.isArray(data.users) ? data.users : [];
  const existingEmails = new Set(users.map((u) => normalizeEmail(u?.email)).filter(Boolean));
  const account = ensureAccount(data, String(req.tenant.to));
  const invites = ensureWorkspaceInvitations(account);
  const created = [];
  const failed = [];
  const seen = new Set();
  for (const email of emails) {
    if (!email || seen.has(email)) continue;
    seen.add(email);
    if (existingEmails.has(email)) {
      failed.push({ email, error: 'already_exists' });
      continue;
    }
    const token = issueInviteToken();
    invites.unshift({
      id: generateId(),
      tokenHash: tokenHash(token),
      accountId,
      email,
      name: '',
      role,
      status: 'pending',
      createdAt: now,
      expiresAt: now + ttl,
      createdByUserId: String(req?.user?.id || ''),
      acceptedAt: null,
      acceptedUserId: null
    });
    created.push({
      email,
      role,
      expiresAt: now + ttl,
      acceptPath: `/?inviteToken=${encodeURIComponent(token)}`
    });
  }
  account.workspace.invitations = invites.slice(0, 1000);
  saveDataDebounced(data);
  return res.status(201).json({ ok: true, created, failed });
});

// Create a shareable one-time invite link (email entered at accept time)
accountRouter.post('/account/invitations/link', validateBody(inviteLinkSchema), (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });
  const actorRole = normalizeRole(req?.user?.role);
  const role = normalizeRole(req?.body?.role);
  if (!role || !canAssignWorkspaceRole(actorRole, role)) return res.status(403).json({ error: 'Insufficient permissions for requested role' });

  const expiresIn = String(req?.body?.expiresIn || '72h');
  const now = Date.now();
  const data = loadData();
  const account = ensureAccount(data, String(req.tenant.to));
  const invites = ensureWorkspaceInvitations(account);
  const token = issueInviteToken();
  invites.unshift({
    id: generateId(),
    tokenHash: tokenHash(token),
    accountId,
    email: '',
    name: '',
    role,
    status: 'pending',
    createdAt: now,
    expiresAt: now + ttlFromInviteExpiry(expiresIn),
    createdByUserId: String(req?.user?.id || ''),
    acceptedAt: null,
    acceptedUserId: null
  });
  account.workspace.invitations = invites.slice(0, 1000);
  saveDataDebounced(data);
  return res.status(201).json({
    ok: true,
    invitation: {
      role,
      expiresAt: now + ttlFromInviteExpiry(expiresIn),
      acceptPath: `/?inviteToken=${encodeURIComponent(token)}`
    }
  });
});

// Team security settings (workspace-scoped)
accountRouter.get('/account/team-security', (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const settings = normalizeTeamSecuritySettings(account?.workspace?.teamSecurity || defaultTeamSecuritySettings());
  account.workspace.teamSecurity = settings;
  saveDataDebounced(data);
  return res.json({ ok: true, settings });
});

accountRouter.patch('/account/team-security', validateBody(teamSecurityPatchSchema), (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const to = req.tenant.to;
  const data = loadData();
  const account = ensureAccount(data, String(to));
  const current = normalizeTeamSecuritySettings(account?.workspace?.teamSecurity || defaultTeamSecuritySettings());
  const patch = req.body || {};
  const merged = normalizeTeamSecuritySettings({
    ...current,
    ...patch,
    updatedAt: Date.now()
  });
  account.workspace.teamSecurity = merged;
  saveDataDebounced(data);
  return res.json({ ok: true, settings: merged });
});

accountRouter.post('/account/team-security/signout-all', validateBody(noBodySchema), (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });
  const actorId = String(req?.user?.id || '').trim();
  const data = loadData();
  const users = Array.isArray(data.users) ? data.users : [];
  let signedOutUsers = 0;
  let revokedSessions = 0;
  for (const user of users) {
    const belongsToAccount = Array.isArray(user?.accountIds) && user.accountIds.map(String).includes(accountId);
    if (!belongsToAccount) continue;
    const userId = String(user?.id || '');
    if (!userId || userId === actorId) continue;
    const removed = destroySessionsForUser(userId);
    if (removed > 0) {
      signedOutUsers += 1;
      revokedSessions += Number(removed);
    }
  }
  saveDataDebounced(data);
  return res.json({ ok: true, signedOutUsers, revokedSessions });
});

// Reset password for a user in current workspace account (owner/admin/superadmin only)
accountRouter.post('/account/users/:userId/reset-password', validateParams(userIdParamSchema), validateBody(resetPasswordSchema), (req, res) => {
  if (!canManageWorkspaceUsers(req.user)) return res.status(403).json({ error: 'Forbidden' });
  const accountId = String(req?.tenant?.accountId || '').trim();
  if (!accountId) return res.status(404).json({ error: 'Not found' });
  const userId = String(req.params?.userId || '').trim();
  const password = String(req?.body?.password || '');
  if (!userId) return res.status(400).json({ error: 'userId is required' });

  const data = loadData();
  const users = Array.isArray(data.users) ? data.users : [];
  const target = users.find((u) => String(u?.id || '') === userId);
  if (!target) return res.status(404).json({ error: 'User not found' });
  const targetRole = normalizeRole(target.role);
  if (targetRole === 'superadmin') return res.status(403).json({ error: 'Cannot reset superadmin password here' });
  const belongsToAccount = Array.isArray(target.accountIds) && target.accountIds.map(String).includes(accountId);
  if (!belongsToAccount) return res.status(404).json({ error: 'User not found' });

  target.passwordHash = hashPassword(password);
  destroySessionsForUser(target.id);
  saveDataDebounced(data);
  return res.json({ ok: true, user: sanitizeUser(target) });
});

module.exports = { accountRouter };
