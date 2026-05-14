const express = require('express');
const { requireRole } = require('../utils/authMiddleware');
const { z, validateBody, validateParams } = require('../utils/validate');
const {
  testCalendarIcsUrl,
  createCalendarOAuthStartForTenant,
  completeCalendarOAuthCallback,
  buildOAuthCallbackHtml,
  connectCalendarIcsForTenant,
  syncCalendarIcsForTenant,
  disconnectCalendarIcsForTenant,
  connectCalendarTwoWayForTenant,
  syncCalendarTwoWayForTenant,
  pushCalendarTwoWayEventForTenant,
  disconnectCalendarTwoWayForTenant,
  getTenantIntegrationSnapshot
} = require('../services/calendarIcsService');
const {
  connectTwilioForTenant,
  testTwilioForTenant,
  disconnectTwilioForTenant,
  getTenantTwilioSnapshot
} = require('../services/twilioIntegrationService');
const {
  connectStripeForTenant,
  testStripeForTenant,
  syncStripeBillingForTenant,
  disconnectStripeForTenant,
  getTenantStripeSnapshot
} = require('../services/stripeIntegrationService');

const integrationsRouter = express.Router();
const publicIntegrationsRouter = express.Router();
const providerSchema = z.object({
  provider: z.enum(['google', 'microsoft', 'outlook'])
});

const stripeConnectSchema = z.object({
  secretKey: z.string().trim().min(10),
  publishableKey: z.string().trim().optional(),
  webhookSecret: z.string().trim().optional(),
  customerId: z.string().trim().optional()
}).passthrough();

const twilioConnectSchema = z.object({
  accountSid: z.string().trim().min(10),
  apiKeySid: z.string().trim().min(10),
  apiKeySecret: z.string().trim().min(10),
  messagingServiceSid: z.string().trim().optional(),
  phoneNumber: z.string().trim().optional(),
  voiceForwardTo: z.string().trim().optional(),
  voiceDialTimeoutSec: z.number().int().min(10).max(60).optional(),
  webhookAuthToken: z.string().trim().optional()
}).passthrough();

const calendarIcsSchema = z.object({
  url: z.string().trim().url(),
  privacyMode: z.boolean().optional(),
  syncMinutes: z.number().int().min(5).max(1440).optional(),
  provider: z.string().trim().max(32).optional(),
  triggerInitialSync: z.boolean().optional()
}).passthrough();

const calendarIcsTestSchema = z.object({
  url: z.string().trim().url(),
  privacyMode: z.boolean().optional()
}).passthrough();

const twoWaySchema = z.object({
  calendarId: z.string().trim().min(1).max(256),
  syncMinutes: z.number().int().min(5).max(1440).optional(),
  accessToken: z.string().trim().min(8).optional(),
  refreshToken: z.string().trim().min(8).optional(),
  triggerInitialSync: z.boolean().optional()
}).passthrough();

const pushSchema = z.object({
  event: z.record(z.any()).optional()
}).passthrough();
const noBodySchema = z.object({}).strict().optional().default({});

integrationsRouter.use(requireRole('owner', 'admin'));

integrationsRouter.get('/integrations', (req, res) => {
  try {
    const tenant = req.tenant;
    const snapshot = getTenantIntegrationSnapshot(tenant);
    const twilio = getTenantTwilioSnapshot(tenant);
    const stripe = getTenantStripeSnapshot(tenant);
    return res.json({ ok: true, ...snapshot, ...twilio, ...stripe });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to load integrations' });
  }
});

integrationsRouter.put('/integrations/stripe', validateBody(stripeConnectSchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const result = await connectStripeForTenant(tenant, req.body || {});
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot, ...getTenantStripeSnapshot(tenant) });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to connect Stripe' });
  }
});

integrationsRouter.post('/integrations/stripe/test', validateBody(noBodySchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const result = await testStripeForTenant(tenant);
    return res.json({ ...result, ...getTenantStripeSnapshot(tenant) });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Stripe test failed' });
  }
});

integrationsRouter.post('/integrations/stripe/sync', validateBody(noBodySchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const result = await syncStripeBillingForTenant(tenant);
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot, ...getTenantStripeSnapshot(tenant) });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Stripe sync failed' });
  }
});

integrationsRouter.delete('/integrations/stripe', validateBody(noBodySchema), (req, res) => {
  try {
    const tenant = req.tenant;
    const result = disconnectStripeForTenant(tenant);
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot, ...getTenantStripeSnapshot(tenant) });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to disconnect Stripe' });
  }
});

integrationsRouter.put('/integrations/twilio', validateBody(twilioConnectSchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const result = await connectTwilioForTenant(tenant, req.body || {});
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot, ...getTenantTwilioSnapshot(tenant) });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to connect Twilio' });
  }
});

integrationsRouter.post('/integrations/twilio/test', validateBody(noBodySchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const result = await testTwilioForTenant(tenant);
    return res.json({ ...result, ...getTenantTwilioSnapshot(tenant) });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Twilio test failed' });
  }
});

integrationsRouter.delete('/integrations/twilio', validateBody(noBodySchema), (req, res) => {
  try {
    const tenant = req.tenant;
    const result = disconnectTwilioForTenant(tenant);
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot, ...getTenantTwilioSnapshot(tenant) });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to disconnect Twilio' });
  }
});

integrationsRouter.post('/integrations/calendar/:provider/oauth/start', validateParams(providerSchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const provider = req.params?.provider;
    const result = createCalendarOAuthStartForTenant(tenant, { provider });
    return res.json(result);
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to start OAuth' });
  }
});

integrationsRouter.post('/integrations/calendar/ics/test', validateBody(calendarIcsTestSchema), async (req, res) => {
  try {
    const { url, privacyMode } = req.body || {};
    const result = await testCalendarIcsUrl({
      url,
      privacyMode: privacyMode !== false
    });
    return res.json(result);
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Calendar test failed' });
  }
});

integrationsRouter.put('/integrations/calendar/ics', validateBody(calendarIcsSchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const { url, privacyMode, syncMinutes, provider, triggerInitialSync } = req.body || {};
    const result = await connectCalendarIcsForTenant(tenant, {
      url,
      privacyMode: privacyMode !== false,
      syncMinutes,
      provider,
      triggerInitialSync: triggerInitialSync !== false
    });
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to save calendar integration' });
  }
});

integrationsRouter.post('/integrations/calendar/ics/sync', validateBody(noBodySchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const result = await syncCalendarIcsForTenant(tenant, { reason: 'manual' });
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Calendar sync failed' });
  }
});

integrationsRouter.delete('/integrations/calendar/ics', validateBody(noBodySchema), (req, res) => {
  try {
    const tenant = req.tenant;
    const result = disconnectCalendarIcsForTenant(tenant);
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to disconnect calendar' });
  }
});

integrationsRouter.put(
  '/integrations/calendar/:provider/two-way',
  validateParams(providerSchema),
  validateBody(twoWaySchema),
  async (req, res) => {
  try {
    const tenant = req.tenant;
    const provider = req.params?.provider;
    const {
      calendarId,
      syncMinutes,
      accessToken,
      refreshToken,
      triggerInitialSync
    } = req.body || {};
    const result = await connectCalendarTwoWayForTenant(tenant, {
      provider,
      calendarId,
      syncMinutes,
      accessToken,
      refreshToken,
      triggerInitialSync: triggerInitialSync !== false
    });
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to connect two-way calendar' });
  }
});

integrationsRouter.post('/integrations/calendar/:provider/two-way/sync', validateParams(providerSchema), validateBody(noBodySchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const provider = req.params?.provider;
    const result = await syncCalendarTwoWayForTenant(tenant, {
      provider,
      reason: 'manual'
    });
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Two-way calendar sync failed' });
  }
});

integrationsRouter.post(
  '/integrations/calendar/:provider/two-way/push',
  validateParams(providerSchema),
  validateBody(pushSchema),
  (req, res) => {
  try {
    const tenant = req.tenant;
    const provider = req.params?.provider;
    const result = pushCalendarTwoWayEventForTenant(tenant, {
      provider,
      event: req.body?.event || req.body
    });
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to push calendar event' });
  }
});

integrationsRouter.delete('/integrations/calendar/:provider/two-way', validateParams(providerSchema), (req, res) => {
  try {
    const tenant = req.tenant;
    const provider = req.params?.provider;
    const result = disconnectCalendarTwoWayForTenant(tenant, { provider });
    const snapshot = getTenantIntegrationSnapshot(tenant);
    return res.json({ ...result, ...snapshot });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to disconnect two-way calendar' });
  }
});

publicIntegrationsRouter.get('/oauth/calendar/:provider/callback', async (req, res) => {
  const provider = req.params?.provider;
  const state = String(req.query?.state || '');
  const code = String(req.query?.code || '');
  const error = String(req.query?.error || '');
  const errorDescription = String(req.query?.error_description || '');
  try {
    await completeCalendarOAuthCallback({
      provider,
      state,
      code,
      error,
      error_description: errorDescription
    });
    return res.status(200).send(buildOAuthCallbackHtml({
      provider,
      ok: true,
      message: `${provider === 'google' ? 'Google' : 'Outlook'} calendar connected successfully.`
    }));
  } catch (err) {
    return res.status(200).send(buildOAuthCallbackHtml({
      provider,
      ok: false,
      message: err?.message || 'OAuth connection failed'
    }));
  }
});

module.exports = { integrationsRouter, publicIntegrationsRouter };
