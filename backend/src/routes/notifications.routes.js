const express = require('express');
const { DEV_MODE } = require('../config/runtime');
const { z, validateBody, validateQuery } = require('../utils/validate');
const {
  defaultNotificationSettings,
  normalizeNotificationSettings,
  getNotificationSettings,
  setNotificationSettings,
  getNotificationLog,
  getDevSettings
} = require('../store/dataStore');
const { emitEvent } = require('../services/notificationService');

const notificationsRouter = express.Router();

const notificationSettingsSchema = z.object({
  channels: z.object({
    email: z.boolean().optional(),
    sms: z.boolean().optional(),
    desktop: z.boolean().optional(),
    inApp: z.boolean().optional()
  }).partial().optional(),
  triggers: z.object({}).passthrough().optional(),
  quietHours: z.object({
    enabled: z.boolean().optional(),
    start: z.string().trim().regex(/^([01]\d|2[0-3]):([0-5]\d)$/).optional(),
    end: z.string().trim().regex(/^([01]\d|2[0-3]):([0-5]\d)$/).optional(),
    timezone: z.string().trim().max(80).optional()
  }).partial().optional(),
  dedupeMinutes: z.coerce.number().int().min(1).max(120).optional(),
  highValueLeadMinCents: z.coerce.number().int().min(0).max(10000000000).optional(),
  escalation: z.object({
    enabled: z.boolean().optional(),
    afterMinutes: z.coerce.number().int().min(1).max(1440).optional(),
    channel: z.string().trim().max(32).optional()
  }).partial().optional()
}).passthrough();

const notificationLogQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).optional().default(50)
});

const emitTestSchema = z.object({
  type: z.string().trim().max(80).optional().default('missed_call'),
  channel: z.string().trim().max(32).optional()
});

function parseLimit(value, fallback = 50) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.min(200, Math.round(n)));
}

function validateSettingsShape(input) {
  const v = input && typeof input === 'object' && !Array.isArray(input);
  if (!v) return 'Body must be an object';
  if (input.channels && typeof input.channels !== 'object') return 'channels must be an object';
  if (input.triggers && typeof input.triggers !== 'object') return 'triggers must be an object';
  if (input.quietHours && typeof input.quietHours !== 'object') return 'quietHours must be an object';
  if (input.escalation && typeof input.escalation !== 'object') return 'escalation must be an object';
  return null;
}

notificationsRouter.get('/notifications/settings', (req, res) => {
  try {
    const accountId = String(req?.tenant?.accountId || '').trim();
    if (!accountId) return res.status(400).json({ error: 'Missing tenant accountId' });
    const settings = getNotificationSettings(accountId);
    return res.json({
      ok: true,
      accountId,
      to: String(req?.tenant?.to || ''),
      settings: normalizeNotificationSettings(settings || defaultNotificationSettings())
    });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to load notification settings' });
  }
});

notificationsRouter.put('/notifications/settings', validateBody(notificationSettingsSchema), (req, res) => {
  try {
    const accountId = String(req?.tenant?.accountId || '').trim();
    if (!accountId) return res.status(400).json({ error: 'Missing tenant accountId' });
    const body = req.body || {};
    const shapeErr = validateSettingsShape(body);
    if (shapeErr) return res.status(400).json({ error: shapeErr });
    const settings = setNotificationSettings(accountId, body);
    return res.json({
      ok: true,
      accountId,
      to: String(req?.tenant?.to || ''),
      settings: normalizeNotificationSettings(settings)
    });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to save notification settings' });
  }
});

notificationsRouter.get('/notifications/log', validateQuery(notificationLogQuerySchema), (req, res) => {
  try {
    const accountId = String(req?.tenant?.accountId || '').trim();
    if (!accountId) return res.status(400).json({ error: 'Missing tenant accountId' });
    const limit = parseLimit(req?.query?.limit, 50);
    const items = getNotificationLog(accountId, limit);
    return res.json({
      ok: true,
      accountId,
      to: String(req?.tenant?.to || ''),
      items,
      total: items.length
    });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to load notification log' });
  }
});

notificationsRouter.post('/notifications/emitTest', validateBody(emitTestSchema), (req, res) => {
  try {
    const dev = getDevSettings();
    const allowed = DEV_MODE === true || dev.enabled === true;
    if (!allowed) return res.status(403).json({ error: 'Test emit is only available in dev mode' });

    const tenant = req.tenant;
    const { type, channel } = req.body || {};
    const eventType = String(type || 'missed_call');
    const result = emitEvent(tenant, {
      type: eventType,
      to: String(tenant.to),
      from: 'test',
      conversationId: `test__${Date.now()}`,
      meta: channel ? { preferredChannel: String(channel) } : {}
    });
    return res.json({ ok: true, result });
  } catch (err) {
    return res.status(400).json({ error: err?.message || 'Failed to emit test notification event' });
  }
});

module.exports = { notificationsRouter };
