const express = require('express');
const { loadData, saveDataDebounced, getRules } = require('../store/dataStore');
const { z, validateBody } = require('../utils/validate');
const { DEV_MODE } = require('../config/runtime');

const rulesRouter = express.Router();

function debugLog(...args) {
  if (DEV_MODE === true) console.log(...args);
}

const ruleSchema = z.object({
  id: z.string().trim().min(1).max(120).optional(),
  enabled: z.boolean().optional().default(true),
  name: z.string().trim().max(160).optional(),
  type: z.string().trim().max(80).optional()
}).passthrough();

const rulesSaveSchema = z.object({
  rules: z.array(ruleSchema).max(500)
});

const vipEntrySchema = z.object({
  from: z.string().trim().min(1).max(32).optional(),
  phone: z.string().trim().min(1).max(32).optional(),
  name: z.string().trim().max(160).optional(),
  notes: z.string().trim().max(500).optional()
}).passthrough();

const vipSaveSchema = z.object({
  vipList: z.array(vipEntrySchema).max(2000)
});

// GET rules for active tenant
rulesRouter.get('/rules', (req, res) => {
  const tenant = req.tenant;
  const to = tenant.to;
  const data = loadData();

  // Migrate old flat array format to per-number object
  if (Array.isArray(data.rules)) {
    data.rules = {};
    saveDataDebounced(data);
  }

  const rules = getRules(tenant.accountId).filter((r) => String(r.to || to) === String(to) || !r.to);
  res.json({ rules });
});

// POST (save) rules for active tenant
rulesRouter.post('/rules', validateBody(rulesSaveSchema), (req, res) => {
  const tenant = req.tenant;
  const to = tenant.to;
  const { rules } = req.body || {};
  if (!Array.isArray(rules)) return res.status(400).json({ error: 'rules must be an array' });

  const data = loadData();

  // Migrate old flat array format
  if (Array.isArray(data.rules)) {
    data.rules = {};
  }

  data.rules[to] = rules.map((r) => ({ ...r, accountId: tenant.accountId }));
  saveDataDebounced(data);
  debugLog(`Rules saved for ${to}: ${rules.length} rules (${rules.filter(r => r.enabled).length} enabled)`);
  res.json({ ok: true });
});

// VIP list sync for active tenant
rulesRouter.get('/vip', (req, res) => {
  const tenant = req.tenant;
  const to = tenant.to;
  const data = loadData();
  const vipList = (data.vipList?.[to] || []).filter((v) => String(v.accountId || tenant.accountId) === String(tenant.accountId));
  res.json({ vipList });
});

rulesRouter.post('/vip', validateBody(vipSaveSchema), (req, res) => {
  const tenant = req.tenant;
  const to = tenant.to;
  const { vipList } = req.body || {};

  const data = loadData();
  data.vipList = data.vipList || {};
  data.vipList[to] = (vipList || []).map((v) => ({ ...v, accountId: tenant.accountId }));
  saveDataDebounced(data);
  debugLog(`VIP list saved for ${to}: ${(vipList || []).length} contacts`);
  res.json({ ok: true });
});

module.exports = { rulesRouter };
