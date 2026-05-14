const express = require('express');
const { z, validateBody, validateParams } = require('../utils/validate');
const { startFlow, advanceFlow } = require('../services/flowEngine');
const { OUTCOME_PACKS } = require('../services/flowTemplates');
const { getOutcomePacks } = require('../services/outcomePackService');

const flowsRouter = express.Router();
const {
  loadData,
  saveDataDebounced,
  getFlows,
  setFlowInData,
  deleteFlowInData
} = require('../store/dataStore');

const fromSchema = z.string().trim().min(1).max(32);
const flowIdSchema = z.string().trim().min(1).max(120);

const flowStartSchema = z.object({
  from: fromSchema,
  flowId: flowIdSchema,
  ruleId: z.string().trim().min(1).max(120).optional().nullable()
});

const flowAdvanceSchema = z.object({
  from: fromSchema,
  event: z.string().trim().min(1).max(120),
  text: z.string().trim().max(4000).optional().default('')
});

const flowSaveSchema = z.object({
  id: flowIdSchema
}).passthrough();

const idParamSchema = z.object({
  id: z.string().trim().min(1).max(120)
});

flowsRouter.post('/flows/start', validateBody(flowStartSchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const { from, flowId, ruleId } = req.body || {};
    const to = tenant.to;
    if (!from || !flowId) return res.status(400).json({ error: 'Missing from/flowId' });
    const convo = await startFlow({
      tenant,
      to: String(to),
      from: String(from),
      flowId: String(flowId),
      ruleId: ruleId ? String(ruleId) : null
    });
    if (!convo) return res.status(404).json({ error: 'Flow not found' });
    res.json({ ok: true, conversation: convo });
  } catch (err) {
    console.error('❌ Flow start error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

flowsRouter.post('/flows/advance', validateBody(flowAdvanceSchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const { from, event, text } = req.body || {};
    const to = tenant.to;
    if (!from || !event) return res.status(400).json({ error: 'Missing from/event' });
    const convo = await advanceFlow({
      tenant,
      to: String(to),
      from: String(from),
      event: String(event),
      text: text ? String(text) : ''
    });
    res.json({ ok: true, conversation: convo });
  } catch (err) {
    console.error('❌ Flow advance error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

flowsRouter.get('/flows', (req, res) => {
  const tenant = req.tenant;
  const flows = getFlows(tenant.accountId);
  res.json({ flows });
});

// Get single flow
flowsRouter.get('/flows/:id', validateParams(idParamSchema), (req, res) => {
  const tenant = req.tenant;
  const flow = getFlows(tenant.accountId).find((f) => String(f.id) === String(req.params.id));
  
  if (!flow || String(flow.accountId || '') !== String(tenant.accountId)) {
    return res.status(404).json({ error: 'Flow not found' });
  }
  
  res.json({ flow });
});

// Create/update flow
flowsRouter.post('/flows', validateBody(flowSaveSchema), (req, res) => {
  const tenant = req.tenant;
  const flow = req.body;
  const data = loadData();
  if (!flow || !flow.id) return res.status(400).json({ error: 'flow.id is required' });

  const saved = setFlowInData(data, tenant.accountId, flow);
  if (!saved) return res.status(400).json({ error: 'Invalid flow payload' });
  
  saveDataDebounced(data);
  res.json({ ok: true, flow: saved });
});

// Delete flow
flowsRouter.delete('/flows/:id', validateParams(idParamSchema), (req, res) => {
  const tenant = req.tenant;
  const data = loadData();

  if (deleteFlowInData(data, tenant.accountId, req.params.id)) {
    saveDataDebounced(data);
  } else {
    return res.status(404).json({ error: 'Flow not found' });
  }
  
  res.json({ ok: true });
});

flowsRouter.get('/outcome-packs', (req, res) => {
  const tenant = req.tenant;
  const packs = getOutcomePacks(tenant.accountId);
  res.json({ packs });
});

function setOutcomePackEnabled(req, res, enabled) {
  const tenant = req.tenant;
  const packId = String(req.params?.id || '');
  const pack = OUTCOME_PACKS?.[packId];
  if (!pack) return res.status(404).json({ error: 'Outcome pack not found' });
  const data = loadData();
  const accountFlows = getFlows(tenant.accountId);
  const ids = new Set((pack.flowTemplateIds || []).map((x) => String(x)));
  let changed = 0;
  for (const flow of accountFlows) {
    if (!ids.has(String(flow?.id || ''))) continue;
    const next = { ...flow, enabled: enabled === true };
    if (setFlowInData(data, tenant.accountId, next)) changed += 1;
  }
  saveDataDebounced(data);
  return res.json({
    ok: true,
    packId,
    enabled: enabled === true,
    affectedFlows: changed
  });
}

flowsRouter.post('/outcome-packs/:id/enable', validateParams(idParamSchema), (req, res) => setOutcomePackEnabled(req, res, true));
flowsRouter.post('/outcome-packs/:id/disable', validateParams(idParamSchema), (req, res) => setOutcomePackEnabled(req, res, false));

module.exports = { flowsRouter };
