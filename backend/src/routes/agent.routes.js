const express = require('express');
const { loadData } = require('../store/dataStore');
const { listReviewQueue } = require('../services/reviewQueueService');
const { z, validateBody, validateParams, validateQuery } = require('../utils/validate');
const {
  startRun,
  resumeRun,
  cancelRun,
  approveReviewItem,
  rejectReviewItem,
  getOpportunityRun
} = require('../services/agentEngine');
const { setAutomationState } = require('../services/opportunitiesService');
const { ensureOpportunityDefaults } = require('../services/revenueIntelligenceService');

const agentRouter = express.Router();

const reviewQueueQuerySchema = z.object({
  status: z.string().trim().max(40).optional().default(''),
  limit: z.coerce.number().int().min(1).max(200).optional().default(100)
});

const idParamSchema = z.object({
  id: z.string().trim().min(1).max(120)
});

const startAgentSchema = z.object({
  opportunityId: z.string().trim().min(1).max(120),
  mode: z.enum(['AUTO', 'ASSISTED']).optional().default('AUTO')
});

const reviewDecisionSchema = z.object({
  notes: z.string().trim().max(1000).optional().default('')
});

const cancelRunSchema = z.object({
  reason: z.string().trim().max(200).optional().default('cancelled_by_user')
});

function findOpportunity(accountId, opportunityId) {
  const data = loadData();
  const opp = (data.revenueOpportunities || []).find((o) =>
    String(o?.accountId || '') === String(accountId || '') && String(o?.id || '') === String(opportunityId || '')
  );
  return { data, opp };
}

agentRouter.get('/agent/review-queue', validateQuery(reviewQueueQuerySchema), (req, res) => {
  const items = listReviewQueue(req.tenant.accountId, {
    status: String(req.query?.status || ''),
    limit: Number(req.query?.limit || 100)
  });
  res.json({ items });
});

agentRouter.post('/agent/review-queue/:id/approve', validateParams(idParamSchema), validateBody(reviewDecisionSchema), async (req, res) => {
  try {
    const out = await approveReviewItem(req.tenant.accountId, req.params.id, req.user?.id || null, req.body?.notes || '');
    if (!out?.ok) return res.status(400).json({ error: out?.reason || 'approve_failed' });
    return res.json({ ok: true, result: out });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'approve_failed' });
  }
});

agentRouter.post('/agent/review-queue/:id/reject', validateParams(idParamSchema), validateBody(reviewDecisionSchema), async (req, res) => {
  try {
    const out = await rejectReviewItem(req.tenant.accountId, req.params.id, req.user?.id || null, req.body?.notes || '');
    if (!out?.ok) return res.status(400).json({ error: out?.reason || 'reject_failed' });
    return res.json({ ok: true, result: out });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'reject_failed' });
  }
});

agentRouter.post('/agent/start', validateBody(startAgentSchema), async (req, res) => {
  try {
    const opportunityId = String(req.body?.opportunityId || '').trim();
    if (!opportunityId) return res.status(400).json({ error: 'opportunityId is required' });
    const mode = String(req.body?.mode || 'AUTO').toUpperCase();
    const out = await startRun(req.tenant.accountId, opportunityId, {
      trigger: 'manual_user_start',
      mode
    });
    if (!out?.ok) return res.status(400).json({ error: out?.reason || 'start_failed' });
    return res.json({ ok: true, run: out.run, reused: out.reused === true });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'start_failed' });
  }
});

agentRouter.post('/agent/opportunity/:id/pause', validateParams(idParamSchema), async (req, res) => {
  try {
    const { opp } = findOpportunity(req.tenant.accountId, req.params.id);
    if (!opp) return res.status(404).json({ error: 'Opportunity not found' });
    ensureOpportunityDefaults(opp);
    const persisted = await setAutomationState(req.tenant.accountId, opp.id, true, {
      route: '/agent/opportunity/:id/pause',
      requestId: req.id || null
    });
    if (!persisted) return res.status(404).json({ error: 'Opportunity not found' });
    const activeRunId = opp?.agentState?.activeRunId;
    if (activeRunId) {
      await cancelRun(req.tenant.accountId, activeRunId, 'paused_by_user');
    }
    return res.json({ ok: true, opportunityId: persisted.id, stopAutomation: true });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'pause_failed' });
  }
});

agentRouter.post('/agent/opportunity/:id/resume', validateParams(idParamSchema), async (req, res) => {
  try {
    const { opp } = findOpportunity(req.tenant.accountId, req.params.id);
    if (!opp) return res.status(404).json({ error: 'Opportunity not found' });
    ensureOpportunityDefaults(opp);
    const persisted = await setAutomationState(req.tenant.accountId, opp.id, false, {
      route: '/agent/opportunity/:id/resume',
      requestId: req.id || null
    });
    if (!persisted) return res.status(404).json({ error: 'Opportunity not found' });
    if (opp?.agentState?.lastRunId) {
      await resumeRun(req.tenant.accountId, opp.agentState.lastRunId);
      return res.json({ ok: true, resumedRunId: opp.agentState.lastRunId });
    }
    const out = await startRun(req.tenant.accountId, persisted.id, { trigger: 'manual_user_start', mode: 'AUTO' });
    if (!out?.ok) return res.status(400).json({ error: out?.reason || 'resume_failed' });
    return res.json({ ok: true, run: out.run });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'resume_failed' });
  }
});

agentRouter.post('/agent/run/:id/cancel', validateParams(idParamSchema), validateBody(cancelRunSchema), async (req, res) => {
  try {
    const out = await cancelRun(req.tenant.accountId, req.params.id, req.body?.reason || 'cancelled_by_user');
    if (!out?.ok) return res.status(400).json({ error: out?.reason || 'cancel_failed' });
    return res.json({ ok: true, run: out.run });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'cancel_failed' });
  }
});

agentRouter.get('/agent/opportunity/:id/run', validateParams(idParamSchema), (req, res) => {
  const run = getOpportunityRun(req.tenant.accountId, req.params.id);
  if (!run) return res.json({ run: null });
  return res.json({ run });
});

module.exports = { agentRouter };
