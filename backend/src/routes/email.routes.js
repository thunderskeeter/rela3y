const express = require('express');
const crypto = require('crypto');
const { z, validateBody, validateQuery } = require('../utils/validate');
const { sendEmailCampaign } = require('../services/emailDeliveryService');
const { CAL_OAUTH_REDIRECT_BASE } = require('../config/runtime');
const {
  loadData,
  saveDataDebounced,
  getConversations,
  getContacts,
  getAccountById
} = require('../store/dataStore');

const emailRouter = express.Router();

const EMAIL_ROLES = new Set(['superadmin', 'owner', 'admin']);

const listCampaignsQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).optional().default(30)
});

const sendCampaignBodySchema = z.object({
  subject: z.string().trim().min(3).max(200),
  body: z.string().trim().min(3).max(20_000),
  templateKey: z.string().trim().max(80).optional().nullable(),
  recipientEmails: z.array(z.string().trim().email().max(254)).max(10000).optional()
});

function canManageEmail(req) {
  const role = String(req?.user?.role || '').trim().toLowerCase();
  return EMAIL_ROLES.has(role);
}

function normalizePhone(raw) {
  const v = String(raw || '').trim();
  if (!v) return '';
  const digits = v.replace(/[^\d+]/g, '');
  if (!digits) return '';
  if (digits.startsWith('+')) return digits;
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  return `+${digits}`;
}

function normalizeEmail(raw) {
  const v = String(raw || '').trim().toLowerCase();
  if (!v) return '';
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v) ? v : '';
}

function ensureEmailCampaignSettings(account) {
  account.settings = account.settings && typeof account.settings === 'object' ? account.settings : {};
  account.settings.emailCampaign = account.settings.emailCampaign && typeof account.settings.emailCampaign === 'object'
    ? account.settings.emailCampaign
    : {};
  account.settings.emailCampaign.history = Array.isArray(account.settings.emailCampaign.history)
    ? account.settings.emailCampaign.history
    : [];
  return account.settings.emailCampaign;
}

function listEmailRecipientsForAccount(accountId) {
  const contacts = getContacts(accountId);
  const contactByPhone = new Map();
  for (const c of contacts) {
    const phone = normalizePhone(c?.phone || '');
    if (phone) contactByPhone.set(phone, c);
  }

  const recipientsByEmail = new Map();
  const conversations = getConversations(accountId);
  for (const row of conversations) {
    const convo = row?.conversation || {};
    const ld = convo?.leadData || {};
    const email = normalizeEmail(ld?.customer_email || ld?.email || '');
    if (!email) continue;
    const phone = normalizePhone(convo?.from || ld?.customer_phone || '');
    const contact = contactByPhone.get(phone) || null;
    const name = String(ld?.customer_name || contact?.name || '').trim() || 'Unknown';
    const item = recipientsByEmail.get(email) || {
      email,
      name,
      phone,
      source: 'booking',
      lastSeenAt: 0
    };
    item.name = item.name || name;
    item.phone = item.phone || phone;
    item.lastSeenAt = Math.max(Number(item.lastSeenAt || 0), Number(convo?.lastActivityAt || convo?.bookingTime || convo?.createdAt || 0));
    recipientsByEmail.set(email, item);
  }

  for (const c of contacts) {
    const email = normalizeEmail(c?.email || '');
    if (!email) continue;
    const phone = normalizePhone(c?.phone || '');
    const name = String(c?.name || '').trim() || 'Unknown';
    const existing = recipientsByEmail.get(email);
    if (existing) {
      existing.name = existing.name || name;
      existing.phone = existing.phone || phone;
      if (existing.source !== 'booking') existing.source = 'contact';
      existing.lastSeenAt = Math.max(Number(existing.lastSeenAt || 0), Number(c?.updatedAt || c?.createdAt || 0));
      recipientsByEmail.set(email, existing);
      continue;
    }
    recipientsByEmail.set(email, {
      email,
      name,
      phone,
      source: 'contact',
      lastSeenAt: Number(c?.updatedAt || c?.createdAt || 0)
    });
  }

  return Array.from(recipientsByEmail.values())
    .sort((a, b) => Number(b?.lastSeenAt || 0) - Number(a?.lastSeenAt || 0));
}

emailRouter.get('/email/contacts', (_req, res) => {
  const req = _req;
  if (!canManageEmail(req)) {
    return res.status(403).json({ error: 'Only owners/admins can manage email campaigns' });
  }
  const tenant = req.tenant;
  const recipients = listEmailRecipientsForAccount(tenant.accountId);
  return res.json({ recipients, total: recipients.length });
});

emailRouter.get('/email/campaigns', validateQuery(listCampaignsQuerySchema), (_req, res) => {
  const req = _req;
  if (!canManageEmail(req)) {
    return res.status(403).json({ error: 'Only owners/admins can manage email campaigns' });
  }
  const tenant = req.tenant;
  const data = loadData();
  const accountRef = getAccountById(data, tenant.accountId);
  if (!accountRef?.account) return res.status(404).json({ error: 'Account not found' });
  const settings = ensureEmailCampaignSettings(accountRef.account);
  const limit = Number(req.query?.limit || 30);
  return res.json({
    campaigns: settings.history.slice(0, limit)
  });
});

emailRouter.post('/email/campaigns/send', validateBody(sendCampaignBodySchema), async (_req, res) => {
  const req = _req;
  if (!canManageEmail(req)) {
    return res.status(403).json({ error: 'Only owners/admins can manage email campaigns' });
  }
  const tenant = req.tenant;
  const data = loadData();
  const accountRef = getAccountById(data, tenant.accountId);
  if (!accountRef?.account) return res.status(404).json({ error: 'Account not found' });

  const settings = ensureEmailCampaignSettings(accountRef.account);
  const body = req.body || {};
  const subject = String(body.subject || '').trim();
  const textBody = String(body.body || '').trim();
  const templateKey = body.templateKey ? String(body.templateKey).trim() : '';

  const allRecipients = listEmailRecipientsForAccount(tenant.accountId);
  const allByEmail = new Map(allRecipients.map((r) => [String(r.email || '').toLowerCase(), r]));
  const requestedEmails = Array.isArray(body.recipientEmails) ? body.recipientEmails.map((x) => normalizeEmail(x)).filter(Boolean) : [];
  const finalRecipients = requestedEmails.length
    ? requestedEmails.map((email) => allByEmail.get(email) || { email, name: 'Unknown', phone: '', source: 'manual', lastSeenAt: 0 })
    : allRecipients;

  if (!finalRecipients.length) {
    return res.status(400).json({ error: 'No email recipients found for this workspace' });
  }

  const workspaceIdentity = accountRef.account?.workspace?.identity || {};
  const fromEmailCandidate = String(
    workspaceIdentity?.businessEmail ||
    accountRef.account?.billing?.details?.billingEmail ||
    accountRef.account?.email ||
    ''
  ).trim();
  const fromNameCandidate = String(
    workspaceIdentity?.businessName ||
    accountRef.account?.businessName ||
    'Relay'
  ).trim();
  const logoAsset = workspaceIdentity?.logoAsset && typeof workspaceIdentity.logoAsset === 'object'
    ? workspaceIdentity.logoAsset
    : null;
  const logoMime = String(logoAsset?.mimeType || '').trim().toLowerCase();
  const logoBase64 = String(logoAsset?.dataBase64 || logoAsset?.base64 || '').trim();
  const logoDataUrl = (logoMime.startsWith('image/') && logoBase64 && logoBase64.length <= (1024 * 1024 * 2))
    ? `data:${logoMime};base64,${logoBase64}`
    : '';
  const logoUrl = String(workspaceIdentity?.logoUrl || '').trim();
  const base = String(CAL_OAUTH_REDIRECT_BASE || '').replace(/\/$/, '') || '';
  const fallbackLogoUrl = base ? `${base}/logos/main.png` : '';

  const delivery = await sendEmailCampaign({
    subject,
    body: textBody,
    recipients: finalRecipients,
    fromEmailCandidate,
    fromNameCandidate,
    branding: {
      brandName: fromNameCandidate,
      logoDataUrl,
      logoUrl: logoUrl || fallbackLogoUrl
    }
  });

  const deliveryFailedCompletely = Number(delivery?.delivered || 0) <= 0;
  const deliveryPartial = Number(delivery?.delivered || 0) > 0 && Number(delivery?.failed || 0) > 0;
  const status = deliveryFailedCompletely ? 'failed' : (deliveryPartial ? 'partial' : 'sent');

  const campaign = {
    id: `emc_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`,
    ts: Date.now(),
    status,
    subject,
    body: textBody,
    templateKey: templateKey || null,
    recipientCount: finalRecipients.length,
    deliveredCount: Number(delivery?.delivered || 0),
    failedCount: Number(delivery?.failed || 0),
    provider: String(delivery?.provider || ''),
    failureSamples: Array.isArray(delivery?.failures) ? delivery.failures.slice(0, 20) : [],
    recipients: finalRecipients.slice(0, 5000).map((r) => ({
      email: String(r?.email || ''),
      name: String(r?.name || ''),
      phone: String(r?.phone || '')
    })),
    sentBy: String(req?.user?.id || ''),
    accountId: String(tenant.accountId || '')
  };

  settings.history.unshift(campaign);
  settings.history = settings.history.slice(0, 200);
  saveDataDebounced(data);

  if (deliveryFailedCompletely) {
    const firstError = String(delivery?.failures?.[0]?.error || '').trim();
    return res.status(502).json({
      error: firstError || 'Email delivery failed for all recipients',
      campaign: {
        id: campaign.id,
        ts: campaign.ts,
        status: campaign.status,
        subject: campaign.subject,
        templateKey: campaign.templateKey,
        recipientCount: campaign.recipientCount,
        deliveredCount: campaign.deliveredCount,
        failedCount: campaign.failedCount,
        provider: campaign.provider
      }
    });
  }

  return res.json({
    ok: true,
    campaign: {
      id: campaign.id,
      ts: campaign.ts,
      status: campaign.status,
      subject: campaign.subject,
      templateKey: campaign.templateKey,
      recipientCount: campaign.recipientCount,
      deliveredCount: campaign.deliveredCount,
      failedCount: campaign.failedCount,
      provider: campaign.provider
    }
  });
});

module.exports = { emailRouter };
