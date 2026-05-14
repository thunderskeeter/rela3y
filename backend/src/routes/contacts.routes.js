const express = require('express');
const { ensureContactFields } = require('../services/complianceService');
const { listContacts, createOrUpdateContact, importContacts } = require('../services/contactsService');
const { z, validateBody, validateQuery } = require('../utils/validate');

const contactsRouter = express.Router();

const contactUpsertSchema = z.object({
  contact: z.object({
    phone: z.string().trim().min(3).max(32),
    name: z.string().trim().max(160).optional(),
    email: z.string().trim().email().max(254).optional(),
    tags: z.array(z.string().trim().max(64)).max(100).optional()
  }).passthrough()
});

const contactImportSchema = z.object({
  contacts: z.array(
    z.object({
      phone: z.string().trim().min(3).max(32),
      name: z.string().trim().max(160).optional()
    }).passthrough()
  ).max(10000)
});
const contactsListQuerySchema = z.object({
  includeUnqualified: z.coerce.boolean().optional().default(false)
});

const SENSITIVE_CONTACT_ROLES = new Set(['superadmin', 'owner', 'admin']);

function normalizeRole(value) {
  return String(value || '').trim().toLowerCase();
}

function canAccessSensitiveContacts(req) {
  return SENSITIVE_CONTACT_ROLES.has(normalizeRole(req?.user?.role));
}

function maskPhone(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const digits = raw.replace(/\D/g, '');
  if (digits.length < 4) return '****';
  const last4 = digits.slice(-4);
  return `***-***-${last4}`;
}

function contactHasQualifiedIdentity(contact) {
  const name = String(contact?.name || '').trim();
  if (name.length >= 2) return true;
  const leadStatus = String(contact?.lifecycle?.leadStatus || '').trim().toLowerCase();
  if (leadStatus === 'booked' || leadStatus === 'closed') return true;
  const tags = Array.isArray(contact?.tags) ? contact.tags.map((t) => String(t || '').toLowerCase()) : [];
  if (tags.includes('booked') || tags.includes('verified')) return true;
  return false;
}

function redactContactForRole(contact, allowSensitive) {
  const next = {
    ...contact,
    flags: { ...(contact?.flags || {}) },
    summary: { ...(contact?.summary || {}) },
    lifecycle: { ...(contact?.lifecycle || {}) }
  };
  if (allowSensitive) return next;
  next.phoneRaw = String(contact?.phone || '');
  next.phone = maskPhone(contact?.phone);
  next.name = String(contact?.name || '').trim() || 'Protected Contact';
  if (next.summary && typeof next.summary === 'object') {
    next.summary.notes = '';
  }
  if ('email' in next) next.email = '';
  return next;
}

// List contacts for a business number
contactsRouter.get('/contacts', validateQuery(contactsListQuerySchema), async (req, res) => {
  try {
    const tenant = req.tenant;
    const allowSensitive = canAccessSensitiveContacts(req);
    const includeUnqualified = Boolean(req.query?.includeUnqualified === true || req.query?.includeUnqualified === 'true');
    const contacts = (await listContacts(tenant.accountId))
      .map((c) => ensureContactFields(c))
      .filter((c) => includeUnqualified || contactHasQualifiedIdentity(c))
      .map((c) => redactContactForRole(c, allowSensitive));

    return res.json({ contacts });
  } catch (err) {
    return res.status(err?.status || 500).json({ error: err?.message || 'Failed to load contacts' });
  }
});

// Upsert a single contact
contactsRouter.post('/contacts', validateBody(contactUpsertSchema), async (req, res) => {
  if (!canAccessSensitiveContacts(req)) {
    return res.status(403).json({ error: 'Only owners/admins can manage contacts' });
  }
  try {
    const tenant = req.tenant;
    const { contact } = req.body || {};
    if (!contact?.phone) return res.status(400).json({ error: 'Missing contact.phone' });
    if (!contactHasQualifiedIdentity(contact)) {
      return res.status(400).json({ error: 'Contact not saved yet. Capture customer name or qualified booking details first.' });
    }

    const saved = await createOrUpdateContact(tenant.accountId, tenant.to, contact, {
      requestId: req.requestId,
      route: 'POST /api/contacts'
    });
    ensureContactFields(saved);
    return res.json({ ok: true, contact: saved });
  } catch (err) {
    return res.status(err?.status || 500).json({ error: err?.message || 'Failed to save contact' });
  }
});

// Bulk import contacts from parsed VCF data
contactsRouter.post('/contacts/import', validateBody(contactImportSchema), async (req, res) => {
  if (!canAccessSensitiveContacts(req)) {
    return res.status(403).json({ error: 'Only owners/admins can import contacts' });
  }
  try {
    const tenant = req.tenant;
    const { contacts: incoming } = req.body || {};
    const result = await importContacts(tenant.accountId, tenant.to, incoming, {
      requestId: req.requestId,
      route: 'POST /api/contacts/import'
    });
    return res.json({ ok: true, ...result });
  } catch (err) {
    return res.status(err?.status || 500).json({ error: err?.message || 'Failed to import contacts' });
  }
});

module.exports = { contactsRouter };
