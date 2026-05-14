const { pool } = require('../db/pool');
const { withTransaction } = require('../db/withTransaction');
const { USE_DB_CONTACTS } = require('../config/runtime');
const {
  listByTenant,
  listByPhones,
  getByPhone,
  upsertByPhone,
  bulkUpsertByPhone
} = require('../repositories/contactsRepo');
const { loadData, getContacts, saveDataDebounced, flushDataNow } = require('../store/dataStore');
const { ensureContactFields } = require('./complianceService');
const { verifyParity, stableNormalize } = require('./migrationParityService');

function normalizeContactsForParity(contacts) {
  return stableNormalize(
    (Array.isArray(contacts) ? contacts : [])
      .map((contact) => {
        const next = { ...(contact || {}) };
        delete next.tenantId;
        ensureContactFields(next);
        return next;
      })
      .sort((a, b) => {
        const left = `${String(a?.phone || '')}|${String(a?.name || '')}`;
        const right = `${String(b?.phone || '')}|${String(b?.name || '')}`;
        return left.localeCompare(right);
      })
  );
}

function normalizePhone(raw) {
  if (!raw) return null;
  let digits = String(raw).replace(/[^\d+]/g, '');
  if (!digits) return null;
  if (!digits.startsWith('+')) {
    if (digits.length === 10) digits = `+1${digits}`;
    else if (digits.length === 11 && digits.startsWith('1')) digits = `+${digits}`;
    else digits = `+${digits}`;
  }
  return digits;
}

function buildContactId(tenantTo, phone) {
  return `${String(tenantTo || '').trim()}__${String(phone || '').trim()}`;
}

function buildUpsertContact(existing, accountId, tenantTo, contactInput, now) {
  const normalizedPhone = normalizePhone(contactInput?.phone);
  if (!normalizedPhone) throw new Error('Invalid contact.phone');
  const merged = {
    ...(existing || {}),
    ...(contactInput || {}),
    id: existing?.id || buildContactId(tenantTo, normalizedPhone),
    accountId: String(accountId),
    to: String(tenantTo),
    phone: normalizedPhone,
    updatedAt: now,
    createdAt: Number(existing?.createdAt || now)
  };
  ensureContactFields(merged);
  merged.tags = Array.isArray(merged.tags) ? merged.tags : [];
  return merged;
}

function buildImportContact(existing, accountId, tenantTo, incoming, now) {
  const normalizedPhone = normalizePhone(incoming?.phone);
  if (!normalizedPhone) return null;
  if (existing) {
    const merged = {
      ...existing,
      accountId: String(accountId),
      to: String(tenantTo),
      phone: normalizedPhone,
      updatedAt: now
    };
    if (!String(existing?.name || '').trim() && String(incoming?.name || '').trim()) {
      merged.name = String(incoming.name).trim();
    }
    ensureContactFields(merged);
    merged.tags = Array.isArray(merged.tags) ? merged.tags : [];
    return merged;
  }
  const created = {
    id: buildContactId(tenantTo, normalizedPhone),
    accountId: String(accountId),
    to: String(tenantTo),
    phone: normalizedPhone,
    name: String(incoming?.name || '').trim(),
    flags: { vip: false, doNotAutoReply: false },
    summary: { notes: '' },
    lifecycle: { leadStatus: 'new' },
    optedOut: false,
    optedOutAt: null,
    consent: false,
    consentSource: null,
    dnrSource: null,
    tags: [],
    createdAt: now,
    updatedAt: now
  };
  ensureContactFields(created);
  return created;
}

function toRepoInput(contact) {
  return {
    id: String(contact?.id || ''),
    phone: String(contact?.phone || ''),
    name: String(contact?.name || ''),
    tags: Array.isArray(contact?.tags) ? contact.tags : [],
    payload: { ...(contact || {}) },
    createdAt: Number(contact?.createdAt || Date.now()),
    updatedAt: Number(contact?.updatedAt || Date.now())
  };
}

function snapshotContactShape(contact) {
  const next = { ...(contact || {}) };
  delete next.tenantId;
  ensureContactFields(next);
  return next;
}

function contactDiffers(left, right) {
  return JSON.stringify(normalizeContactsForParity([left])) !== JSON.stringify(normalizeContactsForParity([right]));
}

function logConsistencyError({
  operation,
  accountId,
  phone = null,
  phones = null,
  service,
  route = null,
  requestId = null,
  errorType,
  error = null
}) {
  const line = {
    level: 'error',
    type: 'contact_migration_consistency_error',
    entity: 'contact',
    operation,
    accountId: String(accountId || ''),
    phone: phone ? String(phone) : null,
    phones: Array.isArray(phones) ? phones.map((value) => String(value || '')) : null,
    service: String(service || ''),
    route: route ? String(route) : null,
    requestId: requestId ? String(requestId) : null,
    errorType: String(errorType || ''),
    message: error?.message ? String(error.message) : null
  };
  console.error(JSON.stringify(line));
}

async function syncSnapshotContactsOrThrow(accountId, contacts, meta) {
  const data = loadData();
  data.contacts = data.contacts && typeof data.contacts === 'object' ? data.contacts : {};
  for (const contact of Array.isArray(contacts) ? contacts : []) {
    const snapshotContact = snapshotContactShape(contact);
    const key = buildContactId(snapshotContact.to, snapshotContact.phone);
    data.contacts[key] = snapshotContact;
  }

  try {
    saveDataDebounced(data);
    await flushDataNow();
  } catch (err) {
    logConsistencyError({
      operation: meta?.operation || 'unknown',
      accountId,
      phone: meta?.phone || null,
      phones: meta?.phones || null,
      service: meta?.service || 'contactsService',
      route: meta?.route || null,
      requestId: meta?.requestId || null,
      errorType: 'snapshot_sync_failed',
      error: err
    });
    throw err;
  }

  for (const contact of Array.isArray(contacts) ? contacts : []) {
    const dbContact = await getByPhone(pool, accountId, contact.phone);
    const snapshotContact = data.contacts[buildContactId(contact.to, contact.phone)];
    if (!dbContact || !snapshotContact || contactDiffers(snapshotContact, dbContact)) {
      const err = new Error(`Contact drift detected for ${String(contact?.phone || '')}`);
      logConsistencyError({
        operation: meta?.operation || 'unknown',
        accountId,
        phone: meta?.operation === 'upsert' ? contact.phone : null,
        phones: meta?.operation === 'import' ? (meta?.phones || [contact.phone]) : null,
        service: meta?.service || 'contactsService',
        route: meta?.route || null,
        requestId: meta?.requestId || null,
        errorType: 'drift_detected',
        error: err
      });
      throw err;
    }
  }
}

async function listContacts(accountId) {
  const oldFactory = async () => getContacts(accountId).map((contact) => ensureContactFields({ ...(contact || {}) }));
  const newFactory = async () => {
    const contacts = await listByTenant(pool, accountId);
    return contacts.map((contact) => ensureContactFields({ ...(contact || {}) }));
  };

  if (USE_DB_CONTACTS) {
    return verifyParity(
      {
        entity: 'contacts',
        service: 'contactsService.listContacts',
        accountId
      },
      oldFactory,
      newFactory,
      normalizeContactsForParity
    );
  }

  return oldFactory();
}

async function createOrUpdateContact(accountId, tenantTo, contactInput, meta = {}) {
  const now = Date.now();
  const normalizedPhone = normalizePhone(contactInput?.phone);
  if (!normalizedPhone) throw new Error('Invalid contact.phone');

  const contact = await withTransaction(pool, async (db) => {
    const existing = await getByPhone(db, accountId, normalizedPhone);
    const merged = buildUpsertContact(existing, accountId, tenantTo, contactInput, now);
    return upsertByPhone(db, accountId, toRepoInput(merged));
  });

  await syncSnapshotContactsOrThrow(accountId, [contact], {
    ...meta,
    operation: 'upsert',
    service: 'contactsService.createOrUpdateContact',
    phone: normalizedPhone
  });

  return snapshotContactShape(contact);
}

async function importContacts(accountId, tenantTo, contacts, meta = {}) {
  const now = Date.now();
  const normalizedPhones = Array.from(new Set((Array.isArray(contacts) ? contacts : []).map((contact) => normalizePhone(contact?.phone)).filter(Boolean)));
  if (!normalizedPhones.length) {
    return { imported: 0, skipped: 0, total: 0 };
  }

  const existingMap = new Map();
  const items = [];
  const seenPhones = new Set();

  const rows = await withTransaction(pool, async (db) => {
    const existingContacts = await listByPhones(db, accountId, normalizedPhones);
    for (const existing of existingContacts) {
      existingMap.set(String(existing.phone), existing);
    }

    for (const raw of Array.isArray(contacts) ? contacts : []) {
      const phone = normalizePhone(raw?.phone);
      if (!phone || seenPhones.has(phone)) continue;
      seenPhones.add(phone);
      const next = buildImportContact(existingMap.get(phone) || null, accountId, tenantTo, raw, now);
      if (!next) continue;
      items.push(toRepoInput(next));
    }

    return bulkUpsertByPhone(db, accountId, items);
  });

  await syncSnapshotContactsOrThrow(accountId, rows, {
    ...meta,
    operation: 'import',
    service: 'contactsService.importContacts',
    phones: normalizedPhones
  });

  let imported = 0;
  let skipped = 0;
  for (const item of items) {
    if (existingMap.has(String(item.phone))) skipped += 1;
    else imported += 1;
  }

  return {
    imported,
    skipped,
    total: items.length
  };
}

module.exports = {
  listContacts,
  createOrUpdateContact,
  importContacts
};
