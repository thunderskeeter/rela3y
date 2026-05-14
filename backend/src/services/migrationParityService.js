const { ENABLE_PARITY_CHECKS, NODE_ENV } = require('../config/runtime');

function stableNormalize(value) {
  if (Array.isArray(value)) {
    return value.map(stableNormalize);
  }
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = stableNormalize(value[key]);
    }
    return out;
  }
  return value;
}

function summarizeMismatch(oldValue, newValue) {
  const before = JSON.stringify(stableNormalize(oldValue));
  const after = JSON.stringify(stableNormalize(newValue));
  if (before === after) return null;
  return {
    oldLength: before.length,
    newLength: after.length,
    oldSample: before.slice(0, 400),
    newSample: after.slice(0, 400)
  };
}

function logParityMismatch(meta, oldValue, newValue) {
  const mismatch = summarizeMismatch(oldValue, newValue);
  if (!mismatch) return;
  const line = {
    level: 'warn',
    type: 'migration_parity_mismatch',
    entity: String(meta?.entity || ''),
    service: String(meta?.service || ''),
    accountId: meta?.accountId ? String(meta.accountId) : null,
    identifiers: meta?.identifiers || null,
    mismatch
  };
  console.warn(JSON.stringify(line));
  if (NODE_ENV === 'test') {
    const err = new Error(`Parity mismatch for ${line.entity} in ${line.service}`);
    err.meta = line;
    throw err;
  }
}

async function verifyParity(meta, oldFactory, newFactory, normalize = (value) => value) {
  const oldValue = await oldFactory();
  const newValue = await newFactory();
  if (ENABLE_PARITY_CHECKS) {
    logParityMismatch(meta, normalize(oldValue), normalize(newValue));
  }
  return newValue;
}

module.exports = {
  stableNormalize,
  verifyParity
};
