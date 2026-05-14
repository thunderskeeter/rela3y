const {
  resolveTenantSelector,
  requireTenant: requireTenantMiddleware,
  requireTenantForWebhook
} = require('./accountContext');

function resolveAccount(req, opts = {}) {
  return resolveTenantSelector(req, opts);
}

function requireTenant(req) {
  const accountId = String(req?.tenant?.accountId || '').trim();
  const to = String(req?.tenant?.to || '').trim();
  if (!accountId || !to) {
    const err = new Error('Missing tenant context on request');
    err.status = 400;
    err.code = 'TENANT_REQUIRED';
    throw err;
  }
  return req.tenant;
}

function assertTenantScope(expectedTenantId, actualTenantId, { entity = 'entity', status = 403 } = {}) {
  const expected = String(expectedTenantId || '').trim();
  const actual = String(actualTenantId || '').trim();
  if (!expected || !actual || expected !== actual) {
    const err = new Error(`Tenant scope mismatch for ${entity}`);
    err.status = status;
    err.code = 'TENANT_SCOPE_MISMATCH';
    err.expectedTenantId = expected || null;
    err.actualTenantId = actual || null;
    throw err;
  }
  return true;
}

module.exports = {
  resolveAccount,
  requireTenantMiddleware,
  requireTenant,
  assertTenantScope,
  requireTenantForWebhook
};
