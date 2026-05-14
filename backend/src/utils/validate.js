const { z } = require('zod');

function formatError(error) {
  const issues = Array.isArray(error?.issues) ? error.issues : [];
  if (!issues.length) return 'Invalid request payload';
  return issues
    .slice(0, 6)
    .map((i) => `${(i.path || []).join('.') || 'payload'}: ${i.message}`)
    .join('; ');
}

function validateBody(schema) {
  return function bodyValidator(req, res, next) {
    const parsed = schema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: formatError(parsed.error) });
    }
    req.body = parsed.data;
    return next();
  };
}

function validateQuery(schema) {
  return function queryValidator(req, res, next) {
    const parsed = schema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).json({ error: formatError(parsed.error) });
    }
    req.query = parsed.data;
    return next();
  };
}

function validateParams(schema) {
  return function paramsValidator(req, res, next) {
    const parsed = schema.safeParse(req.params);
    if (!parsed.success) {
      return res.status(400).json({ error: formatError(parsed.error) });
    }
    req.params = parsed.data;
    return next();
  };
}

module.exports = {
  z,
  validateBody,
  validateQuery,
  validateParams
};
