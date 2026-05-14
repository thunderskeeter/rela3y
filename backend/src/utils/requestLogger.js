const { generateId } = require('./id');

function stripQuery(url) {
  const raw = String(url || '');
  const idx = raw.indexOf('?');
  return idx >= 0 ? raw.slice(0, idx) : raw;
}

function requestLogger(req, res, next) {
  const startedAt = Date.now();
  const requestId = String(req?.headers?.['x-request-id'] || '').trim() || generateId();
  req.requestId = requestId;
  res.setHeader('x-request-id', requestId);
  res.on('finish', () => {
    const line = {
      level: 'info',
      type: 'http_request',
      requestId,
      method: req.method,
      path: stripQuery(req.originalUrl || req.url),
      status: res.statusCode,
      durationMs: Date.now() - startedAt,
      tenantId: req?.tenant?.accountId || null,
      userId: req?.user?.id || null
    };
    console.log(JSON.stringify(line));
  });
  return next();
}

module.exports = {
  requestLogger
};
