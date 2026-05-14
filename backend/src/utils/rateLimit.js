function nowMs() {
  return Date.now();
}

function toNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function normalizeIp(req) {
  const forwarded = String(req?.headers?.['x-forwarded-for'] || '').split(',')[0].trim();
  const remote = String(req?.socket?.remoteAddress || req?.ip || '').trim();
  return forwarded || remote || 'unknown';
}

async function hitCounterInDb({ pool, key, windowMs }) {
  const keySafe = String(key || 'unknown');
  const result = await pool.query(
    `
      INSERT INTO rate_limit_counters (bucket_key, count, reset_at)
      VALUES ($1, 1, NOW() + ($2 || ' milliseconds')::interval)
      ON CONFLICT (bucket_key)
      DO UPDATE SET
        count = CASE
          WHEN rate_limit_counters.reset_at <= NOW() THEN 1
          ELSE rate_limit_counters.count + 1
        END,
        reset_at = CASE
          WHEN rate_limit_counters.reset_at <= NOW() THEN NOW() + ($2 || ' milliseconds')::interval
          ELSE rate_limit_counters.reset_at
        END
      RETURNING count, EXTRACT(EPOCH FROM reset_at) * 1000 AS reset_at_ms
    `,
    [keySafe, String(windowMs)]
  );
  const row = result.rows[0] || {};
  return {
    count: Number(row.count || 1),
    resetAt: Number(row.reset_at_ms || (Date.now() + windowMs))
  };
}

function createRateLimiter({
  windowMs = 60_000,
  max = 60,
  keyFn = null,
  onLimit = null,
  pool = null
} = {}) {
  const hits = new Map();
  const win = toNumber(windowMs, 60_000);
  const cap = toNumber(max, 60);

  return async function rateLimiter(req, res, next) {
    const key = keyFn ? String(keyFn(req) || 'unknown') : normalizeIp(req);
    const t = nowMs();
    let count = 1;
    let resetAt = t + win;
    if (pool) {
      try {
        const row = await hitCounterInDb({ pool, key, windowMs: win });
        count = row.count;
        resetAt = row.resetAt;
      } catch {
        const bucket = hits.get(key);
        if (!bucket || bucket.resetAt <= t) hits.set(key, { count: 1, resetAt: t + win });
        else bucket.count += 1;
        const b = hits.get(key);
        count = Number(b?.count || 1);
        resetAt = Number(b?.resetAt || (t + win));
      }
    } else {
      const bucket = hits.get(key);
      if (!bucket || bucket.resetAt <= t) hits.set(key, { count: 1, resetAt: t + win });
      else bucket.count += 1;
      const b = hits.get(key);
      count = Number(b?.count || 1);
      resetAt = Number(b?.resetAt || (t + win));
    }
    if (count <= cap) return next();
    const retrySeconds = Math.max(1, Math.ceil((resetAt - t) / 1000));
    res.setHeader('Retry-After', String(retrySeconds));
    if (typeof onLimit === 'function') {
      try { onLimit(req, { key, retrySeconds, count }); } catch {}
    }
    return res.status(429).json({ error: 'Rate limit exceeded' });
  };
}

module.exports = {
  createRateLimiter
};
