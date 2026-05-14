async function claim(db, accountId, provider, eventId) {
  const insert = await db.query(
    `
      INSERT INTO webhook_receipts (account_id, provider, event_id)
      VALUES ($1, $2, $3)
      ON CONFLICT DO NOTHING
      RETURNING first_seen_at
    `,
    [String(accountId), String(provider), String(eventId)]
  );
  if (insert.rowCount > 0) {
    await db.query(`DELETE FROM webhook_receipts WHERE first_seen_at < NOW() - INTERVAL '30 days'`);
    return { ok: true, duplicate: false };
  }
  const existing = await db.query(
    `
      SELECT EXTRACT(EPOCH FROM first_seen_at) * 1000 AS first_seen_ms
      FROM webhook_receipts
      WHERE account_id = $1 AND provider = $2 AND event_id = $3
      LIMIT 1
    `,
    [String(accountId), String(provider), String(eventId)]
  );
  return {
    ok: false,
    duplicate: true,
    firstSeenAt: Number(existing.rows[0]?.first_seen_ms || Date.now())
  };
}

module.exports = {
  claim
};
