async function withTransaction(db, fn) {
  const client = typeof db?.connect === 'function' ? await db.connect() : db;
  const ownsClient = client !== db;
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    throw err;
  } finally {
    if (ownsClient && typeof client?.release === 'function') {
      client.release();
    }
  }
}

module.exports = {
  withTransaction
};
