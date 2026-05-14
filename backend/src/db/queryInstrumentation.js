function instrumentDb(db, onQuery) {
  if (!db || typeof db.query !== 'function') {
    throw new Error('instrumentDb requires a db object with query(sql, params)');
  }
  const listener = typeof onQuery === 'function' ? onQuery : () => {};
  return {
    ...db,
    async query(sql, params) {
      listener({ sql: String(sql || ''), params: Array.isArray(params) ? params : [] });
      return db.query(sql, params);
    }
  };
}

async function withQueryCount(db, fn) {
  let count = 0;
  const queries = [];
  const instrumented = instrumentDb(db, (entry) => {
    count += 1;
    queries.push(entry);
  });
  const result = await fn(instrumented);
  return { result, count, queries };
}

module.exports = {
  instrumentDb,
  withQueryCount
};
