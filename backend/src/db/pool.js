const { Pool } = require('pg');

const DATABASE_URL = String(process.env.DATABASE_URL || 'postgres://postgres:postgres@127.0.0.1:5432/relay_dashboard').trim();
const NODE_ENV = String(process.env.NODE_ENV || 'development').trim().toLowerCase();
const IS_PROD = NODE_ENV === 'production';

const pool = new Pool({
  connectionString: DATABASE_URL,
  max: Number(process.env.PG_POOL_MAX || 20),
  idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30_000),
  connectionTimeoutMillis: Number(process.env.PG_CONNECT_TIMEOUT_MS || 5_000),
  ssl: IS_PROD ? { rejectUnauthorized: false } : false
});

pool.on('error', (err) => {
  console.error('[db] unexpected idle client error:', err?.message || err);
});

module.exports = {
  pool
};
