const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '..', '.env') });
const { pool } = require('./pool');

const MIGRATIONS_DIR = path.join(__dirname, '..', '..', 'db', 'migrations');

async function ensureMigrationTable(client = pool) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

function listMigrations() {
  const files = fs.readdirSync(MIGRATIONS_DIR)
    .filter((f) => f.endsWith('.sql'))
    .sort();
  return files.map((file) => ({
    version: file,
    sql: fs.readFileSync(path.join(MIGRATIONS_DIR, file), 'utf8')
  }));
}

async function runMigrations() {
  const client = await pool.connect();
  try {
    await client.query("SELECT pg_advisory_lock(hashtext('relay_schema_migrations'))");
    await ensureMigrationTable(client);
    const applied = await client.query('SELECT version FROM schema_migrations');
    const appliedSet = new Set(applied.rows.map((r) => String(r.version)));
    const pending = listMigrations().filter((m) => !appliedSet.has(m.version));
    for (const migration of pending) {
      try {
        await client.query('BEGIN');
        await client.query(migration.sql);
        await client.query('INSERT INTO schema_migrations(version) VALUES($1)', [migration.version]);
        await client.query('COMMIT');
        console.log(`[db:migrate] applied ${migration.version}`);
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      }
    }
    if (!pending.length) console.log('[db:migrate] no pending migrations');
  } finally {
    try {
      await client.query("SELECT pg_advisory_unlock(hashtext('relay_schema_migrations'))");
    } catch {}
    client.release();
  }
}

if (require.main === module) {
  runMigrations()
    .then(async () => {
      await pool.end();
      process.exit(0);
    })
    .catch(async (err) => {
      console.error('[db:migrate] failed:', err?.stack || err?.message || err);
      await pool.end();
      process.exit(1);
    });
}

module.exports = {
  runMigrations
};
