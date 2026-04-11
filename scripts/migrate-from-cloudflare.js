'use strict';

/**
 * migrate-from-cloudflare.js
 *
 * Migrates data from a Cloudflare D1 database to PostgreSQL.
 *
 * Tables migrated:
 *   _cf_KV      → users  (key = username, value = JSON with password_hash / role)
 *   link_sets   → link_sets
 *   click_logs  → click_logs
 *
 * Usage:
 *   CLOUDFLARE_API_TOKEN=<token> CLOUDFLARE_ACCOUNT_ID=<id> DATABASE_URL=<pg_url> \
 *     node scripts/migrate-from-cloudflare.js
 *
 * Environment variables:
 *   DATABASE_URL           – PostgreSQL connection string (required)
 *   CLOUDFLARE_API_TOKEN   – Cloudflare API token with D1 read access (required)
 *   CLOUDFLARE_ACCOUNT_ID  – Cloudflare account ID (required)
 *   CF_D1_DATABASE_NAME    – D1 database name (default: chamberwu)
 */

const { Pool } = require('pg');
const https = require('https');

// ─── Config ───────────────────────────────────────────────────────────────────

const DATABASE_URL        = process.env.DATABASE_URL;
const CF_API_TOKEN        = process.env.CLOUDFLARE_API_TOKEN  || 'cfat_nMspXVNb865PeoqRNQw9bBduNsiLiFNB05dQjO3U91a51a31';
const CF_ACCOUNT_ID       = process.env.CLOUDFLARE_ACCOUNT_ID || '8e9de460cc8eaa1f6315477443a56bff';
const CF_D1_DATABASE_NAME = process.env.CF_D1_DATABASE_NAME   || 'chamberwu';

if (!DATABASE_URL) {
  console.error('[migrate] ERROR: DATABASE_URL environment variable is not set.');
  process.exit(1);
}

// ─── PostgreSQL pool ──────────────────────────────────────────────────────────

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : false,
});

// ─── Cloudflare D1 HTTP helper ────────────────────────────────────────────────

/**
 * Perform an HTTPS request and return the parsed JSON body.
 */
function httpsRequest(options, body) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(data) });
        } catch (e) {
          reject(new Error(`Failed to parse response: ${data}`));
        }
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

/**
 * Resolve the D1 database UUID from its human-readable name.
 */
async function resolveD1DatabaseId() {
  const options = {
    hostname: 'api.cloudflare.com',
    path: `/client/v4/accounts/${CF_ACCOUNT_ID}/d1/database?name=${encodeURIComponent(CF_D1_DATABASE_NAME)}`,
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${CF_API_TOKEN}`,
      'Content-Type': 'application/json',
    },
  };

  const { status, body } = await httpsRequest(options);

  if (status !== 200 || !body.success) {
    throw new Error(
      `Failed to list D1 databases (HTTP ${status}): ${JSON.stringify(body.errors || body)}`
    );
  }

  const databases = body.result || [];
  const db = databases.find((d) => d.name === CF_D1_DATABASE_NAME);

  if (!db) {
    throw new Error(
      `D1 database "${CF_D1_DATABASE_NAME}" not found. Available: ${databases.map((d) => d.name).join(', ') || '(none)'}`
    );
  }

  return db.uuid;
}

/**
 * Execute a SQL query against the D1 database via the REST API.
 * Returns an array of result objects (one per SQL statement).
 */
async function d1Query(databaseId, sql, params = []) {
  const payload = JSON.stringify({ sql, params });

  const options = {
    hostname: 'api.cloudflare.com',
    path: `/client/v4/accounts/${CF_ACCOUNT_ID}/d1/database/${databaseId}/query`,
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${CF_API_TOKEN}`,
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload),
    },
  };

  const { status, body } = await httpsRequest(options, payload);

  if (status !== 200 || !body.success) {
    throw new Error(
      `D1 query failed (HTTP ${status}): ${JSON.stringify(body.errors || body)}\nSQL: ${sql}`
    );
  }

  return body.result; // array of { results: [...], success, meta }
}

/**
 * Fetch all rows from a D1 table.
 * Returns [] if the table does not exist (graceful degradation).
 */
async function fetchAllRows(databaseId, tableName) {
  try {
    const results = await d1Query(databaseId, `SELECT * FROM "${tableName}"`);
    const rows = (results[0] && results[0].results) ? results[0].results : [];
    console.log(`[migrate] Fetched ${rows.length} row(s) from D1 table "${tableName}".`);
    return rows;
  } catch (err) {
    if (err.message.includes('no such table') || err.message.includes('does not exist')) {
      console.warn(`[migrate] Table "${tableName}" not found in D1 — skipping.`);
      return [];
    }
    throw err;
  }
}

// ─── Migration helpers ────────────────────────────────────────────────────────

/**
 * Migrate _cf_KV → users
 *
 * Expected _cf_KV schema:
 *   key   TEXT  – username (or prefixed key like "user:alice")
 *   value TEXT  – JSON string, e.g. { "password_hash": "...", "role": "admin", "created_at": "..." }
 *
 * Rows whose value cannot be parsed as a user record are skipped.
 */
async function migrateUsers(client, kvRows) {
  let inserted = 0;
  let skipped  = 0;
  let errors   = 0;

  for (const row of kvRows) {
    // Support both plain username keys and "user:<name>" prefixed keys
    const rawKey = String(row.key || '');
    const username = rawKey.startsWith('user:') ? rawKey.slice(5) : rawKey;

    if (!username) {
      skipped++;
      continue;
    }

    let value;
    try {
      value = typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
    } catch {
      console.warn(`[migrate] _cf_KV row key="${rawKey}" — value is not valid JSON, skipping.`);
      skipped++;
      continue;
    }

    // Must look like a user record
    if (!value || typeof value !== 'object' || !value.password_hash) {
      skipped++;
      continue;
    }

    const passwordHash = String(value.password_hash || '');
    const role         = String(value.role || 'user');
    const createdAt    = String(value.created_at || new Date().toISOString());

    try {
      await client.query(
        `INSERT INTO users (username, password_hash, role, created_at)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (username) DO NOTHING`,
        [username, passwordHash, role, createdAt]
      );
      inserted++;
    } catch (err) {
      console.error(`[migrate] Failed to insert user "${username}": ${err.message}`);
      errors++;
    }
  }

  return { inserted, skipped, errors };
}

/**
 * Migrate link_sets rows.
 *
 * D1 link_sets schema (SQLite):
 *   id            TEXT
 *   name          TEXT
 *   links_json    TEXT
 *   current_index INTEGER
 *   click_count   INTEGER
 *   created_at    TEXT
 *   updated_at    TEXT
 *   owner_id      INTEGER  (may be NULL)
 *
 * PostgreSQL link_sets schema is identical — direct copy.
 */
async function migrateLinkSets(client, rows) {
  let inserted = 0;
  let skipped  = 0;
  let errors   = 0;

  for (const row of rows) {
    const id           = String(row.id || '');
    const name         = String(row.name || '');
    const linksJson    = String(row.links_json || '[]');
    const currentIndex = Number(row.current_index || 0);
    const clickCount   = Number(row.click_count   || 0);
    const createdAt    = String(row.created_at    || new Date().toISOString());
    const updatedAt    = String(row.updated_at    || new Date().toISOString());
    // owner_id may be NULL in D1; keep it NULL — the startup migration will
    // reassign orphaned sets to the admin user automatically.
    const ownerId      = row.owner_id != null ? Number(row.owner_id) : null;

    if (!id) {
      skipped++;
      continue;
    }

    try {
      const result = await client.query(
        `INSERT INTO link_sets
           (id, name, links_json, current_index, click_count, created_at, updated_at, owner_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (id) DO NOTHING`,
        [id, name, linksJson, currentIndex, clickCount, createdAt, updatedAt, ownerId]
      );
      if (result.rowCount > 0) {
        inserted++;
      } else {
        skipped++; // already exists
      }
    } catch (err) {
      console.error(`[migrate] Failed to insert link_set id="${id}": ${err.message}`);
      errors++;
    }
  }

  return { inserted, skipped, errors };
}

/**
 * Migrate click_logs rows.
 *
 * D1 click_logs schema (SQLite):
 *   log_id       TEXT
 *   set_id       TEXT
 *   link_index   INTEGER
 *   url          TEXT
 *   clicked_at   TEXT
 *   ua           TEXT
 *   ref          TEXT
 *   ip_hash      TEXT
 */
async function migrateClickLogs(client, rows) {
  let inserted = 0;
  let skipped  = 0;
  let errors   = 0;

  for (const row of rows) {
    const logId      = String(row.log_id     || '');
    const setId      = String(row.set_id     || '');
    const linkIndex  = Number(row.link_index || 0);
    const url        = String(row.url        || '');
    const clickedAt  = String(row.clicked_at || new Date().toISOString());
    const ua         = String(row.ua         || '');
    const ref        = String(row.ref        || '');
    const ipHash     = String(row.ip_hash    || '');

    if (!logId || !setId) {
      skipped++;
      continue;
    }

    try {
      const result = await client.query(
        `INSERT INTO click_logs
           (log_id, set_id, link_index, url, clicked_at, ua, ref, ip_hash)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (log_id) DO NOTHING`,
        [logId, setId, linkIndex, url, clickedAt, ua, ref, ipHash]
      );
      if (result.rowCount > 0) {
        inserted++;
      } else {
        skipped++; // already exists
      }
    } catch (err) {
      console.error(`[migrate] Failed to insert click_log log_id="${logId}": ${err.message}`);
      errors++;
    }
  }

  return { inserted, skipped, errors };
}

// ─── Ensure PostgreSQL tables exist ──────────────────────────────────────────

async function ensureTables(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS users (
      id            SERIAL PRIMARY KEY,
      username      TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      role          TEXT NOT NULL DEFAULT 'user',
      created_at    TEXT NOT NULL
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS link_sets (
      id            TEXT PRIMARY KEY,
      name          TEXT DEFAULT '',
      links_json    TEXT NOT NULL,
      current_index INTEGER NOT NULL DEFAULT 0,
      click_count   INTEGER NOT NULL DEFAULT 0,
      created_at    TEXT NOT NULL,
      updated_at    TEXT NOT NULL
    )
  `);

  await client.query(`
    ALTER TABLE link_sets ADD COLUMN IF NOT EXISTS owner_id INTEGER
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS click_logs (
      log_id      TEXT PRIMARY KEY,
      set_id      TEXT NOT NULL,
      link_index  INTEGER NOT NULL,
      url         TEXT NOT NULL,
      clicked_at  TEXT NOT NULL,
      ua          TEXT DEFAULT '',
      ref         TEXT DEFAULT '',
      ip_hash     TEXT DEFAULT ''
    )
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_click_logs_set_id ON click_logs(set_id)
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_link_sets_owner_id ON link_sets(owner_id)
  `);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║        Cloudflare D1 → PostgreSQL Migration Tool        ║');
  console.log('╚══════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`[migrate] D1 database  : ${CF_D1_DATABASE_NAME}`);
  console.log(`[migrate] CF account   : ${CF_ACCOUNT_ID}`);
  console.log(`[migrate] PostgreSQL   : ${DATABASE_URL.replace(/:\/\/[^@]+@/, '://<credentials>@')}`);
  console.log('');

  // 1. Resolve D1 database UUID
  console.log('[migrate] Resolving D1 database UUID…');
  let databaseId;
  try {
    databaseId = await resolveD1DatabaseId();
    console.log(`[migrate] D1 database UUID: ${databaseId}`);
  } catch (err) {
    console.error(`[migrate] ERROR: ${err.message}`);
    process.exit(1);
  }

  // 2. Fetch all rows from D1
  console.log('');
  console.log('[migrate] Fetching data from Cloudflare D1…');
  let kvRows, linkSetRows, clickLogRows;
  try {
    [kvRows, linkSetRows, clickLogRows] = await Promise.all([
      fetchAllRows(databaseId, '_cf_KV'),
      fetchAllRows(databaseId, 'link_sets'),
      fetchAllRows(databaseId, 'click_logs'),
    ]);
  } catch (err) {
    console.error(`[migrate] ERROR fetching D1 data: ${err.message}`);
    process.exit(1);
  }

  // 3. Connect to PostgreSQL and run migrations
  console.log('');
  console.log('[migrate] Connecting to PostgreSQL…');
  const client = await pool.connect();

  try {
    console.log('[migrate] Ensuring PostgreSQL tables exist…');
    await ensureTables(client);

    await client.query('BEGIN');

    // ── Users (from _cf_KV) ──────────────────────────────────────────────────
    console.log('');
    console.log('[migrate] Migrating users from _cf_KV…');
    const userStats = await migrateUsers(client, kvRows);
    console.log(
      `[migrate] Users     → inserted: ${userStats.inserted}, ` +
      `skipped: ${userStats.skipped}, errors: ${userStats.errors}`
    );

    // ── link_sets ────────────────────────────────────────────────────────────
    console.log('');
    console.log('[migrate] Migrating link_sets…');
    const linkSetStats = await migrateLinkSets(client, linkSetRows);
    console.log(
      `[migrate] link_sets → inserted: ${linkSetStats.inserted}, ` +
      `skipped: ${linkSetStats.skipped}, errors: ${linkSetStats.errors}`
    );

    // ── click_logs ───────────────────────────────────────────────────────────
    console.log('');
    console.log('[migrate] Migrating click_logs…');
    const clickLogStats = await migrateClickLogs(client, clickLogRows);
    console.log(
      `[migrate] click_logs → inserted: ${clickLogStats.inserted}, ` +
      `skipped: ${clickLogStats.skipped}, errors: ${clickLogStats.errors}`
    );

    await client.query('COMMIT');

    // ── Summary ──────────────────────────────────────────────────────────────
    console.log('');
    console.log('┌──────────────────────────────────────────────────────────┐');
    console.log('│                    Migration Summary                     │');
    console.log('├──────────────────┬──────────┬──────────┬────────────────┤');
    console.log('│ Table            │ Inserted │ Skipped  │ Errors         │');
    console.log('├──────────────────┼──────────┼──────────┼────────────────┤');
    console.log(`│ users (_cf_KV)   │ ${String(userStats.inserted).padEnd(8)} │ ${String(userStats.skipped).padEnd(8)} │ ${String(userStats.errors).padEnd(14)} │`);
    console.log(`│ link_sets        │ ${String(linkSetStats.inserted).padEnd(8)} │ ${String(linkSetStats.skipped).padEnd(8)} │ ${String(linkSetStats.errors).padEnd(14)} │`);
    console.log(`│ click_logs       │ ${String(clickLogStats.inserted).padEnd(8)} │ ${String(clickLogStats.skipped).padEnd(8)} │ ${String(clickLogStats.errors).padEnd(14)} │`);
    console.log('└──────────────────┴──────────┴──────────┴────────────────┘');

    const totalErrors = userStats.errors + linkSetStats.errors + clickLogStats.errors;
    if (totalErrors > 0) {
      console.warn(`\n[migrate] ⚠  Migration completed with ${totalErrors} error(s). Check the log above.`);
    } else {
      console.log('\n[migrate] ✓  Migration completed successfully.');
    }
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(`[migrate] FATAL: Transaction rolled back. ${err.message}`);
    console.error(err.stack);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error('[migrate] Unhandled error:', err);
  process.exit(1);
});
