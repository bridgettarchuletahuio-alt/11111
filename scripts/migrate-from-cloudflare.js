'use strict';

/**
 * migrate-from-cloudflare.js
 *
 * Migrates data from a Cloudflare D1 database to PostgreSQL.
 *
 * Tables migrated:
 *   link_sets   → link_sets
 *   click_logs  → click_logs
 *
 * Note: _cf_KV is a Cloudflare-internal table that cannot be accessed via the
 * REST API (returns SQLITE_AUTH / 403). User records are therefore NOT migrated
 * from D1. Any users that already exist in the target PostgreSQL database are
 * left untouched.
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

const CF_API_TOKEN        = process.env.CLOUDFLARE_API_TOKEN  || 'cfat_nMspXVNb865PeoqRNQw9bBduNsiLiFNB05dQjO3U91a51a31';
const CF_ACCOUNT_ID       = process.env.CLOUDFLARE_ACCOUNT_ID || '8e9de460cc8eaa1f6315477443a56bff';
const CF_D1_DATABASE_NAME = process.env.CF_D1_DATABASE_NAME   || 'chamberwu';

// ─── PostgreSQL pool (lazy — only created when running as CLI) ────────────────

function createPool() {
  const DATABASE_URL = process.env.DATABASE_URL;
  if (!DATABASE_URL) {
    throw new Error('DATABASE_URL environment variable is not set.');
  }
  return new Pool({
    connectionString: DATABASE_URL,
    ssl: DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : false,
  });
}

// Module-level pool reference used only in CLI mode (set in the CLI block below)
let pool = null;

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
 * Report existing users in PostgreSQL.
 *
 * _cf_KV is a Cloudflare-internal table that is not accessible via the D1 REST
 * API (SQLITE_AUTH / 403). User migration from D1 is therefore skipped.
 * This function simply counts the users already present in the PostgreSQL
 * database so the summary table remains informative.
 */
async function reportExistingUsers(client) {
  try {
    const res = await client.query('SELECT COUNT(*) AS cnt FROM users');
    const count = parseInt(res.rows[0].cnt, 10) || 0;
    console.log(
      `[migrate] Users: skipping D1 migration (_cf_KV is a Cloudflare-internal table). ` +
      `${count} user(s) already present in PostgreSQL.`
    );
    return { inserted: 0, skipped: count, errors: 0 };
  } catch (err) {
    // users table may not exist yet — that is fine, ensureTables will create it
    console.warn(`[migrate] Could not count existing users: ${err.message}`);
    return { inserted: 0, skipped: 0, errors: 0 };
  }
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

/**
 * Assign orphaned link_sets (owner_id IS NULL) to a specific user.
 *
 * Looks up the user by username in PostgreSQL, then runs:
 *   UPDATE link_sets SET owner_id = <id> WHERE owner_id IS NULL
 *
 * @param {import('pg').PoolClient} client
 * @param {string} username  – e.g. 'wcb8881200'
 * @returns {Promise<{ username: string, userId: number|null, updated: number }>}
 */
async function assignOrphanedLinkSets(client, username) {
  // Look up the user's id
  const userRes = await client.query(
    'SELECT id FROM users WHERE username = $1',
    [username]
  );

  if (userRes.rows.length === 0) {
    console.warn(`[migrate] User "${username}" not found in PostgreSQL — skipping orphan assignment.`);
    return { username, userId: null, updated: 0 };
  }

  const userId = userRes.rows[0].id;

  const updateRes = await client.query(
    'UPDATE link_sets SET owner_id = $1 WHERE owner_id IS NULL',
    [userId]
  );

  const updated = updateRes.rowCount || 0;
  console.log(
    `[migrate] Assigned ${updated} orphaned link_set(s) to user "${username}" (id=${userId}).`
  );

  return { username, userId, updated };
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

// ─── Core migration function (reusable) ──────────────────────────────────────

/**
 * Run the full Cloudflare D1 → PostgreSQL migration.
 *
 * @param {object} [opts]
 * @param {import('pg').Pool} [opts.pgPool]  – existing pg Pool to reuse (skips pool.end())
 * @returns {Promise<{ success: boolean, users: object, linkSets: object, clickLogs: object }>}
 */
async function runCloudflareD1Migration(opts = {}) {
  const externalPool = opts.pgPool || null;
  // Use the caller-supplied pool, or create a fresh one for CLI usage
  const localPool    = externalPool || createPool();
  const ownPool      = !externalPool;

  console.log('[migrate] Resolving D1 database UUID…');
  const databaseId = await resolveD1DatabaseId();
  console.log(`[migrate] D1 database UUID: ${databaseId}`);

  console.log('[migrate] Fetching data from Cloudflare D1 (link_sets, click_logs)…');
  const [linkSetRows, clickLogRows] = await Promise.all([
    fetchAllRows(databaseId, 'link_sets'),
    fetchAllRows(databaseId, 'click_logs'),
  ]);

  console.log('[migrate] Connecting to PostgreSQL…');
  const client = await localPool.connect();

  try {
    console.log('[migrate] Ensuring PostgreSQL tables exist…');
    await ensureTables(client);

    await client.query('BEGIN');

    // _cf_KV is a Cloudflare-internal table — skip D1 user migration entirely
    const userStats = await reportExistingUsers(client);

    console.log('[migrate] Migrating link_sets…');
    const linkSetStats = await migrateLinkSets(client, linkSetRows);
    console.log(
      `[migrate] link_sets → inserted: ${linkSetStats.inserted}, ` +
      `skipped: ${linkSetStats.skipped}, errors: ${linkSetStats.errors}`
    );

    console.log('[migrate] Assigning orphaned link_sets to user "wcb8881200"…');
    const orphanStats = await assignOrphanedLinkSets(client, 'wcb8881200');

    console.log('[migrate] Migrating click_logs…');
    const clickLogStats = await migrateClickLogs(client, clickLogRows);
    console.log(
      `[migrate] click_logs → inserted: ${clickLogStats.inserted}, ` +
      `skipped: ${clickLogStats.skipped}, errors: ${clickLogStats.errors}`
    );

    await client.query('COMMIT');

    const totalErrors = linkSetStats.errors + clickLogStats.errors;
    if (totalErrors > 0) {
      console.warn(`[migrate] ⚠  Migration completed with ${totalErrors} error(s).`);
    } else {
      console.log('[migrate] ✓  Migration completed successfully.');
    }

    return {
      success:      true,
      users:        userStats,
      linkSets:     linkSetStats,
      orphanAssign: orphanStats,
      clickLogs:    clickLogStats,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(`[migrate] FATAL: Transaction rolled back. ${err.message}`);
    throw err;
  } finally {
    client.release();
    // Only close the pool when we created it ourselves (CLI mode)
    if (ownPool) {
      await localPool.end();
    }
  }
}

module.exports = { runCloudflareD1Migration };

// ─── CLI entry-point ──────────────────────────────────────────────────────────

// Run automatically only when executed directly (not when require()'d)
if (require.main === module) {
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║        Cloudflare D1 → PostgreSQL Migration Tool        ║');
  console.log('╚══════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`[migrate] D1 database  : ${CF_D1_DATABASE_NAME}`);
  console.log(`[migrate] CF account   : ${CF_ACCOUNT_ID}`);
  const maskedUrl = (process.env.DATABASE_URL || '').replace(/([^:]+):([^@]+)@/, '<credentials>@');
  console.log(`[migrate] PostgreSQL   : ${maskedUrl}`);
  console.log('[migrate] Tables       : link_sets, click_logs (users skipped — _cf_KV inaccessible)');
  console.log('');

  runCloudflareD1Migration().then((stats) => {
    console.log('');
    console.log('┌──────────────────────────────────────────────────────────┐');
    console.log('│                    Migration Summary                     │');
    console.log('├──────────────────┬──────────┬──────────┬────────────────┤');
    console.log('│ Table            │ Inserted │ Skipped  │ Errors         │');
    console.log('├──────────────────┼──────────┼──────────┼────────────────┤');
    console.log(`│ users (pg only)  │ ${String(stats.users.inserted).padEnd(8)} │ ${String(stats.users.skipped).padEnd(8)} │ ${String(stats.users.errors).padEnd(14)} │`);
    console.log(`│ link_sets        │ ${String(stats.linkSets.inserted).padEnd(8)} │ ${String(stats.linkSets.skipped).padEnd(8)} │ ${String(stats.linkSets.errors).padEnd(14)} │`);
    console.log(`│ click_logs       │ ${String(stats.clickLogs.inserted).padEnd(8)} │ ${String(stats.clickLogs.skipped).padEnd(8)} │ ${String(stats.clickLogs.errors).padEnd(14)} │`);
    console.log('└──────────────────┴──────────┴──────────┴────────────────┘');
    console.log('');
    console.log(
      `Orphan assignment: ${stats.orphanAssign.updated} link_set(s) assigned to ` +
      `"${stats.orphanAssign.username}" (id=${stats.orphanAssign.userId}).`
    );
    console.log('Note: users were not migrated from D1 (_cf_KV is inaccessible via API).');
  }).catch((err) => {
    console.error('[migrate] Unhandled error:', err);
    process.exit(1);
  });
}
