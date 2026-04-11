'use strict';

const express = require('express');
const { Pool } = require('pg');
const crypto = require('crypto');
const { promisify } = require('util');

const scrypt = promisify(crypto.scrypt);

const { runDataMigration } = require('./migration');
const { runCloudflareD1Migration } = require('./scripts/migrate-from-cloudflare');

// ─── Database ────────────────────────────────────────────────────────────────

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('railway')
    ? { rejectUnauthorized: false }
    : false
});

// ─── Password helpers (scrypt) ────────────────────────────────────────────────

const SCRYPT_KEYLEN = 64;
const SCRYPT_PARAMS = { N: 16384, r: 8, p: 1 };

async function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const derived = await scrypt(password, salt, SCRYPT_KEYLEN, SCRYPT_PARAMS);
  return `${salt}:${derived.toString('hex')}`;
}

async function verifyPassword(password, stored) {
  const [salt, hash] = stored.split(':');
  if (!salt || !hash) return false;
  const derived = await scrypt(password, salt, SCRYPT_KEYLEN, SCRYPT_PARAMS);
  const derivedHex = derived.toString('hex');
  // Constant-time comparison
  return crypto.timingSafeEqual(
    Buffer.from(derivedHex, 'hex'),
    Buffer.from(hash, 'hex')
  );
}

// ─── UUID ─────────────────────────────────────────────────────────────────────

function newId() {
  return crypto.randomUUID().replace(/-/g, '').slice(0, 12);
}

// ─── Express app ─────────────────────────────────────────────────────────────

const app = express();

// Parse JSON bodies — accept both application/json and text/plain (frontend sends text/plain)
app.use((req, res, next) => {
  let data = '';
  req.on('data', chunk => { data += chunk; });
  req.on('end', () => {
    if (data) {
      try {
        req.body = JSON.parse(data);
      } catch {
        req.body = {};
      }
    } else {
      req.body = {};
    }
    next();
  });
});

// CORS
app.use((req, res, next) => {
  const origin = req.headers.origin || '*';
  res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Admin-Token,X-User-Password');
  res.setHeader('Vary', 'Origin');
  if (req.method === 'OPTIONS') {
    return res.status(204).end();
  }
  next();
});

// ─── Static files ─────────────────────────────────────────────────────────────

app.get('/', (req, res) => {
  res.sendFile(__dirname + '/index.html');
});

app.use(express.static(__dirname));

// ─── Health check ─────────────────────────────────────────────────────────────

app.get('/health', (req, res) => {
  res.json({ ok: true, service: 'link-dispatch-railway' });
});

// ─── Redirect route ───────────────────────────────────────────────────────────

app.get('/r/:id', async (req, res) => {
  const id = req.params.id;
  try {
    const result = await nextUrlInternal(id, {
      ua: req.headers['user-agent'] || '',
      ref: req.headers['referer'] || '',
      ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''
    });
    res.setHeader('Cache-Control', 'no-store');
    res.redirect(302, result.url);
  } catch (err) {
    res.status(err.status || 500).json({ error: err.message || 'Internal error' });
  }
});

// ─── Main API endpoint ────────────────────────────────────────────────────────

app.post('/api', async (req, res) => {
  try {
    const payload = req.body || {};
    const action = String(payload.action || '').trim();
    const result = await handleAction(action, payload, req);
    res.json(result);
  } catch (err) {
    const status = err.status || 500;
    res.status(status).json({ error: err.message || 'Internal error' });
  }
});

// ─── Cloudflare D1 migration endpoint ────────────────────────────────────────

app.post('/api/migrate', async (req, res) => {
  try {
    console.log('[/api/migrate] Starting Cloudflare D1 → PostgreSQL migration…');
    const result = await runCloudflareD1Migration({ pgPool: pool });
    console.log('[/api/migrate] Migration finished successfully.');
    res.json(result);
  } catch (err) {
    console.error('[/api/migrate] Migration failed:', err.message);
    res.status(500).json({ success: false, error: err.message || 'Migration failed' });
  }
});

// ─── Action dispatcher ────────────────────────────────────────────────────────

async function handleAction(action, payload, req) {
  switch (action) {
    case 'register':
      return handleRegister(payload);
    case 'login':
      return handleLogin(payload);
    case 'createSet':
      return handleCreateSet(payload, req);
    case 'listSets':
      return handleListSets(payload, req);
    case 'nextUrl':
      return handleNextUrl(payload, req);
    case 'getStats':
      return handleGetStats(payload, req);
    case 'updateSet':
      return handleUpdateSet(payload, req);
    case 'listUsers':
      return handleListUsers(payload, req);
    case 'authorizeUser':
      return handleAuthorizeUser(payload, req);
    case 'revokeUser':
      return handleRevokeUser(payload, req);
    default:
      throw httpError(400, 'Unsupported action');
  }
}

// ─── Auth: register ───────────────────────────────────────────────────────────

async function handleRegister(payload) {
  const username = String(payload.username || '').trim();
  const password = String(payload.password || '').trim();

  if (!username || !password) {
    throw httpError(400, '用户名和密码不能为空');
  }
  if (username.length > 32 || password.length > 128) {
    throw httpError(400, '用户名或密码过长');
  }

  const client = await pool.connect();
  try {
    // Check if username already exists
    const existing = await client.query(
      'SELECT id FROM users WHERE username = $1',
      [username]
    );
    if (existing.rows.length > 0) {
      throw httpError(409, '用户名已存在');
    }

    // Determine role: first user is admin
    const countResult = await client.query('SELECT COUNT(*) AS cnt FROM users');
    const isFirst = Number(countResult.rows[0].cnt) === 0;
    const role = isFirst ? 'admin' : 'user';
    const isAuthorized = isFirst; // 管理员自动授权

    const passwordHash = await hashPassword(password);
    const now = new Date().toISOString();

    const insertResult = await client.query(
      'INSERT INTO users (username, password_hash, role, is_authorized, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      [username, passwordHash, role, isAuthorized, now]
    );

    const userId = insertResult.rows[0].id;
    return {
      ok: true,
      user: { id: userId, username, role }
    };
  } finally {
    client.release();
  }
}

// ─── Auth: login ──────────────────────────────────────────────────────────────

async function handleLogin(payload) {
  const username = String(payload.username || '').trim();
  const password = String(payload.password || '').trim();

  if (!username || !password) {
    throw httpError(400, '用户名和密码不能为空');
  }

  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT id, username, password_hash, role, is_authorized FROM users WHERE username = $1',
      [username]
    );
    if (result.rows.length === 0) {
      throw httpError(401, '用户名不存在');
    }

    const user = result.rows[0];
    const valid = await verifyPassword(password, user.password_hash);
    if (!valid) {
      throw httpError(401, '密码错误');
    }

    if (!user.is_authorized) {
      throw httpError(401, '账号未授权，请联系管理员');
    }

    return {
      ok: true,
      user: { id: user.id, username: user.username, role: user.role }
    };
  } finally {
    client.release();
  }
}

// ─── Resolve authenticated user from payload ──────────────────────────────────

async function resolveUser(payload) {
  const username = String(payload.username || '').trim();
  const password = String(payload.password || '').trim();

  if (!username || !password) {
    throw httpError(401, '未登录，请先登录');
  }

  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT id, username, password_hash, role FROM users WHERE username = $1',
      [username]
    );
    if (result.rows.length === 0) {
      throw httpError(401, '用户名不存在');
    }

    const user = result.rows[0];
    const valid = await verifyPassword(password, user.password_hash);
    if (!valid) {
      throw httpError(401, '密码错误');
    }

    return user;
  } finally {
    client.release();
  }
}

// ─── createSet ────────────────────────────────────────────────────────────────

async function handleCreateSet(payload, req) {
  const user = await resolveUser(payload);

  const links = sanitizeLinks(payload.links);
  const name = typeof payload.name === 'string' ? payload.name.trim().slice(0, 120) : '';
  const id = newId();
  const now = new Date().toISOString();

  const client = await pool.connect();
  try {
    await client.query(
      'INSERT INTO link_sets (id, name, links_json, current_index, click_count, created_at, updated_at, owner_id) VALUES ($1, $2, $3, 0, 0, $4, $5, $6)',
      [id, name, JSON.stringify(links), now, now, user.id]
    );
  } finally {
    client.release();
  }

  return { ok: true, id };
}

// ─── updateSet ────────────────────────────────────────────────────────────────

async function handleUpdateSet(payload, req) {
  const user = await resolveUser(payload);

  const id = sanitizeId(String(payload.id || '').trim());
  const links = sanitizeLinks(payload.links);
  const name = typeof payload.name === 'string' ? payload.name.trim().slice(0, 120) : '';
  const now = new Date().toISOString();

  const client = await pool.connect();
  try {
    // Verify the set exists and belongs to the current user
    const existing = await client.query(
      'SELECT id, owner_id FROM link_sets WHERE id = $1',
      [id]
    );
    if (existing.rows.length === 0) {
      throw httpError(404, '链接集合不存在');
    }
    if (existing.rows[0].owner_id !== user.id) {
      throw httpError(403, '无权修改该链接集合');
    }

    await client.query(
      'UPDATE link_sets SET name = $1, links_json = $2, updated_at = $3 WHERE id = $4 AND owner_id = $5',
      [name, JSON.stringify(links), now, id, user.id]
    );
  } finally {
    client.release();
  }

  return { ok: true, id };
}

// ─── listSets ─────────────────────────────────────────────────────────────────

async function handleListSets(payload, req) {
  const user = await resolveUser(payload);
  const limit = clampLimit(Number(payload.limit || 20));

  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT id, name, links_json, current_index, click_count, created_at, updated_at FROM link_sets WHERE owner_id = $1 ORDER BY created_at DESC LIMIT $2',
      [user.id, limit]
    );
    const rows = result.rows;

    const items = rows.map(row => {
      const links = safeParseArray(row.links_json);
      return {
        id: row.id,
        name: row.name || '',
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        currentIndex: Number(row.current_index || 0),
        clickCount: Number(row.click_count || 0),
        count: links.length,
        links
      };
    });

    return { items };
  } finally {
    client.release();
  }
}

// ─── nextUrl (internal) ───────────────────────────────────────────────────────

async function nextUrlInternal(rawId, meta) {
  const id = sanitizeId(rawId);
  const client = await pool.connect();
  try {
    // Fetch current set
    const setResult = await client.query(
      'SELECT id, links_json, current_index FROM link_sets WHERE id = $1',
      [id]
    );
    if (setResult.rows.length === 0) {
      throw httpError(404, 'Link set not found');
    }

    const row = setResult.rows[0];
    const links = safeParseArray(row.links_json);
    if (links.length === 0) {
      throw httpError(409, 'Link set is empty');
    }

    const currentIndex = normalizeIndex(Number(row.current_index || 0), links.length);
    const url = links[currentIndex];

    if (!url || !/^https?:\/\//i.test(url)) {
      throw httpError(409, 'Stored URL is invalid');
    }

    const nextIndex = (currentIndex + 1) % links.length;
    const now = new Date().toISOString();
    const logId = crypto.randomUUID();

    const ua = typeof meta.ua === 'string' ? meta.ua.slice(0, 500) : '';
    const ref = typeof meta.ref === 'string' ? meta.ref.slice(0, 1000) : '';
    const ip = typeof meta.ip === 'string' ? meta.ip.slice(0, 100) : '';
    const ipHash = ip ? crypto.createHash('sha256').update(ip).digest('hex') : '';

    // Update index and log click
    await client.query(
      'UPDATE link_sets SET current_index = $1, click_count = COALESCE(click_count, 0) + 1, updated_at = $2 WHERE id = $3',
      [nextIndex, now, id]
    );

    await client.query(
      'INSERT INTO click_logs (log_id, set_id, link_index, url, clicked_at, ua, ref, ip_hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)',
      [logId, id, currentIndex, url, now, ua, ref, ipHash]
    );

    return { ok: true, id, index: currentIndex, url, nextUrl: url };
  } finally {
    client.release();
  }
}

// ─── nextUrl (API action) ─────────────────────────────────────────────────────

async function handleNextUrl(payload, req) {
  const rawId = String(payload.id || payload.set_id || '').trim();
  if (!rawId) {
    throw httpError(400, '缺少 id 参数');
  }

  const meta = {
    ua: payload.ua || req.headers['user-agent'] || '',
    ref: payload.ref || req.headers['referer'] || '',
    ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''
  };

  return nextUrlInternal(rawId, meta);
}

// ─── getStats ─────────────────────────────────────────────────────────────────

async function handleGetStats(payload, req) {
  const rawId = String(payload.id || payload.set_id || '').trim();
  if (!rawId) {
    throw httpError(400, '缺少 id 参数');
  }

  const id = sanitizeId(rawId);
  const client = await pool.connect();
  try {
    const setResult = await client.query(
      'SELECT links_json FROM link_sets WHERE id = $1',
      [id]
    );
    if (setResult.rows.length === 0) {
      throw httpError(404, 'Link set not found');
    }

    const links = safeParseArray(setResult.rows[0].links_json);

    const logsResult = await client.query(
      'SELECT link_index, COUNT(*) AS clicks, MAX(clicked_at) AS last_clicked_at FROM click_logs WHERE set_id = $1 GROUP BY link_index ORDER BY link_index ASC',
      [id]
    );

    const counts = new Map(
      logsResult.rows.map(row => [
        Number(row.link_index || 0),
        { clicks: Number(row.clicks || 0), lastClickedAt: row.last_clicked_at || null }
      ])
    );

    const stats = links.map((url, index) => {
      const stat = counts.get(index) || { clicks: 0, lastClickedAt: null };
      return { index, url, clicks: stat.clicks, lastClickedAt: stat.lastClickedAt };
    });

    return { id, stats };
  } finally {
    client.release();
  }
}

// ─── Admin: listUsers ─────────────────────────────────────────────────────────

async function handleListUsers(payload, req) {
  const admin = await resolveUser(payload);
  if (admin.role !== 'admin') {
    throw httpError(403, '只有管理员可以管理账号');
  }

  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT id, username, role, is_authorized, created_at FROM users ORDER BY created_at DESC'
    );
    return { users: result.rows };
  } finally {
    client.release();
  }
}

// ─── Admin: authorizeUser ─────────────────────────────────────────────────────

async function handleAuthorizeUser(payload, req) {
  const admin = await resolveUser(payload);
  if (admin.role !== 'admin') {
    throw httpError(403, '只有管理员可以管理账号');
  }

  const userId = Number(payload.userId || 0);
  if (!userId) {
    throw httpError(400, '缺少 userId 参数');
  }

  const client = await pool.connect();
  try {
    await client.query(
      'UPDATE users SET is_authorized = true WHERE id = $1',
      [userId]
    );
    return { ok: true };
  } finally {
    client.release();
  }
}

// ─── Admin: revokeUser ────────────────────────────────────────────────────────

async function handleRevokeUser(payload, req) {
  const admin = await resolveUser(payload);
  if (admin.role !== 'admin') {
    throw httpError(403, '只有管理员可以管理账号');
  }

  const userId = Number(payload.userId || 0);
  if (!userId) {
    throw httpError(400, '缺少 userId 参数');
  }

  // 防止管理员撤销自己的权限
  if (admin.id === userId) {
    throw httpError(400, '无法撤销自己的权限');
  }

  const client = await pool.connect();
  try {
    await client.query(
      'UPDATE users SET is_authorized = false WHERE id = $1',
      [userId]
    );
    return { ok: true };
  } finally {
    client.release();
  }
}

// ─── Utilities ────────────────────────────────────────────────────────────────

function sanitizeId(value) {
  const id = String(value || '').trim();
  if (!/^[a-zA-Z0-9_-]{4,64}$/.test(id)) {
    throw httpError(400, 'Invalid id');
  }
  return id;
}

function sanitizeLinks(links) {
  console.log('[sanitizeLinks] Input type:', typeof links);
  console.log('[sanitizeLinks] Input value:', JSON.stringify(links).slice(0, 200));

  // 检查是否是无效的对象字符串表示
  if (typeof links === 'string' && links === '[object Object]') {
    throw httpError(400, '链接数据格式错误：前端发送了无效的对象表示');
  }

  // 如果是字符串，按换行符分割
  if (typeof links === 'string') {
    console.log('[sanitizeLinks] Converting string to array...');
    links = links.split('\n').map(line => line.trim()).filter(Boolean);
    console.log('[sanitizeLinks] After split:', links.length, 'items');
  }

  if (!Array.isArray(links)) {
    throw httpError(400, 'links 必须是数组或字符串');
  }
  const clean = links
    .map(item => String(item || '').trim())
    .filter(Boolean)
    .filter(item => /^https?:\/\//i.test(item));

  if (clean.length === 0) {
    throw httpError(400, '至少需要一个有效的 http/https 链接');
  }
  if (clean.length > 500) {
    throw httpError(400, '链接数量不能超过 500');
  }
  return clean;
}

function safeParseArray(value) {
  try {
    const parsed = JSON.parse(value || '[]');
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function normalizeIndex(value, length) {
  const parsed = Number(value || 0);
  if (!Number.isFinite(parsed) || parsed < 0) return 0;
  return Math.floor(parsed) % length;
}

function clampLimit(value) {
  if (!Number.isFinite(value) || value <= 0) return 20;
  return Math.min(Math.floor(value), 100);
}

function httpError(status, message) {
  const err = new Error(message);
  err.status = status;
  return err;
}

// ─── Startup migration ────────────────────────────────────────────────────────

async function runMigrations() {
  const client = await pool.connect();
  try {
    // Ensure tables exist (idempotent)
    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user',
        created_at TEXT NOT NULL
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS link_sets (
        id TEXT PRIMARY KEY,
        name TEXT DEFAULT '',
        links_json TEXT NOT NULL,
        current_index INTEGER NOT NULL DEFAULT 0,
        click_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS click_logs (
        log_id TEXT PRIMARY KEY,
        set_id TEXT NOT NULL,
        link_index INTEGER NOT NULL,
        url TEXT NOT NULL,
        clicked_at TEXT NOT NULL,
        ua TEXT DEFAULT '',
        ref TEXT DEFAULT '',
        ip_hash TEXT DEFAULT ''
      )
    `);

    // Add owner_id column to link_sets if it doesn't exist
    await client.query(`
      ALTER TABLE link_sets ADD COLUMN IF NOT EXISTS owner_id INTEGER
    `);

    // Add is_authorized column to users if it doesn't exist
    await client.query(`
      ALTER TABLE users ADD COLUMN IF NOT EXISTS is_authorized BOOLEAN DEFAULT false
    `);

    // Indexes
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_click_logs_set_id ON click_logs(set_id)
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_link_sets_owner_id ON link_sets(owner_id)
    `);

    console.log('Database migrations complete');
  } catch (err) {
    console.error('Migration error:', err.message);
  } finally {
    client.release();
  }
}

// ─── Start server ─────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;

runMigrations().then(() => {
  return runDataMigration(pool);
}).then(() => {
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}).catch(err => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
