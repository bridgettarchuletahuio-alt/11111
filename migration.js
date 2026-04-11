'use strict';

const crypto = require('crypto');
const { promisify } = require('util');

const scrypt = promisify(crypto.scrypt);

const SCRYPT_KEYLEN = 64;
const SCRYPT_PARAMS = { N: 16384, r: 8, p: 1 };

async function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const derived = await scrypt(password, salt, SCRYPT_KEYLEN, SCRYPT_PARAMS);
  return `${salt}:${derived.toString('hex')}`;
}

// ─── Migrate: create wcb8881200 admin user and assign orphaned link_sets ──────

async function runDataMigration(pool) {
  const TARGET_USERNAME = 'wcb8881200';
  const TARGET_PASSWORD = '20241028';
  const TARGET_ROLE = 'admin';

  const client = await pool.connect();
  try {
    // 1. Check whether the target user already exists
    const existing = await client.query(
      'SELECT id FROM users WHERE username = $1',
      [TARGET_USERNAME]
    );

    let userId;

    if (existing.rows.length > 0) {
      userId = existing.rows[0].id;
      console.log(`[migration] User "${TARGET_USERNAME}" already exists (id=${userId}), resetting password and role.`);

      // Reset password and ensure correct role, and ensure authorized
      const passwordHash = await hashPassword(TARGET_PASSWORD);
      await client.query(
        'UPDATE users SET password_hash = $1, role = $2, is_authorized = true WHERE id = $3',
        [passwordHash, TARGET_ROLE, userId]
      );
      console.log(`[migration] Password, role, and authorization updated for user "${TARGET_USERNAME}".`);
    } else {
      // 2. Create the user
      const passwordHash = await hashPassword(TARGET_PASSWORD);
      const now = new Date().toISOString();

      const insertResult = await client.query(
        'INSERT INTO users (username, password_hash, role, is_authorized, created_at) VALUES ($1, $2, $3, true, $4) RETURNING id',
        [TARGET_USERNAME, passwordHash, TARGET_ROLE, now]
      );

      userId = insertResult.rows[0].id;
      console.log(`[migration] Created user "${TARGET_USERNAME}" with role "${TARGET_ROLE}" (id=${userId}).`);
    }

    // 3. Assign all orphaned link_sets (owner_id IS NULL) to this user
    const updateResult = await client.query(
      'UPDATE link_sets SET owner_id = $1 WHERE owner_id IS NULL',
      [userId]
    );

    const affected = updateResult.rowCount;
    if (affected > 0) {
      console.log(`[migration] Assigned ${affected} orphaned link_set(s) to user "${TARGET_USERNAME}".`);
    } else {
      console.log(`[migration] No orphaned link_sets found; nothing to reassign.`);
    }

    console.log('[migration] Data migration complete.');
  } catch (err) {
    console.error('[migration] Data migration error:', err.message);
  } finally {
    client.release();
  }
}

module.exports = { runDataMigration };
