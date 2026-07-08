const crypto = require('crypto');

const STICKY_COOKIE_NAME = 'link_dispatch_sticky';
const STICKY_COOKIE_TTL_DAYS = 30;

function sha256Hex(value) {
  return crypto.createHash('sha256').update(String(value || '')).digest('hex');
}

function getStickyIdentity(meta = {}) {
  const cookieHeader = String(meta.cookieHeader || '');
  const ip = String(meta.ip || '');
  const match = cookieHeader.match(new RegExp(`${STICKY_COOKIE_NAME}=([^;]+)`));
  const cookieValue = match ? decodeURIComponent(match[1].trim()) : null;
  const identity = cookieValue || ip || 'anonymous';
  return {
    cookieValue,
    identity,
    identityHash: sha256Hex(identity)
  };
}

function buildStickyCookie(value, options = {}) {
  const ttlDays = Number(options.ttlDays || STICKY_COOKIE_TTL_DAYS);
  const maxAge = ttlDays * 24 * 60 * 60;
  const safeValue = encodeURIComponent(String(value || ''));
  return `${STICKY_COOKIE_NAME}=${safeValue}; Path=/; Max-Age=${maxAge}; HttpOnly; SameSite=Lax`;
}

module.exports = {
  STICKY_COOKIE_NAME,
  STICKY_COOKIE_TTL_DAYS,
  getStickyIdentity,
  buildStickyCookie
};
