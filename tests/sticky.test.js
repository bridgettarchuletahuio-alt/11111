const test = require('node:test');
const assert = require('node:assert/strict');
const { STICKY_COOKIE_NAME, getStickyIdentity, buildStickyCookie } = require('../sticky');

function sha256Hex(value) {
  const crypto = require('crypto');
  return crypto.createHash('sha256').update(value).digest('hex');
}

test('uses cookie identity when present', () => {
  const result = getStickyIdentity({
    cookieHeader: `${STICKY_COOKIE_NAME}=customer-123`,
    ip: '203.0.113.9'
  });

  assert.equal(result.cookieValue, 'customer-123');
  assert.equal(result.identityHash, sha256Hex('customer-123'));
});

test('falls back to ip identity when no cookie exists', () => {
  const result = getStickyIdentity({
    cookieHeader: '',
    ip: '203.0.113.9'
  });

  assert.equal(result.cookieValue, null);
  assert.equal(result.identityHash, sha256Hex('203.0.113.9'));
});

test('builds a browser cookie for sticky redirects', () => {
  const cookie = buildStickyCookie('customer-123');
  assert.match(cookie, new RegExp(`^${STICKY_COOKIE_NAME}=customer-123`));
  assert.match(cookie, /Path=\/;/);
  assert.match(cookie, /SameSite=Lax/);
});
