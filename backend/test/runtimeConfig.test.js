const assert = require('assert');

const runtimePath = require.resolve('../src/config/runtime');

function withEnv(patch, fn) {
  const original = { ...process.env };
  try {
    process.env = { ...original, ...patch };
    delete require.cache[runtimePath];
    return fn(require('../src/config/runtime'));
  } finally {
    process.env = original;
    delete require.cache[runtimePath];
  }
}

function run() {
  withEnv({
    NODE_ENV: 'production',
    DEV_MODE: 'false',
    DATABASE_URL: 'postgres://user:pass@example.com:5432/relay',
    AUTH_SECRET: 'x'.repeat(48),
    COOKIE_SECURE: 'true',
    APP_PUBLIC_BASE_URL: 'https://relay.example.com',
    CAL_OAUTH_REDIRECT_BASE: 'https://relay.example.com',
    WEBHOOK_PUBLIC_BASE: 'https://relay.example.com',
    CORS_ORIGINS: 'https://relay.example.com'
  }, (runtime) => {
    assert.doesNotThrow(() => runtime.validateRuntimeConfig());
  });

  withEnv({
    NODE_ENV: 'production',
    DEV_MODE: 'false',
    DATABASE_URL: 'postgres://user:pass@example.com:5432/relay',
    AUTH_SECRET: 'x'.repeat(48),
    COOKIE_SECURE: 'true',
    APP_PUBLIC_BASE_URL: 'http://127.0.0.1:3001',
    CORS_ORIGINS: 'http://localhost:3001'
  }, (runtime) => {
    assert.throws(
      () => runtime.validateRuntimeConfig(),
      /APP_PUBLIC_BASE_URL.*https|localhost|CORS_ORIGINS/
    );
  });

  console.log('[runtimeConfig.test] ok');
}

run();
