const fs = require('fs');
const path = require('path');

const ROOT = process.cwd();
const IS_CI = String(process.env.CI || '').toLowerCase() === 'true';

const SKIP_DIRS = new Set([
  '.git',
  'node_modules',
  'dist',
  'build',
  '.cache'
]);

const TEXT_EXT_ALLOW = new Set([
  '.js', '.cjs', '.mjs', '.ts', '.json', '.md', '.yml', '.yaml',
  '.env', '.txt', '.html', '.css', '.sql', '.sh', '.ps1'
]);

const ALLOWLIST_FILES = new Set([
  '.env.example',
  'backend/.env.example'
]);

const SECRET_PATTERNS = [
  { name: 'Anthropic key', re: /sk-ant-[A-Za-z0-9_-]{20,}/g },
  { name: 'OpenAI key', re: /sk-[A-Za-z0-9]{20,}/g },
  { name: 'Stripe live key', re: /sk_live_[A-Za-z0-9]{16,}/g },
  { name: 'AWS access key', re: /AKIA[0-9A-Z]{16}/g }
];

function shouldSkipPath(fullPath) {
  const rel = path.relative(ROOT, fullPath).replace(/\\/g, '/');
  const parts = rel.split('/');
  return parts.some((p) => SKIP_DIRS.has(p));
}

function isProbablyTextFile(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (TEXT_EXT_ALLOW.has(ext)) return true;
  if (path.basename(filePath).startsWith('.env')) return true;
  return false;
}

function walk(dir, out = []) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (shouldSkipPath(full)) continue;
    if (entry.isDirectory()) {
      walk(full, out);
      continue;
    }
    out.push(full);
  }
  return out;
}

function isForbiddenEnvFile(relPath) {
  const base = path.basename(relPath);
  if (!base.startsWith('.env')) return false;
  if (ALLOWLIST_FILES.has(relPath)) return false;
  return true;
}

function scanFile(filePath) {
  const rel = path.relative(ROOT, filePath).replace(/\\/g, '/');
  const findings = [];
  const base = path.basename(rel);

  if (isForbiddenEnvFile(rel) && IS_CI) {
    findings.push({ type: 'forbidden_env_file', rel, detail: 'Do not commit .env files' });
  }

  if (base.startsWith('.env') && !IS_CI) {
    return findings;
  }

  if (!isProbablyTextFile(filePath)) return findings;

  let content = '';
  try {
    content = fs.readFileSync(filePath, 'utf8');
  } catch {
    return findings;
  }

  for (const p of SECRET_PATTERNS) {
    const match = content.match(p.re);
    if (match && match.length) {
      findings.push({
        type: 'secret_pattern',
        rel,
        detail: `${p.name} pattern detected`
      });
    }
  }

  return findings;
}

function main() {
  const files = walk(ROOT);
  const findings = [];
  for (const file of files) {
    findings.push(...scanFile(file));
  }

  if (!findings.length) {
    console.log('[secret-scan] OK');
    process.exit(0);
  }

  console.error('[secret-scan] FAILED');
  for (const f of findings) {
    console.error(`- ${f.rel}: ${f.detail}`);
  }
  process.exit(1);
}

main();
