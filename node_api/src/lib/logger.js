/* eslint-disable no-console */

const TRUE_VALUES = new Set(['1', 'true', 'yes', 'y', 'on', 'debug', 'all', '*']);
const FALSE_VALUES = new Set(['0', 'false', 'no', 'n', 'off', '']);

function parseDebugSpec(rawValue) {
  const raw = String(rawValue || '').trim();
  const lower = raw.toLowerCase();

  if (TRUE_VALUES.has(lower)) {
    return { enabled: true, all: true, categories: new Set() };
  }
  if (FALSE_VALUES.has(lower)) {
    return { enabled: false, all: false, categories: new Set() };
  }

  const tokens = lower
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean);

  if (!tokens.length) return { enabled: false, all: false, categories: new Set() };

  if (tokens.includes('*') || tokens.includes('all')) {
    return { enabled: true, all: true, categories: new Set() };
  }

  return { enabled: true, all: false, categories: new Set(tokens) };
}

const SPEC = parseDebugSpec(process.env.NODE_API_DEBUG);

export function isDebugEnabled(category) {
  if (!SPEC.enabled) return false;
  if (SPEC.all) return true;
  const key = String(category || '').trim().toLowerCase();
  if (!key) return false;
  return SPEC.categories.has(key);
}

export function debugLog(category, message, data) {
  if (!isDebugEnabled(category)) return;
  const label = String(category || 'debug').trim() || 'debug';
  const msg = String(message || '').trim();
  const prefix = `[debug:${label}]`;
  try {
    if (data === undefined) console.log(prefix, msg);
    else if (msg) console.log(prefix, msg, data);
    else console.log(prefix, data);
  } catch {
    // ignore logging failures
  }
}

export function formatError(err) {
  if (!err) return { message: 'unknown_error' };
  if (typeof err === 'string') return { message: err };

  const out = {
    name: err?.name ? String(err.name) : null,
    code: err?.code ? String(err.code) : null,
    message: err?.message ? String(err.message) : String(err)
  };

  if (isDebugEnabled('stack') && err?.stack) {
    out.stack = String(err.stack);
  }

  return out;
}

export function getDebugSpec() {
  return {
    enabled: SPEC.enabled,
    all: SPEC.all,
    categories: Array.from(SPEC.categories.values()).sort()
  };
}
