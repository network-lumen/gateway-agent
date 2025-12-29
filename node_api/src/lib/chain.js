import { CONFIG } from '../config.js';

let lastChainCheckAt = 0;
let lastChainUp = false;

function parseSeeds() {
  const raw = Array.isArray(CONFIG.CHAIN_SEEDS) ? CONFIG.CHAIN_SEEDS : [];
  const out = [];
  for (const entry of raw) {
    if (!entry) continue;
    if (typeof entry === 'string') {
      out.push({ rpc: entry });
    } else if (typeof entry === 'object') {
      const rpc = typeof entry.rpc === 'string' ? entry.rpc : '';
      const rest = typeof entry.rest === 'string' ? entry.rest : '';
      const grpcWeb = typeof entry.grpcWeb === 'string' ? entry.grpcWeb : '';
      if (rpc || rest || grpcWeb) out.push({ rpc, rest, grpcWeb });
    }
  }
  return out;
}

async function checkChainOnce() {
  const seeds = parseSeeds();
  if (!seeds.length) return false;

  for (const seed of seeds) {
    const base = seed.rpc || seed.rest || seed.grpcWeb;
    if (!base) continue;
    try {
      const url = new URL('/health', base).toString();
      const resp = await fetch(url);
      if (resp.ok) return true;
    } catch {
      // ignore and try next seed
    }
  }
  return false;
}

export async function ensureChainOnline(opts = {}) {
  const ttlMs = typeof opts.ttlMs === 'number' && opts.ttlMs > 0 ? opts.ttlMs : 60_000;
  const now = Date.now();

  if (lastChainCheckAt && now - lastChainCheckAt < ttlMs) {
    if (!lastChainUp) {
      const err = new Error('chain_unreachable');
      err.code = 'CHAIN_UNREACHABLE';
      throw err;
    }
    return true;
  }

  const ok = await checkChainOnce();
  lastChainCheckAt = now;
  lastChainUp = ok;

  if (!ok) {
    const err = new Error('chain_unreachable');
    err.code = 'CHAIN_UNREACHABLE';
    throw err;
  }
  return true;
}

export function getChainStatus() {
  return {
    online: !!lastChainUp,
    lastCheckAt: lastChainCheckAt || null
  };
}
