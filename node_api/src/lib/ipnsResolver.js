import { CONFIG } from '../config.js';

const ipnsCache = new Map();

function nowMs() {
  return Date.now();
}

export async function resolveIpnsToRootCid(ipnsKey, { ttlMs = 15 * 60 * 1000 } = {}) {
  const raw = String(ipnsKey || '').trim();
  if (!raw) return null;

  const key = raw.toLowerCase();
  const cached = ipnsCache.get(key);
  if (cached && nowMs() - cached.fetchedAt < ttlMs) {
    return cached.cid;
  }

  const arg = raw.startsWith('/ipns/') ? raw : `/ipns/${raw}`;

  try {
    const url = new URL('/api/v0/name/resolve', CONFIG.KUBO_API_BASE);
    url.searchParams.set('arg', arg);
    url.searchParams.set('recursive', 'false');
    url.searchParams.set('dht-record-count', '1');
    url.searchParams.set('dht-timeout', '5s');

    const resp = await fetch(url.toString(), { method: 'POST' });
    if (!resp.ok) {
      ipnsCache.set(key, { fetchedAt: nowMs(), cid: null });
      return null;
    }

    const text = await resp.text();
    let pathStr = null;
    try {
      const obj = JSON.parse(text);
      pathStr = obj.Path || obj.path || null;
    } catch {
      pathStr = text.trim();
    }

    let cid = null;
    if (pathStr && typeof pathStr === 'string') {
      const match = pathStr.match(/\/ipfs\/([^/?\s]+)/);
      if (match && match[1]) {
        cid = match[1];
      } else if (/^[A-Za-z0-9]+$/.test(pathStr)) {
        cid = pathStr;
      }
    }

    ipnsCache.set(key, { fetchedAt: nowMs(), cid: cid || null });
    return cid || null;
  } catch {
    ipnsCache.set(key, { fetchedAt: nowMs(), cid: null });
    return null;
  }
}

