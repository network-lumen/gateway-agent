import { CONFIG } from '../config.js';

function withTimeout(timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  return {
    signal: controller.signal,
    clear: () => {
      try {
        clearTimeout(t);
      } catch {
        // ignore
      }
    }
  };
}

async function readJsonBestEffort(resp) {
  const text = await resp.text().catch(() => '');
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function stripPeerIdSuffix(addr) {
  const s = String(addr || '').trim();
  if (!s) return '';
  return s.replace(/\/p2p\/[^/]+$/i, '');
}

function parseFirstAddrComponent(ma) {
  const parts = String(ma || '').split('/').filter(Boolean);
  const proto = parts[0] || '';
  const value = parts[1] || '';
  return { proto, value };
}

function isIpv4UnicastPublic(ip) {
  const s = String(ip || '').trim();
  const parts = s.split('.');
  if (parts.length !== 4) return false;
  const octets = parts.map((p) => Number.parseInt(p, 10));
  if (octets.some((n) => !Number.isFinite(n) || n < 0 || n > 255)) return false;

  const [a, b] = octets;
  if (a === 0) return false;
  if (a === 127) return false;
  if (a === 10) return false;
  if (a === 172 && b >= 16 && b <= 31) return false;
  if (a === 192 && b === 168) return false;
  if (a === 169 && b === 254) return false;
  if (a === 100 && b >= 64 && b <= 127) return false;
  if (a >= 224) return false;
  if (a === 255) return false;
  return true;
}

function isIpv6Global(addr) {
  const s = String(addr || '').trim().toLowerCase();
  if (!s) return false;
  if (s === '::' || s === '::1') return false;
  if (s.startsWith('fe80:')) return false;
  if (s.startsWith('fc') || s.startsWith('fd')) return false;
  if (s.startsWith('ff')) return false;
  if (s.includes('%')) return false;
  return true;
}

function isUsableMultiaddr(ma) {
  const s = String(ma || '').trim();
  if (!s.startsWith('/')) return false;
  if (s.toLowerCase().includes('/p2p-circuit')) return false;

  const { proto, value } = parseFirstAddrComponent(s);
  if (proto === 'ip4') return isIpv4UnicastPublic(value);
  if (proto === 'ip6') return isIpv6Global(value);
  if (proto === 'dns' || proto === 'dns4' || proto === 'dns6') {
    const host = String(value || '').trim().toLowerCase();
    if (!host) return false;
    if (host === 'localhost') return false;
    return true;
  }
  return false;
}

async function kuboPostJson(pathname, { timeoutMs = 1200, searchParams = {} } = {}) {
  const url = new URL(pathname, CONFIG.KUBO_API_BASE);
  for (const [k, v] of Object.entries(searchParams || {})) {
    if (v === undefined || v === null) continue;
    url.searchParams.set(k, String(v));
  }

  const { signal, clear } = withTimeout(timeoutMs);
  try {
    const resp = await fetch(url.toString(), { method: 'POST', signal });
    const json = await readJsonBestEffort(resp);
    return { ok: resp.ok, status: resp.status, json };
  } catch (err) {
    return { ok: false, status: 0, json: null, error: String(err?.message || err) };
  } finally {
    clear();
  }
}

export async function getIpfsSeed(_req, res) {
  const timeoutMs = 1200;

  const idRes = await kuboPostJson('/api/v0/id', { timeoutMs, searchParams: { enc: 'json' } });
  if (!idRes.ok || !idRes.json) {
    return res.status(503).json({ error: 'ipfs_unavailable' });
  }

  const peerId = String(idRes.json.ID || idRes.json.id || '').trim();
  if (!peerId) {
    return res.status(503).json({ error: 'ipfs_unavailable' });
  }

  const listenRes = await kuboPostJson('/api/v0/swarm/addrs/listen', {
    timeoutMs,
    searchParams: { enc: 'json' }
  });

  const fromId = Array.isArray(idRes.json.Addresses) ? idRes.json.Addresses : [];
  const fromListen =
    listenRes.ok && listenRes.json && Array.isArray(listenRes.json.Strings)
      ? listenRes.json.Strings
      : [];

  const candidates = [...fromId.map(stripPeerIdSuffix), ...fromListen]
    .map((x) => String(x || '').trim())
    .filter(Boolean);

  const uniq = Array.from(new Set(candidates));
  const multiaddrs = uniq.filter(isUsableMultiaddr);

  if (!multiaddrs.length) {
    return res.status(503).json({ error: 'no_usable_multiaddrs' });
  }

  return res.status(200).json({ peerId, multiaddrs });
}

