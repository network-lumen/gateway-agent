import { CONFIG } from '../config.js';

function fetchWithTimeout(url, { timeoutMs = 3000 } = {}) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { signal: controller.signal })
    .then((resp) => {
      clearTimeout(id);
      return resp;
    })
    .catch((err) => {
      clearTimeout(id);
      throw err;
    });
}

const domainsByOwnerCache = new Map();
const domainDetailsCache = new Map();

function nowMs() {
  return Date.now();
}

export async function fetchDomainsByOwner(owner, { ttlMs = 30_000 } = {}) {
  const key = String(owner || '').trim();
  if (!key) return [];

  const cached = domainsByOwnerCache.get(key);
  if (cached && nowMs() - cached.fetchedAt < ttlMs) {
    return cached.domains;
  }

  try {
    const url = new URL(
      `/lumen/dns/v1/domains_by_owner/${encodeURIComponent(key)}`,
      CONFIG.CHAIN_REST_BASE_URL
    ).toString();
    const resp = await fetchWithTimeout(url, { timeoutMs: 3000 });
    if (!resp.ok) {
      domainsByOwnerCache.set(key, { fetchedAt: nowMs(), domains: [] });
      return [];
    }
    const json = await resp.json().catch(() => null);
    const list = Array.isArray(json?.domains) ? json.domains : [];
    const domains = list
      .map((d) => String(d || '').trim().toLowerCase())
      .filter(Boolean);
    domainsByOwnerCache.set(key, { fetchedAt: nowMs(), domains });
    return domains;
  } catch {
    domainsByOwnerCache.set(key, { fetchedAt: nowMs(), domains: [] });
    return [];
  }
}

export async function fetchDomainDetails(name, { ttlMs = 30_000 } = {}) {
  const key = String(name || '').trim().toLowerCase();
  if (!key) return null;

  const cached = domainDetailsCache.get(key);
  if (cached && nowMs() - cached.fetchedAt < ttlMs) {
    return cached.domain;
  }

  try {
    const url = new URL(
      `/lumen/dns/v1/domain/${encodeURIComponent(key)}`,
      CONFIG.CHAIN_REST_BASE_URL
    ).toString();
    const resp = await fetchWithTimeout(url, { timeoutMs: 3000 });
    if (!resp.ok) {
      domainDetailsCache.set(key, { fetchedAt: nowMs(), domain: null });
      return null;
    }
    const json = await resp.json().catch(() => null);
    domainDetailsCache.set(key, { fetchedAt: nowMs(), domain: json });
    return json;
  } catch {
    domainDetailsCache.set(key, { fetchedAt: nowMs(), domain: null });
    return null;
  }
}

export function domainHasCid(domainObj, cid) {
  if (!domainObj || !cid) return false;
  const target = String(cid || '').trim();
  if (!target) return false;

  const records = Array.isArray(domainObj.records) ? domainObj.records : [];
  for (const rec of records) {
    const val = String(rec?.value || '').trim();
    if (!val) continue;
    if (val === target) return true;
  }
  return false;
}

