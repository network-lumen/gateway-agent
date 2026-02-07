import { CONFIG } from '../config.js';

async function fetchWithTimeout(url, { method = 'GET', timeoutMs = 1500 } = {}) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { method, signal: controller.signal });
    clearTimeout(id);
    return resp;
  } catch (err) {
    clearTimeout(id);
    throw err;
  }
}

async function indexerRequestWithRetry(pathname, { timeoutMs } = {}) {
  const url = new URL(pathname, CONFIG.INDEXER_BASE).toString();
  const timeout = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 1500;
  const maxAttempts = 2;
  let lastErr;

  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      const resp = await fetchWithTimeout(url, { method: 'GET', timeoutMs: timeout });
      return resp;
    } catch (err) {
      lastErr = err;
      if (attempt + 1 >= maxAttempts) break;
      await new Promise((resolve) => setTimeout(resolve, 150));
    }
  }

  const error = lastErr && lastErr.name === 'AbortError' ? 'timeout' : 'unreachable';
  return { ok: false, error };
}

export async function fetchCidInfo(cid, { timeoutMs } = {}) {
  const resp = await indexerRequestWithRetry(`/cid/${encodeURIComponent(cid)}`, {
    timeoutMs
  });
  if (!resp || resp.ok === false) {
    return { ok: false, error: resp?.error || 'unreachable' };
  }
  if (!resp.ok) {
    return { ok: false, error: 'bad_status', status: resp.status };
  }
  try {
    const json = await resp.json();
    return { ok: true, cid: json };
  } catch {
    return { ok: false, error: 'bad_json' };
  }
}

export async function fetchChildren(cid, { timeoutMs } = {}) {
  const resp = await indexerRequestWithRetry(
    `/children/${encodeURIComponent(cid)}`,
    { timeoutMs }
  );

  if (!resp || resp.ok === false) {
    return { ok: false, error: resp?.error || 'unreachable' };
  }
  if (!resp.ok) {
    return { ok: false, error: 'bad_status', status: resp.status };
  }

  try {
    const json = await resp.json();
    if (!json || !Array.isArray(json.children)) {
      return { ok: false, error: 'bad_json' };
    }
    const children = json.children
      .map((c) => (typeof c.cid === 'string' ? c.cid : null))
      .filter(Boolean);
    return { ok: true, children };
  } catch {
    return { ok: false, error: 'bad_json' };
  }
}

export async function searchCidsSimple(
  { kind, tokens, present = 1, limit, offset } = {},
  { timeoutMs } = {}
) {
  const params = new URLSearchParams();
  if (kind) params.set('kind', String(kind));
  if (Array.isArray(tokens)) {
    for (const t of tokens) {
      const tok = String(t || '').trim().toLowerCase();
      if (!tok) continue;
      params.append('token', tok);
    }
  }
  if (present !== undefined && present !== null) {
    params.set('present', present ? '1' : '0');
  }
  if (Number.isFinite(limit) && limit > 0) {
    params.set('limit', String(limit));
  }
  if (Number.isFinite(offset) && offset >= 0) {
    params.set('offset', String(offset));
  }

  const pathname = `/search?${params.toString()}`;
  const resp = await indexerRequestWithRetry(pathname, { timeoutMs });

  if (!resp || resp.ok === false) {
    return { ok: false, error: resp?.error || 'unreachable', items: [], total: 0 };
  }
  if (!resp.ok) {
    return {
      ok: false,
      error: 'bad_status',
      status: resp.status,
      items: [],
      total: 0
    };
  }

  try {
    const json = await resp.json();
    if (!json || !Array.isArray(json.items)) {
      return { ok: false, error: 'bad_json', items: [], total: 0 };
    }
    const total =
      typeof json.total === 'number' && Number.isFinite(json.total)
        ? json.total
        : 0;
    return { ok: true, items: json.items, total };
  } catch {
    return { ok: false, error: 'bad_json', items: [], total: 0 };
  }
}

export async function fetchParents(cid, { timeoutMs } = {}) {
  const resp = await indexerRequestWithRetry(
    `/parents/${encodeURIComponent(cid)}`,
    { timeoutMs }
  );

  if (!resp || resp.ok === false) {
    return { ok: false, error: resp?.error || 'unreachable', parents: [] };
  }
  if (!resp.ok) {
    return { ok: false, error: 'bad_status', status: resp.status, parents: [] };
  }

  try {
    const json = await resp.json();
    if (!json || !Array.isArray(json.parents)) {
      return { ok: false, error: 'bad_json', parents: [] };
    }
    const parents = json.parents
      .map((p) => (typeof p.cid === 'string' ? p.cid : p.parent_cid || null))
      .filter(Boolean);
    return { ok: true, parents };
  } catch {
    return { ok: false, error: 'bad_json', parents: [] };
  }
}
