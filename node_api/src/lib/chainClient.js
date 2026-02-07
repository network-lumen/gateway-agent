import { CONFIG } from '../config.js';

const DEFAULT_CHAIN_REST_BASE = 'http://host.docker.internal:1317';

const CHAIN_REST_BASE =
  typeof CONFIG.CHAIN_REST_BASE_URL === 'string' &&
  CONFIG.CHAIN_REST_BASE_URL.trim()
    ? CONFIG.CHAIN_REST_BASE_URL.replace(/\/+$/, '')
    : DEFAULT_CHAIN_REST_BASE;

const FETCH_TIMEOUT_MS = 4000;
const MAX_ATTEMPTS = 2;
const RETRY_DELAY_MS = 150;

const errorLogTimestamps = {
  unreachable: 0,
  bad_status: 0,
  bad_json: 0
};

function logChainError(kind, details) {
  const now = Date.now();
  const last = errorLogTimestamps[kind] || 0;
  // Simple rate-limit to avoid log spam: once per kind per 60s.
  if (now - last < 60_000) return;
  errorLogTimestamps[kind] = now;
  try {
    // eslint-disable-next-line no-console
    console.error('[walletPlan] chain fetch failed', {
      error: kind,
      ...details
    });
  } catch {
    // ignore logging failures
  }
}

async function fetchWithTimeout(url, { method = 'GET', timeoutMs = FETCH_TIMEOUT_MS } = {}) {
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

async function chainRequestWithRetry(pathname, { timeoutMs } = {}) {
  if (!CHAIN_REST_BASE) {
    return { ok: false, error: 'unreachable' };
  }

  const url = new URL(pathname, CHAIN_REST_BASE).toString();
  const timeout =
    typeof timeoutMs === 'number' && Number.isFinite(timeoutMs) && timeoutMs > 0
      ? timeoutMs
      : FETCH_TIMEOUT_MS;

  let lastErr;

  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
    try {
      const resp = await fetchWithTimeout(url, { method: 'GET', timeoutMs: timeout });
      return resp;
    } catch (err) {
      lastErr = err;
      if (attempt + 1 >= MAX_ATTEMPTS) break;
      // Lightweight backoff
      // eslint-disable-next-line no-await-in-loop
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
    }
  }

  const error = lastErr && lastErr.name === 'AbortError' ? 'unreachable' : 'unreachable';
  return { ok: false, error };
}

function extractList(obj) {
  if (!obj || !Array.isArray(obj.contracts)) return [];
  return obj.contracts;
}

function parseMeta(meta) {
  if (!meta) return {};
  if (typeof meta === 'object') return meta;
  if (typeof meta === 'string') {
    try {
      const j = JSON.parse(meta);
      return j && typeof j === 'object' ? j : {};
    } catch {
      return {};
    }
  }
  return {};
}

function toBytesFromGb(gb) {
  const n = Number(gb);
  return Number.isFinite(n) ? Math.max(0, n) * 1024 ** 3 : 0;
}

async function fetchJson(pathname, { timeoutMs } = {}) {
  const resp = await chainRequestWithRetry(pathname, { timeoutMs });

  if (!resp || resp.ok === false) {
    return { ok: false, error: 'unreachable' };
  }

  if (!resp.ok) {
    let snippet = '';
    try {
      const text = await resp.text();
      snippet = typeof text === 'string' ? text.slice(0, 240) : '';
    } catch {
      // ignore
    }
    return {
      ok: false,
      error: 'bad_status',
      status: resp.status,
      body: snippet
    };
  }

  try {
    const json = await resp.json();
    return { ok: true, json };
  } catch {
    return { ok: false, error: 'bad_json' };
  }
}

function pickBestContract(contracts) {
  if (!Array.isArray(contracts) || contracts.length === 0) return null;

  const active = contracts.filter((c) => {
    const status = String(c?.status ?? '').toUpperCase();
    return status.includes('ACTIVE');
  });

  const candidates = active.length > 0 ? active : contracts;

  let best = null;
  for (const c of candidates) {
    const idNum = Number(c?.id);
    if (!best) {
      best = c;
      // eslint-disable-next-line no-continue
      continue;
    }
    const bestId = Number(best.id);
    if (Number.isFinite(idNum) && (!Number.isFinite(bestId) || idNum > bestId)) {
      best = c;
    }
  }
  return best || null;
}

function planFromContract(contract) {
  if (!contract) return null;

  const meta = parseMeta(contract.metadata || contract.info || contract.extras);

  const pidRaw =
    contract.plan_id ??
    contract.planId ??
    meta.planId ??
    meta.planName ??
    (contract.id != null ? String(contract.id) : '');

  const planId = typeof pidRaw === 'string' ? pidRaw.trim() : String(pidRaw || '').trim();

  const storageGbRaw =
    contract.storage_gb_per_month ??
    contract.storageGbPerMonth ??
    meta.storageGbPerMonth ??
    0;

  const storageGb = Number(storageGbRaw);
  const quotaBytesTotal = Number.isFinite(storageGb) ? toBytesFromGb(storageGb) || null : null;

  const startSeconds = Number(
    contract.start_time != null ? contract.start_time : contract.startTime || 0
  );
  const monthsTotal = Number(
    contract.months_total != null ? contract.months_total : contract.monthsTotal || 0
  );

  return {
    planId: planId || null,
    startSeconds: Number.isFinite(startSeconds) && startSeconds > 0 ? startSeconds : null,
    monthsTotal: Number.isFinite(monthsTotal) && monthsTotal > 0 ? monthsTotal : null,
    quotaBytesTotal: quotaBytesTotal != null && quotaBytesTotal >= 0 ? quotaBytesTotal : null
  };
}

export async function fetchWalletPlanFromChain(wallet) {
  const addr = String(wallet || '').trim();
  if (!addr) {
    return { ok: false, error: 'unreachable' };
  }

  const contractsRes = await fetchJson(
    `/lumen/gateway/v1/contracts?client=${encodeURIComponent(addr)}&limit=200`,
    { timeoutMs: FETCH_TIMEOUT_MS }
  );

  if (!contractsRes.ok) {
    logChainError(contractsRes.error || 'unreachable', {
      wallet: addr,
      status: contractsRes.status,
      body: contractsRes.body
    });
    return { ok: false, error: contractsRes.error || 'unreachable' };
  }

  const list = extractList(contractsRes.json);
  if (!Array.isArray(list) || list.length === 0) {
    return {
      ok: true,
      plan_id: null,
      expires_at: null,
      quota_bytes_total: null
    };
  }

  const best = pickBestContract(list);
  if (!best) {
    return {
      ok: true,
      plan_id: null,
      expires_at: null,
      quota_bytes_total: null
    };
  }

  const paramsRes = await fetchJson('/lumen/gateway/v1/params', {
    timeoutMs: FETCH_TIMEOUT_MS
  });

  if (!paramsRes.ok) {
    logChainError(paramsRes.error || 'unreachable', {
      wallet: addr,
      status: paramsRes.status,
      body: paramsRes.body
    });
    return { ok: false, error: paramsRes.error || 'unreachable' };
  }

  const info = planFromContract(best);
  const monthSecondsRaw = paramsRes.json?.params?.month_seconds;
  const monthSeconds = Number(monthSecondsRaw);

  let expiresAt = null;
  if (
    info.startSeconds != null &&
    info.monthsTotal != null &&
    Number.isFinite(monthSeconds) &&
    monthSeconds > 0
  ) {
    const endSeconds = info.startSeconds + info.monthsTotal * monthSeconds;
    if (Number.isFinite(endSeconds) && endSeconds > 0) {
      expiresAt = endSeconds * 1000;
    }
  }

  return {
    ok: true,
    plan_id: info.planId,
    expires_at: expiresAt,
    quota_bytes_total: info.quotaBytesTotal
  };
}

function parseCoinAmountBigInt(amountRaw) {
  const s = String(amountRaw ?? '').trim();
  if (!s) return 0n;
  try {
    return BigInt(s);
  } catch {
    return 0n;
  }
}

export async function fetchWalletBalanceByDenom(wallet, denom = 'ulmn', { timeoutMs } = {}) {
  const addr = String(wallet || '').trim();
  const d = String(denom || '').trim() || 'ulmn';
  if (!addr) return { ok: false, error: 'unreachable' };

  const res = await fetchJson(
    `/cosmos/bank/v1beta1/balances/${encodeURIComponent(addr)}/by_denom?denom=${encodeURIComponent(d)}`,
    { timeoutMs }
  );

  if (!res.ok) {
    logChainError(res.error || 'unreachable', {
      wallet: addr,
      denom: d,
      status: res.status,
      body: res.body
    });
    return { ok: false, error: res.error || 'unreachable' };
  }

  const amount = parseCoinAmountBigInt(res.json?.balance?.amount ?? '0');
  return { ok: true, denom: d, amount };
}
