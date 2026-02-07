import { fetchWalletBalanceByDenom } from './chainClient.js';

const DEFAULT_TTL_MS = 60 * 1000;
const balanceCache = new Map(); // wallet -> { at, ok, amount, error }

export async function getWalletUlmnBalance(wallet, { ttlMs = DEFAULT_TTL_MS, timeoutMs } = {}) {
  const addr = String(wallet || '').trim();
  if (!addr) return { ok: false, error: 'wallet_required' };

  const ttl =
    typeof ttlMs === 'number' && Number.isFinite(ttlMs) && ttlMs > 0 ? ttlMs : DEFAULT_TTL_MS;
  const cached = balanceCache.get(addr);
  if (cached && Date.now() - Number(cached.at || 0) < ttl) {
    return cached.ok
      ? { ok: true, amount: cached.amount }
      : { ok: false, error: cached.error || 'unreachable' };
  }

  const res = await fetchWalletBalanceByDenom(addr, 'ulmn', { timeoutMs });
  if (!res || !res.ok) {
    const out = { ok: false, error: res?.error || 'unreachable', at: Date.now() };
    balanceCache.set(addr, out);
    return { ok: false, error: out.error };
  }

  const out = { ok: true, amount: res.amount || 0n, at: Date.now() };
  balanceCache.set(addr, out);
  return { ok: true, amount: out.amount };
}

export async function walletHasMinUlmn(wallet, minUlmn, { ttlMs, timeoutMs } = {}) {
  let min = 0n;
  try {
    min = typeof minUlmn === 'bigint' ? minUlmn : BigInt(minUlmn);
  } catch {
    min = 0n;
  }

  const res = await getWalletUlmnBalance(wallet, { ttlMs, timeoutMs });
  if (!res.ok) return { ok: false, error: res.error || 'unreachable' };
  const amount = res.amount || 0n;
  return { ok: true, amount, hasMin: amount >= min };
}

