import { upsertWalletRecord, getWalletRow, updateWalletPlanFromChain } from './walletDb.js';
import { fetchWalletPlanFromChain } from './chainClient.js';

export async function ensureWalletPlanOk(wallet, _planId) {
  const normalizedWallet = String(wallet || '').trim();

  let chainResult = null;
  try {
    chainResult = await fetchWalletPlanFromChain(normalizedWallet);
  } catch (err) {
    try {
      // eslint-disable-next-line no-console
      console.error('[walletPlan] chain fetch threw', {
        wallet: normalizedWallet,
        error: String(err?.message || err)
      });
    } catch {
      // ignore logging failures
    }
    chainResult = { ok: false, error: 'unreachable' };
  }

  if (!chainResult || !chainResult.ok) {
    const err = new Error('chain_unreachable');
    // eslint-disable-next-line no-param-reassign
    err.code = 'CHAIN_UNREACHABLE';
    throw err;
  }

  let finalPlanId = chainResult.plan_id || null;
  let expiresAt = null;
  let quotaBytesTotal = null;
  const now = Date.now();

  if (
    typeof chainResult.expires_at === 'number' &&
    Number.isFinite(chainResult.expires_at)
  ) {
    expiresAt = chainResult.expires_at;
  }
  if (
    typeof chainResult.quota_bytes_total === 'number' &&
    Number.isFinite(chainResult.quota_bytes_total) &&
    chainResult.quota_bytes_total >= 0
  ) {
    quotaBytesTotal = chainResult.quota_bytes_total;
  }

  // Ensure the wallet row exists and store the authoritative plan id,
  // coming from chain (client-provided planId is ignored for persistence).
  await upsertWalletRecord({ wallet: normalizedWallet, planId: finalPlanId });

  try {
    await updateWalletPlanFromChain({
      wallet: normalizedWallet,
      planId: finalPlanId,
      planExpiresAt: expiresAt,
      chainCheckAt: now
    });
  } catch (err) {
    try {
      // eslint-disable-next-line no-console
      console.error('[walletPlan] failed to persist chain info', {
        wallet: normalizedWallet,
        error: String(err?.message || err)
      });
    } catch {
      // ignore logging failures
    }
  }

  const row = await getWalletRow(normalizedWallet);

  return {
    ok: true,
    wallet: row?.wallet || normalizedWallet,
    plan: {
      id: row?.plan_id || finalPlanId || null,
      expires_at: row?.plan_expires_at || expiresAt || null,
      quota_bytes_total: quotaBytesTotal,
      quota_bytes_used: null,
      quota_bytes_remaining: null
    },
    chain: { ok: true }
  };
}
