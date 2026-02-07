import { getWalletRootCids, getWalletRootsSummary } from '../lib/walletDb.js';
import { fetchChildren } from '../lib/indexerClient.js';
import { extractWallet } from '../lib/auth.js';
import { ensureWalletPlanOk } from '../lib/walletPlan.js';
import { sendPqJson } from '../lib/pqResponse.js';

const MAX_NODES = 10_000;
const CONCURRENCY = 6;
const TIME_BUDGET_MS = 2500;

async function computeCidCountForRootCids(rootCids) {
  const visited = new Set();
  const queue = [];

  const list = Array.isArray(rootCids) ? rootCids : [];
  for (const cid of list) {
    const clean = String(cid || '').trim();
    if (!clean) continue;
    queue.push(clean);
  }

  if (queue.length === 0) {
    return {
      totalCids: 0,
      truncated: false,
      truncatedReason: null,
      indexerError: null,
      durationMs: 0
    };
  }

  const start = Date.now();
  let truncatedReason = null;
  let indexerError = null;
  let stop = false;
  let queueIndex = 0;

  async function worker() {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      if (stop) return;
      if (visited.size >= MAX_NODES) {
        truncatedReason = truncatedReason || 'max_nodes';
        return;
      }
      if (Date.now() - start >= TIME_BUDGET_MS) {
        truncatedReason = truncatedReason || 'time_budget';
        return;
      }

      const idx = queueIndex;
      if (idx >= queue.length) return;
      queueIndex += 1;

      const cid = queue[idx];
      if (!cid) continue;

      if (visited.has(cid)) continue;
      visited.add(cid);

      if (visited.size >= MAX_NODES) {
        truncatedReason = truncatedReason || 'max_nodes';
        return;
      }

      const res = await fetchChildren(cid, { timeoutMs: 1500 });
      if (!res.ok) {
        if (!indexerError) {
          indexerError = res.error || 'unreachable';
        }
        if (res.error === 'timeout' || res.error === 'unreachable') {
          stop = true;
          return;
        }
        // For bad_status/bad_json, skip this branch but continue other nodes.
        // eslint-disable-next-line no-continue
        continue;
      }

      for (const child of res.children) {
        if (!visited.has(child)) {
          queue.push(child);
        }
      }
    }
  }

  const workers = [];
  const workerCount = Math.min(CONCURRENCY, queue.length);
  for (let i = 0; i < workerCount; i += 1) {
    workers.push(worker());
  }
  await Promise.all(workers);

  const durationMs = Date.now() - start;

  return {
    totalCids: visited.size,
    truncated: truncatedReason != null,
    truncatedReason,
    indexerError,
    durationMs
  };
}

export async function getWalletUsage(req, res) {
  try {
    const walletFromParam = String(req.params.wallet || '').trim();
    const walletFromReq = String(req.wallet || '').trim();
    const wallet = walletFromReq || walletFromParam;

    if (!wallet) {
      return res.status(400).json({ error: 'wallet_required' });
    }

    // Optional safety: ensure caller wallet matches path wallet when present
    if (walletFromParam) {
      const extracted = extractWallet(req);
      if (extracted.ok && extracted.wallet && extracted.wallet !== walletFromParam) {
        return res
          .status(400)
          .json({ error: 'wallet_mismatch', message: 'wallet_mismatch' });
      }
    }

    const rootCids = await getWalletRootCids(wallet);
    const summary = await getWalletRootsSummary(wallet);

    let planState;
    try {
      planState = await ensureWalletPlanOk(wallet, null);
    } catch (planErr) {
      if (planErr && planErr.code === 'CHAIN_UNREACHABLE') {
        planState = {
          ok: true,
          wallet,
          plan: {
            id: null,
            expires_at: null,
            quota_bytes_total: null,
            quota_bytes_used: null,
            quota_bytes_remaining: null
          },
          chain: { ok: false, error: 'unreachable' }
        };
      } else {
        throw planErr;
      }
    }

    let cidUsage = {
      total_cids: null,
      truncated: false,
      error: null,
      walk_ms: null,
      truncated_reason: null
    };

    try {
      if (rootCids.length > 0) {
        const result = await computeCidCountForRootCids(rootCids);

        const indexerError = result.indexerError || null;
        const totalCids =
          indexerError === 'timeout' || indexerError === 'unreachable'
            ? null
            : result.totalCids;

        cidUsage = {
          total_cids: totalCids,
          truncated: result.truncated,
          error: indexerError,
          walk_ms: result.durationMs,
          truncated_reason: result.truncatedReason
        };
      }
    } catch (err) {
      cidUsage = {
        total_cids: null,
        truncated: false,
        error: 'indexer_unreachable',
        walk_ms: null,
        truncated_reason: null
      };
      console.error('[walletUsage] indexer error', err);
    }

    const quotaBytesUsed = summary.bytesEstimated;

    const quotaBytesTotal =
      planState && planState.plan && typeof planState.plan.quota_bytes_total === 'number'
        ? planState.plan.quota_bytes_total
        : null;

    let quotaBytesRemaining = null;
    if (quotaBytesTotal != null && Number.isFinite(quotaBytesTotal)) {
      const usedForRemaining =
        typeof quotaBytesUsed === 'number' && Number.isFinite(quotaBytesUsed)
          ? quotaBytesUsed
          : 0;
      const diff = quotaBytesTotal - usedForRemaining;
      quotaBytesRemaining = diff > 0 ? diff : 0;
    }

    const plan = {
      id: planState?.plan?.id || null,
      expires_at: planState?.plan?.expires_at || null,
      quota_bytes_total: quotaBytesTotal,
      quota_bytes_used: quotaBytesUsed,
      quota_bytes_remaining: quotaBytesRemaining
    };

    let planSource = 'stub';
    if (planState?.chain?.ok) {
      planSource = 'chain';
    } else if (plan.id != null || plan.expires_at != null) {
      planSource = 'cache';
    }

    const responseBody = {
      wallet,
      plan,
      plan_source: planSource,
      usage: {
        roots_total: summary.totalRoots,
        roots_active: summary.activeRoots,
        cids_total: cidUsage.total_cids,
        cids_truncated: cidUsage.truncated,
        cids_walk_ms: cidUsage.walk_ms,
        cids_walk_truncated_reason: cidUsage.truncated_reason,
        bytes_estimated_total: summary.bytesEstimated,
        indexer_error: cidUsage.error
      },
    };

    return sendPqJson(req, res, 200, responseBody, 'api:/wallet/:wallet/usage');
  } catch (err) {
    console.error('[api:/wallet/:wallet/usage] error', err);
    return sendPqJson(req, res, 500, { error: 'internal_error' }, 'api:/wallet/:wallet/usage');
  }
}
