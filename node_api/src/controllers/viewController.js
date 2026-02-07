import { decryptPqRequest } from '../middleware/pqMiddleware.js';
import { recordCidAccess } from '../lib/usageDb.js';
import { walletHasMinUlmn } from '../lib/walletBalance.js';

const MIN_VIEW_BALANCE_ULMN = (() => {
  const raw = String(process.env.VIEW_MIN_BALANCE_ULMN || '').trim();
  if (!raw) return 100n; // 0.0001 LMN assuming 1 LMN = 1e6 ulmn
  try {
    const v = BigInt(raw);
    return v >= 0n ? v : 0n;
  } catch {
    return 100n;
  }
})();

const notCountedLogAt = new Map(); // `${wallet}:${reason}` -> lastAt
function logNotCounted(wallet, reason) {
  try {
    const w = String(wallet || '').trim();
    const r = String(reason || '').trim();
    if (!w || !r) return;
    const key = `${w}:${r}`;
    const now = Date.now();
    const last = notCountedLogAt.get(key) || 0;
    if (now - last < 60_000) return;
    notCountedLogAt.set(key, now);
    // eslint-disable-next-line no-console
    console.warn('[api:/pq/view] not counted', { wallet: w, reason: r });
  } catch {
    // ignore
  }
}

function isCidLike(q) {
  const s = String(q || '').trim();
  if (!s) return false;
  if (/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/.test(s)) return true;
  if (/^[bB][a-z2-7]{50,}$/.test(s)) return true;
  return false;
}

export async function postPqView(req, res) {
  try {
    const pqHeader = String(req.header('X-Lumen-PQ') || '').trim().toLowerCase();
    if (pqHeader !== 'v1') {
      return res.status(400).json({ error: 'pq_required', message: 'pq_required' });
    }

    const result = await decryptPqRequest(req);
    if (!result.ok) {
      return res
        .status(result.status || 400)
        .json({ error: result.error || 'pq_error', message: result.message || 'pq_error' });
    }

    const payload = result.payload || {};
    const cid = String(payload.cid || '').trim();
    if (!cid) {
      return res.status(400).json({ error: 'cid_required', message: 'cid_required' });
    }
    if (!isCidLike(cid)) {
      return res.status(400).json({ error: 'cid_invalid', message: 'cid_invalid' });
    }

    const balanceCheck = await walletHasMinUlmn(result.wallet, MIN_VIEW_BALANCE_ULMN, {
      ttlMs: 60_000,
      timeoutMs: 2500
    }).catch(() => null);

    if (!balanceCheck || !balanceCheck.ok) {
      logNotCounted(result.wallet, 'balance_unreachable');
      return res.json({ ok: true, cid, counted: false, reason: 'balance_unreachable' });
    }

    if (!balanceCheck.hasMin) {
      logNotCounted(result.wallet, 'balance_too_low');
      return res.json({ ok: true, cid, counted: false, reason: 'balance_too_low' });
    }

    try {
      await recordCidAccess({
        cid,
        wallet: result.wallet,
        ok: true,
        status: 200
      });
    } catch (err) {
      try {
        // eslint-disable-next-line no-console
        console.warn('[api:/pq/view] recordCidAccess failed', String(err?.message || err));
      } catch {
        // ignore
      }
    }

    return res.json({ ok: true, cid, counted: true });
  } catch (err) {
    try {
      // eslint-disable-next-line no-console
      console.error('[api:/pq/view] error', err);
    } catch {
      // ignore
    }
    return res.status(500).json({ error: 'internal_error' });
  }
}
