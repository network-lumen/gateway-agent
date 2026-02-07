import { kuboRequest } from '../lib/kuboClient.js';
import { hasWalletRoot, hasWalletPin } from '../lib/walletDb.js';
import { sendPqJson } from '../lib/pqResponse.js';

export async function getIsPinned(req, res) {
  try {
    const wallet = String(req.wallet || '').trim() || null;

    let cid = '';
    if (req.query && typeof req.query.cid !== 'undefined') {
      cid = String(req.query.cid || '').trim();
    } else if (req.body && typeof req.body === 'object' && typeof req.body.cid !== 'undefined') {
      cid = String(req.body.cid || '').trim();
    }

    const send = (statusCode, body) => sendPqJson(req, res, statusCode, body, 'api:/ispinned');

    if (!cid) return send(400, { error: 'cid_required' });

    let isPinnedGlobal = false;

    try {
      const resp = await kuboRequest(
        `/api/v0/pin/ls?arg=${encodeURIComponent(
          cid
        )}&type=recursive&stream=false&quiet=false`,
        { timeoutMs: 4000 }
      );
      if (resp.ok) {
        const text = await resp.text();
        const parsed = JSON.parse(text);
        isPinnedGlobal = !!parsed.Keys?.[cid];
      }
    } catch {
      // Best-effort: if IPFS is down, we leave isPinned = false.
    }

    let hasWalletEntry = false;
    if (wallet) {
      try {
        // Consider both logical pins (wallet_roots) and legacy pins (wallet_pins).
        const byRoot = await hasWalletRoot(wallet, cid);
        const byPin = await hasWalletPin(wallet, cid);
        hasWalletEntry = byRoot || byPin;
      } catch (dbErr) {
        // eslint-disable-next-line no-console
        console.error('[api:/ispinned] wallet lookup failed', dbErr);
      }
    }

    const isPinnedForWallet = isPinnedGlobal && hasWalletEntry;

    return send(200, {
      wallet,
      cid,
      pinned: isPinnedForWallet
    });
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[api:/ispinned] error', err);
    return sendPqJson(req, res, 500, { error: 'internal_error' }, 'api:/ispinned');
  }
}

