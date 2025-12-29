import crypto from 'node:crypto';
import { kuboRequest } from '../lib/kuboClient.js';
import { hasWalletRoot, hasWalletPin } from '../lib/walletDb.js';

export async function getIsPinned(req, res) {
  try {
    const wallet = String(req.wallet || '').trim() || null;

    let cid = '';
    if (req.query && typeof req.query.cid !== 'undefined') {
      cid = String(req.query.cid || '').trim();
    } else if (req.body && typeof req.body === 'object' && typeof req.body.cid !== 'undefined') {
      cid = String(req.body.cid || '').trim();
    }

    const send = (statusCode, body) => {
      const aesKey = req.pqAesKey;
      if (aesKey && Buffer.isBuffer(aesKey)) {
        try {
          const plaintext = Buffer.from(JSON.stringify(body ?? null), 'utf8');
          const iv = crypto.randomBytes(12);
          const cipher = crypto.createCipheriv('aes-256-gcm', aesKey, iv);
          const ct = Buffer.concat([cipher.update(plaintext), cipher.final()]);
          const tag = cipher.getAuthTag();

          return res.status(statusCode).json({
            ciphertext: ct.toString('base64'),
            iv: iv.toString('base64'),
            tag: tag.toString('base64')
          });
        } catch (encErr) {
          // eslint-disable-next-line no-console
          console.error('[api:/ispinned] pq response encrypt error', encErr);
          return res
            .status(500)
            .json({ error: 'pq_encrypt_failed', message: 'failed_to_encrypt_response' });
        }
      }

      return res.status(statusCode).json(body);
    };

    if (!cid) return send(400, { error: 'cid_required' });

    let isPinnedGlobal = false;

    try {
      const resp = await kuboRequest(
        `/api/v0/pin/ls?arg=${encodeURIComponent(
          cid
        )}&type=recursive&stream=false&quiet=false`
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
    const aesKey = req.pqAesKey;
    if (aesKey && Buffer.isBuffer(aesKey)) {
      try {
        const plaintext = Buffer.from(JSON.stringify({ error: 'internal_error' }), 'utf8');
        const iv = crypto.randomBytes(12);
        const cipher = crypto.createCipheriv('aes-256-gcm', aesKey, iv);
        const ct = Buffer.concat([cipher.update(plaintext), cipher.final()]);
        const tag = cipher.getAuthTag();

        return res.status(500).json({
          ciphertext: ct.toString('base64'),
          iv: iv.toString('base64'),
          tag: tag.toString('base64')
        });
      } catch (encErr) {
        // eslint-disable-next-line no-console
        console.error('[api:/ispinned] pq response encrypt error in catch', encErr);
      }
    }
    res.status(500).json({ error: 'internal_error' });
  }
}

