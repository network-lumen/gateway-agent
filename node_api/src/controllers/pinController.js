import crypto from 'node:crypto';
import { kuboRequest } from '../lib/kuboClient.js';
import { ensureChainOnline } from '../lib/chain.js';
import { registerWalletHit, recordPin, recordUnpin } from '../lib/walletRegistry.js';
import {
  addWalletPin,
  removeWalletPin,
  hasWalletPin,
  countWalletPinsForCid,
  hasWalletRoot,
  getWalletsForRootCid,
  removeWalletRoot
} from '../lib/walletDb.js';
import { sendWebhookEvent } from '../lib/webhook.js';

export async function postPin(req, res) {
  try {
    const wallet = req.wallet;

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
          console.error('[api:/pin] pq response encrypt error', encErr);
          return res
            .status(500)
            .json({ error: 'pq_encrypt_failed', message: 'failed_to_encrypt_response' });
        }
      }

      return res.status(statusCode).json(body);
    };

    // Require chain to be reachable (cached TTL)
    try {
      await ensureChainOnline();
    } catch (err) {
      if (err && err.code === 'CHAIN_UNREACHABLE') {
        return send(503, { error: 'chain_unreachable' });
      }
      throw err;
    }

    registerWalletHit(wallet);

    const cid = String(req.body?.cid || '').trim();
    if (!cid) return send(400, { error: 'cid_required' });

    const resp = await kuboRequest(
      `/api/v0/pin/add?arg=${encodeURIComponent(cid)}`
    );
    const text = await resp.text();
    if (!resp.ok) {
      return send(502, { error: 'ipfs_pin_failed', details: text.slice(0, 240) });
    }

    recordPin(wallet);

    try {
      await addWalletPin(wallet, cid);
    } catch (dbErr) {
      console.error('[api:/pin] wallet_pins insert failed', dbErr);
    }

    // Fire-and-forget webhook
    void sendWebhookEvent('pin', { wallet, cid });

    return send(200, { ok: true, cid, wallet });
  } catch (err) {
    console.error('[api:/pin] error', err);
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
        console.error('[api:/pin] pq response encrypt error in catch', encErr);
      }
    }
    res.status(500).json({ error: 'internal_error' });
  }
}

export async function postUnpin(req, res) {
  try {
    const wallet = req.wallet;

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
          console.error('[api:/unpin] pq response encrypt error', encErr);
          return res
            .status(500)
            .json({ error: 'pq_encrypt_failed', message: 'failed_to_encrypt_response' });
        }
      }

      return res.status(statusCode).json(body);
    };

    const cid = String(req.body?.cid || '').trim();
    if (!cid) return send(400, { error: 'cid_required' });

    let walletHasPin = false;
    try {
      walletHasPin = await hasWalletPin(wallet, cid);
    } catch (dbErr) {
      console.error('[api:/unpin] wallet_pins hasWalletPin failed', dbErr);
    }

    let walletHasRoot = false;
    try {
      walletHasRoot = await hasWalletRoot(wallet, cid);
    } catch (dbErr) {
      console.error('[api:/unpin] wallet_roots hasWalletRoot failed', dbErr);
    }

    let totalPins = 0;
    try {
      totalPins = await countWalletPinsForCid(cid);
    } catch (dbErr) {
      console.error('[api:/unpin] wallet_pins count failed', dbErr);
    }

    let totalRootOwners = 0;
    try {
      const owners = await getWalletsForRootCid(cid);
      totalRootOwners = Array.isArray(owners) ? owners.length : 0;
    } catch (dbErr) {
      console.error('[api:/unpin] wallet_roots getWalletsForRootCid failed', dbErr);
    }

    const walletHasAnyRef = walletHasPin || walletHasRoot;
    const totalLogicalPins = totalPins + totalRootOwners;

    // If this wallet never had a logical pin/root for this CID,
    // we treat the operation as idempotent at the API level and do not touch IPFS.
    if (!walletHasAnyRef) {
      return send(200, { ok: true, cid, wallet, changed: false });
    }

    // Remove logical references for this wallet (pins + roots).
    try {
      await removeWalletPin(wallet, cid);
    } catch (dbErr) {
      console.error('[api:/unpin] wallet_pins delete failed', dbErr);
    }

    try {
      await removeWalletRoot(wallet, cid);
    } catch (dbErr) {
      console.error('[api:/unpin] wallet_roots delete failed', dbErr);
    }

    // Other wallets still reference this CID:
    // we only remove the entry for this wallet and leave IPFS untouched.
    if (totalLogicalPins > 1) {
      recordUnpin(wallet);

      // Fire-and-forget webhook
      void sendWebhookEvent('unpin', { wallet, cid });

      return send(200, { ok: true, cid, wallet });
    }

    // totalLogicalPins <= 1 => this wallet is the last (or only) logical owner.
    // We then try to remove the Kubo pin; on failure, we keep the DB entry intact.
    const resp = await kuboRequest(
      `/api/v0/pin/rm?arg=${encodeURIComponent(cid)}`
    );
    const text = await resp.text();
    if (!resp.ok) {
      return send(502, { error: 'ipfs_unpin_failed', details: text.slice(0, 240) });
    }

    try {
      await removeWalletPin(wallet, cid);
    } catch (dbErr) {
      console.error('[api:/unpin] wallet_pins delete failed after ipfs', dbErr);
    }

    recordUnpin(wallet);

    // Fire-and-forget webhook
    void sendWebhookEvent('unpin', { wallet, cid });

    return send(200, { ok: true, cid, wallet });
  } catch (err) {
    console.error('[api:/unpin] error', err);
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
        console.error('[api:/unpin] pq response encrypt error in catch', encErr);
      }
    }
    res.status(500).json({ error: 'internal_error' });
  }
}
