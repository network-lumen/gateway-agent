import crypto from 'node:crypto';
import { getWalletPinnedCidsPage } from '../lib/walletDb.js';

export async function getWalletPinnedCids(req, res) {
  try {
    const wallet = String(req.wallet || '').trim();
    if (!wallet) {
      return res.status(400).json({ error: 'wallet_required' });
    }

    const rawPage =
      req.query?.page != null ? String(req.query.page) : String(req.body?.page || '');
    let page = Number.parseInt(rawPage, 10);
    if (!Number.isFinite(page) || page < 1) page = 1;

    const PAGE_SIZE = 200;
    const limitInternal = PAGE_SIZE + 1;
    const offset = (page - 1) * PAGE_SIZE;

    const rows = await getWalletPinnedCidsPage(wallet, {
      limit: limitInternal,
      offset
    });

    const hasMore = rows.length > PAGE_SIZE;
    const slice = hasMore ? rows.slice(0, PAGE_SIZE) : rows;
    const cids = slice
      .map((r) => String(r.cid || '').trim())
      .filter((c) => !!c);

    const body = {
      wallet,
      page,
      page_size: PAGE_SIZE,
      cids,
      has_more: hasMore
    };

    const aesKey = req.pqAesKey;
    if (aesKey && Buffer.isBuffer(aesKey)) {
      try {
        const plaintext = Buffer.from(JSON.stringify(body ?? null), 'utf8');
        const iv = crypto.randomBytes(12);
        const cipher = crypto.createCipheriv('aes-256-gcm', aesKey, iv);
        const ct = Buffer.concat([cipher.update(plaintext), cipher.final()]);
        const tag = cipher.getAuthTag();

        return res.json({
          ciphertext: ct.toString('base64'),
          iv: iv.toString('base64'),
          tag: tag.toString('base64')
        });
      } catch (encErr) {
        // eslint-disable-next-line no-console
        console.error('[api:/wallet/cids] pq response encrypt error', encErr);
        return res
          .status(500)
          .json({ error: 'pq_encrypt_failed', message: 'failed_to_encrypt_response' });
      }
    }

    return res.json(body);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[api:/wallet/cids] error', err);
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
        console.error('[api:/wallet/cids] pq response encrypt error in catch', encErr);
      }
    }
    return res.status(500).json({ error: 'internal_error' });
  }
}

