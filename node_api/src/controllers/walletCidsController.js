import { getWalletPinnedCidsPage } from '../lib/walletDb.js';
import { sendPqJson } from '../lib/pqResponse.js';

export async function getWalletPinnedCids(req, res) {
  try {
    const wallet = String(req.wallet || '').trim();
    if (!wallet) {
      return sendPqJson(req, res, 400, { error: 'wallet_required' }, 'api:/wallet/cids');
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

    return sendPqJson(req, res, 200, body, 'api:/wallet/cids');
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[api:/wallet/cids] error', err);
    return sendPqJson(req, res, 500, { error: 'internal_error' }, 'api:/wallet/cids');
  }
}

