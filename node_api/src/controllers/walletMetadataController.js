import { hasWalletPin, hasWalletRoot, setWalletCidDisplayName } from '../lib/walletDb.js';
import { sendPqJson } from '../lib/pqResponse.js';

export async function postWalletCidRename(req, res) {
  try {
    const wallet = String(req.wallet || '').trim();

    const send = (statusCode, body) =>
      sendPqJson(req, res, statusCode, body, 'api:/wallet/cid/rename');

    if (!wallet) return send(400, { error: 'wallet_required' });

    const cid = String(req.body?.cid || '').trim();
    if (!cid) return send(400, { error: 'cid_required' });

    const nameRaw =
      req.body && typeof req.body.displayName === 'string'
        ? req.body.displayName
        : req.body && typeof req.body.display_name === 'string'
          ? req.body.display_name
          : req.body && typeof req.body.name === 'string'
            ? req.body.name
            : null;

    if (nameRaw === null) return send(400, { error: 'name_required' });

    let hasRef = false;
    try {
      const [hasPin, hasRoot] = await Promise.all([
        hasWalletPin(wallet, cid),
        hasWalletRoot(wallet, cid)
      ]);
      hasRef = hasPin || hasRoot;
    } catch (dbErr) {
      // eslint-disable-next-line no-console
      console.error('[api:/wallet/cid/rename] wallet ref check failed', dbErr);
      return send(500, { error: 'internal_error' });
    }

    if (!hasRef) return send(404, { error: 'cid_not_found' });

    const displayName = await setWalletCidDisplayName(wallet, cid, nameRaw);

    return send(200, {
      ok: true,
      wallet,
      cid,
      display_name: displayName
    });
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[api:/wallet/cid/rename] error', err);
    return sendPqJson(req, res, 500, { error: 'internal_error' }, 'api:/wallet/cid/rename');
  }
}

