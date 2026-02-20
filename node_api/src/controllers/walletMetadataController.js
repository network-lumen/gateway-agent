import { CID } from 'multiformats/cid';
import { setWalletCidDisplayName } from '../lib/walletDb.js';
import { sendPqJson } from '../lib/pqResponse.js';

function expandCidVariants(cid) {
  const raw = String(cid || '').trim();
  if (!raw) return [];

  try {
    const parsed = CID.parse(raw);
    const variants = new Set([raw]);
    variants.add(parsed.toString());
    try {
      variants.add(parsed.toV1().toString());
    } catch {
      // ignore
    }
    try {
      variants.add(parsed.toV0().toString());
    } catch {
      // ignore
    }
    return Array.from(variants).filter(Boolean);
  } catch {
    return [];
  }
}

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

    // UX: allow renaming even while an upload is still ingesting (root not yet in DB),
    // and store the name across common CID string variants (v0/v1) to avoid mismatch.
    const variants = expandCidVariants(cid);
    if (!variants.length) return send(400, { error: 'cid_invalid' });

    let displayName = null;
    for (const c of variants) {
      // eslint-disable-next-line no-await-in-loop
      displayName = await setWalletCidDisplayName(wallet, c, nameRaw);
    }

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
