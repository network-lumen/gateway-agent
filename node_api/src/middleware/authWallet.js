import { decryptPqRequest } from './pqMiddleware.js';

export async function authWallet(req, res, next) {
  try {
    const pqHeader = String(req.header('X-Lumen-PQ') || '').trim().toLowerCase();

    const path = String(req.path || '');
    const isUsageRoute = path === '/wallet/usage' ||
      (path.startsWith('/wallet/') && path.endsWith('/usage'));
    const isCidsRoute = path === '/wallet/cids';
    const isPinRoute = path === '/pin';
    const isUnpinRoute = path === '/unpin';
    const isIspinnedRoute = path === '/ispinned';
    const isIngestReadyRoute = path === '/ingest/ready';
    const isIngestInitRoute = path === '/ingest/init';

    const isPqMandatoryRoute =
      isUsageRoute ||
      isCidsRoute ||
      isPinRoute ||
      isUnpinRoute ||
      isIspinnedRoute ||
      isIngestReadyRoute ||
      isIngestInitRoute;

    // All authWallet routes are PQ-only.
    if (pqHeader !== 'v1') {
      return res
        .status(400)
        .json({ error: 'pq_required', message: 'pq_required' });
    }

    // PQ path: encrypted authWallet request using Kyber + AES-GCM
    if (pqHeader === 'v1') {
      const result = await decryptPqRequest(req);
      if (!result.ok) {
        return res
          .status(result.status ?? 400)
          .json({ error: result.error ?? 'auth_failed', message: result.message });
      }

      req.wallet = result.wallet;
      req.body = result.payload;
      if (result.aesKey) {
        try {
          req.pqAesKey = Buffer.from(result.aesKey);
        } catch {
          req.pqAesKey = null;
        }
      }

      return next();
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[authWallet] error', err);
    res.status(500).json({ error: 'internal_error' });
  }
}
