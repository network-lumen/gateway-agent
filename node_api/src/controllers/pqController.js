import { createHash } from 'node:crypto';
import { getKyberContext } from '../lib/kyberContext.js';

export function getKyberPublicKey(req, res) {
  try {
    const ctx = getKyberContext();
    const pubB64 = Buffer.from(ctx.pubKeyBytes).toString('base64');
    const pubkeyHash = createHash('sha256')
      .update(Buffer.from(ctx.pubKeyBytes))
      .digest('base64');
    res.json({
      alg: ctx.alg,
      key_id: ctx.keyId,
      pub: pubB64,
      pubkey_hash: pubkeyHash
    });
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[pq:/pq/pub] error', err);
    res.status(500).json({ error: 'pq_not_available' });
  }
}
