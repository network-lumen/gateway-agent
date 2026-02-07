import crypto from 'node:crypto';

export function sendPqJson(req, res, statusCode, body, logLabel = 'pq') {
  const code =
    typeof statusCode === 'number' && Number.isFinite(statusCode) && statusCode > 0
      ? Math.floor(statusCode)
      : 200;

  const aesKey = req?.pqAesKey;
  if (aesKey && Buffer.isBuffer(aesKey)) {
    try {
      const plaintext = Buffer.from(JSON.stringify(body ?? null), 'utf8');
      const iv = crypto.randomBytes(12);
      const cipher = crypto.createCipheriv('aes-256-gcm', aesKey, iv);
      const ct = Buffer.concat([cipher.update(plaintext), cipher.final()]);
      const tag = cipher.getAuthTag();

      return res.status(code).json({
        ciphertext: ct.toString('base64'),
        iv: iv.toString('base64'),
        tag: tag.toString('base64')
      });
    } catch (err) {
      try {
        // eslint-disable-next-line no-console
        console.error(`[${logLabel}] pq response encrypt error`, err);
      } catch {
        // ignore
      }
      return res
        .status(500)
        .json({ error: 'pq_encrypt_failed', message: 'failed_to_encrypt_response' });
    }
  }

  return res.status(code).json(body);
}

