export function extractWallet(req) {
  const headerWallet = req.header('X-Lumen-Addr')?.trim() || '';
  const ctxWallet =
    typeof req.wallet === 'string' ? String(req.wallet || '').trim() : '';

  const wallet = headerWallet || ctxWallet;

  if (!wallet) {
    return { ok: false, error: 'wallet_required', message: 'wallet_required' };
  }

  if (!/^lmn1[0-9a-z]+$/.test(wallet)) {
    return { ok: false, error: 'wallet_invalid', message: 'wallet_invalid' };
  }

  return { ok: true, wallet };
}

export function requireWallet(req, res) {
  const result = extractWallet(req);
  if (!result.ok) {
    res.status(400).json({ error: result.error, message: result.message });
    return null;
  }
  return result.wallet;
}
