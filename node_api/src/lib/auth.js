import { CONFIG } from '../config.js';

function escapeRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function isWalletSyntaxValid(wallet) {
  const hrp = String(CONFIG.ADDR_HRP || 'lmn').trim() || 'lmn';
  const re = new RegExp(`^${escapeRegex(hrp)}1[0-9a-z]+$`);
  return re.test(String(wallet || '').trim());
}

export function extractWallet(req) {
  const headerWallet = req.header('X-Lumen-Addr')?.trim() || '';
  const ctxWallet =
    typeof req.wallet === 'string' ? String(req.wallet || '').trim() : '';

  const wallet = headerWallet || ctxWallet;

  if (!wallet) {
    return { ok: false, error: 'wallet_required', message: 'wallet_required' };
  }

  if (!isWalletSyntaxValid(wallet)) {
    return { ok: false, error: 'wallet_invalid', message: 'wallet_invalid' };
  }

  return { ok: true, wallet };
}
