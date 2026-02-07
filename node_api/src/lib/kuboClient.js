import { CONFIG } from '../config.js';

function fetchWithTimeout(url, init, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  const signal = init && init.signal ? init.signal : null;
  if (signal) {
    if (signal.aborted) controller.abort();
    else signal.addEventListener('abort', () => controller.abort(), { once: true });
  }

  const p = fetch(url, { ...init, signal: controller.signal });
  return p.finally(() => clearTimeout(t));
}

export async function kuboRequest(pathname, init = {}) {
  const url = new URL(pathname, CONFIG.KUBO_API_BASE).toString();
  const timeoutMs =
    typeof init.timeoutMs === 'number' && Number.isFinite(init.timeoutMs) && init.timeoutMs > 0
      ? Math.floor(init.timeoutMs)
      : CONFIG.KUBO_REQUEST_TIMEOUT_MS;

  // Allow passing `timeoutMs` without leaking it into fetch() options.
  // eslint-disable-next-line no-unused-vars
  const { timeoutMs: _timeoutMs, ...fetchInit } = init;

  const resp = await fetchWithTimeout(url, { method: 'POST', ...fetchInit }, timeoutMs);
  return resp;
}
