import { CONFIG } from '../config.js';
import { debugLog, formatError } from './logger.js';

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

  const method = String(fetchInit.method || 'POST').toUpperCase();

  const start = Date.now();
  debugLog('kubo', 'request', { method, pathname: String(pathname), timeoutMs });
  try {
    const resp = await fetchWithTimeout(url, { method: 'POST', ...fetchInit }, timeoutMs);
    debugLog('kubo', 'response', {
      method,
      pathname: String(pathname),
      status: resp.status,
      ok: resp.ok,
      ms: Date.now() - start
    });
    return resp;
  } catch (err) {
    debugLog('kubo', 'error', {
      method,
      pathname: String(pathname),
      ms: Date.now() - start,
      ...formatError(err)
    });
    throw err;
  }
}
