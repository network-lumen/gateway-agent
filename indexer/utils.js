import { CONFIG } from './config.js';
import { logError } from './log.js';

export function nowMs() {
  return Date.now();
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function fetchWithTimeout(
  url,
  init = {},
  {
    timeoutMs = CONFIG.REQUEST_TIMEOUT_MS,
    retries = 1,
    retryDelayMs = 300
  } = {}
) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const resp = await fetch(url, {
        ...init,
        signal: controller.signal
      });
      clearTimeout(id);
      return resp;
    } catch (err) {
      clearTimeout(id);
      lastErr = err;
      if (attempt < retries) {
        await sleep(retryDelayMs);
        continue;
      }
      logError('fetchWithTimeout error', url, err?.message || err);
      throw err;
    }
  }
  throw lastErr;
}

export async function readResponseBodyLimited(resp, maxBytes) {
  const cap = Number.isFinite(maxBytes) && maxBytes > 0 ? maxBytes : 0;
  if (!cap) {
    return Buffer.alloc(0);
  }

  const body = resp.body;
  if (!body || typeof body.getReader !== 'function') {
    const buf = Buffer.from(await resp.arrayBuffer());
    if (buf.length <= cap) return buf;
    return buf.subarray(0, cap);
  }

  const reader = body.getReader();
  const chunks = [];
  let total = 0;

  try {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const { done, value } = await reader.read();
      if (done || !value) break;
      const remaining = cap - total;
      if (remaining <= 0) break;
      if (value.length <= remaining) {
        chunks.push(value);
        total += value.length;
      } else {
        chunks.push(value.subarray(0, remaining));
        total += remaining;
        break;
      }
    }
  } finally {
    try {
      await reader.cancel();
    } catch {
      // ignore
    }
  }

  if (!chunks.length) {
    return Buffer.alloc(0);
  }
  return Buffer.concat(chunks, total);
}
