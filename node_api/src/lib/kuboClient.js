import { CONFIG } from '../config.js';

export async function kuboRequest(pathname, init = {}) {
  const url = new URL(pathname, CONFIG.KUBO_API_BASE).toString();
  const resp = await fetch(url, { method: 'POST', ...init });
  return resp;
}

