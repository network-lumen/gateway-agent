import { CONFIG } from './config.js';
import { fetchWithTimeout } from './utils.js';

export async function kuboRequest(pathname, init = {}) {
  const url = new URL(pathname, CONFIG.KUBO_API_BASE).toString();
  const resp = await fetchWithTimeout(
    url,
    { method: 'POST', ...init },
    { retries: 2 }
  );
  return resp;
}

export async function kuboLs(cid) {
  const encoded = encodeURIComponent(cid);
  const resp = await kuboRequest(
    `/api/v0/ls?arg=${encoded}&resolve-type=true`
  );
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(
      `kubo ls failed (${resp.status}): ${text.slice(0, 240)}`
    );
  }

  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error(`kubo ls bad JSON: ${text.slice(0, 240)}`);
  }

  const objects =
    Array.isArray(parsed.Objects) && parsed.Objects.length
      ? parsed.Objects
      : parsed.Objects && typeof parsed.Objects === 'object'
      ? Object.values(parsed.Objects)
      : [];

  const links = [];
  for (const obj of objects) {
    const objLinks = Array.isArray(obj.Links) ? obj.Links : [];
    for (const link of objLinks) {
      const childCid =
        link.Hash || link.Cid || link.CID || (link.Target && link.Target['/']);
      if (!childCid) continue;
      links.push({
        cid: String(childCid),
        name: link.Name || null,
        size:
          typeof link.Size === 'number' && link.Size >= 0
            ? link.Size
            : null
      });
    }
  }

  return links;
}

