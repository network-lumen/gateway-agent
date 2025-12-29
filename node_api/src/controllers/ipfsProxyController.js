import { CONFIG } from '../config.js';
import { decryptPqRequest } from '../middleware/pqMiddleware.js';

const GATEWAY_BASE = CONFIG.KUBO_GATEWAY_BASE;

async function proxyToKubo(pathname, res, queryParams) {
  try {
    const url = new URL(pathname, GATEWAY_BASE);
    if (queryParams && typeof queryParams === 'object') {
      for (const [key, value] of Object.entries(queryParams)) {
        if (value === undefined || value === null) continue;
        url.searchParams.set(key, String(value));
      }
    }

    const upstream = await fetch(url.toString(), {
      method: 'GET',
      // Explicitly disable automatic decompression so we don't confuse Express.
      // Kubo usually serves plain text/HTML here.
      // @ts-ignore
      compress: false
    });

    res.status(upstream.status);
    upstream.headers.forEach((value, key) => {
      const k = key.toLowerCase();
      if (k === 'connection' || k === 'keep-alive' || k === 'transfer-encoding') return;
      res.setHeader(key, value);
    });

    try {
      const ab = await upstream.arrayBuffer();
      if (ab && ab.byteLength) {
        res.send(Buffer.from(ab));
      } else {
        res.end();
      }
    } catch {
      res.end();
    }
  } catch (err) {
    console.error('[api:/ipfs-proxy] error', err);
    if (!res.headersSent) {
      res.status(502).json({ error: 'ipfs_gateway_error' });
    } else {
      res.end();
    }
  }
}

export async function proxyIpfs(req, res) {
  try {
    // Preserve only path + query/hash when proxying.
    const path = req.originalUrl || req.url || '/';
    const targetUrl = new URL(path, GATEWAY_BASE).toString();

    const upstream = await fetch(targetUrl, {
      method: req.method,
      // Explicitly disable automatic decompression so we don't confuse Express.
      // Kubo usually serves plain text/HTML here.
      // @ts-ignore
      compress: false
    });

    res.status(upstream.status);
    upstream.headers.forEach((value, key) => {
      const k = key.toLowerCase();
      if (k === 'connection' || k === 'keep-alive' || k === 'transfer-encoding') return;
      res.setHeader(key, value);
    });

    // For HEAD requests, we only need headers + status.
    if (req.method === 'HEAD') {
      res.end();
      return;
    }

    // Buffer the body (responses are typically small HTML or JSON)
    try {
      const ab = await upstream.arrayBuffer();
      if (ab && ab.byteLength) {
        res.send(Buffer.from(ab));
      } else {
        res.end();
      }
    } catch {
      // If reading the body fails, still end the response.
      res.end();
    }
  } catch (err) {
    console.error('[api:/ipfs-proxy] error', err);
    if (!res.headersSent) {
      res.status(502).json({ error: 'ipfs_gateway_error' });
    } else {
      res.end();
    }
  }
}

export async function postPqIpfs(req, res) {
  const pqHeader = String(req.header('X-Lumen-PQ') || '').trim().toLowerCase();
  if (pqHeader !== 'v1') {
    return res.status(400).json({ error: 'pq_required', message: 'pq_required' });
  }

  const result = await decryptPqRequest(req);
  if (!result.ok) {
    return res
      .status(result.status || 400)
      .json({ error: result.error || 'pq_error', message: result.message || 'pq_error' });
  }

  const payload = result.payload || {};
  const cid = String(payload.cid || '').trim();
  const path = payload.path ? String(payload.path).trim() : '';
  const query = payload.query && typeof payload.query === 'object' ? payload.query : undefined;

  if (!cid) {
    return res.status(400).json({ error: 'cid_required', message: 'cid is required' });
  }

  let pathname = `/ipfs/${encodeURIComponent(cid)}`;
  if (path) {
    const cleanPath = path.startsWith('/') ? path : `/${path}`;
    pathname += cleanPath;
  }

  return proxyToKubo(pathname, res, query);
}

export async function postPqIpns(req, res) {
  const pqHeader = String(req.header('X-Lumen-PQ') || '').trim().toLowerCase();
  if (pqHeader !== 'v1') {
    return res.status(400).json({ error: 'pq_required', message: 'pq_required' });
  }

  const result = await decryptPqRequest(req);
  if (!result.ok) {
    return res
      .status(result.status || 400)
      .json({ error: result.error || 'pq_error', message: result.message || 'pq_error' });
  }

  const payload = result.payload || {};
  const name = String(payload.name || payload.ipns || '').trim();
  const path = payload.path ? String(payload.path).trim() : '';
  const query = payload.query && typeof payload.query === 'object' ? payload.query : undefined;

  if (!name) {
    return res.status(400).json({ error: 'ipns_required', message: 'ipns name is required' });
  }

  let pathname = `/ipns/${encodeURIComponent(name)}`;
  if (path) {
    const cleanPath = path.startsWith('/') ? path : `/${path}`;
    pathname += cleanPath;
  }

  return proxyToKubo(pathname, res, query);
}
