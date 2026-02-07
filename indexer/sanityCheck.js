import http from 'node:http';
import { log, logError } from './log.js';

function makeBaseUrl() {
  const env = process.env.INDEXER_BASE_URL;
  if (env && typeof env === 'string' && env.trim()) {
    return env.replace(/\/+$/, '');
  }
  return 'http://localhost:8790';
}

function makeKuboUrl() {
  const env = process.env.KUBO_API_BASE;
  if (env && typeof env === 'string' && env.trim()) {
    return env.replace(/\/+$/, '');
  }
  return 'http://ipfs:5001';
}

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, (res) => {
      const chunks = [];
      res.on('data', (d) => chunks.push(d));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        resolve({ statusCode: res.statusCode || 0, body });
      });
    });
    req.on('error', reject);
    req.setTimeout(8000, () => {
      req.destroy(new Error('timeout'));
    });
  });
}

function httpPost(url) {
  return new Promise((resolve, reject) => {
    const req = http.request(url, { method: 'POST' }, (res) => {
      const chunks = [];
      res.on('data', (d) => chunks.push(d));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        resolve({ statusCode: res.statusCode || 0, body });
      });
    });
    req.on('error', reject);
    req.setTimeout(8000, () => {
      req.destroy(new Error('timeout'));
    });
    req.end();
  });
}

async function main() {
  const base = makeBaseUrl();
  const kuboBase = makeKuboUrl();
  let ok = true;

  const sampleCid =
    typeof process.env.SANITY_SAMPLE_CID === 'string'
      ? process.env.SANITY_SAMPLE_CID.trim()
      : '';

  try {
    const resp = await httpGet(`${base}/health`);
    if (resp.statusCode === 200) {
      log('[sanity] /health OK');
    } else {
      ok = false;
      logError('[sanity] /health FAIL status=', resp.statusCode);
    }
  } catch (err) {
    ok = false;
    logError('[sanity] /health error', err);
  }

  try {
    const resp = await httpGet(`${base}/metrics`);
    if (resp.statusCode === 200) {
      log('[sanity] /metrics OK');
    } else {
      ok = false;
      logError('[sanity] /metrics FAIL status=', resp.statusCode);
    }
  } catch (err) {
    ok = false;
    logError('[sanity] /metrics error', err);
  }

  try {
    const resp = await httpPost(`${kuboBase}/api/v0/version`);
    if (resp.statusCode === 200) {
      log('[sanity] kubo /api/v0/version OK');
    } else {
      ok = false;
      logError('[sanity] kubo /api/v0/version FAIL status=', resp.statusCode);
    }
  } catch (err) {
    ok = false;
    logError('[sanity] kubo /api/v0/version error', err);
  }

  try {
    const resp = await httpGet(`${base}/metrics/state`);
    if (resp.statusCode === 200) {
      let parsed = null;
      try {
        parsed = JSON.parse(resp.body);
      } catch {
        parsed = null;
      }

      const pinsCurrent =
        parsed && typeof parsed.pins_current === 'number' ? parsed.pins_current : null;
      const dbRowsCids =
        parsed && typeof parsed.db_rows_cids === 'number' ? parsed.db_rows_cids : null;

      if (pinsCurrent == null || dbRowsCids == null) {
        ok = false;
        logError('[sanity] /metrics/state bad_json');
      } else {
        log(`[sanity] /metrics/state OK pins_current=${pinsCurrent} db_rows_cids=${dbRowsCids}`);
      }
    } else {
      ok = false;
      logError('[sanity] /metrics/state FAIL status=', resp.statusCode);
    }
  } catch (err) {
    ok = false;
    logError('[sanity] /metrics/state error', err);
  }

  try {
    const resp = await httpGet(`${base}/search?token=pdf&present=1&limit=1&offset=0`);
    if (resp.statusCode === 200) {
      let parsed = null;
      try {
        parsed = JSON.parse(resp.body);
      } catch {
        parsed = null;
      }
      const itemsCount =
        parsed && Array.isArray(parsed.items) ? parsed.items.length : null;
      if (itemsCount == null) {
        ok = false;
        logError('[sanity] /search bad_json');
      } else {
        log(`[sanity] /search OK items=${itemsCount}`);
      }
    } else {
      ok = false;
      logError('[sanity] /search FAIL status=', resp.statusCode);
    }
  } catch (err) {
    ok = false;
    logError('[sanity] /search error', err);
  }

  if (sampleCid) {
    try {
      const resp = await httpGet(`${base}/cid/${encodeURIComponent(sampleCid)}`);
      if (resp.statusCode === 200) {
        log(`[sanity] /cid sample OK cid=${sampleCid}`);
      } else {
        ok = false;
        logError('[sanity] /cid sample FAIL status=', resp.statusCode);
      }
    } catch (err) {
      ok = false;
      logError('[sanity] /cid sample error', err);
    }
  }

  if (!ok) {
    process.exitCode = 1;
  } else {
    log('[sanity] all mandatory checks passed');
  }
}

main().catch((err) => {
  logError('[sanity] fatal error', err);
  process.exitCode = 1;
});
