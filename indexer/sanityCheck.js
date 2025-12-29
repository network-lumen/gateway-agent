import http from 'node:http';
import { log, logError } from './log.js';

function makeBaseUrl() {
  const env = process.env.INDEXER_BASE_URL;
  if (env && typeof env === 'string' && env.trim()) {
    return env.replace(/\/+$/, '');
  }
  return 'http://localhost:8790';
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
  let ok = true;
  let presentTotal = null;
  let sampleCid = null;
  let samplePresentSource = null;

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
    const resp = await httpPost('http://ipfs:5001/api/v0/version');
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
    const resp = await httpGet(`${base}/cids?present=1&limit=5`);
    if (resp.statusCode === 200) {
      let parsed = null;
      try {
        parsed = JSON.parse(resp.body);
      } catch {
        parsed = null;
      }
      const count =
        parsed && Array.isArray(parsed.items) ? parsed.items.length : 0;
      presentTotal =
        parsed && typeof parsed.total === 'number' ? parsed.total : null;
      if (count > 0) {
        const first = parsed.items[0];
        sampleCid = first && typeof first.cid === 'string' ? first.cid : null;
        samplePresentSource =
          first && typeof first.present_source === 'string'
            ? first.present_source
            : null;
      }
      log(
        `[sanity] /cids present=1 limit=5 OK items=${count} total=${presentTotal ?? 'n/a'}`
      );
    } else if (resp.statusCode === 404) {
      // older builds might not have /cids; do not fail hard
      log('[sanity] /cids not found (ignoring)');
    } else {
      ok = false;
      logError('[sanity] /cids FAIL status=', resp.statusCode);
    }
  } catch (err) {
    // optional, do not mark as fatal, but log
    logError('[sanity] /cids error (non-fatal)', err);
  }

  // Check that /cid/:cid returns the same present_source as /cids for a sample row
  if (sampleCid) {
    try {
      const resp = await httpGet(`${base}/cid/${encodeURIComponent(sampleCid)}`);
      if (resp.statusCode === 200) {
        let parsed = null;
        try {
          parsed = JSON.parse(resp.body);
        } catch {
          parsed = null;
        }
        const presentSourceCid =
          parsed && typeof parsed.present_source === 'string'
            ? parsed.present_source
            : null;
        if (samplePresentSource != null && presentSourceCid != null) {
          if (samplePresentSource !== presentSourceCid) {
            ok = false;
            logError(
              `[sanity] mismatch: /cids present_source=${samplePresentSource} vs /cid present_source=${presentSourceCid} for cid=${sampleCid}`
            );
          } else {
            log(
              `[sanity] /cid present_source matches /cids for cid=${sampleCid} (${presentSourceCid})`
            );
          }
        }
      } else {
        ok = false;
        logError('[sanity] /cid sample FAIL status=', resp.statusCode);
      }
    } catch (err) {
      ok = false;
      logError('[sanity] /cid sample error', err);
    }
  }

  // Optional consistency check: compare /cids total vs /metrics/state pins_current
  if (presentTotal != null) {
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
          parsed && typeof parsed.pins_current === 'number'
            ? parsed.pins_current
            : null;
        if (pinsCurrent != null && pinsCurrent !== presentTotal) {
          ok = false;
          logError(
            `[sanity] mismatch: /cids total present=${presentTotal} vs /metrics pins_current=${pinsCurrent}`
          );
        } else if (pinsCurrent != null) {
          log(
            `[sanity] metrics pins_current matches /cids total (${pinsCurrent})`
          );
        }
      } else {
        ok = false;
        logError(
          '[sanity] /metrics/state FAIL status=',
          resp.statusCode
        );
      }
    } catch (err) {
      ok = false;
      logError('[sanity] /metrics/state error', err);
    }
  }

  // Check for present=1 with removed_at != NULL
  try {
    const resp = await httpGet(`${base}/consistency`);
    if (resp.statusCode === 200) {
      let parsed = null;
      try {
        parsed = JSON.parse(resp.body);
      } catch {
        parsed = null;
      }
      const n =
        parsed && typeof parsed.present_with_removed_at === 'number'
          ? parsed.present_with_removed_at
          : null;
      if (n != null && n > 0) {
        ok = false;
        logError(
          `[sanity] inconsistency: ${n} rows with present=1 and removed_at IS NOT NULL`
        );
      } else {
        log('[sanity] no present=1 && removed_at!=NULL rows');
      }
    } else {
      ok = false;
      logError('[sanity] /consistency FAIL status=', resp.statusCode);
    }
  } catch (err) {
    ok = false;
    logError('[sanity] /consistency error', err);
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
