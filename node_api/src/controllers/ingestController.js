import crypto from 'node:crypto';
import { once } from 'node:events';
import fs from 'node:fs';
import path from 'node:path';
import { kuboRequest } from '../lib/kuboClient.js';
import { recordIngest } from '../lib/walletRegistry.js';
import { sendWebhookEvent } from '../lib/webhook.js';
import { ensureWalletPlanOk } from '../lib/walletPlan.js';
import { addOrUpdateWalletRoots, setWalletCidDisplayName } from '../lib/walletDb.js';
import { enqueueIngestJob } from '../services/ingestQueue.js';
import { CONFIG } from '../config.js';

const TOKEN_TTL_MS = 10 * 60 * 1000; // 10 minutes
const ingestTokens = new Map();
const MAX_CAR_BYTES = Number.isFinite(Number(CONFIG.INGEST_MAX_BYTES))
  ? Number(CONFIG.INGEST_MAX_BYTES)
  : 500 * 1024 * 1024;

const DEFAULT_WALLET_DB_PATH = '/data/node_api/wallets.sqlite';
const WALLET_DB_PATH =
  process.env.NODE_API_WALLET_DB_PATH && process.env.NODE_API_WALLET_DB_PATH.trim()
    ? process.env.NODE_API_WALLET_DB_PATH.trim()
    : DEFAULT_WALLET_DB_PATH;
const DEFAULT_INGEST_TMP_DIR = path.join(path.dirname(WALLET_DB_PATH), 'ingest_tmp');
const INGEST_TMP_DIR = String(process.env.INGEST_TMP_DIR || DEFAULT_INGEST_TMP_DIR).trim()
  || DEFAULT_INGEST_TMP_DIR;

function cleanupTokens() {
  const now = Date.now();
  for (const [token, info] of ingestTokens.entries()) {
    const createdAt = info && typeof info.createdAt === 'number' ? info.createdAt : 0;
    if (!createdAt || now - createdAt > TOKEN_TTL_MS) {
      ingestTokens.delete(token);
    }
  }
}

export function getIngestReady(req, res) {
  const walletFromAuth = String(req.wallet || '').trim();
  const walletFromQuery = String(req.query.wallet || '').trim();
  const wallet = walletFromAuth || walletFromQuery || null;
  return res.json({
    ok: true,
    wallet,
    status: 'ready'
  });
}

export async function postIngestInit(req, res) {
  try {
    const wallet = String(req.wallet || '').trim();
    if (!wallet) {
      return res.status(400).json({ error: 'wallet_required' });
    }

    const planIdRaw =
      req.body && typeof req.body.planId === 'string'
        ? req.body.planId
        : '';
    const planId = planIdRaw.trim() || null;

    let estBytes = null;
    if (req.body && typeof req.body.estBytes === 'number') {
      if (Number.isFinite(req.body.estBytes) && req.body.estBytes > 0) {
        estBytes = Math.floor(req.body.estBytes);
      }
    }

    const displayNameRaw =
      req.body && typeof req.body.displayName === 'string'
        ? req.body.displayName
        : req.body && typeof req.body.display_name === 'string'
          ? req.body.display_name
          : req.body && typeof req.body.name === 'string'
            ? req.body.name
            : req.body && typeof req.body.fileName === 'string'
              ? req.body.fileName
              : req.body && typeof req.body.filename === 'string'
                ? req.body.filename
                : '';
    const displayName = String(displayNameRaw || '').trim() || null;

    try {
      await ensureWalletPlanOk(wallet, null);
    } catch (planErr) {
      // eslint-disable-next-line no-console
      console.error('[api:/ingest/init] plan validation error', {
        code: planErr && planErr.code ? String(planErr.code) : 'unknown_error'
      });
      if (planErr && planErr.code === 'CHAIN_UNREACHABLE') {
        return res.status(503).json({ error: 'chain_unreachable' });
      }
      return res.status(503).json({ error: 'plan_validation_failed' });
    }

    cleanupTokens();

    const token = crypto.randomBytes(32).toString('hex');
    ingestTokens.set(token, {
      wallet,
      planId,
      estBytes,
      displayName,
      createdAt: Date.now()
    });

    return res.json({
      ok: true,
      upload_token: token,
      planId,
      wallet
    });
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[api:/ingest/init] error', {
      error: err && err.code ? String(err.code) : 'internal_error'
    });
    return res.status(500).json({ error: 'internal_error' });
  }
}

export async function postIngestCar(req, res) {
  try {
    const token = String(req.query.token || '').trim();
    if (!token) {
      return res.status(400).json({ error: 'upload_token_required' });
    }

    cleanupTokens();
    const entry = ingestTokens.get(token);
    if (!entry || !entry.wallet) {
      return res.status(400).json({ error: 'upload_token_invalid' });
    }
    ingestTokens.delete(token);

    const wallet = String(entry.wallet || '').trim();
    const displayNameFromToken =
      typeof entry.displayName === 'string' && entry.displayName.trim()
        ? entry.displayName.trim()
        : null;
    const planIdFromToken =
      typeof entry.planId === 'string' && entry.planId.trim()
        ? entry.planId.trim()
        : null;

    const planIdQuery = String(req.query.planId || '').trim();
    const planId = planIdFromToken || planIdQuery || null;

    // Plan validation / cache: requires on-chain gateways module to be reachable
    try {
      await ensureWalletPlanOk(wallet, null);
    } catch (planErr) {
      // eslint-disable-next-line no-console
      console.error('[api:/ingest/car] plan validation error', {
        code: planErr && planErr.code ? String(planErr.code) : 'unknown_error'
      });
      if (planErr && planErr.code === 'CHAIN_UNREACHABLE') {
        return res.status(503).json({ error: 'chain_unreachable' });
      }
      return res.status(503).json({ error: 'plan_validation_failed' });
    }

    const fileCt = req.header('Content-Type') || 'application/car';
    let uploadedBytes = 0;
    let tooLarge = false;

    // Spool to disk to avoid buffering large uploads in RAM.
    let tmpFile = null;
    try {
      fs.mkdirSync(INGEST_TMP_DIR, { recursive: true });
      const name = `upload-${Date.now()}-${crypto.randomBytes(8).toString('hex')}.car`;
      tmpFile = path.join(INGEST_TMP_DIR, name);
    } catch (mkErr) {
      // eslint-disable-next-line no-console
      console.error('[api:/ingest/car] failed to create tmp dir', {
        dir: INGEST_TMP_DIR,
        error: mkErr?.message || String(mkErr)
      });
      return res.status(500).json({ error: 'internal_error' });
    }

    const ws = fs.createWriteStream(tmpFile, { flags: 'wx' });
    const writeDone = new Promise((resolve, reject) => {
      ws.on('finish', resolve);
      ws.on('error', reject);
    });

    for await (const chunk of req) {
      if (!chunk) continue;
      const buf =
        Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      uploadedBytes += buf.length;
      if (MAX_CAR_BYTES && uploadedBytes > MAX_CAR_BYTES) {
        tooLarge = true;
        // Continue draining the stream but do not write additional bytes.
        continue;
      }
      if (!ws.write(buf)) await once(ws, 'drain');
    }

    ws.end();
    await writeDone;

    if (tooLarge) {
      try { await fs.promises.unlink(tmpFile); } catch {}
      return res.status(413).json({
        error: 'car_too_large',
        max_bytes: MAX_CAR_BYTES
      });
    }

    const jobId = enqueueIngestJob(async () => {
      try {
      const boundary = `----lumenFormBoundary${Math.random()
        .toString(16)
        .slice(2)}`;
      const filename = 'upload.car';
      const pre = Buffer.from(
        `--${boundary}\r\n` +
          `Content-Disposition: form-data; name="file"; filename="${filename}"\r\n` +
          `Content-Type: ${fileCt}\r\n\r\n`,
        'utf8'
      );
      const post = Buffer.from(`\r\n--${boundary}--\r\n`, 'utf8');

      async function* multipartStream() {
        yield pre;
        for await (const buf of fs.createReadStream(tmpFile)) {
          yield buf;
        }
        yield post;
      }

      const resp = await kuboRequest('/api/v0/dag/import?pin-roots=true', {
        method: 'POST',
        headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}` },
        // @ts-ignore duplex is still experimental in Node
        duplex: 'half',
        body: multipartStream(),
        timeoutMs: CONFIG.KUBO_IMPORT_TIMEOUT_MS
      });

      const text = await resp.text();
      if (!resp.ok) {
        return;
      }

      const roots = [];
      for (const line of String(text).split(/\r?\n/)) {
        try {
          const obj = JSON.parse(line);
          const c = obj?.Root?.Cid?.['/'] || obj?.Root?.['/'];
          if (typeof c === 'string' && c) roots.push(c);
        } catch {}
      }
      const uniqueRoots = Array.from(new Set(roots));

      recordIngest(wallet, uploadedBytes);

      try {
        await addOrUpdateWalletRoots({
          wallet,
          roots: uniqueRoots,
          bytesEstimated: uploadedBytes
        });
      } catch (dbErr) {
        // eslint-disable-next-line no-console
        console.error('[api:/ingest/car] wallet_roots insert failed', {
          error: dbErr && dbErr.code ? String(dbErr.code) : 'db_error'
        });
      }

      if (displayNameFromToken) {
        for (const rootCid of uniqueRoots) {
          try {
            // eslint-disable-next-line no-await-in-loop
            await setWalletCidDisplayName(wallet, rootCid, displayNameFromToken);
          } catch (nameErr) {
            // eslint-disable-next-line no-console
            console.error('[api:/ingest/car] wallet_cid_metadata upsert failed', {
              error: nameErr && nameErr.code ? String(nameErr.code) : 'db_error'
            });
          }
        }
      }

      const meta = {
        wallet,
        planId: planId || null,
        uploadedBytes
      };

      void sendWebhookEvent('ingest', {
        ...meta,
        roots: uniqueRoots
      });
      } finally {
        try { await fs.promises.unlink(tmpFile); } catch {}
      }
    }, { bytes: uploadedBytes });

    res.json({
      ok: true,
      roots: [],
      meta: {
        wallet,
        planId: planId || null,
        uploadedBytes,
        jobId
      }
    });
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[api:/ingest/car] error', {
      error: err && err.code ? String(err.code) : 'internal_error'
    });
    res.status(500).json({ error: 'internal_error' });
  }
}
