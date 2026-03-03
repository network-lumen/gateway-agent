import express from 'express';
import crypto from 'node:crypto';
import path from 'node:path';
import { CONFIG } from './config.js';
import { buildRouter } from './routes/index.js';
import { initSearchModels } from './search/modelLifecycle.js';
import { httpMetricsMiddleware } from './middleware/httpMetrics.js';
import { initKyberContext } from './lib/kyberContext.js';
import { debugLog, getDebugSpec } from './lib/logger.js';

const app = express();
app.use(express.json({ limit: '1mb' }));

// HTTP metrics (latency, status, path)
app.use(httpMetricsMiddleware);
// Attach body hash for signature payload
app.use((req, _res, next) => {
  if (req.is('application/json') && req.body) {
    try {
      req.hash = crypto.createHash('sha256').update(JSON.stringify(req.body)).digest('hex');
    } catch {
      req.hash = '';
    }
  } else {
    req.hash = '';
  }
  next();
});

// Basic CORS
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, X-Lumen-Addr, X-Lumen-Sig, X-Lumen-Nonce, X-Lumen-Ts, X-Lumen-PubKey, X-Lumen-PQ, X-Lumen-KEM, X-Lumen-KeyId');
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

app.use('/', buildRouter());

// Initialise Kyber context and search models once at startup.
await (async () => {
  debugLog('init', 'config', {
    configFile: CONFIG.CONFIG_FILE,
    port: CONFIG.PORT,
    region: CONFIG.REGION,
    public: CONFIG.PUBLIC_ENDPOINT,
    kuboApiBase: CONFIG.KUBO_API_BASE,
    kuboGatewayBase: CONFIG.KUBO_GATEWAY_BASE,
    indexerBase: CONFIG.INDEXER_BASE,
    chainRestBaseUrl: CONFIG.CHAIN_REST_BASE_URL,
    debug: getDebugSpec()
  });

  const walletDbPath =
    process.env.NODE_API_WALLET_DB_PATH && process.env.NODE_API_WALLET_DB_PATH.trim()
      ? process.env.NODE_API_WALLET_DB_PATH.trim()
      : '/data/node_api/wallets.sqlite';
  const ingestTmpDir =
    String(process.env.INGEST_TMP_DIR || '').trim() ||
    path.join(path.dirname(walletDbPath), 'ingest_tmp');
  debugLog('init', 'storage', {
    walletDbPath,
    ingestTmpDir
  });

  try {
    await initKyberContext();
  } catch (err) {
    // initKyberContext is expected to exit the process on fatal errors
    // eslint-disable-next-line no-console
    console.error('[kyber:init] failed', err);
    process.exit(1);
  }
  try {
    await initSearchModels();
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[search:init] failed', err);
  }
})();

app.listen(CONFIG.PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`[node_api] listening on :${CONFIG.PORT}`);
});

export { app };
