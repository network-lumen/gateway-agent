import path from 'node:path';
import fs from 'node:fs';

function parsePositiveInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

function resolveAddrHrp(gatewayConfig) {
  const fromEnv = String(process.env.ADDR_HRP || '').trim();
  if (fromEnv) return fromEnv;

  const opAddr = gatewayConfig?.operator?.address;
  if (typeof opAddr === 'string' && opAddr.trim()) {
    const addr = opAddr.trim();
    const i = addr.indexOf('1');
    if (i > 0) return addr.slice(0, i);
  }

  return 'lmn';
}

const CONFIG_FILE = process.env.GATEWAY_CONFIG_FILE || path.resolve(process.cwd(), 'config.json');

let gatewayConfig = null;
try {
  const raw = fs.readFileSync(CONFIG_FILE, 'utf8');
  gatewayConfig = JSON.parse(raw);
} catch {
  gatewayConfig = null;
}

const PORT = Number(process.env.PORT || 8787);
const REGION =
  process.env.REGION ||
  (gatewayConfig && typeof gatewayConfig.region === 'string'
    ? gatewayConfig.region
    : 'unknown');
const PUBLIC_ENDPOINT = process.env.PUBLIC_ENDPOINT || (
      gatewayConfig && typeof gatewayConfig.public === 'string' ? 
      gatewayConfig.public : `http://localhost:${PORT}`
    );

// In Docker, we talk to the Kubo and indexer services by their compose service names.
// Outside Docker (systemd, bare-metal), override these with env vars.
const KUBO_API_BASE = (process.env.KUBO_API_BASE || 'http://ipfs:5001').replace(/\/+$/, '');
const KUBO_GATEWAY_BASE = (
  process.env.IPFS_GATEWAY_BASE ||
  process.env.KUBO_GATEWAY_BASE ||
  'http://ipfs:8080'
).replace(/\/+$/, '');
const INDEXER_BASE =
  (process.env.INDEXER_BASE_URL || 'http://indexer:8790').replace(
    /\/+$/,
    ''
  );

function resolveChainRestBaseUrl(configObj) {
  const fromEnv = String(process.env.CHAIN_REST_BASE_URL || '').trim();
  if (fromEnv) return fromEnv.replace(/\/+$/, '');

  const seeds = Array.isArray(configObj?.chainSeeds) ? configObj.chainSeeds : [];
  for (const entry of seeds) {
    const rest = typeof entry?.rest === 'string' ? entry.rest.trim() : '';
    if (rest) return rest.replace(/\/+$/, '');
  }

  return 'http://host.docker.internal:1317';
}

const CHAIN_REST_BASE_URL = resolveChainRestBaseUrl(gatewayConfig);

const KUBO_REQUEST_TIMEOUT_MS = parsePositiveInt(process.env.KUBO_REQUEST_TIMEOUT_MS, 15_000);
const KUBO_IMPORT_TIMEOUT_MS = parsePositiveInt(process.env.KUBO_IMPORT_TIMEOUT_MS, 5 * 60_000);
const ADDR_HRP = resolveAddrHrp(gatewayConfig);

export const CONFIG = {
  PORT,
  REGION,
  PUBLIC_ENDPOINT,
  KUBO_API_BASE,
  KUBO_GATEWAY_BASE,
  INDEXER_BASE,
  CHAIN_REST_BASE_URL,
  CONFIG_FILE,
  GATEWAY: gatewayConfig,
  PRICING:
    gatewayConfig && Array.isArray(gatewayConfig.pricing)
      ? gatewayConfig.pricing
      : null,
  OPERATOR:
    gatewayConfig && gatewayConfig.operator
      ? gatewayConfig.operator
      : null,
  CHAIN_SEEDS:
    gatewayConfig && Array.isArray(gatewayConfig.chainSeeds)
      ? gatewayConfig.chainSeeds
      : [],
  WEBHOOK:
    gatewayConfig && gatewayConfig.webhook
      ? gatewayConfig.webhook
      : null,
  INGEST_MAX_BYTES:
    Number.isFinite(Number(process.env.INGEST_MAX_BYTES))
      ? Number(process.env.INGEST_MAX_BYTES)
      : 500 * 1024 * 1024,
  KUBO_REQUEST_TIMEOUT_MS,
  KUBO_IMPORT_TIMEOUT_MS,
  ADDR_HRP
};
