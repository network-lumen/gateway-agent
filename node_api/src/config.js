import path from 'node:path';
import fs from 'node:fs';

const CONFIG_FILE =
  process.env.GATEWAY_CONFIG_FILE || path.resolve(process.cwd(), 'config.json');

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
const PUBLIC_ENDPOINT =
  process.env.PUBLIC_ENDPOINT ||
  (gatewayConfig && typeof gatewayConfig.public === 'string'
    ? gatewayConfig.public
    : `http://localhost:${PORT}`);

// In Docker, we talk to the Kubo and indexer services by their compose service names.
const KUBO_API_BASE = 'http://ipfs:5001'.replace(/\/+$/, '');
const KUBO_GATEWAY_BASE = 'http://ipfs:8080'.replace(/\/+$/, '');
const INDEXER_BASE =
  (process.env.INDEXER_BASE_URL || 'http://indexer:8790').replace(
    /\/+$/,
    ''
  );
const CHAIN_REST_BASE_URL =
  (process.env.CHAIN_REST_BASE_URL || 'http://host.docker.internal:1317').replace(
    /\/+$/,
    ''
  );

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
      : 500 * 1024 * 1024
};
