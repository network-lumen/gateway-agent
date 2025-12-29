# Lumen Gateway Agent

This repository contains everything needed to run a Lumen “gateway agent”:

- an IPFS Kubo daemon (data plane),
- an indexer (content typing, tagging, search signals, metrics),
- a node API (wallet‑aware gateway API, PQ transport),
- Prometheus + Grafana (monitoring).

The goal is a minimal but production‑ready way to operate an IPFS gateway with:

- on‑chain plans and quotas per wallet,
- deterministic tagging and search,
- post‑quantum confidentiality for all wallet‑authenticated control‑plane requests.

---

## 1. Repository layout

- `node_api/`
  - `src/server.js` – Express bootstrap.
  - `src/routes/index.js` – public + admin routes.
  - `src/controllers/*` – HTTP handlers (`/pin`, `/wallet/usage`, `/wallet/cids`, `/search`, `/metrics`, etc.).
  - `src/middleware/authWallet.js` – wallet auth + PQ envelope decryption.
  - `src/lib/*` – IPFS client, indexer client, wallet DB, chain client, webhook client, Kyber key context.
  - `scripts/gen_kyber_key.js` – CLI to generate a Kyber keypair and on‑chain metadata.
- `indexer/`
  - `config.js` – indexer configuration (Kubo endpoints, timing, sampling limits).
  - `detectType.js` – type detector (magic bytes + containers + heuristics).
  - `contentSniffer.js` – HTML/text/doc/image/video content analysis.
  - `textTagger.js` – text topics/tokens via `@xenova/transformers`.
  - `imageTagger.js` – CLIP zero‑shot image tags via `@xenova/transformers`.
  - `metrics.js` / `httpMetrics.js` – Prometheus metrics for the indexer.
- `grafana/` – dashboards and provisioning for Grafana.
- `prometheus.yml` – minimal Prometheus config scraping node_api + indexer.
- `docker-compose.yml` – reference stack (ipfs + indexer + node_api + prometheus + grafana).
- `config.json` – gateway configuration (operator address, region/public URL, pricing, webhook).

For detailed behavior of the node API, see `node_api/README.md`. For the indexer internals, see `indexer/README.md`.

---

## 2. Running a gateway quickly (Docker)

From a clone of this repository on a Linux host:

```bash
cd /path/to/gateway-agent

# 1) Prepare host directories
sudo mkdir -p /opt/lumen/gateway/ipfs_data
sudo mkdir -p /opt/lumen/gateway/indexer_data
sudo mkdir -p /opt/lumen/gateway/node_api_data
sudo mkdir -p /opt/lumen/gateway/secrets
sudo chown -R "$USER":"$USER" /opt/lumen/gateway

# 2) Generate a Kyber keypair
cd node_api
node scripts/gen_kyber_key.js --write-secret ./kyber.json > /tmp/kyber-meta.json
mv ./kyber.json /opt/lumen/gateway/secrets/kyber.json

# 3) Go back to the root and start the stack
cd ..
docker compose up --build
```

The reference `docker-compose.yml` wires volumes and ports as follows:

- Kubo:
  - host: `/opt/lumen/gateway/ipfs_data` → container: `/data/ipfs`,
  - host ports: `5001` (Kubo API), `18080` (Kubo gateway).
- indexer:
  - host: `/opt/lumen/gateway/indexer_data` → container: `/data`,
  - host port: `8790`.
- node_api:
  - host: `./config.json` → container: `/app/config.json` (read‑only),
  - host: `/opt/lumen/gateway/ipfs_data` → container: `/data/ipfs` (read‑only),
  - host: `/opt/lumen/gateway/node_api_data` → container: `/data/node_api`,
  - host: `/opt/lumen/gateway/secrets/kyber.json` → container: `/secrets/kyber.json` (read‑only),
  - env: `NODE_API_WALLET_DB_PATH=/data/node_api/wallets.sqlite`,
  - env: `CHAIN_REST_BASE_URL=http://142.132.201.187:1317`,
  - env: `LUMEN_GATEWAY_KYBER_KEY_PATH=/secrets/kyber.json`,
  - host port: `8787` (public gateway API).

### 2.1 Health checks

Once the stack is up, from the host:

```bash
# IPFS API (local only; GET is 405, use POST)
curl -s -X POST 'http://localhost:5001/api/v0/id?enc=json'

# Node API status
curl -s http://localhost:8787/status | jq .

# Kyber public key + hash
curl -s http://localhost:8787/pq/pub | jq .

# Indexer metrics
curl -s http://localhost:8790/metrics | head -n5
```

From a remote machine (using the public IP `91.99.166.223` in this example):

```bash
curl -s http://91.99.166.223:8787/status | jq .
```

If you use `ufw`, a minimal firewall setup is:

```bash
sudo ufw allow 22/tcp
sudo ufw allow 8787/tcp
sudo ufw enable
```

This exposes only SSH and the node API; Kubo’s API (`5001`) and raw gateway (`18080`) stay private by default.

### 2.2 Adjusting environment variables

Before running in your own environment, review and adapt the `environment` section of the `node_api` service in `docker-compose.yml`, for example:

```yaml
  node_api:
    environment:
      - NODE_API_WALLET_DB_PATH=/data/node_api/wallets.sqlite
      - CHAIN_REST_BASE_URL=http://YOUR-CHAIN-NODE:1317
      - LUMEN_GATEWAY_KYBER_KEY_PATH=/secrets/kyber.json
```

Typical changes:

- set `CHAIN_REST_BASE_URL` to your own Lumen REST endpoint (local full node, sentry, or provider),
- keep `NODE_API_WALLET_DB_PATH` and `LUMEN_GATEWAY_KYBER_KEY_PATH` aligned with the volumes you mount,
- update `config.json` (at the repo root) with your `operator.address`, `region`, and `public` URL so `/status` reflects your deployment.
- REGION=eu-west
- PUBLIC_ENDPOINT=http://<ip>:<port>

---

## 3. Registering the gateway on-chain (Kyber)

The node API enforces PQ‑encrypted authWallet requests using a Kyber768 keypair. The public key is authenticated via a hash stored in the gateway’s on‑chain metadata.

### 3.1 Generate a Kyber keypair

The `node_api/scripts/gen_kyber_key.js` script generates:

- `gateway_metadata.crypto.kyber` – the on‑chain metadata fragment,
- `gateway_local_secret` – the local secret JSON for the gateway.

Example:

```bash
cd /path/to/gateway-agent/node_api
node scripts/gen_kyber_key.js --write-secret ./kyber.json > /tmp/kyber-meta.json
cat /tmp/kyber-meta.json | jq .gateway_metadata.crypto.kyber
```

The `gateway_metadata.crypto.kyber` block looks like:

```json
{
  "alg": "kyber768",
  "key_id": "gw-2025-12",
  "pubkey_hash": "BASE64_SHA256_OF_PUBKEY"
}
```

The `gateway_local_secret` block looks like:

```json
{
  "alg": "kyber768",
  "key_id": "gw-2025-12",
  "pubkey": "BASE64_PUBLIC_KEY",
  "privkey": "BASE64_PRIVATE_KEY"
}
```

You are responsible for storing `gateway_local_secret` (typically moved to `/opt/lumen/gateway/secrets/kyber.json`) and setting `LUMEN_GATEWAY_KYBER_KEY_PATH` to point to it inside the container.

### 3.2 Publish Kyber metadata on-chain

In the Lumen gateway management UI (`lumen://gateways`) or via CLI, set the gateway metadata to include at least the `crypto.kyber` block:

```json
{
  "crypto": {
    "kyber": {
      "alg": "kyber768",
      "key_id": "gw-2025-12",
      "pubkey_hash": "BASE64_SHA256_OF_PUBKEY"
    }
  }
}
```

Clients will:

1. resolve this metadata from chain,
2. read `crypto.kyber.pubkey_hash`,
3. fetch `GET /pq/pub` from the gateway,
4. hash the `pub` and compare to `pubkey_hash`,
5. refuse to talk to the gateway if the hash mismatches.

You can inspect the live key from the gateway with:

```bash
curl -s http://localhost:8787/pq/pub | jq .
```

---

## 4. Determinism and gateway integrity

Given the same pinned CIDs, software version and configuration, two independent `gateway-agent` deployments will converge to the same content types, tags and search results for a given query. All ranking and classification is derived from on‑disk bytes (plus deterministic models and heuristics); there is no hidden per‑user state, click tracking or popularity signal.

This means the integrity of a gateway can be audited with equivalent data: if two operators ingest the same CARs/pins and keep the same configuration, their public `/search` (and related) responses should match within normal limits. Any moderation or policy layer (for example, blocking clearly illegal content) is expected to be implemented as an explicit, documented layer on top of the gateway; the reference implementation does not include a “silent” censorship path.

---

## 5. Where to go next

- **node API internals and PQ transport** – see `node_api/README.md` for a detailed description of endpoints, PQ envelopes, and wallet usage reporting.
- **Indexer internals and tagging** – see `indexer/README.md` for content sniffing, type detection, tagging, and metrics.
- **Browser client** – the companion Electron app (in `browser/browser`) shows how to:
  - resolve gateway endpoints from on‑chain DNS,
  - enforce Kyber pubkey hash checks,
  - send PQ‑encrypted authWallet requests for `/wallet/usage`, `/wallet/cids`, `/pin`, `/unpin`, `/ispinned`, `/ingest/*`,
  - surface usage, plans and pinned state in a UI.

