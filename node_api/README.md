# Gateway Node API

This service exposes a small HTTP API for:

- pinning / unpinning CIDs,
- tracking wallet usage and pinned CIDs,
- orchestrating CAR ingestion into a local Kubo node,
- exposing Prometheus metrics.

It is designed to run alongside:

- an IPFS Kubo daemon (data plane),
- the `indexer/` service (type detection, DAG traversal, metrics),
- Prometheus + Grafana.

The node API focuses on wallet semantics, quotas, and post‑quantum transport; all DAG walking and tagging lives in the indexer and Kubo.

---

## 1. Security model (PQ transport)

### 1.1 Control plane vs data plane

- **Control plane (PQ‑encrypted)** – all routes that depend on `authWallet` and touch wallet state are protected with Kyber768 (ML‑KEM) + AES‑256‑GCM:
  - `POST /wallet/usage`
  - `POST /wallet/cids`
  - `POST /wallet/cid/rename`
  - `POST /pin`
  - `POST /unpin`
  - `POST /ispinned`
  - `POST /ingest/ready`
  - `POST /ingest/init`

  For these routes, clear‑text authWallet requests are rejected with:

  ```json
  { "error": "pq_required", "message": "pq_required" }
  ```

- **Data plane (plain HTTP)** – `POST /ingest/car` streams raw CAR bytes to Kubo. The CAR itself is not Kyber‑wrapped; confidentiality of file content relies on libp2p/IPFS as usual.

  To break trivial timing correlation between authWallet requests and CAR upload, `POST /ingest/car` enqueues a background ingest job with a random delay before calling Kubo.

### 1.2 Kyber keypair and on‑chain hash

On startup, the gateway loads a Kyber keypair from a JSON file pointed to by `LUMEN_GATEWAY_KYBER_KEY_PATH`:

```json
{
  "alg": "kyber768",
  "key_id": "gw-2025-12",
  "pubkey": "BASE64_PUBLIC_KEY",
  "privkey": "BASE64_PRIVATE_KEY"
}
```

If the file is missing or malformed, the process exits with a `FATAL:` message. A gateway must not start without a valid Kyber private key.

The public key is exposed via:

```http
GET /pq/pub

{
  "alg": "kyber768",
  "key_id": "gw-2025-12",
  "pub": "BASE64_PUBLIC_KEY",
  "pubkey_hash": "BASE64_SHA256_OF_PUBKEY"
}
```

On‑chain, only the hash is stored in gateway metadata:

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

PQ‑aware clients:

1. resolve the gateway metadata from the chain,
2. read `crypto.kyber.pubkey_hash`,
3. fetch `/pq/pub` from the gateway,
4. recompute `sha256(pub)` and compare to the on‑chain hash,
5. fail hard on mismatch.

This ties the gateway’s Kyber public key to on‑chain metadata and prevents MITM key substitution even on plain HTTP.

### 1.3 Encrypted envelope format

For PQ‑mandatory routes the client sends:

- headers:
  - `X-Lumen-PQ: v1`
  - `X-Lumen-KEM: kyber768`
  - `X-Lumen-KeyId: <key_id>`
- body:

  ```json
  {
    "kem_ct": "<base64 kyber ciphertext>",
    "ciphertext": "<base64 aes-gcm ciphertext>",
    "iv": "<base64 12-byte iv>",
    "tag": "<base64 gcm tag>"
  }
  ```

On the server side (`authWallet` + `decryptPqRequest`):

1. The Kyber ciphertext is decapsulated using the private key from `kyberContext`.
2. An AES‑256 key is derived; AES‑GCM decrypts the payload.
3. The decrypted JSON contains at minimum:

   ```json
   {
     "wallet": "lmn1...",
     "payload": { /* route-specific body */ },
     "signature": "BASE64_SIGNATURE",
     "timestamp": 1766868302493,
     "nonce": "01d6bd0bed617778edc97e7c"
   }
   ```

4. On success:
   - `req.wallet` is set to the authenticated wallet,
   - `req.body` is set to the decrypted `payload`,
   - `req.pqAesKey` holds the AES key for optional encrypted responses.

When a controller sees `req.pqAesKey`, it may AES‑GCM encrypt the JSON response and return:

```json
{
  "ciphertext": "<base64>",
  "iv": "<base64>",
  "tag": "<base64>"
}
```

AES‑GCM always operates over the JSON response body (`ciphertext || tag`) with the IV provided.

---

## 2. Configuration and deployment

### 2.1 Config file

The node API reads a JSON config at startup:

- `GATEWAY_CONFIG_FILE` (optional): path to config file,
- default: `config.json` at the root of `gateway-agent/`.

`src/config.js` exposes it as `CONFIG.GATEWAY`. A typical file:

```json
{
  "operator": {
    "address": "lmn1ksjeadh54n72f5792c9nn6xqdjvtp85j6akkkc",
    "chainId": "lumen"
  },
  "region": "eu-de-hetzner",
  "public": "http://91.99.166.223:8787",
  "pricing": [ /* pricing entries */ ]
}
```

`CONFIG.REGION` and `CONFIG.PUBLIC_ENDPOINT` are derived from environment variables and/or this file and are reported by `/status`.

### 2.2 Environment variables

Key environment variables:

- `PORT` (default `8787`): HTTP listen port.
- `REGION` (default `"unknown"`): region label in `/status`.
- `PUBLIC_ENDPOINT` (default `http://localhost:<PORT>`): advertised public URL.
- `CHAIN_REST_BASE_URL` (recommended in prod): base URL of the Lumen chain REST API, used to fetch plans from the gateways module. If omitted, the service falls back to the first `chainSeeds[].rest` entry from `config.json` (and finally to `http://host.docker.internal:1317`).
- `NODE_API_WALLET_DB_PATH`: SQLite path for wallet state (e.g. `/data/node_api/wallets.sqlite` in Docker).
- `LUMEN_GATEWAY_KYBER_KEY_PATH` (required): path to the Kyber key file (`kyber.json` inside the container).
- `KUBO_REQUEST_TIMEOUT_MS` (default `15000`): timeout (ms) for node_api → Kubo requests.
- `KUBO_IMPORT_TIMEOUT_MS` (default `300000`): timeout (ms) for CAR ingest (`/api/v0/dag/import`).
- `INGEST_TMP_DIR` (optional): directory used to spool CAR uploads before background ingest (defaults next to `NODE_API_WALLET_DB_PATH`).

Other optional fields (`CONFIG.GATEWAY.pricing`, `CONFIG.GATEWAY.webhook`, `CONFIG.GATEWAY.operator`, `CONFIG.GATEWAY.chainSeeds`) drive pricing and webhook logic but are not required for a minimal deployment.

### 2.3 Docker layout (production)

The top‑level `gateway-agent/docker-compose.yml` is configured for a host layout like:

- `/opt/lumen/gateway/ipfs_data` – Kubo repo (blocks, pins, config),
- `/opt/lumen/gateway/indexer_data` – indexer SQLite,
- `/opt/lumen/gateway/node_api_data` – node API SQLite,
- `/opt/lumen/gateway/secrets/kyber.json` – Kyber key JSON.

On Ubuntu:

```bash
sudo mkdir -p /opt/lumen/gateway/ipfs_data
sudo mkdir -p /opt/lumen/gateway/indexer_data
sudo mkdir -p /opt/lumen/gateway/node_api_data
sudo mkdir -p /opt/lumen/gateway/secrets
sudo chown -R "$USER":"$USER" /opt/lumen/gateway
```

Generate and move the Kyber secret:

```bash
cd /path/to/gateway-agent/node_api
node scripts/gen_kyber_key.js --write-secret ./kyber.json > /tmp/kyber-meta.json
mv ./kyber.json /opt/lumen/gateway/secrets/kyber.json
```

The relevant parts of `gateway-agent/docker-compose.yml`:

```yaml
services:
  ipfs:
    image: ${KUBO_IMAGE:-ipfs/kubo@sha256:8bc99f94ea109f27ac408354e92b74f4f6f804fe0b4576f424520a20475dc88e}
    restart: unless-stopped
    environment:
      IPFS_PROFILE: server
      IPFS_PATH: /data/ipfs
    volumes:
      - /opt/lumen/gateway/ipfs_data:/data/ipfs
      - ipfs_export:/export
    ports:
      - "${KUBO_API_BIND:-127.0.0.1}:5001:5001"      # IPFS API (do NOT expose on the public internet)
      - "${KUBO_GATEWAY_BIND:-127.0.0.1}:18080:8080" # IPFS gateway (optional public)

  indexer:
    build:
      context: .
      dockerfile: ./indexer/Dockerfile
    restart: unless-stopped
    environment:
      - KUBO_API_BASE=http://ipfs:5001
      - IPFS_GATEWAY_BASE=http://ipfs:8080
      - INDEXER_DB_PATH=/data/indexer.sqlite
      - INDEXER_PORT=8790
      - PIN_LS_REFRESH_SECONDS=30
    volumes:
      - /opt/lumen/gateway/indexer_data:/data
    depends_on:
      - ipfs
    ports:
      - "8790:8790"

  node_api:
    build:
      context: ./node_api
    restart: unless-stopped
    environment:
      - NODE_API_WALLET_DB_PATH=/data/node_api/wallets.sqlite
      - CHAIN_REST_BASE_URL=http://142.132.201.187:1317
      - LUMEN_GATEWAY_KYBER_KEY_PATH=/secrets/kyber.json
    volumes:
      - ./config.json:/app/config.json:ro
      - /opt/lumen/gateway/ipfs_data:/data/ipfs:ro
      - /opt/lumen/gateway/node_api_data:/data/node_api
      - /opt/lumen/gateway/secrets/kyber.json:/secrets/kyber.json:ro
    depends_on:
      - ipfs
    ports:
      - "8787:8787"
```

In production, it is recommended to:

- keep `5001` closed at the host firewall level (IPFS API must not be exposed),
- expose `4001/tcp` and `4001/udp` if you want the node to behave as a full IPFS peer,
- optionally expose `18080/tcp` only if you explicitly want the raw IPFS HTTP gateway public,
- expose `22/tcp` (SSH) and `8787/tcp` (node API).

Example `ufw` setup (gateway API + IPFS P2P, no raw IPFS HTTP):

```bash
sudo ufw allow 22/tcp
sudo ufw allow 8787/tcp
sudo ufw allow 4001/tcp
sudo ufw allow 4001/udp
sudo ufw enable
```

---

## 3. HTTP API overview

### 3.1 Public endpoints

- `GET /health` – container health check:

  ```json
  { "ok": true }
  ```

- `GET /status` – gateway health:

  ```json
  {
    "version": "0.1.2",
    "region": "eu-west",
    "public": "http://<PUBLIC_IP>:8787",
    "ipfs": { "online": true },
    "time": "2025-12-28T02:25:43.580Z"
  }
  ```

- `GET /pq/pub` – Kyber public key + hash (see above).

- `GET /pricing` – pricing table derived from `CONFIG.PRICING` (optional).

- `GET /metrics` – Prometheus metrics for Grafana dashboards.

### 3.2 Authenticated PQ endpoints

All of the following require a valid PQ envelope and wallet signature.

#### `POST /wallet/usage`

Returns plan and usage for the authenticated wallet:

```json
{
  "wallet": "lmn1.......",
  "plan": {
    "id": "basic",
    "expires_at": 1768608900000,
    "quota_bytes_total": 107374182400,
    "quota_bytes_used": 70583782,
    "quota_bytes_remaining": 107303598618
  },
  "plan_source": "chain",
  "usage": {
    "roots_total": 2,
    "roots_active": 2,
    "cids_total": null,
    "cids_truncated": false,
    "cids_walk_ms": 3154,
    "cids_walk_truncated_reason": null,
    "bytes_estimated_total": 70583782,
    "indexer_error": "timeout"
  }
}
```

Internally:

- `wallet_roots` and `wallets` (SQLite) provide cached roots and bytes,
- `ensureWalletPlanOk` queries the chain gateways module for the authoritative plan,
- the indexer is queried to count CIDs with a node cap and time budget.

If `req.pqAesKey` is present, the response is AES‑GCM encrypted instead of returned in clear‑text.

#### `POST /wallet/cids`

Paginates CIDs pinned for the authenticated wallet. Payload:

```json
{ "page": 1 }
```

Response:

```json
{
  "wallet": "lmn1...",
  "page": 1,
  "page_size": 200,
  "cids": [
    "Qm...",
    "Qm..."
  ],
  "items": [
    { "cid": "Qm...", "created_at": 1768608900000, "display_name": "my-file.mp4" },
    { "cid": "Qm...", "created_at": 1768608800000, "display_name": null }
  ],
  "names": {
    "Qm...": "my-file.mp4"
  },
  "has_more": true
}
```

Used by the electron client to inspect pinned CIDs without spamming `/ispinned` per entry.

#### `POST /wallet/cid/rename`

Sets (or clears) the per-wallet display name for a CID. Payload:

```json
{ "cid": "Qm...", "displayName": "movie.mp4" }
```

`displayName` may be sent as `display_name` or `name`. Sending an empty string clears the stored name.

Response:

```json
{ "ok": true, "wallet": "lmn1...", "cid": "Qm...", "display_name": "movie.mp4" }
```

#### `POST /pin` / `POST /unpin` / `POST /ispinned`

- `POST /pin` – starts the pin flow for a CID on this gateway (control plane only; data plane uses `/ingest/*`). Validates the wallet’s plan before proceeding.
- `POST /unpin` – removes the logical pin for this wallet, and if this wallet is the last reference, attempts to unpin from Kubo.
- `POST /ispinned` – returns whether the given CID is pinned and billed for this wallet (per‑wallet view; the fact that Kubo may have the CID pinned for other wallets is not leaked).

#### `POST /ingest/ready` / `POST /ingest/init` / `POST /ingest/car`

The ingest flow is split in three:

1. `POST /ingest/ready` – PQ‑authenticated readiness check; returns `{ ok, wallet, status }`.
2. `POST /ingest/init` – PQ‑authenticated intent:

   ```json
   {
     "ok": true,
     "upload_token": "hex...",
     "planId": "basic",
     "wallet": "lmn1..."
   }
   ```

   The request payload may include an optional `displayName` (or `display_name` / `name` / `filename` / `fileName`).
   If present, it is saved as the per-wallet display name for each ingested root CID.

3. `POST /ingest/car?token=<upload_token>&planId=basic` – streams the CAR body to Kubo via a background job with a random delay and records roots + approximate bytes in `wallet_roots`.

The CAR request itself is not PQ‑encrypted; only the control‑plane steps (`/ingest/ready`, `/ingest/init`) are.

---

## 4. Wallet state and local database

The node API maintains a small SQLite database for wallet‑scoped state:

- `wallets` – one row per wallet:
  - `wallet` (primary key, e.g. `lmn1...`)
  - `plan_id` (opaque ID cached from chain)
  - `plan_expires_at` (expiry timestamp ms)
  - `last_chain_check_at` (timestamp ms)

- `wallet_roots` – mapping between wallets and root CIDs:
  - `wallet`
  - `root_cid`
  - `created_at` (timestamp ms)
  - `bytes_estimated` (rough byte estimate from ingest)
  - `status` (`active` / `removed`)

- `wallet_pins` – mapping between wallets and pinned CIDs (explicit `/pin`):
  - `wallet`
  - `cid`
  - `created_at` (timestamp ms)

- `wallet_cid_metadata` – per-wallet metadata (display name):
  - `wallet`
  - `cid`
  - `display_name`
  - `created_at` / `updated_at` (timestamps ms)

`NODE_API_WALLET_DB_PATH` controls where this file lives; in Docker it is `/data/node_api/wallets.sqlite` mapped to `/opt/lumen/gateway/node_api_data`.

`/wallet/usage` aggregates these tables with chain and indexer data to provide an accurate, auditable view of storage usage per wallet.

---

## 5. Responsibility separation

- **node_api**:
  - PQ‑encrypted wallet control plane (pin/unpin/ingest/usage),
  - wallet authentication and signature verification,
  - plan/quota checks against the chain,
  - local cache of wallet roots and bytes,
  - Prometheus metrics and basic `/status`/`/pricing`/`/search` endpoints.

- **indexer**:
  - Observation of Kubo pins and DAG edges via the IPFS API,
  - type detection
  - text/image tagging and search signals,
  - Prometheus `/metrics` for Grafana dashboards.

This split keeps the node API minimal and focused, while the indexer handles all content and DAG‑level work.
