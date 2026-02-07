# Operator guide: manage your gateway in `contributor/browser` (`lumen://gateways`)

Gateway operators can register and update their gateway **on-chain** directly from the Contributor Browser UI at `lumen://gateways`.

This document explains how to connect that UI workflow with a running `gateway-agent` deployment.

---

## Prerequisites

- A running `gateway-agent` stack (see `gateway-agent/README.md`).
- `contributor/browser` installed and running.
- An **active profile** in the browser with the **operator wallet** (not a guest profile).
- Enough LMN to pay the on-chain fees shown in the UI (“Register fee” / “Update fee”).
- Your gateway node API is reachable from the browser:
  - `GET /status` works on your public endpoint (for example `http://<ip>:8787/status`).

---

## 1) Generate Kyber keys (required for PQ endpoints)

`gateway-agent` uses a Kyber768 keypair to secure wallet-authenticated control-plane requests (PQ transport).

Generate the keypair and keep the **private key file** on the server:

```bash
cd gateway-agent/node_api
node scripts/gen_kyber_key.js --write-secret ./kyber.json > /tmp/kyber-meta.json
```

- Move `./kyber.json` to your secrets directory (example: `/opt/lumen/gateway/secrets/kyber.json`).
- The on-chain metadata you must publish is in `/tmp/kyber-meta.json`:
  - copy the JSON fragment `gateway_metadata.crypto.kyber`
  - **never** publish `gateway_local_secret` on-chain.

---

## 2) Register your gateway in `lumen://gateways`

In Contributor Browser:

1. Open `lumen://gateways`
2. Select the profile that owns your operator wallet
3. Click **Create gateway**
4. Fill the form:
   - **Endpoint**: a hostname like `gateway.city` (no scheme, no path)
   - **Regions**: comma/space-separated (example: `eu-west, us-east`)
   - **Payout address**: where rewards should be sent (often your operator wallet)
   - **Metadata (JSON object)**: paste a JSON object that includes the Kyber public key hash

Minimal recommended metadata:

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

5. Click **Create** and confirm the transaction.

After the transaction is included, the gateway appears in “My gateways”.

---

## 3) Update gateway settings

On an existing gateway card, you can change:

- endpoint / regions / payout address
- metadata JSON
- active/inactive status

Click **Save changes** → it submits an on-chain update transaction.

---

## 4) Make sure the on-chain gateway points to your running node API

The gateway record “Endpoint” should resolve (via the browser logic) to the base URL that serves the node API.

Operational check (from any machine that can reach your gateway):

```bash
curl -s http://<your-gateway-base>:8787/status | jq .
curl -s http://<your-gateway-base>:8787/pq/pub | jq .
```

If the browser refuses to use your gateway for PQ calls, re-check that:

- the Kyber `pubkey_hash` in metadata matches the hash of `GET /pq/pub`,
- the gateway is marked **Active** on-chain,
- the browser profile wallet matches the on-chain operator when managing the gateway.

---

## Troubleshooting

### `CHAIN_UNREACHABLE` during `/ingest/init` or `/wallet/usage`

Your node API cannot reach the chain REST endpoint used to fetch gateway plans/contracts.

- Set `CHAIN_REST_BASE_URL` in your `.env` (recommended), for example:

```env
CHAIN_REST_BASE_URL=https://lumen-api.linknode.org
```

Then recreate the `node_api` container.

### “Metadata must be a JSON object”

The UI only accepts a JSON **object** at the top level (not an array, string, etc.). Paste exactly one object.

