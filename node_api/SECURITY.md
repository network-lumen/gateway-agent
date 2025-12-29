## Lumen Gateway Node API - Security invariants

- All `authWallet`-protected HTTP routes must be called through the PQ helper on the client and are post-quantum encrypted (Kyber768 + AES-256-GCM).
- The gateway Kyber public key used for encryption is authenticated via on-chain metadata: the metadata stores a SHA-256 hash (`pubkey_hash`) of the public key, and the full key is served by `/pq/pub`; clients verify that `SHA-256(pub) == pubkey_hash`.
- Request payloads are canonically serialized and wallet-signed before encryption; the gateway verifies the signature after decryption.
- The AES-GCM wire format is `{ kem_ct, ciphertext, iv, tag }`; the backend must reconstruct `ciphertext || tag` before attempting decryption.
- IPFS ingest is decoupled from authWallet timing: control-plane calls enqueue ingest jobs, which execute after a random 100-5000 ms delay to reduce wallet↔CID timing correlation.

