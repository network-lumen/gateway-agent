#!/usr/bin/env node
import { ml_kem768 } from '@noble/post-quantum/ml-kem.js';
import { access, writeFile } from 'node:fs/promises';
import { constants as fsConstants } from 'node:fs';
import { createHash, webcrypto, randomBytes as nodeRandomBytes } from 'node:crypto';
import process from 'node:process';

// Ensure crypto.getRandomValues is available for @noble/post-quantum in Node.js
if (
  typeof globalThis.crypto === 'undefined' ||
  typeof globalThis.crypto.getRandomValues !== 'function'
) {
  if (webcrypto && typeof webcrypto.getRandomValues === 'function') {
    // Node >= 18: use built-in WebCrypto
    // eslint-disable-next-line no-global-assign
    globalThis.crypto = webcrypto;
  } else {
    // Fallback: shim getRandomValues from node:crypto.randomBytes
    // eslint-disable-next-line no-global-assign
    globalThis.crypto = {
      getRandomValues(b) {
        const buf = nodeRandomBytes(b.length);
        b.set(buf);
        return b;
      }
    };
  }
}

function parseArgs(argv) {
  const args = argv.slice(2);
  const out = {
    writeSecretPath: null
  };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === '--write-secret') {
      const next = args[i + 1];
      if (!next || next.startsWith('--')) {
        throw new Error('missing_path_for_write_secret');
      }
      out.writeSecretPath = next;
      i += 1;
    }
  }
  return out;
}

async function fileExists(path) {
  try {
    await access(path, fsConstants.F_OK);
    return true;
  } catch {
    return false;
  }
}

function toBase64(bytes) {
  return Buffer.from(bytes).toString('base64');
}

function sha256Base64(bytes) {
  const hash = createHash('sha256');
  hash.update(bytes);
  return hash.digest('base64');
}

function generateKeyId() {
  const now = new Date();
  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, '0');
  return `gw-${year}-${month}`;
}

async function main() {
  const { writeSecretPath } = parseArgs(process.argv);

  const { publicKey, secretKey } = ml_kem768.keygen();
  const keyId = generateKeyId();

  const gatewayMetadata = {
    crypto: {
      kyber: {
        alg: 'kyber768',
        key_id: keyId,
        pubkey_hash: sha256Base64(publicKey)
      }
    }
  };

  const gatewayLocalSecret = {
    alg: 'kyber768',
    key_id: keyId,
    pubkey: toBase64(publicKey),
    privkey: toBase64(secretKey)
  };

  if (writeSecretPath) {
    if (await fileExists(writeSecretPath)) {
      throw new Error('secret_path_already_exists');
    }
    const secretJson = JSON.stringify(gatewayLocalSecret, null, 2);
    await writeFile(writeSecretPath, `${secretJson}\n`, 'utf8');
  }

  const output = {
    gateway_metadata: gatewayMetadata,
    gateway_local_secret: gatewayLocalSecret
  };

  // Single JSON object to stdout, no extra logs
  process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
}

main().catch((err) => {
  // Errors are printed to stderr; stdout remains clean JSON-only on success
  process.stderr.write(
    `${err && err.message ? String(err.message) : 'gen_kyber_key_failed'}\n`
  );
  process.exit(1);
});
