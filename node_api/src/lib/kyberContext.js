import { readFile } from 'node:fs/promises';
import process from 'node:process';

let kyberContext = null;

function fatal(msg) {
  // eslint-disable-next-line no-console
  console.error(`FATAL: ${msg}`);
  process.exit(1);
}

function decodeBase64ToUint8(str, fieldName) {
  try {
    const clean = String(str || '').trim();
    if (!clean) fatal(`invalid kyber key file (missing ${fieldName})`);
    const buf = Buffer.from(clean, 'base64');
    if (!buf.length) {
      fatal(`invalid kyber key file (bad base64 in ${fieldName})`);
    }
    return new Uint8Array(buf);
  } catch {
    fatal(`invalid kyber key file (bad base64 in ${fieldName})`);
  }
}

export async function initKyberContext() {
  if (kyberContext) return kyberContext;

  const keyPath = process.env.LUMEN_GATEWAY_KYBER_KEY_PATH;
  if (!keyPath) {
    fatal('LUMEN_GATEWAY_KYBER_KEY_PATH is not set');
  }

  let raw;
  try {
    raw = await readFile(keyPath, 'utf8');
  } catch {
    fatal(`invalid kyber key file path: ${keyPath}`);
  }

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    fatal('invalid kyber key file (not valid JSON)');
  }

  const alg = String(parsed.alg || '').trim();
  const keyId = String(parsed.key_id || '').trim();
  const pubkeyB64 = parsed.pubkey;
  const privkeyB64 = parsed.privkey;

  if (alg !== 'kyber768') {
    fatal('invalid kyber key file (alg must be "kyber768")');
  }
  if (!keyId) {
    fatal('invalid kyber key file (missing key_id)');
  }
  if (!pubkeyB64) {
    fatal('invalid kyber key file (missing pubkey)');
  }
  if (!privkeyB64) {
    fatal('invalid kyber key file (missing privkey)');
  }

  const pubKeyBytes = decodeBase64ToUint8(pubkeyB64, 'pubkey');
  const privKeyBytes = decodeBase64ToUint8(privkeyB64, 'privkey');

  kyberContext = {
    alg,
    keyId,
    pubKeyBytes,
    privKeyBytes
  };

  return kyberContext;
}

export function getKyberContext() {
  if (!kyberContext) {
    throw new Error('kyber_context_not_initialized');
  }
  return kyberContext;
}

