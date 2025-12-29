import { ml_kem768 } from '@noble/post-quantum/ml-kem.js';
import crypto from 'node:crypto';
import { getKyberContext } from '../lib/kyberContext.js';
import { verifyWalletCanonicalSignature } from '../lib/authSig.js';

const REPLAY_WINDOW_MS = 5 * 60 * 1000; // 5 minutes
const NONCE_TTL_MS = 10 * 60 * 1000; // 10 minutes

const nonces = new Map(); // nonce -> ts

function purgeNonces(nowMs) {
  for (const [nonce, ts] of nonces.entries()) {
    if (nowMs - Number(ts || 0) > NONCE_TTL_MS) {
      nonces.delete(nonce);
    }
  }
}

function decodeBase64(str, fieldName) {
  try {
    const clean = String(str || '').trim();
    if (!clean) {
      return { ok: false, error: `pq_invalid_${fieldName}`, message: `${fieldName} is empty` };
    }
    const buf = Buffer.from(clean, 'base64');
    if (!buf.length) {
      return { ok: false, error: `pq_invalid_${fieldName}`, message: `${fieldName} is not valid base64` };
    }
    return { ok: true, value: buf };
  } catch {
    return { ok: false, error: `pq_invalid_${fieldName}`, message: `${fieldName} is not valid base64` };
  }
}

function deriveAesKey(sharedSecret) {
  // Derive AES-256 key using HKDF-SHA256 with empty salt and a fixed info string.
  return crypto.hkdfSync(
    'sha256',
    Buffer.alloc(0),
    Buffer.from(sharedSecret),
    Buffer.from('lumen-authwallet-v1'),
    32
  );
}

export async function decryptPqRequest(req) {
  let ctx;
  try {
    ctx = getKyberContext();
  } catch {
    return {
      ok: false,
      status: 500,
      error: 'pq_unavailable',
      message: 'kyber_context_not_initialized'
    };
  }

  const kemHeader = String(req.header('X-Lumen-KEM') || '').trim().toLowerCase();
  const keyIdHeader =
    String(req.header('X-Lumen-KeyId') || req.header('X-Lumen-KeyID') || '').trim();

  if (kemHeader !== 'kyber768') {
    return {
      ok: false,
      status: 400,
      error: 'pq_unsupported_kem',
      message: 'unsupported_kem'
    };
  }

  if (keyIdHeader && keyIdHeader !== ctx.keyId) {
    return {
      ok: false,
      status: 400,
      error: 'pq_key_mismatch',
      message: 'pq_key_mismatch'
    };
  }

  if (!req.body || typeof req.body !== 'object') {
    return {
      ok: false,
      status: 400,
      error: 'pq_bad_body',
      message: 'pq_body_required'
    };
  }

  const { kem_ct, ciphertext, iv, tag } = req.body;

  if (
    typeof kem_ct !== 'string' ||
    typeof ciphertext !== 'string' ||
    typeof iv !== 'string' ||
    typeof tag !== 'string'
  ) {
    return {
      ok: false,
      status: 400,
      error: 'pq_bad_body',
      message: 'pq_body_fields_invalid'
    };
  }

  const kemCtDecoded = decodeBase64(kem_ct, 'kem_ct');
  if (!kemCtDecoded.ok) {
    return { ok: false, status: 400, error: kemCtDecoded.error, message: kemCtDecoded.message };
  }
  const ctDecoded = decodeBase64(ciphertext, 'ciphertext');
  if (!ctDecoded.ok) {
    return { ok: false, status: 400, error: ctDecoded.error, message: ctDecoded.message };
  }
  const ivDecoded = decodeBase64(iv, 'iv');
  if (!ivDecoded.ok) {
    return { ok: false, status: 400, error: ivDecoded.error, message: ivDecoded.message };
  }
  const tagDecoded = decodeBase64(tag, 'tag');
  if (!tagDecoded.ok) {
    return { ok: false, status: 400, error: tagDecoded.error, message: tagDecoded.message };
  }

  const kemCtBytes = new Uint8Array(kemCtDecoded.value);
  const ctBytes = ctDecoded.value;
  const ivBytes = ivDecoded.value;
  const tagBytes = tagDecoded.value;

  let sharedSecret;
  try {
    sharedSecret = ml_kem768.decapsulate(kemCtBytes, ctx.privKeyBytes);
  } catch {
    return {
      ok: false,
      status: 400,
      error: 'pq_decapsulate_failed',
      message: 'invalid_kem_ciphertext'
    };
  }

  const aesKey = deriveAesKey(sharedSecret);

  let plaintext;
  try {
    const decipher = crypto.createDecipheriv('aes-256-gcm', aesKey, ivBytes);
    decipher.setAuthTag(tagBytes);
    plaintext = Buffer.concat([decipher.update(ctBytes), decipher.final()]);
  } catch {
    return {
      ok: false,
      status: 400,
      error: 'pq_decrypt_failed',
      message: 'invalid_ciphertext'
    };
  }

  let envelope;
  try {
    envelope = JSON.parse(plaintext.toString('utf8'));
  } catch {
    return {
      ok: false,
      status: 400,
      error: 'pq_bad_envelope',
      message: 'envelope_not_json'
    };
  }

  const wallet = String(envelope.wallet || '').trim();
  const nonce = String(envelope.nonce || '').trim();
  const tsRaw = envelope.timestamp;
  const signature = String(envelope.signature || '').trim();
  const payload = envelope.payload;

  if (!wallet) {
    return {
      ok: false,
      status: 400,
      error: 'wallet_required',
      message: 'wallet_required'
    };
  }

  if (!/^lmn1[0-9a-z]+$/.test(wallet)) {
    return {
      ok: false,
      status: 400,
      error: 'wallet_invalid',
      message: 'wallet_invalid'
    };
  }

  const ts = Number(tsRaw);
  if (!Number.isFinite(ts)) {
    return {
      ok: false,
      status: 400,
      error: 'auth_failed',
      message: 'invalid_timestamp'
    };
  }

  const now = Date.now();
  if (Math.abs(now - ts) > REPLAY_WINDOW_MS) {
    return {
      ok: false,
      status: 401,
      error: 'auth_failed',
      message: 'timestamp_out_of_window'
    };
  }

  if (!nonce) {
    return {
      ok: false,
      status: 400,
      error: 'auth_failed',
      message: 'nonce_required'
    };
  }

  purgeNonces(now);
  if (nonces.has(nonce)) {
    return {
      ok: false,
      status: 401,
      error: 'auth_failed',
      message: 'nonce_replay'
    };
  }
  nonces.set(nonce, ts);

  if (!signature) {
    return {
      ok: false,
      status: 400,
      error: 'auth_failed',
      message: 'missing_signature'
    };
  }

  // Reconstruct canonical payload hash as the client did
  let payloadHashHex = '';
  try {
    const canonicalPayload = JSON.stringify(payload ?? null);
    const hash = crypto.createHash('sha256').update(canonicalPayload).digest('hex');
    payloadHashHex = hash;
  } catch {
    payloadHashHex = '';
  }

  const pubkey = String(envelope.pubkey || envelope.pubKey || '').trim();
  if (!pubkey) {
    return {
      ok: false,
      status: 400,
      error: 'auth_failed',
      message: 'missing_pubkey'
    };
  }

  const canonicalPayload = `${req.method}|${req.path}|${nonce}|${tsRaw}|${payloadHashHex}`;
  const sigOk = await verifyWalletCanonicalSignature(wallet, canonicalPayload, signature, pubkey);
  if (!sigOk) {
    return {
      ok: false,
      status: 401,
      error: 'auth_failed',
      message: 'invalid_signature'
    };
  }

  return {
    ok: true,
    wallet,
    payload,
    aesKey
  };
}
