import crypto from 'node:crypto';
import { Secp256k1, Secp256k1Signature, Sha256, Ripemd160 } from '@cosmjs/crypto';
import { toBech32 } from '@cosmjs/encoding';

const REPLAY_WINDOW_SEC = 300; // 5 minutes
const NONCE_TTL_MS = 10 * 60 * 1000; // 10 minutes

const nonces = new Map(); // nonce -> ts

function decodeBytes(value) {
  if (value == null) return null;
  if (value instanceof Uint8Array) return value;
  const str = String(value).trim();
  if (!str) return null;
  const hexCandidate = str.startsWith('0x') ? str.slice(2) : str;
  if (/^[0-9a-fA-F]+$/.test(hexCandidate) && hexCandidate.length % 2 === 0) {
    try {
      const buf = Buffer.from(hexCandidate, 'hex');
      if (buf.length > 0) return new Uint8Array(buf);
    } catch {
      /* ignore */
    }
  }
  try {
    const buf = Buffer.from(str, 'base64');
    if (buf.length > 0) return new Uint8Array(buf);
  } catch {
    /* ignore */
  }
  return null;
}

function sha256Bytes(data) {
  return new Sha256(new TextEncoder().encode(String(data ?? ''))).digest();
}

function pubkeyToAddress(pubkeyBytes, prefix) {
  const compressed = pubkeyBytes.length === 33 ? pubkeyBytes : Secp256k1.compressPubkey(pubkeyBytes);
  const sha = new Sha256(compressed).digest();
  const rip = new Ripemd160(sha).digest();
  return toBech32(prefix, rip);
}

function resolvePrefix(addr) {
  if (typeof addr !== 'string') return 'lmn';
  const i = addr.indexOf('1');
  if (i > 0) return addr.slice(0, i);
  return 'lmn';
}

function parseSignatureInput(sigInput) {
  if (sigInput == null) return { signature: null, pubkey: null };
  if (typeof sigInput === 'string') {
    const trimmed = sigInput.trim();
    if (!trimmed) return { signature: null, pubkey: null };
    if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
      try {
        const parsed = JSON.parse(trimmed);
        if (parsed && typeof parsed === 'object') {
          return {
            signature: parsed.signature || parsed.sig || parsed.signatureB64 || parsed.signatureHex || parsed.value,
            pubkey: parsed.pubkey || parsed.pubKey || parsed.pubkeyB64 || parsed.pubkeyHex || parsed.publicKey || parsed.pub
          };
        }
      } catch {
        /* ignore */
      }
    }
    return { signature: trimmed, pubkey: null };
  }
  if (typeof sigInput === 'object') {
    return {
      signature: sigInput.signature || sigInput.sig || sigInput.signatureB64 || sigInput.signatureHex || sigInput.value,
      pubkey: sigInput.pubkey || sigInput.pubKey || sigInput.pubkeyB64 || sigInput.pubkeyHex || sigInput.publicKey || sigInput.pub
    };
  }
  return { signature: null, pubkey: null };
}

async function verifySecp256k1(addr, payload, sigInput) {
  try {
    const address = String(addr || '').trim();
    if (!address) return false;
    const { signature, pubkey } = parseSignatureInput(sigInput);
    const signatureBytes = decodeBytes(signature);
    if (!signatureBytes) return false;
    let pubkeyBytes = decodeBytes(pubkey);
    if (!pubkeyBytes && typeof sigInput === 'object' && sigInput && Array.isArray(sigInput.pubkeyBytes)) {
      try {
        pubkeyBytes = new Uint8Array(sigInput.pubkeyBytes);
      } catch {
        pubkeyBytes = null;
      }
    }
    if (!pubkeyBytes) return false;

    const digest = sha256Bytes(payload ?? '');
    let sigObj = null;
    try {
      if (signatureBytes.length === 64) sigObj = Secp256k1Signature.fromFixedLength(signatureBytes);
    } catch {}
    if (!sigObj && signatureBytes.length === 65) {
      try {
        const trimmed = signatureBytes.slice(0, 64);
        sigObj = Secp256k1Signature.fromFixedLength(trimmed);
      } catch {}
    }
    if (!sigObj) {
      try {
        sigObj = Secp256k1Signature.fromDer(signatureBytes);
      } catch {}
    }
    if (!sigObj) return false;

    const pubUncompressed =
      pubkeyBytes.length === 33 ? Secp256k1.uncompressPubkey(pubkeyBytes) : pubkeyBytes;
    const validSig = await Secp256k1.verifySignature(sigObj, digest, pubUncompressed);
    if (!validSig) return false;

    const prefix = resolvePrefix(address);
    const derivedAddr = pubkeyToAddress(pubkeyBytes, prefix);
    return derivedAddr === address;
  } catch (err) {
    console.warn('[auth] verifySecp256k1 error', err);
    return false;
  }
}

function purgeNonces(nowMs) {
  for (const [nonce, ts] of nonces.entries()) {
    if (nowMs - Number(ts || 0) > NONCE_TTL_MS) {
      nonces.delete(nonce);
    }
  }
}

export async function verifyRequestSignature(req) {
  const addr = req.header('X-Lumen-Addr')?.trim();
  const sig = req.header('X-Lumen-Sig')?.trim();
  const pubkey = req.header('X-Lumen-PubKey')?.trim();
  const nonce = req.header('X-Lumen-Nonce')?.trim();
  const tsRaw = req.header('X-Lumen-Ts')?.trim();

  if (!addr || !sig || !nonce || !tsRaw) {
    return { ok: false, status: 401, error: 'auth_failed', message: 'missing headers' };
  }

  const ts = Number(tsRaw);
  if (!Number.isFinite(ts)) {
    return { ok: false, status: 401, error: 'auth_failed', message: 'invalid timestamp' };
  }

  const now = Date.now();
  const windowMs = Math.max(1, REPLAY_WINDOW_SEC) * 1000;
  if (Math.abs(now - ts) > windowMs) {
    return { ok: false, status: 401, error: 'auth_failed', message: 'timestamp_out_of_window' };
  }

  purgeNonces(now);
  if (nonces.has(nonce)) {
    return { ok: false, status: 401, error: 'auth_failed', message: 'nonce_replay' };
  }

  const payload = `${req.method}|${req.path}|${nonce}|${tsRaw}|${req.hash || ''}`;
  const ok = await verifySecp256k1(addr, payload, { signature: sig, pubkey });
  if (!ok) {
    return { ok: false, status: 401, error: 'auth_failed', message: 'invalid_signature' };
  }

  nonces.set(nonce, ts);
  return { ok: true, wallet: addr };
}

export async function verifyWalletCanonicalSignature(addr, canonicalPayload, signatureB64, pubkeyB64) {
  const address = String(addr || '').trim();
  if (!address) return false;
  const sig = String(signatureB64 || '').trim();
  const pk = String(pubkeyB64 || '').trim();
  if (!sig || !pk) return false;
  return verifySecp256k1(address, canonicalPayload, { signature: sig, pubkey: pk });
}
