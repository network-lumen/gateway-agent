const registry = new Map();

export function registerWalletHit(wallet) {
  const now = Date.now();
  const existing = registry.get(wallet);
  if (existing) {
    existing.lastSeenAt = now;
    return existing;
  }
  const record = {
    wallet,
    contractRef: null,
    contractDigest: null,
    createdAt: now,
    lastSeenAt: now,
    pinCount: 0,
    unpinCount: 0,
    ingestBytes: 0
  };
  registry.set(wallet, record);
  return record;
}

export function getWalletRecord(wallet) {
  return registry.get(wallet) || null;
}

export function recordPin(wallet) {
  const rec = registerWalletHit(wallet);
  rec.pinCount += 1;
}

export function recordUnpin(wallet) {
  const rec = registerWalletHit(wallet);
  rec.unpinCount += 1;
}

export function recordIngest(wallet, bytes) {
  const rec = registerWalletHit(wallet);
  const val = Number(bytes) || 0;
  if (val > 0) rec.ingestBytes += val;
  // Treat a successful ingest as a logical "pin" operation for metrics.
  rec.pinCount += 1;
}

export function getAllWalletRecords() {
  return Array.from(registry.values());
}

export function getRegistryStats() {
  let walletCount = 0;
  let pinCount = 0;
  let unpinCount = 0;
  let ingestBytes = 0;
  for (const rec of registry.values()) {
    walletCount += 1;
    pinCount += rec.pinCount || 0;
    unpinCount += rec.unpinCount || 0;
    ingestBytes += rec.ingestBytes || 0;
  }
  return { walletCount, pinCount, unpinCount, ingestBytes };
}
