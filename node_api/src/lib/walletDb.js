import fs from 'node:fs';
import path from 'node:path';
import sqlite3 from 'sqlite3';

const DEFAULT_DB_PATH = '/data/node_api/wallets.sqlite';

const DB_PATH =
  process.env.NODE_API_WALLET_DB_PATH && process.env.NODE_API_WALLET_DB_PATH.trim()
    ? process.env.NODE_API_WALLET_DB_PATH.trim()
    : DEFAULT_DB_PATH;

let dbInstance = null;
let initPromise = null;
let schemaEnsured = false;

function openDb() {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(DB_PATH, (err) => {
      if (err) reject(err);
      else resolve(db);
    });
  });
}

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows || []);
    });
  });
}

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row || null);
    });
  });
}

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

const SCHEMA_SQL = `
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS wallets (
  wallet TEXT PRIMARY KEY,
  plan_id TEXT,
  plan_expires_at INTEGER,
  last_chain_check_at INTEGER
);

CREATE TABLE IF NOT EXISTS wallet_roots (
  wallet TEXT NOT NULL,
  root_cid TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  bytes_estimated INTEGER,
  status TEXT NOT NULL DEFAULT 'active',
  PRIMARY KEY (wallet, root_cid)
);

CREATE INDEX IF NOT EXISTS idx_wallet_roots_wallet
  ON wallet_roots(wallet);
CREATE INDEX IF NOT EXISTS idx_wallet_roots_status
  ON wallet_roots(status);

CREATE TABLE IF NOT EXISTS wallet_pins (
  wallet TEXT NOT NULL,
  cid TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (wallet, cid)
);

CREATE INDEX IF NOT EXISTS idx_wallet_pins_wallet
  ON wallet_pins(wallet);
CREATE INDEX IF NOT EXISTS idx_wallet_pins_cid
  ON wallet_pins(cid);
`;

async function ensureSchema(db) {
  if (schemaEnsured) return;
  await exec(db, SCHEMA_SQL);
  schemaEnsured = true;
}

async function initDb() {
  if (dbInstance) return dbInstance;
  if (initPromise) return initPromise;

  const dir = path.dirname(DB_PATH);
  try {
    fs.mkdirSync(dir, { recursive: true });
  } catch (err) {
    // If directory creation fails, surface the error when opening DB.
    // eslint-disable-next-line no-console
    console.error('[walletDb] failed to create db dir', { dir, error: String(err?.message || err) });
  }

  initPromise = (async () => {
    const db = await openDb();
    await ensureSchema(db);
    dbInstance = db;
    return dbInstance;
  })();

  return initPromise;
}

async function getDb() {
  if (dbInstance) return dbInstance;
  return initDb();
}

async function runInTransaction(work) {
  const db = await getDb();
  await run(db, 'BEGIN IMMEDIATE TRANSACTION');
  try {
    await work(db);
    await run(db, 'COMMIT');
  } catch (err) {
    try {
      await run(db, 'ROLLBACK');
    } catch {
      // ignore rollback errors
    }
    throw err;
  }
}

export async function upsertWalletRecord({ wallet, planId }) {
  const db = await getDb();
  const normalizedPlanId =
    typeof planId === 'string' && planId.trim() ? planId.trim() : null;

  await run(
    db,
    `
    INSERT INTO wallets (wallet, plan_id, plan_expires_at, last_chain_check_at)
    VALUES (?, ?, NULL, NULL)
    ON CONFLICT(wallet) DO UPDATE SET
      plan_id = COALESCE(excluded.plan_id, wallets.plan_id)
  `,
    [wallet, normalizedPlanId]
  );
}

export async function touchWalletChainCheck(wallet, timestampMs) {
  const db = await getDb();
  const ts =
    typeof timestampMs === 'number' && Number.isFinite(timestampMs)
      ? timestampMs
      : Date.now();
  await run(
    db,
    `
    UPDATE wallets
    SET last_chain_check_at = ?
    WHERE wallet = ?
  `,
    [ts, wallet]
  );
}

export async function updateWalletPlanFromChain({
  wallet,
  planId,
  planExpiresAt,
  chainCheckAt
}) {
  const db = await getDb();
  const normalizedPlanId =
    typeof planId === 'string' && planId.trim() ? planId.trim() : null;
  const expires =
    typeof planExpiresAt === 'number' && Number.isFinite(planExpiresAt)
      ? planExpiresAt
      : null;
  const lastCheck =
    typeof chainCheckAt === 'number' && Number.isFinite(chainCheckAt)
      ? chainCheckAt
      : Date.now();

  await run(
    db,
    `
    UPDATE wallets
    SET
      plan_id = COALESCE(?, plan_id),
      plan_expires_at = ?,
      last_chain_check_at = ?
    WHERE wallet = ?
  `,
    [normalizedPlanId, expires, lastCheck, wallet]
  );
}

export async function addOrUpdateWalletRoots({
  wallet,
  roots,
  bytesEstimated
}) {
  if (!Array.isArray(roots) || roots.length === 0) return;
  const now = Date.now();
  const est =
    typeof bytesEstimated === 'number' && Number.isFinite(bytesEstimated)
      ? bytesEstimated
      : null;

  const perRootBytes =
    est && est > 0 ? Math.floor(est / roots.length) : null;

  await runInTransaction(async (db) => {
    for (const rootCid of roots) {
      if (!rootCid) continue;
      // One insert/update per root; all succeed or all rollback.
      // eslint-disable-next-line no-await-in-loop
      await run(
        db,
        `
        INSERT INTO wallet_roots (
          wallet, root_cid, created_at, bytes_estimated, status
        )
        VALUES (?, ?, ?, ?, 'active')
        ON CONFLICT(wallet, root_cid) DO UPDATE SET
          status = 'active',
          created_at = MIN(wallet_roots.created_at, excluded.created_at),
          bytes_estimated = COALESCE(excluded.bytes_estimated, wallet_roots.bytes_estimated)
      `,
        [wallet, rootCid, now, perRootBytes]
      );
    }
  });
}

export async function getWalletRow(wallet) {
  const db = await getDb();
  return get(
    db,
    `
    SELECT wallet, plan_id, plan_expires_at, last_chain_check_at
    FROM wallets
    WHERE wallet = ?
  `,
    [wallet]
  );
}

export async function getWalletRoots(wallet) {
  const db = await getDb();
  return all(
    db,
    `
    SELECT wallet, root_cid, created_at, bytes_estimated, status
    FROM wallet_roots
    WHERE wallet = ?
    ORDER BY created_at DESC, root_cid ASC
  `,
    [wallet]
  );
}

export async function getWalletRootsSummary(wallet) {
  const db = await getDb();
  const rows = await all(
    db,
    `
    SELECT
      COUNT(*) AS total_roots,
      SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_roots,
      SUM(bytes_estimated) AS bytes_total_estimated
    FROM wallet_roots
    WHERE wallet = ?
  `,
    [wallet]
  );
  const row = rows[0] || {};
  return {
    totalRoots:
      typeof row.total_roots === 'number' ? row.total_roots : 0,
    activeRoots:
      typeof row.active_roots === 'number' ? row.active_roots : 0,
    bytesEstimated:
      typeof row.bytes_total_estimated === 'number'
        ? row.bytes_total_estimated
        : null
  };
}

export async function getWalletsForRootCid(rootCid) {
  const cid = String(rootCid || '').trim();
  if (!cid) return [];
  const db = await getDb();
  const rows = await all(
    db,
    `
    SELECT wallet
    FROM wallet_roots
    WHERE root_cid = ?
      AND status = 'active'
    ORDER BY wallet ASC
  `,
    [cid]
  );
  return rows.map((r) => String(r.wallet || '').trim()).filter(Boolean);
}

export async function hasWalletRoot(wallet, rootCid) {
  const w = String(wallet || '').trim();
  const c = String(rootCid || '').trim();
  if (!w || !c) return false;
  const db = await getDb();
  const row = await get(
    db,
    `
    SELECT 1 AS ok
    FROM wallet_roots
    WHERE wallet = ? AND root_cid = ? AND status = 'active'
    LIMIT 1
  `,
    [w, c]
  );
  return !!row;
}

export async function getWalletPinnedCidsPage(wallet, { limit, offset } = {}) {
  const w = String(wallet || '').trim();
  if (!w) return [];

  const db = await getDb();
  const safeLimit =
    Number.isFinite(limit) && limit > 0 ? Math.min(Number(limit), 1000) : 201;
  const safeOffset = Number.isFinite(offset) && offset >= 0 ? Number(offset) : 0;

  return all(
    db,
    `
    SELECT cid, created_at FROM (
      SELECT root_cid AS cid, created_at
      FROM wallet_roots
      WHERE wallet = ? AND status = 'active'
      UNION
      SELECT cid AS cid, created_at
      FROM wallet_pins
      WHERE wallet = ?
    )
    ORDER BY created_at DESC, cid ASC
    LIMIT ? OFFSET ?
  `,
    [w, w, safeLimit, safeOffset]
  );
}

export async function removeWalletRoot(wallet, rootCid) {
  const w = String(wallet || '').trim();
  const c = String(rootCid || '').trim();
  if (!w || !c) return;
  const db = await getDb();
  await run(
    db,
    `
    DELETE FROM wallet_roots
    WHERE wallet = ? AND root_cid = ?
  `,
    [w, c]
  );
}

export async function addWalletPin(wallet, cid) {
  const w = String(wallet || '').trim();
  const c = String(cid || '').trim();
  if (!w || !c) return;
  const db = await getDb();
  const now = Date.now();
  await run(
    db,
    `
    INSERT INTO wallet_pins (wallet, cid, created_at)
    VALUES (?, ?, ?)
    ON CONFLICT(wallet, cid) DO UPDATE SET
      created_at = MIN(wallet_pins.created_at, excluded.created_at)
  `,
    [w, c, now]
  );
}

export async function removeWalletPin(wallet, cid) {
  const w = String(wallet || '').trim();
  const c = String(cid || '').trim();
  if (!w || !c) return;
  const db = await getDb();
  await run(
    db,
    `
    DELETE FROM wallet_pins
    WHERE wallet = ? AND cid = ?
  `,
    [w, c]
  );
}

export async function hasWalletPin(wallet, cid) {
  const w = String(wallet || '').trim();
  const c = String(cid || '').trim();
  if (!w || !c) return false;
  const db = await getDb();
  const row = await get(
    db,
    `
    SELECT 1 AS ok
    FROM wallet_pins
    WHERE wallet = ? AND cid = ?
    LIMIT 1
  `,
    [w, c]
  );
  return !!row;
}

export async function countWalletPinsForCid(cid) {
  const c = String(cid || '').trim();
  if (!c) return 0;
  const db = await getDb();
  const row = await get(
    db,
    `
    SELECT COUNT(*) AS n
    FROM wallet_pins
    WHERE cid = ?
  `,
    [c]
  );
  const n =
    row && typeof row.n === 'number' && Number.isFinite(row.n) && row.n >= 0
      ? row.n
      : 0;
  return n;
}
