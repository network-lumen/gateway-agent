import { CID } from 'multiformats/cid';
import {
  parseSqliteBusyTimeoutMs,
  createSqliteStore
} from './sqliteUtils.js';

const DEFAULT_DB_PATH = '/data/node_api/wallets.sqlite';

const DB_PATH =
  process.env.NODE_API_WALLET_DB_PATH && process.env.NODE_API_WALLET_DB_PATH.trim()
    ? process.env.NODE_API_WALLET_DB_PATH.trim()
    : DEFAULT_DB_PATH;

const SQLITE_BUSY_TIMEOUT_MS = parseSqliteBusyTimeoutMs(
  'NODE_API_SQLITE_BUSY_TIMEOUT_MS',
  5000
);
const WALLET_DB = createSqliteStore({
  dbPath: DB_PATH,
  busyTimeoutMs: SQLITE_BUSY_TIMEOUT_MS,
  logLabel: 'walletDb',
  enableTransactions: true,
  schemaSql: ({ busyTimeoutMs }) => `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = ${busyTimeoutMs};

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
CREATE INDEX IF NOT EXISTS idx_wallet_roots_root_cid_status
  ON wallet_roots(root_cid, status);

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
`
});

function expandCidVariants(cid) {
  const raw = String(cid || '').trim();
  if (!raw) return [];

  const variants = new Set([raw]);
  try {
    const parsed = CID.parse(raw);
    variants.add(parsed.toString());
    try {
      variants.add(parsed.toV1().toString());
    } catch {
      // ignore
    }
    try {
      variants.add(parsed.toV0().toString());
    } catch {
      // ignore
    }
  } catch {
    // ignore invalid CIDs
  }

  return Array.from(variants).filter(Boolean);
}

const dbRun = WALLET_DB.dbRun;
const dbGet = WALLET_DB.dbGet;
const dbAll = WALLET_DB.dbAll;
const runInTransaction = WALLET_DB.runInTransaction;

export async function upsertWalletRecord({ wallet, planId }) {
  const normalizedPlanId =
    typeof planId === 'string' && planId.trim() ? planId.trim() : null;

  await dbRun(
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
  const ts =
    typeof timestampMs === 'number' && Number.isFinite(timestampMs)
      ? timestampMs
      : Date.now();
  await dbRun(
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

  await dbRun(
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
  return dbGet(
    `
    SELECT wallet, plan_id, plan_expires_at, last_chain_check_at
    FROM wallets
    WHERE wallet = ?
  `,
    [wallet]
  );
}

export async function getWalletRootCids(wallet) {
  const w = String(wallet || '').trim();
  if (!w) return [];
  const rows = await dbAll(
    `
    SELECT root_cid
    FROM wallet_roots
    WHERE wallet = ? AND status = 'active'
    ORDER BY created_at DESC, root_cid ASC
  `,
    [w]
  );
  return rows.map((r) => String(r.root_cid || '').trim()).filter(Boolean);
}

export async function getWalletRootsSummary(wallet) {
  const rows = await dbAll(
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

  const variants = expandCidVariants(cid);
  const placeholders = variants.map(() => '?').join(', ');
  const params = [...variants, ...variants];

  const rows = await dbAll(
    `
    SELECT wallet FROM (
      SELECT wallet
      FROM wallet_roots
      WHERE status = 'active'
        AND root_cid IN (${placeholders})

      UNION

      SELECT wallet
      FROM wallet_pins
      WHERE cid IN (${placeholders})
    )
    ORDER BY wallet ASC
  `,
    params
  );

  return rows.map((r) => String(r.wallet || '').trim()).filter(Boolean);
}

export async function hasWalletRoot(wallet, rootCid) {
  const w = String(wallet || '').trim();
  const c = String(rootCid || '').trim();
  if (!w || !c) return false;
  const row = await dbGet(
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

  const safeLimit =
    Number.isFinite(limit) && limit > 0 ? Math.min(Number(limit), 1000) : 201;
  const safeOffset = Number.isFinite(offset) && offset >= 0 ? Number(offset) : 0;

  return dbAll(
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
  await dbRun(
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
  const now = Date.now();
  await dbRun(
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
  await dbRun(
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
  const row = await dbGet(
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
  const row = await dbGet(
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

export async function countWalletReplicationForCids(cidsInput, { sinceMs } = {}) {
  const raw = Array.isArray(cidsInput) ? cidsInput : [];
  const cids = Array.from(new Set(raw.map((c) => String(c || '').trim()).filter(Boolean)));
  const counts = new Map();
  if (!cids.length) return counts;

  const threshold =
    typeof sinceMs === 'number' && Number.isFinite(sinceMs) ? sinceMs : null;

  const placeholders = cids.map(() => '?').join(', ');

  let sql = '';
  let params = [];

  if (threshold !== null) {
    sql = `
      SELECT cid, COUNT(DISTINCT wallet) AS n
      FROM (
        SELECT root_cid AS cid, wallet
        FROM wallet_roots
        WHERE status = 'active'
          AND root_cid IN (${placeholders})
          AND created_at >= ?

        UNION ALL

        SELECT cid AS cid, wallet
        FROM wallet_pins
        WHERE cid IN (${placeholders})
          AND created_at >= ?
      ) AS t
      GROUP BY cid
    `;
    params = [...cids, threshold, ...cids, threshold];
  } else {
    sql = `
      SELECT cid, COUNT(DISTINCT wallet) AS n
      FROM (
        SELECT root_cid AS cid, wallet
        FROM wallet_roots
        WHERE status = 'active'
          AND root_cid IN (${placeholders})

        UNION ALL

        SELECT cid AS cid, wallet
        FROM wallet_pins
        WHERE cid IN (${placeholders})
      ) AS t
      GROUP BY cid
    `;
    params = [...cids, ...cids];
  }

  const rows = await dbAll(sql, params);
  for (const row of rows) {
    const cid = String(row?.cid || '').trim();
    if (!cid) continue;
    const n = typeof row.n === 'number' && Number.isFinite(row.n) && row.n >= 0 ? row.n : 0;
    counts.set(cid, n);
  }

  return counts;
}
