import {
  parseSqliteBusyTimeoutMs,
  createSqliteStore,
  sqliteAll,
  sqliteRun
} from './sqliteUtils.js';

const DEFAULT_DB_PATH = '/data/node_api/usage.sqlite';

const DB_PATH =
  process.env.NODE_API_USAGE_DB_PATH && process.env.NODE_API_USAGE_DB_PATH.trim()
    ? process.env.NODE_API_USAGE_DB_PATH.trim()
    : DEFAULT_DB_PATH;

const SQLITE_BUSY_TIMEOUT_MS = parseSqliteBusyTimeoutMs(
  'NODE_API_SQLITE_BUSY_TIMEOUT_MS',
  5000
);

const RETENTION_MS = 90 * 24 * 60 * 60 * 1000; // 90 days
const PURGE_INTERVAL_MS = 6 * 60 * 60 * 1000; // 6 hours
let lastPurgeAtMs = 0;

const run = sqliteRun;
const all = sqliteAll;
const USAGE_DB = createSqliteStore({
  dbPath: DB_PATH,
  busyTimeoutMs: SQLITE_BUSY_TIMEOUT_MS,
  logLabel: 'usageDb',
  schemaSql: ({ busyTimeoutMs }) => `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = ${busyTimeoutMs};

CREATE TABLE IF NOT EXISTS cid_wallet_usage (
  cid TEXT NOT NULL,
  wallet TEXT NOT NULL,
  last_access_at INTEGER NOT NULL,
  last_status INTEGER,
  last_ok INTEGER,
  PRIMARY KEY (cid, wallet)
);

CREATE INDEX IF NOT EXISTS idx_cid_wallet_usage_cid
  ON cid_wallet_usage(cid);
CREATE INDEX IF NOT EXISTS idx_cid_wallet_usage_cid_last_access_at
  ON cid_wallet_usage(cid, last_access_at);
CREATE INDEX IF NOT EXISTS idx_cid_wallet_usage_last_access_at
  ON cid_wallet_usage(last_access_at);
`
});

function nowMs() {
  return Date.now();
}

async function purgeOldRows(db, now) {
  if (now - lastPurgeAtMs < PURGE_INTERVAL_MS) return;
  lastPurgeAtMs = now;
  const cutoff = now - RETENTION_MS;
  try {
    await run(db, 'DELETE FROM cid_wallet_usage WHERE last_access_at < ?', [cutoff]);
  } catch (err) {
    try {
      // eslint-disable-next-line no-console
      console.warn('[usageDb] purge failed', String(err?.message || err));
    } catch {
      // ignore
    }
  }
}

export async function recordCidAccess({ cid, wallet, ok, status, atMs } = {}) {
  const c = String(cid || '').trim();
  const w = String(wallet || '').trim();
  if (!c || !w) return;

  const ts =
    typeof atMs === 'number' && Number.isFinite(atMs) ? Math.floor(atMs) : nowMs();
  const statusCode =
    typeof status === 'number' && Number.isFinite(status) ? Math.floor(status) : null;
  const okInt = ok === true ? 1 : ok === false ? 0 : null;

  return USAGE_DB.exclusive(async (db) => {
    await purgeOldRows(db, ts);

    await run(
      db,
      `
      INSERT INTO cid_wallet_usage (cid, wallet, last_access_at, last_status, last_ok)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(cid, wallet) DO UPDATE SET
        last_access_at = excluded.last_access_at,
        last_status = excluded.last_status,
        last_ok = excluded.last_ok
    `,
      [c, w, ts, statusCode, okInt]
    );
  });
}

export async function getUsageStatsForCids(cidsInput, { sinceMs } = {}) {
  const raw = Array.isArray(cidsInput) ? cidsInput : [];
  const cids = Array.from(
    new Set(raw.map((c) => String(c || '').trim()).filter(Boolean))
  );
  const stats = new Map();
  if (!cids.length) return stats;

  const threshold =
    typeof sinceMs === 'number' && Number.isFinite(sinceMs) ? sinceMs : nowMs() - 7 * 24 * 60 * 60 * 1000;

  return USAGE_DB.exclusive(async (db) => {
    await purgeOldRows(db, nowMs());

    const placeholders = cids.map(() => '?').join(', ');
    const rows = await all(
      db,
      `
      SELECT
        cid,
        COUNT(*) AS wallets,
        SUM(CASE WHEN last_ok = 1 THEN 1 ELSE 0 END) AS ok_wallets,
        SUM(CASE WHEN last_ok = 0 THEN 1 ELSE 0 END) AS bad_wallets
      FROM cid_wallet_usage
      WHERE last_access_at >= ?
        AND cid IN (${placeholders})
      GROUP BY cid
    `,
      [threshold, ...cids]
    );

    for (const row of rows) {
      const cid = String(row?.cid || '').trim();
      if (!cid) continue;
      stats.set(cid, {
        wallets: typeof row.wallets === 'number' ? row.wallets : 0,
        ok_wallets: typeof row.ok_wallets === 'number' ? row.ok_wallets : 0,
        bad_wallets: typeof row.bad_wallets === 'number' ? row.bad_wallets : 0
      });
    }

    return stats;
  });
}
