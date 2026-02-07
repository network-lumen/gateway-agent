import fs from 'node:fs';
import path from 'node:path';
import { AsyncLocalStorage } from 'node:async_hooks';
import sqlite3 from 'sqlite3';
import { CONFIG } from './config.js';
import { log, logError } from './log.js';

const DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 5000;
const SQLITE_BUSY_TIMEOUT_MS = (() => {
  const raw = String(process.env.INDEXER_SQLITE_BUSY_TIMEOUT_MS || '').trim();
  if (!raw) return DEFAULT_SQLITE_BUSY_TIMEOUT_MS;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n)) return DEFAULT_SQLITE_BUSY_TIMEOUT_MS;
  return Math.min(Math.max(n, 0), 60_000);
})();

let dbInstance = null;
let initPromise = null;
let opQueue = Promise.resolve();
const txStore = new AsyncLocalStorage();

function queueDbOp(work) {
  const next = opQueue.then(work, work);
  opQueue = next.catch(() => undefined);
  return next;
}

function openDb(dbPath) {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(dbPath, (err) => {
      if (err) {
        reject(err);
      } else {
        try {
          db.configure('busyTimeout', SQLITE_BUSY_TIMEOUT_MS);
        } catch {
          // ignore
        }
        resolve(db);
      }
    });
    try {
      db.serialize();
    } catch {
      // ignore
    }
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

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row || null);
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

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

async function ensureSchemaMigrations(db) {
  // Ensure cids has directory expansion and present_source columns
  try {
    const cols = await all(db, "PRAGMA table_info('cids')");
    const names = new Set(cols.map((c) => c.name));

    const alters = [];
    if (!names.has('is_directory')) {
      alters.push(
        'ALTER TABLE cids ADD COLUMN is_directory INTEGER NOT NULL DEFAULT 0'
      );
    }
    if (!names.has('expanded_at')) {
      alters.push(
        'ALTER TABLE cids ADD COLUMN expanded_at INTEGER'
      );
    }
    if (!names.has('expand_error')) {
      alters.push(
        'ALTER TABLE cids ADD COLUMN expand_error TEXT'
      );
    }
    if (!names.has('expand_depth')) {
      alters.push(
        'ALTER TABLE cids ADD COLUMN expand_depth INTEGER NOT NULL DEFAULT 0'
      );
    }
    if (!names.has('present_source')) {
      alters.push(
        'ALTER TABLE cids ADD COLUMN present_source TEXT'
      );
    }
    if (!names.has('present_reason')) {
      alters.push(
        'ALTER TABLE cids ADD COLUMN present_reason TEXT'
      );
    }
    if (!names.has('site_entry_path')) {
      alters.push(
        'ALTER TABLE cids ADD COLUMN site_entry_path TEXT'
      );
    }
    if (!names.has('site_entry_cid')) {
      alters.push(
        'ALTER TABLE cids ADD COLUMN site_entry_cid TEXT'
      );
    }
    if (!names.has('site_entry_indexed_at')) {
      alters.push(
        'ALTER TABLE cids ADD COLUMN site_entry_indexed_at INTEGER'
      );
    }

    for (const sql of alters) {
      await exec(db, sql);
    }
  } catch (err) {
    logError('Failed to migrate cids schema', err);
  }

  // Ensure metrics has dir expansion counters
  try {
    const cols = await all(db, "PRAGMA table_info('metrics')");
    const names = new Set(cols.map((c) => c.name));
 
    const alters = [];
    if (!names.has('dirs_expanded_total')) {
      alters.push(
        'ALTER TABLE metrics ADD COLUMN dirs_expanded_total INTEGER NOT NULL DEFAULT 0'
      );
    }
    if (!names.has('dir_expand_errors_total')) {
      alters.push(
        'ALTER TABLE metrics ADD COLUMN dir_expand_errors_total INTEGER NOT NULL DEFAULT 0'
      );
    }
    if (!names.has('ipfs_range_ignored_total')) {
      alters.push(
        'ALTER TABLE metrics ADD COLUMN ipfs_range_ignored_total INTEGER NOT NULL DEFAULT 0'
      );
    }

    for (const sql of alters) {
      await exec(db, sql);
    }
  } catch (err) {
    logError('Failed to migrate metrics schema', err);
  }

  // Ensure cid_edges table exists
  try {
    await exec(
      db,
      `
      CREATE TABLE IF NOT EXISTS cid_edges (
        parent_cid TEXT NOT NULL,
        child_cid TEXT NOT NULL,
        first_seen_at INTEGER NOT NULL,
        last_seen_at INTEGER NOT NULL,
        PRIMARY KEY (parent_cid, child_cid)
      );
      CREATE INDEX IF NOT EXISTS idx_edges_parent ON cid_edges(parent_cid);
      CREATE INDEX IF NOT EXISTS idx_edges_child ON cid_edges(child_cid);
    `
    );
  } catch (err) {
      logError('Failed to ensure cid_edges schema', err);
  }

  // Ensure cid_paths table exists (per-root path index)
  try {
    await exec(
      db,
      `
      CREATE TABLE IF NOT EXISTS cid_paths (
        root_cid TEXT NOT NULL,
        path TEXT NOT NULL,
        leaf_cid TEXT NOT NULL,
        depth INTEGER NOT NULL DEFAULT 0,
        mime_hint TEXT,
        PRIMARY KEY (root_cid, path)
      );
      CREATE INDEX IF NOT EXISTS idx_cid_paths_leaf ON cid_paths(leaf_cid);
    `
    );
  } catch (err) {
    logError('Failed to ensure cid_paths schema', err);
  }

  // Ensure cid_tokens table exists (token -> cid inverted index for search)
  try {
    await exec(
      db,
      `
      CREATE TABLE IF NOT EXISTS cid_tokens (
        token TEXT NOT NULL,
        cid TEXT NOT NULL,
        count INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (token, cid)
      );
      CREATE INDEX IF NOT EXISTS idx_cid_tokens_cid ON cid_tokens(cid);
    `
    );
  } catch (err) {
    logError('Failed to ensure cid_tokens schema', err);
  }

  // Gateway policy: keep the token index compact. Short tokens (< 3 chars) are too noisy and
  // can explode the cid_tokens table size, so prune any legacy rows.
  try {
    await exec(db, 'DELETE FROM cid_tokens WHERE LENGTH(token) < 3');
  } catch (err) {
    logError('Failed to prune short cid_tokens rows', err);
  }

  // Ensure metrics row id=1 exists
  try {
    await exec(
      db,
      `INSERT INTO metrics (id, pins_current, db_rows_cids, types_indexed_total)
       VALUES (1, 0, 0, 0)
       ON CONFLICT(id) DO NOTHING`
    );
  } catch (err) {
    logError('Failed to ensure metrics row', err);
  }

  // Repair any legacy rows where present=1 but removed_at is non-NULL
  try {
    await exec(
      db,
      'UPDATE cids SET removed_at = NULL WHERE present = 1 AND removed_at IS NOT NULL'
    );
  } catch (err) {
    logError('Failed to repair cids present/removed_at invariant', err);
  }
}

export async function initDb() {
  if (dbInstance) return dbInstance;
  if (initPromise) return initPromise;

  const dbPath = CONFIG.INDEXER_DB_PATH;
  const dir = path.dirname(dbPath);
  fs.mkdirSync(dir, { recursive: true });

  initPromise = (async () => {
    const db = await openDb(dbPath);

    try {
      await exec(db, 'PRAGMA journal_mode = WAL;');
      await exec(db, 'PRAGMA synchronous = NORMAL;');
      await exec(db, 'PRAGMA foreign_keys = ON;');
      await exec(db, `PRAGMA busy_timeout = ${SQLITE_BUSY_TIMEOUT_MS};`);
      await exec(
        db,
        `
        CREATE TABLE IF NOT EXISTS cids (
          cid TEXT PRIMARY KEY,
          present INTEGER NOT NULL DEFAULT 0,
          first_seen_at INTEGER,
          last_seen_at INTEGER,
          removed_at INTEGER,
          size_bytes INTEGER,
          mime TEXT,
          ext_guess TEXT,
          kind TEXT,
          confidence REAL,
          source TEXT,
          signals_json TEXT,
          tags_json TEXT,
          detector_version TEXT,
          indexed_at INTEGER,
          error TEXT,
          updated_at INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_cids_present ON cids(present);

        CREATE TABLE IF NOT EXISTS metrics (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          pins_current INTEGER NOT NULL DEFAULT 0,
          pins_last_refresh_ts INTEGER,
          pins_last_refresh_duration_ms INTEGER,
          pins_last_refresh_success INTEGER,
          types_indexed_total INTEGER NOT NULL DEFAULT 0,
          db_rows_cids INTEGER NOT NULL DEFAULT 0,
          dirs_expanded_total INTEGER NOT NULL DEFAULT 0,
          dir_expand_errors_total INTEGER NOT NULL DEFAULT 0,
          ipfs_range_ignored_total INTEGER NOT NULL DEFAULT 0
        );
      `
      );

      await ensureSchemaMigrations(db);

      dbInstance = db;
      log('SQLite DB initialised at', dbPath);
      return dbInstance;
    } catch (err) {
      logError('Failed to initialise SQLite DB', err);
      db.close();
      throw err;
    }
  })();

  return initPromise;
}

export async function getDb() {
  if (dbInstance) return dbInstance;
  return initDb();
}

export async function dbRun(sql, params = []) {
  const tx = txStore.getStore();
  if (tx && tx.db) {
    return run(tx.db, sql, params);
  }
  return queueDbOp(async () => {
    const db = await getDb();
    return run(db, sql, params);
  });
}

export async function dbGet(sql, params = []) {
  const tx = txStore.getStore();
  if (tx && tx.db) {
    return get(tx.db, sql, params);
  }
  return queueDbOp(async () => {
    const db = await getDb();
    return get(db, sql, params);
  });
}

export async function dbAll(sql, params = []) {
  const tx = txStore.getStore();
  if (tx && tx.db) {
    return all(tx.db, sql, params);
  }
  return queueDbOp(async () => {
    const db = await getDb();
    return all(db, sql, params);
  });
}

export async function runInTransaction(work) {
  const existing = txStore.getStore();
  if (existing && existing.db) {
    existing.depth += 1;
    try {
      return await work(existing.db);
    } finally {
      existing.depth -= 1;
    }
  }

  return queueDbOp(async () => {
    const db = await getDb();
    await run(db, 'BEGIN IMMEDIATE TRANSACTION');
    return txStore.run({ db, depth: 1 }, async () => {
      try {
        const result = await work(db);
        await run(db, 'COMMIT');
        return result;
      } catch (err) {
        try {
          await run(db, 'ROLLBACK');
        } catch (rollbackErr) {
          logError('SQLite rollback failed', rollbackErr);
        }
        throw err;
      }
    });
  });
}
