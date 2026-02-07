import fs from 'node:fs';
import path from 'node:path';
import { AsyncLocalStorage } from 'node:async_hooks';
import sqlite3 from 'sqlite3';

export function parseSqliteBusyTimeoutMs(envVarName, defaultMs = 5000) {
  const raw = String(process.env[envVarName] || '').trim();
  if (!raw) return defaultMs;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n)) return defaultMs;
  return Math.min(Math.max(n, 0), 60_000);
}

export function ensureParentDir(filePath, logLabel = 'sqlite') {
  const dir = path.dirname(String(filePath || '').trim() || '.');
  try {
    fs.mkdirSync(dir, { recursive: true });
  } catch (err) {
    try {
      // eslint-disable-next-line no-console
      console.error(`[${logLabel}] failed to create db dir`, {
        dir,
        error: String(err?.message || err)
      });
    } catch {
      // ignore
    }
  }
}

export function createOpQueue() {
  let opQueue = Promise.resolve();
  return function queueDbOp(work) {
    const next = opQueue.then(work, work);
    opQueue = next.catch(() => undefined);
    return next;
  };
}

export function openSqliteDb(dbPath, { busyTimeoutMs } = {}) {
  const timeout =
    typeof busyTimeoutMs === 'number' && Number.isFinite(busyTimeoutMs) && busyTimeoutMs >= 0
      ? Math.floor(busyTimeoutMs)
      : 5000;

  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(dbPath, (err) => {
      if (err) return reject(err);
      try {
        db.configure('busyTimeout', timeout);
      } catch {
        // ignore
      }
      resolve(db);
    });

    try {
      db.serialize();
    } catch {
      // ignore
    }
  });
}

export function sqliteRun(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

export function sqliteAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows || []);
    });
  });
}

export function sqliteGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row || null);
    });
  });
}

export function sqliteExec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

export function createSqliteStore({
  dbPath,
  schemaSql,
  logLabel = 'sqlite',
  busyTimeoutMs = 5000,
  enableTransactions = false
} = {}) {
  const resolvedPath = String(dbPath || '').trim();
  if (!resolvedPath) {
    throw new Error('createSqliteStore: dbPath is required');
  }

  const queueDbOp = createOpQueue();
  const txStore = enableTransactions ? new AsyncLocalStorage() : null;

  let dbInstance = null;
  let initPromise = null;
  let schemaEnsured = false;

  function getSchemaSql() {
    if (typeof schemaSql === 'function') {
      return schemaSql({ busyTimeoutMs });
    }
    return String(schemaSql || '').trim();
  }

  async function ensureSchema(db) {
    if (schemaEnsured) return;
    const sql = getSchemaSql();
    if (sql) {
      await sqliteExec(db, sql);
    }
    schemaEnsured = true;
  }

  async function initDb() {
    if (dbInstance) return dbInstance;
    if (initPromise) return initPromise;

    ensureParentDir(resolvedPath, logLabel);

    initPromise = (async () => {
      const db = await openSqliteDb(resolvedPath, { busyTimeoutMs });
      await ensureSchema(db);
      dbInstance = db;
      return dbInstance;
    })().catch((err) => {
      initPromise = null;
      throw err;
    });

    return initPromise;
  }

  async function getDb() {
    if (dbInstance) return dbInstance;
    return initDb();
  }

  async function exclusive(work) {
    return queueDbOp(async () => {
      const db = await getDb();
      return work(db);
    });
  }

  async function dbRun(sql, params = []) {
    const tx = txStore ? txStore.getStore() : null;
    if (tx && tx.db) {
      return sqliteRun(tx.db, sql, params);
    }
    return queueDbOp(async () => {
      const db = await getDb();
      return sqliteRun(db, sql, params);
    });
  }

  async function dbGet(sql, params = []) {
    const tx = txStore ? txStore.getStore() : null;
    if (tx && tx.db) {
      return sqliteGet(tx.db, sql, params);
    }
    return queueDbOp(async () => {
      const db = await getDb();
      return sqliteGet(db, sql, params);
    });
  }

  async function dbAll(sql, params = []) {
    const tx = txStore ? txStore.getStore() : null;
    if (tx && tx.db) {
      return sqliteAll(tx.db, sql, params);
    }
    return queueDbOp(async () => {
      const db = await getDb();
      return sqliteAll(db, sql, params);
    });
  }

  async function runInTransaction(work) {
    if (!enableTransactions || !txStore) {
      throw new Error('createSqliteStore: transactions not enabled for this store');
    }

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
      await sqliteRun(db, 'BEGIN IMMEDIATE TRANSACTION');
      return txStore.run({ db, depth: 1 }, async () => {
        try {
          const result = await work(db);
          await sqliteRun(db, 'COMMIT');
          return result;
        } catch (err) {
          try {
            await sqliteRun(db, 'ROLLBACK');
          } catch {
            // ignore rollback errors
          }
          throw err;
        }
      });
    });
  }

  return {
    dbPath: resolvedPath,
    busyTimeoutMs,
    initDb,
    getDb,
    exclusive,
    dbRun,
    dbGet,
    dbAll,
    runInTransaction: enableTransactions ? runInTransaction : null
  };
}
