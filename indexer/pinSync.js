import { kuboRequest } from './kuboClient.js';
import { dbAll, dbRun, runInTransaction } from './db.js';
import { updatePinSyncMetrics } from './metrics.js';
import { CONFIG } from './config.js';
import { nowMs } from './utils.js';
import { log, logError } from './log.js';

let timer = null;
let running = false;

export async function runPinSyncOnce() {
  const startedAt = nowMs();

  let pinsCurrent = 0;
  let dbRowsCids = 0;
  let success = false;

  try {
    const resp = await kuboRequest(
      '/api/v0/pin/ls?type=recursive&stream=false&quiet=false'
    );
    const text = await resp.text();
    if (!resp.ok) {
      throw new Error(
        `pin/ls failed (${resp.status}): ${text.slice(0, 240)}`
      );
    }

    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch (err) {
      throw new Error(
        `pin/ls bad JSON: ${text.slice(0, 240)}`
      );
    }

    const keys = Object.keys(parsed.Keys || {}).sort();
    pinsCurrent = keys.length;
    const now = nowMs();

    const existing = await dbAll(
      'SELECT cid, present, present_source FROM cids'
    );
    const existingMap = new Map(existing.map((r) => [r.cid, r]));
    const newSet = new Set(keys);

    await runInTransaction(async () => {
      for (const cid of keys) {
        const row = existingMap.get(cid);
        if (!row) {
          await dbRun(
            `INSERT INTO cids (
               cid, present, present_source, present_reason,
               first_seen_at, last_seen_at,
               removed_at, updated_at
             )
             VALUES (?, 1, 'pinls', 'pinls', ?, ?, NULL, ?)`,
            [cid, now, now, now]
          );
        } else {
          await dbRun(
            `UPDATE cids
             SET present = 1,
                 present_source = 'pinls',
                 present_reason = 'pinls',
                 last_seen_at = ?,
                 removed_at = NULL,
                 updated_at = ?
             WHERE cid = ?`,
            [now, now, cid]
          );
        }
      }

      for (const row of existing) {
        if (
          row.present &&
          row.present_source === 'pinls' &&
          !newSet.has(row.cid)
        ) {
          await dbRun(
            `UPDATE cids
             SET present = 0,
                 removed_at = ?,
                 updated_at = ?
             WHERE cid = ?
               AND present != 0
               AND present_source = 'pinls'`,
            [now, now, row.cid]
          );
        }
      }
    });

    dbRowsCids = await dbGetCount();

    success = true;
    const durationMs = nowMs() - startedAt;

    try {
      await updatePinSyncMetrics({
        lastRefreshTs: startedAt,
        durationMs,
        success
      });
    } catch (metricsErr) {
      logError('updatePinSyncMetrics (success path) failed', metricsErr);
    }

    log(
      `pinSync: synced ${pinsCurrent} pins (rows=${dbRowsCids}) in ${durationMs}ms`
    );
  } catch (err) {
    const durationMs = nowMs() - startedAt;

    try {
      await updatePinSyncMetrics({
        lastRefreshTs: startedAt,
        durationMs,
        success: false
      });
    } catch (metricsErr) {
      logError('updatePinSyncMetrics (failure path) failed', metricsErr);
    }

    logError('pinSync error', err);
  }

  return { pinsCurrent, dbRowsCids };
}

async function dbGetCount() {
  const rows = await dbAll('SELECT COUNT(*) AS c FROM cids');
  const row = rows[0] || { c: 0 };
  return row.c || 0;
}

export function startPinSyncWorker() {
  if (timer) return;

  const intervalMs = CONFIG.PIN_LS_REFRESH_SECONDS * 1000;

  const run = async () => {
    if (running) return;
    running = true;
    try {
      await runPinSyncOnce();
    } catch (err) {
      logError('pinSync worker iteration failed', err);
    } finally {
      running = false;
    }
  };

  // Kick off immediately (fire-and-forget)
  void run();

  timer = setInterval(run, intervalMs);
  log(`pinSync worker started, interval=${intervalMs}ms`);
}
