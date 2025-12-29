import { dbAll, dbRun } from './db.js';
import { CONFIG } from './config.js';
import { detectTypeForCid, DETECTOR_VERSION } from './detectType.js';
import { buildTags } from './tags.js';
import { analyzeContentForCid, mapContentClassFromKind } from './contentSniffer.js';
import { incrementTypesIndexed } from './metrics.js';
import { nowMs, readResponseBodyLimited } from './utils.js';
import { log, logError } from './log.js';

let timer = null;
let running = false;

const DEBUG_ENABLED = CONFIG.TYPECRAWLER_DEBUG;
const DEBUG_MAX_EVENTS = 50;

const debugState = {
  lastRun: null,
  events: []
};

function pushDebugEvent(evt) {
  if (!DEBUG_ENABLED) return;
  const event = {
    ts: nowMs(),
    ...evt
  };
  debugState.events.push(event);
  if (debugState.events.length > DEBUG_MAX_EVENTS) {
    debugState.events.splice(0, debugState.events.length - DEBUG_MAX_EVENTS);
  }
}

export function getTypeCrawlerDebugSnapshot() {
  return {
    debugEnabled: DEBUG_ENABLED,
    lastRun: debugState.lastRun,
    events: debugState.events
  };
}

export async function runTypeCrawlOnce() {
  const candidates = await dbAll(
    `SELECT cid, present, size_bytes, detector_version, error,
            is_directory, present_source
     FROM cids
     WHERE present = 1
       AND (detector_version IS NULL
         OR detector_version <> ?
         OR mime IS NULL
         OR error IS NOT NULL)`,
    [DETECTOR_VERSION]
  );

  if (!candidates.length) {
    return { processed: 0, updated: 0 };
  }

  const concurrency = CONFIG.CRAWL_CONCURRENCY || 1;
  const startedAt = nowMs();

  const stats = {
    startedAt,
    finishedAt: null,
    candidates: candidates.length,
    attempted: 0,
    ok: 0,
    skipped: 0,
    failed: 0
  };

  let index = 0;
  let processed = 0;
  let updated = 0;

  const worker = async () => {
    for (;;) {
      const current = index;
      if (current >= candidates.length) return;
      index += 1;

      const row = candidates[current];
      processed += 1;

      if (row.is_directory === 1) {
        stats.skipped += 1;
        pushDebugEvent({
          type: 'skip',
          reason: 'is_directory',
          cid: row.cid,
          present_source: row.present_source ?? null
        });
        continue;
      }

      stats.attempted += 1;

      try {
        const detection = await detectTypeForCid(row.cid, {
          sizeBytes: row.size_bytes ?? undefined
        });

        pushDebugEvent({
          type: 'detect_ok',
          cid: row.cid,
          mime: detection.mime || null,
          ext_guess: detection.ext_guess || null,
          kind: detection.kind || null,
          confidence: detection.confidence ?? null,
          source: detection.source || null,
          size: detection.size ?? null
        });

        const tagsArray = buildTags({ detection });

        let contentMeta = null;
        try {
          contentMeta = await analyzeContentForCid(row.cid, detection);
        } catch (err) {
          logError('[typeCrawler] contentSniffer error for cid', row.cid, err);
        }

        const baseTagsJson =
          contentMeta ||
          {
            topics: [],
            content_class: mapContentClassFromKind(detection.kind),
            lang: 'en',
            confidence:
              typeof detection.confidence === 'number'
                ? detection.confidence
                : 0,
            signals: {
              from: [],
              bytes_read: 0
            }
          };

        const tagsJson = {
          ...baseTagsJson,
          tags: tagsArray
        };
        const now = nowMs();

        const stmt = await dbRun(
          `UPDATE cids
           SET size_bytes = ?,
               mime = ?,
               ext_guess = ?,
               kind = ?,
           confidence = ?,
           source = ?,
           signals_json = ?,
           tags_json = ?,
               detector_version = ?,
               indexed_at = ?,
               error = NULL,
               updated_at = ?
           WHERE cid = ?`,
          [
            detection.size ?? null,
            detection.mime || null,
            detection.ext_guess || null,
            detection.kind || null,
            detection.confidence ?? null,
            detection.source || null,
            JSON.stringify(detection.signals || {}),
            JSON.stringify(tagsJson),
            detection.detector_version,
            detection.indexed_at,
            now,
            row.cid
          ]
        );

        const changes =
          stmt && typeof stmt.changes === 'number' ? stmt.changes : null;

        pushDebugEvent({
          type: 'db_update',
          cid: row.cid,
          changes
        });

        updated += 1;
        stats.ok += 1;
      } catch (err) {
        stats.failed += 1;
        const now = nowMs();
        try {
          await dbRun(
            `UPDATE cids
             SET error = ?,
                 detector_version = ?,
                 updated_at = ?
             WHERE cid = ?`,
            [
              String(err && err.message ? err.message : 'detect_error'),
              DETECTOR_VERSION,
              now,
              row.cid
            ]
          );
        } catch (dbErr) {
          logError(
            '[typeCrawler] failed to record error in DB for cid',
            row.cid,
            dbErr
          );
        }
        pushDebugEvent({
          type: 'error',
          cid: row.cid,
          message: err && err.message ? err.message : String(err)
        });
        logError('[typeCrawler] detect error for cid', row.cid, err);
      }
    }
  };

  const workers = [];
  for (let i = 0; i < concurrency; i += 1) {
    workers.push(worker());
  }
  await Promise.all(workers);

  if (updated > 0) {
    incrementTypesIndexed(updated);
  }

  const durationMs = nowMs() - startedAt;
  stats.finishedAt = nowMs();
  debugState.lastRun = stats;

  log(
    `[typeCrawler] candidates=${stats.candidates} processed=${processed} attempted=${stats.attempted} ok=${stats.ok} skipped=${stats.skipped} failed=${stats.failed} duration=${durationMs}ms`
  );

  return { processed, updated };
}

export async function sampleDebugForCid(cid) {
  const SAMPLE_BYTES = CONFIG.SAMPLE_BYTES;
  const url = new URL(`/ipfs/${cid}`, CONFIG.IPFS_GATEWAY_BASE).toString();

  const result = {
    cid,
    head: null,
    tail: null,
    mid: null,
    detect: null,
    supports_range: null,
    range_status: null,
    range_ignored: null,
    bytes_returned: null,
    warnings: []
  };

  // HEAD size probe
  try {
    const resp = await fetch(url, { method: 'HEAD' });
    result.head = {
      url,
      status: resp.status,
      content_length: resp.headers.get('content-length') || null
    };
  } catch (err) {
    result.head = {
      url,
      error: err && err.message ? err.message : String(err)
    };
  }

  // Simple range helpers for debug (best-effort)
  async function fetchRangeDebug(rangeStart, rangeEnd) {
    const r = `bytes=${rangeStart}-${rangeEnd}`;
    const resp = await fetch(url, {
      method: 'GET',
      headers: { Range: r }
    });
    const contentRange = resp.headers.get('content-range') || null;
    const supportsRange = resp.status === 206 || !!contentRange;
    const rangeIgnored = resp.status === 200 && !contentRange;
    if (rangeIgnored) {
      result.warnings.push('range_not_supported_fallback_used');
    }
    const buf = await readResponseBodyLimited(resp, SAMPLE_BYTES);
    result.supports_range = supportsRange;
    result.range_status = resp.status;
    result.range_ignored = rangeIgnored;
    result.bytes_returned = buf.length;
    return {
      url,
      range: r,
      status: resp.status,
      content_range: contentRange,
      content_length: resp.headers.get('content-length') || null,
      bytes: buf.subarray(0, 256).toString('hex')
    };
  }

  try {
    result.sample = await fetchRangeDebug(0, SAMPLE_BYTES - 1);
  } catch (err) {
    result.sample = {
      url,
      error: err && err.message ? err.message : String(err)
    };
  }

  // Run full detector as well, to expose normalized result
  try {
    const detection = await detectTypeForCid(cid);
    result.detect = {
      mime: detection.mime || null,
      ext_guess: detection.ext_guess || null,
      kind: detection.kind || null,
      confidence: detection.confidence ?? null,
      source: detection.source || null,
      size: detection.size ?? null
    };
  } catch (err) {
    result.detect = {
      error: err && err.message ? err.message : String(err)
    };
  }

  return result;
}

export function startTypeCrawlerWorker() {
  if (timer) return;

  const intervalMs = CONFIG.TYPE_CRAWL_REFRESH_SECONDS * 1000;

  const run = async () => {
    if (running) return;
    running = true;
    try {
      await runTypeCrawlOnce();
    } catch (err) {
      logError('[typeCrawler] worker iteration failed', err);
    } finally {
      running = false;
    }
  };

  // Kick off immediately
  void run();

  timer = setInterval(run, intervalMs);
  log(`[typeCrawler] worker started, interval=${intervalMs}ms`);
}
