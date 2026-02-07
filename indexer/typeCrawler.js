import { dbAll, dbRun } from './db.js';
import { CONFIG } from './config.js';
import { detectTypeForCid, DETECTOR_VERSION } from './detectType.js';
import { buildTags } from './tags.js';
import { analyzeContentForCid, mapContentClassFromKind } from './contentSniffer.js';
import { incrementTypesIndexed } from './metrics.js';
import { nowMs } from './utils.js';
import { log, logError } from './log.js';

let timer = null;
let running = false;

const TOKEN_INDEX_MAX =
  typeof CONFIG.SEARCH_TOKEN_INDEX_MAX_TOKENS === 'number' &&
  CONFIG.SEARCH_TOKEN_INDEX_MAX_TOKENS > 0
    ? CONFIG.SEARCH_TOKEN_INDEX_MAX_TOKENS
    : 128;

function extractIndexableTokens(tagsJson) {
  const tokensObj =
    tagsJson && tagsJson.tokens && typeof tagsJson.tokens === 'object'
      ? tagsJson.tokens
      : null;
  if (!tokensObj) return [];

  const pairs = [];
  for (const [rawToken, rawCount] of Object.entries(tokensObj)) {
    const token = String(rawToken || '').trim().toLowerCase();
    if (!token) continue;
    if (token.length < 3) continue;
    if (/\s/.test(token)) continue;
    if (!/^[a-z0-9]+$/.test(token)) continue;

    const count = Number(rawCount);
    if (!Number.isFinite(count) || count <= 0) continue;
    pairs.push([token, Math.min(1000, Math.floor(count))]);
  }

  pairs.sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return String(a[0]).localeCompare(String(b[0]));
  });

  return pairs.slice(0, TOKEN_INDEX_MAX);
}

export async function runTypeCrawlOnce() {
  const now = nowMs();
  const retryTtlSeconds =
    typeof CONFIG.DOC_EXTRACT_RETRY_TTL_SECONDS === 'number' &&
    CONFIG.DOC_EXTRACT_RETRY_TTL_SECONDS > 0
      ? CONFIG.DOC_EXTRACT_RETRY_TTL_SECONDS
      : 6 * 60 * 60;
  const docRetryCutoff = now - retryTtlSeconds * 1000;
  const docRetryCutoffIso = new Date(docRetryCutoff).toISOString();

  const candidates = await dbAll(
    `SELECT cid, present, size_bytes, detector_version, error,
            is_directory, present_source, mime, kind, indexed_at
     FROM cids
     WHERE present = 1
        AND (detector_version IS NULL
          OR detector_version <> ?
          OR mime IS NULL
          OR error IS NOT NULL
          OR (
            kind = 'text'
            AND (tags_json IS NULL OR tags_json NOT LIKE '%"preview"%')
          )
          OR (
            kind = 'doc'
            AND tags_json LIKE '%"description":"PK%'
          )
          OR (
            kind = 'doc'
            AND (ext_guess = 'epub' OR mime LIKE '%epub%')
            AND (tags_json IS NULL OR tags_json NOT LIKE '%doc:epub_text_v2%')
            AND (tags_json IS NULL OR tags_json NOT LIKE '%doc:epub_parse_failed%')
          )
          OR (
            kind = 'doc'
            AND (
              tags_json IS NULL
              OR tags_json LIKE '%doc:sample%'
              OR tags_json LIKE '%doc:too_large%'
              OR tags_json LIKE '%doc:full_fetch_failed%'
              OR tags_json LIKE '%doc:pdf_url_extract_failed%'
            )
            AND (tags_json IS NULL OR tags_json NOT LIKE '%doc:epub_parse_failed%')
            AND (indexed_at IS NULL OR indexed_at < ?)
          ))`,
    [DETECTOR_VERSION, docRetryCutoffIso]
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

      const rowKind = row.kind ?? null;
      const rowMime = row.mime ?? null;
      const shouldSkipAsDir =
        row.is_directory === 1 &&
        (!rowKind || rowKind === 'unknown' || rowKind === 'ipld' || rowKind === 'dag' || !rowMime);

      if (shouldSkipAsDir) {
        stats.skipped += 1;
        continue;
      }

      stats.attempted += 1;

      try {
        const detection = await detectTypeForCid(row.cid, {
          sizeBytes: row.size_bytes ?? undefined
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
        const tokenPairs = extractIndexableTokens(tagsJson);

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

        try {
          await dbRun('DELETE FROM cid_tokens WHERE cid = ?', [row.cid]);
          if (tokenPairs.length) {
            for (const [token, count] of tokenPairs) {
              // eslint-disable-next-line no-await-in-loop
              await dbRun(
                'INSERT OR REPLACE INTO cid_tokens (token, cid, count) VALUES (?, ?, ?)',
                [token, row.cid, count]
              );
            }
          }
        } catch (tokenErr) {
          logError('[typeCrawler] failed to update cid_tokens for cid', row.cid, tokenErr);
        }

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

  log(
    `[typeCrawler] candidates=${stats.candidates} processed=${processed} attempted=${stats.attempted} ok=${stats.ok} skipped=${stats.skipped} failed=${stats.failed} duration=${durationMs}ms`
  );

  return { processed, updated };
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
