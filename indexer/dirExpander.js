import { CONFIG } from './config.js';
import { kuboLs } from './kuboClient.js';
import { dbAll, dbGet, dbRun, runInTransaction } from './db.js';
import { detectTypeForCid } from './detectType.js';
import { buildTags } from './tags.js';
import { analyzeContentForCid, mapContentClassFromKind } from './contentSniffer.js';
import {
  incrementDirsExpanded,
  incrementDirExpandErrors
} from './metrics.js';
import { nowMs, sleep } from './utils.js';
import { log, logError } from './log.js';

let timer = null;
let running = false;

const PATH_MAX_FILES_PER_ROOT =
  CONFIG.PATH_INDEX_MAX_FILES_PER_ROOT || CONFIG.DIR_EXPAND_MAX_CHILDREN || 1000;
const PATH_MAX_DEPTH =
  CONFIG.PATH_INDEX_MAX_DEPTH || CONFIG.DIR_EXPAND_MAX_DEPTH || 10;

const PATH_EXT_ALLOWLIST = new Set([
  'html',
  'htm',
  'xhtml',
  'txt',
  'md',
  'markdown',
  'srt',
  'vtt',
  'pdf',
  'mp4',
  'm4v',
  'webm',
  'mkv',
  'mov',
  'm3u8',
  'm3u',
  'mp3',
  'flac',
  'wav',
  'ogg',
  'oga',
  'opus',
  'jpg',
  'jpeg',
  'png',
  'gif',
  'webp',
  'avif',
  'svg'
]);

function pickSiteEntrypoint(links) {
  const list = Array.isArray(links) ? links : [];
  const candidates = [];

  for (const link of list) {
    const name = String(link?.name || '').trim();
    const cid = String(link?.cid || '').trim();
    if (!name || !cid) continue;

    const lower = name.toLowerCase();
    let score = 0;

    if (lower === 'index.html' || lower === 'index.htm' || lower === 'index.xhtml') {
      score = 100;
    } else if (lower === 'home.html' || lower === 'default.html') {
      score = 90;
    } else if (lower.endsWith('.html') || lower.endsWith('.htm') || lower.endsWith('.xhtml')) {
      score = 80;
    } else if (lower === 'readme.md' || lower === 'readme.txt') {
      score = 70;
    } else if (lower.endsWith('.md') || lower.endsWith('.markdown') || lower.endsWith('.txt')) {
      score = 60;
    } else {
      continue;
    }

    const size = typeof link?.size === 'number' && link.size >= 0 ? link.size : null;
    candidates.push({ cid, name, lower, score, size });
  }

  candidates.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    const as = Number.isFinite(a.size) ? a.size : -1;
    const bs = Number.isFinite(b.size) ? b.size : -1;
    if (bs !== as) return bs - as;
    return a.lower.localeCompare(b.lower);
  });

  return candidates[0] || null;
}

async function getTagsJsonFromDb(cid) {
  try {
    const row = await dbGet('SELECT tags_json FROM cids WHERE cid = ?', [cid]);
    const raw = row?.tags_json;
    if (!raw) return null;
    return typeof raw === 'string' ? JSON.parse(raw) : raw;
  } catch {
    return null;
  }
}

function normalizeTagsJsonForSite(tagsJson, { derivedFrom } = {}) {
  const base = tagsJson && typeof tagsJson === 'object' ? tagsJson : {};

  const topics = Array.isArray(base.topics)
    ? base.topics.map((t) => String(t || '').trim()).filter(Boolean)
    : [];

  const tokens =
    base.tokens && typeof base.tokens === 'object' ? base.tokens : {};

  const signals = base.signals && typeof base.signals === 'object'
    ? base.signals
    : { from: [], bytes_read: 0 };

  const from = Array.isArray(signals.from) ? signals.from.slice(0, 20) : [];
  if (derivedFrom?.path) {
    const marker = `dir_entry:${String(derivedFrom.path).trim()}`;
    if (marker && !from.includes(marker)) from.unshift(marker);
  }

  return {
    ...base,
    topics,
    tokens,
    content_class: base.content_class || 'site',
    signals: {
      ...signals,
      from
    },
    derived_from: derivedFrom || base.derived_from || null
  };
}

async function buildTagsJsonForCid(cid) {
  const detection = await detectTypeForCid(cid, {});
  const tagsArray = buildTags({ detection });

  let contentMeta = null;
  try {
    contentMeta = await analyzeContentForCid(cid, detection);
  } catch (err) {
    logError('[dirExpander] analyzeContentForCid error', cid, err);
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

  return {
    ...baseTagsJson,
    tags: tagsArray
  };
}

async function maybeUpdateRootSiteTags(rootCid, links) {
  const entry = pickSiteEntrypoint(links);
  if (!entry) return;

  const derivedFrom = { cid: entry.cid, path: entry.name };

  let current = null;
  try {
    current = await getTagsJsonFromDb(rootCid);
  } catch {
    current = null;
  }

  const alreadyFromSameEntrypoint =
    current &&
    typeof current === 'object' &&
    current.derived_from &&
    typeof current.derived_from === 'object' &&
    String(current.derived_from.cid || '').trim() === entry.cid &&
    String(current.derived_from.path || '').trim() === entry.name &&
    ((Array.isArray(current.topics) && current.topics.length > 0) ||
      (current.tokens && typeof current.tokens === 'object' && Object.keys(current.tokens).length > 0));

  if (alreadyFromSameEntrypoint) return;

  let tagsJson = await getTagsJsonFromDb(entry.cid);
  if (!tagsJson) {
    tagsJson = await buildTagsJsonForCid(entry.cid);
  }

  const normalized = normalizeTagsJsonForSite(tagsJson, { derivedFrom });

  try {
    await dbRun(
      `UPDATE cids
       SET tags_json = ?,
           updated_at = ?
       WHERE cid = ?`,
      [JSON.stringify(normalized), nowMs(), rootCid]
    );
  } catch (err) {
    logError('[dirExpander] failed to update root tags_json', rootCid, err);
  }
}

function getExtFromName(name) {
  const idx = name.lastIndexOf('.');
  if (idx <= 0 || idx === name.length - 1) return '';
  return name.slice(idx + 1).toLowerCase();
}

function shouldIndexPathName(name) {
  const ext = getExtFromName(name.trim().toLowerCase());
  if (!ext) return false;
  return PATH_EXT_ALLOWLIST.has(ext);
}

function guessMimeHintFromName(name) {
  const ext = getExtFromName(name.trim().toLowerCase());
  if (!ext) return null;
  if (ext === 'html' || ext === 'htm' || ext === 'xhtml') return 'text/html';
  if (ext === 'txt' || ext === 'md' || ext === 'markdown') return 'text/plain';
  if (ext === 'srt' || ext === 'vtt') return 'text/plain';
  if (ext === 'pdf') return 'application/pdf';
  if (ext === 'mp4' || ext === 'm4v' || ext === 'webm' || ext === 'mkv' || ext === 'mov') {
    return 'video/*';
  }
  if (
    ext === 'mp3' ||
    ext === 'flac' ||
    ext === 'wav' ||
    ext === 'ogg' ||
    ext === 'oga' ||
    ext === 'opus'
  ) {
    return 'audio/*';
  }
  if (
    ext === 'jpg' ||
    ext === 'jpeg' ||
    ext === 'png' ||
    ext === 'gif' ||
    ext === 'webp' ||
    ext === 'avif' ||
    ext === 'svg'
  ) {
    return 'image/*';
  }
  return null;
}

async function updatePathsForDir(rootCid, links) {
  if (!PATH_MAX_FILES_PER_ROOT || PATH_MAX_FILES_PER_ROOT <= 0) return;
  if (!Array.isArray(links) || links.length === 0) return;

  let countRow;
  try {
    countRow = await dbGet(
      'SELECT COUNT(*) AS c FROM cid_paths WHERE root_cid = ?',
      [rootCid]
    );
  } catch (err) {
    logError('dirExpander: failed to count cid_paths for root', rootCid, err);
    return;
  }

  let total =
    countRow && typeof countRow.c === 'number' ? countRow.c : 0;
  if (total >= PATH_MAX_FILES_PER_ROOT) return;

  const seen = new Set();

  for (const link of links) {
    const name = (link.name || '').trim();
    const childCid = link.cid || link.Cid || link.Hash || null;
    if (!name || !childCid) continue;
    if (!shouldIndexPathName(name)) continue;

    const path = name;
    const depth = 1;
    if (depth > PATH_MAX_DEPTH) continue;
    if (total >= PATH_MAX_FILES_PER_ROOT) break;

    const key = `${rootCid}|${path}`;
    if (seen.has(key)) continue;
    seen.add(key);

    const mimeHint = guessMimeHintFromName(name);

    try {
      await dbRun(
        `INSERT INTO cid_paths (root_cid, path, leaf_cid, depth, mime_hint)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(root_cid, path) DO UPDATE SET
           leaf_cid = excluded.leaf_cid,
           depth = excluded.depth,
           mime_hint = COALESCE(excluded.mime_hint, mime_hint)
        `,
        [rootCid, path, childCid, depth, mimeHint]
      );
      total += 1;
      if (total >= PATH_MAX_FILES_PER_ROOT) break;
    } catch (err) {
      logError('dirExpander: failed to upsert cid_paths', err);
    }
  }
}

function isLikelyDirectory(row) {
  const kind = row.kind || null;
  const mime = row.mime || null;
  const source = row.source || null;
  const presentSource = row.present_source || null;
  const depth =
    typeof row.expand_depth === 'number' && row.expand_depth >= 0
      ? row.expand_depth
      : 0;

  // Always treat pin roots (depth 0, present_source='pinls') as directory
  // candidates so we can discover children like about.html, params.html, etc.
  if (presentSource === 'pinls' && depth === 0) return true;

  if (!mime && !kind) return true;
  if (!kind) return true;
  if (kind === 'unknown' || kind === 'ipld' || kind === 'dag') return true;
  if (!source) return true;
  return false;
}

export async function runDirExpandOnce() {
  const maxDepth = CONFIG.DIR_EXPAND_MAX_DEPTH;
  const maxBatch = CONFIG.DIR_EXPAND_MAX_BATCH || 50;
  const maxChildren = CONFIG.DIR_EXPAND_MAX_CHILDREN;
  const ttlMs = CONFIG.DIR_EXPAND_TTL_SECONDS * 1000;
  const now = nowMs();
  const cutoff = now - ttlMs;

  const candidates = await dbAll(
    `
    SELECT cid, expand_depth, mime, kind, source, is_directory,
           expanded_at, expand_error, present_source
    FROM cids
    WHERE present = 1
      AND expand_depth < ?
      AND (
        expanded_at IS NULL
        OR expanded_at < ?
        OR expand_error IS NOT NULL
        OR (is_directory = 0 AND present_source = 'pinls')
      )
    ORDER BY last_seen_at DESC
    LIMIT ?
  `,
    [maxDepth, cutoff, maxBatch]
  );

  if (!candidates.length) {
    return { processed: 0, expanded: 0, errors: 0 };
  }

  const concurrency = CONFIG.DIR_EXPAND_CONCURRENCY || 1;
  let index = 0;
  let processed = 0;
  let expandedCount = 0;
  let errorCount = 0;

  const worker = async () => {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const current = index;
      if (current >= candidates.length) return;
      index += 1;

      const row = candidates[current];
      processed += 1;

      try {
        const result = await expandOne(row, maxChildren);
        if (result.expanded) expandedCount += 1;
      } catch (err) {
        errorCount += 1;
        logError('dirExpander expandOne error for cid', row.cid, err);
      }
    }
  };

  const workers = [];
  for (let i = 0; i < concurrency; i += 1) {
    workers.push(worker());
  }
  await Promise.all(workers);

  if (expandedCount > 0) {
    await incrementDirsExpanded(expandedCount);
  }
  if (errorCount > 0) {
    await incrementDirExpandErrors(errorCount);
  }

  log(
    `dirExpander: processed=${processed}, expanded=${expandedCount}, errors=${errorCount}`
  );

  return { processed, expanded: expandedCount, errors: errorCount };
}

async function expandOne(row, maxChildren) {
  const now = nowMs();
  const trackEdges = CONFIG.DIR_EXPAND_TRACK_PARENT !== 0;
  const pruneChildren = CONFIG.DIR_EXPAND_PRUNE_CHILDREN !== 0;

  if (!isLikelyDirectory(row)) {
    await dbRun(
      `
      UPDATE cids
      SET is_directory = 0,
          expanded_at = ?,
          expand_error = NULL
      WHERE cid = ?
    `,
      [now, row.cid]
    );
    return { expanded: false };
  }

  let listing;
  try {
    listing = await kuboLs(row.cid);
  } catch (err) {
    await dbRun(
      `
      UPDATE cids
      SET expand_error = ?,
          expanded_at = NULL
      WHERE cid = ?
    `,
      [String(err && err.message ? err.message : 'ls_error').slice(0, 240), row.cid]
    );
    throw err;
  }

  const links = Array.isArray(listing) ? listing : [];
  if (!links.length) {
    await dbRun(
      `
      UPDATE cids
      SET is_directory = 0,
          expanded_at = ?,
          expand_error = NULL
      WHERE cid = ?
    `,
      [now, row.cid]
    );
    return { expanded: false };
  }

  const parentDepth =
    typeof row.expand_depth === 'number' && row.expand_depth >= 0
      ? row.expand_depth
      : 0;
  const childDepth = parentDepth + 1;

  const limitedLinks =
    links.length > maxChildren ? links.slice(0, maxChildren) : links;
  const truncated = links.length > maxChildren;

  const shouldTagRootSite =
    row.present_source === 'pinls' &&
    (typeof row.expand_depth !== 'number' || row.expand_depth <= 0);

  await runInTransaction(async () => {
    await dbRun(
      `
      UPDATE cids
      SET is_directory = 1,
          expanded_at = ?,
          expand_error = ?
      WHERE cid = ?
    `,
      [
        now,
        truncated
          ? `too_many_children:${links.length} (max=${maxChildren})`
          : null,
        row.cid
      ]
    );

    const currentChildSet = new Set();

    for (const link of limitedLinks) {
      const childCid = link.cid || link.Cid || link.Hash || null;
      if (!childCid) continue;
      currentChildSet.add(childCid);

      if (trackEdges) {
        await dbRun(
          `
          INSERT INTO cid_edges (parent_cid, child_cid, first_seen_at, last_seen_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(parent_cid, child_cid) DO UPDATE SET
            last_seen_at = excluded.last_seen_at,
            first_seen_at = MIN(first_seen_at, excluded.first_seen_at)
        `,
          [row.cid, childCid, now, now]
        );
      }

      await dbRun(
        `
        INSERT INTO cids (cid, present, present_source, present_reason,
                          first_seen_at, last_seen_at, expand_depth,
                          removed_at, updated_at)
        VALUES (?, 1, 'expanded', 'dir_expander', ?, ?, ?, NULL, ?)
        ON CONFLICT(cid) DO UPDATE SET
          present = 1,
          last_seen_at = excluded.last_seen_at,
          expand_depth = CASE
            WHEN cids.expand_depth IS NULL
              OR cids.expand_depth > excluded.expand_depth
              THEN excluded.expand_depth
            ELSE cids.expand_depth
          END,
          present_source = CASE
            WHEN cids.present_source = 'pinls' THEN 'pinls'
            ELSE COALESCE(cids.present_source, excluded.present_source)
          END,
          present_reason = CASE
            WHEN cids.present_source = 'pinls' THEN cids.present_reason
            ELSE COALESCE(cids.present_reason, excluded.present_reason)
          END,
          removed_at = NULL,
          updated_at = excluded.updated_at
      `,
        [childCid, now, now, childDepth, now]
      );
    }

    // Per-directory path index (CID/filename) for interesting types
    const isPinRoot = row.present_source === 'pinls';
    if (isPinRoot) {
      try {
        await updatePathsForDir(row.cid, limitedLinks);
      } catch (err) {
        logError('dirExpander: updatePathsForDir failed for root', row.cid, err);
      }
    }

    if (trackEdges && pruneChildren) {
      const existingEdges = await dbAll(
        'SELECT child_cid FROM cid_edges WHERE parent_cid = ?',
        [row.cid]
      );
      const removed = existingEdges
        .map((e) => e.child_cid)
        .filter((cid) => !currentChildSet.has(cid));

      for (const childCid of removed) {
        await dbRun(
          'DELETE FROM cid_edges WHERE parent_cid = ? AND child_cid = ?',
          [row.cid, childCid]
        );

        const countRow = await dbAll(
          'SELECT COUNT(*) AS n FROM cid_edges WHERE child_cid = ?',
          [childCid]
        );
        const n =
          countRow && countRow[0] && typeof countRow[0].n === 'number'
            ? countRow[0].n
            : 0;
        if (n === 0) {
          await dbRun(
            `UPDATE cids
             SET present = 0,
                 removed_at = ?,
                 updated_at = ?
             WHERE cid = ?
               AND present_source = 'expanded'`,
            [now, now, childCid]
          );
        }
      }
    }
  });

  if (shouldTagRootSite) {
    try {
      await maybeUpdateRootSiteTags(row.cid, limitedLinks);
    } catch (err) {
      logError('[dirExpander] root tag derivation failed', row.cid, err);
    }
  }

  return { expanded: true };
}

export function startDirExpanderWorker() {
  if (timer) return;

  const intervalMs = CONFIG.DIR_EXPAND_REFRESH_SECONDS * 1000;

  const run = async () => {
    if (running) return;
    running = true;
    try {
      await runDirExpandOnce();
    } catch (err) {
      logError('dirExpander worker iteration failed', err);
    } finally {
      running = false;
    }
  };

  // Kick off shortly after start to avoid hammering Kubo at boot
  void (async () => {
    await sleep(2000);
    await run();
  })();

  timer = setInterval(run, intervalMs);
  log(`dirExpander worker started, interval=${intervalMs}ms`);
}
