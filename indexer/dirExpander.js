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
  'epub',
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

function isHtmlLikeName(name) {
  const ext = getExtFromName(String(name || '').trim().toLowerCase());
  return ext === 'html' || ext === 'htm' || ext === 'xhtml';
}

function scoreHtmlEntrypointPath(pathValue) {
  const p = String(pathValue || '').trim();
  if (!p) return -Infinity;
  const lower = p.toLowerCase();
  if (!isHtmlLikeName(lower)) return -Infinity;

  let score = 0;
  if (lower === 'index.html' || lower === 'index.htm' || lower === 'index.xhtml') score += 2000;
  if (lower.endsWith('/index.html') || lower.endsWith('/index.htm') || lower.endsWith('/index.xhtml')) {
    score += 1500;
  }
  if (lower === 'home.html' || lower.endsWith('/home.html')) score += 1200;
  if (lower === 'default.html' || lower.endsWith('/default.html')) score += 1100;
  score += 500;
  score -= lower.split('/').length;
  score -= Math.min(lower.length, 256) / 128;
  return score;
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
  if (ext === 'epub') return 'application/epub+zip';
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

async function pickBestHtmlEntrypointFromCidPaths(rootCid) {
  const maxDepth = CONFIG.SITE_ENTRYPOINT_MAX_DEPTH || 2;
  const maxCandidates = CONFIG.SITE_ENTRYPOINT_MAX_CANDIDATES || 500;

  let rows = [];
  try {
    rows = await dbAll(
      `
      SELECT path, leaf_cid, depth
      FROM cid_paths
      WHERE root_cid = ?
        AND depth <= ?
        AND (
          lower(path) LIKE '%.html'
          OR lower(path) LIKE '%.htm'
          OR lower(path) LIKE '%.xhtml'
        )
      ORDER BY depth ASC, LENGTH(path) ASC
      LIMIT ?
    `,
      [rootCid, maxDepth, maxCandidates]
    );
  } catch (err) {
    logError('[dirExpander] failed to query cid_paths for entrypoint', rootCid, err);
    return null;
  }

  let best = null;
  let bestScore = -Infinity;
  for (const r of rows) {
    const p = String(r?.path || '').trim();
    const leafCid = String(r?.leaf_cid || '').trim();
    if (!p || !leafCid) continue;
    const sc = scoreHtmlEntrypointPath(p);
    if (sc > bestScore) {
      bestScore = sc;
      best = { cid: leafCid, path: p, score: sc };
    }
  }
  return best;
}

async function maybeUpdateRootSiteEntrypoint(rootCid, links) {
  const root = String(rootCid || '').trim();
  if (!root) return null;

  const fromPaths = await pickBestHtmlEntrypointFromCidPaths(root);
  let entry = fromPaths;

  if (!entry) {
    const list = Array.isArray(links) ? links : [];
    let best = null;
    let bestScore = -Infinity;
    for (const link of list) {
      const name = String(link?.name || '').trim();
      const cid = String(link?.cid || '').trim();
      if (!name || !cid) continue;
      if (!isHtmlLikeName(name)) continue;
      const sc = scoreHtmlEntrypointPath(name);
      if (sc > bestScore) {
        bestScore = sc;
        best = { cid, path: name, score: sc };
      }
    }
    entry = best;
  }

  if (!entry) return null;

  try {
    const current = await dbGet(
      'SELECT site_entry_path, site_entry_cid FROM cids WHERE cid = ?',
      [root]
    );
    const curPath = String(current?.site_entry_path || '').trim();
    const curCid = String(current?.site_entry_cid || '').trim();
    if (curPath === entry.path && curCid === entry.cid) return entry;

    await dbRun(
      `UPDATE cids
       SET site_entry_path = ?,
           site_entry_cid = ?,
           site_entry_indexed_at = ?,
           updated_at = ?
       WHERE cid = ?`,
      [entry.path, entry.cid, nowMs(), nowMs(), root]
    );
  } catch (err) {
    logError('[dirExpander] failed to persist root site entrypoint', root, err);
  }

  return entry;
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

  const maxDirs = CONFIG.PATH_INDEX_MAX_DIRS_PER_ROOT || 200;
  const maxChildren = CONFIG.DIR_EXPAND_MAX_CHILDREN || 1000;
  const visitedDirs = new Set([rootCid]);
  const seenPaths = new Set();
  const dirQueue = [{ cid: rootCid, prefix: '', depth: 0, links }];

  let processedDirs = 0;

  while (dirQueue.length) {
    const cur = dirQueue.shift();
    if (!cur) break;
    processedDirs += 1;
    if (processedDirs > maxDirs) break;

    const listRaw = Array.isArray(cur.links) ? cur.links : [];
    const list = listRaw.length > maxChildren ? listRaw.slice(0, maxChildren) : listRaw;
    for (const link of list) {
      if (total >= PATH_MAX_FILES_PER_ROOT) break;

      const name = String(link?.name || '').trim();
      const childCidRaw = link?.cid || link?.Cid || link?.Hash || null;
      const childCid = String(childCidRaw || '').trim();
      if (!name || !childCid) continue;

      const relPath = cur.prefix ? `${cur.prefix}/${name}` : name;
      const depth = cur.depth + 1;
      if (depth > PATH_MAX_DEPTH) continue;

      const type = String(link?.type || '').toLowerCase().trim();
      const ext = getExtFromName(name.trim().toLowerCase());
      const maybeDirName = !ext && !name.includes('.');
      const isDir = type === 'dir' || (type === 'unknown' && maybeDirName);

      if (isDir) {
        if (cur.depth >= PATH_MAX_DEPTH) continue;
        if (visitedDirs.has(childCid)) continue;
        if (visitedDirs.size >= maxDirs) continue;
        visitedDirs.add(childCid);
        try {
          // eslint-disable-next-line no-await-in-loop
          const childLinks = await kuboLs(childCid);
          const nextLinksRaw = Array.isArray(childLinks) ? childLinks : [];
          const nextLinks =
            nextLinksRaw.length > maxChildren
              ? nextLinksRaw.slice(0, maxChildren)
              : nextLinksRaw;
          if (!nextLinks.length) continue;
          dirQueue.push({
            cid: childCid,
            prefix: relPath,
            depth,
            links: nextLinks
          });
        } catch {
          // ignore directory traversal errors
        }
        continue;
      }

      if (!shouldIndexPathName(name)) continue;

      const key = `${rootCid}|${relPath}`;
      if (seenPaths.has(key)) continue;
      seenPaths.add(key);

      const mimeHint = guessMimeHintFromName(name);
      try {
        // eslint-disable-next-line no-await-in-loop
        await dbRun(
          `INSERT INTO cid_paths (root_cid, path, leaf_cid, depth, mime_hint)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(root_cid, path) DO UPDATE SET
             leaf_cid = excluded.leaf_cid,
             depth = excluded.depth,
             mime_hint = COALESCE(excluded.mime_hint, mime_hint)
          `,
          [rootCid, relPath, childCid, depth, mimeHint]
        );
        total += 1;
      } catch (err) {
        logError('dirExpander: failed to upsert cid_paths', err);
      }
    }

    if (total >= PATH_MAX_FILES_PER_ROOT) break;
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

  // For pinned roots, only treat them as directory candidates when we don't have a
  // clear non-directory kind yet (website folders, CAR/IPLD roots, etc.).
  // This avoids marking pinned single-file PDFs/DOCX/EPUB as directories.
  if (presentSource === 'pinls' && depth === 0) {
    if (!mime && !kind) return true;
    if (!kind) return true;
    if (kind === 'unknown' || kind === 'ipld' || kind === 'dag') return true;
    return false;
  }

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
      SET is_directory = 1,
          expanded_at = ?,
          expand_error = NULL
      WHERE cid = ?
    `,
      [now, row.cid]
    );
    return { expanded: true };
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

  // Per-directory path index (CID/path) for pin roots only.
  // Done outside the DB transaction since it can involve multiple Kubo ls calls.
  const isPinRoot = row.present_source === 'pinls';
  if (isPinRoot) {
    try {
      await updatePathsForDir(row.cid, limitedLinks);
    } catch (err) {
      logError('dirExpander: updatePathsForDir failed for root', row.cid, err);
    }
  }

  if (shouldTagRootSite) {
    try {
      const entry = await maybeUpdateRootSiteEntrypoint(row.cid, limitedLinks);
      if (entry && entry.cid && entry.path) {
        await maybeUpdateRootSiteTags(row.cid, [
          { cid: entry.cid, name: entry.path, size: null, type: 'file' }
        ]);
      } else {
        await maybeUpdateRootSiteTags(row.cid, limitedLinks);
      }
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
