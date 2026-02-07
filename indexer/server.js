import http from 'node:http';
import os from 'node:os';
import process from 'node:process';
import { CONFIG } from './config.js';
import {
  getPrometheusMetricsSnapshot,
  getMetricsRow,
  getCountsSnapshot
} from './metrics.js';
import { dbGet, dbAll } from './db.js';
import { log, logError } from './log.js';
import { recordHttpRequest, getHttpMetricsSnapshot } from './httpMetrics.js';
import { CID } from 'multiformats/cid';

function buildCidLookupCandidates(input) {
  const raw = String(input || '').trim();
  if (!raw) return [];

  const out = [];
  const push = (v) => {
    const s = String(v || '').trim();
    if (!s) return;
    if (!out.includes(s)) out.push(s);
  };

  push(raw);

  try {
    const cid = CID.parse(raw);
    push(cid.toString());

    if (cid.version === 0) {
      push(cid.toV1().toString());
    } else if (cid.version === 1) {
      try {
        push(cid.toV0().toString());
      } catch {
        // ignore (not convertible to CIDv0)
      }
    }
  } catch {
    // ignore parse errors (treat as raw string only)
  }

  return out;
}

export function startHttpServer() {
  const port = CONFIG.INDEXER_PORT;

  const server = http.createServer((req, res) => {
    const method = req.method || 'GET';
    const url = new URL(req.url || '/', `http://localhost:${port}`);
    const start = process.hrtime.bigint();

    res.on('finish', () => {
      try {
        const end = process.hrtime.bigint();
        const durationMs = Number(end - start) / 1e6;
        recordHttpRequest({
          method,
          path: url.pathname,
          statusCode: res.statusCode || 0,
          durationMs
        });
      } catch {
        // Never break HTTP flow on metrics errors
      }
    });

    if (method === 'GET' && url.pathname === '/health') {
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true }));
      return;
    }

    if (method === 'GET' && url.pathname === '/metrics') {
      void (async () => {
        try {
          const m = await getPrometheusMetricsSnapshot();
          const httpMetrics = getHttpMetricsSnapshot();
          const lines = [];

          lines.push('# HELP indexer_pins_current Number of currently pinned CIDs');
          lines.push('# TYPE indexer_pins_current gauge');
          lines.push(`indexer_pins_current ${m.pins_current}`);

          lines.push(
            '# HELP indexer_pins_last_refresh_timestamp Last pins refresh attempt timestamp (seconds since epoch)'
          );
          lines.push('# TYPE indexer_pins_last_refresh_timestamp gauge');
          lines.push(
            `indexer_pins_last_refresh_timestamp ${m.pins_last_refresh_timestamp}`
          );

          lines.push(
            '# HELP indexer_pins_last_refresh_duration_ms Duration of last pins refresh in milliseconds'
          );
          lines.push('# TYPE indexer_pins_last_refresh_duration_ms gauge');
          lines.push(
            `indexer_pins_last_refresh_duration_ms ${m.pins_last_refresh_duration_ms}`
          );

          lines.push(
            '# HELP indexer_pins_last_refresh_success Whether last pins refresh succeeded (1) or failed (0)'
          );
          lines.push('# TYPE indexer_pins_last_refresh_success gauge');
          lines.push(
            `indexer_pins_last_refresh_success ${m.pins_last_refresh_success}`
          );

          lines.push(
            '# HELP indexer_types_indexed_total Total number of CID type rows successfully indexed'
          );
          lines.push('# TYPE indexer_types_indexed_total counter');
          lines.push(`indexer_types_indexed_total ${m.types_indexed_total}`);

          lines.push(
            '# HELP indexer_db_rows_cids Number of rows currently stored in cids table'
          );
          lines.push('# TYPE indexer_db_rows_cids gauge');
          lines.push(`indexer_db_rows_cids ${m.db_rows_cids}`);

          lines.push(
            '# HELP indexer_dirs_expanded_total Total number of directories successfully expanded'
          );
          lines.push('# TYPE indexer_dirs_expanded_total counter');
          lines.push(`indexer_dirs_expanded_total ${m.dirs_expanded_total}`);

          lines.push(
            '# HELP indexer_dir_expand_errors_total Total number of directory expansion errors'
          );
          lines.push('# TYPE indexer_dir_expand_errors_total counter');
          lines.push(
            `indexer_dir_expand_errors_total ${m.dir_expand_errors_total}`
          );

          lines.push(
            '# HELP indexer_ipfs_range_ignored_total Total number of IPFS gateway range requests that were ignored and required fallback'
          );
          lines.push('# TYPE indexer_ipfs_range_ignored_total counter');
          lines.push(
            `indexer_ipfs_range_ignored_total ${m.ipfs_range_ignored_total}`
          );

          // Process / OS metrics for indexer
          const mem = process.memoryUsage();
          const uptimeSec = process.uptime();
          const osTotalMem = os.totalmem();
          const osFreeMem = os.freemem();
          const load = os.loadavg();

          lines.push('# TYPE indexer_process_uptime_seconds gauge');
          lines.push(`indexer_process_uptime_seconds ${uptimeSec}`);
          lines.push('# TYPE indexer_process_memory_rss_bytes gauge');
          lines.push(`indexer_process_memory_rss_bytes ${mem.rss}`);
          lines.push('# TYPE indexer_process_memory_heap_used_bytes gauge');
          lines.push(
            `indexer_process_memory_heap_used_bytes ${mem.heapUsed}`
          );

          lines.push('# TYPE indexer_os_memory_total_bytes gauge');
          lines.push(`indexer_os_memory_total_bytes ${osTotalMem}`);
          lines.push('# TYPE indexer_os_memory_free_bytes gauge');
          lines.push(`indexer_os_memory_free_bytes ${osFreeMem}`);
          if (Array.isArray(load) && load.length >= 3) {
            lines.push('# TYPE indexer_os_load1 gauge');
            lines.push(`indexer_os_load1 ${load[0]}`);
            lines.push('# TYPE indexer_os_load5 gauge');
            lines.push(`indexer_os_load5 ${load[1]}`);
            lines.push('# TYPE indexer_os_load15 gauge');
            lines.push(`indexer_os_load15 ${load[2]}`);
          }

          // HTTP metrics (per method / normalized path / status)
          if (httpMetrics.counters.length > 0) {
            lines.push('# TYPE indexer_http_requests_total counter');
            for (const c of httpMetrics.counters) {
              const imethod = String(c.method || '').toUpperCase();
              const ipath = String(c.path || '/')
                .replace(/\\/g, '\\\\')
                .replace(/"/g, '\\"');
              const icode = Number.isFinite(c.code) ? c.code : 0;
              const icount =
                Number.isFinite(c.count) && c.count >= 0 ? c.count : 0;
              lines.push(
                `indexer_http_requests_total{method="${imethod}",path="${ipath}",code="${icode}"} ${icount}`
              );
            }
          }

          if (httpMetrics.durations.length > 0) {
            lines.push('# TYPE indexer_http_request_duration_ms summary');
            for (const d of httpMetrics.durations) {
              const imethod = String(d.method || '').toUpperCase();
              const ipath = String(d.path || '/')
                .replace(/\\/g, '\\\\')
                .replace(/"/g, '\\"');
              const baseLabels = `method="${imethod}",path="${ipath}"`;
              const sumMs =
                Number.isFinite(d.sumMs) && d.sumMs >= 0 ? d.sumMs : 0;
              const count =
                Number.isFinite(d.count) && d.count >= 0 ? d.count : 0;
              const maxMs =
                Number.isFinite(d.maxMs) && d.maxMs >= 0 ? d.maxMs : 0;
              lines.push(
                `indexer_http_request_duration_ms_sum{${baseLabels}} ${sumMs}`
              );
              lines.push(
                `indexer_http_request_duration_ms_count{${baseLabels}} ${count}`
              );
              lines.push(
                `indexer_http_request_duration_ms_max{${baseLabels}} ${maxMs}`
              );
            }
          }

          res.statusCode = 200;
          res.setHeader(
            'Content-Type',
            'text/plain; version=0.0.4; charset=utf-8'
          );
          res.end(`${lines.join('\n')}\n`);
        } catch (err) {
          logError('metrics endpoint error', err);
          res.statusCode = 500;
          res.setHeader('Content-Type', 'text/plain; charset=utf-8');
          res.end('# metrics_unavailable\n');
        }
      })();
      return;
    }

    if (method === 'GET' && url.pathname.startsWith('/cid/')) {
      void handleGetCid(url, res);
      return;
    }

    if (method === 'GET' && url.pathname === '/search') {
      void handleSearch(url, res);
      return;
    }

    if (method === 'GET' && url.pathname.startsWith('/children/')) {
      void handleGetChildren(url, res);
      return;
    }

    if (method === 'GET' && url.pathname.startsWith('/parents/')) {
      void handleGetParents(url, res);
      return;
    }

    if (method === 'GET' && url.pathname === '/metrics/state') {
      void handleGetMetricsState(res);
      return;
    }

    res.statusCode = 404;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ error: 'not_found' }));
  });

  server.listen(port, () => {
    log(`HTTP server listening on :${port}`);
  });

  return server;
}

async function handleGetCid(url, res) {
  try {
    const cid = url.pathname.slice('/cid/'.length);
    const trimmed = cid.trim();
    if (!trimmed) {
      res.statusCode = 400;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ error: 'cid_required' }));
      return;
    }

    const candidates = buildCidLookupCandidates(trimmed);
    let row = null;
    for (const cand of candidates) {
      // eslint-disable-next-line no-await-in-loop
      const found = await dbGet(
        `SELECT cid, present, first_seen_at, last_seen_at, removed_at,
                size_bytes, mime, ext_guess, kind, confidence, source,
                present_source, signals_json, tags_json,
                detector_version, indexed_at, error, updated_at,
                is_directory, expanded_at, expand_error, expand_depth,
                site_entry_path, site_entry_cid, site_entry_indexed_at
         FROM cids
         WHERE cid = ?`,
        [cand]
      );
      if (found) {
        row = found;
        break;
      }
    }

    if (!row) {
      res.statusCode = 404;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ error: 'not_found' }));
      return;
    }

    let signals = null;
    let tags = null;
    if (row.signals_json) {
      try {
        signals = JSON.parse(row.signals_json);
      } catch {
        signals = null;
      }
    }
    if (row.tags_json) {
      try {
        tags = JSON.parse(row.tags_json);
      } catch {
        tags = null;
      }
    }

    const payload = {
      cid: row.cid,
      present: row.present === 1,
      first_seen_at: row.first_seen_at,
      last_seen_at: row.last_seen_at,
      removed_at: row.removed_at,
      size_bytes: row.size_bytes,
      mime: row.mime,
      ext_guess: row.ext_guess,
      kind: row.kind,
      confidence: row.confidence,
      source: row.source,
      present_source: row.present_source || null,
      detector_version: row.detector_version,
      indexed_at: row.indexed_at,
      error: row.error,
      updated_at: row.updated_at,
      is_directory: row.is_directory === 1,
      expanded_at: row.expanded_at,
      expand_error: row.expand_error,
      expand_depth: row.expand_depth,
      site_entry_path: row.site_entry_path || null,
      site_entry_cid: row.site_entry_cid || null,
      site_entry_indexed_at: row.site_entry_indexed_at || null,
      signals,
      tags
    };

    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(payload));
  } catch (err) {
    logError('/cid error', err);
    res.statusCode = 500;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ error: 'internal_error' }));
  }
}

async function handleGetMetricsState(res) {
  try {
    const [counts, row] = await Promise.all([
      getCountsSnapshot(),
      getMetricsRow()
    ]);

    const payload = {
      pins_current: counts.pins_current,
      db_rows_cids: counts.db_rows_cids,
      pins_last_refresh_ts: row?.pins_last_refresh_ts ?? null,
      pins_last_refresh_duration_ms:
        row?.pins_last_refresh_duration_ms ?? null,
      pins_last_refresh_success: row?.pins_last_refresh_success ?? null,
      types_indexed_total: row?.types_indexed_total ?? 0
    };

    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(payload));
  } catch (err) {
    logError('/metrics/state error', err);
    res.statusCode = 500;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ error: 'internal_error' }));
  }
}

async function handleSearch(url, res) {
  try {
    const params = url.searchParams;

    let limit = Number.parseInt(params.get('limit') || '', 10);
    if (!Number.isFinite(limit) || limit <= 0) limit = 50;
    if (limit > 200) limit = 200;

    let offset = Number.parseInt(params.get('offset') || '', 10);
    if (!Number.isFinite(offset) || offset < 0) offset = 0;

    const whereClauses = [];
    const whereParams = [];

    // Gateway policy: do not surface unknown binary blobs.
    // Always exclude generic octet-stream from search responses.
    whereClauses.push('(c.mime IS NULL OR c.mime != ?)');
    whereParams.push('application/octet-stream');

    // Gateway policy: only surface kinds we actively support in search.
    // This intentionally excludes media types we no longer index (and any null/unknown kinds).
    whereClauses.push(
      "c.kind IN ('image', 'doc', 'html', 'text', 'archive', 'ipld', 'package')"
    );

    const tokens = params
      .getAll('token')
      .map((t) => String(t || '').trim().toLowerCase())
      .filter(Boolean)
      .filter((t) => /^[a-z0-9]+$/.test(t))
      .slice(0, 12);

    const kind = (params.get('kind') || '').trim();
    if (kind) {
      whereClauses.push('c.kind = ?');
      whereParams.push(kind);
    }

    const mime = (params.get('mime') || '').trim();
    if (mime) {
      whereClauses.push('c.mime = ?');
      whereParams.push(mime);
    }

    const presentParam = params.get('present');
    if (presentParam === '0' || presentParam === '1') {
      whereClauses.push('c.present = ?');
      whereParams.push(Number(presentParam));
    }

    const source = (params.get('source') || '').trim();
    if (source) {
      whereClauses.push('c.source = ?');
      whereParams.push(source);
    }

    const presentSource = (params.get('present_source') || '').trim();
    if (presentSource) {
      whereClauses.push('c.present_source = ?');
      whereParams.push(presentSource);
    }

    const isDirParam = params.get('is_directory');
    if (isDirParam === '0' || isDirParam === '1') {
      whereClauses.push('c.is_directory = ?');
      whereParams.push(Number(isDirParam));
    }

    const tags = params.getAll('tag').map((t) => t.trim()).filter(Boolean);
    for (const tag of tags) {
      // naive tag search: look for the quoted tag in JSON array
      whereClauses.push('c.tags_json LIKE ?');
      whereParams.push(`%"${tag}"%`);
    }

    const whereSql = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';

    let total = 0;
    let rows = [];

    if (tokens.length) {
      const tokenClauses = [];
      const tokenParams = [];

      // Prefix matching implemented as an index-friendly range query.
      // This avoids `LIKE` (which can miss indexes depending on collation settings).
      for (const t of tokens) {
        const start = String(t || '').trim();
        if (!start) continue;
        tokenClauses.push('(token >= ? AND token < ?)');
        tokenParams.push(start, `${start}\uFFFF`);
      }

      const tokenWhereSql = tokenClauses.length ? tokenClauses.join(' OR ') : '0';

      const countRow = await dbGet(
        `WITH matches AS (
           SELECT cid,
                  COUNT(*) AS matched_tokens,
                  SUM(CASE WHEN count > 50 THEN 50 ELSE count END) AS score
           FROM cid_tokens
           WHERE ${tokenWhereSql}
           GROUP BY cid
         )
         SELECT COUNT(*) AS c
         FROM matches m
         JOIN cids c ON c.cid = m.cid
         ${whereSql}`,
        [...tokenParams, ...whereParams]
      );
      total = countRow && typeof countRow.c === 'number' ? countRow.c : 0;

      rows = await dbAll(
        `WITH matches AS (
           SELECT cid,
                  COUNT(*) AS matched_tokens,
                  SUM(CASE WHEN count > 50 THEN 50 ELSE count END) AS score
           FROM cid_tokens
           WHERE ${tokenWhereSql}
           GROUP BY cid
         )
         SELECT c.cid,
                c.present,
                c.first_seen_at,
                c.last_seen_at,
                c.removed_at,
                c.size_bytes,
                c.mime,
                c.ext_guess,
                c.kind,
                c.confidence,
                c.source,
                c.present_source,
                c.signals_json,
                c.tags_json,
                c.detector_version,
                c.indexed_at,
                c.error,
                c.updated_at,
                c.is_directory,
                c.expanded_at,
                c.expand_error,
                c.expand_depth,
                p.root_cid,
                p.path,
                p.mime_hint AS path_mime_hint,
                m.matched_tokens,
                m.score AS token_score
         FROM matches m
         JOIN cids c ON c.cid = m.cid
         LEFT JOIN (
           SELECT leaf_cid,
                  MIN(root_cid) AS root_cid,
                  MIN(path) AS path,
                  MIN(mime_hint) AS mime_hint
           FROM cid_paths
           GROUP BY leaf_cid
         ) AS p
           ON p.leaf_cid = c.cid
         ${whereSql}
         ORDER BY m.matched_tokens DESC, m.score DESC, (c.last_seen_at IS NULL) ASC, c.last_seen_at DESC, c.cid ASC
         LIMIT ? OFFSET ?`,
        [...tokenParams, ...whereParams, limit, offset]
      );
    } else {
      const countRow = await dbGet(
        `SELECT COUNT(*) AS c FROM cids c ${whereSql}`,
        whereParams
      );
      total = countRow && typeof countRow.c === 'number' ? countRow.c : 0;

      rows = await dbAll(
        `SELECT c.cid,
                c.present,
                c.first_seen_at,
                c.last_seen_at,
                c.removed_at,
                c.size_bytes,
                c.mime,
                c.ext_guess,
                c.kind,
                c.confidence,
                c.source,
                c.present_source,
                c.signals_json,
                c.tags_json,
                c.detector_version,
                c.indexed_at,
                c.error,
                c.updated_at,
                c.is_directory,
                c.expanded_at,
                c.expand_error,
                c.expand_depth,
                p.root_cid,
                p.path,
                p.mime_hint AS path_mime_hint
         FROM cids c
         LEFT JOIN (
           SELECT leaf_cid,
                  MIN(root_cid) AS root_cid,
                  MIN(path) AS path,
                  MIN(mime_hint) AS mime_hint
           FROM cid_paths
           GROUP BY leaf_cid
         ) AS p
           ON p.leaf_cid = c.cid
         ${whereSql}
         ORDER BY (c.last_seen_at IS NULL) ASC, c.last_seen_at DESC, c.cid ASC
         LIMIT ? OFFSET ?`,
        [...whereParams, limit, offset]
      );
    }

    const items = rows.map((row) => ({
      cid: row.cid,
      present: row.present === 1,
      first_seen_at: row.first_seen_at,
      last_seen_at: row.last_seen_at,
      removed_at: row.removed_at,
      size_bytes: row.size_bytes,
      mime: row.mime,
      ext_guess: row.ext_guess,
      kind: row.kind,
      confidence: row.confidence,
      source: row.source,
      present_source: row.present_source || null,
      signals_json: row.signals_json || null,
      tags_json: row.tags_json || null,
      detector_version: row.detector_version,
      indexed_at: row.indexed_at,
      error: row.error,
      updated_at: row.updated_at,
      is_directory: row.is_directory === 1,
      expanded_at: row.expanded_at,
      expand_error: row.expand_error,
      expand_depth: row.expand_depth,
      root_cid: row.root_cid || null,
      path: row.path || null,
      path_mime_hint: row.path_mime_hint || null
    }));

    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ items, limit, offset, total }));
  } catch (err) {
    logError('/search error', err);
    res.statusCode = 500;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ error: 'internal_error' }));
  }
}

// intentionally no debug endpoints in prod builds

async function handleGetChildren(url, res) {
  try {
    const cid = url.pathname.slice('/children/'.length).trim();
    if (!cid) {
      res.statusCode = 400;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ error: 'cid_required' }));
      return;
    }

    const rows = await dbAll(
      `SELECT child_cid, first_seen_at, last_seen_at
       FROM cid_edges
       WHERE parent_cid = ?
       ORDER BY last_seen_at DESC, child_cid ASC
       LIMIT 200`,
      [cid]
    );

    const children = rows.map((r) => ({
      cid: r.child_cid,
      first_seen_at: r.first_seen_at,
      last_seen_at: r.last_seen_at
    }));

    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ parent: cid, children }));
  } catch (err) {
    logError('/children error', err);
    res.statusCode = 500;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ error: 'internal_error' }));
  }
}

async function handleGetParents(url, res) {
  try {
    const cid = url.pathname.slice('/parents/'.length).trim();
    if (!cid) {
      res.statusCode = 400;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ error: 'cid_required' }));
      return;
    }

    const rows = await dbAll(
      `SELECT parent_cid, first_seen_at, last_seen_at
       FROM cid_edges
       WHERE child_cid = ?
       ORDER BY last_seen_at DESC, parent_cid ASC
       LIMIT 50`,
      [cid]
    );

    const parents = rows.map((r) => ({
      cid: r.parent_cid,
      first_seen_at: r.first_seen_at,
      last_seen_at: r.last_seen_at
    }));

    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ child: cid, parents }));
  } catch (err) {
    logError('/parents error', err);
    res.statusCode = 500;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ error: 'internal_error' }));
  }
}
