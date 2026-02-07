import path from 'node:path';

const num = (val, def) => {
  const n = Number(val);
  return Number.isFinite(n) && n > 0 ? n : def;
};

const bool = (val, def) => {
  if (val === undefined || val === null) return def;
  const s = String(val).trim().toLowerCase();
  if (!s) return def;
  if (s === '1' || s === 'true' || s === 'yes' || s === 'on') return true;
  if (s === '0' || s === 'false' || s === 'no' || s === 'off') return false;
  return def;
};

const KUBO_API_BASE = (process.env.KUBO_API_BASE || 'http://ipfs:5001').replace(
  /\/+$/,
  ''
);

const IPFS_GATEWAY_BASE = (
  process.env.IPFS_GATEWAY_BASE || 'http://ipfs:8080'
).replace(/\/+$/, '');

const INDEXER_DB_PATH =
  process.env.INDEXER_DB_PATH ||
  path.resolve(process.cwd(), 'indexer', 'indexer.sqlite');

const TRANSFORMERS_CACHE_DIR = (() => {
  const raw = String(process.env.TRANSFORMERS_CACHE_DIR || '').trim();
  if (!raw) return null;
  return path.resolve(raw);
})();

const TRANSFORMERS_REVISION = String(process.env.TRANSFORMERS_REVISION || 'main').trim() || 'main';
const TRANSFORMERS_LOCAL_FILES_ONLY = bool(process.env.TRANSFORMERS_LOCAL_FILES_ONLY, false);
const TEXT_TAGGER_ENABLE = bool(process.env.TEXT_TAGGER_ENABLE, true);
const IMAGE_TAGGER_ENABLE = bool(process.env.IMAGE_TAGGER_ENABLE, true);
const TEXT_TAGGER_MODEL =
  String(process.env.TEXT_TAGGER_MODEL || 'Xenova/distilbert-base-uncased-mnli').trim()
    || 'Xenova/distilbert-base-uncased-mnli';
const IMAGE_TAGGER_MODEL =
  String(process.env.IMAGE_TAGGER_MODEL || 'Xenova/clip-vit-base-patch32').trim()
    || 'Xenova/clip-vit-base-patch32';
const ML_WORKER_ENABLE = bool(process.env.ML_WORKER_ENABLE, true);
const ML_WORKER_TASK_TIMEOUT_MS = num(process.env.ML_WORKER_TASK_TIMEOUT_MS, 120000);

export const CONFIG = {
  KUBO_API_BASE,
  IPFS_GATEWAY_BASE,
  INDEXER_DB_PATH,
  INDEXER_PORT: num(process.env.INDEXER_PORT, 8790),
  PIN_LS_REFRESH_SECONDS: num(process.env.PIN_LS_REFRESH_SECONDS, 1800),
  TYPE_CRAWL_REFRESH_SECONDS: num(
    process.env.TYPE_CRAWL_REFRESH_SECONDS,
    300
  ),
  SAMPLE_BYTES: num(process.env.SAMPLE_BYTES, 262144),
  MAX_TOTAL_BYTES: num(process.env.MAX_TOTAL_BYTES, 786432),
  DOC_EXTRACT_MAX_BYTES: num(process.env.DOC_EXTRACT_MAX_BYTES, 32 * 1024 * 1024),
  DOC_EXTRACT_MAX_CHARS: num(process.env.DOC_EXTRACT_MAX_CHARS, 20000),
  DOC_EXTRACT_TIMEOUT_MS: num(process.env.DOC_EXTRACT_TIMEOUT_MS, 60000),
  DOC_EXTRACT_RETRIES: num(process.env.DOC_EXTRACT_RETRIES, 1),
  DOC_EXTRACT_RETRY_TTL_SECONDS: num(
    process.env.DOC_EXTRACT_RETRY_TTL_SECONDS,
    6 * 60 * 60
  ),
  PDF_EXTRACT_MAX_PAGES: num(process.env.PDF_EXTRACT_MAX_PAGES, 10),
  EPUB_EXTRACT_MAX_FILES: num(process.env.EPUB_EXTRACT_MAX_FILES, 25),
  SEARCH_TOKEN_INDEX_MAX_TOKENS: num(process.env.SEARCH_TOKEN_INDEX_MAX_TOKENS, 128),
  CRAWL_CONCURRENCY: num(process.env.CRAWL_CONCURRENCY, 3),
  DIR_EXPAND_REFRESH_SECONDS: num(
    process.env.DIR_EXPAND_REFRESH_SECONDS,
    600
  ),
  DIR_EXPAND_MAX_CHILDREN: num(
    process.env.DIR_EXPAND_MAX_CHILDREN,
    1000
  ),
  DIR_EXPAND_MAX_DEPTH: num(process.env.DIR_EXPAND_MAX_DEPTH, 10),
  DIR_EXPAND_CONCURRENCY: num(
    process.env.DIR_EXPAND_CONCURRENCY,
    1
  ),
  DIR_EXPAND_TTL_SECONDS: num(
    process.env.DIR_EXPAND_TTL_SECONDS,
    1800
  ),
  DIR_EXPAND_PRUNE_CHILDREN: num(
    process.env.DIR_EXPAND_PRUNE_CHILDREN,
    1
  ),
  DIR_EXPAND_TRACK_PARENT: num(
    process.env.DIR_EXPAND_TRACK_PARENT,
    1
  ),
  DIR_EXPAND_MAX_BATCH: num(process.env.DIR_EXPAND_MAX_BATCH, 50),
  PATH_INDEX_MAX_FILES_PER_ROOT: num(
    process.env.PATH_INDEX_MAX_FILES_PER_ROOT,
    1000
  ),
  PATH_INDEX_MAX_DEPTH: num(
    process.env.PATH_INDEX_MAX_DEPTH,
    10
  ),
  PATH_INDEX_MAX_DIRS_PER_ROOT: num(
    process.env.PATH_INDEX_MAX_DIRS_PER_ROOT,
    200
  ),
  SITE_ENTRYPOINT_MAX_DEPTH: num(
    process.env.SITE_ENTRYPOINT_MAX_DEPTH,
    2
  ),
  SITE_ENTRYPOINT_MAX_CANDIDATES: num(
    process.env.SITE_ENTRYPOINT_MAX_CANDIDATES,
    500
  ),
  MAGIKA_URL: process.env.MAGIKA_URL || null,
  REQUEST_TIMEOUT_MS: num(process.env.REQUEST_TIMEOUT_MS, 15000),

  // ML models (Transformers.js / @xenova/transformers)
  TRANSFORMERS_CACHE_DIR,
  TRANSFORMERS_REVISION,
  TRANSFORMERS_LOCAL_FILES_ONLY,
  TEXT_TAGGER_ENABLE,
  IMAGE_TAGGER_ENABLE,
  TEXT_TAGGER_MODEL,
  IMAGE_TAGGER_MODEL,
  ML_WORKER_ENABLE,
  ML_WORKER_TASK_TIMEOUT_MS
};
