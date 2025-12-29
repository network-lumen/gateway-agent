import path from 'node:path';

const num = (val, def) => {
  const n = Number(val);
  return Number.isFinite(n) && n > 0 ? n : def;
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
    2
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
  VIDEO_METADATA_MAX_DURATION_SECONDS: num(
    process.env.VIDEO_METADATA_MAX_DURATION_SECONDS,
    3 * 60 * 60
  ),
  VIDEO_METADATA_MAX_SIZE_BYTES: num(
    process.env.VIDEO_METADATA_MAX_SIZE_BYTES,
    10 * 1024 * 1024 * 1024
  ),
  TYPECRAWLER_DEBUG:
    Number.isFinite(Number(process.env.TYPECRAWLER_DEBUG))
      ? Number(process.env.TYPECRAWLER_DEBUG) !== 0
      : false,
  MAGIKA_URL: process.env.MAGIKA_URL || null,
  REQUEST_TIMEOUT_MS: num(process.env.REQUEST_TIMEOUT_MS, 15000)
};
