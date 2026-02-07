# Indexer Service

This folder contains a standalone indexer process that keeps a local SQLite cache of IPFS pins, type metadata, and deterministic tags. It is designed to be:

- Deterministic
- Bounded in I/O (HTTP range sampling with hard caps).
- Easy to run in Docker and on Windows (uses the `sqlite3` driver with prebuilt binaries).

It runs as an independent service and is consumed by the `node_api` for search and introspection.

## Features

- Periodic sync against Kubo `pin/ls` (source of truth).
- Local SQLite database:
  - `cids` – one row per CID (type, size, tags, content metadata, directory flags).
  - `metrics` – global counters and gauges.
  - `cid_edges` – parent/child relationships for directory expansion.
  - `cid_paths` – per-root path index for interesting files.
- Type detection pipeline using HTTP range sampling, `file-type`, and container sniffers.
- Content analysis:
  - HTML/text/doc: bag-of-words + zero-shot text labels.
  - Images: filename analysis + CLIP zero-shot tags.
- Deterministic, low-cardinality tags stored as JSON.
- Periodic type crawling with configurable concurrency.
- Directory expansion (`dirExpander`) that walks pinned UnixFS directories and inserts child CIDs into `cids`.
- Minimal HTTP server exposing:
  - `GET /health`
  - `GET /metrics` (Prometheus format)
  - `GET /metrics/state`
  - `GET /cid/:cid`
  - `GET /search`
  - `GET /children/:cid`, `GET /parents/:cid`

## Environment

All environment variables are optional; defaults are shown. `KUBO_API_BASE` **must** include the TCP port `:5001` and `IPFS_GATEWAY_BASE` **must** include `:8080` (the Kubo API and gateway ports):

- `KUBO_API_BASE` (default `http://ipfs:5001`)
- `IPFS_GATEWAY_BASE` (default `http://ipfs:8080`)
- `INDEXER_DB_PATH` (default `./indexer/indexer.sqlite`)
- `INDEXER_PORT` (default `8790`)
- `PIN_LS_REFRESH_SECONDS` (default `1800`)
- `TYPE_CRAWL_REFRESH_SECONDS` (default `300`)
- `SAMPLE_BYTES` (default `262144`) – bytes sampled at head/tail for content detection.
- `MAX_TOTAL_BYTES` (default `786432`) – hard cap on total sampled bytes per CID.
- `DOC_EXTRACT_MAX_BYTES` (default `33554432`) – max bytes downloaded to extract text from document containers (PDF/DOCX/EPUB).
- `DOC_EXTRACT_MAX_CHARS` (default `20000`) – max extracted characters considered for tokenization.
- `DOC_EXTRACT_TIMEOUT_MS` (default `60000`) – timeout for full document download/parsing attempts.
- `DOC_EXTRACT_RETRIES` (default `1`) – retries for full document download attempts.
- `DOC_EXTRACT_RETRY_TTL_SECONDS` (default `21600`) – minimum time between re-attempts when a document is still in fallback mode (`doc:sample`, `doc:too_large`, `doc:full_fetch_failed`); retry throttling is based on `indexed_at` (pin sync updates `updated_at` frequently).
- `PDF_EXTRACT_MAX_PAGES` (default `10`) – maximum PDF pages extracted (best-effort).
- `EPUB_EXTRACT_MAX_FILES` (default `25`) – maximum EPUB content files scanned (best-effort).
- `CRAWL_CONCURRENCY` (default `3`) – concurrent typeCrawler workers.
- `MAGIKA_URL` (optional) – HTTP endpoint for Magika classification (expects `{ size, head_base64, tail_base64 }` and returns `{ mime, ext?, kind?, confidence? }`).
- `REQUEST_TIMEOUT_MS` (default `15000`).
- `DIR_EXPAND_REFRESH_SECONDS` (default `600`) – how often directory expansion runs.
- `DIR_EXPAND_TTL_SECONDS` (default `1800`) – minimum age before a directory is re-expanded.
- `DIR_EXPAND_MAX_CHILDREN` (default `1000`) – max children per directory expansion.
- `DIR_EXPAND_MAX_DEPTH` (default `10`) – max recursive depth for expansion.
- `DIR_EXPAND_CONCURRENCY` (default `1`) – number of concurrent expansion workers.
- `DIR_EXPAND_PRUNE_CHILDREN` (default `1`) – whether to prune children no longer present in a directory.
- `DIR_EXPAND_TRACK_PARENT` (default `1`) – maintain `cid_edges` parent/child relations.
- `DIR_EXPAND_MAX_BATCH` (default `50`) – max number of directories processed per expansion cycle.
- `PATH_INDEX_MAX_FILES_PER_ROOT` (default `1000`) – per-root limit for `cid_paths` (path index).
- `PATH_INDEX_MAX_DEPTH` (default `10`) – max path depth stored in `cid_paths`.

To reset the indexer state **(for development only)**, you can safely delete the on-disk SQLite file used by the Docker volume:

- Stop the stack.
- Remove `./indexer_data/indexer.sqlite`.
- Start the stack again (`docker compose up --build`).

## Running locally

From `gateway-agent/` (indexer is fully isolated from `node_api`):

```bash
npm --prefix indexer install
npm --prefix indexer run indexer
```

To run the sanity check (health + metrics + metrics/state + token search + kubo version):

```bash
npm --prefix indexer run sanity
```

You can override the base URL used by the sanity script with:

```bash
INDEXER_BASE_URL=http://indexer:8790 npm --prefix indexer run sanity
```

You can optionally test a `/cid/:cid` response by providing a known CID:

```bash
SANITY_SAMPLE_CID=<some-cid> npm --prefix indexer run sanity
```

## HTTP endpoints

- `GET http://localhost:8790/health` → `{ "ok": true }`.
- `GET http://localhost:8790/metrics` → Prometheus metrics (no jobs triggered), including:
  - `indexer_pins_current`
  - `indexer_pins_last_refresh_timestamp`
  - `indexer_pins_last_refresh_duration_ms`
  - `indexer_pins_last_refresh_success`
  - `indexer_types_indexed_total`
  - `indexer_db_rows_cids`
  - `indexer_dirs_expanded_total`
  - `indexer_dir_expand_errors_total`
  - `indexer_ipfs_range_ignored_total`
  - `indexer_process_uptime_seconds`
  - `indexer_process_memory_rss_bytes`
  - `indexer_process_memory_heap_used_bytes`
  - `indexer_os_memory_total_bytes`
  - `indexer_os_memory_free_bytes`
  - `indexer_os_load1`, `indexer_os_load5`, `indexer_os_load15`
  - `indexer_http_requests_total{method, path, code}`
  - `indexer_http_request_duration_ms_sum{method, path}`
  - `indexer_http_request_duration_ms_count{method, path}`
  - `indexer_http_request_duration_ms_max{method, path}`
- `GET http://localhost:8790/metrics/state` → current metrics snapshot as JSON:
  - `pins_current`, `db_rows_cids`
  - `pins_last_refresh_ts`, `pins_last_refresh_duration_ms`, `pins_last_refresh_success`
  - `types_indexed_total`
- `GET http://localhost:8790/cid/:cid` → single `cids` row (with parsed `signals` and `tags`), including:
  - `present`, `present_source`
  - `mime`, `ext_guess`, `kind`, `confidence`, `source`
  - directory flags: `is_directory`, `expanded_at`, `expand_error`, `expand_depth`
- `GET "http://localhost:8790/search?token=...&kind=...&present=1&tag=...&limit=50"` → token search over `cid_tokens`, with optional filters:
  - `token` (repeated), `tag` (repeated)
  - `kind`, `mime`, `present=1|0`, `source`, `is_directory=1|0`, `present_source`
- `GET http://localhost:8790/children/:cid` → list of edges where `cid` is parent:
  - `{ parent: "<cid>", children: [{ cid, first_seen_at, last_seen_at }, ...] }`
- `GET http://localhost:8790/parents/:cid` → list of edges where `cid` is child:
  - `{ child: "<cid>", parents: [{ cid, first_seen_at, last_seen_at }, ...] }`

Example manual checks with `curl` (from the host):

```bash
curl -s http://localhost:8790/health
curl -s http://localhost:8790/metrics | head
curl -s http://localhost:8790/metrics/state
curl -s http://localhost:8790/cid/<some-cid>
curl -s "http://localhost:8790/search?token=pdf&present=1&limit=10"
curl -s http://localhost:8790/children/<some-directory-cid>
curl -s http://localhost:8790/parents/<some-child-cid>
```

### Field semantics: `source` vs `present_source`

- `source` refers to the type detector origin (`magic`, `container`, `magika`, `heuristic`, etc.).
- `present_source` describes how the row became `present=1`:
  - `'pinls'` for root CIDs coming directly from Kubo `pin/ls`.
  - `'expanded'` for child CIDs discovered by the directory expander.

The `/search` endpoint supports filtering by `present_source`, for example:

```bash
curl -s "http://localhost:8790/search?present=1&present_source=expanded&limit=50"
```

### HTTP range sampling and fallback

The type detector samples CIDs via HTTP `Range` requests against `IPFS_GATEWAY_BASE` whenever possible. If the gateway ignores `Range` (for example, returns `200` without a `Content-Range` header), the indexer:

- Marks the condition in detection signals (`signals.http.range_ignored=true`).
- Falls back to a capped read of the response body (never exceeding `MAX_TOTAL_BYTES` in total).
- Increments the Prometheus counter `indexer_ipfs_range_ignored_total`.

This keeps sampling bounded while still allowing type detection to proceed even when the gateway does not fully support range requests.

### Content analysis and tagging

For each non-directory CID, the indexer runs a content analysis pipeline and stores the result in `tags_json` (JSON) alongside low-level detection signals:

- **Type detection (`detectType.js`)**
  - Uses `file-type` magic bytes, container sniffing (PDF, ZIP/Office, CAR, HTML, etc.), optional Magika, and a fallback heuristic.
  - Normalizes to a small `kind` set: `image`, `html`, `text`, `doc`, `archive`, `ipld`, `package`, `unknown`.

- **Content classification (`contentSniffer.js`)**
  - HTML/text/doc:
    - Samples head/tail bytes, normalizes to plain ASCII text.
    - Extracts tokens (bag-of-words) with EN/FR stopwords.
    - Derives `topics` from the most important tokens.
    - Enriches with a text micro-model (`textTagger.js`, Xenova distilbert zero-shot) which outputs high-level labels (documentation, blog post, news article, legal terms, etc.), merged back into `tokens`/`topics`.
    - For document containers (PDF/DOCX/EPUB), performs a bounded full-text extraction to avoid missing keywords that are not present in the sampled bytes:
      - PDF prefers a range-based extractor (pdf.js over HTTP Range) and falls back to a full download when needed.
      - DOCX/EPUB use a bounded full download (up to `DOC_EXTRACT_MAX_BYTES`).
      - Limits are capped by `DOC_EXTRACT_TIMEOUT_MS`, `DOC_EXTRACT_MAX_BYTES`, `DOC_EXTRACT_MAX_CHARS`, and `PDF_EXTRACT_MAX_PAGES` / `EPUB_EXTRACT_MAX_FILES`.
  - Images:
    - Uses filename tokens.
    - Enriches with a CLIP zero-shot image classifier (`imageTagger.js`, `@xenova/transformers`) on a fixed vocabulary (UI/code/docs/diagrams/marketing, etc.).
    - Merges image tags into `tags_json.tokens`/`topics`.

- **Tag synthesis (`tags.js`)**
  - Produces deterministic tags such as:
    - `kind:<kind>`, `category:<media|document|package|unknown>`
    - `mime:<mime>`, `ext:<ext>`, `detected_by:<source>`, `confidence:<low|medium|high>`
    - `size_bucket:<xs|s|m|l|xl|xxl>`
    - `container:zip|pdf|car`, `office:*`, `ebook:epub`
    - `needs:metadata` (documents and images), `needs:ai_tags` (images only)

The combined `tags_json` structure is intended to be stable and low‑cardinality so it can be safely consumed by external search/ranking logic without additional migrations.

## Token Search (Scalable)

To support scalable keyword search (beyond a small “recent window”), the indexer maintains an inverted index table:

- `cid_tokens(token, cid, count)` populated by the `typeCrawler` from `tags_json.tokens` (capped by `SEARCH_TOKEN_INDEX_MAX_TOKENS`).

The `/search` endpoint accepts repeated `token` query params:

- `GET /search?token=hack&token=security&present=1&kind=doc`

When `token` is provided, results are ranked by token match count/score and then by recency. When `token` is not provided, `/search` behaves like a “recent content” feed (no substring/LIKE filtering).
