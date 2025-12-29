import { dbGet, dbRun } from './db.js';

export async function getMetricsRow() {
  const row = await dbGet('SELECT * FROM metrics WHERE id = 1');
  if (!row) {
    return {
      pins_current: 0,
      pins_last_refresh_ts: null,
      pins_last_refresh_duration_ms: null,
      pins_last_refresh_success: null,
      types_indexed_total: 0,
      db_rows_cids: 0,
      dirs_expanded_total: 0,
      dir_expand_errors_total: 0,
      ipfs_range_ignored_total: 0
    };
  }
  return row;
}

export async function updatePinSyncMetrics({
  lastRefreshTs,
  durationMs,
  success
}) {
  await dbRun(
    `UPDATE metrics
     SET
       pins_last_refresh_ts = ?,
       pins_last_refresh_duration_ms = ?,
       pins_last_refresh_success = ?
     WHERE id = 1`,
    [lastRefreshTs ?? null, durationMs ?? null, success ? 1 : 0]
  );
}

export async function incrementTypesIndexed(count) {
  const delta = Number.isFinite(count) && count > 0 ? count : 0;
  if (!delta) return;
  await dbRun(
    `UPDATE metrics
     SET types_indexed_total = types_indexed_total + ?
     WHERE id = 1`,
    [delta]
  );
}

export async function getCountsSnapshot() {
  const pinsRow = await dbGet(
    'SELECT COUNT(*) AS n FROM cids WHERE present = 1'
  );
  const rowsRow = await dbGet('SELECT COUNT(*) AS n FROM cids');
  return {
    pins_current:
      pinsRow && typeof pinsRow.n === 'number' && pinsRow.n >= 0
        ? pinsRow.n
        : 0,
    db_rows_cids:
      rowsRow && typeof rowsRow.n === 'number' && rowsRow.n >= 0
        ? rowsRow.n
        : 0
  };
}

export async function getPrometheusMetricsSnapshot() {
  const [counts, row] = await Promise.all([
    getCountsSnapshot(),
    getMetricsRow()
  ]);

  return {
    pins_current: counts.pins_current,
    pins_last_refresh_timestamp: row?.pins_last_refresh_ts
      ? Math.floor(row.pins_last_refresh_ts / 1000)
      : 0,
    pins_last_refresh_duration_ms:
      row?.pins_last_refresh_duration_ms ?? 0,
    pins_last_refresh_success: row?.pins_last_refresh_success ?? 0,
    types_indexed_total: row?.types_indexed_total ?? 0,
    db_rows_cids: counts.db_rows_cids,
    dirs_expanded_total: row?.dirs_expanded_total ?? 0,
    dir_expand_errors_total: row?.dir_expand_errors_total ?? 0,
    ipfs_range_ignored_total: row?.ipfs_range_ignored_total ?? 0
  };
}

export async function incrementDirsExpanded(count) {
  const delta = Number.isFinite(count) && count > 0 ? count : 0;
  if (!delta) return;
  await dbRun(
    `UPDATE metrics
     SET dirs_expanded_total = dirs_expanded_total + ?
     WHERE id = 1`,
    [delta]
  );
}

export async function incrementDirExpandErrors(count) {
  const delta = Number.isFinite(count) && count > 0 ? count : 0;
  if (!delta) return;
  await dbRun(
    `UPDATE metrics
     SET dir_expand_errors_total = dir_expand_errors_total + ?
     WHERE id = 1`,
    [delta]
  );
}

export async function incrementIpfsRangeIgnored(count = 1) {
  const delta = Number.isFinite(count) && count > 0 ? count : 0;
  if (!delta) return;
  await dbRun(
    `UPDATE metrics
     SET ipfs_range_ignored_total = ipfs_range_ignored_total + ?
     WHERE id = 1`,
    [delta]
  );
}
