const requestCounters = new Map();
const latencyStats = new Map();

function normalizePath(path) {
  if (!path) return '/';
  const base = path.split('?')[0] || '/';

  if (base.startsWith('/cid/')) return '/cid/:cid';
  if (base.startsWith('/children/')) return '/children/:cid';
  if (base.startsWith('/parents/')) return '/parents/:cid';

  return base;
}

export function recordHttpRequest({ method, path, statusCode, durationMs }) {
  const m = String(method || 'GET').toUpperCase();
  const p = normalizePath(path || '/');
  const code = Number.isFinite(statusCode) ? statusCode : 0;

  const counterKey = `${m}|${p}|${code}`;
  const prevCount = requestCounters.get(counterKey) || 0;
  requestCounters.set(counterKey, prevCount + 1);

  const latencyKey = `${m}|${p}`;
  const prevLatency = latencyStats.get(latencyKey) || {
    count: 0,
    sumMs: 0,
    maxMs: 0
  };
  const safeDuration = Number.isFinite(durationMs) && durationMs >= 0 ? durationMs : 0;
  prevLatency.count += 1;
  prevLatency.sumMs += safeDuration;
  if (safeDuration > prevLatency.maxMs) {
    prevLatency.maxMs = safeDuration;
  }
  latencyStats.set(latencyKey, prevLatency);
}

export function getHttpMetricsSnapshot() {
  const counters = [];
  for (const [key, value] of requestCounters.entries()) {
    const [method, path, codeStr] = key.split('|');
    const code = Number.parseInt(codeStr, 10) || 0;
    counters.push({
      method,
      path,
      code,
      count: value
    });
  }

  const durations = [];
  for (const [key, stats] of latencyStats.entries()) {
    const [method, path] = key.split('|');
    durations.push({
      method,
      path,
      count: stats.count,
      sumMs: stats.sumMs,
      maxMs: stats.maxMs
    });
  }

  return { counters, durations };
}

