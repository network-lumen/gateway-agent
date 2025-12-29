let queue = [];
let running = false;
let nextId = 1;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randomDelayMs() {
  const min = 100;
  const max = 5000;
  return min + Math.floor(Math.random() * (max - min + 1));
}

export function enqueueIngestJob(fn, meta = {}) {
  const jobId = `ingest-${Date.now()}-${nextId++}`;
  queue.push({ id: jobId, fn, meta: { ...meta } });
  if (!running) {
    running = true;
    void worker();
  }
  return jobId;
}

async function worker() {
  while (queue.length > 0) {
    const job = queue.shift();
    const delay = randomDelayMs();
    try {
      await sleep(delay);
      await job.fn();
      try {
        // eslint-disable-next-line no-console
        console.log('[ingestQueue] job completed', {
          jobId: job.id,
          bytes: job.meta.bytes ?? null
        });
      } catch {}
    } catch (err) {
      try {
        // eslint-disable-next-line no-console
        console.error('[ingestQueue] job failed', {
          jobId: job.id,
          error: err && err.message ? String(err.message) : 'unknown_error'
        });
      } catch {}
    }
  }
  running = false;
}
