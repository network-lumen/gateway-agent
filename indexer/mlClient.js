import { Worker } from 'node:worker_threads';
import { CONFIG } from './config.js';
import { logError } from './log.js';

const START_FAILURE_BACKOFF_MS = 30_000;

let worker = null;
let workerInitPromise = null;
let lastStartFailureAtMs = 0;

let nextId = 1;
const pending = new Map();

function makeError(message, code, cause) {
  const err = new Error(message);
  if (code) err.code = code;
  if (cause) err.cause = cause;
  return err;
}

function rejectAllPending(err) {
  for (const [id, entry] of pending.entries()) {
    pending.delete(id);
    try {
      clearTimeout(entry.timeout);
    } catch {
      // ignore
    }
    try {
      entry.reject(err);
    } catch {
      // ignore
    }
  }
}

async function resetWorker(reason, err) {
  const w = worker;
  worker = null;
  workerInitPromise = null;

  const error =
    err instanceof Error
      ? err
      : makeError(`ml worker reset: ${String(reason || 'unknown')}`, 'ML_WORKER_RESET');

  rejectAllPending(error);

  try {
    if (w) {
      await w.terminate();
    }
  } catch {
    // ignore
  }
}

function handleMessage(msg) {
  const id = msg && typeof msg.id === 'number' ? msg.id : null;
  if (id === null) return;

  const entry = pending.get(id);
  if (!entry) return;
  pending.delete(id);

  try {
    clearTimeout(entry.timeout);
  } catch {
    // ignore
  }

  if (msg && msg.ok === true) {
    entry.resolve(msg.result ?? null);
    return;
  }

  const errInfo = msg && msg.error ? msg.error : null;
  const message =
    errInfo && typeof errInfo.message === 'string'
      ? errInfo.message
      : 'ml worker error';
  const code =
    errInfo && typeof errInfo.code === 'string' ? errInfo.code : 'ML_WORKER_ERROR';

  entry.reject(makeError(message, code));
}

async function startWorker() {
  if (!CONFIG.ML_WORKER_ENABLE) {
    throw makeError('ml worker disabled', 'ML_WORKER_DISABLED');
  }

  const now = Date.now();
  if (lastStartFailureAtMs && now - lastStartFailureAtMs < START_FAILURE_BACKOFF_MS) {
    throw makeError('ml worker start backoff', 'ML_WORKER_BACKOFF');
  }

  try {
    const w = new Worker(new URL('./mlWorker.js', import.meta.url), {
      type: 'module'
    });

    w.on('message', handleMessage);
    w.on('error', (e) => {
      logError('mlClient: worker error', e?.message || e);
      void resetWorker('error', e);
    });
    w.on('exit', (code) => {
      if (code === 0) {
        void resetWorker('exit');
        return;
      }
      const e = makeError(`ml worker exited with code ${code}`, 'ML_WORKER_EXIT');
      logError('mlClient: worker exit', e.message);
      void resetWorker('exit', e);
    });

    worker = w;
    return w;
  } catch (err) {
    lastStartFailureAtMs = Date.now();
    throw err;
  }
}

async function getWorker() {
  if (worker) return worker;
  if (workerInitPromise) return workerInitPromise;

  workerInitPromise = startWorker().catch((err) => {
    workerInitPromise = null;
    throw err;
  });

  return workerInitPromise;
}

async function callWorker(method, payload, { timeoutMs } = {}) {
  const w = await getWorker();
  const id = nextId;
  nextId += 1;

  const effectiveTimeout =
    typeof timeoutMs === 'number' && Number.isFinite(timeoutMs) && timeoutMs > 0
      ? Math.floor(timeoutMs)
      : CONFIG.ML_WORKER_TASK_TIMEOUT_MS;

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      pending.delete(id);
      const e = makeError('ml worker timeout', 'ML_WORKER_TIMEOUT');
      reject(e);
      void resetWorker('timeout', e);
    }, effectiveTimeout);

    pending.set(id, { resolve, reject, timeout });
    try {
      w.postMessage({ id, method, payload });
    } catch (err) {
      pending.delete(id);
      try {
        clearTimeout(timeout);
      } catch {
        // ignore
      }
      reject(err);
      void resetWorker('postMessage', err);
    }
  });
}

export async function mlTagText(text) {
  return callWorker('tagText', { text });
}

export async function mlTagImage(cid, detection) {
  return callWorker('tagImage', { cid, detection });
}

