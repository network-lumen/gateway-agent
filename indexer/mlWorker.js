import { parentPort } from 'node:worker_threads';

if (!parentPort) {
  throw new Error('mlWorker must be started as a worker thread');
}

let opQueue = Promise.resolve();
let textModPromise = null;
let imageModPromise = null;

async function tagText(text) {
  if (!textModPromise) {
    textModPromise = import('./textTaggerInProcess.js');
  }
  const mod = await textModPromise;
  return mod.tagTextWithModel(text);
}

async function tagImage(cid, detection) {
  if (!imageModPromise) {
    imageModPromise = import('./imageTaggerInProcess.js');
  }
  const mod = await imageModPromise;
  return mod.tagImageWithClip(cid, detection);
}

async function handleMessage(msg) {
  const id = msg && typeof msg.id === 'number' ? msg.id : null;
  if (id === null) return;

  const method = msg && typeof msg.method === 'string' ? msg.method : '';
  const payload = msg && msg.payload && typeof msg.payload === 'object' ? msg.payload : {};

  try {
    let result = null;

    if (method === 'tagText') {
      result = await tagText(payload.text);
    } else if (method === 'tagImage') {
      result = await tagImage(payload.cid, payload.detection);
    } else {
      const err = new Error('unknown mlWorker method');
      err.code = 'UNKNOWN_METHOD';
      throw err;
    }

    parentPort.postMessage({ id, ok: true, result: result ?? null });
  } catch (err) {
    const message = err?.message ? String(err.message) : String(err);
    const code = typeof err?.code === 'string' ? err.code : 'ML_WORKER_ERROR';
    parentPort.postMessage({ id, ok: false, error: { message, code } });
  }
}

parentPort.on('message', (msg) => {
  opQueue = opQueue.then(
    () => handleMessage(msg),
    () => handleMessage(msg)
  );
});

