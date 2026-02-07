import { CONFIG } from './config.js';
import { logError } from './log.js';
import { mlTagImage } from './mlClient.js';

let fallbackPromise = null;

async function fallbackTagImage(cid, detection) {
  if (!fallbackPromise) {
    fallbackPromise = import('./imageTaggerInProcess.js');
  }
  const mod = await fallbackPromise;
  return mod.tagImageWithClip(cid, detection);
}

export async function tagImageWithClip(cid, detection) {
  try {
    if (!CONFIG.IMAGE_TAGGER_ENABLE) return null;
    if (!CONFIG.ML_WORKER_ENABLE) return fallbackTagImage(cid, detection);

    try {
      return await mlTagImage(cid, detection);
    } catch (err) {
      logError('imageTagger: worker failed, falling back', cid || '', err?.message || err);
      return fallbackTagImage(cid, detection);
    }
  } catch (err) {
    logError('imageTagger.tagImageWithClip error', cid || '', err?.message || err);
    return null;
  }
}

