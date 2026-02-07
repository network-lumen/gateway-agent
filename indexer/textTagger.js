import { CONFIG } from './config.js';
import { logError } from './log.js';
import { mlTagText } from './mlClient.js';

let fallbackPromise = null;

async function fallbackTagText(text) {
  if (!fallbackPromise) {
    fallbackPromise = import('./textTaggerInProcess.js');
  }
  const mod = await fallbackPromise;
  return mod.tagTextWithModel(text);
}

export async function tagTextWithModel(text) {
  try {
    if (!CONFIG.TEXT_TAGGER_ENABLE) return null;
    if (!CONFIG.ML_WORKER_ENABLE) return fallbackTagText(text);

    try {
      return await mlTagText(text);
    } catch (err) {
      logError('textTagger: worker failed, falling back', err?.message || err);
      return fallbackTagText(text);
    }
  } catch (err) {
    logError('textTagger.tagTextWithModel error', err?.message || err);
    return null;
  }
}

