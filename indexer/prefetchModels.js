import fs from 'node:fs';
import { CONFIG } from './config.js';
import { log, logError } from './log.js';

async function prefetchOne(task, model, opts) {
  try {
    const { pipeline } = await import('@xenova/transformers');
    await pipeline(task, model, opts);
    log('[prefetch] ok', { task, model });
    return true;
  } catch (err) {
    logError('[prefetch] failed', { task, model, error: err?.message || String(err) });
    return false;
  }
}

async function main() {
  const { env } = await import('@xenova/transformers');

  try {
    if (CONFIG.TRANSFORMERS_CACHE_DIR) {
      fs.mkdirSync(CONFIG.TRANSFORMERS_CACHE_DIR, { recursive: true });
      env.cacheDir = CONFIG.TRANSFORMERS_CACHE_DIR;
    }
    if (CONFIG.TRANSFORMERS_LOCAL_FILES_ONLY) {
      env.allowRemoteModels = false;
    }
  } catch {
    // ignore cache FS errors
  }

  const opts = {
    revision: CONFIG.TRANSFORMERS_REVISION,
    local_files_only: CONFIG.TRANSFORMERS_LOCAL_FILES_ONLY
  };
  if (CONFIG.TRANSFORMERS_CACHE_DIR) {
    opts.cache_dir = CONFIG.TRANSFORMERS_CACHE_DIR;
  }

  let ok = true;
  if (CONFIG.TEXT_TAGGER_ENABLE) {
    ok = (await prefetchOne('zero-shot-classification', CONFIG.TEXT_TAGGER_MODEL, opts)) && ok;
  } else {
    log('[prefetch] text tagger disabled, skipping');
  }

  if (CONFIG.IMAGE_TAGGER_ENABLE) {
    ok = (await prefetchOne('zero-shot-image-classification', CONFIG.IMAGE_TAGGER_MODEL, opts)) && ok;
  } else {
    log('[prefetch] image tagger disabled, skipping');
  }

  if (!ok) process.exitCode = 1;
}

main().catch((err) => {
  logError('[prefetch] fatal', err?.message || err);
  process.exitCode = 1;
});

