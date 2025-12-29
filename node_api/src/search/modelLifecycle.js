import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const SUPPORTED_LANGS = ['en', 'fr'];
const DEFAULT_LANG = 'en';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '..', '..');
const DATASETS_DIR = path.join(ROOT_DIR, 'datasets');
const MODELS_DIR = path.join(ROOT_DIR, 'models');

// In-memory cache of loaded models by language.
// Shape: { lang, intentModel, targetModel }
const MODELS_BY_LANG = new Map();

function resolveLang(rawLang) {
  const lang = String(rawLang || '').toLowerCase();
  if (SUPPORTED_LANGS.includes(lang)) return lang;
  return DEFAULT_LANG;
}

async function pathExists(p) {
  try {
    await fsp.access(p, fs.constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

async function sha256File(filePath) {
  const buf = await fsp.readFile(filePath);
  return crypto.createHash('sha256').update(buf).digest('hex');
}

function sha256Buffer(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

function makeCidFromHash(hashHex) {
  // Simple deterministic CID abstraction based on sha256 hex.
  return `cid-sha256-${hashHex}`;
}

function getDefaultLabelForModel(model) {
  if (!model || model.kind !== 'target') {
    return 'unknown';
  }
  return 'mixed';
}

async function readDataset(lang, kind) {
  const filePath = path.join(DATASETS_DIR, lang, `${kind}.txt`);
  const content = await fsp.readFile(filePath, 'utf8');
  const lines = content.split(/\r?\n/);
  const samples = [];

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    if (!trimmed.startsWith('__label__')) continue;
    const firstSpace = trimmed.indexOf(' ');
    if (firstSpace <= 0) continue;
    const labelPart = trimmed.slice(0, firstSpace);
    const textPart = trimmed.slice(firstSpace + 1).trim();
    if (!labelPart.startsWith('__label__')) continue;
    const label = labelPart.slice('__label__'.length);
    if (!label || !textPart) continue;
    samples.push({ label, text: textPart });
  }

  return samples;
}

function trainSimpleModel(lang, kind, samples) {
  const labelTotals = new Map();
  const wordLabelCounts = new Map(); // word -> Map<label, count>

  for (const sample of samples) {
    const label = sample.label;
    const text = sample.text;
    if (!label || !text) continue;

    labelTotals.set(label, (labelTotals.get(label) || 0) + 1);

    const words = String(text).split(/\s+/);
    const seenInSample = new Set();

    for (const rawWord of words) {
      const word = rawWord.trim();
      if (!word) continue;
      const key = word;
      // Avoid overweighting repeated words in same sample.
      const sampleKey = `${label}:${key}`;
      if (seenInSample.has(sampleKey)) continue;
      seenInSample.add(sampleKey);

      let perLabel = wordLabelCounts.get(key);
      if (!perLabel) {
        perLabel = new Map();
        wordLabelCounts.set(key, perLabel);
      }
      perLabel.set(label, (perLabel.get(label) || 0) + 1);
    }
  }

  const labels = Array.from(labelTotals.keys()).sort();

  return {
    kind,
    lang,
    labels,
    labelTotals,
    wordLabelCounts
  };
}

function serializeModel(model) {
  const labelTotals = {};
  for (const [label, count] of model.labelTotals.entries()) {
    labelTotals[label] = count;
  }
  const wordLabelCounts = {};
  for (const [word, perLabel] of model.wordLabelCounts.entries()) {
    const entry = {};
    for (const [label, count] of perLabel.entries()) {
      entry[label] = count;
    }
    wordLabelCounts[word] = entry;
  }
  const payload = {
    kind: model.kind,
    lang: model.lang,
    labels: model.labels,
    labelTotals,
    wordLabelCounts
  };
  return Buffer.from(JSON.stringify(payload), 'utf8');
}

function deserializeModel(buf) {
  const payload = JSON.parse(buf.toString('utf8'));
  const labelTotals = new Map();
  for (const [label, count] of Object.entries(payload.labelTotals || {})) {
    labelTotals.set(label, Number(count) || 0);
  }
  const wordLabelCounts = new Map();
  for (const [word, perLabelObj] of Object.entries(payload.wordLabelCounts || {})) {
    const perLabel = new Map();
    for (const [label, count] of Object.entries(perLabelObj || {})) {
      perLabel.set(label, Number(count) || 0);
    }
    wordLabelCounts.set(word, perLabel);
  }
  return {
    kind: payload.kind,
    lang: payload.lang,
    labels: Array.isArray(payload.labels) ? payload.labels.slice() : [],
    labelTotals,
    wordLabelCounts
  };
}

function predictFromModel(model, text) {
  if (!model || !text) {
    return {
      label: getDefaultLabelForModel(model),
      confidence: 0
    };
  }

  const scores = {};
  for (const label of model.labels) {
    scores[label] = 0;
  }

  const words = String(text).split(/\s+/);
  let any = false;
  for (const rawWord of words) {
    const word = rawWord.trim();
    if (!word) continue;
    const perLabel = model.wordLabelCounts.get(word);
    if (!perLabel) continue;
    any = true;
    for (const [label, count] of perLabel.entries()) {
      scores[label] = (scores[label] || 0) + count;
    }
  }

  if (!any) {
    return {
      label: getDefaultLabelForModel(model),
      confidence: 0
    };
  }

  let bestLabel = null;
  let bestScore = -1;
  let totalScore = 0;

  for (const label of model.labels) {
    const s = scores[label] || 0;
    totalScore += s;
    if (s > bestScore) {
      bestScore = s;
      bestLabel = label;
    }
  }

  if (!bestLabel || totalScore <= 0 || bestScore < 0) {
    return {
      label: getDefaultLabelForModel(model),
      confidence: 0
    };
  }

  const confidence = bestScore / totalScore;
  return { label: bestLabel, confidence };
}

export function getSearchModels(langRaw) {
  const lang = resolveLang(langRaw);
  return MODELS_BY_LANG.get(lang) || null;
}

export function predictIntent(langRaw, cleanText) {
  const models = getSearchModels(langRaw);
  if (!models || !models.intentModel) {
    return { label: 'unknown', confidence: 0 };
  }
  return predictFromModel(models.intentModel, cleanText);
}

export function predictTarget(langRaw, cleanText) {
  const models = getSearchModels(langRaw);
  if (!models || !models.targetModel) {
    return { label: 'mixed', confidence: 0 };
  }
  return predictFromModel(models.targetModel, cleanText);
}

export async function initSearchModels() {
  await fsp.mkdir(MODELS_DIR, { recursive: true });

  for (const lang of SUPPORTED_LANGS) {
    const langDatasetDir = path.join(DATASETS_DIR, lang);
    const intentDatasetPath = path.join(langDatasetDir, 'intent.txt');
    const targetDatasetPath = path.join(langDatasetDir, 'target.txt');

    const hasIntentDataset = await pathExists(intentDatasetPath);
    const hasTargetDataset = await pathExists(targetDatasetPath);

    if (!hasIntentDataset || !hasTargetDataset) {
      // eslint-disable-next-line no-console
      console.warn(`[search:init] lang=${lang} missing dataset files, skipping`);
      continue;
    }

    const intentDatasetHash = await sha256File(intentDatasetPath);
    const targetDatasetHash = await sha256File(targetDatasetPath);

    const langModelsDir = path.join(MODELS_DIR, lang);
    await fsp.mkdir(langModelsDir, { recursive: true });

    const intentModelPath = path.join(langModelsDir, 'intent.bin');
    const targetModelPath = path.join(langModelsDir, 'target.bin');
    const metadataPath = path.join(langModelsDir, 'metadata.json');

    const hasIntentModel = await pathExists(intentModelPath);
    const hasTargetModel = await pathExists(targetModelPath);

    let intentModel;
    let targetModel;

    if (hasIntentModel && hasTargetModel) {
      const intentBuf = await fsp.readFile(intentModelPath);
      const targetBuf = await fsp.readFile(targetModelPath);
      intentModel = deserializeModel(intentBuf);
      targetModel = deserializeModel(targetBuf);
    } else {
      const intentSamples = await readDataset(lang, 'intent');
      const targetSamples = await readDataset(lang, 'target');
      intentModel = trainSimpleModel(lang, 'intent', intentSamples);
      targetModel = trainSimpleModel(lang, 'target', targetSamples);

      const intentBuf = serializeModel(intentModel);
      const targetBuf = serializeModel(targetModel);

      await fsp.writeFile(intentModelPath, intentBuf);
      await fsp.writeFile(targetModelPath, targetBuf);
    }

    const intentModelBuf = await fsp.readFile(intentModelPath);
    const targetModelBuf = await fsp.readFile(targetModelPath);

    const intentModelHash = sha256Buffer(intentModelBuf);
    const targetModelHash = sha256Buffer(targetModelBuf);

    const intentCid = makeCidFromHash(intentModelHash);
    const targetCid = makeCidFromHash(targetModelHash);

    const createdAt = new Date().toISOString();

    const metadata = {
      lang,
      dataset: {
        intent_hash: intentDatasetHash,
        target_hash: targetDatasetHash
      },
      model: {
        intent_hash: intentModelHash,
        target_hash: targetModelHash,
        intent_cid: intentCid,
        target_cid: targetCid
      },
      created_at: createdAt
    };

    await fsp.writeFile(metadataPath, JSON.stringify(metadata, null, 2), 'utf8');

    MODELS_BY_LANG.set(lang, {
      lang,
      intentModel,
      targetModel
    });

    // eslint-disable-next-line no-console
    console.log(`[search:init] lang=${lang}`);
    // eslint-disable-next-line no-console
    console.log(`  dataset.intent=sha256:${intentDatasetHash}`);
    // eslint-disable-next-line no-console
    console.log(`  dataset.target=sha256:${targetDatasetHash}`);
    // eslint-disable-next-line no-console
    console.log(`  model.intent=cid:${intentCid}`);
    // eslint-disable-next-line no-console
    console.log(`  model.target=cid:${targetCid}`);
  }
}

export { resolveLang };
