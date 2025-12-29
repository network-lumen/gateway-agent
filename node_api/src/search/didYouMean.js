import { searchCidsSimple } from '../lib/indexerClient.js';

let vocabLoaded = false;
let vocabEntries = [];

function normalizeText(input) {
  if (!input) return '';
  try {
    const raw = String(input || '').toLowerCase();
    const normalized = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    const stripped = normalized.replace(/[^a-z0-9\s?]/g, ' ');
    return stripped.replace(/\s+/g, ' ').trim();
  } catch {
    return String(input || '').trim().toLowerCase();
  }
}

async function loadVocabFromIndexer() {
  const entries = [];
  const seen = new Set();

  const PAGE_SIZE = 200;
  const MAX_PAGES = 5;
  const MAX_TERMS = 10000;

  for (let page = 0; page < MAX_PAGES; page += 1) {
    const offset = page * PAGE_SIZE;
    let result;
    try {
      result = await searchCidsSimple(
        { present: 1, limit: PAGE_SIZE, offset },
        { timeoutMs: 2000 }
      );
    } catch {
      break;
    }

    if (!result || !result.ok || !Array.isArray(result.items) || !result.items.length) {
      break;
    }

    for (const item of result.items) {
      if (!item || !item.tags_json) continue;

      let tags;
      try {
        tags =
          typeof item.tags_json === 'string'
            ? JSON.parse(item.tags_json)
            : item.tags_json;
      } catch {
        continue;
      }
      if (!tags || (typeof tags !== 'object' && !Array.isArray(tags))) continue;

      const tokens = Array.isArray(tags.tokens) ? tags.tokens : [];
      const topics = Array.isArray(tags.topics) ? tags.topics : [];

      for (const rawValue of [...tokens, ...topics]) {
        const raw = String(rawValue || '').trim();
        if (!raw) continue;

        const norm = normalizeText(raw);
        if (!norm || norm.length < 3) continue;
        if (norm.length > 64) continue;

        if (seen.has(norm)) continue;
        seen.add(norm);
        entries.push({ raw, norm });

        if (entries.length >= MAX_TERMS) {
          return entries;
        }
      }
    }
  }

  return entries;
}

async function loadVocab() {
  if (vocabLoaded) return vocabEntries;

  let entries = [];
  try {
    entries = await loadVocabFromIndexer();
  } catch {
    entries = [];
  }

  vocabEntries = entries;
  vocabLoaded = true;
  return vocabEntries;
}

function levenshtein(a, b) {
  const s = a;
  const t = b;
  const n = s.length;
  const m = t.length;
  if (n === 0) return m;
  if (m === 0) return n;

  const prev = new Array(m + 1);
  const curr = new Array(m + 1);

  for (let j = 0; j <= m; j += 1) {
    prev[j] = j;
  }

  for (let i = 1; i <= n; i += 1) {
    curr[0] = i;
    const si = s.charCodeAt(i - 1);
    for (let j = 1; j <= m; j += 1) {
      const cost = si === t.charCodeAt(j - 1) ? 0 : 1;
      const del = prev[j] + 1;
      const ins = curr[j - 1] + 1;
      const sub = prev[j - 1] + cost;
      let val = del;
      if (ins < val) val = ins;
      if (sub < val) val = sub;
      curr[j] = val;
    }
    for (let j = 0; j <= m; j += 1) {
      prev[j] = curr[j];
    }
  }

  return prev[m];
}

export async function suggestDidYouMean(rawQuery) {
  const vocab = await loadVocab();
  if (!vocab.length) return null;

  const normQ = normalizeText(rawQuery);
  if (!normQ || normQ.length < 3) return null;

  let best = null;
  let bestDist = Infinity;

  for (const entry of vocab) {
    const candidate = entry.norm;
    if (!candidate || candidate === normQ) continue;

    const maxLen = Math.max(normQ.length, candidate.length);
    // Skip obviously too far strings to save work
    if (Math.abs(normQ.length - candidate.length) > Math.ceil(maxLen * 0.6)) {
      continue;
    }

    const dist = levenshtein(normQ, candidate);
    if (dist < bestDist) {
      bestDist = dist;
      best = entry;
    }
  }

  if (!best) return null;

  const maxLen = Math.max(normQ.length, best.norm.length);
  const ratio = maxLen > 0 ? bestDist / maxLen : 1;

  // Require reasonably close match (edit distance small relative to length)
  if (bestDist <= 2 || ratio <= 0.3) {
    return best.raw;
  }

  return null;
}
