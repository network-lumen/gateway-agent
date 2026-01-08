import { CONFIG } from './config.js';
import { fetchWithTimeout, readResponseBodyLimited } from './utils.js';
import { logError } from './log.js';
import { tagImageWithClip } from './imageTagger.js';
import { tagTextWithModel } from './textTagger.js';

const SAMPLE_BYTES = 32 * 1024;

const STOPWORDS_EN = new Set([
  'the',
  'and',
  'for',
  'with',
  'this',
  'that',
  'from',
  'your',
  'you',
  'are',
  'have',
  'has',
  'not',
  'but',
  'about',
  'into',
  'over',
  'then',
  'than',
  'can',
  'use',
  'using',
  'file',
  'data',
  'content',
  'video',
  'image',
  'site',
  'page',
  'http',
  'https',
  'www',
  'com',
  'lumen',
  'ipfs',
  'gateway'
]);

const STOPWORDS_FR = new Set([
  'les',
  'des',
  'une',
  'dans',
  'avec',
  'pour',
  'par',
  'est',
  'sont',
  'sur',
  'pas',
  'plus',
  'que',
  'qui',
  'quoi',
  'vous',
  'nous',
  'eux',
  'elles',
  'lumen',
  'ipfs',
  'gateway'
]);

async function readSampleBytes(cid, sizeHint) {
  const url = new URL(`/ipfs/${cid}`, CONFIG.IPFS_GATEWAY_BASE).toString();
  const headRange = `bytes=0-${SAMPLE_BYTES - 1}`;

  let headBytes = Buffer.alloc(0);
  let tailBytes = Buffer.alloc(0);

  try {
    const resp = await fetchWithTimeout(
      url,
      { method: 'GET', headers: { Range: headRange } },
      { timeoutMs: CONFIG.REQUEST_TIMEOUT_MS, retries: 0 }
    );
    headBytes = await readResponseBodyLimited(resp, SAMPLE_BYTES);
  } catch (err) {
    logError('contentSniffer.readSampleBytes head error', cid, err?.message || err);
  }

  const size = typeof sizeHint === 'number' && sizeHint > 0 ? sizeHint : null;
  if (size && size > SAMPLE_BYTES) {
    const start = Math.max(size - SAMPLE_BYTES, 0);
    const tailRange = `bytes=${start}-${size - 1}`;
    try {
      const resp = await fetchWithTimeout(
        url,
        { method: 'GET', headers: { Range: tailRange } },
        { timeoutMs: CONFIG.REQUEST_TIMEOUT_MS, retries: 0 }
      );
      tailBytes = await readResponseBodyLimited(resp, SAMPLE_BYTES);
    } catch (err) {
      logError('contentSniffer.readSampleBytes tail error', cid, err?.message || err);
    }
  }

  return {
    headBytes,
    tailBytes,
    bytesRead: headBytes.length + tailBytes.length
  };
}

function normalizeText(bytes) {
  if (!bytes || !bytes.length) return '';
  try {
    const text = Buffer.isBuffer(bytes)
      ? bytes.toString('utf8')
      : Buffer.from(bytes).toString('utf8');
    // Remove accents first so that "éléments" -> "elements"
    const deaccented = text
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '');
    const ascii = deaccented.replace(/[^\x09\x0A\x0D\x20-\x7E]/g, ' ');
    const lower = ascii.toLowerCase();
    const noPunct = lower.replace(/[^a-z0-9\s]/g, ' ');
    return noPunct.replace(/\s+/g, ' ').trim();
  } catch {
    return '';
  }
}

function extractTokens(text, lang) {
  if (!text) return new Map();
  const tokens = new Map();
  const stopwords = lang === 'fr' ? STOPWORDS_FR : STOPWORDS_EN;

  const parts = String(text || '')
    .split(' ')
    .map((t) => t.trim())
    .filter((t) => t.length >= 3)
    .filter((t) => /^[a-z]+$/.test(t));

  for (const t of parts) {
    if (stopwords.has(t)) continue;
    const cur = tokens.get(t) || 0;
    tokens.set(t, cur + 1);
    if (tokens.size >= 256) break;
  }

  return tokens;
}

function deriveTopics(tokenCounts) {
  const entries = Array.from(tokenCounts.entries());
  entries.sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
  });

  const generic = new Set(['file', 'data', 'content']);
  const topics = [];
  for (const [token] of entries) {
    if (generic.has(token)) continue;
    topics.push(token);
    if (topics.length >= 5) break;
  }
  return topics;
}

function mapContentClassFromKind(kind, { isSubtitles } = {}) {
  const k = String(kind || '').toLowerCase().trim();
  if (k === 'html') return 'site';
  if (k === 'video') return 'video';
  if (k === 'image') return 'image';
  if (k === 'text' || k === 'doc') {
    if (isSubtitles) return 'video';
    return 'doc';
  }
  return 'doc';
}

function decodeHtmlEntities(input) {
  if (!input) return '';
  const s = String(input);
  const basic = s
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");

  const withNumeric = basic
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => {
      try {
        const code = Number.parseInt(hex, 16);
        if (!Number.isFinite(code) || code <= 0) return ' ';
        return String.fromCodePoint(code);
      } catch {
        return ' ';
      }
    })
    .replace(/&#([0-9]+);/g, (_, dec) => {
      try {
        const code = Number.parseInt(dec, 10);
        if (!Number.isFinite(code) || code <= 0) return ' ';
        return String.fromCodePoint(code);
      } catch {
        return ' ';
      }
    });

  return withNumeric;
}

function getHtmlAttr(tag, attrName) {
  if (!tag || !attrName) return null;
  const re = new RegExp(
    `${String(attrName).replace(/[-/\\\\^$*+?.()|[\\]{}]/g, '\\\\$&')}\\s*=\\s*(\"([^\"]*)\"|'([^']*)'|([^\\s\"'>]+))`,
    'i'
  );
  const m = String(tag).match(re);
  if (!m) return null;
  return (m[2] || m[3] || m[4] || '').trim() || null;
}

function stripHtmlTags(input) {
  if (!input) return '';
  return String(input).replace(/<[^>]+>/g, ' ');
}

function extractHtmlMetadata(html) {
  const raw = String(html || '');
  let title = null;
  let description = null;

  const titleMatch = raw.match(/<title[^>]*>([\s\S]{0,512}?)<\/title>/i);
  if (titleMatch && titleMatch[1]) {
    const cleaned = decodeHtmlEntities(stripHtmlTags(titleMatch[1]))
      .replace(/\s+/g, ' ')
      .trim();
    if (cleaned) title = cleaned.slice(0, 200);
  }

  const metaTags = raw.match(/<meta\b[^>]*>/gi) || [];
  for (const tag of metaTags) {
    const name =
      (getHtmlAttr(tag, 'name') || getHtmlAttr(tag, 'property') || '').trim().toLowerCase();
    const content = getHtmlAttr(tag, 'content');
    if (!name || !content) continue;

    const cleaned = decodeHtmlEntities(content).replace(/\s+/g, ' ').trim();
    if (!cleaned) continue;

    if (!description && (name === 'description' || name === 'og:description' || name === 'twitter:description')) {
      description = cleaned.slice(0, 300);
    }
    if (!title && (name === 'og:title' || name === 'twitter:title')) {
      title = cleaned.slice(0, 200);
    }

    if (title && description) break;
  }

  return { title, description };
}

function extractReadableTextFromHtml(html) {
  const raw = String(html || '');

  const withoutBlocks = raw
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<svg[\s\S]*?<\/svg>/gi, ' ');

  const stripped = stripHtmlTags(withoutBlocks);
  const decoded = decodeHtmlEntities(stripped);
  // Second pass: if the page contains encoded HTML (e.g., code snippets),
  // decoding can reintroduce tags; strip again to avoid noisy tokens like "div", "class", etc.
  const strippedAgain = stripHtmlTags(decoded);
  return strippedAgain.replace(/\s+/g, ' ').trim();
}

async function analyzeHtmlFromText(headText, tailText, lang, bytesRead) {
  const html = `${headText} ${tailText}`;
  const { title, description } = extractHtmlMetadata(html);
  const readable = extractReadableTextFromHtml(html);
  const modelText = [title, description, readable].filter(Boolean).join(' ').trim();
  const normalizedFull = normalizeText(Buffer.from(modelText || readable, 'utf8'));
  const tokenCounts = extractTokens(normalizedFull, lang);
  const derived = deriveTopics(tokenCounts);
  const tokens = {};
  for (const [t, c] of tokenCounts.entries()) {
    if (!t) continue;
    tokens[t] = c;
  }

  let topics = [];

  try {
    const aiTags = await tagTextWithModel(modelText || readable || normalizedFull);
    if (aiTags && aiTags.tokens) {
      for (const [k, v] of Object.entries(aiTags.tokens)) {
        const key = String(k || '').trim().toLowerCase();
        const val = Number(v);
        if (!key || !Number.isFinite(val) || val <= 0) continue;
        tokens[key] = (tokens[key] || 0) + val;
      }
    }
    if (aiTags && Array.isArray(aiTags.topics)) {
      for (const t of aiTags.topics) {
        const key = String(t || '').trim().toLowerCase();
        if (!key) continue;
        if (!topics.includes(key)) topics.push(key);
      }
    }
  } catch (err) {
    logError('contentSniffer.analyzeHtml text tag enrichment error', err?.message || err);
  }

  for (const t of derived) {
    const key = String(t || '').trim().toLowerCase();
    if (!key) continue;
    if (!topics.includes(key)) topics.push(key);
  }

  return {
    topics,
    tokens,
    title: title || null,
    description: description || null,
    content_class: 'site',
    lang,
    confidence: 0.75,
    signals: {
      from: ['html:content'],
      bytes_read: bytesRead
    }
  };
}

async function analyzeTextFromSample(headBytes, lang, bytesRead) {
  const raw = Buffer.isBuffer(headBytes)
    ? headBytes.toString('utf8')
    : Buffer.from(headBytes).toString('utf8');
  const lines = raw.split(/\r?\n/).slice(0, 20);
  const joined = lines.join(' ');
  const modelText = joined.replace(/\s+/g, ' ').trim();
  const normalized = normalizeText(Buffer.from(joined, 'utf8'));
  const tokenCounts = extractTokens(normalized, lang);
  const derived = deriveTopics(tokenCounts);
  const tokens = {};
  for (const [t, c] of tokenCounts.entries()) {
    if (!t) continue;
    tokens[t] = c;
  }

  let topics = [];

  try {
    const aiTags = await tagTextWithModel(modelText || normalized);
    if (aiTags && aiTags.tokens) {
      for (const [k, v] of Object.entries(aiTags.tokens)) {
        const key = String(k || '').trim().toLowerCase();
        const val = Number(v);
        if (!key || !Number.isFinite(val) || val <= 0) continue;
        tokens[key] = (tokens[key] || 0) + val;
      }
    }
    if (aiTags && Array.isArray(aiTags.topics)) {
      for (const t of aiTags.topics) {
        const key = String(t || '').trim().toLowerCase();
        if (!key) continue;
        if (!topics.includes(key)) topics.push(key);
      }
    }
  } catch (err) {
    logError('contentSniffer.analyzeText text tag enrichment error', err?.message || err);
  }

  for (const t of derived) {
    const key = String(t || '').trim().toLowerCase();
    if (!key) continue;
    if (!topics.includes(key)) topics.push(key);
  }

  const isSubtitles = lines.some((line) => line.includes('-->'));
  const content_class = mapContentClassFromKind('text', { isSubtitles });
  const confidence = 0.6;

  return {
    topics,
    tokens,
    content_class,
    lang,
    confidence,
    signals: {
      from: ['text:head'],
      bytes_read: bytesRead
    }
  };
}

async function analyzeImage(detection, bytesRead, lang, cidForTags) {
  const baseTokens = new Map();

  if (detection.filename) {
    const norm = normalizeText(
      Buffer.from(String(detection.filename), 'utf8')
    );
    const fileTokens = extractTokens(norm, lang);
    for (const [t, c] of fileTokens.entries()) {
      if (!t) continue;
      baseTokens.set(t, (baseTokens.get(t) || 0) + c);
    }
  }

  const topics = deriveTopics(baseTokens);
  const tokens = {};
  for (const [t, c] of baseTokens.entries()) {
    if (!t) continue;
    tokens[t] = c;
  }

  try {
    if (cidForTags) {
      const aiTags = await tagImageWithClip(cidForTags, detection);
      if (aiTags && aiTags.tokens) {
        for (const [k, v] of Object.entries(aiTags.tokens)) {
          const key = String(k || '').trim().toLowerCase();
          const val = Number(v);
          if (!key || !Number.isFinite(val) || val <= 0) continue;
          tokens[key] = (tokens[key] || 0) + val;
        }
      }
      if (aiTags && Array.isArray(aiTags.topics)) {
        for (const t of aiTags.topics) {
          const key = String(t || '').trim().toLowerCase();
          if (!key) continue;
          if (!topics.includes(key)) topics.push(key);
        }
      }
    }
  } catch (err) {
    logError('contentSniffer.analyzeImage tag enrichment error', cidForTags || '', err?.message || err);
  }

  const content_class = 'image';
  const confidence = 0.4;
  const fromSignals = ['kind:image'];

  return {
    topics,
    tokens,
    content_class,
    lang,
    confidence,
    signals: {
      from: fromSignals,
      bytes_read: bytesRead
    }
  };
}

function analyzeVideo(detection, bytesRead, lang) {
  const tokensMap = new Map();
  tokensMap.set('video', (tokensMap.get('video') || 0) + 1);

  const containerType = detection.signals?.container?.type;
  if (containerType) {
    const key = String(containerType || '').toLowerCase().trim();
    if (key) tokensMap.set(key, (tokensMap.get(key) || 0) + 1);
  }

  if (detection.filename) {
    const norm = normalizeText(
      Buffer.from(String(detection.filename), 'utf8')
    );
    const fileTokens = extractTokens(norm, lang);
    for (const [t, c] of fileTokens.entries()) {
      if (!t) continue;
      tokensMap.set(t, (tokensMap.get(t) || 0) + c);
    }
  }

  const ext = String(detection.ext_guess || '').toLowerCase().trim();
  if (ext === 'mp4') tokensMap.set('mp4', (tokensMap.get('mp4') || 0) + 1);
  if (ext === 'm3u8') tokensMap.set('hls', (tokensMap.get('hls') || 0) + 1);

  const content_class = 'video';
  const confidence = 0.75;
  const fromSignals = ['kind:video'];

  const topics = deriveTopics(tokensMap);
  const tokens = {};
  for (const [t, c] of tokensMap.entries()) {
    if (!t) continue;
    tokens[t] = c;
  }

  return {
    topics,
    tokens,
    content_class,
    lang,
    confidence,
    signals: {
      from: fromSignals,
      bytes_read: bytesRead
    }
  };
}

export async function analyzeContentForCid(cid, detection) {
  const kind = detection.kind || 'unknown';
  const size = detection.size;
  const lang = 'en';

  if (kind !== 'html' && kind !== 'text' && kind !== 'doc' && kind !== 'image' && kind !== 'video') {
    return null;
  }

  const sample = await readSampleBytes(cid, size);
  const bytesRead = sample.bytesRead;

  if (!sample.headBytes.length && !sample.tailBytes.length) {
    return null;
  }

  if (kind === 'html') {
    const headText = sample.headBytes.toString('utf8');
    const tailText =
      sample.tailBytes && sample.tailBytes.length
        ? sample.tailBytes.toString('utf8')
        : '';
    return analyzeHtmlFromText(headText, tailText, lang, bytesRead);
  }

  if (kind === 'text' || kind === 'doc') {
    return analyzeTextFromSample(sample.headBytes, lang, bytesRead);
  }

  if (kind === 'image') {
    return analyzeImage(detection, bytesRead, lang, cid);
  }

  if (kind === 'video') {
    return analyzeVideo(detection, bytesRead, lang);
  }

  return null;
}

export { readSampleBytes, normalizeText, extractTokens, deriveTopics, mapContentClassFromKind };
