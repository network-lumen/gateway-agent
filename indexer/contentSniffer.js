import { CONFIG } from './config.js';
import { fetchWithTimeout, readResponseBodyLimited } from './utils.js';
import { logError } from './log.js';
import { tagImageWithClip } from './imageTagger.js';
import { tagTextWithModel } from './textTagger.js';
import {
  extractDocxTextFromBytes,
  extractEpubTextFromBytes,
  extractPdfTextFromBytes,
  extractPdfTextFromUrl
} from './docExtractors.js';

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

async function readFullBytes(cid, maxBytes) {
  const limit = typeof maxBytes === 'number' && maxBytes > 0 ? maxBytes : 0;
  if (!limit) return null;

  const url = new URL(`/ipfs/${cid}`, CONFIG.IPFS_GATEWAY_BASE).toString();

  try {
    const resp = await fetchWithTimeout(
      url,
      { method: 'GET' },
      {
        timeoutMs: CONFIG.DOC_EXTRACT_TIMEOUT_MS || CONFIG.REQUEST_TIMEOUT_MS,
        retries: CONFIG.DOC_EXTRACT_RETRIES ?? 0
      }
    );
    if (!resp.ok) return null;

    const lenHeader = resp.headers?.get ? resp.headers.get('content-length') : null;
    const contentLen = lenHeader ? Number.parseInt(String(lenHeader), 10) : null;
    if (Number.isFinite(contentLen) && contentLen > limit) {
      return { tooLarge: true, bytes: null, bytesRead: 0 };
    }

    const bytes = await readResponseBodyLimited(resp, limit);
    // If we hit the cap, treat it as truncated and avoid parsing formats that require full bytes.
    const lengthKnownAndWithinLimit =
      Number.isFinite(contentLen) && contentLen >= 0 && contentLen <= limit;
    if (bytes.length >= limit && !lengthKnownAndWithinLimit) {
      return { tooLarge: true, bytes: null, bytesRead: bytes.length };
    }

    return { tooLarge: false, bytes, bytesRead: bytes.length };
  } catch (err) {
    logError('contentSniffer.readFullBytes error', cid, err?.message || err);
    return null;
  }
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

function looksLikeUsefulHumanText(value) {
  const s = String(value || '').trim();
  if (!s) return false;
  // Require at least some letters to avoid numeric junk like "123 456".
  if (!/[a-z]/i.test(s)) return false;
  // Avoid lines that are overwhelmingly punctuation.
  const letters = (s.match(/[a-z]/gi) || []).length;
  const nonSpace = (s.match(/\S/g) || []).length;
  if (!nonSpace) return false;
  return letters / nonSpace >= 0.15;
}

function looksLikePdfInternalText(rawText) {
  const raw = String(rawText || '');
  if (!raw) return false;
  const sample = raw.slice(0, 8192).toLowerCase();

  // xref-like tables: "0003756830 00000 n"
  const xrefLineMatches = sample.match(/\b\d{6,12}\s+\d{5}\s+[nf]\b/g) || [];
  if (xrefLineMatches.length >= 3) {
    const nonXref = sample.replace(/[0-9nf\s]/g, '');
    if (nonXref.length <= 10) return true;
  }

  const hasObjDecl = /\b\d+\s+\d+\s+obj\b/.test(sample);
  const hasEndobj = sample.includes('endobj');
  const hasXref = sample.includes('xref');
  const hasTrailer = sample.includes('trailer');
  const hasStream = sample.includes('stream');
  const hasEndstream = sample.includes('endstream');

  let score = 0;
  if (hasObjDecl) score += 2;
  if (hasEndobj) score += 1;
  if (hasXref) score += 1;
  if (hasTrailer) score += 1;
  if (hasStream) score += 1;
  if (hasEndstream) score += 1;

  if (score >= 4) return true;

  // Dictionary-heavy signature without explicit object header.
  const hasFlate = sample.includes('flatedecode');
  const hasXobject = sample.includes('xobject');
  const hasColorspace = sample.includes('colorspace');
  const hasBits = sample.includes('bitspercomponent');
  const hasMediabox = sample.includes('mediabox');
  const hasCropbox = sample.includes('cropbox');
  const hasResources = sample.includes('resources');
  const hasFont = sample.includes('font');

  let dictScore = 0;
  if (hasFlate) dictScore += 1;
  if (hasXobject) dictScore += 1;
  if (hasColorspace) dictScore += 1;
  if (hasBits) dictScore += 1;
  if (hasMediabox) dictScore += 1;
  if (hasCropbox) dictScore += 1;
  if (hasResources) dictScore += 1;
  if (hasFont) dictScore += 1;

  const hasStreamish = hasStream && hasEndstream;
  return hasStreamish && dictScore >= 3;
}

function looksLikeZipContainerText(rawText) {
  const raw = String(rawText || '');
  if (!raw) return false;

  // ZIP local file header signature ("PK\u0003\u0004") rendered as "PK" + control chars.
  if (!raw.startsWith('PK')) return false;

  const sample = raw.slice(0, 8192).toLowerCase();

  // EPUB / Office containers often leak these strings early in the stream when decoded as UTF-8.
  if (sample.includes('meta-inf/container.xml')) return true;
  if (sample.includes('mimetypeapplication/epub+zip')) return true;
  if (sample.includes('[content_types].xml')) return true;
  if (sample.includes('_rels/.rels')) return true;
  if (sample.includes('word/')) return true;
  if (sample.includes('ppt/')) return true;
  if (sample.includes('xl/')) return true;

  return false;
}

function extractTextTitleAndDescription(
  rawText,
  { maxTitle = 120, maxDescription = 800, markdownTitleOnly = false } = {}
) {
  const raw = String(rawText || '');
  const lines = raw.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  if (!lines.length) return { title: null, description: null };

  let title = null;

  // Prefer markdown heading as title.
  for (const line of lines.slice(0, 40)) {
    const m = /^#{1,6}\s+(.+)$/.exec(line);
    if (m && looksLikeUsefulHumanText(m[1])) {
      title = m[1].trim();
      break;
    }
  }

  if (!title && !markdownTitleOnly) {
    for (const line of lines.slice(0, 60)) {
      if (!looksLikeUsefulHumanText(line)) continue;
      title = line;
      break;
    }
  }

  if (title) {
    title = title.replace(/\s+/g, ' ').trim();
    if (title.length > maxTitle) title = `${title.slice(0, maxTitle).trimEnd()}…`;
  }

  // Description: first "useful" chunk, skipping a title-like line when possible.
  const descParts = [];
  const titleNormalized = title ? title.replace(/\s+/g, ' ').trim().toLowerCase() : null;

  for (const line of lines.slice(0, 120)) {
    const compact = line.replace(/\s+/g, ' ').trim();
    if (!compact) continue;
    if (titleNormalized && compact.toLowerCase() === titleNormalized) continue;
    // Skip very short numeric / symbol lines.
    if (compact.length < 4 && !/[a-z]/i.test(compact)) continue;

    // Keep a few lines even if they are JSON-ish, as long as they include letters.
    if (!looksLikeUsefulHumanText(compact)) continue;

    descParts.push(compact);
    if (descParts.join(' ').length >= maxDescription) break;
    if (descParts.length >= 5) break;
  }

  let description = descParts.join(' ').replace(/\s+/g, ' ').trim();
  if (description && description.length > maxDescription) {
    description = `${description.slice(0, maxDescription).trimEnd()}…`;
  }

  if (description && !looksLikeUsefulHumanText(description)) {
    description = null;
  }

  return { title: title || null, description: description || null };
}

function extractTextPreview(rawText, { maxChars = 600 } = {}) {
  const raw = String(rawText || '');
  if (!raw) return null;
  const safe = raw
    .replace(/\r/g, '\n')
    .replace(/[^\x09\x0A\x0D\x20-\x7E]/g, ' ');
  const trimmed = safe.trim();
  if (!trimmed) return null;
  if (trimmed.length <= maxChars) return trimmed;
  return `${trimmed.slice(0, maxChars).trimEnd()}…`;
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

function mapContentClassFromKind(kind) {
  const k = String(kind || '').toLowerCase().trim();
  if (k === 'html') return 'site';
  if (k === 'image') return 'image';
  if (k === 'text' || k === 'doc') return 'doc';
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

function looksLikeIpfsDirectoryListingHtml(html, { title, description, readable } = {}) {
  const raw = String(html || '');
  const lower = raw.toLowerCase();

  // Kubo gateway directory listing contains this exact phrase.
  if (lower.includes('a directory of content-addressed files hosted on ipfs')) return true;

  const t = String(title || '').trim().toLowerCase();
  const d = String(description || '').trim().toLowerCase();
  const r = String(readable || '').trim().toLowerCase();

  // Kubo uses "/ipfs/<cid>/" as the <title> for directory listings.
  if (t.startsWith('/ipfs/')) return true;

  if (d.includes('a directory of content-addressed files hosted on ipfs')) return true;

  const hasIpfsPathMarker = lower.includes('/ipfs/');
  const hasParentDirectory = r.includes('parent directory') || lower.includes('parent directory');
  const hasHrefParent = lower.includes('href="../"') || lower.includes("href='../'");
  const hasIndexOf =
    t === 'index of' ||
    t.startsWith('index of ') ||
    r.includes('index of') ||
    lower.includes('<h1>index of') ||
    lower.includes('>index of<');

  if (hasIpfsPathMarker && hasIndexOf && hasParentDirectory) return true;
  if (hasIpfsPathMarker && (hasParentDirectory || hasHrefParent) && t.startsWith('/ipfs/')) return true;
  if (hasIpfsPathMarker && (t.startsWith('index of ') || t === 'index of')) return true;

  return false;
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

  const isDirListing = looksLikeIpfsDirectoryListingHtml(html, { title, description, readable });
  const content_class = isDirListing ? 'dir_listing' : 'site';

  return {
    topics,
    tokens,
    title: title || null,
    description: description || null,
    content_class,
    lang,
    confidence: 0.75,
    signals: {
      from: [isDirListing ? 'html:dir_listing' : 'html:content'],
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
  let { title, description } = extractTextTitleAndDescription(raw, { markdownTitleOnly: true });
  let preview = extractTextPreview(raw);
  const normalized = normalizeText(Buffer.from(joined, 'utf8'));
  const tokenCounts = extractTokens(normalized, lang);
  const derived = deriveTopics(tokenCounts);
  const tokens = {};
  for (const [t, c] of tokenCounts.entries()) {
    if (!t) continue;
    tokens[t] = c;
  }

  let topics = [];

  const lowSignal =
    !looksLikeUsefulHumanText(modelText) ||
    looksLikePdfInternalText(raw) ||
    looksLikeZipContainerText(raw);

  if (lowSignal) {
    title = null;
    description = null;
  }

  if (!lowSignal) {
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
  }

  for (const t of derived) {
    const key = String(t || '').trim().toLowerCase();
    if (!key) continue;
    if (!topics.includes(key)) topics.push(key);
  }

  const content_class = mapContentClassFromKind('text');
  const confidence = 0.6;

  return {
    topics,
    tokens,
    title,
    description,
    preview,
    content_class,
    lang,
    confidence,
    signals: {
      from: [lowSignal ? 'text:head:low_signal' : 'text:head'],
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

async function analyzeDocFromExtractedText(extractedText, lang, bytesRead, { title, description, from } = {}) {
  const text = String(extractedText || '').trim();
  const hasMeta =
    (typeof title === 'string' && title.trim()) ||
    (typeof description === 'string' && description.trim());
  if (!text && !hasMeta) return null;

  const derivedMeta = text ? extractTextTitleAndDescription(text) : { title: null, description: null };
  const finalTitle = title || derivedMeta.title;
  const finalDescription = description || derivedMeta.description;

  const tokenSource = text || [finalTitle, finalDescription].filter(Boolean).join(' ').trim();
  const normalized = normalizeText(Buffer.from(tokenSource, 'utf8'));
  const tokenCounts = extractTokens(normalized, lang);
  const derived = deriveTopics(tokenCounts);
  const tokens = {};
  for (const [t, c] of tokenCounts.entries()) {
    if (!t) continue;
    tokens[t] = c;
  }

  let topics = [];

  try {
    const modelText = [finalTitle, finalDescription, text ? text.slice(0, 2000) : null]
      .filter(Boolean)
      .join(' ')
      .trim();
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
    logError('contentSniffer.analyzeDoc text tag enrichment error', err?.message || err);
  }

  for (const t of derived) {
    const key = String(t || '').trim().toLowerCase();
    if (!key) continue;
    if (!topics.includes(key)) topics.push(key);
  }

  const fromSignals = Array.isArray(from) ? [...from] : ['doc:fulltext'];
  if (!text && hasMeta && !fromSignals.includes('doc:metadata_only')) {
    fromSignals.unshift('doc:metadata_only');
  }

  return {
    topics,
    tokens,
    title: finalTitle || null,
    description: finalDescription || null,
    content_class: 'doc',
    lang,
    confidence: text ? 0.75 : 0.65,
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

  if (kind !== 'html' && kind !== 'text' && kind !== 'doc' && kind !== 'image') {
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

  if (kind === 'doc') {
    const maxBytes = CONFIG.DOC_EXTRACT_MAX_BYTES;
    const maxChars = CONFIG.DOC_EXTRACT_MAX_CHARS;
    const ext = String(detection.ext_guess || '').trim().toLowerCase();
    const mime = String(detection.mime || '').trim().toLowerCase();

    const maybeParseFull =
      ext === 'pdf' ||
      ext === 'docx' ||
      ext === 'epub' ||
      mime.includes('pdf') ||
      mime.includes('epub') ||
      mime.includes('wordprocessingml');

    let skippedTooLarge = false;
    let fullFetchFailed = false;
    let pdfUrlExtractFailed = false;
    let epubParseFailed = false;

    if (maybeParseFull) {
      try {
        // PDF: prefer range-based extraction via URL to avoid full downloads on large files.
        if (ext === 'pdf' || mime.includes('pdf')) {
          const url = new URL(`/ipfs/${cid}`, CONFIG.IPFS_GATEWAY_BASE).toString();
          try {
            const parsed = await extractPdfTextFromUrl(url, {
              maxChars,
              maxPages: CONFIG.PDF_EXTRACT_MAX_PAGES,
              timeoutMs: CONFIG.DOC_EXTRACT_TIMEOUT_MS
            });
            const meta = await analyzeDocFromExtractedText(parsed.text, lang, bytesRead, {
              title: parsed.title,
              description: parsed.description,
              from: ['doc:pdf_text']
            });
            if (meta) return meta;
          } catch (err) {
            pdfUrlExtractFailed = true;
            logError('contentSniffer.analyzeDoc pdf url extract error', cid, err?.message || err);
          }
        }
      } catch (err) {
        logError('contentSniffer.analyzeDoc pdf url extract wrapper error', cid, err?.message || err);
      }

      const full = await readFullBytes(cid, maxBytes);
      if (!full) {
        fullFetchFailed = true;
      } else if (full && full.tooLarge) {
        skippedTooLarge = true;
      }
      if (full && !full.tooLarge && full.bytes && full.bytes.length) {
        if (ext === 'pdf' || mime.includes('pdf')) {
          try {
            const parsed = await extractPdfTextFromBytes(full.bytes, {
              maxChars,
              maxPages: CONFIG.PDF_EXTRACT_MAX_PAGES,
              timeoutMs: CONFIG.DOC_EXTRACT_TIMEOUT_MS
            });
            const meta = await analyzeDocFromExtractedText(parsed.text, lang, full.bytesRead, {
              title: parsed.title,
              description: parsed.description,
              from: ['doc:pdf_text']
            });
            if (meta) return meta;
          } catch (err) {
            logError('contentSniffer.analyzeDoc pdf parse error', cid, err?.message || err);
          }
        }

        if (ext === 'docx' || mime.includes('wordprocessingml')) {
          try {
            const parsed = await extractDocxTextFromBytes(full.bytes, { maxChars });
            const meta = await analyzeDocFromExtractedText(parsed.text, lang, full.bytesRead, {
              title: parsed.title,
              description: parsed.description,
              from: ['doc:docx_text']
            });
            if (meta) return meta;
          } catch (err) {
            logError('contentSniffer.analyzeDoc docx parse error', cid, err?.message || err);
          }
        }

        if (ext === 'epub' || mime.includes('epub')) {
          try {
            const parsed = await extractEpubTextFromBytes(full.bytes, {
              maxChars,
              maxFiles: CONFIG.EPUB_EXTRACT_MAX_FILES
            });
            const meta = await analyzeDocFromExtractedText(parsed.text, lang, full.bytesRead, {
              title: parsed.title,
              description: parsed.description,
              from: ['doc:epub_text', 'doc:epub_text_v2']
            });
            if (meta) return meta;
          } catch (err) {
            epubParseFailed = true;
            logError('contentSniffer.analyzeDoc epub parse error', cid, err?.message || err);
          }
        }
      }
    }

    const fallback = await analyzeTextFromSample(sample.headBytes, lang, bytesRead);
    if (fallback && fallback.signals && Array.isArray(fallback.signals.from)) {
      if (!fallback.signals.from.includes('doc:sample')) {
        fallback.signals.from.unshift('doc:sample');
      }
      if (skippedTooLarge) {
        const marker = `doc:too_large:max=${maxBytes}`;
        if (!fallback.signals.from.includes(marker)) {
          fallback.signals.from.unshift(marker);
        }
      }
      if (fullFetchFailed) {
        if (!fallback.signals.from.includes('doc:full_fetch_failed')) {
          fallback.signals.from.unshift('doc:full_fetch_failed');
        }
      }
      if (pdfUrlExtractFailed) {
        if (!fallback.signals.from.includes('doc:pdf_url_extract_failed')) {
          fallback.signals.from.unshift('doc:pdf_url_extract_failed');
        }
      }
      if (epubParseFailed) {
        if (!fallback.signals.from.includes('doc:epub_parse_failed')) {
          fallback.signals.from.unshift('doc:epub_parse_failed');
        }
      }
    }
    return fallback;
  }

  if (kind === 'text') {
    return analyzeTextFromSample(sample.headBytes, lang, bytesRead);
  }

  if (kind === 'image') {
    return analyzeImage(detection, bytesRead, lang, cid);
  }

  return null;
}

export { mapContentClassFromKind };
