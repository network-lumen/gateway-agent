import { predictIntent, predictTarget, resolveLang } from '../search/modelLifecycle.js';
import { buildSearchQuery } from '../search/searchPlanner.js';
import {
  searchCidsSimple,
  fetchCidInfo,
  fetchParents,
  fetchChildren
} from '../lib/indexerClient.js';
import { countWalletReplicationForCids, getWalletsForRootCid } from '../lib/walletDb.js';
import {
  fetchDomainsByOwner,
  fetchDomainDetails
} from '../lib/dnsClient.js';
import { scoreDomainMatch } from '../lib/rootsDomains.js';
import { resolveIpnsToRootCid } from '../lib/ipnsResolver.js';
import { getUsageStatsForCids } from '../lib/usageDb.js';
import { decryptPqRequest } from '../middleware/pqMiddleware.js';
import { kuboRequest } from '../lib/kuboClient.js';
import { sendPqJson } from '../lib/pqResponse.js';
import { CID } from 'multiformats/cid';

function cleanSearch(input) {
  try {
    const raw = String(input || '').toLowerCase();
    const normalized = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    const stripped = normalized.replace(/[^a-z0-9\s?]/g, ' ');
    const cleanedText = stripped.replace(/\s+/g, ' ').trim();
    return {
      clean: cleanedText,
      hasQuestionMark: raw.includes('?'),
      length: cleanedText.length
    };
  } catch {
    const fallback = String(input || '').trim();
    return {
      clean: fallback,
      hasQuestionMark: String(input || '').includes('?'),
      length: fallback.length
    };
  }
}

function extractTokens(q) {
  if (!q) return [];
  const raw = String(q || '')
    .split(' ')
    .map((t) => t.trim())
    .filter(Boolean)
    .map((t) => t.toLowerCase())
    .filter((t) => /^[a-z0-9]+$/.test(t));
  return Array.from(new Set(raw));
}

const FORMAT_QUERY_ALLOWLIST = new Set([
  'pdf',
  'epub',
  'md',
  'markdown',
  'txt',
  'rtf',
  'csv',
  'json',
  'yaml',
  'yml',
  'toml',
  'xml',
  'html',
  'htm',
  'zip'
]);

function normalizeFormatQuery(cleanedQuery) {
  const q = String(cleanedQuery ?? '').trim().toLowerCase();
  if (!q) return null;
  if (q.includes(' ')) return null;
  if (!/^[a-z0-9]+$/.test(q)) return null;
  if (!FORMAT_QUERY_ALLOWLIST.has(q)) return null;
  return q;
}

function isCidLike(q) {
  const s = String(q || '').trim();
  if (!s) return false;
  if (/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/.test(s)) return true;
  if (/^[bB][a-z2-7]{50,}$/.test(s)) return true;
  return false;
}

function cidKey(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return CID.parse(raw).toV1().toString();
  } catch {
    return null;
  }
}

function extractCidFromRecordValue(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (isCidLike(raw)) return raw;

  const match = raw.match(
    /(?:^|\/)(Qm[1-9A-HJ-NP-Za-km-z]{44}|[bB][a-z2-7]{50,})(?=$|[/?#])/
  );
  if (match && match[1]) return match[1];

  return null;
}

function isLikelyIpnsValue(value) {
  const raw = String(value || '').trim();
  if (!raw) return false;
  if (/^k51[0-9a-z]{10,}$/i.test(raw)) return true;
  if (/^ipns:\/\//i.test(raw)) return true;
  if (/(?:^|\/)ipns\/[^/?#]+/i.test(raw)) return true;
  return false;
}

function normalizeIpnsValue(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;

  const m0 = raw.match(/^ipns:\/\/(?:ipns\/)?([^/?#]+)/i);
  if (m0 && m0[1]) return m0[1];

  const m1 = raw.match(/(?:^|\/)ipns\/([^/?#]+)/i);
  if (m1 && m1[1]) return m1[1];

  return raw;
}

function coerceTimestampMs(value) {
  if (typeof value === 'number' && Number.isFinite(value) && value > 0) {
    return value;
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return 0;
}

function pickActivityAtMs(hit) {
  const lastSeen = coerceTimestampMs(hit?.last_seen_at);
  if (lastSeen) return lastSeen;
  const firstSeen = coerceTimestampMs(hit?.first_seen_at);
  if (firstSeen) return firstSeen;
  const updatedAt = coerceTimestampMs(hit?.updated_at);
  if (updatedAt) return updatedAt;
  const indexedAt = coerceTimestampMs(hit?.indexed_at);
  if (indexedAt) return indexedAt;
  return 0;
}

function filterOctetStreamHits(hits) {
  if (!Array.isArray(hits) || hits.length === 0) return [];
  return hits.filter((hit) => {
    const mime = String(hit?.mime || '').trim().toLowerCase();
    if (mime !== 'application/octet-stream') return true;

    // Some gateways / sniffers label known formats as octet-stream. Keep the row when the
    // path/ext hints at a supported preview type.
    const ext = String(hit?.ext_guess || '').trim().toLowerCase();
    const path = String(hit?.path || '').trim().toLowerCase();
    if (ext && ['pdf', 'docx', 'epub', 'html', 'htm', 'txt'].includes(ext)) return true;
    if (
      path.endsWith('.pdf') ||
      path.endsWith('.docx') ||
      path.endsWith('.epub') ||
      path.endsWith('.html') ||
      path.endsWith('.htm') ||
      path.endsWith('.txt')
    ) {
      return true;
    }

    return false;
  });
}

function isHtmlPath(pathValue) {
  const p = String(pathValue ?? '').trim().toLowerCase();
  return p.endsWith('.html') || p.endsWith('.htm');
}

function isHtmlMime(mimeValue) {
  const m = String(mimeValue ?? '').trim().toLowerCase();
  if (!m) return false;
  return m.includes('text/html') || m.includes('application/xhtml+xml');
}

function looksLikeUnixfsFileChunks(entries) {
  const list = Array.isArray(entries) ? entries : [];
  if (!list.length) return false;

  // UnixFS file DAGs can have numbered links (0, 1, 2...) to raw blocks. Those are not directories.
  const hasDir = list.some((e) => e && String(e.type || '').toLowerCase() === 'dir');
  if (hasDir) return false;

  const names = list.map((e) => String(e?.name || '').trim()).filter(Boolean);
  if (!names.length) return false;
  return names.every((n) => /^\d+$/.test(n));
}

function looksLikeDocxPath(pathValue) {
  const p = String(pathValue ?? '').trim().toLowerCase();
  return p.endsWith('.docx');
}

function looksLikeEpubPath(pathValue) {
  const p = String(pathValue ?? '').trim().toLowerCase();
  return p.endsWith('.epub');
}

function looksLikePdfPath(pathValue) {
  const p = String(pathValue ?? '').trim().toLowerCase();
  return p.endsWith('.pdf');
}

function looksLikeTxtPath(pathValue) {
  const p = String(pathValue ?? '').trim().toLowerCase();
  return p.endsWith('.txt');
}

function looksLikePdfObjectStreamHit(hit) {
  const snippet = String(hit?.snippet || '').trim().toLowerCase();
  const title = String(hit?.title || '').trim().toLowerCase();

  const tokenCounts =
    hit?.tags_json && hit.tags_json.tokens && typeof hit.tags_json.tokens === 'object'
      ? hit.tags_json.tokens
      : null;

  const getCount = (k) => {
    if (!tokenCounts) return 0;
    const v = Number(tokenCounts[k]);
    return Number.isFinite(v) ? v : 0;
  };

  const combined = `${title}\n${snippet}`.trim();

  // PDF xref tables can look like pure numeric text (e.g. "0024721696 00000 n") and may
  // not include "xref" in the sampled snippet. Detect and filter them aggressively.
  const xrefLineMatches = combined.match(/\b\d{6,12}\s+\d{5}\s+[nf]\b/g) || [];
  if (xrefLineMatches.length >= 3) {
    const nonXref = combined.replace(/[0-9nf\s]/g, '');
    if (nonXref.length <= 10) return true;
  }

  if (tokenCounts) {
    // Same idea, but relying on token histograms (snippet/title can be empty for numeric junk).
    // Xref tables generate lots of 6-12 digit offsets and repeated "00000" generation numbers.
    const keys = Object.keys(tokenCounts).map((k) => String(k || '').trim().toLowerCase());
    const numericOffsets = keys.filter((k) => /^\d{6,12}$/.test(k)).length;
    const hasGenZeros = !!tokenCounts['00000'];
    if (hasGenZeros && numericOffsets >= 8) return true;
  }

  const hasObjPattern =
    /\b\d+\s+\d+\s+obj\b/.test(snippet) ||
    /\b\d+\s+\d+\s+obj\b/.test(title) ||
    getCount('obj') >= 2;

  const flate = getCount('flatedecode') || (snippet.includes('flatedecode') ? 1 : 0);
  const mediabox = getCount('mediabox') || (snippet.includes('mediabox') ? 1 : 0);
  const cropbox = getCount('cropbox') || (snippet.includes('cropbox') ? 1 : 0);
  const resources = getCount('resources') || (snippet.includes('resources') ? 1 : 0);
  const font = getCount('font') || (snippet.includes('font') ? 1 : 0);

  const endobj = getCount('endobj') || (snippet.includes('endobj') ? 1 : 0);
  const xref = getCount('xref') || (snippet.includes('xref') ? 1 : 0);
  const trailer = getCount('trailer') || (snippet.includes('trailer') ? 1 : 0);
  const stream = getCount('stream') || (snippet.includes('stream') ? 1 : 0);
  const endstream = getCount('endstream') || (snippet.includes('endstream') ? 1 : 0);
  const xobject = getCount('xobject') || (snippet.includes('xobject') ? 1 : 0);

  const syntaxScore =
    (endobj > 0 ? 1 : 0) +
    (xref > 0 ? 1 : 0) +
    (trailer > 0 ? 1 : 0) +
    (stream > 0 ? 1 : 0) +
    (endstream > 0 ? 1 : 0) +
    (xobject > 0 ? 1 : 0) +
    (flate > 0 ? 1 : 0) +
    (mediabox > 0 ? 1 : 0) +
    (cropbox > 0 ? 1 : 0) +
    (resources > 0 ? 1 : 0) +
    (font > 0 ? 1 : 0);

  const hasStreamish = stream > 0 || endstream > 0;
  const hasPdfDictish = flate > 0 || mediabox > 0 || cropbox > 0 || resources > 0 || font > 0 || xobject > 0;

  // Some extracted PDF internals won't include the "N N obj" header in the sampled snippet/tokens.
  // Still filter when we have multiple strong PDF-internal markers.
  if (hasObjPattern) return syntaxScore >= 2;
  return hasStreamish && hasPdfDictish && syntaxScore >= 3;
}

function looksLikeBrokenPdfPreviewHit(hit) {
  const mime = String(hit?.mime || hit?.path_mime_hint || '').trim().toLowerCase();
  const ext = String(hit?.ext_guess || '').trim().toLowerCase();
  const isPdf = ext === 'pdf' || mime.includes('pdf');
  if (!isPdf) return false;

  const tags = hit?.tags_json && typeof hit.tags_json === 'object' ? hit.tags_json : null;
  const signalsFrom = tags?.signals && Array.isArray(tags.signals.from) ? tags.signals.from : [];
  const from = signalsFrom.map((s) => String(s || '').toLowerCase());

  // If our PDF text extraction failed and even the sample analysis was low-signal,
  // these entries are typically not previewable and are noise in Explore.
  const hadExtractFailure =
    from.includes('doc:pdf_url_extract_failed') ||
    from.includes('doc:full_fetch_failed');
  const lowSignal = from.includes('text:head:low_signal') || from.includes('doc:sample');
  if (!hadExtractFailure || !lowSignal) return false;

  const tokenCounts =
    tags && tags.tokens && typeof tags.tokens === 'object'
      ? tags.tokens
      : null;

  // If tokens are empty (or only PDF syntax), there's nothing useful to rank/search on.
  const keys = tokenCounts ? Object.keys(tokenCounts).map((k) => String(k || '').trim().toLowerCase()) : [];
  const meaningful = keys.filter((k) => k && !['pdf', 'obj', 'endobj', 'xref', 'linearized'].includes(k));
  if (tokenCounts && keys.length && meaningful.length) return false;

  return true;
}

function looksLikeZipBinarySnippet(value) {
  const s = String(value ?? '').trim();
  if (!s) return false;

  // Many ZIP-based formats start with "PK\u0003\u0004" (rendered as "PK" + control chars).
  // If a doc preview starts with that, it's almost certainly binary junk being treated as text.
  if (s.startsWith('PK')) return true;

  const lower = s.toLowerCase();
  if (lower.includes('mimetypeapplication/epub+zip')) return true;
  if (lower.includes('jfif')) return true;
  return false;
}

function looksLikeBrokenEpubPreviewHit(hit) {
  const mime = String(hit?.mime || hit?.path_mime_hint || '').trim().toLowerCase();
  const ext = String(hit?.ext_guess || '').trim().toLowerCase();
  const path = String(hit?.path || '').trim().toLowerCase();
  const isEpub = ext === 'epub' || mime.includes('epub') || looksLikeEpubPath(path);
  if (!isEpub) return false;

  const snippet = String(hit?.snippet || '').trim();
  if (snippet && looksLikeZipBinarySnippet(snippet)) return true;

  const tags = hit?.tags_json && typeof hit.tags_json === 'object' ? hit.tags_json : null;
  const signalsFrom = tags?.signals && Array.isArray(tags.signals.from) ? tags.signals.from : [];
  const from = signalsFrom.map((s) => String(s || '').toLowerCase());
  if (from.includes('doc:epub_parse_failed')) return true;

  const title = String(hit?.title || '').trim();
  if (!title && !snippet && from.includes('doc:sample')) return true;

  return false;
}

function looksLikeLowSignalTextHit(hit) {
  const path = String(hit?.path || '').trim().toLowerCase();
  const title = String(hit?.title || '').trim();
  const snippet = String(hit?.snippet || '').trim();

  const tokenCounts =
    hit?.tags_json && hit.tags_json.tokens && typeof hit.tags_json.tokens === 'object'
      ? hit.tags_json.tokens
      : null;
  const topics = hit?.tags_json && Array.isArray(hit.tags_json.topics)
    ? hit.tags_json.topics
    : Array.isArray(hit?.topics)
      ? hit.topics
      : [];

  const tokenCount = tokenCounts ? Object.keys(tokenCounts).length : 0;
  const topicCount = Array.isArray(topics) ? topics.length : 0;

  // If it's part of a path index, it's probably a real user file (README.md, foo.json, etc.).
  if (path) return false;

  // If we extracted any meaningful text metadata/tokens, keep it.
  if (title) return false;
  if (snippet) return false;

  // Some legacy rows have no path/title/snippet and their tokens are dominated by the zero-shot
  // label vocabulary (multi-word phrases). Treat those as low-signal to avoid surfacing junk.
  if (tokenCounts && tokenCount > 0) {
    const keys = Object.keys(tokenCounts)
      .map((k) => String(k || '').trim())
      .filter(Boolean);
    const withSpaces = keys.filter((k) => /\s/.test(k)).length;
    const ratio = keys.length ? withSpaces / keys.length : 0;
    if (keys.length >= 15 && ratio >= 0.8) {
      return true;
    }
    return false;
  }

  if (topicCount > 0) return false;

  // Otherwise it's likely numeric garbage / internal streams surfaced via heuristic.
  return true;
}

function exploreTypeForHit(hit) {
  const kind = String(hit?.kind || '').trim().toLowerCase();
  const mime = String(hit?.mime || hit?.path_mime_hint || '').trim().toLowerCase();
  const ext = String(hit?.ext_guess || '').trim().toLowerCase();
  const path = String(hit?.path || '').trim();
  const pathLower = path.toLowerCase();
  const title = String(hit?.title || '').trim();
  const snippet = String(hit?.snippet || '').trim();

  // Images
  if (kind === 'image' || mime.startsWith('image/')) return 'image';

  // PDFs
  if (ext === 'pdf' || mime.includes('pdf') || looksLikePdfPath(pathLower)) {
    if (looksLikeBrokenPdfPreviewHit(hit)) return null;
    return 'pdf';
  }

  // EPUB
  if (ext === 'epub' || mime.includes('epub') || looksLikeEpubPath(pathLower)) {
    if (looksLikeBrokenEpubPreviewHit(hit)) return null;
    return 'epub';
  }

  // HTML/HTM (exclude obvious directory listings)
  if (ext === 'html' || ext === 'htm' || isHtmlPath(pathLower) || mime.includes('text/html') || mime.includes('application/xhtml+xml')) {
    if (
      looksLikeIpfsDirectoryListingTitleOrPath(title) ||
      looksLikeIpfsDirectoryListingSnippet(snippet) ||
      looksLikeDirectoryListingTitle(title)
    ) {
      return null;
    }
    return 'html';
  }

  // Plain text
  if (ext === 'txt' || looksLikeTxtPath(pathLower) || mime.startsWith('text/plain')) {
    // Avoid surfacing PDF object streams misdetected as text/plain (not useful content).
    if (looksLikePdfObjectStreamHit(hit)) return null;
    // Avoid surfacing "no signal" text blobs (numeric junk, internal fragments) as top-level content.
    if (looksLikeLowSignalTextHit(hit)) return null;
    return 'txt';
  }

  return null;
}

function isExploreAllowedHit(hit) {
  return !!exploreTypeForHit(hit);
}

function isDirectoryListingContentClass(value) {
  return String(value ?? '').trim().toLowerCase() === 'dir_listing';
}

function looksLikeIpfsDirectoryListingSnippet(snippetValue) {
  const s = String(snippetValue ?? '').trim().toLowerCase();
  if (!s) return false;
  return s.includes('a directory of content-addressed files hosted on ipfs');
}

function looksLikeIpfsDirectoryListingTitleOrPath(titleValue) {
  const t = String(titleValue ?? '').trim().toLowerCase();
  if (!t) return false;
  if (t.startsWith('/ipfs/')) return true;
  if (t.startsWith('ipfs/')) return true;
  return false;
}

function looksLikeDirectoryListingTitle(titleValue) {
  const t = String(titleValue ?? '').trim().toLowerCase();
  if (!t) return false;
  if (t === 'index of') return true;
  if (t.startsWith('index of ')) return true;
  return false;
}

function isHtmlSiteHit(hit) {
  const pathNorm = normalizeEntryPath(hit?.path);
  if (pathNorm && isHtmlPath(pathNorm)) return true;

  const title = String(hit?.title || '').trim();
  if (looksLikeDirectoryListingTitle(title)) return false;

  const mime = String(hit?.mime || hit?.path_mime_hint || '').trim();
  if (isHtmlMime(mime)) return true;

  const ext = String(hit?.ext_guess || '').trim().toLowerCase();
  return ext === 'html' || ext === 'htm';
}

function filterHtmlSiteHits(hits) {
  const list = Array.isArray(hits) ? hits : [];
  return list.filter((h) => isHtmlSiteHit(h));
}

function normalizeEntryPath(pathValue) {
  const raw = String(pathValue ?? '').trim();
  if (!raw) return null;
  // indexer paths are usually relative, but be tolerant
  const p = raw.startsWith('/') ? raw.slice(1) : raw;
  if (!p) return null;
  // basic safety: prevent obvious traversal
  if (p.includes('..')) return null;
  return p;
}

async function hasIndexSignal(tokens) {
  for (const token of tokens) {
    const tok = String(token || '').trim().toLowerCase();
    if (!tok) continue;

    const res = await searchCidsSimple(
      { tokens: [tok], limit: 1, offset: 0 },
      { timeoutMs: 600 }
    );
    if (res.ok && Array.isArray(res.items) && res.items.length > 0) {
      return true;
    }
  }
  return false;
}

async function isMeaningfulQuery(q) {
  if (!q) return false;
  const cleaned = String(q || '').trim();
  const compact = cleaned.replace(/\s+/g, '');
  if (!compact) return false;

  // trivial spam like "aaaaa"
  if (/^(.)\1+$/.test(compact)) return false;

  const tokens = extractTokens(cleaned);

  // Allow very short queries (1-2 chars) to act like "explore recent" instead of
  // returning empty results. The actual ranking happens later (multi-signal ordering).
  if (!tokens.length) {
    const hasAlphaNum = /[a-z0-9]/i.test(compact);
    return hasAlphaNum && compact.length <= 2;
  }

  // short, simple queries (brand / keyword) are considered meaningful
  if (cleaned.length <= 10 && tokens.length <= 2) {
    return true;
  }

  // otherwise require at least one index signal
  const hasSignal = await hasIndexSignal(tokens);
  return hasSignal;
}

function mapKindToResourceType(kind) {
  const k = String(kind || '').toLowerCase().trim();
  if (k === 'site') return 'site';
  if (k === 'image') return 'image';
  return 'all';
}

function normalizeStrictTypeFilter(typeValue) {
  const t = String(typeValue || '').toLowerCase().trim();
  if (t === 'image' || t === 'images') return 'image';
  return '';
}

function hitMatchesStrictType(hit, strictType) {
  const t = normalizeStrictTypeFilter(strictType);
  if (!t) return true;

  const kind = String(hit?.kind || '').toLowerCase().trim();
  const mime = String(hit?.mime || hit?.path_mime_hint || '').toLowerCase().trim();
  const resourceTypeRaw = String(hit?.resourceType || hit?.resource_type || '').toLowerCase().trim();
  const resourceType = resourceTypeRaw || mapKindToResourceType(kind);

  if (t === 'image') return resourceType === 'image' || kind === 'image' || mime.startsWith('image/');
  return true;
}

function applyStrictTypeFilter(hits, strictType) {
  const t = normalizeStrictTypeFilter(strictType);
  if (!t) return hits;
  const list = Array.isArray(hits) ? hits : [];
  return list.filter((h) => hitMatchesStrictType(h, t));
}

function clamp01(value) {
  const v = Number.isFinite(value) ? value : 0;
  if (v <= 0) return 0;
  if (v >= 1) return 1;
  return v;
}

// Filter out HTML-structural tokens that are common in raw markup and not useful as "site tags".
const SITE_TAG_STOPWORDS = new Set([
  'doctype',
  'html',
  'head',
  'meta',
  'charset',
  'viewport',
  'width',
  'device',
  'initial',
  'scale',
  'name',
  'content',
  'lang',
  'utf',
  'title',
  'href',
  'rel',
  'link',
  'stylesheet',
  'src',
  'class',
  'id',
  'div',
  'span',
  'button',
  'self',
  'script',
  'style',
  'http',
  'https',
  'www',
  'com',
  'lumen',
  'ipfs',
  'gateway'
]);

function shouldKeepSiteTag(tag) {
  const t = String(tag || '').trim().toLowerCase();
  if (!t) return false;
  if (SITE_TAG_STOPWORDS.has(t)) return false;
  // drop very short tokens (mostly markup noise)
  if (t.length < 3) return false;
  return true;
}

function extractSiteTagsFromCidInfo(cidInfo) {
  const tagsJson =
    cidInfo && cidInfo.tags && typeof cidInfo.tags === 'object'
      ? cidInfo.tags
      : null;

  const topicsRaw = Array.isArray(tagsJson?.topics) ? tagsJson.topics : [];
  const tokensObj =
    tagsJson && tagsJson.tokens && typeof tagsJson.tokens === 'object'
      ? tagsJson.tokens
      : null;

  const out = [];

  for (const t of topicsRaw) {
    const v = String(t || '').trim().toLowerCase();
    if (!shouldKeepSiteTag(v)) continue;
    if (!out.includes(v)) out.push(v);
  }

  if (tokensObj) {
    const scored = Object.entries(tokensObj)
      .map(([k, v]) => [String(k || '').trim().toLowerCase(), Number(v)])
      .filter(([k, v]) => k && Number.isFinite(v) && v > 0)
      .sort((a, b) => {
        if (b[1] !== a[1]) return b[1] - a[1];
        return String(a[0]).localeCompare(String(b[0]));
      })
      .slice(0, 50)
      .map(([k]) => k);

    for (const k of scored) {
      if (!shouldKeepSiteTag(k)) continue;
      if (!out.includes(k)) out.push(k);
      if (out.length >= 20) break;
    }
  }

  return out.slice(0, 20);
}

export function classifySearch(rawInput, opts = {}) {
  const lang = resolveLang(opts.lang);
  const features = cleanSearch(rawInput);
  const clean = features.clean;

  const intentPred = predictIntent(lang, clean);
  const targetPred = predictTarget(lang, clean);

  const intent =
    intentPred.confidence >= 0.6 ? intentPred.label : 'unknown';
  const target =
    targetPred.confidence >= 0.6 ? targetPred.label : 'mixed';

  return {
    raw: String(rawInput || ''),
    clean,
    lang,
    intent,
    intentConfidence: intentPred.confidence,
    target,
    targetConfidence: targetPred.confidence
  };
}

async function executePlanAgainstIndexerRaw(plan, queryTokens = [], opts = {}) {
  if (!plan || plan.noQuery) {
    return [];
  }

  const limit = plan.limit;
  const offset = plan.offset;
  const scanLimit = Math.min(limit + offset + 100, 200);

  let items = [];

  if (plan.targetKind) {
    const res = await searchCidsSimple(
      { kind: plan.targetKind, tokens: queryTokens, present: 1, limit: scanLimit, offset: 0 },
      {}
    );
    if (res.ok && Array.isArray(res.items)) {
      items = res.items;
    }
  } else if (Array.isArray(plan.baseKinds) && plan.baseKinds.length > 0) {
    const combined = [];
    const seen = new Set();

    for (const kind of plan.baseKinds) {
      const res = await searchCidsSimple(
        { kind, tokens: queryTokens, present: 1, limit: scanLimit, offset: 0 },
        {}
      );
      if (!res.ok || !Array.isArray(res.items)) continue;
      for (const row of res.items) {
        if (!row || typeof row.cid !== 'string') continue;
        if (seen.has(row.cid)) continue;
        seen.add(row.cid);
        combined.push(row);
      }
    }

    items = combined;
  } else {
    const res = await searchCidsSimple(
      { tokens: queryTokens, present: 1, limit: scanLimit, offset: 0 },
      {}
    );
    if (res.ok && Array.isArray(res.items)) {
      items = res.items;
    }
  }

  items.sort((a, b) => {
    const aTs = pickActivityAtMs(a);
    const bTs = pickActivityAtMs(b);
    if (aTs !== bTs) return bTs - aTs;
    const aCid = a && typeof a.cid === 'string' ? a.cid : '';
    const bCid = b && typeof b.cid === 'string' ? b.cid : '';
    if (aCid < bCid) return -1;
    if (aCid > bCid) return 1;
    return 0;
  });

  return items.map((row) => {
    let tagsJson = null;
    let topics = [];
    let title = null;
    let snippet = null;
    let signalsJson = null;
    const isText = String(row.kind || '').toLowerCase() === 'text';
    try {
      if (row.tags_json) {
        tagsJson =
          typeof row.tags_json === 'string'
            ? JSON.parse(row.tags_json)
            : row.tags_json;
        if (tagsJson && Array.isArray(tagsJson.topics)) {
          topics = tagsJson.topics
            .map((t) => String(t || '').trim().toLowerCase())
            .filter(Boolean);
        }
        if (tagsJson && typeof tagsJson.title === 'string') {
          title = tagsJson.title;
        }
        if (isText && tagsJson && typeof tagsJson.preview === 'string') {
          snippet = tagsJson.preview;
        } else if (tagsJson && typeof tagsJson.description === 'string') {
          snippet = tagsJson.description;
        }
      }
    } catch {
      tagsJson = null;
      topics = [];
    }

    try {
      if (row.signals_json) {
        signalsJson =
          typeof row.signals_json === 'string'
            ? JSON.parse(row.signals_json)
            : row.signals_json;
      }
    } catch {
      signalsJson = null;
    }

    const indexedAtMs = coerceTimestampMs(row.indexed_at);

    return {
      cid: row.cid,
      present: row.present === true || row.present === 1,
      present_source: row.present_source || null,
      first_seen_at:
        typeof row.first_seen_at === 'number' && Number.isFinite(row.first_seen_at)
          ? row.first_seen_at
          : null,
      last_seen_at:
        typeof row.last_seen_at === 'number' && Number.isFinite(row.last_seen_at)
          ? row.last_seen_at
          : null,
      removed_at:
        typeof row.removed_at === 'number' && Number.isFinite(row.removed_at)
          ? row.removed_at
          : null,
      kind: row.kind || null,
      confidence:
        typeof row.confidence === 'number' ? row.confidence : null,
      mime: row.mime || null,
      ext_guess: row.ext_guess || null,
      size_bytes:
        typeof row.size_bytes === 'number' ? row.size_bytes : null,
      indexed_at:
        indexedAtMs ? indexedAtMs : null,
      updated_at:
        typeof row.updated_at === 'number' && Number.isFinite(row.updated_at)
          ? row.updated_at
          : null,
      error: row.error || null,
      resourceType: mapKindToResourceType(row.kind),
      tags_json: tagsJson,
      signals_json: signalsJson,
      matched_tokens:
        typeof row.matched_tokens === 'number' && Number.isFinite(row.matched_tokens)
          ? row.matched_tokens
          : null,
      token_score:
        typeof row.token_score === 'number' && Number.isFinite(row.token_score)
          ? row.token_score
          : null,
      topics,
      title: title || null,
      snippet: snippet || null,
      root_cid: row.root_cid || row.cid || null,
      path: typeof row.path === 'string' ? row.path : null,
      path_mime_hint:
        typeof row.path_mime_hint === 'string'
          ? row.path_mime_hint
          : null
    };
  });
}

const RANKING_WEIGHTS_V1 = {
  popularity: 0.3,
  relevance: 0.3,
  freshness: 0.2,
  availability: 0.1,
  onchain: 0.1
};

const POPULARITY_USAGE_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
const POPULARITY_REPLICATION_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;
const POPULARITY_USAGE_CAP = 10;
const POPULARITY_REPLICATION_CAP = 10;
const FRESHNESS_DECAY_MS = 30 * 24 * 60 * 60 * 1000;

const ONCHAIN_LINK_CACHE_TTL_MS = 15 * 60 * 1000;
const ONCHAIN_LINK_MAX_ROOTS = 40;
const onchainLinkCache = new Map(); // rootCidKey -> { fetchedAt, domain }

async function executePlanAgainstIndexerPaged(plan, queryTokens, opts = {}) {
  const allHits = await executePlanAgainstIndexerRaw(plan, queryTokens, opts);
  if (!allHits.length) return [];

  const tokens = Array.isArray(queryTokens) ? queryTokens : [];
  const preFilter = opts && typeof opts.preFilter === 'function' ? opts.preFilter : null;
  const pool = preFilter ? allHits.filter(preFilter) : allHits;
  if (!pool.length) return [];

  const limit = plan.limit;
  const offset = plan.offset;
  const now = Date.now();

  const contentTokens = tokens
    .map((t) => String(t || '').trim().toLowerCase())
    .filter((t) => t.length >= 3);

  const hasContentTokens = contentTokens.length > 0;

  // 1) Compute pure content relevance (first-class).
  // Empty query still uses multi-signal ranking (popularity/freshness/availability), but without content match.
  //
  // IMPORTANT: Do not hard-filter out hits at this stage. The indexer already did token filtering, and
  // metadata can be missing/partial (which would incorrectly drop valid results).
  const relevanceFiltered = hasContentTokens
    ? pool.map((hit) => {
        const raw = scoreHitWithQuery(hit, contentTokens);
        const hasMatch = hasTokenMatchForHit(hit, contentTokens);
        return {
          ...hit,
          _content_raw: raw,
          _relevance: hasMatch ? normalizeContentScore(raw) : 0
        };
      })
    : pool.map((hit) => ({
        ...hit,
        _content_raw: 0,
        _relevance: 0
      }));

  if (!relevanceFiltered.length) return [];

  // 2) Gather per-root network signals (real usage + short-term replication).
  const roots = Array.from(
    new Set(
      relevanceFiltered
        .map((h) => String(h.root_cid || h.cid || '').trim())
        .filter(Boolean)
    )
  );

  const [usageStats, replicationCounts] = await Promise.all([
    getUsageStatsForCids(roots, { sinceMs: now - POPULARITY_USAGE_WINDOW_MS }),
    countWalletReplicationForCids(roots, { sinceMs: now - POPULARITY_REPLICATION_WINDOW_MS })
  ]);

  // 3) Pre-score roots (without on-chain lookup) and compute per-hit components.
  const rootBaseScore = new Map(); // root -> max base score among its hits
  const withComponents = relevanceFiltered.map((hit) => {
    const root = String(hit.root_cid || hit.cid || '').trim();
    const usage = usageStats.get(root) || { wallets: 0, ok_wallets: 0, bad_wallets: 0 };
    const usageOk = typeof usage.ok_wallets === 'number' ? usage.ok_wallets : 0;
    const usageScore = saturatingLogScore(usageOk, POPULARITY_USAGE_CAP);

    const replicationCount = replicationCounts.get(root) || 0;
    const replicationScore = saturatingLogScore(replicationCount, POPULARITY_REPLICATION_CAP);

    const popularity = clamp01(0.6 * usageScore + 0.4 * replicationScore);
    const freshness = computeFreshnessScore(hit, now);
    const availability = computeAvailabilityScore(hit, usage);
    const relevance =
      hasContentTokens && typeof hit._relevance === 'number' && Number.isFinite(hit._relevance)
        ? hit._relevance
        : 0;

    const baseScore =
      RANKING_WEIGHTS_V1.popularity * popularity +
      RANKING_WEIGHTS_V1.relevance * relevance +
      RANKING_WEIGHTS_V1.freshness * freshness +
      RANKING_WEIGHTS_V1.availability * availability;

    const prevRootBest = rootBaseScore.get(root) || 0;
    if (baseScore > prevRootBest) rootBaseScore.set(root, baseScore);

    return {
      ...hit,
      _rank: {
        root,
        relevance,
        popularity,
        usageScore,
        usageOkWallets: usageOk,
        replicationScore,
        freshness,
        availability,
        baseScore
      }
    };
  });

  // 4) Apply on-chain linkage bonus (small, never authoritative), with cache + cap.
  const rootsToCheck = Array.from(rootBaseScore.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, ONCHAIN_LINK_MAX_ROOTS)
    .map(([root]) => root);

  const linkedDomainsByRootKey = await getOnchainLinkedDomains(rootsToCheck, {
    ttlMs: ONCHAIN_LINK_CACHE_TTL_MS
  });

  // 5) Final scoring + sorting.
  const ranked = withComponents.map((hit) => {
    const comp = hit._rank || {};
    const root = String(comp.root || hit.root_cid || hit.cid || '').trim();
    const rootKey = cidKey(root) || root;
    const linkedDomain = linkedDomainsByRootKey.get(rootKey) || null;
    const linked = !!linkedDomain;
    const finalScore =
      (typeof comp.baseScore === 'number' && Number.isFinite(comp.baseScore) ? comp.baseScore : 0) +
      RANKING_WEIGHTS_V1.onchain * (linked ? 1 : 0);

    return {
      ...hit,
      _score: finalScore,
      views_unique_7d: typeof comp.usageOkWallets === 'number' ? comp.usageOkWallets : 0,
      linked_domain: linkedDomain || undefined,
      rank_signals: buildRankSignals({
        relevance: comp.relevance || 0,
        popularity: comp.popularity || 0,
        usage: comp.usageScore || 0,
        replication: comp.replicationScore || 0,
        freshness: comp.freshness || 0,
        availability: comp.availability || 0,
        onchain: linked
      })
    };
  });

  ranked.sort((a, b) => {
    const aScore = typeof a._score === 'number' && Number.isFinite(a._score) ? a._score : 0;
    const bScore = typeof b._score === 'number' && Number.isFinite(b._score) ? b._score : 0;
    if (bScore !== aScore) return bScore - aScore;
    const aTs = pickActivityAtMs(a);
    const bTs = pickActivityAtMs(b);
    if (aTs !== bTs) return bTs - aTs;
    const aCid = String(a.cid || '');
    const bCid = String(b.cid || '');
    return aCid.localeCompare(bCid);
  });

  const sliced = ranked.slice(offset, offset + limit);

  return sliced.map((hit) => {
    const { _rank, _content_raw, _relevance, ...rest } = hit;
    return rest;
  });
}

function saturatingLogScore(count, cap) {
  const n = typeof count === 'number' && Number.isFinite(count) ? count : 0;
  if (n <= 0) return 0;
  const c = typeof cap === 'number' && Number.isFinite(cap) && cap > 1 ? cap : 10;
  const bounded = Math.min(n, c);
  return clamp01(Math.log1p(bounded) / Math.log1p(c));
}

function normalizeContentScore(rawScore) {
  const s = typeof rawScore === 'number' && Number.isFinite(rawScore) && rawScore > 0 ? rawScore : 0;
  // Saturating curve: big raw scores flatten quickly so no single hit dominates purely by token counts.
  // 120 is an empirical scale factor matching the existing token histogram scoring.
  return clamp01(1 - Math.exp(-s / 120));
}

function computeFreshnessScore(hit, nowMs) {
  const now = typeof nowMs === 'number' && Number.isFinite(nowMs) ? nowMs : Date.now();
  const activityAt = pickActivityAtMs(hit);
  if (!activityAt) return 0;
  const age = Math.max(0, now - activityAt);
  return clamp01(Math.exp(-age / FRESHNESS_DECAY_MS));
}

function computeAvailabilityScore(hit, usageStat) {
  const present = hit && (hit.present === true || hit.present === 1);
  if (!present) return 0;

  let score = 1;

  const presentSource = String(hit?.present_source || '').trim().toLowerCase();
  if (presentSource && presentSource !== 'pinls') score *= 0.9;

  if (hit && hit.error) score *= 0.7;

  // Reliability from real PQ access (distinct wallets within the usage window).
  const totalWallets =
    usageStat && typeof usageStat.wallets === 'number' && Number.isFinite(usageStat.wallets)
      ? usageStat.wallets
      : 0;
  if (totalWallets > 0) {
    const okWallets =
      usageStat && typeof usageStat.ok_wallets === 'number' && Number.isFinite(usageStat.ok_wallets)
        ? usageStat.ok_wallets
        : 0;
    const okRatio = clamp01(okWallets / totalWallets);
    score *= 0.6 + 0.4 * okRatio;
  }

  return clamp01(score);
}

function pickBetterLinkedDomain(prev, next) {
  const a = String(prev || '').trim().toLowerCase();
  const b = String(next || '').trim().toLowerCase();
  if (!a) return b || null;
  if (!b) return a || null;

  const aParts = a.split('.').filter(Boolean).length;
  const bParts = b.split('.').filter(Boolean).length;
  if (bParts < aParts) return b;
  if (bParts > aParts) return a;
  return b.localeCompare(a) < 0 ? b : a;
}

function bucket3(score, lowCutoff, highCutoff, labels) {
  const s = typeof score === 'number' && Number.isFinite(score) ? score : 0;
  const low = typeof lowCutoff === 'number' && Number.isFinite(lowCutoff) ? lowCutoff : 0.33;
  const high = typeof highCutoff === 'number' && Number.isFinite(highCutoff) ? highCutoff : 0.66;
  const lbl = Array.isArray(labels) && labels.length === 3 ? labels : ['low', 'medium', 'high'];
  if (s >= high) return lbl[2];
  if (s >= low) return lbl[1];
  return lbl[0];
}

function buildRankSignals({ relevance, popularity, usage, replication, freshness, availability, onchain } = {}) {
  const onchainValue = onchain === true ? 'linked' : onchain === false ? 'none' : 'unknown';
  return {
    relevance: bucket3(relevance, 0.33, 0.66),
    popularity: bucket3(popularity, 0.25, 0.6),
    usage: bucket3(usage, 0.25, 0.6),
    replication: bucket3(replication, 0.25, 0.6),
    freshness: bucket3(freshness, 0.4, 0.75, ['old', 'recent', 'new']),
    availability: bucket3(availability, 0.6, 0.85, ['bad', 'degraded', 'good']),
    onchain: onchainValue
  };
}

async function getOnchainLinkedDomains(rootsInput, { ttlMs = ONCHAIN_LINK_CACHE_TTL_MS } = {}) {
  const raw = Array.isArray(rootsInput) ? rootsInput : [];
  const roots = Array.from(new Set(raw.map((r) => String(r || '').trim()).filter(Boolean)));
  const linkedDomainsByRoot = new Map();
  if (!roots.length) return linkedDomainsByRoot;

  const now = Date.now();
  const walletDomainsCache = new Map(); // wallet -> [domains]
  const domainDetailsCache = new Map(); // domain -> details|null

  for (const root of roots) {
    const rootCidKey = cidKey(root);
    if (!rootCidKey) continue;

    const cached = onchainLinkCache.get(rootCidKey);
    if (cached && now - Number(cached.fetchedAt || 0) < ttlMs) {
      const cachedDomain = String(cached.domain || '').trim();
      if (cachedDomain) linkedDomainsByRoot.set(rootCidKey, cachedDomain);
      continue;
    }

    let bestDomain = null;

    try {
      // eslint-disable-next-line no-await-in-loop
      const wallets = await getWalletsForRootCid(root);
      const uniqueWallets = Array.from(
        new Set((wallets || []).map((w) => String(w || '').trim()).filter(Boolean))
      )
        .slice(0, 10)
        .sort();

      for (const wallet of uniqueWallets) {
        let domains = walletDomainsCache.get(wallet);
        if (!domains) {
          // eslint-disable-next-line no-await-in-loop
          domains = await fetchDomainsByOwner(wallet, { ttlMs });
          walletDomainsCache.set(wallet, domains);
        }

        const domainList = Array.isArray(domains) ? domains : [];
        const uniqueDomains = Array.from(
          new Set(domainList.map((d) => String(d || '').trim().toLowerCase()).filter(Boolean))
        )
          .sort()
          .slice(0, 30);

        for (const domainName of uniqueDomains) {
          let details = domainDetailsCache.get(domainName);
          if (details === undefined) {
            // eslint-disable-next-line no-await-in-loop
            details = await fetchDomainDetails(domainName, { ttlMs });
            domainDetailsCache.set(domainName, details || null);
          }

          const domainObj = details && (details.domain || details);
          const rootDomain = String(domainObj?.name || domainName).trim().toLowerCase();
          if (!rootDomain) continue;

          const recordsRaw = Array.isArray(domainObj?.records) ? domainObj.records : [];
          const records = recordsRaw.slice().sort((a, b) => {
            const ka = String(a?.key || '').trim().toLowerCase();
            const kb = String(b?.key || '').trim().toLowerCase();
            if (ka !== kb) return ka.localeCompare(kb);
            const va = String(a?.value || '').trim();
            const vb = String(b?.value || '').trim();
            return va.localeCompare(vb);
          });

          for (const rec of records) {
            const recordKey = String(rec?.key || '').trim();
            const recordKeyLower = recordKey.toLowerCase();
            const valueRaw = String(rec?.value || '').trim();
            if (!valueRaw) continue;

            const fullDomain =
              recordKeyLower && recordKeyLower !== 'cid' && recordKeyLower !== 'ipns'
                ? `${recordKeyLower}.${rootDomain}`
                : rootDomain;

            const valueCid = extractCidFromRecordValue(valueRaw);
            const valueCidKey = valueCid ? cidKey(valueCid) : null;
            if (valueCidKey && valueCidKey === rootCidKey) {
              bestDomain = pickBetterLinkedDomain(bestDomain, fullDomain);
              break;
            }

            const shouldTryIpns =
              recordKeyLower === 'ipns' || (!valueCid && isLikelyIpnsValue(valueRaw));
            if (shouldTryIpns) {
              const ipnsName = normalizeIpnsValue(valueRaw);
              if (!ipnsName) continue;
              // eslint-disable-next-line no-await-in-loop
              const resolved = await resolveIpnsToRootCid(ipnsName, { ttlMs });
              const resolvedKey = resolved ? cidKey(resolved) : null;
              if (resolvedKey && resolvedKey === rootCidKey) {
                bestDomain = pickBetterLinkedDomain(bestDomain, fullDomain);
                break;
              }
            }
          }

          if (bestDomain) break;
        }

        if (bestDomain) break;
      }
    } catch (err) {
      try {
        // eslint-disable-next-line no-console
        console.warn('[search:onchain] linkage check failed', {
          root,
          error: String(err?.message || err)
        });
      } catch {
        // ignore
      }
    }

    onchainLinkCache.set(rootCidKey, { fetchedAt: now, domain: bestDomain });
    if (bestDomain) linkedDomainsByRoot.set(rootCidKey, bestDomain);
  }

  return linkedDomainsByRoot;
}

function scoreHitWithQuery(hit, queryTokens) {
  let score = 0;
  const tokens = Array.isArray(queryTokens) ? queryTokens : [];
  const cid = String(hit.cid || '');

  if (tokens.length === 1 && cid === tokens[0]) {
    score += 1000;
  }

  // Bag-of-words scoring from per-CID token histogram
  const tokenCounts =
    hit.tags_json && hit.tags_json.tokens && typeof hit.tags_json.tokens === 'object'
      ? hit.tags_json.tokens
      : null;

  if (tokenCounts && tokens.length) {
    const docTerms = Object.entries(tokenCounts);
    for (const t of tokens) {
      const q = String(t || '').toLowerCase().trim();
      if (!q) continue;
      let exactAdded = false;
      let bestPartial = 0;

      for (const [term, rawCount] of docTerms) {
        const termKey = String(term || '').toLowerCase().trim();
        const count = Number(rawCount);
        if (!termKey || !Number.isFinite(count) || count <= 0) continue;

        if (termKey === q) {
          score += count * 10;
          exactAdded = true;
          continue;
        }

        // Partial match: prefix/substring coverage
        if (q.length >= 3 && termKey.length >= 3) {
          if (termKey.includes(q) || q.includes(termKey)) {
            const shared = Math.min(q.length, termKey.length);
            const coverage = shared / q.length; // fraction of query covered
            if (coverage >= 0.5) {
              const partialScore = count * 10 * coverage;
              if (partialScore > bestPartial) bestPartial = partialScore;
            }
          }
        }
      }

      if (!exactAdded && bestPartial > 0) {
        score += bestPartial;
      }
    }
  }

  const topics = (hit.tags_json && Array.isArray(hit.tags_json.topics)
    ? hit.tags_json.topics
    : hit.topics) || [];

  for (const t of tokens) {
    if (topics.includes(t)) {
      score += 100;
    }
  }

  const kind = String(hit.kind || '').toLowerCase().trim();
  if (kind && tokens.includes(kind)) {
    score += 200;
  }

  const conf =
    typeof hit.confidence === 'number' ? hit.confidence : null;
  if (conf != null) {
    score += conf * 10;
    if (conf < 0.3) score -= 1000;
    if (conf < 0.2) score -= 2000;
    if (conf < 0.1) score -= 3000;
  }

  return score;
}

function hasTokenMatchForHit(hit, queryTokens) {
  const tokens = Array.isArray(queryTokens) ? queryTokens : [];
  if (!tokens.length) return true;

  const tokenCounts =
    hit.tags_json && hit.tags_json.tokens && typeof hit.tags_json.tokens === 'object'
      ? hit.tags_json.tokens
      : null;

  if (tokenCounts) {
    const docTerms = Object.keys(tokenCounts).map((k) =>
      String(k || '').toLowerCase().trim()
    );
    for (const t of tokens) {
      const q = String(t || '').toLowerCase().trim();
      if (!q) continue;
      // exact token
      if (tokenCounts[q]) return true;
      // partial token match (prefix / substring) with sufficient coverage
      if (q.length >= 3) {
        for (const term of docTerms) {
          if (!term || term.length < 3) continue;
          if (term.includes(q) || q.includes(term)) {
            const shared = Math.min(q.length, term.length);
            const coverage = shared / q.length;
            if (coverage >= 0.5) return true;
          }
        }
      }
    }
  }

  const topics = (hit.tags_json && Array.isArray(hit.tags_json.topics)
    ? hit.tags_json.topics
    : hit.topics) || [];

  for (const t of tokens) {
    if (topics.includes(t)) return true;
  }

  return false;
}

export async function getSearch(req, res) {
  try {
    const qRaw = String(req.query?.q || '').trim();
    const modeRaw = String(req.query?.mode || '').trim();
    const mode = modeRaw.toLowerCase();
    const typeRaw = String(req.query?.type || '').trim();
    const type = typeRaw.toLowerCase();
    const strictType = normalizeStrictTypeFilter(type);
    const isSiteSearch = !strictType && (mode === 'sites' || type === 'site');
    const isExploreEverything = mode === 'everything';
    const langParam = String(req.query?.lang || '').trim();
    const analysis = classifySearch(qRaw, { lang: langParam });
    const qInfo = cleanSearch(qRaw);
    const q = qInfo.clean;
    const limitRaw = Number(req.query?.limit || 20);
    const offsetRaw = Number(req.query?.offset || 0);
    const facetRaw = String(req.query?.facet || '0');

    const limit = Math.max(
      1,
      Math.min(100, Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : 20)
    );
    const offset = Math.max(
      0,
      Number.isFinite(offsetRaw) && offsetRaw >= 0 ? offsetRaw : 0
    );
    const facet = facetRaw === '1';

    let hits = [];
    let plan = null;
    let ui = null;
    let siteResults = null;
    let directCidLookup = false;

    // IMPORTANT: CIDv0 is case-sensitive; do not use the lowercased `q` for CID detection.
    if (qRaw && isCidLike(qRaw)) {
      try {
        const resCid = await fetchCidInfo(qRaw, { timeoutMs: 800 });
        if (resCid.ok && resCid.cid) {
          directCidLookup = true;
          const row = resCid.cid;
          const rowExt = String(row.ext_guess || '').trim().toLowerCase();
          const rowPath = normalizeEntryPath(row.path);
          const isDirectory =
            row.is_directory === true || row.is_directory === 1 || row.is_directory === '1';
          let topics = [];
          if (row.tags && Array.isArray(row.tags.topics)) {
            topics = row.tags.topics
              .map((t) => String(t || '').trim().toLowerCase())
              .filter(Boolean);
          }

          const hit = {
            cid: row.cid,
            kind: row.kind || null,
            confidence:
              typeof row.confidence === 'number' ? row.confidence : null,
            mime: row.mime || null,
            size_bytes:
              typeof row.size_bytes === 'number' ? row.size_bytes : null,
            indexed_at:
              typeof row.indexed_at === 'number' ? row.indexed_at : null,
            resourceType: mapKindToResourceType(row.kind),
            tags_json: row.tags || null,
            topics
          };

          hits = strictType ? applyStrictTypeFilter([hit], strictType) : [hit];

          // Site mode: return a stable "site" entry for the CID itself so the UI can render it
          // consistently (domain may be resolved separately).
          if (isSiteSearch && hits.length > 0) {
            const rowTitle =
              row.tags && typeof row.tags.title === 'string' ? row.tags.title : '';
            const isHtmlDirect =
              !looksLikeDirectoryListingTitle(rowTitle) &&
              ((rowPath ? isHtmlPath(rowPath) : false) ||
                rowExt === 'html' ||
                rowExt === 'htm');

            let entryCid = String(row.cid || '').trim();
            let entryPath = null;

            // If the CID is a directory, try to find an HTML/HTM child as an entry path.
            let looksDirectory = isDirectory;
            if (!looksDirectory && entryCid) {
              try {
                const resChildren = await fetchChildren(entryCid, { timeoutMs: 800 });
                const children = resChildren.ok && Array.isArray(resChildren.children)
                  ? resChildren.children
                  : [];
                looksDirectory = children.length > 0;
              } catch {
                // ignore
              }
            }

            if ((!isHtmlDirect || looksDirectory) && looksDirectory && entryCid) {
              try {
                const resChildren = await fetchChildren(entryCid, { timeoutMs: 800 });
                const children = resChildren.ok && Array.isArray(resChildren.children)
                  ? resChildren.children
                  : [];
                const maxProbe = 20;
                for (const childCid of children.slice(0, maxProbe)) {
                  // eslint-disable-next-line no-await-in-loop
                  const childInfo = await fetchCidInfo(childCid, { timeoutMs: 800 });
                  if (!childInfo.ok || !childInfo.cid) continue;
                  const c = childInfo.cid;
                  const cExt = String(c.ext_guess || '').trim().toLowerCase();
                  const cRoot = String(c.root_cid || '').trim();
                  const cPath = normalizeEntryPath(c.path);
                  const isHtmlChild =
                    (cPath ? isHtmlPath(cPath) : false) ||
                    (!cPath && (cExt === 'html' || cExt === 'htm'));
                  if (!isHtmlChild) continue;
                  // Ensure the path is relative to the directory CID we are resolving.
                  if (cRoot && cRoot !== entryCid) continue;
                  if (cPath) {
                    entryPath = cPath;
                    break;
                  }
                }
              } catch {
                // ignore
              }
            }

            // Only treat it as a "site" in site mode if it resolves to HTML/HTM.
            if ((!looksDirectory && isHtmlDirect) || !!entryPath) {
              const tags = extractSiteTagsFromCidInfo(row);
              const title =
                row.tags && typeof row.tags.title === 'string'
                  ? row.tags.title
                  : null;
              const snippet =
                row.tags && typeof row.tags.description === 'string'
                  ? row.tags.description
                  : null;

              siteResults = [
                {
                  type: 'site',
                  domain: null,
                  rootDomain: null,
                  record: '',
                  recordType: 'cid',
                  cid: entryCid,
                  entry_cid: entryCid,
                  entry_path: entryPath,
                  wallet: null,
                  score: 1,
                  owned: false,
                  tags,
                  title,
                  snippet
                }
              ];
              hits = [];
            } else {
              siteResults = [];
              hits = [];
              ui = { state: 'no_results', reason: 'not_html' };
            }
          }
        }
      } catch {
        // fall through to normal flow on error
      }
    }

    if (!directCidLookup) {
      if (isExploreEverything) {
        // Explore Everything: stable, predictable scan across a curated set of "content" kinds.
        // Always include html/text/image/doc so PDFs/EPUB/DOCX can be found via ext/mime filtering.
        plan = buildSearchQuery({
          intent: 'unknown',
          target: 'mixed',
          limit,
          offset
        });
        plan.noQuery = false;
        plan.targetKind = null;
        plan.baseKinds = ['html', 'text', 'image', 'doc'];
      } else {
        plan = buildSearchQuery({
          intent: analysis.intent,
          target: isSiteSearch ? 'site' : analysis.target,
          limit,
          offset
        });
      }

      if (q && !(await isMeaningfulQuery(q))) {
        ui = {
          state: 'no_results',
          reason: 'no_index_signal'
        };
      } else if (plan.intent !== 'navigation') {
        const formatQuery = isExploreEverything ? normalizeFormatQuery(q) : null;
        const isVeryShortQuery = !formatQuery && qInfo && qInfo.length > 0 && qInfo.length <= 2;
        const queryTokens = formatQuery ? [formatQuery] : extractTokens(q);
        const preFilter = isSiteSearch
          ? isHtmlSiteHit
          : isExploreEverything
            ? isExploreAllowedHit
            : null;
        hits = await executePlanAgainstIndexerPaged(
          plan,
          queryTokens,
          preFilter ? { preFilter } : undefined
        );
        if (hits.length === 0 && (isSiteSearch || analysis.intent !== 'unknown' || analysis.target !== 'mixed')) {
          plan = buildSearchQuery({
            intent: 'unknown',
            target: 'mixed',
            limit,
            offset
          });
          if (!plan.noQuery) {
            hits = await executePlanAgainstIndexerPaged(
              plan,
              queryTokens,
              preFilter ? { preFilter } : undefined
            );
          }
        }

        if (!hits.length && isVeryShortQuery) {
          // UX: do not show "no results" for 1-2 char queries. They are too ambiguous and often used
          // to probe Explore/Trending ordering. Fall back to "explore" results instead.
          hits = await executePlanAgainstIndexerPaged(
            plan,
            [],
            preFilter ? { preFilter } : undefined
          );
        }

        if (!hits.length) {
          ui = {
            state: 'no_results',
            reason: 'no_match'
          };
        }
      }

      hits = filterOctetStreamHits(hits);
      if (isSiteSearch && hits.length > 0) {
        hits = filterHtmlSiteHits(hits);
      }
      if (isExploreEverything && hits.length > 0) {
        hits = hits.filter((h) => isExploreAllowedHit(h));
      }
      if ((!hits || hits.length === 0) && !ui) {
        ui = { state: 'no_results', reason: isSiteSearch ? 'html_filter' : 'mime_filter' };
      }
    } else if ((!hits || hits.length === 0) && !siteResults && !ui) {
      ui = {
        state: 'no_results',
        reason: strictType ? 'type_filter' : 'no_match'
      };
    }

    if (isSiteSearch && !siteResults && hits.length > 0) {
      try {
        // STEP 1: candidate CIDs and root CIDs from content hits
        const candidateCidSet = new Set();
        const candidateCidKeySet = new Set();
        const rootMeta = new Map();
        const rootsWithDomains = new Set();
        let maxContentScore = 0;

        for (const hit of hits) {
          const cid = String(hit.cid || '').trim();
          const rootCid = String(hit.root_cid || cid).trim();
          if (cid) {
            candidateCidSet.add(cid);
            const k = cidKey(cid);
            if (k) candidateCidKeySet.add(k);
          }
          if (rootCid) {
            candidateCidSet.add(rootCid);
            const k = cidKey(rootCid);
            if (k) candidateCidKeySet.add(k);
          }

          if (rootCid) {
            const contentScore =
              typeof hit._score === 'number' && Number.isFinite(hit._score)
                ? hit._score
                : 0;
            const existing = rootMeta.get(rootCid) || {
              contentScore: 0,
              indexedAt: 0,
              title: null,
              snippet: null,
              entryPath: null
            };
            if (contentScore > existing.contentScore) {
              existing.contentScore = contentScore;
              if (typeof hit.title === 'string' && hit.title.trim()) {
                existing.title = hit.title.trim();
              }
              if (typeof hit.snippet === 'string' && hit.snippet.trim()) {
                existing.snippet = hit.snippet.trim();
              }
              const p = normalizeEntryPath(hit.path);
              if (p && isHtmlPath(p)) existing.entryPath = p;
            }
            const idx =
              typeof hit.indexed_at === 'number' && Number.isFinite(hit.indexed_at)
                ? hit.indexed_at
                : 0;
            if (idx > existing.indexedAt) {
              existing.indexedAt = idx;
            }
            rootMeta.set(rootCid, existing);
            if (existing.contentScore > maxContentScore) {
              maxContentScore = existing.contentScore;
            }
          }
        }

        if (rootMeta.size > 0) {
          // STEP 2: wallets per root, STEP 3: domains per wallet, STEP 4: records matching CIDs
          const walletDomainsCache = new Map();
          const domainDetailsCache = new Map();
          const parentCache = new Map();
          const childrenCache = new Map();
          const kuboLsCache = new Map();
          const sitesMap = new Map();
          const cidInfoCache = new Map(); // cid -> { ok, cid, tags, title, snippet, kind, mime, ext, isDirectory, rootCid, path }

          const getCidInfoCached = async (cid) => {
            const key = String(cid || '').trim();
            if (!key) {
              return {
                ok: false,
                cid: null,
                tags: [],
                title: null,
                snippet: null,
                kind: null,
                mime: null,
                ext: '',
                isDirectory: false,
                rootCid: null,
                path: null
              };
            }

            if (cidInfoCache.has(key)) {
              const cached = cidInfoCache.get(key);
              return cached && typeof cached === 'object'
                ? cached
                : {
                    ok: false,
                    cid: key,
                    tags: [],
                    title: null,
                    snippet: null,
                    kind: null,
                    mime: null,
                    ext: '',
                    isDirectory: false,
                    rootCid: null,
                    path: null
                  };
            }

            let ok = false;
            let tags = [];
            let title = null;
            let snippet = null;
            let kind = null;
            let mime = null;
            let ext = '';
            let isDirectory = false;
            let rootCid = null;
            let path = null;
            let siteEntryPath = null;
            let siteEntryCid = null;
            let contentClass = null;

            try {
              const info = await fetchCidInfo(key, { timeoutMs: 2000 });
              if (info.ok && info.cid) {
                ok = true;
                const row = info.cid;
                tags = extractSiteTagsFromCidInfo(row);
                if (row.tags && typeof row.tags.title === 'string') {
                  title = row.tags.title;
                }
                if (row.tags && typeof row.tags.description === 'string') {
                  snippet = row.tags.description;
                }
                if (row.tags && typeof row.tags.content_class === 'string') {
                  contentClass = row.tags.content_class;
                }
                kind = row.kind || null;
                mime = row.mime || null;
                ext = String(row.ext_guess || '').trim().toLowerCase();
                isDirectory =
                  row.is_directory === true || row.is_directory === 1 || row.is_directory === '1';
                rootCid = String(row.root_cid || row.cid || '').trim() || null;
                path = normalizeEntryPath(row.path);
                siteEntryPath = normalizeEntryPath(row.site_entry_path);
                siteEntryCid = String(row.site_entry_cid || '').trim() || null;
              }
            } catch {
              // ignore
            }

            const out = {
              ok,
              cid: key,
              tags,
              title,
              snippet,
              kind,
              mime,
              ext,
              isDirectory,
              rootCid,
              path,
              siteEntryPath,
              siteEntryCid,
              contentClass
            };
            cidInfoCache.set(key, out);
            return out;
          };

          const getChildrenCached = async (cid) => {
            const key = String(cid || '').trim();
            if (!key) return [];
            if (childrenCache.has(key)) {
              const cached = childrenCache.get(key);
              return Array.isArray(cached) ? cached : [];
            }
            try {
              const resChildren = await fetchChildren(key, { timeoutMs: 800 });
              const children =
                resChildren.ok && Array.isArray(resChildren.children)
                  ? resChildren.children
                  : [];
              const uniq = Array.from(
                new Set(
                  children
                    .map((c) => String(c || '').trim())
                    .filter(Boolean)
                )
              );
              childrenCache.set(key, uniq);
              return uniq;
            } catch {
              childrenCache.set(key, []);
              return [];
            }
          };

          const kuboLsCached = async (cidOrPath) => {
            const raw = String(cidOrPath || '').trim();
            if (!raw) return { ok: false, entries: [], error: 'empty' };
            if (kuboLsCache.has(raw)) {
              const cached = kuboLsCache.get(raw);
              return cached && typeof cached === 'object'
                ? cached
                : { ok: false, entries: [], error: 'cache_bad' };
            }
            try {
              const url = new URL('/api/v0/ls', 'http://kubo.local');
              url.searchParams.set('arg', raw);
              url.searchParams.set('resolve-type', 'true');
              const resp = await kuboRequest(`${url.pathname}?${url.searchParams.toString()}`);
              if (!resp.ok) {
                const text = await resp.text().catch(() => '');
                const out = { ok: false, entries: [], error: text || `http_${resp.status}` };
                kuboLsCache.set(raw, out);
                return out;
              }
              const json = await resp.json().catch(() => null);
              const links =
                Array.isArray(json?.Objects) && json.Objects.length > 0
                  ? json.Objects[0]?.Links || []
                  : Array.isArray(json?.Links)
                    ? json.Links
                    : [];
              const entries = Array.isArray(links)
                ? links
                    .map((obj) => {
                      const type = obj?.Type === 1 ? 'dir' : obj?.Type === 2 ? 'file' : 'unknown';
                      return {
                        cid: String(obj?.Hash || ''),
                        name: String(obj?.Name || ''),
                        size: typeof obj?.Size === 'number' ? obj.Size : null,
                        type
                      };
                    })
                    .filter((e) => e && e.name)
                : [];
              const out = { ok: true, entries };
              kuboLsCache.set(raw, out);
              return out;
            } catch (e) {
              const out = { ok: false, entries: [], error: String(e?.message || e) };
              kuboLsCache.set(raw, out);
              return out;
            }
          };

          const pickBestHtmlNameAtLevel = (entries) => {
            const list = Array.isArray(entries) ? entries : [];
            const files = list
              .filter((e) => e && e.type === 'file' && typeof e.name === 'string' && e.name)
              .map((e) => e.name);
            if (!files.length) return null;
            const lower = files.map((f) => String(f).trim()).filter(Boolean);
            const index =
              lower.find((n) => n.toLowerCase() === 'index.html') ||
              lower.find((n) => n.toLowerCase() === 'index.htm') ||
              null;
            if (index) return index;
            const anyHtml = lower.find((n) => isHtmlPath(n)) || null;
            return anyHtml;
          };

          const findHtmlEntryPathForDirectory = async (directoryCid) => {
            const root = String(directoryCid || '').trim();
            if (!root) return null;

            // Prefer index.html at root (or nested).
            let best = null;
            let bestScore = -Infinity;

            const visitedDirs = new Set([root]);
            const queue = [{ cid: root, depth: 0 }];
            const maxDepth = 2;
            const maxDirs = 25;
            const maxFiles = 200;
            let seenFiles = 0;

            while (queue.length) {
              const next = queue.shift();
              if (!next) break;
              const { cid, depth } = next;

              // eslint-disable-next-line no-await-in-loop
              const children = await getChildrenCached(cid);
              for (const childCid of children) {
                if (!childCid) continue;
                // eslint-disable-next-line no-await-in-loop
                const info = await getCidInfoCached(childCid);
                if (!info.ok) continue;

                const isDir = !!info.isDirectory;
                const childPath = info.path;
                const childRoot = info.rootCid;

                if (isDir) {
                  if (
                    depth < maxDepth &&
                    !visitedDirs.has(childCid) &&
                    visitedDirs.size < maxDirs
                  ) {
                    visitedDirs.add(childCid);
                    queue.push({ cid: childCid, depth: depth + 1 });
                  }
                  continue;
                }

                seenFiles += 1;
                if (seenFiles > maxFiles) break;

                // Only accept files that belong to this directory root and have an HTML/HTM path.
                if (childRoot && childRoot !== root) continue;
                if (!childPath || !isHtmlPath(childPath)) continue;

                const p = childPath.toLowerCase();
                let score = 0;
                if (p === 'index.html' || p === 'index.htm') score += 1000;
                if (p.endsWith('/index.html') || p.endsWith('/index.htm')) score += 500;
                score += 100; // any html gets base
                score -= p.split('/').length; // shorter path slightly better

                if (score > bestScore) {
                  bestScore = score;
                  best = childPath;
                }
              }

              if (best && bestScore >= 1000) break; // perfect root index
              if (seenFiles > maxFiles) break;
            }

            if (best) return best;

            // Fallback: if the indexer doesn't have edges/path info yet, ask the local Kubo node.
            // This also helps filter out "directory listing HTML" false-positives (e.g. directories
            // with only app.js / json / etc and no html entrypoint).
            try {
              const resRoot = await kuboLsCached(root);
              const rootEntries = resRoot.ok && Array.isArray(resRoot.entries) ? resRoot.entries : [];
              if (!rootEntries.length) return null;

              const bestAtRoot = pickBestHtmlNameAtLevel(rootEntries);
              if (bestAtRoot) return bestAtRoot;

              const visited = new Set();
              const dirQueue = [];

              for (const e of rootEntries) {
                if (!e || e.type !== 'dir') continue;
                const name = String(e.name || '').trim();
                if (!name) continue;
                visited.add(name.toLowerCase());
                dirQueue.push({ prefix: name, depth: 1 });
                if (dirQueue.length >= 15) break;
              }

              const maxDepth = 2;
              const maxDirs = 25;
              let processed = 0;

              while (dirQueue.length) {
                const cur = dirQueue.shift();
                if (!cur) break;
                processed += 1;
                if (processed > maxDirs) break;

                const resDir = await kuboLsCached(`${root}/${cur.prefix}`);
                const entries =
                  resDir.ok && Array.isArray(resDir.entries) ? resDir.entries : [];
                const bestHere = pickBestHtmlNameAtLevel(entries);
                if (bestHere) return `${cur.prefix}/${bestHere}`;

                if (cur.depth >= maxDepth) continue;
                for (const e of entries) {
                  if (!e || e.type !== 'dir') continue;
                  const name = String(e.name || '').trim();
                  if (!name) continue;
                  const nextPrefix = `${cur.prefix}/${name}`;
                  const key = nextPrefix.toLowerCase();
                  if (visited.has(key)) continue;
                  visited.add(key);
                  dirQueue.push({ prefix: nextPrefix, depth: cur.depth + 1 });
                  if (dirQueue.length >= 50) break;
                }
              }
            } catch {
              // ignore
            }

            return null;
          };

          for (const [rootCid, rootScoreMeta] of rootMeta.entries()) {
            // eslint-disable-next-line no-await-in-loop
            const wallets = await getWalletsForRootCid(rootCid);
            const uniqueWallets = Array.from(
              new Set(
                (wallets || []).map((w) => String(w || '').trim()).filter(Boolean)
              )
            );

            for (const wallet of uniqueWallets) {
              let domains = walletDomainsCache.get(wallet);
              if (!domains) {
                // eslint-disable-next-line no-await-in-loop
                domains = await fetchDomainsByOwner(wallet, {
                  ttlMs: 15 * 60 * 1000
                });
                walletDomainsCache.set(wallet, domains);
              }

              for (const name of domains || []) {
                const domainName = String(name || '').trim().toLowerCase();
                if (!domainName) continue;

                let details = domainDetailsCache.get(domainName);
                if (details === undefined) {
                  // eslint-disable-next-line no-await-in-loop
                  details = await fetchDomainDetails(domainName, {
                    ttlMs: 15 * 60 * 1000
                  });
                  domainDetailsCache.set(domainName, details);
                }
                if (!details || !details.domain) continue;

                const domainObj = details.domain;
                const records = Array.isArray(domainObj.records)
                  ? domainObj.records
                  : [];

                for (const rec of records) {
                  const recordKey = String(rec?.key || '').trim();
                  const recordKeyLower = recordKey.toLowerCase();
                  const valueRaw = String(rec?.value || '').trim();
                  if (!valueRaw) continue;

                  let recordType = 'cid';
                  let confidenceCoeff = 0;
                  let matchedCid = null;

                  const recordCid = extractCidFromRecordValue(valueRaw);
                  const recordCidKey = recordCid ? cidKey(recordCid) : null;

                  // CASE A – CID record
                  if (recordCid && recordKeyLower !== 'ipns' && !isLikelyIpnsValue(valueRaw)) {
                    if (recordCidKey && candidateCidKeySet.has(recordCidKey)) {
                      confidenceCoeff = 1.0;
                      matchedCid = recordCid;
                      recordType = 'cid';
                    } else {
                      // descendant: candidate is child of recordCid
                      // cheap check via /parents for candidateCid, single hop
                      for (const c of candidateCidSet) {
                        const cacheKey = `parents:${c}`;
                        let parents = parentCache.get(cacheKey);
                        if (!parents) {
                          // eslint-disable-next-line no-await-in-loop
                          const resParents = await fetchParents(c, {
                            timeoutMs: 500
                          });
                          parents =
                            resParents.ok && Array.isArray(resParents.parents)
                              ? resParents.parents
                              : [];
                          parentCache.set(cacheKey, parents);
                        }
                        if (
                          recordCidKey &&
                          parents
                            .map((p) => cidKey(p))
                            .filter(Boolean)
                            .includes(recordCidKey)
                        ) {
                          confidenceCoeff = 0.85;
                          // Return the canonical on-chain CID (the record value), not the matched descendant CID.
                          matchedCid = recordCid;
                          recordType = 'cid';
                          break;
                        }
                      }
                    }
                  }

                  // CASE B — IPNS record
                  if (
                    !matchedCid &&
                    (recordKeyLower === 'ipns' || (!recordCid && isLikelyIpnsValue(valueRaw)))
                  ) {
                    const ipnsName = normalizeIpnsValue(valueRaw);
                    if (!ipnsName) continue;
                    // Resolve IPNS name to a CID (with internal cache in ipnsResolver).
                    // eslint-disable-next-line no-await-in-loop
                    const resolvedCid = await resolveIpnsToRootCid(ipnsName, {
                      ttlMs: 15 * 60 * 1000
                    });

                    if (resolvedCid) {
                      const resolvedKey = cidKey(resolvedCid);
                      if (resolvedKey && candidateCidKeySet.has(resolvedKey)) {
                        confidenceCoeff = 0.9;
                        matchedCid = resolvedCid;
                        recordType = 'ipns';
                      } else {
                        // descendant: candidate is child of resolvedCid
                        // reuse the same /parents check as for CID records
                        const resolvedCidKey = cidKey(resolvedCid);
                        for (const c of candidateCidSet) {
                          const cacheKey = `parents:${c}`;
                          let parents = parentCache.get(cacheKey);
                          if (!parents) {
                            // eslint-disable-next-line no-await-in-loop
                            const resParents = await fetchParents(c, {
                              timeoutMs: 500
                            });
                            parents =
                              resParents.ok && Array.isArray(resParents.parents)
                                ? resParents.parents
                                : [];
                              parentCache.set(cacheKey, parents);
                          }
                          if (
                            resolvedCidKey &&
                            parents
                              .map((p) => cidKey(p))
                              .filter(Boolean)
                              .includes(resolvedCidKey)
                          ) {
                            confidenceCoeff = 0.8;
                            // Return the canonical CID resolved from the on-chain IPNS record.
                            matchedCid = resolvedCid;
                            recordType = 'ipns';
                            break;
                          }
                        }
                      }
                    }
                  }

                  if (!matchedCid || confidenceCoeff <= 0) continue;

                  // IMPORTANT: Do not treat *this* `rootCid` as "domain-linked" just because one of its
                  // wallets owns a domain record. The record can point to a completely different CID.
                  // Use the matched CID's root instead, so unrelated roots (e.g. other pinned sites)
                  // still appear in CID-only fallback results.
                  let matchedRootCid = matchedCid;
                  try {
                    // eslint-disable-next-line no-await-in-loop
                    const matchedInfo = await getCidInfoCached(matchedCid);
                    if (matchedInfo && matchedInfo.rootCid) matchedRootCid = matchedInfo.rootCid;
                  } catch {
                    matchedRootCid = matchedCid;
                  }
                  if (matchedRootCid) rootsWithDomains.add(matchedRootCid);

                  const rootDomain = String(domainObj.name || domainName).trim().toLowerCase();
                  if (!rootDomain) continue;

                  const fullDomain =
                    recordKeyLower && recordKeyLower.length > 0 && recordKeyLower !== 'cid' && recordKeyLower !== 'ipns'
                      ? `${recordKeyLower}.${rootDomain}`
                      : rootDomain;

                  const key = `${fullDomain}|${wallet}`;
                  const existing = sitesMap.get(key) || {
                    type: 'site',
                    domain: fullDomain,
                    rootDomain,
                    record: recordKeyLower || '',
                    recordType,
                    cid: matchedCid,
                    wallet,
                    contentScore: 0,
                    domainScore: 0,
                    confidenceCoeff: 0,
                    hits: [],
                    roots: []
                  };

                  const matchedRootScoreMeta = matchedRootCid ? rootMeta.get(matchedRootCid) : null;
                  const contentScore =
                    matchedRootScoreMeta &&
                    typeof matchedRootScoreMeta.contentScore === 'number' &&
                    Number.isFinite(matchedRootScoreMeta.contentScore)
                      ? matchedRootScoreMeta.contentScore
                      : 0;
                  const weighted = contentScore * confidenceCoeff;
                  if (weighted > existing.contentScore) {
                    existing.contentScore = weighted;
                    existing.confidenceCoeff = confidenceCoeff;
                  }
                  existing.hits.push({
                    rootCid: matchedRootCid,
                    cid: matchedCid
                  });
                  if (matchedRootCid && !existing.roots.includes(matchedRootCid)) {
                    existing.roots.push(matchedRootCid);
                  }

                  sitesMap.set(key, existing);
                }
              }
            }
          }

          const sites = Array.from(sitesMap.values());
          const maxScore = maxContentScore > 0 ? maxContentScore : 1;

          let viewsByRoot = new Map();
          try {
            const viewRoots = Array.from(
              new Set(
                [
                  ...sites.map((s) => String(s?.cid || '').trim()).filter(Boolean),
                  ...Array.from(rootMeta.keys())
                ].filter(Boolean)
              )
            );
            viewsByRoot = await getUsageStatsForCids(viewRoots, {
              sinceMs: Date.now() - POPULARITY_USAGE_WINDOW_MS
            });
          } catch {
            viewsByRoot = new Map();
          }

          const normalizedSites = [];
          for (const s of sites) {
            const baseContent =
              typeof s.contentScore === 'number' && Number.isFinite(s.contentScore)
                ? s.contentScore / maxScore
                : 0;
            const domainRelevance = scoreDomainMatch(q, s.domain);
            const finalScore = clamp01(0.7 * baseContent + 0.3 * domainRelevance);

            // Stable tags: use the canonical on-chain CID (domain record value / resolved IPNS CID),
            // not query-dependent content-hit aggregation.
            // eslint-disable-next-line no-await-in-loop
            const meta = await getCidInfoCached(s.cid);

            let entryCid = null;
            let entryPath = null;
            if (Array.isArray(s.roots) && s.roots.length > 0) {
              let best = -1;
              for (const rCid of s.roots) {
                const r = rootMeta.get(rCid);
                const sc =
                  r && typeof r.contentScore === 'number' && Number.isFinite(r.contentScore)
                    ? r.contentScore
                    : 0;
                if (sc > best) {
                  best = sc;
                  entryCid = rCid;
                  entryPath = r && r.entryPath ? r.entryPath : null;
                }
              }
            }
            if (!entryCid) entryCid = s.cid;
            if (!entryPath && entryCid && rootMeta.has(entryCid)) {
              const r = rootMeta.get(entryCid);
              entryPath = r && r.entryPath ? r.entryPath : null;
            }

            // Use the precomputed site entrypoint when available (more stable than hit-derived paths).
            // eslint-disable-next-line no-await-in-loop
            const entryRootInfo =
              entryCid === s.cid ? meta : await getCidInfoCached(entryCid);
            if (!entryPath && entryRootInfo && entryRootInfo.siteEntryPath) {
              entryPath = entryRootInfo.siteEntryPath;
            }

            // Prefer title/snippet/tags from the entrypoint leaf CID (if we have a stable mapping).
            let displayMeta = entryRootInfo || meta;
            const entryPathNorm = normalizeEntryPath(entryPath);
            if (
              entryRootInfo &&
              entryRootInfo.siteEntryCid &&
              entryRootInfo.siteEntryPath &&
              entryPathNorm &&
              normalizeEntryPath(entryRootInfo.siteEntryPath) === entryPathNorm
            ) {
              // eslint-disable-next-line no-await-in-loop
              const leaf = await getCidInfoCached(entryRootInfo.siteEntryCid);
              if (leaf && leaf.ok) displayMeta = leaf;
            }

            normalizedSites.push({
              type: 'site',
              domain: s.domain,
              rootDomain: s.rootDomain,
              record: s.record,
              recordType: s.recordType,
              cid: s.cid,
              entry_cid: entryCid,
              entry_path: entryPath,
              wallet: s.wallet,
              score: finalScore,
              views_unique_7d: (() => {
                const root = String(s.cid || '').trim();
                const usage = root ? viewsByRoot.get(root) : null;
                const okWallets = usage && typeof usage.ok_wallets === 'number' ? usage.ok_wallets : 0;
                return okWallets;
              })(),
              owned: true,
              tags: displayMeta.tags,
              title: displayMeta.title || meta.title || null,
              snippet: displayMeta.snippet || meta.snippet || null
            });
          }

          // CID-only fallback sites (no domain mapping). Still useful for discovery and recents.
          for (const [rootCid, meta] of rootMeta.entries()) {
            if (rootsWithDomains.has(rootCid)) continue;

            // eslint-disable-next-line no-await-in-loop
            const canonical = await getCidInfoCached(rootCid);

            const baseContent =
              typeof meta.contentScore === 'number' && Number.isFinite(meta.contentScore)
                ? meta.contentScore / maxScore
                : 0;

            let entryPath = meta.entryPath || null;
            if (canonical && canonical.siteEntryPath) entryPath = canonical.siteEntryPath;

            let displayMeta = canonical;
            const entryPathNorm = normalizeEntryPath(entryPath);
            if (
              canonical &&
              canonical.siteEntryCid &&
              canonical.siteEntryPath &&
              entryPathNorm &&
              normalizeEntryPath(canonical.siteEntryPath) === entryPathNorm
            ) {
              // eslint-disable-next-line no-await-in-loop
              const leaf = await getCidInfoCached(canonical.siteEntryCid);
              if (leaf && leaf.ok) displayMeta = leaf;
            }

            normalizedSites.push({
              type: 'site',
              domain: null,
              rootDomain: null,
              record: '',
              recordType: 'cid',
              cid: rootCid,
              entry_cid: rootCid,
              entry_path: entryPath,
              wallet: null,
              score: clamp01(baseContent),
              views_unique_7d: (() => {
                const root = String(rootCid || '').trim();
                const usage = root ? viewsByRoot.get(root) : null;
                const okWallets = usage && typeof usage.ok_wallets === 'number' ? usage.ok_wallets : 0;
                return okWallets;
              })(),
              owned: false,
              tags: displayMeta.tags,
              title: displayMeta.title || canonical.title || meta.title || null,
              snippet: displayMeta.snippet || canonical.snippet || meta.snippet || null
            });
          }

          const sorted = normalizedSites.sort((a, b) => {
            if (b.score !== a.score) return b.score - a.score;
            const ao = a.owned ? 1 : 0;
            const bo = b.owned ? 1 : 0;
            if (ao !== bo) return bo - ao;
            const da = a.domain || '';
            const db = b.domain || '';
            return da.localeCompare(db);
          });

          // FINAL FILTER: only return HTML/HTM entrypoints.
          // - For file CIDs: require ext/path to be .html/.htm.
          // - For directory CIDs: require a discovered `entry_path` to a .html/.htm file.
          const needed = offset + limit;
          const filteredSites = [];

          for (const s of sorted) {
            if (!s) continue;
            const entryCid = String(s.entry_cid || s.cid || '').trim();
            if (!entryCid) continue;

            let entryPath = normalizeEntryPath(s.entry_path);
            if (entryPath && !isHtmlPath(entryPath)) entryPath = null;

            // eslint-disable-next-line no-await-in-loop
            const entryInfo = await getCidInfoCached(entryCid);
            const isHtmlLike =
              isHtmlMime(entryInfo?.mime) || entryInfo?.ext === 'html' || entryInfo?.ext === 'htm';

            // If the indexer mislabels a directory as html (gateway directory listing),
            // `is_directory` may be false. Use the edges table as a fallback signal.
            // eslint-disable-next-line no-await-in-loop
            const children = await getChildrenCached(entryCid);
            let looksDirectory = !!(entryInfo.ok && entryInfo.isDirectory);
            if (!looksDirectory && entryInfo.ok && children.length > 0) {
              // UnixFS files can have internal chunk links, which appear as children in the edges table.
              // Don’t treat those as a directory signal for HTML files.
              if (!isHtmlLike) {
                looksDirectory = true;
              }
            }

            // If the indexer hasn't recorded edges yet, consult the local Kubo node.
            //
            // NOTE: Kubo `ls` on UnixFS *files* can return internal chunk links (sometimes with names),
            // which look like "children" but are not directories. In site mode, treat HTML content as a
            // file by default to avoid dropping valid single-file websites.
            const skipKuboLsDirCheck = isHtmlLike || !entryInfo.ok;
            if (!looksDirectory && !skipKuboLsDirCheck) {
              // eslint-disable-next-line no-await-in-loop
              const ls = await kuboLsCached(entryCid);
              const entries = ls.ok && Array.isArray(ls.entries) ? ls.entries : [];
              // Avoid false-positive "directory" detection for UnixFS files (chunk links).
              if (entries.length > 0 && !looksLikeUnixfsFileChunks(entries)) {
                looksDirectory = true;
              }
            }

            if (looksDirectory) {
              if (!entryPath) {
                const cachedSiteEntry = entryInfo && entryInfo.siteEntryPath ? entryInfo.siteEntryPath : null;
                if (cachedSiteEntry && isHtmlPath(cachedSiteEntry)) {
                  entryPath = cachedSiteEntry;
                }
              }
              if (!entryPath) {
                // eslint-disable-next-line no-await-in-loop
                entryPath = await findHtmlEntryPathForDirectory(entryCid);
              }
              if (!entryPath || !isHtmlPath(entryPath)) continue;

              // Exclude Kubo "directory listing" HTML from Sites results.
              const entryPathNorm = normalizeEntryPath(entryPath);
              if (
                entryInfo &&
                entryInfo.siteEntryCid &&
                entryInfo.siteEntryPath &&
                entryPathNorm &&
                normalizeEntryPath(entryInfo.siteEntryPath) === entryPathNorm
              ) {
                // eslint-disable-next-line no-await-in-loop
                const leafInfo = await getCidInfoCached(entryInfo.siteEntryCid);
                if (
                  leafInfo &&
                  (isDirectoryListingContentClass(leafInfo.contentClass) ||
                    looksLikeIpfsDirectoryListingSnippet(leafInfo.snippet) ||
                    looksLikeIpfsDirectoryListingTitleOrPath(leafInfo.title) ||
                    looksLikeDirectoryListingTitle(leafInfo.title))
                ) {
                  continue;
                }
              }

              filteredSites.push({
                ...s,
                entry_cid: entryCid,
                entry_path: entryPath
              });
            } else {
              const infoPath = entryInfo && entryInfo.path ? entryInfo.path : null;
              const titleForHeuristics =
                entryInfo && entryInfo.ok && typeof entryInfo.title === 'string' ? entryInfo.title : s.title;
              const snippetForHeuristics =
                entryInfo && entryInfo.ok && typeof entryInfo.snippet === 'string'
                  ? entryInfo.snippet
                  : s.snippet;
              if (
                isDirectoryListingContentClass(entryInfo.contentClass) ||
                looksLikeIpfsDirectoryListingSnippet(snippetForHeuristics) ||
                looksLikeIpfsDirectoryListingTitleOrPath(titleForHeuristics) ||
                (!infoPath && looksLikeDirectoryListingTitle(titleForHeuristics))
              ) {
                continue;
              }
              const isHtmlFile =
                (infoPath && isHtmlPath(infoPath)) ||
                (entryInfo && (entryInfo.ext === 'html' || entryInfo.ext === 'htm')) ||
                isHtmlMime(entryInfo?.mime) ||
                !entryInfo.ok;
              if (!isHtmlFile) continue;
              filteredSites.push({
                ...s,
                entry_cid: entryCid,
                entry_path: null
              });
            }

            if (filteredSites.length >= needed) break;
          }

          siteResults = filteredSites.slice(offset, offset + limit);

          if ((!siteResults || siteResults.length === 0) && !ui) {
            ui = {
              state: 'no_results',
              reason: 'no_match'
            };
          }
        }
      } catch (err) {
        try {
          // eslint-disable-next-line no-console
          console.error('[api:/search] site mode resolve error', err);
        } catch {
          // ignore
        }
      }
    }

    if (strictType && !isSiteSearch) {
      hits = applyStrictTypeFilter(hits, strictType);
      if ((!hits || hits.length === 0) && !ui) {
        ui = { state: 'no_results', reason: 'type_filter' };
      }
    }

    const hitsPayload = Array.isArray(hits)
      ? hits.map((h) => {
          if (!h || typeof h !== 'object') return h;
          // UX: do not expose a single numeric rank/score. Sorting is done server-side.
          // Keep interpretable `rank_signals` instead.
          // eslint-disable-next-line no-unused-vars
          const { _score, ...rest } = h;
          return rest;
        })
      : hits;

    const response = {
      ok: true,
      analysis,
      params: {
        q_raw: qRaw,
        q,
        features: qInfo,
        lang: analysis.lang,
        limit,
        offset,
        facet
      },
      plan,
      hits: hitsPayload,
      ui
    };

    if (isSiteSearch) {
      response.results = Array.isArray(siteResults)
        ? siteResults.map((r) => {
            if (!r || typeof r !== 'object') return r;
            // Same UX principle: don’t expose a single numeric score.
            // eslint-disable-next-line no-unused-vars
            const { score, ...rest } = r;
            return rest;
          })
        : [];
      response.hits = [];
    }

    return sendPqJson(req, res, 200, response, 'api:/search');
  } catch (err) {
    try {
      // eslint-disable-next-line no-console
      console.error('[api:/search] error', err);
    } catch {
      // ignore
    }
    return sendPqJson(req, res, 500, { error: 'internal_error' }, 'api:/search');
  }
}

export async function postSearchPq(req, res) {
  try {
    const pqHeader = String(req.header('X-Lumen-PQ') || '').trim().toLowerCase();
    if (pqHeader !== 'v1') {
      return res.status(400).json({ error: 'pq_required', message: 'pq_required' });
    }

    const result = await decryptPqRequest(req);
    if (!result.ok) {
      return res
        .status(result.status ?? 400)
        .json({ error: result.error ?? 'auth_failed', message: result.message });
    }

    const payload = result.payload || {};
    if (result.aesKey) {
      try {
        req.pqAesKey = Buffer.from(result.aesKey);
      } catch {
        req.pqAesKey = null;
      }
    }

    const q = String(payload.q || '').trim();
    const limit = payload.limit;
    const offset = payload.offset;
    const facet = payload.facet;
    const lang = payload.lang;
    const mode = payload.mode;
    const type = payload.type;

    req.query = {
      q,
      limit,
      offset,
      facet,
      lang,
      mode,
      type
    };

    return getSearch(req, res);
  } catch (err) {
    try {
      // eslint-disable-next-line no-console
      console.error('[api:/pq/search] error', err);
    } catch {
      // ignore
    }
    return res.status(500).json({ error: 'internal_error' });
  }
}
