import crypto from 'node:crypto';
import { predictIntent, predictTarget, resolveLang } from '../search/modelLifecycle.js';
import { buildSearchQuery } from '../search/searchPlanner.js';
import { searchCidsSimple, fetchCidInfo, fetchParents } from '../lib/indexerClient.js';
import { suggestDidYouMean } from '../search/didYouMean.js';
import { getWalletsForRootCid } from '../lib/walletDb.js';
import {
  fetchDomainsByOwner,
  fetchDomainDetails
} from '../lib/dnsClient.js';
import { scoreDomainMatch } from '../lib/rootsDomains.js';
import { resolveIpnsToRootCid } from '../lib/ipnsResolver.js';
import { decryptPqRequest } from '../middleware/pqMiddleware.js';

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
  return String(q || '')
    .split(' ')
    .map((t) => t.trim())
    .filter((t) => t.length >= 3);
}

function isCidLike(q) {
  const s = String(q || '').trim();
  if (!s) return false;
  if (/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/.test(s)) return true;
  if (/^[bB][a-z2-7]{50,}$/.test(s)) return true;
  return false;
}

function filterOctetStreamHits(hits) {
  if (!Array.isArray(hits) || hits.length === 0) return [];
  return hits.filter((hit) => {
    const mime = String(hit?.mime || '').trim().toLowerCase();
    return mime !== 'application/octet-stream';
  });
}

async function hasIndexSignal(tokens) {
  for (const token of tokens) {
    const res = await searchCidsSimple(
      { q: token, limit: 1, offset: 0 },
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
  const tokens = extractTokens(cleaned);
  if (!tokens.length) return false;

  // trivial spam like "aaaaa"
  if (/^(.)\1+$/.test(cleaned)) return false;

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
  if (k === 'video') return 'video';
  if (k === 'image') return 'image';
  return 'all';
}

function normalizeStrictTypeFilter(typeValue) {
  const t = String(typeValue || '').toLowerCase().trim();
  if (t === 'video' || t === 'videos') return 'video';
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
  if (t === 'video') return resourceType === 'video' || kind === 'video' || mime.startsWith('video/');
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

function extractHitTags(hit) {
  const tagsJson = hit && typeof hit.tags_json === 'object' ? hit.tags_json : null;
  const topicsRaw = Array.isArray(tagsJson?.topics)
    ? tagsJson.topics
    : Array.isArray(hit?.topics)
      ? hit.topics
      : [];

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
    try {
      const scored = Object.entries(tokensObj)
        .map(([k, v]) => [String(k || '').trim().toLowerCase(), Number(v)])
        .filter(([k, v]) => k && Number.isFinite(v) && v > 0)
        .sort((a, b) => {
          if (b[1] !== a[1]) return b[1] - a[1];
          return String(a[0]).localeCompare(String(b[0]));
        })
        .slice(0, 40)
        .map(([k]) => k);

      for (const k of scored) {
        if (!shouldKeepSiteTag(k)) continue;
        if (!out.includes(k)) out.push(k);
      }
    } catch {
      // ignore
    }
  }

  return out;
}

function addTagScores(scoreMap, tags, weight) {
  const w = Number.isFinite(weight) && weight > 0 ? weight : 1;
  const m = scoreMap instanceof Map ? scoreMap : new Map();
  for (const t of Array.isArray(tags) ? tags : []) {
    const key = String(t || '').trim().toLowerCase();
    if (!key) continue;
    const prev = m.get(key) || 0;
    m.set(key, prev + w);
  }
  return m;
}

function topTagsFromScores(scoreMap, limit = 20) {
  const pairs = [];
  for (const [k, v] of (scoreMap instanceof Map ? scoreMap : new Map()).entries()) {
    pairs.push([k, Number.isFinite(v) ? v : 0]);
  }
  pairs.sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return String(a[0]).localeCompare(String(b[0]));
  });
  return pairs
    .map((p) => String(p[0]))
    .filter(Boolean)
    .slice(0, Math.max(0, limit));
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

async function executePlanAgainstIndexerRaw(plan) {
  if (!plan || plan.noQuery) {
    return [];
  }

  const limit = plan.limit;
  const offset = plan.offset;
  const scanLimit = Math.min(limit + offset + 100, 200);

  let items = [];

  if (plan.targetKind) {
    const res = await searchCidsSimple(
      { kind: plan.targetKind, present: 1, limit: scanLimit, offset: 0 },
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
        { kind, present: 1, limit: scanLimit, offset: 0 },
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
      { present: 1, limit: scanLimit, offset: 0 },
      {}
    );
    if (res.ok && Array.isArray(res.items)) {
      items = res.items;
    }
  }

  items.sort((a, b) => {
    const aIdx = a && typeof a.indexed_at === 'number' ? a.indexed_at : 0;
    const bIdx = b && typeof b.indexed_at === 'number' ? b.indexed_at : 0;
    if (aIdx !== bIdx) return bIdx - aIdx;
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
        if (tagsJson && typeof tagsJson.description === 'string') {
          snippet = tagsJson.description;
        }
      }
    } catch {
      tagsJson = null;
      topics = [];
    }

    return {
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
      tags_json: tagsJson,
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

async function executePlanAgainstIndexerPaged(plan, queryTokens) {
  const allHits = await executePlanAgainstIndexerRaw(plan);
  if (!allHits.length) return [];

  const tokens = Array.isArray(queryTokens) ? queryTokens : [];

  const scored = allHits.map((hit) => ({
    ...hit,
    _score: scoreHitWithQuery(hit, tokens)
  }));

  const filtered = tokens.length
    ? scored.filter((hit) => hasTokenMatchForHit(hit, tokens))
    : scored;

  if (!filtered.length) return [];

  filtered.sort((a, b) => b._score - a._score);

  const limit = plan.limit;
  const offset = plan.offset;
  const sliced = filtered.slice(offset, offset + limit);

  return sliced.map(({ _score, ...rest }) => ({
    ...rest,
    _score
  }));
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
    let didYouMean = null;

    // IMPORTANT: CIDv0 is case-sensitive; do not use the lowercased `q` for CID detection.
    if (qRaw && isCidLike(qRaw)) {
      try {
        const resCid = await fetchCidInfo(qRaw, { timeoutMs: 800 });
        if (resCid.ok && resCid.cid) {
          const row = resCid.cid;
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
          // Do not apply the octet-stream suppression on direct CID lookup.
          // If the user (or UI) explicitly asks for a CID, return metadata even if MIME is generic.

           return res.json({
             ok: true,
             analysis,
             params: {
               q_raw: qRaw,
               q: qRaw,
               features: qInfo,
               lang: analysis.lang,
               limit,
               offset,
               facet
             },
            plan,
            hits,
            ui: hits.length
              ? ui
              : {
                  state: 'no_results',
                  reason: strictType ? 'type_filter' : 'no_match'
                }
          });
        }
      } catch {
        // fall through to normal flow on error
      }
    }

    plan = buildSearchQuery({
      intent: analysis.intent,
      target: analysis.target,
      limit,
      offset
    });

    if (q && !(await isMeaningfulQuery(q))) {
      ui = {
        state: 'no_results',
        reason: 'no_index_signal'
      };
      didYouMean = await suggestDidYouMean(q);
      if (didYouMean) {
        const meaningful = await isMeaningfulQuery(didYouMean);
        if (!meaningful) {
          didYouMean = null;
        }
      }
    } else if (plan.intent !== 'navigation') {
      const queryTokens = extractTokens(q);
      hits = await executePlanAgainstIndexerPaged(plan, queryTokens);
      if (
        hits.length === 0 &&
        (analysis.intent !== 'unknown' || analysis.target !== 'mixed')
      ) {
        plan = buildSearchQuery({
          intent: 'unknown',
          target: 'mixed',
          limit,
          offset
        });
        if (!plan.noQuery) {
          hits = await executePlanAgainstIndexerPaged(plan, queryTokens);
        }
      }

      if (!hits.length) {
        ui = {
          state: 'no_results',
          reason: 'no_match'
        };
        didYouMean = await suggestDidYouMean(q);
        if (didYouMean) {
          const meaningful = await isMeaningfulQuery(didYouMean);
          if (!meaningful) {
            didYouMean = null;
          }
        }
      }
    }

    hits = filterOctetStreamHits(hits);
    if ((!hits || hits.length === 0) && !ui) {
      ui = { state: 'no_results', reason: 'mime_filter' };
    }

    if (isSiteSearch && hits.length > 0) {
      try {
        // STEP 1: candidate CIDs and root CIDs from content hits
        const candidateCidSet = new Set();
        const rootScores = new Map();
        let maxContentScore = 0;
        const tagScoresByRootCid = new Map();
        const tagScoresByCid = new Map();

        for (const hit of hits) {
          const cid = String(hit.cid || '').trim();
          const rootCid = String(hit.root_cid || cid).trim();
          if (cid) candidateCidSet.add(cid);
          if (rootCid) candidateCidSet.add(rootCid);

          // Precompute tag score maps for later site enrichment.
          const hitScore =
            typeof hit._score === 'number' && Number.isFinite(hit._score)
              ? hit._score
              : 1;
          const hitTags = extractHitTags(hit);
          if (cid && hitTags.length) {
            const prev = tagScoresByCid.get(cid) || new Map();
            tagScoresByCid.set(cid, addTagScores(prev, hitTags, hitScore));
          }
          if (rootCid && hitTags.length) {
            const prev = tagScoresByRootCid.get(rootCid) || new Map();
            tagScoresByRootCid.set(rootCid, addTagScores(prev, hitTags, hitScore));
          }

          if (rootCid) {
            const contentScore =
              typeof hit._score === 'number' && Number.isFinite(hit._score)
                ? hit._score
                : 0;
            const prev = rootScores.get(rootCid) || 0;
            const next = contentScore > prev ? contentScore : prev;
            rootScores.set(rootCid, next);
            if (next > maxContentScore) maxContentScore = next;
          }
        }

        if (rootScores.size > 0) {
          // STEP 2: wallets per root, STEP 3: domains per wallet, STEP 4: records matching CIDs
          const walletDomainsCache = new Map();
          const domainDetailsCache = new Map();
          const parentCache = new Map();
          const sitesMap = new Map();

          for (const [rootCid, rootScore] of rootScores.entries()) {
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
                  const value = String(rec?.value || '').trim();
                  if (!value) continue;

                  let recordType = 'cid';
                  let confidenceCoeff = 0;
                  let matchedCid = null;

                  // CASE A — CID record
                  if (recordKey === 'cid') {
                    if (candidateCidSet.has(value)) {
                      confidenceCoeff = 1.0;
                      matchedCid = value;
                      recordType = 'cid';
                    } else {
                      // descendant: candidate is child of record.value
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
                        if (parents.includes(value)) {
                          confidenceCoeff = 0.85;
                          matchedCid = c;
                          recordType = 'cid';
                          break;
                        }
                      }
                    }
                  }

                  // CASE B — IPNS record
                  if (!matchedCid && recordKey === 'ipns') {
                    // Resolve IPNS name to a CID (with internal cache in ipnsResolver).
                    // eslint-disable-next-line no-await-in-loop
                    const resolvedCid = await resolveIpnsToRootCid(value, {
                      ttlMs: 15 * 60 * 1000
                    });

                    if (resolvedCid) {
                      if (candidateCidSet.has(resolvedCid)) {
                        confidenceCoeff = 0.9;
                        matchedCid = resolvedCid;
                        recordType = 'ipns';
                      } else {
                        // descendant: candidate is child of resolvedCid
                        // reuse the same /parents check as for CID records
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
                          if (parents.includes(resolvedCid)) {
                            confidenceCoeff = 0.8;
                            matchedCid = c;
                            recordType = 'ipns';
                            break;
                          }
                        }
                      }
                    }
                  }

                  if (!matchedCid || confidenceCoeff <= 0) continue;

                  const rootDomain = String(domainObj.name || domainName).trim().toLowerCase();
                  if (!rootDomain) continue;

                  const fullDomain =
                    recordKey && recordKey.length > 0 && recordKey !== 'cid' && recordKey !== 'ipns'
                      ? `${recordKey}.${rootDomain}`
                      : rootDomain;

                  const key = `${fullDomain}|${wallet}`;
                  const existing = sitesMap.get(key) || {
                    type: 'site',
                    domain: fullDomain,
                    rootDomain,
                    record: recordKey || '',
                    recordType,
                    cid: matchedCid,
                    wallet,
                    contentScore: 0,
                    domainScore: 0,
                    confidenceCoeff: 0,
                    hits: []
                  };

                  const contentScore =
                    typeof rootScore === 'number' && Number.isFinite(rootScore)
                      ? rootScore
                      : 0;
                  const weighted = contentScore * confidenceCoeff;
                  if (weighted > existing.contentScore) {
                    existing.contentScore = weighted;
                    existing.confidenceCoeff = confidenceCoeff;
                  }
                  existing.hits.push({
                    rootCid,
                    cid: matchedCid
                  });

                  sitesMap.set(key, existing);
                }
              }
            }
          }

          const sites = Array.from(sitesMap.values());
          const maxScore = maxContentScore > 0 ? maxContentScore : 1;

          // Roots already covered by a domain (via hits[]).
          const coveredRoots = new Set();
          for (const s of sites) {
            for (const h of s.hits || []) {
              if (h && h.rootCid) coveredRoots.add(h.rootCid);
            }
          }

          const normalizedSites = sites.map((s) => {
            const baseContent =
              typeof s.contentScore === 'number' && Number.isFinite(s.contentScore)
                ? s.contentScore / maxScore
                : 0;
            const domainRelevance = scoreDomainMatch(q, s.domain);
            const finalScore = clamp01(0.7 * baseContent + 0.3 * domainRelevance);

            // Attach best-effort tags derived from the underlying content hits that matched this site.
            const mergedTagScores = new Map();
            for (const h of Array.isArray(s.hits) ? s.hits : []) {
              const r = h && h.rootCid ? String(h.rootCid).trim() : '';
              const c = h && h.cid ? String(h.cid).trim() : '';
              if (r && tagScoresByRootCid.has(r)) {
                const scoreMap = tagScoresByRootCid.get(r);
                for (const [tag, score] of (scoreMap instanceof Map ? scoreMap : new Map()).entries()) {
                  const prev = mergedTagScores.get(tag) || 0;
                  mergedTagScores.set(tag, prev + (Number.isFinite(score) ? score : 0));
                }
              }
              if (c && tagScoresByCid.has(c)) {
                const scoreMap = tagScoresByCid.get(c);
                for (const [tag, score] of (scoreMap instanceof Map ? scoreMap : new Map()).entries()) {
                  const prev = mergedTagScores.get(tag) || 0;
                  mergedTagScores.set(tag, prev + (Number.isFinite(score) ? score : 0));
                }
              }
            }
            const tags = topTagsFromScores(mergedTagScores, 20);

            return {
              type: 'site',
              domain: s.domain,
              rootDomain: s.rootDomain,
              record: s.record,
              recordType: s.recordType,
              cid: s.cid,
              wallet: s.wallet,
              score: finalScore,
              tags
            };
          });

          const combined = normalizedSites;

          if (combined.length > 0) {
            siteResults = combined.sort((a, b) => {
              if (b.score !== a.score) return b.score - a.score;
              const da = a.domain || '';
              const db = b.domain || '';
              return da.localeCompare(db);
            });
          }

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
      hits,
      ui,
      did_you_mean: didYouMean
    };

    if (isSiteSearch) {
      response.results = siteResults || [];
      response.hits = [];
    }

    const aesKey = req.pqAesKey;
    if (aesKey && Buffer.isBuffer(aesKey)) {
      try {
        const plaintext = Buffer.from(JSON.stringify(response ?? null), 'utf8');
        const iv = crypto.randomBytes(12);
        const cipher = crypto.createCipheriv('aes-256-gcm', aesKey, iv);
        const ct = Buffer.concat([cipher.update(plaintext), cipher.final()]);
        const tag = cipher.getAuthTag();

        return res.json({
          ciphertext: ct.toString('base64'),
          iv: iv.toString('base64'),
          tag: tag.toString('base64')
        });
      } catch (encErr) {
        // eslint-disable-next-line no-console
        console.error('[api:/search] pq response encrypt error', encErr);
        return res.json({
          error: 'pq_encrypt_failed',
          message: 'failed_to_encrypt_response'
        });
      }
    }

    return res.json(response);
  } catch (err) {
    try {
      // eslint-disable-next-line no-console
      console.error('[api:/search] error', err);
    } catch {
      // ignore
    }
    return res.status(500).json({ error: 'internal_error' });
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
