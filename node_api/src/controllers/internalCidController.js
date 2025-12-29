import { getWalletsForRootCid } from '../lib/walletDb.js';
import {
  fetchDomainsByOwner,
  fetchDomainDetails,
  domainHasCid
} from '../lib/dnsClient.js';

function scoreDomainMatch(query, domainName) {
  const qTokens = String(query || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .split(/\s+/)
    .filter((t) => t.length >= 2);

  const d = String(domainName || '').toLowerCase().trim();
  if (!qTokens.length || !d) return 0;

  const lastDot = d.lastIndexOf('.');
  if (lastDot <= 0 || lastDot === d.length - 1) return 0;

  const label = d.slice(0, lastDot);
  const ext = d.slice(lastDot + 1);
  if (!label || !ext) return 0;

  const wLabel = 0.8;
  const wExt = 0.2;

  let extensionScore = 0;
  for (const tok of qTokens) {
    if (tok === ext) {
      extensionScore = 1;
      break;
    }
  }

  let labelAccum = 0;
  for (const tok of qTokens) {
    if (label.includes(tok)) {
      labelAccum += tok.length / label.length;
    }
  }
  const labelScore = Math.min(labelAccum, 1);

  let score = wLabel * labelScore + wExt * extensionScore;
  if (score <= 0) return 0;
  if (score > 1) score = 1;
  return score;
}

export async function postRootsDomains(req, res) {
  try {
    const qRaw = String(req.query?.q || '').trim();
    const rawList = Array.isArray(req.body?.roots) ? req.body.roots : [];
    const roots = Array.from(
      new Set(
        rawList
          .map((r) => String(r || '').trim())
          .filter((r) => r.length > 0)
      )
    );

    if (!roots.length) {
      return res.status(400).json({ error: 'roots_required' });
    }

    const perRoot = {};
    const walletDomainsCache = new Map();
    const domainDetailsCache = new Map();

    for (const root of roots) {
      const entry = {
        wallets: [],
        domains: []
      };
      perRoot[root] = entry;

      try {
        // eslint-disable-next-line no-await-in-loop
        const wallets = await getWalletsForRootCid(root);
        entry.wallets = wallets;

        const domainSet = new Set();

        for (const wallet of wallets) {
          let domains = walletDomainsCache.get(wallet);
          if (!domains) {
            // eslint-disable-next-line no-await-in-loop
            domains = await fetchDomainsByOwner(wallet);
            walletDomainsCache.set(wallet, domains);
          }

          for (const name of domains) {
            if (!name) continue;
            let details = domainDetailsCache.get(name);
            if (details === undefined) {
              // eslint-disable-next-line no-await-in-loop
              details = await fetchDomainDetails(name);
              domainDetailsCache.set(name, details);
            }
            if (details && domainHasCid(details.domain || details, root)) {
              domainSet.add(name);
            }
          }
        }

        entry.domains = Array.from(domainSet);
      } catch (err) {
        try {
          // eslint-disable-next-line no-console
          console.error('[internal:roots_domains] error', {
            root,
            error: String(err?.message || err)
          });
        } catch {
          // ignore
        }
      }
    }

    let rankedDomains = [];

    if (qRaw) {
      const domainAgg = new Map();

      for (const [root, entry] of Object.entries(perRoot)) {
        const rootId = String(root || '').trim();
        if (!rootId) continue;
        const list = Array.isArray(entry.domains) ? entry.domains : [];
        for (const name of list) {
          const key = String(name || '').trim().toLowerCase();
          if (!key) continue;
          let agg = domainAgg.get(key);
          if (!agg) {
            agg = { name: key, roots: new Set() };
            domainAgg.set(key, agg);
          }
          agg.roots.add(rootId);
        }
      }

      rankedDomains = Array.from(domainAgg.values())
        .map((agg) => {
          const rootsCount = agg.roots.size;
          const baseScore = scoreDomainMatch(qRaw, agg.name);
          const bonus = Math.min(rootsCount * 0.1, 0.2);
          let score = baseScore + bonus;
          if (score > 1) score = 1;
          return {
            name: agg.name,
            roots_count: rootsCount,
            base_score: baseScore,
            bonus,
            score
          };
        })
        .filter((d) => d.score > 0)
        .sort((a, b) => {
          if (b.score !== a.score) return b.score - a.score;
          return a.name.localeCompare(b.name);
        });
    }

    return res.json({
      ok: true,
      roots: perRoot,
      domains_ranked: rankedDomains
    });
  } catch (err) {
    try {
      // eslint-disable-next-line no-console
      console.error('[internal:roots_domains] error', err);
    } catch {
      // ignore
    }
    return res.status(500).json({ error: 'internal_error' });
  }
}
