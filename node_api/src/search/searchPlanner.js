const ALLOWED_TARGETS = new Set([
  'site',
  'video',
  'image',
  'music',
  'doc',
  'code',
  'file',
  'media',
  'mixed'
]);

// Indexer kinds are more granular than UI targets. Keep mapping here so search plans
// query the indexer with valid `kind` filters.
function expandTargetToIndexerKinds(target) {
  const t = String(target || '').toLowerCase().trim();

  switch (t) {
    case 'site':
      // "Sites" are discovered primarily through HTML/doc/text pages, then aggregated to roots/domains.
      return ['html', 'doc', 'text'];
    case 'music':
      return ['audio'];
    case 'media':
      return ['video', 'image', 'audio'];
    case 'file':
      // Best-effort: non-media blobs and packages tend to end up here.
      return ['archive', 'package', 'ipld', 'unknown'];
    case 'code':
      // Code is usually detected as text (or packaged as archive).
      return ['text', 'archive'];
    default:
      return t ? [t] : [];
  }
}

function normalizeTarget(target) {
  const t = String(target || '').toLowerCase().trim();
  if (ALLOWED_TARGETS.has(t)) return t;
  return 'mixed';
}

export function buildSearchQuery({ intent, target, limit, offset }) {
  const cleanIntent = String(intent || '').toLowerCase().trim();
  const cleanTarget = normalizeTarget(target);

  let lim = Number.parseInt(String(limit ?? ''), 10);
  if (!Number.isFinite(lim) || lim <= 0) lim = 20;
  if (lim > 100) lim = 100;

  let off = Number.parseInt(String(offset ?? ''), 10);
  if (!Number.isFinite(off) || off < 0) off = 0;

  const plan = {
    intent: cleanIntent || 'unknown',
    baseKinds: null,
    targetKind: null,
    limit: lim,
    offset: off,
    noQuery: false
  };

  switch (plan.intent) {
    case 'navigation':
      plan.noQuery = true;
      break;

    case 'question':
      plan.baseKinds = ['doc', 'html'];
      break;

    case 'content':
      plan.baseKinds = ['video', 'image', 'audio'];
      break;

    case 'discover':
      if (!limit || !Number.isFinite(limit) || limit <= 0) {
        plan.limit = 50;
      }
      break;

    case 'download':
      plan.baseKinds = ['archive', 'package', 'doc', 'text'];
      break;

    case 'action':
    case 'unknown':
    default:
      break;
  }

  // Ensure any baseKinds are indexer-valid (expand high-level targets).
  if (Array.isArray(plan.baseKinds) && plan.baseKinds.length > 0) {
    const expanded = [];
    const seen = new Set();
    for (const k of plan.baseKinds) {
      for (const v of expandTargetToIndexerKinds(k)) {
        if (!v || seen.has(v)) continue;
        seen.add(v);
        expanded.push(v);
      }
    }
    plan.baseKinds = expanded;
  }

  if (cleanTarget !== 'mixed') {
    const expanded = expandTargetToIndexerKinds(cleanTarget);
    if (expanded.length === 1) {
      // Single indexer kind -> use `targetKind` for a narrower query.
      plan.targetKind = expanded[0];
    } else if (expanded.length > 1) {
      // Multi-kind target -> query via baseKinds and keep targetKind null.
      const seen = new Set(plan.baseKinds || []);
      plan.baseKinds = Array.isArray(plan.baseKinds) ? plan.baseKinds : [];
      for (const k of expanded) {
        if (!k || seen.has(k)) continue;
        seen.add(k);
        plan.baseKinds.push(k);
      }
    }
  }

  return plan;
}

export { normalizeTarget };
