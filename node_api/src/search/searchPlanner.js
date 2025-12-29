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
      plan.baseKinds = ['doc', 'site'];
      break;

    case 'content':
      plan.baseKinds = ['video', 'music', 'image', 'media'];
      break;

    case 'discover':
      if (!limit || !Number.isFinite(limit) || limit <= 0) {
        plan.limit = 50;
      }
      break;

    case 'download':
      plan.baseKinds = ['file', 'code', 'doc'];
      break;

    case 'action':
    case 'unknown':
    default:
      break;
  }

  if (cleanTarget !== 'mixed') {
    plan.targetKind = cleanTarget;
  }

  return plan;
}

export { normalizeTarget };
