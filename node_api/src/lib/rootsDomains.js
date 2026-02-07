export function scoreDomainMatch(query, domainName) {
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
