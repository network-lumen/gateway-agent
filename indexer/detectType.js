import { fileTypeFromBuffer } from 'file-type';
import { CONFIG } from './config.js';
import { fetchWithTimeout, nowMs, readResponseBodyLimited } from './utils.js';
import { incrementIpfsRangeIgnored } from './metrics.js';
import { log, logError } from './log.js';

// Bump when detection + content analysis logic changes (e.g., AI tags, HTML text extraction).
export const DETECTOR_VERSION = 'v1';

function kindFromMime(mime) {
  if (!mime) return 'unknown';
  if (mime.startsWith('image/')) return 'image';
  if (mime === 'text/html' || mime === 'application/xhtml+xml') return 'html';
  if (mime.startsWith('text/')) return 'text';
  if (
    mime === 'application/pdf' ||
    mime.startsWith('application/msword') ||
    mime.startsWith(
      'application/vnd.openxmlformats-officedocument'
    ) ||
    mime === 'application/epub+zip'
  ) {
    return 'doc';
  }
  if (
    mime === 'application/zip' ||
    mime === 'application/x-tar' ||
    mime === 'application/x-7z-compressed' ||
    mime === 'application/x-rar-compressed'
  ) {
    return 'archive';
  }
  if (mime === 'application/vnd.ipld.car') return 'ipld';
  return 'unknown';
}

function magicConfidenceForMime(mime) {
  if (!mime) return 0.6;
  if (mime === 'application/octet-stream') return 0.6;
  if (mime === 'application/zip') return 0.9;
  return 0.98;
}

function bufferToText(buf) {
  if (!buf || buf.length === 0) return '';
  return buf.toString('utf8').toLowerCase();
}

function detectContainer({ head, tail, mid, size }) {
  const headText = bufferToText(head);
  const tailText = bufferToText(tail);
  const signals = {};

  // PDF
  if (headText.includes('%pdf-')) {
    const mime = 'application/pdf';
    signals.container = { type: 'pdf' };
    return {
      mime,
      ext_guess: 'pdf',
      kind: 'doc',
      confidence: 0.95,
      source: 'container',
      signals
    };
  }

  // ZIP and derivatives (docx/xlsx/pptx/epub/apk)
  if (head && head.length >= 4 && head[0] === 0x50 && head[1] === 0x4b) {
    const midText = bufferToText(mid);
    const combinedText = headText + midText + tailText;
    let ext = 'zip';
    let mime = 'application/zip';
    const container = { type: 'zip' };

    if (combinedText.includes('word/')) {
      ext = 'docx';
      mime =
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document';
      container.subtype = 'docx';
    } else if (combinedText.includes('xl/')) {
      ext = 'xlsx';
      mime =
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
      container.subtype = 'xlsx';
    } else if (combinedText.includes('ppt/')) {
      ext = 'pptx';
      mime =
        'application/vnd.openxmlformats-officedocument.presentationml.presentation';
      container.subtype = 'pptx';
    } else if (
      combinedText.includes('mimetypeapplication/epub+zip') ||
      combinedText.includes('application/epub+zip')
    ) {
      ext = 'epub';
      mime = 'application/epub+zip';
      container.subtype = 'epub';
    } else if (
      combinedText.includes('androidmanifest.xml') ||
      combinedText.includes('classes.dex') ||
      combinedText.includes('resources.arsc')
    ) {
      ext = 'apk';
      mime = 'application/vnd.android.package-archive';
      container.subtype = 'apk';
    }

    signals.container = container;
    return {
      mime,
      ext_guess: ext,
      kind:
        container.subtype === 'epub'
          ? 'doc'
          : container.subtype === 'apk'
          ? 'package'
          : 'archive',
      confidence: container.subtype ? 0.97 : 0.9,
      source: 'container',
      signals
    };
  }

  // HTML sniff
  if (
    headText.includes('<html') ||
    headText.includes('<!doctype html') ||
    headText.includes('<head') ||
    headText.includes('<body')
  ) {
    signals.container = { type: 'html-sniff' };
    return {
      mime: 'text/html',
      ext_guess: 'html',
      kind: 'html',
      confidence: 0.9,
      source: 'container',
      signals
    };
  }

  // Simple CAR heuristic (very weak, best-effort)
  if (head && head.length >= 4) {
    const first = head[0];
    const second = head[1];
    if (first === 0x0a && (second === 0x01 || second === 0xa1)) {
      signals.container = { type: 'car' };
      return {
        mime: 'application/vnd.ipld.car',
        ext_guess: 'car',
        kind: 'ipld',
        confidence: 0.85,
        source: 'container',
        signals
      };
    }
  }

  return null;
}

function isMostlyText(buf) {
  if (!buf || buf.length === 0) return false;
  const len = Math.min(buf.length, 4096);
  let ascii = 0;
  for (let i = 0; i < len; i += 1) {
    const c = buf[i];
    if (c === 0) return false;
    if (c >= 9 && c <= 13) {
      ascii += 1;
    } else if (c >= 32 && c < 127) {
      ascii += 1;
    }
  }
  return ascii / len > 0.8;
}

function looksLikePdfObjectStreamText(buf) {
  if (!buf || !buf.length) return false;
  const sample = buf.subarray(0, Math.min(buf.length, 8192)).toString('utf8');
  const lower = sample.toLowerCase();

  // PDF xref tables can be almost pure numbers: "0024721696 00000 n"
  const xrefLineMatches = lower.match(/\b\d{6,12}\s+\d{5}\s+[nf]\b/g) || [];
  if (xrefLineMatches.length >= 3) {
    const nonXref = lower.replace(/[0-9nf\s]/g, '');
    if (nonXref.length <= 10) return true;
  }

  const hasObjDecl = /\b\d+\s+\d+\s+obj\b/.test(lower);
  const hasEndobj = lower.includes('endobj');
  const hasXref = lower.includes('xref');
  const hasTrailer = lower.includes('trailer');
  const hasStream = lower.includes('stream');
  const hasEndstream = lower.includes('endstream');

  let score = 0;
  if (hasObjDecl) score += 2;
  if (hasEndobj) score += 1;
  if (hasXref) score += 1;
  if (hasTrailer) score += 1;
  if (hasStream) score += 1;
  if (hasEndstream) score += 1;

  if (score >= 4) return true;

  // Fallback: PDFs often expose internal dictionaries in text-like streams.
  // Accept a "dictionary-heavy" signature even if the object header isn't present in the sample.
  const hasFlate = lower.includes('flatedecode');
  const hasXobject = lower.includes('xobject');
  const hasColorspace = lower.includes('colorspace');
  const hasBits = lower.includes('bitspercomponent');
  const hasMediabox = lower.includes('mediabox');
  const hasCropbox = lower.includes('cropbox');
  const hasResources = lower.includes('resources');
  const hasFont = lower.includes('font');

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

function normalizeMimeHeader(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return null;
  const primary = raw.split(';')[0]?.trim();
  return primary || null;
}

async function probeHead(cid) {
  const url = new URL(`/ipfs/${cid}`, CONFIG.IPFS_GATEWAY_BASE).toString();
  try {
    const resp = await fetchWithTimeout(
      url,
      { method: 'HEAD' },
      { retries: 1 }
    );
    if (!resp.ok) return null;

    const lenHeader = resp.headers.get('content-length');
    const size = lenHeader ? Number.parseInt(lenHeader, 10) : null;
    const sizeBytes =
      Number.isFinite(size) && size >= 0 ? size : null;

    const contentType = normalizeMimeHeader(resp.headers.get('content-type'));

    return {
      sizeBytes,
      contentType
    };
  } catch (err) {
    logError('HEAD probe failed for cid', cid, err);
    return null;
  }
}

async function fetchRange(cid, start, end, httpMeta) {
  const url = new URL(`/ipfs/${cid}`, CONFIG.IPFS_GATEWAY_BASE).toString();
  const rangeHeader = `bytes=${start}-${end}`;
  const resp = await fetchWithTimeout(
    url,
    {
      method: 'GET',
      headers: {
        Range: rangeHeader
      }
    },
    { retries: 2 }
  );

  const contentRange = resp.headers.get('content-range');
  const hasContentRange = !!contentRange;
  const supportsRange = resp.status === 206 || hasContentRange;
  const rangeIgnored = resp.status === 200 && !hasContentRange;

  if (!resp.ok && resp.status !== 206 && resp.status !== 200) {
    throw new Error(
      `gateway range fetch failed (${resp.status}) for cid=${cid}`
    );
  }

  if (rangeIgnored) {
    if (httpMeta) {
      httpMeta.anyRangeIgnored = true;
      if (httpMeta.supportsRange == null) {
        httpMeta.supportsRange = false;
      }
    }
    try {
      await incrementIpfsRangeIgnored(1);
    } catch (err) {
      logError('incrementIpfsRangeIgnored error', err);
    }
    log(
      '[detectType] gateway range ignored, falling back to capped read',
      { cid, status: resp.status }
    );
  } else if (supportsRange && httpMeta && httpMeta.supportsRange == null) {
    httpMeta.supportsRange = true;
  }

  const rangeLen = end - start + 1;
  const buf = await readResponseBodyLimited(resp, rangeLen);
  return buf;
}

async function sampleCid(cid, size) {
  const sampleBytes = CONFIG.SAMPLE_BYTES;
  const maxTotal = CONFIG.MAX_TOTAL_BYTES;

  const segments = { head: null, tail: null, mid: null };
  let total = 0;
  const httpMeta = {
    supportsRange: null,
    anyRangeIgnored: false
  };

  // Head
  const headEnd =
    typeof size === 'number' && size > 0
      ? Math.min(size - 1, sampleBytes - 1)
      : sampleBytes - 1;
  segments.head = await fetchRange(cid, 0, headEnd, httpMeta);
  total += segments.head.length;

  if (typeof size === 'number' && size > sampleBytes && total < maxTotal) {
    const tailStart = Math.max(size - sampleBytes, 0);
    const tailEnd = size - 1;
    if (tailStart > headEnd) {
      segments.tail = await fetchRange(cid, tailStart, tailEnd, httpMeta);
      total += segments.tail.length;
    }
  }

  if (
    typeof size === 'number' &&
    size > 2 * sampleBytes &&
    total < maxTotal
  ) {
    const midStart = Math.max(
      Math.floor(size / 2 - sampleBytes / 2),
      0
    );
    const midEnd = Math.min(midStart + sampleBytes - 1, size - 1);
    if (
      (!segments.tail || midEnd < size - sampleBytes) &&
      midStart > headEnd
    ) {
      segments.mid = await fetchRange(cid, midStart, midEnd, httpMeta);
      total += segments.mid.length;
    }
  }

  return {
    ...segments,
    totalBytes: total,
    size,
    httpMeta
  };
}

function buildFallback(head, size) {
  const textLike = isMostlyText(head);
  const looksLikePdfObject = textLike && looksLikePdfObjectStreamText(head);
  const mime =
    textLike && !looksLikePdfObject
      ? 'text/plain'
      : 'application/octet-stream';
  return {
    mime,
    ext_guess: textLike && !looksLikePdfObject ? 'txt' : null,
    kind: textLike && !looksLikePdfObject ? 'text' : 'unknown',
    confidence: textLike && !looksLikePdfObject ? 0.7 : 0.4,
    source: 'heuristic',
    signals: { heuristic: { textLike, size, pdf_object_syntax: looksLikePdfObject } }
  };
}

async function magicDetect(head) {
  if (!head || head.length === 0) return null;
  const ft = await fileTypeFromBuffer(head);
  if (!ft) return null;
  const confidence = magicConfidenceForMime(ft.mime);
  const kind = kindFromMime(ft.mime);
  return {
    mime: ft.mime,
    ext_guess: ft.ext,
    kind,
    confidence,
    source: 'magic',
    signals: { magic: { ...ft, confidence } }
  };
}

function mergeSignals(target, extra) {
  if (!extra) return target;
  for (const [k, v] of Object.entries(extra)) {
    if (target[k] == null) {
      target[k] = v;
    } else if (typeof target[k] === 'object' && typeof v === 'object') {
      target[k] = { ...target[k], ...v };
    }
  }
  return target;
}

async function magikaDetect(sample, size) {
  if (!CONFIG.MAGIKA_URL) return null;
  try {
    const body = {
      size,
      head_base64: sample.head
        ? sample.head.toString('base64')
        : null,
      tail_base64: sample.tail
        ? sample.tail.toString('base64')
        : null
    };
    const resp = await fetchWithTimeout(CONFIG.MAGIKA_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    if (!data || !data.mime) return null;

    const mime = data.mime;
    const extGuess = data.ext || null;
    const kind = data.kind || kindFromMime(mime);
    const confidence =
      typeof data.confidence === 'number'
        ? Math.max(0, Math.min(1, data.confidence))
        : 0.8;

    return {
      mime,
      ext_guess: extGuess,
      kind,
      confidence,
      source: 'magika',
      signals: { magika: data }
    };
  } catch (err) {
    logError('magikaDetect error', err);
    return null;
  }
}

export async function detectTypeForCid(cid, opts = {}) {
  const warnings = [];
  const startMs = nowMs();

  const head = await probeHead(cid);

  let size = opts.sizeBytes;
  if (typeof size !== 'number' || size < 0) {
    size = head?.sizeBytes ?? null;
  }
  if (size == null) {
    warnings.push('size_unknown');
  }

  const headMime = head?.contentType ?? null;
  if (headMime && (headMime.startsWith('video/') || headMime.startsWith('audio/'))) {
    warnings.push('excluded_media');

    return {
      cid,
      mime: headMime,
      ext_guess: null,
      kind: 'unknown',
      confidence: 0.7,
      source: 'head',
      signals: {
        head: { mime: headMime, size_bytes: size }
      },
      detector_version: DETECTOR_VERSION,
      indexed_at: new Date().toISOString(),
      size,
      disagreement: false,
      warnings,
      sample: {
        size,
        head_bytes: 0,
        tail_bytes: 0,
        mid_bytes: 0,
        total_bytes: 0,
        supports_range: null,
        range_ignored: null
      }
    };
  }

  let best = null;
  const signals = {};
  let disagreement = false;

  const sample = await sampleCid(cid, size);

  const httpMeta = sample.httpMeta;
  if (httpMeta) {
    const supportsRange =
      typeof httpMeta.supportsRange === 'boolean'
        ? httpMeta.supportsRange
        : null;
    const rangeIgnored = !!httpMeta.anyRangeIgnored;
    if (rangeIgnored) {
      warnings.push('range_not_supported_fallback_used');
    }
    mergeSignals(signals, {
      http: {
        supports_range: supportsRange,
        range_ignored: rangeIgnored
      }
    });
  }

  // magicDetect via file-type
  const magic = await magicDetect(sample.head);
  if (magic) {
    best = magic;
    mergeSignals(signals, magic.signals);
    const isGenericZip = magic.mime === 'application/zip';
    if (magic.confidence >= 0.95 && !isGenericZip) {
      return {
        cid,
        mime: magic.mime,
        ext_guess: magic.ext_guess,
        kind: magic.kind,
        confidence: magic.confidence,
        source: magic.source,
        signals,
        detector_version: DETECTOR_VERSION,
        indexed_at: new Date().toISOString(),
        size,
        disagreement: false,
        warnings,
        sample: {
          size,
          head_bytes: sample.head?.length ?? 0,
          tail_bytes: sample.tail?.length ?? 0,
          mid_bytes: sample.mid?.length ?? 0,
          total_bytes: sample.totalBytes,
          supports_range:
            typeof sample.httpMeta?.supportsRange === 'boolean'
              ? sample.httpMeta.supportsRange
              : null,
          range_ignored: !!sample.httpMeta?.anyRangeIgnored
        }
      };
    }
  }

  // containerDetect({head, tail, mid, size})
  const container = detectContainer({
    head: sample.head,
    tail: sample.tail,
    mid: sample.mid,
    size
  });
  if (container) {
    if (best && (best.mime !== container.mime || best.kind !== container.kind)) {
      disagreement = true;
    }
    if (!best || container.confidence >= best.confidence) {
      best = container;
    }
    mergeSignals(signals, container.signals);
    if (container.confidence >= 0.85) {
      return {
        cid,
        mime: best.mime,
        ext_guess: best.ext_guess,
        kind: best.kind,
        confidence: best.confidence,
        source: best.source,
        signals,
        detector_version: DETECTOR_VERSION,
        indexed_at: new Date().toISOString(),
        size,
        disagreement,
        warnings,
        sample: {
          size,
          head_bytes: sample.head?.length ?? 0,
          tail_bytes: sample.tail?.length ?? 0,
          mid_bytes: sample.mid?.length ?? 0,
          total_bytes: sample.totalBytes,
          supports_range:
            typeof sample.httpMeta?.supportsRange === 'boolean'
              ? sample.httpMeta.supportsRange
              : null,
          range_ignored: !!sample.httpMeta?.anyRangeIgnored
        }
      };
    }
  }

  // optional Magika fallback if configured
  const magika = await magikaDetect(sample, size);
  if (magika) {
    if (best && (best.mime !== magika.mime || best.kind !== magika.kind)) {
      disagreement = true;
    }
    if (!best || magika.confidence >= best.confidence) {
      best = magika;
    }
    mergeSignals(signals, magika.signals);
  }

  // fallback
  if (!best) {
    best = buildFallback(sample.head, size);
    mergeSignals(signals, best.signals);
  }

  const durationMs = nowMs() - startMs;
  mergeSignals(signals, { timing_ms: durationMs });

  return {
    cid,
    mime: best.mime,
    ext_guess: best.ext_guess || null,
    kind: best.kind,
    confidence: best.confidence,
    source: best.source,
    signals,
    detector_version: DETECTOR_VERSION,
    indexed_at: new Date().toISOString(),
    size,
    disagreement,
    warnings,
    sample: {
      size,
      head_bytes: sample.head?.length ?? 0,
      tail_bytes: sample.tail?.length ?? 0,
      mid_bytes: sample.mid?.length ?? 0,
      total_bytes: sample.totalBytes,
      supports_range:
        typeof sample.httpMeta?.supportsRange === 'boolean'
          ? sample.httpMeta.supportsRange
          : null,
      range_ignored: !!sample.httpMeta?.anyRangeIgnored
    }
  };
}
