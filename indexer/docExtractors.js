import path from 'node:path';
import JSZip from 'jszip';

function clampInt(value, def, { min = 1, max = 100000 } = {}) {
  const n = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, n));
}

function withTimeout(promise, timeoutMs, { onTimeout } = {}) {
  const ms = Number(timeoutMs);
  if (!Number.isFinite(ms) || ms <= 0) return promise;

  let timer = null;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      try {
        if (typeof onTimeout === 'function') onTimeout();
      } catch {
        // ignore
      }
      reject(new Error(`timeout_after_${ms}ms`));
    }, ms);
  });

  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

function decodeEntities(input) {
  if (!input) return '';
  const s = String(input);
  const basic = s
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");

  return basic
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
}

function squeezeWhitespace(text) {
  return String(text || '')
    .replace(/\r/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/[ \t]{2,}/g, ' ')
    .trim();
}

function stripHtmlToText(html) {
  const s = String(html || '');
  if (!s) return '';
  const noScripts = s
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ');
  const withBreaks = noScripts
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<\/div>/gi, '\n')
    .replace(/<\/li>/gi, '\n');
  const noTags = withBreaks.replace(/<[^>]+>/g, ' ');
  return squeezeWhitespace(decodeEntities(noTags));
}

function stripXmlToText(xml) {
  const raw = String(xml || '');
  if (!raw) return '';
  const noCdata = raw.replace(/<!\[CDATA\[[\s\S]*?\]\]>/g, (m) =>
    m.replace('<![CDATA[', '').replace(']]>', ' ')
  );
  const noTags = noCdata.replace(/<[^>]+>/g, ' ');
  return squeezeWhitespace(decodeEntities(noTags));
}

function parseXmlAttributes(tag) {
  const s = String(tag || '');
  const attrs = {};
  const re = /([A-Za-z_][A-Za-z0-9_:\-\.]*)\s*=\s*(['"])([\s\S]*?)\2/g;
  for (const m of s.matchAll(re)) {
    const key = String(m[1] || '').trim().toLowerCase();
    if (!key) continue;
    attrs[key] = decodeEntities(m[3] || '').trim();
  }
  return attrs;
}

function safeDecodeUriComponent(value) {
  const s = String(value ?? '');
  if (!s) return '';
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

function resolveEpubHref(opfPath, href) {
  const opf = String(opfPath || '').replace(/\\/g, '/').replace(/^\/+/, '');
  const baseDir = opf && opf !== '.' ? path.posix.dirname(opf) : '';
  const rawHref = String(href || '').trim();
  if (!rawHref) return null;
  const clean = safeDecodeUriComponent(rawHref)
    .split('#')[0]
    .split('?')[0]
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
    .replace(/^\.\//, '');
  if (!clean) return null;
  const joined = baseDir && baseDir !== '.' ? path.posix.join(baseDir, clean) : clean;
  return path.posix.normalize(joined).replace(/^\/+/, '');
}

function clampString(value, maxLen) {
  const s = String(value || '').trim();
  if (!s) return null;
  return s.length > maxLen ? s.slice(0, maxLen).trim() : s;
}

function extractFirstXmlTagText(xml, tagName) {
  const raw = String(xml || '');
  if (!raw) return null;
  const name = String(tagName || '').trim();
  if (!name) return null;
  const re = new RegExp(`<${name}\\b[^>]*>([\\s\\S]*?)<\\/${name}>`, 'gi');
  for (const m of raw.matchAll(re)) {
    const text = stripXmlToText(m[1]);
    if (text) return text;
  }
  return null;
}

function extractOpfMetadata(opfXml) {
  const xml = String(opfXml || '');
  if (!xml) return { title: null, description: null, creator: null };

  // Common OPF metadata fields (namespace varies, so match both "dc:title" and "title").
  const title =
    extractFirstXmlTagText(xml, 'dc:title') || extractFirstXmlTagText(xml, 'title');
  const creator =
    extractFirstXmlTagText(xml, 'dc:creator') || extractFirstXmlTagText(xml, 'creator');
  const description =
    extractFirstXmlTagText(xml, 'dc:description') ||
    extractFirstXmlTagText(xml, 'description');

  return {
    title: title || null,
    description: description || null,
    creator: creator || null
  };
}

function buildSpineFileList(opfXml, opfPath) {
  const xml = String(opfXml || '');
  if (!xml) return [];

  const manifestMatch = xml.match(/<manifest\b[\s\S]*?<\/manifest>/i);
  const spineMatch = xml.match(/<spine\b[\s\S]*?<\/spine>/i);
  const manifestXml = manifestMatch ? manifestMatch[0] : '';
  const spineXml = spineMatch ? spineMatch[0] : '';

  if (!manifestXml || !spineXml) return [];

  const idToHref = new Map();
  for (const m of manifestXml.matchAll(/<item\b[^>]*?>/gi)) {
    const attrs = parseXmlAttributes(m[0]);
    const id = String(attrs.id || '').trim();
    const href = String(attrs.href || '').trim();
    if (!id || !href) continue;
    idToHref.set(id, href);
  }

  const out = [];
  for (const m of spineXml.matchAll(/<itemref\b[^>]*?>/gi)) {
    const attrs = parseXmlAttributes(m[0]);
    const idref = String(attrs.idref || '').trim();
    if (!idref) continue;
    const href = idToHref.get(idref);
    if (!href) continue;
    const resolved = resolveEpubHref(opfPath, href);
    if (resolved) out.push(resolved);
  }

  return out;
}

function extractDocxTextFromXml(xml) {
  const raw = String(xml || '');
  if (!raw) return '';

  const paras = raw.match(/<w:p[\s\S]*?<\/w:p>/g) || [];
  const out = [];
  for (const para of paras) {
    const parts = [];
    const matches = para.matchAll(/<w:t[^>]*>([\s\S]*?)<\/w:t>/g);
    for (const m of matches) {
      const v = decodeEntities(m[1]);
      if (v) parts.push(v);
    }
    const joined = parts.join('');
    if (joined.trim()) out.push(joined.trim());
  }

  // fallback: if no paragraphs matched, do a generic tag strip
  if (!out.length) {
    return squeezeWhitespace(decodeEntities(raw.replace(/<[^>]+>/g, ' ')));
  }

  return squeezeWhitespace(out.join('\n'));
}

function pickTitleAndDescription(text, { titleMax = 120, descMax = 280 } = {}) {
  const clean = squeezeWhitespace(text);
  if (!clean) return { title: null, description: null };

  const firstLine = clean.split('\n').map((l) => l.trim()).find(Boolean) || '';
  const title = firstLine ? firstLine.slice(0, titleMax).trim() : null;

  const compact = clean.replace(/\s+/g, ' ').trim();
  const description = compact ? compact.slice(0, descMax).trim() : null;
  return { title: title || null, description: description || null };
}

export async function extractDocxTextFromBytes(bytes, opts = {}) {
  const maxChars = clampInt(opts.maxChars, 20000, { min: 1000, max: 200000 });
  const zip = await JSZip.loadAsync(bytes);
  const file = zip.file('word/document.xml');
  if (!file) return { text: '', title: null, description: null, bytes_read: bytes?.length || 0 };

  const xml = await file.async('string');
  const text = extractDocxTextFromXml(xml).slice(0, maxChars);
  const meta = pickTitleAndDescription(text);
  return { text, ...meta, bytes_read: bytes?.length || 0 };
}

export async function extractEpubTextFromBytes(bytes, opts = {}) {
  const maxChars = clampInt(opts.maxChars, 20000, { min: 1000, max: 200000 });
  const maxFiles = clampInt(opts.maxFiles, 25, { min: 1, max: 200 });

  const zip = await JSZip.loadAsync(bytes);

  const zipFiles = Object.keys(zip.files).filter((name) => !zip.files[name]?.dir);
  const zipNameByLower = new Map();
  for (const name of zipFiles) {
    zipNameByLower.set(String(name || '').toLowerCase(), name);
  }

  let opfPath = null;
  try {
    const containerName = zipNameByLower.get('meta-inf/container.xml');
    if (containerName) {
      const containerXml = await zip.file(containerName)?.async('string');
      const rootfile = containerXml
        ? containerXml.match(/<rootfile\b[^>]*full-path\s*=\s*['"]([^'"]+)['"][^>]*>/i)
        : null;
      if (rootfile && rootfile[1]) {
        // `full-path` in container.xml is relative to the EPUB root, not to META-INF/.
        opfPath = resolveEpubHref('', rootfile[1]) || null;
      }
    }
  } catch {
    opfPath = null;
  }

  if (!opfPath) {
    const fallbackOpf = zipFiles.find((n) =>
      String(n || '').toLowerCase().endsWith('.opf')
    );
    opfPath = fallbackOpf || null;
  } else {
    const resolvedName = zipNameByLower.get(opfPath.toLowerCase());
    opfPath = resolvedName || opfPath;
  }

  let opfXml = '';
  if (opfPath) {
    try {
      opfXml = (await zip.file(opfPath)?.async('string')) || '';
    } catch {
      opfXml = '';
    }
  }

  const meta = extractOpfMetadata(opfXml);
  const metaTitle = clampString(meta.title, 120);
  const metaDescription = clampString(meta.description, 280);

  // Use spine order when available (much closer to the real reading order than filename sorting).
  const spineList = buildSpineFileList(opfXml, opfPath || '');
  const spineFiles = spineList
    .map((p) => zipNameByLower.get(String(p || '').toLowerCase()) || null)
    .filter(Boolean);

  const files =
    spineFiles.length > 0
      ? spineFiles.slice(0, maxFiles)
      : zipFiles
          .filter((name) => {
            const lower = String(name || '').toLowerCase();
            // Avoid metadata/control files that are common but not useful as "document text".
            if (lower.startsWith('meta-inf/')) return false;
            if (lower.endsWith('.opf')) return false;
            if (lower.endsWith('.ncx')) return false;
            return (
              lower.endsWith('.xhtml') ||
              lower.endsWith('.html') ||
              lower.endsWith('.htm') ||
              lower.endsWith('.txt') ||
              lower.endsWith('.xml')
            );
          })
          .sort((a, b) => a.localeCompare(b))
          .slice(0, maxFiles);

  let combined = '';
  for (const name of files) {
    const f = zip.file(name);
    if (!f) continue;
    const raw = await f.async('string');
    const lower = name.toLowerCase();

    // Many EPUBs store XHTML content as `.xml` instead of `.xhtml`. Keep it, but skip obvious
    // non-content XML (container/OPF/NCX already filtered above) and require "HTML-ish" markers.
    if (lower.endsWith('.xml')) {
      const head = String(raw || '').slice(0, 4096).toLowerCase();
      const looksXhtml =
        head.includes('<html') ||
        head.includes('<body') ||
        head.includes('<p') ||
        head.includes('xmlns="http://www.w3.org/1999/xhtml"') ||
        head.includes("xmlns='http://www.w3.org/1999/xhtml'");
      if (!looksXhtml) continue;
    }

    const text = lower.endsWith('.txt') ? squeezeWhitespace(raw) : stripHtmlToText(raw);
    if (text) {
      combined = `${combined}\n${text}`.trim();
      if (combined.length >= maxChars) break;
    }
  }

  const text = combined.slice(0, maxChars);
  const derived = pickTitleAndDescription(text);
  return {
    text,
    title: metaTitle || derived.title,
    description: metaDescription || derived.description,
    bytes_read: bytes?.length || 0
  };
}

export async function extractPdfTextFromBytes(bytes, opts = {}) {
  const maxChars = clampInt(opts.maxChars, 20000, { min: 1000, max: 200000 });
  const maxPages = clampInt(opts.maxPages, 10, { min: 1, max: 100 });
  const timeoutMs = clampInt(opts.timeoutMs, 60000, { min: 1000, max: 10 * 60 * 1000 });

  // pdfjs-dist expects a Uint8Array (passing a Node Buffer triggers warnings/errors in newer versions).
  const pdfData = Buffer.isBuffer(bytes) ? new Uint8Array(bytes) : bytes;

  // Lazy import: avoids paying the startup cost unless we actually process PDFs.
  const pdfjs = await import('pdfjs-dist/legacy/build/pdf.mjs');
  const pdfjsLib = pdfjs?.default || pdfjs;

  const loadingTask = pdfjsLib.getDocument({
    data: pdfData,
    disableWorker: true
  });

  const doc = await withTimeout(loadingTask.promise, timeoutMs, {
    onTimeout: () => {
      try {
        loadingTask.destroy();
      } catch {
        // ignore
      }
    }
  });
  const out = [];

  try {
    const totalPages = typeof doc.numPages === 'number' ? doc.numPages : 0;
    const pages = Math.min(totalPages, maxPages);
    for (let i = 1; i <= pages; i++) {
      const page = await withTimeout(doc.getPage(i), timeoutMs);
      const content = await withTimeout(page.getTextContent(), timeoutMs);
      const strings = Array.isArray(content?.items)
        ? content.items.map((it) => String(it?.str || '')).filter(Boolean)
        : [];
      if (strings.length) out.push(strings.join(' '));
      if (out.join(' ').length >= maxChars) break;
    }
  } finally {
    try {
      doc.destroy();
    } catch {
      // ignore
    }
  }

  const text = squeezeWhitespace(out.join('\n')).slice(0, maxChars);
  const meta = pickTitleAndDescription(text);
  return { text, ...meta, bytes_read: bytes?.length || 0 };
}

export async function extractPdfTextFromUrl(url, opts = {}) {
  const maxChars = clampInt(opts.maxChars, 20000, { min: 1000, max: 200000 });
  const maxPages = clampInt(opts.maxPages, 10, { min: 1, max: 100 });
  const timeoutMs = clampInt(opts.timeoutMs, 60000, { min: 1000, max: 10 * 60 * 1000 });
  const rangeChunkSize = clampInt(opts.rangeChunkSize, 64 * 1024, { min: 16 * 1024, max: 1024 * 1024 });

  const pdfjs = await import('pdfjs-dist/legacy/build/pdf.mjs');
  const pdfjsLib = pdfjs?.default || pdfjs;

  const loadingTask = pdfjsLib.getDocument({
    url,
    disableWorker: true,
    disableStream: true,
    disableAutoFetch: true,
    rangeChunkSize
  });

  const doc = await withTimeout(loadingTask.promise, timeoutMs, {
    onTimeout: () => {
      try {
        loadingTask.destroy();
      } catch {
        // ignore
      }
    }
  });

  const out = [];
  try {
    const totalPages = typeof doc.numPages === 'number' ? doc.numPages : 0;
    const pages = Math.min(totalPages, maxPages);
    for (let i = 1; i <= pages; i++) {
      const page = await withTimeout(doc.getPage(i), timeoutMs);
      const content = await withTimeout(page.getTextContent(), timeoutMs);
      const strings = Array.isArray(content?.items)
        ? content.items.map((it) => String(it?.str || '')).filter(Boolean)
        : [];
      if (strings.length) out.push(strings.join(' '));
      if (out.join(' ').length >= maxChars) break;
    }
  } finally {
    try {
      doc.destroy();
    } catch {
      // ignore
    }
  }

  const text = squeezeWhitespace(out.join('\n')).slice(0, maxChars);
  const meta = pickTitleAndDescription(text);
  return { text, ...meta, bytes_read: null };
}
