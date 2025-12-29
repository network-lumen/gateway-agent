function categoryFromKind(kind) {
  switch (kind) {
    case 'image':
    case 'video':
    case 'audio':
      return 'media';
    case 'doc':
    case 'html':
    case 'text':
      return 'document';
    case 'archive':
    case 'ipld':
      return 'package';
    default:
      return 'unknown';
  }
}

function confidenceBucket(conf) {
  if (!Number.isFinite(conf)) return 'low';
  if (conf >= 0.9) return 'high';
  if (conf >= 0.6) return 'medium';
  return 'low';
}

function sizeBucket(size) {
  if (!Number.isFinite(size) || size < 0) return null;
  const kb64 = 64 * 1024;
  const mb1 = 1024 * 1024;
  const mb50 = 50 * mb1;
  const mb500 = 500 * mb1;
  const gb5 = 5 * 1024 * mb1;

  if (size < kb64) return 'xs';
  if (size < mb1) return 's';
  if (size < mb50) return 'm';
  if (size < mb500) return 'l';
  if (size < gb5) return 'xl';
  return 'xxl';
}

function durationBucket(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) return null;
  const shortMax = 10 * 60;
  const mediumMax = 60 * 60;
  const longMax = 3 * 60 * 60;

  if (seconds <= shortMax) return 'short';
  if (seconds <= mediumMax) return 'medium';
  if (seconds <= longMax) return 'long';
  return null;
}

export function buildTags({ detection }) {
  const tags = [];
  const kind = detection.kind || 'unknown';
  const category = categoryFromKind(kind);
  const mime = detection.mime || 'application/octet-stream';
  const ext = detection.ext_guess || 'unknown';
  const source = detection.source || 'heuristic';
  const confBucket = confidenceBucket(detection.confidence);

  // Required, stable tags
  tags.push(`kind:${kind}`);
  tags.push(`category:${category}`);
  tags.push(`mime:${mime}`);
  tags.push(`ext:${ext}`);
  tags.push(`detected_by:${source}`);
  tags.push(`confidence:${confBucket}`);

  const size = detection.size;
  const bucket = sizeBucket(size);
  if (bucket) {
    tags.push(`size_bucket:${bucket}`);
    if (kind === 'audio') {
      tags.push(`audio_size_bucket:${bucket}`);
    }
  }

  if (kind === 'video') {
    const signals = detection.signals || {};
    const media = signals.media || null;
    const seconds =
      media && typeof media.duration_seconds === 'number'
        ? media.duration_seconds
        : null;
    const durBucket = durationBucket(seconds);
    if (durBucket) {
      tags.push(`video_duration:${durBucket}`);
    }
  }

  // Container / format derived tags
  const signals = detection.signals || {};
  const container = signals.container || {};

  if (container.type === 'zip') {
    tags.push('container:zip');
  } else if (container.type === 'mp4') {
    tags.push('container:mp4');
  } else if (container.type === 'pdf') {
    tags.push('container:pdf');
  } else if (container.type === 'car') {
    tags.push('container:car');
  }

  if (container.subtype === 'docx' || ext === 'docx') {
    tags.push('office:docx');
  }
  if (container.subtype === 'xlsx' || ext === 'xlsx') {
    tags.push('office:xlsx');
  }
  if (container.subtype === 'pptx' || ext === 'pptx') {
    tags.push('office:pptx');
  }
  if (container.subtype === 'epub' || ext === 'epub') {
    tags.push('ebook:epub');
  }

  if (kind === 'video' || kind === 'audio') {
    tags.push('streamable:maybe');
  }

  // Needs further enrichment
  if (kind === 'image' || kind === 'video' || kind === 'audio') {
    tags.push('needs:metadata');
    tags.push('needs:ai_tags');
  } else if (kind === 'doc' || kind === 'html' || kind === 'text') {
    tags.push('needs:metadata');
  }

  return tags;
}
