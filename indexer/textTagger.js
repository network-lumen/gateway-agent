import { logError } from './log.js';

let textPipelinePromise = null;

async function getTextPipeline() {
  if (!textPipelinePromise) {
    textPipelinePromise = (async () => {
      const { pipeline } = await import('@xenova/transformers');
      const pipe = await pipeline(
        'zero-shot-classification',
        'Xenova/distilbert-base-uncased-mnli'
      );
      return pipe;
    })().catch((err) => {
      textPipelinePromise = null;
      logError('textTagger: failed to init text pipeline', err?.message || err);
      throw err;
    });
  }
  return textPipelinePromise;
}

const TEXT_TAG_LABELS = [
  // General document / page types
  'documentation',
  'api reference',
  'tutorial',
  'how to guide',
  'readme',
  'changelog',
  'release notes',
  'faq',
  'specification',
  'design document',
  'whitepaper',
  'case study',
  'user manual',
  'reference manual',
  'knowledge base article',
  'help center article',
  'support ticket',

  // Content styles / formats
  'blog post',
  'news article',
  'opinion piece',
  'press release',
  'forum thread',
  'q and a discussion',
  'wiki page',
  'product review',
  'comparison review',
  'landing page',
  'marketing page',
  'sales page',
  'product description',
  'newsletter',
  'survey results',

  // Legal / policy / jobs
  'legal terms',
  'terms of service',
  'privacy policy',
  'cookie policy',
  'license agreement',
  'job listing',
  'job description',
  'resume',
  'curriculum vitae',
  'cover letter',

  // Dev / infra / data
  'source code snippet',
  'configuration file',
  'infrastructure configuration',
  'docker configuration',
  'kubernetes manifest',
  'shell script',
  'build configuration',
  'log output',
  'error log',
  'api error message',
  'json data',
  'yaml config',
  'csv data table',
  'database schema documentation',

  // Web / app domains
  'web application documentation',
  'mobile app documentation',
  'ui component documentation',
  'design system documentation',

  // Web3 / blockchain / protocol
  'blockchain documentation',
  'smart contract documentation',
  'network protocol documentation',
  'token economics documentation',

  // Technical guides
  'cli reference',
  'deployment guide',
  'installation guide',
  'troubleshooting guide',
  'performance tuning guide',

  // Business / product
  'business plan',
  'product roadmap',
  'feature specification',
  'marketing strategy',
  'sales playbook',
  'pricing page',

  // Finance / trading
  'personal finance guide',
  'investment guide',
  'trading strategy article',
  'market analysis',

  // Science / education
  'scientific article',
  'research paper summary',
  'course syllabus',
  'lecture notes',
  'exam preparation guide',

  // Lifestyle / general web
  'travel guide',
  'food recipe',
  'restaurant review',
  'fitness guide',
  'health advice article',
  'parenting advice article',
  'fashion guide',
  'home improvement guide',

  // Entertainment / media
  'movie review',
  'tv series review',
  'music review',
  'video game review',
  'event announcement',

  // Community / social
  'social media post',
  'community guidelines',
  'online contest announcement',

  // News verticals
  'politics news article',
  'technology news article',
  'sports news article',
  'local news article',
  'world news article',
  'entertainment news article',

  // Sports
  'football match report',
  'basketball match report',
  'tennis match report',
  'sports statistics article',

  // Weather / maps / transport
  'weather forecast article',
  'traffic information article',
  'public transport schedule article',
  'flight status information page',

  // Shopping / e‑commerce
  'online shopping category page',
  'e commerce product listing page',
  'coupon and discount page',
  'price comparison page',
  'shopping cart help article',

  // Online services help
  'email troubleshooting guide',
  'online banking help article',
  'cloud service troubleshooting guide',
  'account recovery help article',

  // People / profiles
  'biography page',
  'celebrity gossip article',
  'influencer profile page'
];

export async function tagTextWithModel(text) {
  try {
    if (!text || typeof text !== 'string') return null;
    const trimmed = text.trim();
    if (!trimmed) return null;

    const maxCharsPerChunk = 4000;
    const maxChunks = 10;
    const maxTotalChars = maxCharsPerChunk * maxChunks;

    const limitedText =
      trimmed.length > maxTotalChars
        ? trimmed.slice(0, maxTotalChars)
        : trimmed;

    const chunks = [];
    for (let i = 0; i < limitedText.length; i += maxCharsPerChunk) {
      const chunk = limitedText.slice(i, i + maxCharsPerChunk).trim();
      if (chunk) chunks.push(chunk);
    }

    if (!chunks.length) return null;

    const pipe = await getTextPipeline();
    const labelScores = new Map();

    for (const chunk of chunks) {
      const result = await pipe(chunk, TEXT_TAG_LABELS, { multi_label: true });
      if (!Array.isArray(result)) continue;
      for (const item of result) {
        if (!item || typeof item.label !== 'string') continue;
        const label = item.label.trim().toLowerCase();
        const score = typeof item.score === 'number' ? item.score : 0;
        if (!label || score <= 0) continue;
        const prev = labelScores.get(label) || 0;
        labelScores.set(label, prev + score);
      }
    }

    if (!labelScores.size) return null;

    const avgScores = Array.from(labelScores.entries()).map(
      ([label, totalScore]) => {
        const avg = totalScore / chunks.length;
        return [label, avg];
      }
    );

    avgScores.sort((a, b) => b[1] - a[1]);

    const tokens = {};
    const topics = [];

    for (let i = 0; i < avgScores.length; i += 1) {
      const [label, score] = avgScores[i];
      const weight = Math.round(score * 100);
      if (weight <= 0) continue;
      tokens[label] = weight;
      if (topics.length < 5) {
        topics.push(label);
      }
    }

    if (!Object.keys(tokens).length && !topics.length) {
      return null;
    }

    return { topics, tokens };
  } catch (err) {
    logError('textTagger.tagTextWithModel error', err?.message || err);
    return null;
  }
}
