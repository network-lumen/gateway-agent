import { CONFIG } from './config.js';
import { logError } from './log.js';

let clipPipelinePromise = null;

async function getClipPipeline() {
  if (!clipPipelinePromise) {
    clipPipelinePromise = (async () => {
      const { pipeline } = await import('@xenova/transformers');
      const pipe = await pipeline(
        'zero-shot-image-classification',
        'Xenova/clip-vit-base-patch32'
      );
      return pipe;
    })().catch((err) => {
      clipPipelinePromise = null;
      logError('imageTagger: failed to init CLIP pipeline', err?.message || err);
      throw err;
    });
  }
  return clipPipelinePromise;
}

// Fixed, deterministic vocabulary of tags for images.
// style, subject, scene, domain (UI/code/docs), marketing, and safety.
const IMAGE_TAG_LABELS = [
  // Style / format
  'photo',
  'illustration',
  'drawing',
  'comic',
  'cartoon',
  '3d render',
  'pixel art',
  'sketch',
  'minimalist',
  'flat design',
  'vector art',
  'black and white photo',
  'vintage photo',
  'polaroid photo',
  'macro photo',
  'aesthetic wallpaper',

  // UI / screenshots / products
  'screenshot',
  'desktop screenshot',
  'mobile screenshot',
  'website screenshot',
  'app ui',
  'dashboard',
  'form ui',
  'terminal screenshot',
  'code editor screenshot',
  'ide screenshot',
  'admin dashboard',
  'login screen',

  // Logos / icons / branding
  'logo',
  'brand logo',
  'app icon',
  'badge',
  'emblem',
  'favicon',
  'company logo',

  // Text / documents
  'text document',
  'handwritten text',
  'scanned document',
  'presentation slide',
  'spreadsheet',
  'code snippet',
  'terminal output',
  'book page',
  'magazine page',
  'newspaper article',
  'receipt',
  'invoice',
  'form document',
  'resume document',

  // Diagrams / data viz
  'diagram',
  'flowchart',
  'network graph',
  'mind map',
  'chart',
  'bar chart',
  'line chart',
  'pie chart',
  'map',
  'mind map diagram',
  'org chart',

  // People / portraits
  'person',
  'portrait',
  'selfie',
  'group photo',
  'crowd',
  'conference',
  'meeting',
  'team photo',
  'family photo',
  'baby photo',
  'child portrait',
  'man portrait',
  'woman portrait',
  'couple photo',

  // People attributes (soft, for ranking)
  'face closeup',
  'profile portrait',
  'full body photo',

  // Nature / landscapes
  'landscape',
  'nature',
  'forest',
  'mountain',
  'beach',
  'sea',
  'sunset',
  'sunrise',
  'night sky',
  'city skyline',
  'mountain landscape',
  'beach landscape',
  'desert landscape',
  'waterfall',
  'field of flowers',

  // Objects / scenes
  'street scene',
  'indoor scene',
  'office',
  'classroom',
  'kitchen',
  'bedroom',
  'car interior',
  'store shelf',
  'restaurant interior',
  'living room',
  'workspace desk',

  // Everyday objects / popular queries
  'food dish',
  'dessert',
  'pizza',
  'burger',
  'sushi',
  'coffee cup',
  'tea cup',
  'shoe',
  'sneaker',
  't shirt',
  'dress',
  'watch',
  'jewelry',
  'car',
  'motorcycle',
  'bicycle',
  'book',
  'camera',
  'headphones',

  // Media / entertainment
  'movie poster',
  'game screenshot',
  'anime scene',
  'cartoon character',
  'music album cover',
  'podcast cover',
  'concert photo',
  'sports match',
  'stadium',

  // Tech / devices
  'computer',
  'laptop',
  'smartphone',
  'tablet',
  'server rack',
  'robot',
  'smartwatch',
  'drone',

  // Content categories
  'tutorial graphic',
  'documentation graphic',
  'infographic',
  'marketing banner',
  'social media post',
  'meme',
  'profile picture',
  'product photo',
  'e commerce product photo',
  'advertising banner',
  'event flyer',
  'newsletter header',

  // Education / business
  'whiteboard drawing',
  'classroom board',
  'business presentation',
  'timeline graphic',
  'roadmap graphic',

  // Web3 / dev-ish (light, but useful for Lumen)
  'blockchain diagram',
  'network topology diagram',
  'api diagram',
  'terminal window',

  // Animals / wildlife
  'dog',
  'puppy',
  'cat',
  'kitten',
  'bird',
  'parrot',
  'horse',
  'cow',
  'sheep',
  'goat',
  'fish',
  'shark',
  'dolphin',
  'butterfly',
  'insect',
  'lion',
  'tiger',
  'bear',
  'wildlife photo',

  // Buildings / architecture / travel
  'city street',
  'skyscraper',
  'apartment building',
  'suburban house',
  'cottage house',
  'castle',
  'palace',
  'bridge',
  'tower',
  'church building',
  'temple',
  'mosque',
  'tourist landmark',
  'famous monument',

  // Events / lifestyle
  'wedding photo',
  'birthday party',
  'festival crowd',
  'parade',
  'fireworks show',
  'concert stage',
  'night club',
  'street market',
  'graduation ceremony',

  // Industry / work
  'factory interior',
  'construction site',
  'warehouse interior',
  'farm field',
  'hospital room',
  'laboratory interior',
  'school hallway',

  // Sports / fitness
  'football match',
  'soccer match',
  'basketball game',
  'tennis match',
  'running race',
  'gym workout',
  'yoga pose',
  'cycling race',
  'swimming pool',

  // Vehicles / transport
  'bus on street',
  'train on tracks',
  'airplane in sky',
  'ship on sea',
  'boat on lake',
  'bicycle on road',
  'scooter',
  'truck on highway',

  // Weather / time of day
  'sunny day',
  'cloudy sky',
  'rainy day',
  'snowy landscape',
  'foggy landscape',
  'stormy sky',
  'night city',
  'golden hour light',

  // Background / graphic usage
  'transparent background',
  'solid color background',
  'gradient background',
  'pattern background',
  'texture background',
  'abstract pattern',

  // Kids / education
  'children drawing',
  'coloring page',
  'cartoon kids',
  'school project poster',

  // Product categories (common e-commerce)
  'beauty product',
  'makeup palette',
  'perfume bottle',
  'furniture sofa',
  'furniture chair',
  'furniture table',
  'bed furniture',
  'kitchen appliance',
  'home decor',
  'lamp',
  'mirror',

  // Codes / utility visuals
  'qr code',
  'barcode',
  'calendar screenshot',
  'map screenshot',
  'error message screenshot',

  // Emotions / mood (soft)
  'happy face',
  'sad face',
  'angry face',
  'surprised face',
  'cute animal',
  'funny meme',
  'inspirational quote graphic',

  // Orientation / resolution / wallpapers
  'portrait orientation photo',
  'landscape orientation photo',
  'square image',
  'panoramic photo',
  'low resolution image',
  'high resolution photo',
  'hd wallpaper',
  '4k wallpaper',
  'mobile wallpaper',
  'desktop wallpaper',

  // Holidays / celebrations
  'christmas tree',
  'christmas lights',
  'christmas decoration',
  'santa claus illustration',
  'halloween pumpkin',
  'halloween costume',
  'easter eggs',
  'birthday cake',
  'birthday balloons',
  'valentines hearts',
  'fireworks at night',
  'new year celebration',

  // Flags / symbols
  'country flag',
  'world map',
  'globe illustration',

  // Extra education / school
  'science classroom',
  'math blackboard',
  'chemistry lab',
  'history textbook page',

  // Extra health / medical
  'medical illustration',
  'x ray image',
  'mri scan image',
  'doctor with patient',
  'hospital hallway',

  // Extra UI / tools
  'chat conversation screenshot',
  'email inbox screenshot',
  'calendar app screenshot',
  'music player ui',
  'video player ui',
  'map app ui',

  // Extra dev / tooling
  'code review screenshot',
  'version control history screenshot',
  'issue tracker screenshot',
  'documentation website screenshot',
  'terminal log output',

  // NSFW / safety (soft signals, for ranking / filtering)
  'nsfw content',
  'safe content'
];

export async function tagImageWithClip(cid, detection) {
  try {
    const size = detection && typeof detection.size === 'number' ? detection.size : null;
    const maxBytes =
      typeof CONFIG.IMAGE_TAG_MAX_BYTES === 'number' && CONFIG.IMAGE_TAG_MAX_BYTES > 0
        ? CONFIG.IMAGE_TAG_MAX_BYTES
        : 4 * 1024 * 1024;

    if (size && size > maxBytes) {
      return null;
    }

    const url = new URL(`/ipfs/${cid}`, CONFIG.IPFS_GATEWAY_BASE).toString();

    const pipe = await getClipPipeline();
    const result = await pipe(url, IMAGE_TAG_LABELS, {
      // We want multiple tags, not just a single class.
      multi_label: true
    });

    if (!Array.isArray(result) || !result.length) {
      return null;
    }

    const tokens = {};
    const topics = [];

    // result is sorted by score desc.
    for (let i = 0; i < result.length; i += 1) {
      const item = result[i];
      if (!item || typeof item.label !== 'string') continue;
      const label = item.label.trim().toLowerCase();
      const score = typeof item.score === 'number' ? item.score : 0;
      if (!label || score <= 0) continue;

      // Convert score to a small integer weight (0-100).
      const weight = Math.round(score * 100);
      if (weight <= 0) continue;

      tokens[label] = weight;

      // Keep top ~5 labels as topics for quick search / display.
      if (topics.length < 5) {
        topics.push(label);
      }
    }

    if (!Object.keys(tokens).length && !topics.length) {
      return null;
    }

    return {
      topics,
      tokens
    };
  } catch (err) {
    logError('imageTagger.tagImageWithClip error', cid, err?.message || err);
    return null;
  }
}
