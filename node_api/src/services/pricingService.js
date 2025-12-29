import { CONFIG } from '../config.js';

export async function readPricing() {
  if (Array.isArray(CONFIG.PRICING)) {
    return CONFIG.PRICING;
  }

  return [];
}
