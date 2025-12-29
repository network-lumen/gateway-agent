import { readPricing } from '../services/pricingService.js';

export async function getPricing(_req, res) {
  try {
    const pricing = await readPricing();
    res.json(pricing);
  } catch (err) {
    console.error('[api:/pricing] error', err);
    res.status(500).json({ error: 'internal_error' });
  }
}

