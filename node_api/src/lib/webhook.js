import { CONFIG } from '../config.js';

export async function sendWebhookEvent(type, data) {
  const cfg = CONFIG.WEBHOOK;
  const url = cfg && typeof cfg.url === 'string' ? cfg.url.trim() : '';
  if (!url) return;

  const payload = {
    type,
    time: new Date().toISOString(),
    data: data || {}
  };

  try {
    await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });
  } catch (err) {
    // Non bloquant: on log et on continue
    console.error('[webhook] failed to send event', type, err);
  }
}

