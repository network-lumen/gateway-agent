import fs from 'node:fs';
import path from 'node:path';
import { CONFIG } from '../config.js';
import { kuboRequest } from '../lib/kuboClient.js';

const PACKAGE_FILE = path.resolve(process.cwd(), 'package.json');
let NODE_API_VERSION = '0.0.0';

try {
  const raw = fs.readFileSync(PACKAGE_FILE, 'utf8');
  const pkg = JSON.parse(raw);
  if (pkg && typeof pkg.version === 'string' && pkg.version.trim()) {
    NODE_API_VERSION = pkg.version.trim();
  }
} catch {
  NODE_API_VERSION = '0.0.0';
}

export async function getStatus(_req, res) {
  let ipfsOnline = false;
  try {
    const resp = await kuboRequest('/api/v0/version');
    ipfsOnline = resp.ok;
  } catch {
    ipfsOnline = false;
  }

  res.json({
    version: NODE_API_VERSION,
    region: CONFIG.REGION,
    public: CONFIG.PUBLIC_ENDPOINT,
    ipfs: { online: ipfsOnline },
    time: new Date().toISOString()
  });
}
