import express from 'express';
import { getStatus } from '../controllers/statusController.js';
import { getPricing } from '../controllers/pricingController.js';
import { postPin, postUnpin } from '../controllers/pinController.js';
import { getIsPinned } from '../controllers/ispinnedController.js';
import { getIngestReady, postIngestInit, postIngestCar } from '../controllers/ingestController.js';
import { postSearchPq } from '../controllers/searchController.js';
import { getMetrics } from '../controllers/adminController.js';
import { getWalletUsage } from '../controllers/walletUsageController.js';
import { getWalletPinnedCids } from '../controllers/walletCidsController.js';
import { authWallet } from '../middleware/authWallet.js';
import { postPqIpfs, postPqIpns } from '../controllers/ipfsProxyController.js';
import { postRootsDomains } from '../controllers/internalCidController.js';
import { getKyberPublicKey } from '../controllers/pqController.js';
import { getIpfsSeed } from '../controllers/ipfsSeedController.js';

export function buildRouter() {
  const router = express.Router();

  // PQ-protected IPFS/IPNS access only (no legacy /ipfs or /ipns proxy)
  router.post('/pq/ipfs', express.json(), postPqIpfs);
  router.post('/pq/ipns', express.json(), postPqIpns);

  // Public API
  router.get('/status', getStatus);
  router.get('/pricing', getPricing);
  router.get('/ipfs/seed', getIpfsSeed);
  router.post('/pq/search', express.json(), postSearchPq);
  router.get('/pq/pub', getKyberPublicKey);

  router.post('/pin', authWallet, postPin);
  router.post('/unpin', authWallet, postUnpin);
  router.post('/ingest/init', authWallet, postIngestInit);
  router.post('/ingest/ready', authWallet, getIngestReady);
  router.post('/ingest/car', postIngestCar);

  router.get('/ispinned', authWallet, getIsPinned);
  router.post('/ispinned', authWallet, getIsPinned);
  router.post('/wallet/usage', authWallet, getWalletUsage);
  router.post('/wallet/cids', authWallet, getWalletPinnedCids);

  // Admin / internal
  router.get('/metrics', getMetrics);
  router.post('/admin/_internal/roots/domains', express.json(), postRootsDomains);

  return router;
}
