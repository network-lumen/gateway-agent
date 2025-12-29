import os from 'node:os';
import process from 'node:process';
import { exec as _exec } from 'node:child_process';
import { promisify } from 'node:util';
import { getWalletRecord, getAllWalletRecords, getRegistryStats } from '../lib/walletRegistry.js';
import { getChainStatus, ensureChainOnline } from '../lib/chain.js';
import { kuboRequest } from '../lib/kuboClient.js';
import { getHttpMetricsSnapshot } from '../services/httpMetrics.js';

let lastIpfsCheckAt = 0;
let lastIpfsOnline = false;
const exec = promisify(_exec);

async function getIpfsStatus() {
  const now = Date.now();
  if (lastIpfsCheckAt && now - lastIpfsCheckAt < 10_000) {
    return { online: lastIpfsOnline, lastCheckAt: lastIpfsCheckAt };
  }
  try {
    const resp = await kuboRequest('/api/v0/version');
    lastIpfsOnline = resp.ok;
  } catch {
    lastIpfsOnline = false;
  }
  lastIpfsCheckAt = now;
  return { online: lastIpfsOnline, lastCheckAt: lastIpfsCheckAt };
}

async function getDiskUsage() {
  try {
    // Prefer IPFS data volume if mounted, fallback to app FS
    const { stdout } = await exec('df -kP /data/ipfs || df -kP /app');
    const lines = stdout.trim().split('\n');
    if (lines.length < 2) return null;
    const parts = lines[1].trim().split(/\s+/);
    if (parts.length < 5) return null;
    const totalKb = Number.parseInt(parts[1], 10);
    const usedKb = Number.parseInt(parts[2], 10);
    const availKb = Number.parseInt(parts[3], 10);
    if (!Number.isFinite(totalKb) || !Number.isFinite(usedKb) || !Number.isFinite(availKb)) {
      return null;
    }
    return {
      totalBytes: totalKb * 1024,
      usedBytes: usedKb * 1024,
      freeBytes: availKb * 1024
    };
  } catch {
    return null;
  }
}

async function computeStats() {
  const reg = getRegistryStats();
  const ipfs = await getIpfsStatus();

  // update chain status (but don't throw for metrics)
  try {
    await ensureChainOnline();
  } catch {
    // ignore, status will reflect offline
  }
  const chain = getChainStatus();

  const mem = process.memoryUsage();
  const osTotalMem = os.totalmem();
  const osFreeMem = os.freemem();
  const load = os.loadavg();
  const disk = await getDiskUsage();

  return {
    wallets: reg.walletCount,
    pins: reg.pinCount,
    unpins: reg.unpinCount,
    ingestBytes: reg.ingestBytes,
    ipfs,
    chain,
    hardware: {
      process: {
        pid: process.pid,
        uptimeSec: process.uptime(),
        rss: mem.rss,
        heapUsed: mem.heapUsed,
        heapTotal: mem.heapTotal
      },
      os: {
        uptimeSec: os.uptime(),
        totalMem: osTotalMem,
        freeMem: osFreeMem,
        loadAvg: load,
        cpuCount: os.cpus().length,
        platform: process.platform,
        arch: process.arch
      },
      disk
    }
  };
}

function formatDiskMetrics(lines, disk) {
  if (!disk) return;
  lines.push(`# TYPE gateway_disk_total_bytes gauge`);
  lines.push(`gateway_disk_total_bytes ${disk.totalBytes}`);
  lines.push(`# TYPE gateway_disk_used_bytes gauge`);
  lines.push(`gateway_disk_used_bytes ${disk.usedBytes}`);
  lines.push(`# TYPE gateway_disk_free_bytes gauge`);
  lines.push(`gateway_disk_free_bytes ${disk.freeBytes}`);
}

export async function getWallet(req, res) {
  const wallet = String(req.params.wallet || '').trim();
  if (!wallet) return res.status(400).json({ error: 'wallet_required' });
  const rec = getWalletRecord(wallet);
  if (!rec) return res.status(404).json({ error: 'not_found' });
  res.json(rec);
}

function isPrivateIp(remote) {
  if (!remote || typeof remote !== 'string') return false;
  const ip = remote.replace(/^::ffff:/, '');
  if (ip === '127.0.0.1' || ip === '::1') return true;
  if (ip.startsWith('10.') || ip.startsWith('192.168.')) return true;
  if (ip.startsWith('172.')) {
    const parts = ip.split('.');
    const second = Number.parseInt(parts[1] || '0', 10);
    if (second >= 16 && second <= 31) return true;
  }
  return false;
}

export async function getMetrics(req, res) {
  const remote = (req.socket && req.socket.remoteAddress) || '';
  if (!isPrivateIp(remote)) {
    return res.status(403).send('metrics_forbidden');
  }
  const stats = await computeStats();
  const httpMetrics = getHttpMetricsSnapshot();

  const lines = [];
  // core counters
  lines.push(`# TYPE gateway_wallets_total gauge`);
  lines.push(`gateway_wallets_total ${stats.wallets}`);
  lines.push(`# TYPE gateway_pins_total counter`);
  lines.push(`gateway_pins_total ${stats.pins}`);
  lines.push(`# TYPE gateway_unpins_total counter`);
  lines.push(`gateway_unpins_total ${stats.unpins}`);
  lines.push(`# TYPE gateway_ingest_bytes_total counter`);
  lines.push(`gateway_ingest_bytes_total ${stats.ingestBytes}`);

  // ipfs / chain
  lines.push(`# TYPE gateway_ipfs_up gauge`);
  lines.push(`gateway_ipfs_up ${stats.ipfs.online ? 1 : 0}`);
  lines.push(`# TYPE gateway_chain_up gauge`);
  lines.push(`gateway_chain_up ${stats.chain.online ? 1 : 0}`);

  // process metrics
  const p = stats.hardware.process;
  lines.push(`# TYPE gateway_process_uptime_seconds gauge`);
  lines.push(`gateway_process_uptime_seconds ${p.uptimeSec}`);
  lines.push(`# TYPE gateway_process_memory_rss_bytes gauge`);
  lines.push(`gateway_process_memory_rss_bytes ${p.rss}`);
  lines.push(`# TYPE gateway_process_memory_heap_used_bytes gauge`);
  lines.push(`gateway_process_memory_heap_used_bytes ${p.heapUsed}`);

  // OS metrics
  const o = stats.hardware.os;
  lines.push(`# TYPE gateway_os_uptime_seconds gauge`);
  lines.push(`gateway_os_uptime_seconds ${o.uptimeSec}`);
  lines.push(`# TYPE gateway_os_memory_total_bytes gauge`);
  lines.push(`gateway_os_memory_total_bytes ${o.totalMem}`);
  lines.push(`# TYPE gateway_os_memory_free_bytes gauge`);
  lines.push(`gateway_os_memory_free_bytes ${o.freeMem}`);
  if (Array.isArray(o.loadAvg) && o.loadAvg.length >= 3) {
    lines.push(`# TYPE gateway_os_load1 gauge`);
    lines.push(`gateway_os_load1 ${o.loadAvg[0]}`);
    lines.push(`# TYPE gateway_os_load5 gauge`);
    lines.push(`gateway_os_load5 ${o.loadAvg[1]}`);
    lines.push(`# TYPE gateway_os_load15 gauge`);
    lines.push(`gateway_os_load15 ${o.loadAvg[2]}`);
  }
  lines.push(`# TYPE gateway_os_cpu_count gauge`);
  lines.push(`gateway_os_cpu_count ${o.cpuCount}`);

  // Disk metrics
  formatDiskMetrics(lines, stats.hardware.disk);

  // HTTP metrics (per method / normalized path / status)
  if (httpMetrics.counters.length > 0) {
    lines.push(`# TYPE gateway_http_requests_total counter`);
    for (const c of httpMetrics.counters) {
      const method = String(c.method || '').toUpperCase();
      const path = String(c.path || '/')
        .replace(/\\/g, '\\\\')
        .replace(/"/g, '\\"');
      const code = Number.isFinite(c.code) ? c.code : 0;
      const count = Number.isFinite(c.count) && c.count >= 0 ? c.count : 0;
      lines.push(
        `gateway_http_requests_total{method="${method}",path="${path}",code="${code}"} ${count}`
      );
    }
  }

  if (httpMetrics.durations.length > 0) {
    lines.push(`# TYPE gateway_http_request_duration_ms summary`);
    for (const d of httpMetrics.durations) {
      const method = String(d.method || '').toUpperCase();
      const path = String(d.path || '/')
        .replace(/\\/g, '\\\\')
        .replace(/"/g, '\\"');
      const baseLabels = `method="${method}",path="${path}"`;
      const sumMs = Number.isFinite(d.sumMs) && d.sumMs >= 0 ? d.sumMs : 0;
      const count = Number.isFinite(d.count) && d.count >= 0 ? d.count : 0;
      const maxMs = Number.isFinite(d.maxMs) && d.maxMs >= 0 ? d.maxMs : 0;
      lines.push(`gateway_http_request_duration_ms_sum{${baseLabels}} ${sumMs}`);
      lines.push(`gateway_http_request_duration_ms_count{${baseLabels}} ${count}`);
      lines.push(`gateway_http_request_duration_ms_max{${baseLabels}} ${maxMs}`);
    }
  }

  res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
  res.send(lines.join('\n') + '\n');
}

export async function getAllWallets(_req, res) {
  const all = getAllWalletRecords();
  res.json({ wallets: all, count: all.length });
}
