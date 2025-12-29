import process from 'node:process';
import { recordHttpRequest } from '../services/httpMetrics.js';

export function httpMetricsMiddleware(req, res, next) {
  const start = process.hrtime.bigint();

  res.on('finish', () => {
    try {
      const end = process.hrtime.bigint();
      const durationMs = Number(end - start) / 1e6;
      recordHttpRequest({
        method: req.method || 'GET',
        path: req.path || req.originalUrl || '/',
        statusCode: res.statusCode || 0,
        durationMs
      });
    } catch {
      // Metrics must never break the request flow
    }
  });

  next();
}

