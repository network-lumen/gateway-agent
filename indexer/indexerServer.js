import { initDb } from './db.js';
import { startPinSyncWorker } from './pinSync.js';
import { startTypeCrawlerWorker } from './typeCrawler.js';
import { startDirExpanderWorker } from './dirExpander.js';
import { startHttpServer } from './server.js';
import { log, logError } from './log.js';

async function main() {
  try {
    await initDb();
    startPinSyncWorker();
    startTypeCrawlerWorker();
    startDirExpanderWorker();
    startHttpServer();
    log('indexer service started');
  } catch (err) {
    logError('indexer startup failed', err);
    process.exitCode = 1;
  }
}

main();
