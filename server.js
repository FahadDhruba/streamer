// server.js
// Express HTTP entry point. Boots the RTMP server, the API, and shared
// state. Designed to never crash the whole process if one stream fails —
// every per-stream failure is caught inside its own module.

import 'dotenv/config';
import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import apiRouter from './routes/api.js';
import { startRtmpServer, shutdownRtmp } from './rtmp.js';
import { purgeTmpRoot } from './cleanup.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const EXPRESS_PORT = parseInt(process.env.EXPRESS_PORT || '3000', 10);
const TMP_DIR      = path.resolve(__dirname, process.env.TMP_DIR || './tmp/live');

function assertEnv() {
  const required = ['R2_ACCOUNT_ID', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_BUCKET_NAME', 'R2_PUBLIC_URL'];
  const missing  = required.filter((k) => !process.env[k]);
  if (missing.length) {
    console.warn(`[server] WARNING: missing R2 env vars: ${missing.join(', ')}. Uploads will fail until configured.`);
  }
}

async function main() {
  assertEnv();

  // Crash recovery: clear leftover live segment dirs before accepting streams.
  await purgeTmpRoot(TMP_DIR);

  const app = express();
  app.use(express.json({ limit: '256kb' }));

  // Tiny request log.
  app.use((req, _res, next) => {
    console.log(`[http ${new Date().toISOString()}] ${req.method} ${req.url}`);
    next();
  });

  app.get('/', (_req, res) => {
    res.type('text/plain').send(
      'Live Streaming Server is up.\n' +
      'See /api/streams for active streams.\n' +
      'See README.md for OBS, VLC, and HLS.js instructions.\n',
    );
  });

  app.use('/api', apiRouter);

  app.use((err, _req, res, _next) => {
    console.error('[http] unhandled error:', err);
    res.status(500).json({ error: 'internal server error' });
  });

  const httpServer = app.listen(EXPRESS_PORT, () => {
    console.log(`[server] HTTP API listening on :${EXPRESS_PORT}`);
  });

  startRtmpServer();

  // Global safety nets — log but never exit.
  process.on('uncaughtException',  (err) => {
    console.error('[server] uncaughtException:', err);
  });
  process.on('unhandledRejection', (err) => {
    console.error('[server] unhandledRejection:', err);
  });

  const shutdown = async (signal) => {
    console.log(`[server] received ${signal}, shutting down`);
    try { await shutdownRtmp(); } catch (e) { console.warn(e.message); }
    httpServer.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5_000).unref();
  };
  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('[server] fatal startup error:', err);
  process.exit(1);
});
