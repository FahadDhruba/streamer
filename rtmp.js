// rtmp.js
// node-media-server configuration + lifecycle wiring.
//
// Flow:
//   prePublish   → validate stream key, reject if invalid / already live
//   postPublish  → start chokidar watcher, spawn ffmpeg transcoder
//   donePublish  → stop transcoder, flush uploads, kick off VOD generation
//
// Active streams are tracked in an in-memory Map. The HTTP API consumes
// it through `getActiveStreams()` and `getActiveStream()`.

import 'dotenv/config';
import NodeMediaServer from 'node-media-server';

import { validateKey, autoRegisterKey, getLabel } from './streamkeys.js';
import {
  startWatcher,
  stopWatcher,
  flushPending,
  sweepStreamDir,
  liveMasterUrl,
  vodMasterUrl,
  vodMp4Url,
} from './uploader.js';
import { startTranscoder, stopTranscoder, isTranscoding } from './transcoder.js';
import { generateVod } from './vod.js';
import { ensureDir, deleteDir } from './cleanup.js';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const RTMP_PORT = parseInt(process.env.RTMP_PORT || '1935', 10);
const TMP_DIR   = path.resolve(__dirname, process.env.TMP_DIR || './tmp/live');

/**
 * OPEN_MODE controls how stream keys are authorized:
 *   - true  (default) → any well-formed key is accepted; first use auto-registers
 *                       the key in streamkeys.json. Frictionless for solo / dev use.
 *   - false           → strict allow-list: only keys explicitly added via
 *                       `npm run addkey` or POST /api/admin/streamkey are allowed.
 *
 * Defaults to true so OBS just works out of the box. Flip to false for
 * production deployments where you want to gate broadcasters.
 */
const OPEN_MODE = (process.env.OPEN_MODE ?? 'true').toString().toLowerCase() !== 'false';

// streamKey -> {
//   id, label, streamKey, startedAt (ISO), status, hlsUrl, vodUrl,
//   recordingUrl, transcoderHandle
// }
const active = new Map();

let nms = null;

/**
 * Active stream snapshot (sanitized — no internal refs).
 */
function snapshot(entry) {
  return {
    streamKey:    entry.streamKey,
    label:        entry.label,
    startedAt:    entry.startedAt,
    status:       entry.status,
    hlsUrl:       entry.hlsUrl,
    vodUrl:       entry.vodUrl       || null,
    recordingUrl: entry.recordingUrl || null,
  };
}

export function getActiveStreams() {
  return Array.from(active.values()).map(snapshot);
}

export function getActiveStream(streamKey) {
  const entry = active.get(streamKey);
  return entry ? snapshot(entry) : null;
}

/**
 * Parse "/live/{streamKey}" → "{streamKey}".
 * Anything else returns null and the connection is rejected.
 */
function extractStreamKey(streamPath) {
  if (typeof streamPath !== 'string') return null;
  const m = streamPath.match(/^\/live\/([A-Za-z0-9_-]+)\/?$/);
  return m ? m[1] : null;
}

/**
 * Resolve a stream key against either the strict allow-list or open mode.
 * Returns the key entry on success, null when the key is rejected.
 */
async function resolveKey(streamKey) {
  return OPEN_MODE
    ? await autoRegisterKey(streamKey)
    : await validateKey(streamKey);
}

/**
 * Build & start the RTMP server. Idempotent.
 */
export function startRtmpServer() {
  if (nms) return nms;
  console.log(`[rtmp] auth mode: ${OPEN_MODE ? 'OPEN (any key works, auto-registers)' : 'STRICT (allow-list only)'}`);

  const config = {
    rtmp: {
      port:       RTMP_PORT,
      chunk_size: 60000,
      gop_cache:  true,
      ping:       30,
      ping_timeout: 60,
    },
    // We expose HLS via R2, not via node-media-server's built-in http,
    // so we do not enable the http block here.
    logType: 2,
  };

  nms = new NodeMediaServer(config);
  attachHandlers(nms);
  nms.run();
  console.log(`[rtmp] node-media-server listening on rtmp://0.0.0.0:${RTMP_PORT}`);
  return nms;
}

/**
 * Attach session handlers.
 */
function attachHandlers(server) {
  server.on('prePublish', async (id, streamPath /*, args */) => {
    const streamKey = extractStreamKey(streamPath);
    const session   = server.getSession(id);

    if (!streamKey) {
      console.warn(`[rtmp] reject ${id}: malformed path "${streamPath}"`);
      try { session?.reject(); } catch { /* ignore */ }
      return;
    }

    // Validate against streamkeys.json (strict mode) or auto-register (open mode).
    const entry = await resolveKey(streamKey);
    if (!entry) {
      const reason = OPEN_MODE ? 'malformed key' : 'unknown/inactive key';
      console.warn(`[rtmp] reject ${id}: ${reason} "${streamKey}"`);
      try { session?.reject(); } catch { /* ignore */ }
      return;
    }

    // Refuse duplicate publishers for the same key.
    if (active.has(streamKey) || isTranscoding(streamKey)) {
      console.warn(`[rtmp] reject ${id}: key "${streamKey}" is already streaming`);
      try { session?.reject(); } catch { /* ignore */ }
      return;
    }

    console.log(`[rtmp] accept publish ${id} for ${streamKey} (${entry.label})`);
  });

  server.on('postPublish', async (id, streamPath /*, args */) => {
    const streamKey = extractStreamKey(streamPath);
    if (!streamKey) return;
    const entry = await resolveKey(streamKey);
    if (!entry) return;

    // The prePublish guard already filtered here; but be defensive in case of
    // races with stale state.
    if (active.has(streamKey)) return;

    const label = await getLabel(streamKey);

    const streamDir = path.join(TMP_DIR, streamKey);
    // Always start from a clean slate so old segments from a previous crashed
    // session don't get re-uploaded.
    await deleteDir(streamDir);
    await ensureDir(streamDir);

    // Start chokidar BEFORE ffmpeg so we don't miss the very first segment.
    startWatcher(streamKey);

    let transcoderHandle = null;
    try {
      transcoderHandle = await startTranscoder(streamKey);
    } catch (err) {
      console.error(`[rtmp] failed to start transcoder for ${streamKey}: ${err.message}`);
      await stopWatcher(streamKey);
      // Drop the publisher — they shouldn't keep pushing if we can't transcode.
      try { server.getSession(id)?.reject(); } catch { /* ignore */ }
      return;
    }

    // Watch for ffmpeg crashes so we can mark the stream dead and free state
    // even if the publisher is still connected.
    transcoderHandle.exitPromise.then(({ code, signal }) => {
      const a = active.get(streamKey);
      if (!a) return;
      // If exit happens while session is still live, treat it as a crash
      // and tear down. donePublish will run later but be a no-op.
      if (a.status === 'live' && code !== 0 && signal !== 'SIGINT' && signal !== 'SIGTERM') {
        console.error(`[rtmp] ffmpeg for ${streamKey} crashed (code=${code} signal=${signal}); tearing down`);
        a.status = 'crashed';
        try { server.getSession(id)?.reject(); } catch { /* ignore */ }
      }
    });

    active.set(streamKey, {
      id,
      streamKey,
      label,
      startedAt:        new Date().toISOString(),
      status:           'live',
      hlsUrl:           liveMasterUrl(streamKey),
      vodUrl:           null,
      recordingUrl:     null,
      transcoderHandle,
    });

    console.log(`[rtmp] stream LIVE: ${streamKey} (${label})`);
  });

  server.on('donePublish', async (id, streamPath /*, args */) => {
    const streamKey = extractStreamKey(streamPath);
    if (!streamKey) return;
    const a = active.get(streamKey);
    if (!a) return;

    console.log(`[rtmp] stream ENDING: ${streamKey}`);
    a.status = 'ending';

    // Run the rest off the event loop's main path so node-media-server can
    // continue handling other sessions without backpressure.
    finalizeStream(streamKey).catch((err) => {
      console.error(`[rtmp] finalize failed for ${streamKey}: ${err.message}`);
      active.delete(streamKey);
    });
  });
}

/**
 * Stop FFmpeg, drain pending uploads, generate VOD, then evict from the
 * active map.
 */
async function finalizeStream(streamKey) {
  const a = active.get(streamKey);
  if (!a) return;

  try {
    await stopTranscoder(streamKey);
  } catch (err) {
    console.error(`[rtmp] stopTranscoder error for ${streamKey}: ${err.message}`);
  }

  try {
    await flushPending(streamKey);
  } catch (err) {
    console.error(`[rtmp] flushPending error for ${streamKey}: ${err.message}`);
  }

  // CRITICAL: ffmpeg's trailing segments may still be on disk with their
  // chokidar events not yet fired. Walk the dir explicitly and upload
  // anything that hasn't shipped yet — otherwise the VOD comes up short.
  try {
    await sweepStreamDir(streamKey);
  } catch (err) {
    console.error(`[rtmp] sweepStreamDir error for ${streamKey}: ${err.message}`);
  }

  await stopWatcher(streamKey);

  a.status = 'archiving';

  let vodResult = null;
  try {
    vodResult = await generateVod(streamKey);
  } catch (err) {
    console.error(`[rtmp] VOD generation failed for ${streamKey}: ${err.message}`);
  }

  if (vodResult) {
    a.vodUrl       = vodResult.masterUrl    || vodMasterUrl(streamKey);
    a.recordingUrl = vodResult.recordingUrl || vodMp4Url(streamKey);
  }
  a.status   = 'ended';
  a.endedAt  = new Date().toISOString();

  console.log(`[rtmp] stream ENDED: ${streamKey}`);

  // Hand-off complete — remove from the active map. The viewer can still
  // probe vodUrl directly on R2.
  active.delete(streamKey);
}

/**
 * Graceful shutdown: stop every running ffmpeg + watcher.
 */
export async function shutdownRtmp() {
  console.log('[rtmp] shutting down...');
  for (const streamKey of [...active.keys()]) {
    try { await stopTranscoder(streamKey); } catch { /* ignore */ }
    try { await flushPending(streamKey);   } catch { /* ignore */ }
    try { await stopWatcher(streamKey);    } catch { /* ignore */ }
    active.delete(streamKey);
  }
  if (nms) {
    try { nms.stop?.(); } catch { /* ignore */ }
  }
}
