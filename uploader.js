// uploader.js
// Cloudflare R2 upload pipeline.
//
// Responsibilities:
//   1. Maintain a single S3 client pointed at R2.
//   2. Watch each stream's tmp directory with chokidar.
//   3. For every new/changed file:
//        - upload to R2 (max MAX_UPLOAD_CONCURRENCY in parallel, server-wide)
//        - retry up to 3 times with exponential backoff
//        - on success: delete local .ts files immediately
//          (keep .m3u8 playlists so ffmpeg can rewrite them in place)
//   4. Keep at most ~8 segments per quality in R2 (rotation).
//
// Public API:
//   getR2Client()                     -> S3 client (lazily built)
//   uploadFile({key, body, type})     -> generic upload helper
//   deleteR2Object(key)               -> delete one R2 object
//   startWatcher(streamKey)           -> begin watching tmp/live/<streamKey>
//   stopWatcher(streamKey)            -> close watcher (called when stream ends)
//   flushPending(streamKey)           -> awaits any in-flight uploads for the stream
//   uploadLocalFile(streamKey, file)  -> manually upload a specific file (used by VOD)

import 'dotenv/config';
import path from 'node:path';
import { promises as fs } from 'node:fs';
import { fileURLToPath } from 'node:url';
import chokidar from 'chokidar';
import pLimit from 'p-limit';
import {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  ListObjectsV2Command,
  CopyObjectCommand,
  GetObjectCommand,
} from '@aws-sdk/client-s3';
import { createWriteStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';

import { deleteFile } from './cleanup.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const TMP_DIR        = path.resolve(__dirname, process.env.TMP_DIR || './tmp/live');
const BUCKET         = process.env.R2_BUCKET_NAME;
const ACCOUNT_ID     = process.env.R2_ACCOUNT_ID;
const ACCESS_KEY     = process.env.R2_ACCESS_KEY_ID;
const SECRET_KEY     = process.env.R2_SECRET_ACCESS_KEY;
const PUBLIC_URL     = (process.env.R2_PUBLIC_URL || '').replace(/\/+$/, '');
const MAX_PARALLEL   = parseInt(process.env.MAX_UPLOAD_CONCURRENCY || '5', 10);

// Number of newest segments per quality to keep in R2. Anything older is purged.
const LIVE_ROTATION_KEEP = 8;

// Renditions whose segments are NOT rotated during live — we need them for
// the post-stream MP4 concat / VOD playlist. Other qualities rotate to
// minimize R2 storage cost while live.
const VOD_SOURCE_QUALITIES = new Set(['1080p']);

// ---------------------------------------------------------------------------
// S3 / R2 client (lazy singleton)
// ---------------------------------------------------------------------------
let _client = null;
export function getR2Client() {
  if (_client) return _client;
  if (!ACCOUNT_ID || !ACCESS_KEY || !SECRET_KEY || !BUCKET) {
    throw new Error('[uploader] R2_* environment variables are not fully configured');
  }
  _client = new S3Client({
    region: 'auto',
    endpoint: `https://${ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId:     ACCESS_KEY,
      secretAccessKey: SECRET_KEY,
    },
  });
  return _client;
}

// ---------------------------------------------------------------------------
// Server-wide upload concurrency limiter
// ---------------------------------------------------------------------------
const limit = pLimit(MAX_PARALLEL);

// Per-stream bookkeeping. Each entry:
//   {
//     watcher,                       // chokidar instance
//     pending: Set<Promise>,         // in-flight upload promises (so we can flush)
//     segmentHistory: Map<quality, string[]>  // ordered seg filenames per quality
//   }
const streams = new Map();

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function contentTypeFor(file) {
  if (file.endsWith('.m3u8')) return 'application/vnd.apple.mpegurl';
  if (file.endsWith('.ts'))   return 'video/MP2T';
  if (file.endsWith('.mp4'))  return 'video/mp4';
  return 'application/octet-stream';
}

function cacheControlFor(file) {
  if (file.endsWith('.m3u8')) return 'no-cache, no-store, must-revalidate';
  if (file.endsWith('.ts'))   return 'public, max-age=31536000, immutable';
  if (file.endsWith('.mp4'))  return 'public, max-age=31536000, immutable';
  return 'public, max-age=60';
}

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

/**
 * Build the public URL of an R2 object key. Used by API responses and
 * the master playlist generator.
 */
export function publicUrlFor(key) {
  return `${PUBLIC_URL}/${key}`;
}

// ---------------------------------------------------------------------------
// Low-level R2 ops
// ---------------------------------------------------------------------------

/**
 * Upload an in-memory body or a file path to R2 with retries.
 * Internal — callers should usually go through `uploadLocalFile`.
 */
async function putWithRetry({ key, body, contentType, cacheControl }) {
  const client = getR2Client();
  let attempt = 0;
  let lastErr;
  while (attempt < 3) {
    try {
      await client.send(new PutObjectCommand({
        Bucket: BUCKET,
        Key:    key,
        Body:   body,
        ContentType:  contentType,
        CacheControl: cacheControl,
      }));
      return;
    } catch (err) {
      lastErr = err;
      attempt += 1;
      const delay = 250 * Math.pow(2, attempt); // 500ms, 1000ms, 2000ms
      console.warn(`[uploader] PUT ${key} failed (attempt ${attempt}/3): ${err.message}. retrying in ${delay}ms`);
      await sleep(delay);
    }
  }
  throw new Error(`[uploader] PUT ${key} failed after 3 attempts: ${lastErr?.message}`);
}

/**
 * Generic upload helper used by transcoder.js (master.m3u8) and vod.js.
 */
export async function uploadFile({ key, body, contentType, cacheControl }) {
  const ct = contentType  || contentTypeFor(key);
  const cc = cacheControl || cacheControlFor(key);
  return limit(() => putWithRetry({ key, body, contentType: ct, cacheControl: cc }));
}

/**
 * Delete one R2 object. Failures are logged but do not throw — segment
 * rotation is a best-effort cleanup.
 */
export async function deleteR2Object(key) {
  try {
    const client = getR2Client();
    await client.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: key }));
    console.log(`[uploader ${new Date().toISOString()}] R2 deleted: ${key}`);
  } catch (err) {
    console.warn(`[uploader] R2 delete ${key} failed: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// Per-stream upload pipeline
// ---------------------------------------------------------------------------

/**
 * Translate a local segment path under tmp/live/<streamKey>/... into the
 * matching R2 object key under live/<streamKey>/...
 */
function localToR2Key(streamKey, localPath) {
  const streamRoot = path.join(TMP_DIR, streamKey);
  const rel = path.relative(streamRoot, localPath).split(path.sep).join('/');
  return `live/${streamKey}/${rel}`;
}

/**
 * Upload a single local file to R2. After a successful .ts upload the local
 * file is deleted to keep disk usage minimal. Playlist files are kept locally
 * because ffmpeg keeps overwriting them while the stream is live.
 */
export async function uploadLocalFile(streamKey, localPath) {
  const state = streams.get(streamKey);
  if (!state) return; // watcher already torn down

  const key = localToR2Key(streamKey, localPath);
  const ext = path.extname(localPath).toLowerCase();

  const job = limit(async () => {
    let body;
    try {
      body = await fs.readFile(localPath);
    } catch (err) {
      // The file vanished between the chokidar event and the read — likely
      // because ffmpeg's `delete_segments` already rotated it. Not fatal.
      if (err.code === 'ENOENT') return;
      throw err;
    }

    await putWithRetry({
      key,
      body,
      contentType:  contentTypeFor(localPath),
      cacheControl: cacheControlFor(localPath),
    });

    console.log(`[uploader ${new Date().toISOString()}] uploaded: ${key} (${body.length} bytes)`);

    if (ext === '.ts') {
      // Local file is no longer needed — keep disk near zero.
      await deleteFile(localPath);
      // Track for live rotation in R2.
      trackSegmentForRotation(streamKey, localPath, key);
    }
    // For .m3u8 we KEEP the local copy because ffmpeg will rewrite it.
    // For .mp4 (recording) the VOD module manages its own cleanup.
  });

  state.pending.add(job);
  job.finally(() => state.pending.delete(job));

  try {
    await job;
  } catch (err) {
    console.error(`[uploader] upload pipeline error for ${localPath}: ${err.message}`);
  }
}

/**
 * Track a freshly uploaded .ts segment and prune anything beyond the
 * LIVE_ROTATION_KEEP newest segments for that quality.
 */
function trackSegmentForRotation(streamKey, localPath, r2Key) {
  const state = streams.get(streamKey);
  if (!state) return;

  // quality is the immediate parent dir under the stream tmp root.
  const quality = path.basename(path.dirname(localPath));
  if (!state.segmentHistory.has(quality)) {
    state.segmentHistory.set(quality, []);
  }
  const list = state.segmentHistory.get(quality);
  list.push(r2Key);

  // VOD source qualities (e.g. 1080p) are kept on R2 for post-stream
  // concat/VOD playlist generation. Other qualities rotate aggressively.
  if (VOD_SOURCE_QUALITIES.has(quality)) return;

  while (list.length > LIVE_ROTATION_KEEP) {
    const stale = list.shift();
    deleteR2Object(stale).catch(() => { /* logged inside */ });
  }
}

// ---------------------------------------------------------------------------
// Chokidar watcher per stream
// ---------------------------------------------------------------------------

/**
 * Begin watching ./tmp/live/<streamKey>/ for new/updated files.
 * Idempotent — calling twice for the same stream is a no-op.
 */
export function startWatcher(streamKey) {
  if (streams.has(streamKey)) return;

  const streamDir = path.join(TMP_DIR, streamKey);

  const watcher = chokidar.watch(streamDir, {
    ignoreInitial:    false,
    persistent:       true,
    awaitWriteFinish: {
      // Wait for ffmpeg to finish writing the file before we read it.
      stabilityThreshold: 250,
      pollInterval:       50,
    },
  });

  const state = {
    watcher,
    pending:         new Set(),
    segmentHistory:  new Map(),
  };
  streams.set(streamKey, state);

  const onFile = (localPath) => {
    if (!localPath) return;
    const ext = path.extname(localPath).toLowerCase();
    if (ext !== '.ts' && ext !== '.m3u8') return;
    uploadLocalFile(streamKey, localPath);
  };

  watcher
    .on('add',    onFile)
    .on('change', onFile)
    .on('error',  (err) => console.error(`[uploader] watcher error for ${streamKey}: ${err.message}`));

  console.log(`[uploader] watching ${streamDir}`);
}

/**
 * Wait for any in-flight uploads for this stream to settle.
 */
export async function flushPending(streamKey) {
  const state = streams.get(streamKey);
  if (!state) return;
  while (state.pending.size > 0) {
    await Promise.allSettled(Array.from(state.pending));
  }
}

/**
 * Close the chokidar watcher and forget about the stream.
 * Caller should typically `flushPending` first.
 */
export async function stopWatcher(streamKey) {
  const state = streams.get(streamKey);
  if (!state) return;
  try {
    await state.watcher.close();
  } catch (err) {
    console.warn(`[uploader] watcher close error for ${streamKey}: ${err.message}`);
  }
  streams.delete(streamKey);
  console.log(`[uploader] stopped watching ${streamKey}`);
}

/**
 * List every object key under a given prefix (paginated).
 */
export async function listKeys(prefix) {
  const client = getR2Client();
  const keys = [];
  let token;
  do {
    const out = await client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: prefix,
      ContinuationToken: token,
    }));
    for (const o of (out.Contents || [])) keys.push(o.Key);
    token = out.IsTruncated ? out.NextContinuationToken : undefined;
  } while (token);
  return keys;
}

/**
 * Server-side copy from one R2 key to another. Uses the upload-concurrency
 * limit so we don't exhaust R2's per-request budget.
 */
export async function copyR2Object(sourceKey, destKey) {
  return limit(async () => {
    const client = getR2Client();
    await client.send(new CopyObjectCommand({
      Bucket:     BUCKET,
      Key:        destKey,
      CopySource: encodeURIComponent(`${BUCKET}/${sourceKey}`),
    }));
  });
}

/**
 * Stream-download an R2 object to a local file path.
 */
export async function downloadToFile(key, localPath) {
  return limit(async () => {
    const client = getR2Client();
    const out = await client.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
    if (!out.Body) throw new Error(`R2 GET ${key}: empty body`);
    await pipeline(out.Body, createWriteStream(localPath));
  });
}

/**
 * Bulk-delete every object under a prefix. Used to wipe live/<streamKey>/*
 * after the VOD has been generated.
 */
export async function deletePrefix(prefix) {
  const keys = await listKeys(prefix);
  if (!keys.length) return 0;
  const client = getR2Client();
  // S3 DeleteObjects takes max 1000 keys per call.
  for (let i = 0; i < keys.length; i += 1000) {
    const batch = keys.slice(i, i + 1000);
    try {
      await client.send(new DeleteObjectsCommand({
        Bucket: BUCKET,
        Delete: { Objects: batch.map((Key) => ({ Key })), Quiet: true },
      }));
    } catch (err) {
      console.warn(`[uploader] bulk delete batch failed: ${err.message}`);
    }
  }
  console.log(`[uploader ${new Date().toISOString()}] R2 deleted ${keys.length} objects under prefix ${prefix}`);
  return keys.length;
}

/**
 * Build the public HLS URL for a stream's master playlist.
 */
export function liveMasterUrl(streamKey) {
  return publicUrlFor(`live/${streamKey}/master.m3u8`);
}

/**
 * Build the public VOD URL for a finished stream.
 */
export function vodMasterUrl(streamKey) {
  return publicUrlFor(`vod/${streamKey}/master.m3u8`);
}

/**
 * Build the public MP4 recording URL for a finished stream.
 */
export function vodMp4Url(streamKey) {
  return publicUrlFor(`vod/${streamKey}/recording.mp4`);
}
