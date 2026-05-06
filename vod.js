// vod.js
// Post-stream archival pipeline. Runs after the FFmpeg transcoder for a
// stream has fully exited and its remaining segments have been flushed
// to R2.
//
// Source quality selection:
//   We prefer 1080p, but if no 1080p segments exist on R2 (e.g. ffmpeg
//   crashed early, or a future config skipped 1080p) we fall back to the
//   next-highest rendition that has segments. Renditions are walked in
//   the order returned by `getRenditions()` — currently 1080p → 720p → 480p.
//
// Pipeline:
//   1. Discover the highest-quality rendition with segments on R2.
//   2. Server-side copy live/<streamKey>/<q>/*.ts → vod/<streamKey>/<q>/...
//      so they survive any later live/ cleanup or lifecycle rules.
//   3. Download them to a local tmp/vod/<streamKey>/concat/ dir.
//   4. ffmpeg -f concat ... -c copy recording.mp4   (no re-encode).
//   5. Upload recording.mp4 to vod/<streamKey>/recording.mp4.
//   6. Generate & upload:
//        vod/<streamKey>/<q>/index.m3u8   (HLS VOD media playlist)
//        vod/<streamKey>/master.m3u8       (HLS master)
//   7. Delete live/<streamKey>/* from R2 and the local tmp tree.

import 'dotenv/config';
import path from 'node:path';
import { promises as fs } from 'node:fs';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { ensureDir, deleteDir } from './cleanup.js';
import {
  listKeys,
  copyR2Object,
  downloadToFile,
  deletePrefix,
  uploadFile,
  publicUrlFor,
} from './uploader.js';
import { getRenditions } from './transcoder.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const FFMPEG_PATH    = process.env.FFMPEG_PATH || 'ffmpeg';
const TMP_DIR_LIVE   = path.resolve(__dirname, process.env.TMP_DIR || './tmp/live');
const TMP_DIR_VOD    = path.resolve(__dirname, path.dirname(process.env.TMP_DIR || './tmp/live'), 'vod');
const HLS_TARGET_S   = 4;          // matches transcoder hls_time

/**
 * Preferred VOD quality, in priority order. The first one that actually
 * has segments on R2 is used. `getRenditions()` already returns them
 * highest-first, but we also expose this as a constant for clarity.
 */
const PREFERRED_VOD_QUALITY = '1080p';

/**
 * Sort segment object keys by their numeric index so concat order is correct.
 * Expected naming pattern: ".../seg00001.ts" "seg00002.ts" ...
 */
function sortSegmentKeys(keys) {
  const num = (k) => {
    const m = k.match(/seg(\d+)\.ts$/i);
    return m ? parseInt(m[1], 10) : 0;
  };
  return [...keys].sort((a, b) => num(a) - num(b));
}

/**
 * Walk renditions highest → lowest and return the first one that has
 * .ts segments at  live/<streamKey>/<quality>/  on R2. Returns
 * { rendition, segmentKeys } or null if no quality has any segments.
 */
async function findVodSource(streamKey) {
  // Renditions are already ordered highest→lowest in transcoder.js, but
  // we promote PREFERRED_VOD_QUALITY to the front defensively in case that
  // ordering ever changes.
  const renditions = getRenditions();
  renditions.sort((a, b) => {
    if (a.name === PREFERRED_VOD_QUALITY) return -1;
    if (b.name === PREFERRED_VOD_QUALITY) return  1;
    // Otherwise prefer higher height.
    return b.height - a.height;
  });

  for (const rendition of renditions) {
    const prefix = `live/${streamKey}/${rendition.name}/`;
    const allKeys = await listKeys(prefix);
    const segmentKeys = sortSegmentKeys(allKeys.filter((k) => k.endsWith('.ts')));
    if (segmentKeys.length > 0) {
      return { rendition, segmentKeys };
    }
    console.log(`[vod] ${streamKey}: no segments at ${prefix}, trying next quality`);
  }
  return null;
}

/**
 * Run an ffmpeg child process and wait for it to exit.
 */
function runFfmpeg(args, label) {
  return new Promise((resolve, reject) => {
    console.log(`[vod] ${label}: ${FFMPEG_PATH} ${args.join(' ')}`);
    const proc = spawn(FFMPEG_PATH, args, { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
    proc.stdout.on('data', (c) => process.stdout.write(`[vod-ffmpeg ${label}] ${c}`));
    proc.stderr.on('data', (c) => process.stderr.write(`[vod-ffmpeg ${label}] ${c}`));
    proc.once('error', reject);
    proc.once('exit', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg ${label} exited with code ${code}`));
    });
  });
}

/**
 * Build the VOD media playlist (#EXT-X-PLAYLIST-TYPE:VOD) for the chosen
 * source rendition. References segments by their public R2 URL.
 */
function buildVodMediaPlaylist(streamKey, sortedKeys) {
  // Each segment is HLS_TARGET_S long except possibly the last; we cannot
  // know real durations cheaply without probing each .ts, so we advertise
  // the target. Players tolerate small deviations on the trailing segment.
  const lines = [
    '#EXTM3U',
    '#EXT-X-VERSION:3',
    `#EXT-X-TARGETDURATION:${HLS_TARGET_S}`,
    '#EXT-X-MEDIA-SEQUENCE:0',
    '#EXT-X-PLAYLIST-TYPE:VOD',
  ];

  for (const key of sortedKeys) {
    const vodKey = key.replace(`live/${streamKey}/`, `vod/${streamKey}/`);
    lines.push(`#EXTINF:${HLS_TARGET_S.toFixed(3)},`);
    lines.push(publicUrlFor(vodKey));
  }
  lines.push('#EXT-X-ENDLIST', '');
  return lines.join('\n');
}

/**
 * Build the VOD master playlist that references the chosen rendition's
 * VOD media playlist.
 */
function buildVodMasterPlaylist(streamKey, rendition) {
  const codec = 'avc1.4d401f,mp4a.40.2';
  return [
    '#EXTM3U',
    '#EXT-X-VERSION:3',
    `#EXT-X-STREAM-INF:BANDWIDTH=${rendition.bandwidth},RESOLUTION=${rendition.width}x${rendition.height},CODECS="${codec}",NAME="${rendition.name}"`,
    publicUrlFor(`vod/${streamKey}/${rendition.name}/index.m3u8`),
    '',
  ].join('\n');
}

/**
 * Main entry point. Resolves with `{ recordingUrl, masterUrl }` on success.
 */
export async function generateVod(streamKey) {
  console.log(`[vod] starting VOD pipeline for ${streamKey}`);

  const concatDir = path.join(TMP_DIR_VOD, streamKey, 'concat');
  const outDir    = path.join(TMP_DIR_VOD, streamKey);
  await ensureDir(concatDir);

  // ---- 1. Pick the highest-quality rendition that has segments on R2.
  const source = await findVodSource(streamKey);
  if (!source) {
    console.warn(`[vod] no segments found on R2 for any quality (${streamKey}); aborting VOD`);
    await deleteDir(path.join(TMP_DIR_VOD, streamKey));
    await deletePrefix(`live/${streamKey}/`);
    return null;
  }
  const { rendition, segmentKeys } = source;
  const quality = rendition.name;

  if (quality !== PREFERRED_VOD_QUALITY) {
    console.warn(`[vod] ${streamKey}: ${PREFERRED_VOD_QUALITY} unavailable, falling back to ${quality}`);
  }
  console.log(`[vod] ${segmentKeys.length} ${quality} segments to archive for ${streamKey}`);

  // ---- 2. Server-side copy live -> vod (parallel-safe via uploader's limiter).
  const copyJobs = segmentKeys.map((srcKey) => {
    const dstKey = srcKey.replace(`live/${streamKey}/`, `vod/${streamKey}/`);
    return copyR2Object(srcKey, dstKey).catch((err) => {
      console.warn(`[vod] copy ${srcKey} -> ${dstKey} failed: ${err.message}`);
    });
  });
  await Promise.all(copyJobs);

  // ---- 3. Download segments locally for concat.
  const localFiles = [];
  for (let i = 0; i < segmentKeys.length; i += 1) {
    const srcKey = segmentKeys[i];
    const localFile = path.join(concatDir, `seg${String(i).padStart(6, '0')}.ts`);
    try {
      await downloadToFile(srcKey, localFile);
      localFiles.push(localFile);
    } catch (err) {
      console.warn(`[vod] download ${srcKey} failed: ${err.message}; skipping`);
    }
  }
  if (!localFiles.length) {
    throw new Error(`[vod] no segments could be downloaded for ${streamKey}`);
  }

  // ---- 4. Concat to MP4 (no re-encode).
  const listFile = path.join(concatDir, 'concat.txt');
  await fs.writeFile(
    listFile,
    localFiles.map((p) => `file '${p.replace(/'/g, "'\\''")}'`).join('\n'),
    'utf8',
  );

  const mp4Path = path.join(outDir, 'recording.mp4');
  await runFfmpeg(
    [
      '-hide_banner', '-loglevel', 'warning',
      '-f', 'concat', '-safe', '0',
      '-i', listFile,
      '-c', 'copy',
      '-bsf:a', 'aac_adtstoasc',
      '-movflags', '+faststart',
      '-y', mp4Path,
    ],
    `concat-${streamKey}`,
  );

  // ---- 5. Upload recording.mp4.
  const mp4Body = await fs.readFile(mp4Path);
  await uploadFile({
    key:          `vod/${streamKey}/recording.mp4`,
    body:         mp4Body,
    contentType:  'video/mp4',
    cacheControl: 'public, max-age=31536000, immutable',
  });
  console.log(`[vod] recording.mp4 uploaded for ${streamKey} (${mp4Body.length} bytes)`);

  // ---- 6. Build & upload VOD playlists.
  const mediaPlaylist  = buildVodMediaPlaylist(streamKey, segmentKeys);
  const masterPlaylist = buildVodMasterPlaylist(streamKey, rendition);

  await uploadFile({
    key:          `vod/${streamKey}/${quality}/index.m3u8`,
    body:         Buffer.from(mediaPlaylist, 'utf8'),
    contentType:  'application/vnd.apple.mpegurl',
    cacheControl: 'public, max-age=300',
  });
  await uploadFile({
    key:          `vod/${streamKey}/master.m3u8`,
    body:         Buffer.from(masterPlaylist, 'utf8'),
    contentType:  'application/vnd.apple.mpegurl',
    cacheControl: 'public, max-age=300',
  });

  // ---- 7. Cleanup.
  await deleteDir(path.join(TMP_DIR_LIVE, streamKey));
  await deleteDir(path.join(TMP_DIR_VOD,  streamKey));
  await deletePrefix(`live/${streamKey}/`);

  console.log(`[vod] DONE ${streamKey}`);
  return {
    recordingUrl: publicUrlFor(`vod/${streamKey}/recording.mp4`),
    masterUrl:    publicUrlFor(`vod/${streamKey}/master.m3u8`),
  };
}
