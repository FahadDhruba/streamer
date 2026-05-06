// transcoder.js
// Spawns one FFmpeg process per live stream that pulls from the local
// node-media-server RTMP loopback and produces three HLS renditions
// (1080p / 720p / 480p) plus a custom master.m3u8 pointing to the R2
// public URLs.
//
// Public API:
//   startTranscoder(streamKey)   -> Promise<TranscoderHandle>
//   stopTranscoder(streamKey)    -> Promise<void>
//   isTranscoding(streamKey)     -> boolean
//   getRenditions()              -> array describing each rendition

import 'dotenv/config';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { ensureDir } from './cleanup.js';
import { uploadFile, publicUrlFor } from './uploader.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const FFMPEG_PATH = process.env.FFMPEG_PATH || 'ffmpeg';
const TMP_DIR = path.resolve(__dirname, process.env.TMP_DIR || './tmp/live');
const RTMP_PORT = parseInt(process.env.RTMP_PORT || '1935', 10);

/**
 * Adaptive-bitrate ladder.
 * `bandwidth` is the value advertised in master.m3u8 (≈ video + audio bitrate
 * with a safety margin).
 */
const RENDITIONS = [
  // { name: '1080p', width: 1920, height: 1080, vBitrate: '4000k', vMaxRate: '4280k', vBufSize: '6000k', aBitrate: '192k', bandwidth: 4500000 },
  { name: '720p', width: 1280, height: 720, vBitrate: '2500k', vMaxRate: '2675k', vBufSize: '3750k', aBitrate: '128k', bandwidth: 2800000 },
  { name: '480p', width: 854, height: 480, vBitrate: '1000k', vMaxRate: '1070k', vBufSize: '1500k', aBitrate: '96k', bandwidth: 1200000 },
];

const HLS_SEGMENT_DURATION = 2;   // faster playlist updates = less freezing
const HLS_LIST_SIZE = 4;   // less disk/memory on t2.micro    // segments retained in live playlist
const KEYFRAME_INTERVAL_S = 2;       // forced GOP length (seconds)

// streamKey -> { proc, stopping, exitPromise, handle }
const processes = new Map();

export function getRenditions() {
  return RENDITIONS.map((r) => ({ ...r }));
}

export function isTranscoding(streamKey) {
  return processes.has(streamKey);
}

/**
 * Build the full ffmpeg argv for one stream. We use a single process with
 * -filter_complex split + -var_stream_map so all three renditions come from
 * one decode pass — cheaper CPU than three independent ffmpegs.
 */
function buildFfmpegArgs(streamKey) {
  const inputUrl = `rtmp://127.0.0.1:${RTMP_PORT}/live/${streamKey}`;
  const streamDir = path.join(TMP_DIR, streamKey);

  // %v gets replaced per-rendition by ffmpeg using the `name:` token in var_stream_map.
  const segmentTemplate = path.join(streamDir, '%v', 'seg%05d.ts');
  const playlistTemplate = path.join(streamDir, '%v', 'index.m3u8');

  // [0:v]split=3[v1][v2][v3]; [vN]scale=w=W:h=H[vNout]
  const filter =
    `[0:v]split=${RENDITIONS.length}` +
    RENDITIONS.map((_, i) => `[v${i + 1}]`).join('') +
    '; ' +
    RENDITIONS.map((r, i) => `[v${i + 1}]scale=w=${r.width}:h=${r.height}:force_original_aspect_ratio=decrease,pad=${r.width}:${r.height}:(ow-iw)/2:(oh-ih)/2[v${i + 1}out]`).join('; ');

  const args = [
    '-hide_banner',
    '-loglevel', 'warning',
    '-fflags', '+genpts+nobuffer+discardcorrupt',
    '-flags', 'low_delay',
    '-probesize', '32768',
    '-analyzeduration', '0',
    '-thread_queue_size', '512',
    '-i', inputUrl,
    '-filter_complex', filter,
  ];

  // Per-rendition video encoders.
  RENDITIONS.forEach((r, i) => {
    args.push(
      '-map', `[v${i + 1}out]`,
      `-c:v:${i}`, 'libx264',
      `-preset:v:${i}`, 'ultrafast',
      `-tune:v:${i}`, 'zerolatency',   // ← ADD
      `-profile:v:${i}`, 'baseline',      // ← baseline not main (better compat)
      `-threads`, '1',             // ← limit per-rendition threads on 1 vCPU
      `-pix_fmt`, 'yuv420p',
      `-b:v:${i}`, r.vBitrate,
      `-maxrate:v:${i}`, r.vMaxRate,
      `-bufsize:v:${i}`, r.vBufSize,
      `-force_key_frames:v:${i}`, `expr:gte(t,n_forced*${KEYFRAME_INTERVAL_S})`,
      `-sc_threshold`, '0',
    );
  });

  // Per-rendition audio encoders (one AAC track per rendition, all from input audio).
  RENDITIONS.forEach((r, i) => {
    args.push(
      '-map', 'a:0?',
      `-c:a:${i}`, 'aac',
      `-b:a:${i}`, r.aBitrate,
      `-ac:a:${i}`, '2',
      `-ar:a:${i}`, '48000',
    );
  });

  // HLS muxing.
  const varStreamMap = RENDITIONS
    .map((r, i) => `v:${i},a:${i},name:${r.name}`)
    .join(' ');

  args.push(
    '-f', 'hls',
    '-hls_time', String(HLS_SEGMENT_DURATION),
    '-hls_list_size', String(HLS_LIST_SIZE),
    '-hls_flags', 'delete_segments+independent_segments+program_date_time',
    '-hls_segment_type', 'mpegts',
    '-hls_segment_filename', segmentTemplate,
    '-master_pl_name', '_ffmpeg_master.m3u8',
    '-var_stream_map', varStreamMap,
    playlistTemplate,
  );

  return args;
}

/**
 * Build and upload our own master.m3u8 with absolute R2 URLs.
 * Called once per stream right after ffmpeg starts.
 */
async function uploadMasterPlaylist(streamKey) {
  const lines = ['#EXTM3U', '#EXT-X-VERSION:3'];
  for (const r of RENDITIONS) {
    const codec = 'avc1.4d401f,mp4a.40.2'; // H.264 main + AAC-LC
    lines.push(
      `#EXT-X-STREAM-INF:BANDWIDTH=${r.bandwidth},RESOLUTION=${r.width}x${r.height},CODECS="${codec}",NAME="${r.name}"`,
      publicUrlFor(`live/${streamKey}/${r.name}/index.m3u8`),
    );
  }
  const body = lines.join('\n') + '\n';
  await uploadFile({
    key: `live/${streamKey}/master.m3u8`,
    body: Buffer.from(body, 'utf8'),
    contentType: 'application/vnd.apple.mpegurl',
    cacheControl: 'no-cache, no-store, must-revalidate',
  });
  console.log(`[transcoder] master.m3u8 uploaded for ${streamKey}`);
}

/**
 * Spawn the FFmpeg ABR pipeline for `streamKey`.
 * Resolves once the process is running. Caller still has to await
 * `handle.exitPromise` to know when ffmpeg has finished.
 */
export async function startTranscoder(streamKey) {
  if (processes.has(streamKey)) {
    console.warn(`[transcoder] ${streamKey} already transcoding`);
    return processes.get(streamKey).handle;
  }

  const streamDir = path.join(TMP_DIR, streamKey);

  // Pre-create per-rendition output dirs so ffmpeg never races on mkdir.
  await ensureDir(streamDir);
  await Promise.all(RENDITIONS.map((r) => ensureDir(path.join(streamDir, r.name))));

  const args = buildFfmpegArgs(streamKey);

  console.log(`[transcoder] spawning ffmpeg for ${streamKey}`);
  console.log(`[transcoder] cmd: ${FFMPEG_PATH} ${args.join(' ')}`);

  const proc = spawn(FFMPEG_PATH, args, {
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
  });

  proc.stdout.on('data', (chunk) => {
    process.stdout.write(`[ffmpeg ${streamKey} stdout] ${chunk}`);
  });
  proc.stderr.on('data', (chunk) => {
    // ffmpeg writes everything to stderr — most of it is informational.
    process.stderr.write(`[ffmpeg ${streamKey}] ${chunk}`);
  });

  const exitPromise = new Promise((resolve) => {
    proc.once('exit', (code, signal) => {
      console.log(`[transcoder] ffmpeg for ${streamKey} exited code=${code} signal=${signal}`);
      processes.delete(streamKey);
      resolve({ code, signal });
    });
    proc.once('error', (err) => {
      console.error(`[transcoder] ffmpeg spawn error for ${streamKey}: ${err.message}`);
      processes.delete(streamKey);
      resolve({ code: -1, signal: null, error: err });
    });
  });

  const handle = {
    streamKey,
    proc,
    streamDir,
    exitPromise,
    /** Ask ffmpeg to flush & exit gracefully. */
    stop: () => stopTranscoder(streamKey),
  };

  processes.set(streamKey, { proc, stopping: false, exitPromise, handle });

  // Build the canonical master.m3u8 with absolute R2 URLs and push it once.
  // Even though ffmpeg also writes a master playlist, we override it because
  // we need URLs that point at the R2 public bucket, not local filenames.
  uploadMasterPlaylist(streamKey).catch((err) => {
    console.error(`[transcoder] failed to upload master.m3u8 for ${streamKey}: ${err.message}`);
  });

  return handle;
}

/**
 * Gracefully stop the ffmpeg process for `streamKey`. Sends SIGINT to allow
 * ffmpeg to flush the trailing segment & playlist, with a hard SIGKILL after
 * a 10-second grace period.
 */
export async function stopTranscoder(streamKey) {
  const entry = processes.get(streamKey);
  if (!entry) return;
  if (entry.stopping) return entry.exitPromise;
  entry.stopping = true;

  const { proc, exitPromise } = entry;

  try {
    if (process.platform === 'win32') {
      // SIGINT delivery is unreliable on Windows; SIGTERM works for ffmpeg.
      proc.kill('SIGTERM');
    } else {
      proc.kill('SIGINT');
    }
  } catch (err) {
    console.warn(`[transcoder] kill error for ${streamKey}: ${err.message}`);
  }

  const timeout = new Promise((resolve) => setTimeout(() => {
    if (!proc.killed) {
      console.warn(`[transcoder] ffmpeg for ${streamKey} did not exit in 10s, force killing`);
      try { proc.kill('SIGKILL'); } catch { /* ignore */ }
    }
    resolve('timeout');
  }, 10_000));

  await Promise.race([exitPromise, timeout]);
  // Make sure exit handler ran.
  await exitPromise;
}
