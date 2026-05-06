// routes/api.js
// All HTTP endpoints exposed by the Express app.
//
//   GET    /api/streams                            list active streams
//   GET    /api/stream/:streamKey/status           live/vod URLs for a key
//   GET    /api/stream/:streamKey/master.m3u8      302 → R2 master playlist
//   POST   /api/admin/streamkey                    create new key
//   DELETE /api/admin/streamkey/:streamKey         deactivate a key

import 'dotenv/config';
import { Router } from 'express';

import { getActiveStreams, getActiveStream } from '../rtmp.js';
import { addKey, deactivateKey, loadKeys, validateKey } from '../streamkeys.js';
import {
  liveMasterUrl,
  vodMasterUrl,
  vodMp4Url,
} from '../uploader.js';

const ADMIN_TOKEN = (process.env.ADMIN_TOKEN || '').trim();
const RTMP_PORT   = parseInt(process.env.RTMP_PORT || '1935', 10);

const router = Router();

/**
 * Optional shared-secret guard on /api/admin/* routes.
 * If ADMIN_TOKEN is empty (the default in `.env.example`), the guard is a
 * no-op so local development is frictionless. In production set the env var
 * and clients must send `X-Admin-Token: <value>`.
 */
function adminGuard(req, res, next) {
  if (!ADMIN_TOKEN) return next();
  const provided = req.get('X-Admin-Token') || '';
  if (provided !== ADMIN_TOKEN) {
    return res.status(401).json({ error: 'invalid admin token' });
  }
  next();
}

/**
 * GET /api/streams
 * List currently active streams.
 */
router.get('/streams', (_req, res) => {
  const streams = getActiveStreams().map((s) => ({
    streamKey: s.streamKey,
    label:     s.label,
    startedAt: s.startedAt,
    status:    s.status,
    viewerUrl: s.hlsUrl,
  }));
  res.json({ count: streams.length, streams });
});

/**
 * GET /api/stream/:streamKey/status
 * Returns live + vod URLs for a key (whether it's currently broadcasting or not).
 */
router.get('/stream/:streamKey/status', async (req, res) => {
  const { streamKey } = req.params;

  const live = getActiveStream(streamKey);
  if (live) {
    return res.json({
      live:         true,
      status:       live.status,
      label:        live.label,
      startedAt:    live.startedAt,
      hlsUrl:       live.hlsUrl,
      vodUrl:       live.vodUrl,
      recordingUrl: live.recordingUrl,
    });
  }

  // Not currently live — surface the would-be URLs so a client can probe
  // R2 directly to see if a VOD exists for this key.
  const keys = await loadKeys();
  if (!keys[streamKey]) {
    return res.status(404).json({ error: 'unknown stream key' });
  }

  res.json({
    live:         false,
    label:        keys[streamKey].label,
    hlsUrl:       liveMasterUrl(streamKey),
    vodUrl:       vodMasterUrl(streamKey),
    recordingUrl: vodMp4Url(streamKey),
  });
});

/**
 * GET /api/stream/:streamKey/master.m3u8
 * Convenience proxy: 302-redirects to the R2 master playlist URL so HLS.js
 * users can paste a server URL like:
 *   https://yourapp.com/api/stream/sk_xxx/master.m3u8
 */
router.get('/stream/:streamKey/master.m3u8', (req, res) => {
  const { streamKey } = req.params;
  res.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.redirect(302, liveMasterUrl(streamKey));
});

/**
 * POST /api/admin/streamkey
 * Body: { label: "MyChannel" }
 * Creates a new stream key and returns the connection details.
 */
router.post('/admin/streamkey', adminGuard, async (req, res) => {
  const label = (req.body?.label || '').toString().trim();
  if (!label) {
    return res.status(400).json({ error: 'label is required' });
  }
  try {
    const created = await addKey(label);
    const host    = req.get('host') || 'yourserver';
    res.status(201).json({
      streamKey: created.streamKey,
      label:     created.label,
      rtmpUrl:   `rtmp://${host.split(':')[0]}:${RTMP_PORT}/live/${created.streamKey}`,
      hlsUrl:    liveMasterUrl(created.streamKey),
      vodUrl:    vodMasterUrl(created.streamKey),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * DELETE /api/admin/streamkey/:streamKey
 * Deactivates a key (sets active: false). Past VODs remain accessible.
 */
router.delete('/admin/streamkey/:streamKey', adminGuard, async (req, res) => {
  const { streamKey } = req.params;
  const ok = await deactivateKey(streamKey);
  if (!ok) return res.status(404).json({ error: 'unknown stream key' });
  res.json({ streamKey, active: false });
});

/**
 * Internal sanity endpoint (handy when wiring up encoders / firewalls).
 */
router.get('/health', async (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

export default router;
