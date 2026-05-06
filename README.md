# Live Streaming Server

A production-ready Node.js + Express live streaming server.

- **Ingest:** RTMP (OBS, Streamlabs, ffmpeg, any RTMP encoder)
- **Transcode:** FFmpeg → adaptive bitrate HLS (1080p / 720p / 480p)
- **Storage:** Cloudflare R2 (S3-compatible)
- **Disk usage:** Near-zero — `.ts` segments are deleted locally as soon as they hit R2
- **VOD:** Stream end → 1080p MP4 recording + HLS VOD playlist on R2
- **Scale:** Designed for 1–200 concurrent streams on a single host
- **Players:** HLS.js (browser), VLC, Safari, ffplay, anything HLS-compatible
- **Auth:** Open mode by default — *any* stream key works (auto-registered on first use); flip `OPEN_MODE=false` for a strict allow-list in production

---

## 1. Prerequisites

### Node.js 18+

```bash
node --version   # must be >= 18
```

### FFmpeg

The server spawns the system `ffmpeg` binary. Install it first.

| OS | Command |
|---|---|
| Ubuntu / Debian | `sudo apt update && sudo apt install -y ffmpeg` |
| RHEL / CentOS / Rocky | `sudo dnf install -y ffmpeg` (enable RPM Fusion) |
| macOS (Homebrew) | `brew install ffmpeg` |
| Windows | Download from [ffmpeg.org](https://ffmpeg.org/download.html) and add to `PATH` |

Verify:

```bash
ffmpeg -version
```

Then point the server at the binary in `.env` (`FFMPEG_PATH=/usr/bin/ffmpeg`, etc.).

---

## 2. Cloudflare R2 setup

### 2.1 Create a bucket

1. Cloudflare Dashboard → **R2** → **Create bucket**.
2. Pick a name (e.g. `streaming-prod`) and an automatic location.

### 2.2 Make it publicly readable

We rely on direct public reads for the HLS playlists / segments.

1. Open the bucket → **Settings** → **Public access**.
2. Either enable the **R2.dev subdomain** (fast for dev) or attach a custom domain (recommended for prod, e.g. `https://cdn.example.com`).
3. Copy that public URL into `R2_PUBLIC_URL` in `.env` (no trailing slash).

### 2.3 Create an API token

1. R2 dashboard → **Manage R2 API Tokens** → **Create API token**.
2. Permission: **Object Read & Write** for the bucket above.
3. Copy `Account ID`, `Access Key ID`, `Secret Access Key` into `.env`.

### 2.4 CORS (so HLS.js in the browser can fetch segments)

Bucket → **Settings** → **CORS Policy** → paste:

```json
[
  {
    "AllowedOrigins": ["*"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedHeaders": ["*"],
    "ExposeHeaders": ["ETag", "Content-Length", "Content-Type"],
    "MaxAgeSeconds": 86400
  }
]
```

For production you can lock `AllowedOrigins` to your specific player domain(s).

### 2.5 (Recommended) Lifecycle rule for safety

Bucket → **Settings** → **Object lifecycle rules** → **Add rule**:

- Prefix: `live/`
- Action: **Delete** after **1 day**

This is a safety net; the application also rotates live segments aggressively while a stream is running.

---

## 3. Install & run

```bash
# 1. Install deps
npm install

# 2. Configure environment
cp .env.example .env
# then edit .env — fill in R2_* and FFMPEG_PATH

# 3. Start the server
npm start
```

That's it. With `OPEN_MODE=true` (the default) you can immediately broadcast to **any** stream key — for example `1234`, `mychan`, or `sk_anything`. The server auto-registers the key on first use.

Flip to a strict allow-list when you're ready:

```bash
# .env
OPEN_MODE=false

# then create explicit keys (label is optional — defaults to a generated one)
npm run addkey
npm run addkey -- "MyChannel"
```

You should see:

```
[server] HTTP API listening on :3000
[rtmp]   node-media-server listening on rtmp://0.0.0.0:1935
```

---

## 4. Broadcasting from OBS

Settings → **Stream**:

- **Service:** Custom...
- **Server:** `rtmp://<your-server-ip>:1935/live`
- **Stream Key:** anything you like in open mode (e.g. `1234`, `mychan`); or the value printed by `npm run addkey` in strict mode

Settings → **Output** (recommended):

- **Output Mode:** Advanced
- **Encoder:** x264 (or hardware: NVENC / QuickSync / AMF)
- **Bitrate:** 4500–6000 kbps
- **Keyframe Interval:** 2 seconds
- **CPU Usage Preset:** veryfast (if x264)
- **Profile:** main
- **Tune:** zerolatency

Settings → **Video**:

- **Output (Scaled) Resolution:** 1920x1080
- **FPS:** 30 or 60

Click **Start Streaming**.

You can verify ingest with:

```bash
curl http://<your-server>:3000/api/streams
```

---

## 5. Watching in VLC

VLC → **Media** → **Open Network Stream** → paste the master playlist URL printed at startup, e.g.:

```
https://<R2_PUBLIC_URL>/live/sk_xxxxxxxx/master.m3u8
```

Or use the convenience proxy (which 302-redirects to R2):

```
http://<your-server>:3000/api/stream/sk_xxxxxxxx/master.m3u8
```

---

## 6. Watching with HLS.js (browser)

Minimal HTML, drop-in:

```html
<!doctype html>
<html>
  <head><meta charset="utf-8"><title>Live</title></head>
  <body style="margin:0;background:#000">
    <video id="v" controls autoplay muted style="width:100%;height:100vh"></video>
    <script src="https://cdn.jsdelivr.net/npm/hls.js@1"></script>
    <script>
      const video = document.getElementById('v');
      const url   = 'https://<R2_PUBLIC_URL>/live/sk_xxxxxxxx/master.m3u8';

      if (Hls.isSupported()) {
        const hls = new Hls({ lowLatencyMode: true });
        hls.loadSource(url);
        hls.attachMedia(video);
      } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
        // Safari has native HLS.
        video.src = url;
      }
    </script>
  </body>
</html>
```

---

## 7. Stream-key management

In **open mode** (default) you don't need to run any of this — just stream. Keys are added to `streamkeys.json` automatically the first time they're used. The commands below are for **strict mode** (`OPEN_MODE=false`):

```bash
# Create a new key (label optional)
npm run addkey
npm run addkey -- "MyChannel"

# List keys (read the file directly)
cat streamkeys.json

# Deactivate via API
curl -X DELETE http://localhost:3000/api/admin/streamkey/sk_xxxxxxxx
# (with admin auth)
curl -X DELETE \
     -H "X-Admin-Token: $ADMIN_TOKEN" \
     http://localhost:3000/api/admin/streamkey/sk_xxxxxxxx

# Create via API
curl -X POST -H "Content-Type: application/json" \
     -H "X-Admin-Token: $ADMIN_TOKEN" \
     -d '{"label":"MyChannel"}' \
     http://localhost:3000/api/admin/streamkey
```

---

## 8. HTTP API

| Method | Path | Description |
|---|---|---|
| `GET`    | `/api/health` | Liveness probe. |
| `GET`    | `/api/streams` | List currently active streams. |
| `GET`    | `/api/stream/:streamKey/status` | Live + VOD status & URLs for one key. |
| `GET`    | `/api/stream/:streamKey/master.m3u8` | 302 → R2 master playlist. Useful as the URL you give to HLS.js. |
| `POST`   | `/api/admin/streamkey` | Body `{ "label": "..." }`. Creates a new key. |
| `DELETE` | `/api/admin/streamkey/:streamKey` | Deactivates a key. |

Set `ADMIN_TOKEN=...` in `.env` to require an `X-Admin-Token` header on the admin routes.

---

## 9. R2 layout

```
live/<streamKey>/master.m3u8           # adaptive bitrate manifest (R2 absolute URLs)
live/<streamKey>/1080p/index.m3u8      # rolling live playlist (last 6 segments)
live/<streamKey>/1080p/seg00001.ts     # segments (preserved for VOD)
live/<streamKey>/720p/...              # rotated: only ~8 newest segments retained
live/<streamKey>/480p/...              # rotated: only ~8 newest segments retained

vod/<streamKey>/master.m3u8            # HLS VOD master
vod/<streamKey>/1080p/index.m3u8       # full VOD media playlist (#EXT-X-PLAYLIST-TYPE:VOD)
vod/<streamKey>/1080p/seg*.ts          # 1080p segments (server-side copied from live/)
vod/<streamKey>/recording.mp4          # single-file MP4 download (no re-encode)
```

After VOD generation completes, `live/<streamKey>/*` is wiped from R2.

---

## 10. Performance & cost notes

- FFmpeg uses preset `veryfast` to balance CPU vs. bitrate efficiency.
- 4-second segments → ~4× fewer R2 PUT requests than 2-second segments.
- Local `.ts` files are **deleted immediately** after each successful R2 upload.
- Local disk usage per stream rarely exceeds ~30s of segments (≈30 MB).
- Only **1080p** segments survive the post-stream cleanup (used for VOD); 720p and 480p rotate live and are wiped at the end.
- Live segment rotation in R2 keeps at most ~8 segments per non-source quality.
- The R2 lifecycle rule (`live/` → 1 day) is a safety net for crashed processes.
- Server process never exits when one stream's ffmpeg crashes — the failure is contained per-stream.

---

## 11. Folder structure

```
.
├── server.js              Express + lifecycle entry point
├── rtmp.js                node-media-server config & session handlers
├── transcoder.js          FFmpeg ABR pipeline + master.m3u8 builder
├── uploader.js            R2 client + chokidar watcher + retry queue
├── vod.js                 Post-stream MP4 concat + VOD playlist generator
├── streamkeys.js          Stream-key CRUD/validation
├── cleanup.js             Local disk-hygiene helpers
├── routes/
│   └── api.js             HTTP API
├── scripts/
│   └── addkey.js          CLI: npm run addkey -- "ChannelName"
├── streamkeys.json        Persisted key store
├── .env.example
├── package.json
└── README.md
```

---

## 12. Troubleshooting

- **OBS says “Failed to connect to server.”** → Check that port `1935/tcp` is open and that `npm start` shows `node-media-server listening on rtmp://0.0.0.0:1935`.
- **Stream is rejected immediately.** → The key isn't in `streamkeys.json`, is `active: false`, or the same key is already broadcasting from another encoder. Run `npm run addkey -- "name"` and try again.
- **HLS.js logs CORS errors.** → Re-paste the CORS JSON from §2.4.
- **VLC plays for a few seconds and stops.** → Make sure your encoder’s keyframe interval is **2 seconds** and that segments are reaching R2 (`curl <R2_PUBLIC_URL>/live/<streamKey>/1080p/index.m3u8`).
- **`spawn ffmpeg ENOENT`** → `FFMPEG_PATH` in `.env` is wrong. Run `which ffmpeg` (macOS/Linux) or `where ffmpeg` (Windows) and use that absolute path.
