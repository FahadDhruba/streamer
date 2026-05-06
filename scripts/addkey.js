#!/usr/bin/env node
// scripts/addkey.js
// CLI for generating a new stream key.
//
// Usage:
//   npm run addkey -- "MyChannel"
//   node scripts/addkey.js "MyChannel"

import 'dotenv/config';
import { addKey } from '../streamkeys.js';

const RTMP_PORT = parseInt(process.env.RTMP_PORT || '1935', 10);
const PUBLIC_URL = (process.env.R2_PUBLIC_URL || 'https://your-r2-public-url.com').replace(/\/+$/, '');

async function main() {
  // Label is optional. If omitted, we mint a default like "Channel-1714…"
  // so `npm run addkey` works with zero arguments.
  let label = process.argv.slice(2).join(' ').trim();
  if (!label) {
    label = `Channel-${Date.now().toString(36)}`;
    console.log(`[addkey] no label given; using "${label}"`);
  }

  const created = await addKey(label);

  const host = process.env.PUBLIC_HOST || 'yourserver.com';

  console.log('');
  console.log('   New stream key created');
  console.log('   ----------------------');
  console.log(`   label     : ${created.label}`);
  console.log(`   streamKey : ${created.streamKey}`);
  console.log('');
  console.log('   OBS / encoder settings');
  console.log(`     Server      : rtmp://${host}:${RTMP_PORT}/live`);
  console.log(`     Stream Key  : ${created.streamKey}`);
  console.log('');
  console.log('   Viewer URLs');
  console.log(`     HLS (live)  : ${PUBLIC_URL}/live/${created.streamKey}/master.m3u8`);
  console.log(`     HLS (vod)   : ${PUBLIC_URL}/vod/${created.streamKey}/master.m3u8`);
  console.log(`     MP4 download: ${PUBLIC_URL}/vod/${created.streamKey}/recording.mp4`);
  console.log('');
}

main().catch((err) => {
  console.error('[addkey] error:', err.message);
  process.exit(1);
});
