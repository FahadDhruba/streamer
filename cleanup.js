// cleanup.js
// Local-disk hygiene helpers. Every delete is logged with a timestamp so
// operators can audit disk-usage behavior.

import { promises as fs } from 'node:fs';
import path from 'node:path';

/**
 * Delete a single file. Logs the operation. Swallows ENOENT so we don't
 * crash when chokidar fires `add` and we race against ffmpeg's own
 * `delete_segments` rotation.
 */
export async function deleteFile(filePath) {
  try {
    await fs.unlink(filePath);
    console.log(`[cleanup ${new Date().toISOString()}] deleted file: ${filePath}`);
    return true;
  } catch (err) {
    if (err.code === 'ENOENT') return false;
    console.warn(`[cleanup ${new Date().toISOString()}] failed to delete ${filePath}: ${err.message}`);
    return false;
  }
}

/**
 * Recursively delete a directory and all its contents.
 */
export async function deleteDir(dirPath) {
  try {
    await fs.rm(dirPath, { recursive: true, force: true });
    console.log(`[cleanup ${new Date().toISOString()}] deleted dir : ${dirPath}`);
    return true;
  } catch (err) {
    console.warn(`[cleanup ${new Date().toISOString()}] failed to delete dir ${dirPath}: ${err.message}`);
    return false;
  }
}

/**
 * Wipe the entire `tmp/live/` tree. Called once at server startup so any
 * orphaned segments left behind by a previous crash don't pile up.
 */
export async function purgeTmpRoot(tmpRoot) {
  try {
    const entries = await fs.readdir(tmpRoot).catch(() => []);
    if (!entries.length) {
      await fs.mkdir(tmpRoot, { recursive: true });
      return;
    }
    console.log(`[cleanup ${new Date().toISOString()}] purging stale entries in ${tmpRoot}: ${entries.join(', ')}`);
    for (const entry of entries) {
      await deleteDir(path.join(tmpRoot, entry));
    }
    await fs.mkdir(tmpRoot, { recursive: true });
  } catch (err) {
    console.warn(`[cleanup] purgeTmpRoot failed: ${err.message}`);
  }
}

/**
 * Ensure a directory exists.
 */
export async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}
