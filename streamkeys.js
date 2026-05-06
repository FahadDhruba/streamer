// streamkeys.js
// Loads, saves, and validates stream keys from streamkeys.json.
// File format:
//   {
//     "sk_a1b2c3d4": { "label": "ChannelName", "active": true, "createdAt": "2026-01-01T..." },
//     ...
//   }

import { promises as fs } from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const KEYS_PATH = path.join(__dirname, 'streamkeys.json');

// Serialize all writes so concurrent calls don't clobber the file.
let writeChain = Promise.resolve();

/**
 * Load the streamkeys.json file. Returns an empty object if the file
 * does not exist or is malformed.
 */
export async function loadKeys() {
  try {
    const raw = await fs.readFile(KEYS_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch (err) {
    if (err.code === 'ENOENT') return {};
    console.error('[streamkeys] Failed to parse streamkeys.json:', err.message);
    return {};
  }
}

/**
 * Persist the keys map atomically (write to tmp file then rename).
 */
export async function saveKeys(keys) {
  writeChain = writeChain.then(async () => {
    const tmp = `${KEYS_PATH}.tmp`;
    const data = JSON.stringify(keys, null, 2);
    await fs.writeFile(tmp, data, 'utf8');
    await fs.rename(tmp, KEYS_PATH);
  });
  return writeChain;
}

/**
 * Generate a fresh, URL-safe stream key.
 * Format: sk_<8 hex chars>  (e.g. sk_a1b2c3d4)
 */
export function generateKey() {
  return 'sk_' + crypto.randomBytes(4).toString('hex');
}

/**
 * Add a new key to streamkeys.json.
 * Returns the freshly created key entry { streamKey, label, active }.
 */
export async function addKey(label) {
  if (!label || typeof label !== 'string') {
    throw new Error('label is required and must be a string');
  }
  const keys = await loadKeys();

  // Loop until we find an unused random key (collisions are astronomically unlikely).
  let streamKey;
  do {
    streamKey = generateKey();
  } while (keys[streamKey]);

  keys[streamKey] = {
    label: label.trim(),
    active: true,
    createdAt: new Date().toISOString(),
  };
  await saveKeys(keys);
  return { streamKey, ...keys[streamKey] };
}

/**
 * Soft-deactivate a stream key (sets active: false). Does NOT remove it
 * so historic VOD URLs keep resolving.
 */
export async function deactivateKey(streamKey) {
  const keys = await loadKeys();
  if (!keys[streamKey]) return false;
  keys[streamKey].active = false;
  keys[streamKey].deactivatedAt = new Date().toISOString();
  await saveKeys(keys);
  return true;
}

/**
 * Stream-key syntax check. Allows letters, digits, dashes, underscores, 4-64 chars.
 * Used by both validateKey and autoRegisterKey to keep nonsense (`../`, spaces,
 * unicode) out of the keystore.
 */
export function isWellFormedKey(streamKey) {
  return typeof streamKey === 'string'
      && streamKey.length >= 1
      && streamKey.length <= 64
      && /^[A-Za-z0-9_-]+$/.test(streamKey);
}

/**
 * Validate a stream key. Returns the key entry if valid & active, otherwise null.
 */
export async function validateKey(streamKey) {
  if (!isWellFormedKey(streamKey)) return null;
  const keys = await loadKeys();
  const entry = keys[streamKey];
  if (!entry) return null;
  if (!entry.active) return null;
  return { streamKey, ...entry };
}

/**
 * "Open mode" entry point — used when OPEN_MODE=true.
 *
 * - If the key already exists & is active, returns it (same as validateKey).
 * - If the key exists but is inactive, re-activates it and returns the entry.
 * - If the key has never been seen, registers it with a default label and
 *   returns the new entry.
 *
 * Returns null only when the key string itself is malformed (empty, too long,
 * illegal characters).
 */
export async function autoRegisterKey(streamKey) {
  if (!isWellFormedKey(streamKey)) return null;

  const keys = await loadKeys();
  let entry  = keys[streamKey];

  if (!entry) {
    entry = {
      label:     streamKey,
      active:    true,
      auto:      true,
      createdAt: new Date().toISOString(),
    };
    keys[streamKey] = entry;
    await saveKeys(keys);
    console.log(`[streamkeys] auto-registered "${streamKey}" (open mode)`);
  } else if (!entry.active) {
    entry.active = true;
    entry.reactivatedAt = new Date().toISOString();
    delete entry.deactivatedAt;
    await saveKeys(keys);
    console.log(`[streamkeys] auto-reactivated "${streamKey}" (open mode)`);
  }

  return { streamKey, ...entry };
}

/**
 * Return the human-readable label for a key, or the key itself if not found.
 */
export async function getLabel(streamKey) {
  const keys = await loadKeys();
  return keys[streamKey]?.label || streamKey;
}
