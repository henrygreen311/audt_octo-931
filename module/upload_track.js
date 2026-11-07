#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const { createClient } = require('@supabase/supabase-js');
const { File } = require('node:buffer');

// --- Safely resolve SDK ---
let sdkPkg;
try {
  sdkPkg = require('@audius/sdk');
} catch (e) {
  console.error('[!] Failed to require @audius/sdk:', e.message);
  process.exit(1);
}

let sdkFactory;
if (typeof sdkPkg === 'function') sdkFactory = sdkPkg;
else if (typeof sdkPkg.sdk === 'function') sdkFactory = sdkPkg.sdk;
else if (typeof sdkPkg.default === 'function') sdkFactory = sdkPkg.default;
else if (sdkPkg.default && typeof sdkPkg.default.sdk === 'function') sdkFactory = sdkPkg.default.sdk;
else sdkFactory = Object.values(sdkPkg).find(v => typeof v === 'function' && v.name.toLowerCase().includes('sdk'));

if (!sdkFactory) {
  console.error('[!] Could not find SDK factory function in @audius/sdk exports.');
  process.exit(1);
}

// --- Silence SDK noise ---
const log = console.log.bind(console);
for (const level of ['log', 'info', 'debug', 'warn']) {
  const orig = console[level].bind(console);
  console[level] = (...args) => {
    if (typeof args[0] === 'string' && args[0].startsWith('[audius-sdk]')) return;
    orig(...args);
  };
}

const ROOT_DIR = path.resolve(__dirname, '..');
const DB_FILE = path.join(ROOT_DIR, 'db.txt');
const MUSIC_DIR = path.join(ROOT_DIR, 'music');
const IMAGES_DIR = path.join(MUSIC_DIR, 'images');

const TARGET_ADDRESS = "qringgreen@gmail.com";

// Helper: Find the most recently modified file with a specific extension in a directory
function getNewestFile(dir, ext) {
  if (!fs.existsSync(dir)) return null;
  const files = fs.readdirSync(dir)
    .filter(f => f.endsWith(ext))
    .map(f => ({ name: f, time: fs.statSync(path.join(dir, f)).mtime.getTime() }))
    .sort((a, b) => b.time - a.time); // Sort descending (newest first)
  return files.length > 0 ? path.join(dir, files[0].name) : null;
}

async function main() {
  if (!fs.existsSync(DB_FILE)) {
    console.error(`[!] db.txt not found at: ${DB_FILE}`);
    process.exit(1);
  }

  // 1. Load Supabase Credentials
  const dbText = fs.readFileSync(DB_FILE, 'utf8');
  const urlMatch = dbText.match(/SUPABASE_URL\s*=\s*["']([^"']+)["']/);
  const keyMatch = dbText.match(/SUPABASE_KEY\s*=\s*["']([^"']+)["']/);
  if (!urlMatch || !keyMatch) {
    console.error('[!] Could not read SUPABASE_URL / SUPABASE_KEY from db.txt');
    process.exit(1);
  }
  const supabase = createClient(urlMatch[1], keyMatch[1]);

  // 2. Fetch target user details
  log(`[*] Fetching profile data for ${TARGET_ADDRESS}...`);
  const { data: account, error } = await supabase
    .from('audius')
    .select('user_id, app_data')
    .eq('address', TARGET_ADDRESS)
    .single();

  if (error || !account) {
    console.error(`[!] Failed to fetch account data: ${error?.message || 'Account not found'}`);
    process.exit(1);
  }
  if (!account.user_id || !account.app_data || !account.app_data.api_key) {
    console.error('[!] Missing required Audius credentials or user_id for this account.');
    process.exit(1);
  }

  // 3. Generate track using Python script
  log(`\n[*] Triggering music generation (max_tracks=1)...`);
  try {
    // Run python script from root directory, forcing MAX_TRACKS=1
    execSync('MAX_TRACKS=1 python3 music/create_track.py', { 
      cwd: ROOT_DIR, 
      stdio: 'inherit',
      env: { ...process.env, MAX_TRACKS: '1' }
    });
  } catch (e) {
    console.error(`[!] Python script failed: ${e.message}`);
    process.exit(1);
  }

  // 4. Locate generated files
  log(`\n[*] Locating generated files...`);
  const newestMp3 = getNewestFile(MUSIC_DIR, '.mp3');
  const newestJpg = getNewestFile(IMAGES_DIR, '.jpg');

  if (!newestMp3) {
    console.error('[!] No .mp3 file found in /music folder. Generation may have failed.');
    process.exit(1);
  }
  if (!newestJpg) {
    console.error('[!] No .jpg file found in /music/images folder. Cover generation may have failed.');
    process.exit(1);
  }

  const audioFilename = path.basename(newestMp3);
  const coverFilename = path.basename(newestJpg);
  
  // Extract title from filename (e.g., "my-song-title.mp3" -> "My Song Title")
  const rawTitle = audioFilename.replace('.mp3', '').replace(/-/g, ' ');
  const trackTitle = rawTitle.replace(/\b\w/g, c => c.toUpperCase()); 

  log(`[+] Found Audio: ${audioFilename}`);
  log(`[+] Found Cover: ${coverFilename}`);
  log(`[+] Derived Title: "${trackTitle}"`);

  // 5. Upload to Audius
  log(`\n[*] Initializing Audius SDK...`);
  try {
    const sdk = sdkFactory({
      apiKey: account.app_data.api_key,
      apiSecret: account.app_data.api_secret,
      bearerToken: account.app_data.bearer_token,
    });

    const audioBuffer = fs.readFileSync(newestMp3);
    const coverBuffer = fs.readFileSync(newestJpg);

    const audioFile = new File([audioBuffer], audioFilename, { type: 'audio/mpeg' });
    const coverFile = new File([coverBuffer], coverFilename, { type: 'image/jpeg' });

    log(`[*] Uploading track to Audius... (This may take a minute)`);
    const trackResponse = await sdk.tracks.createTrack({
      userId: account.user_id,
      audioFile: audioFile,
      coverArtFile: coverFile,
      metadata: {
        title: trackTitle,
        genre: 'Electronic',
        description: 'AI-generated track.',
        isStreamGated: false,
        isDownloadGated: false,
        isUnlisted: false,
      }
    });

    // Handle different SDK response formats
    const trackId = trackResponse?.data?.id || trackResponse?.trackId || trackResponse?.id || 'Unknown ID';
    log(`\n[+] SUCCESS! Track uploaded successfully.`);
    log(`    Track ID: ${trackId}`);
    log(`    Audius URL: https://audius.co${account.app_data.handle ? '/' + account.app_data.handle : ''}/${trackTitle.toLowerCase().replace(/\s+/g, '-')}`);
    
  } catch (err) {
    console.error(`\n[!] Audius upload failed:`, err.message || err);
  }
}

main().catch(err => {
  console.error('[!] Fatal error:', err.message || err);
  process.exit(1);
});
