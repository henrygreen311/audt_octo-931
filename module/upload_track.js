#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');
const { execSync } = require('child_process');
const { createClient } = require('@supabase/supabase-js');

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
// Local crash-recovery journal (not a database object).
const STATE_FILE = path.join(ROOT_DIR, '.audius_daily_state.json');

const CFG = {
  timezone: process.env.PUBLISH_TIMEZONE || 'UTC',
  accountsPerDay: 20,
  tracksPerDay: 3,
  priorityHandles: ['qring', 'tradex', 'obi350965'],
  priorityPerWeek: 3,
  lockTtlMs: Number(process.env.LOCK_TTL_MINUTES || 120) * 60000,
  // A lock of unknown ownership with no data recorded for today is treated as stale after this long (0 = off).
  lockIdleMs: Number(process.env.LOCK_IDLE_MINUTES ?? 10) * 60000,
  genTimeoutMs: Number(process.env.GEN_TIMEOUT_MINUTES || 60) * 60000,
  genTopUpAttempts: Number(process.env.GEN_TOPUP_ATTEMPTS ?? 1),
  retries: Number(process.env.RETRY_ATTEMPTS || 3),
  retryBaseMs: Number(process.env.RETRY_BASE_MS || 2000),
  verifyTries: 5,
  verifyDelayMs: Number(process.env.VERIFY_DELAY_MS || 3000),
  uploadDelayMs: Number(process.env.UPLOAD_DELAY_MS || 1500),
};

const ALREADY_DONE_MSG = "[!] Today's publishing batch has already been completed. Exiting.";

// ---------------------------------------------------------------------------
// Description generator (unchanged from the working implementation)
// ---------------------------------------------------------------------------
const DESC = {
  opener: [
    "A late-night journey called", "Meet", "Introducing", "Say hello to", "Press play on", "Dive into", "Step inside",
    "Turn it up for", "Hit play and get lost in", "Fresh out of the studio:", "Straight from the lab,", "Something new:",
    "Headphones on for", "Pure energy in", "An experiment in sound,", "Brand new vibes with", "Feel the pulse of",
    "Welcome to", "Get ready for", "Enter the world of", "Out now:", "Listen closely to", "Crafted for the night:"
  ],
  genre: [
    "electronic", "synth-driven", "bass-heavy", "melodic", "ambient", "downtempo", "hyperpop", "house", "techno", "lo-fi",
    "chillwave", "synthwave", "future-bass", "drum and bass", "breakbeat", "trap-inspired", "electro", "indie electronic",
    "deep house", "progressive", "trip-hop", "glitchy", "cinematic", "retro-futuristic", "garage", "dance",
    "experimental", "club-ready", "minimal", "ethereal"
  ],
  vibe: [
    "late-night", "dreamy", "moody", "hypnotic", "nostalgic", "euphoric", "melancholic", "energetic", "atmospheric", "driving",
    "dark and smooth", "warm", "cold and crisp", "raw", "glossy", "gritty", "floating", "restless", "peaceful", "intense",
    "bittersweet", "spacey", "playful", "cinematic", "futuristic", "rain-soaked", "neon-lit", "sun-drenched", "introspective", "wild"
  ],
  element: [
    "pulsing synths", "deep sub-bass", "crisp drums", "shimmering pads", "chopped vocal samples", "warm analog textures",
    "razor-sharp hi-hats", "rolling basslines", "glitchy percussion", "lush chords", "tape-saturated drums", "ringing arpeggios",
    "layered harmonies", "distorted 808s", "soft piano", "vintage keys", "echoing vocals", "punchy kicks", "airy atmospheres",
    "swirling reverbs", "detuned leads", "sidechained pads", "vinyl crackle", "filtered breakbeats", "wide stereo textures",
    "stuttering vocal cuts", "slow-building risers", "warped bass hits", "metallic percussion", "gentle guitar plucks"
  ],
  scene: [
    "a midnight drive", "a rooftop at sunrise", "an empty dance floor", "a long train ride", "a rainy city window", "the last hour of a party",
    "a quiet study session", "a neon-lit street", "an early morning run", "a road trip at dusk", "a dimly lit studio", "a solo walk home",
    "a summer night", "a deep focus session", "a crowded club", "a lonely highway", "the edge of a dream", "a slow Sunday",
    "a late-night coding session", "a sleepless city", "a beach at midnight", "a workout that won't quit", "a foggy morning", "a space-station window"
  ],
  feeling: [
    "wonder", "nostalgia", "freedom", "tension", "calm", "adrenaline", "longing", "hope", "mystery", "momentum", "release",
    "focus", "melancholy", "joy", "restlessness", "confidence", "serenity", "chaos", "warmth", "curiosity", "power", "escape"
  ],
  closer: [
    "Best enjoyed on good headphones.", "Turn it up.", "Let it loop.", "Press repeat.", "Play it loud.", "More music coming soon.",
    "Thanks for listening.", "Hope it moves you.", "Feel free to share.", "Made for late nights.", "Stay tuned for more.",
    "Hit repeat and drift away.", "Best played at full volume.", "Let me know what you think.", "Enjoy the ride.", "Close your eyes and listen.",
    "Add it to your playlist.", "Perfect for headphones and night drives.", "Take it in from start to finish.", "Fresh sounds, more on the way."
  ],
  action: [
    "blends", "stacks", "balances", "weaves together", "plays with", "leans on", "builds around", "mixes",
    "pairs", "fuses", "pushes forward with", "drifts through", "layers up", "plays off"
  ]
};

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

// Builds a 2-3 sentence description that uses the track title and its words.
// Optional extras (from the audio analysis) are folded in when available.
function generateDescription(title, extras = {}) {
  const words = title.split(/\s+/).filter(w => w.length > 2);
  const keyWord = words.length ? pick(words).toLowerCase() : title.toLowerCase();
  const T = `"${title}"`;

  const templates = [
    () => `${pick(DESC.opener)} ${T}: ${an(pick(DESC.vibe))} ${pick(DESC.genre)} track with ${pick(DESC.element)} and ${pick(DESC.element)}. ${pick(DESC.closer)}`,
    () => `${T} is ${an(pick(DESC.vibe))} ${pick(DESC.genre)} track built for ${pick(DESC.scene)}. It ${pick(DESC.action)} ${pick(DESC.element)} and ${pick(DESC.element)}. ${pick(DESC.closer)}`,
    () => {
      const [v1, v2] = [...DESC.vibe].sort(() => Math.random() - 0.5);
      return `Think ${keyWord}, ${v1} and ${v2}. ${T} ${pick(DESC.action)} ${pick(DESC.element)}, leaving a feeling of ${pick(DESC.feeling)}. ${pick(DESC.closer)}`;
    },
    () => `${pick(DESC.opener)} ${T}. ${cap(an(pick(DESC.genre)))} track with ${pick(DESC.element)}, made for ${pick(DESC.scene)}. ${pick(DESC.closer)}`,
    () => `${T}: ${pick(DESC.feeling)}, ${pick(DESC.feeling)} and a little ${pick(DESC.feeling)}. ${cap(an(pick(DESC.vibe)))} ${pick(DESC.genre)} ride through ${pick(DESC.element)} and ${pick(DESC.element)}. ${pick(DESC.closer)}`,
    () => `If ${keyWord} had a sound, it might be ${T}. ${cap(an(pick(DESC.vibe)))} ${pick(DESC.genre)} track with ${pick(DESC.element)} and ${pick(DESC.element)}, perfect for ${pick(DESC.scene)}. ${pick(DESC.closer)}`,
    () => `${pick(DESC.opener)} ${T}. It ${pick(DESC.action)} ${pick(DESC.element)} until the ${keyWord} takes over. ${cap(pick(DESC.feeling))} from the first beat to the last. ${pick(DESC.closer)}`,
  ];

  let text = pick(templates)();

  const facts = [];
  if (extras.bpm) facts.push(`${Math.round(extras.bpm)} BPM`);
  if (extras.musicalKey) facts.push(extras.musicalKey);
  if (facts.length) text += ` ${facts.join(', ')}.`;

  return text;
}

function an(word) {
  return (/^[aeiou]/i.test(word) ? 'an ' : 'a ') + word;
}

function cap(str) {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

// Description that is unique within the batch (the audio analysis extras are reused).
function uniqueDescription(title, extras, used) {
  let d = generateDescription(title, extras);
  for (let i = 0; i < 30 && used.has(d); i++) d = generateDescription(title, extras);
  while (used.has(d)) d += ` ${pick(DESC.closer)}`;
  used.add(d);
  return d;
}

// ---------------------------------------------------------------------------
// Title generator for the 17 non-original accounts. The vocabulary matches the
// genre/mood published by createTrack (Electronic / Energizing), and half of
// the titles borrow a word from the track's own generated title so they stay
// thematically tied to the actual audio.
// ---------------------------------------------------------------------------
const TITLE = {
  adj: ["Midnight", "Neon", "Velvet", "Electric", "Hollow", "Golden", "Silent", "Crimson", "Fading", "Distant", "Burning",
    "Frozen", "Static", "Lunar", "Chrome", "Wandering", "Restless", "Glass", "Endless", "Silver", "Wild", "Slow", "Radiant",
    "Broken", "Violet", "Phantom", "Cosmic", "Liquid", "Amber", "Northern", "Digital", "Hidden", "Paper", "Shadow", "Solar"],
  noun: ["Skyline", "Echoes", "Horizon", "Signal", "Afterglow", "Pulse", "Circuit", "Mirage", "Gravity", "Voltage", "Embers",
    "Tides", "Orbit", "Daydream", "Highway", "Satellite", "Reflections", "Currents", "Silhouette", "Rhythm", "Lights",
    "Memories", "Fever", "Atlas", "Drift", "Runway", "Canyon", "Sunrise", "Thunder", "Compass", "Frequency", "Skyway"],
  tail: ["After Dark", "Tonight", "at Midnight", "in Slow Motion", "Till Sunrise", "in the Rain", "Come Alive", "Never Sleeps",
    "Calling", "on Repeat", "Forever", "Rising"],
};

function generateTitle(originalTitle, usedLower) {
  const keys = originalTitle.split(/\s+/).filter(w => w.replace(/[^a-z]/gi, '').length >= 4);
  const key = keys.length ? cap(pick(keys).toLowerCase()) : null;
  const makers = [
    () => `${pick(TITLE.adj)} ${pick(TITLE.noun)}`,
    () => `${pick(TITLE.noun)} ${pick(TITLE.tail)}`,
    () => `${pick(TITLE.noun)} of ${pick(TITLE.adj)} ${pick(TITLE.noun)}`,
    () => `${pick(TITLE.adj)} ${pick(TITLE.noun)} ${pick(TITLE.tail)}`,
  ];
  if (key) makers.push(() => `${key} ${pick(TITLE.noun)}`, () => `${pick(TITLE.adj)} ${key}`, () => `${key} ${pick(TITLE.tail)}`);

  let title;
  for (let i = 0; i < 80; i++) {
    title = pick(makers)();
    if (!usedLower.has(title.toLowerCase())) break;
    title = null;
  }
  if (!title) {
    do { title = `${pick(TITLE.adj)} ${pick(TITLE.noun)} ${Math.floor(Math.random() * 90) + 10}`; }
    while (usedLower.has(title.toLowerCase()));
  }
  usedLower.add(title.toLowerCase());
  return title;
}

// ---------------------------------------------------------------------------
// Generic helpers
// ---------------------------------------------------------------------------
const sleep = ms => new Promise(r => setTimeout(r, ms));
const isObj = v => !!v && typeof v === 'object' && !Array.isArray(v);

function shuffle(arr, rnd = Math.random) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rnd() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function hashSeed(str) {
  let h = 2166136261 >>> 0;
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 16777619) >>> 0; }
  return h;
}

function mulberry32(a) {
  return function () {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function isTransient(err) {
  const status = err && ((err.response && err.response.status) || err.status);
  if (status) return status === 408 || status === 429 || status >= 500;
  const msg = String((err && err.message) || err).toLowerCase();
  const code = String((err && (err.code || (err.cause && err.cause.code))) || '').toUpperCase();
  return /ECONNRESET|ETIMEDOUT|ECONNREFUSED|ENOTFOUND|EAI_AGAIN|EPIPE|UND_ERR/.test(code) ||
    /fetch failed|network|timeout|timed out|socket hang up|econnreset|temporar|rate limit|too many requests|bad gateway|service unavailable|gateway/.test(msg);
}

async function withRetry(label, fn, { tries = CFG.retries, retryIf = isTransient } = {}) {
  let lastErr;
  for (let i = 1; i <= tries; i++) {
    try {
      return await fn(i);
    } catch (e) {
      lastErr = e;
      if (i === tries || !retryIf(e)) break;
      const wait = CFG.retryBaseMs * 2 ** (i - 1);
      log(`[~] ${label} failed (${e.message || e}); retry ${i}/${tries - 1} in ${wait}ms`);
      await sleep(wait);
    }
  }
  throw lastErr;
}

// Reads the body of an SDK error (it carries a fetch Response) like the original script did.
async function describeError(err) {
  let detail = '';
  try {
    if (err && err.response && typeof err.response.clone === 'function') {
      detail = `HTTP ${err.response.status} ${(await err.response.clone().text()).slice(0, 300)}`;
    }
  } catch (_) {}
  return detail || (err && err.message) || String(err);
}

// ---------------------------------------------------------------------------
// Date / week helpers (Saturday-based week in the configured timezone)
// ---------------------------------------------------------------------------
function addDays(ymd, n) {
  const [y, m, d] = ymd.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10);
}

function buildContext(now) {
  let parts;
  try {
    parts = new Intl.DateTimeFormat('en-US', {
      timeZone: CFG.timezone, year: 'numeric', month: '2-digit', day: '2-digit', weekday: 'long',
    }).formatToParts(now);
  } catch (e) {
    throw new Error(`Invalid PUBLISH_TIMEZONE "${CFG.timezone}": ${e.message}`);
  }
  const p = {};
  for (const x of parts) p[x.type] = x.value;
  const today = `${p.year}-${p.month}-${p.day}`;
  const dayIndex = ['Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'].indexOf(p.weekday);
  return { today, dayName: p.weekday, dayIndex, weekStart: addDays(today, -dayIndex), tz: CFG.timezone };
}

// ---------------------------------------------------------------------------
// up_tracks history helpers
//   { week_start, days: [ {date, day, handle, track_id, original_title,
//                          published_title, generated_track, status, ...} ] }
//   status: reserved -> publishing -> success | failed   (only "success" counts)
//   The coordinator row additionally carries a temporary "lock" key.
// ---------------------------------------------------------------------------
const isCurrentWeek = (raw, weekStart) => isObj(raw) && raw.week_start === weekStart && Array.isArray(raw.days);

function normalizeHistory(raw, weekStart) {
  if (isCurrentWeek(raw, weekStart)) return { ...raw, days: raw.days.map(d => ({ ...d })) };
  const h = { week_start: weekStart, days: [] }; // new week (or default []): previous history dropped
  if (isObj(raw) && raw.lock) h.lock = raw.lock;
  return h;
}

function cleanEntry(e) {
  const out = {
    date: e.date, day: e.day, handle: e.handle == null ? null : e.handle,
    track_id: e.track_id == null ? null : e.track_id,
    original_title: e.original_title, published_title: e.published_title,
    generated_track: e.generated_track, status: e.status,
  };
  if (e.attempt_started_at) out.attempt_started_at = e.attempt_started_at;
  if (e.error) out.error = e.error;
  return out;
}

// ---------------------------------------------------------------------------
// Supabase access
// ---------------------------------------------------------------------------
function connectSupabase() {
  if (!fs.existsSync(DB_FILE)) throw new Error(`db.txt not found at: ${DB_FILE}`);
  const dbText = fs.readFileSync(DB_FILE, 'utf8');
  const urlMatch = dbText.match(/SUPABASE_URL\s*=\s*["']([^"']+)["']/);
  const keyMatch = dbText.match(/SUPABASE_KEY\s*=\s*["']([^"']+)["']/);
  if (!urlMatch || !keyMatch) throw new Error('Could not read SUPABASE_URL / SUPABASE_KEY from db.txt');
  return createClient(urlMatch[1], keyMatch[1]);
}

const anyErr = () => true;

async function fetchAllRows(sb) {
  const out = [];
  const page = 1000;
  for (let from = 0; ; from += page) {
    const data = await withRetry('load accounts', async () => {
      const { data, error } = await sb.from('audius').select('*').order('address', { ascending: true }).range(from, from + page - 1);
      if (error) throw new Error(error.message);
      return data || [];
    }, { retryIf: anyErr });
    out.push(...data);
    if (data.length < page) break;
  }
  return out;
}

async function readRow(sb, address) {
  const { data, error } = await sb.from('audius').select('address, up_tracks').eq('address', address).limit(1);
  if (error) throw new Error(error.message);
  return data && data[0];
}

// Read-modify-write of one account's history. Only the lock holder writes, so no clobbering.
async function updateHistory(sb, address, weekStart, mutate) {
  return withRetry(`db update ${address}`, async () => {
    const row = await readRow(sb, address);
    if (!row) throw new Error(`account row not found: ${address}`);
    const hist = normalizeHistory(row.up_tracks, weekStart);
    if (mutate(hist) === false) return hist;
    const { data, error } = await sb.from('audius').update({ up_tracks: hist }).eq('address', address).select('address, up_tracks');
    if (error) throw new Error(error.message);
    if (!data || data.length !== 1) throw new Error(`update affected ${data ? data.length : 0} rows`);
    return normalizeHistory(data[0].up_tracks, weekStart); // value as stored by the database
  }, { retryIf: anyErr });
}

// Insert/update today's record for an account and verify what the database stored.
async function upsertEntry(sb, ctx, address, patch) {
  const stored = await updateHistory(sb, address, ctx.weekStart, hist => {
    const i = hist.days.findIndex(d => d.date === patch.date);
    if (i >= 0) {
      if (hist.days[i].status === 'success' && patch.status !== 'success') return false; // never downgrade
      hist.days[i] = cleanEntry({ ...hist.days[i], ...patch });
    } else {
      hist.days.push(cleanEntry(patch));
    }
    return true;
  });
  const saved = stored.days.find(d => d.date === patch.date);
  if (saved && saved.status === 'success' && patch.status !== 'success') return saved;
  if (!saved || saved.status !== patch.status || (patch.track_id !== undefined && (saved.track_id || null) !== (patch.track_id || null))) {
    throw new Error(`database verification failed for ${address} (${patch.date})`);
  }
  return saved;
}

// Saturday reset: any row whose history is not for the current week is re-initialised.
// Rows already holding the current week are left untouched (idempotent).
async function weeklyResetPass(sb, rows, ctx) {
  const stale = rows.filter(r => !isCurrentWeek(r.up_tracks, ctx.weekStart));
  if (!stale.length) return 0;
  log(`[*] Week ${ctx.weekStart}: initialising weekly history for ${stale.length} account(s)...`);
  for (let i = 0; i < stale.length; i += 10) {
    await Promise.all(stale.slice(i, i + 10).map(r => updateHistory(sb, r.address, ctx.weekStart, () => true)));
  }
  return stale.length;
}

// ---------------------------------------------------------------------------
// Batch lock (atomic compare-and-set on one coordinator row's up_tracks.lock)
// ---------------------------------------------------------------------------
class LockLostError extends Error {}

// true = owner process is alive on this host, false = it is gone, null = cannot tell
// (lock written by an older version without pid/host, or by another machine).
function lockOwnerAlive(lock) {
  if (!lock.pid || lock.host !== os.hostname()) return null;
  try { process.kill(lock.pid, 0); return true; } catch (e) { return e.code === 'EPERM'; }
}

// Does the stored history hold ANY entry for today? ("days": [] => no.)
function hasDataForToday(rows, ctx) {
  return rows.some(r => normalizeHistory(r.up_tracks, ctx.weekStart).days.some(d => d.date === ctx.today));
}

// Returns why an existing lock may be taken over, or null if it must be respected.
function staleLockReason(lock, rows, ctx) {
  if (!(Date.parse(lock.expires_at) > Date.now())) return 'it expired';
  const alive = lockOwnerAlive(lock);
  if (alive === true) return null;                      // live owner on this machine: never take over
  if (alive === false) return 'its owner process is no longer running';
  // Owner unknown: look at the data. Nothing recorded for today ("days": []) means no work
  // to protect, so a lock that old is a leftover of a run that died before saving anything.
  const age = Date.now() - Date.parse(lock.acquired_at);
  if (CFG.lockIdleMs > 0 && age >= CFG.lockIdleMs && !hasDataForToday(rows, ctx)) {
    return `no data is recorded for ${ctx.today} ("days" is empty)`;
  }
  return null;
}

async function acquireLock(sb, rows, ctx) {
  const coordAddress = rows[0].address; // first row by address: same choice for every run
  const fresh = await readRow(sb, coordAddress);
  const raw = fresh ? fresh.up_tracks : null;
  const existing = isObj(raw) && isObj(raw.lock) ? raw.lock : null;
  if (existing && existing.token) {
    const why = staleLockReason(existing, rows, ctx);
    if (!why) {
      log(`[!] Another run holds the batch lock (since ${existing.acquired_at}). Exiting.`);
      return null;
    }
    log(`[~] Taking over batch lock from ${existing.acquired_at}: ${why}.`);
  }

  const token = crypto.randomUUID();
  const lock = {
    token, acquired_at: new Date().toISOString(), expires_at: new Date(Date.now() + CFG.lockTtlMs).toISOString(),
    pid: process.pid, host: os.hostname(),
  };
  const next = isObj(raw) ? { ...raw, lock } : { week_start: ctx.weekStart, days: [], lock };

  let q = sb.from('audius').update({ up_tracks: next }).eq('address', coordAddress);
  q = existing && existing.token
    ? q.eq('up_tracks->lock->>token', existing.token).eq('up_tracks->lock->>expires_at', existing.expires_at)
    : q.is('up_tracks->lock->>token', null);
  const { data, error } = await q.select('address');
  if (error) throw new Error(`lock acquisition failed: ${error.message}`);
  if (!data || data.length !== 1) {
    log('[!] Lost the race for the batch lock to another run. Exiting.');
    return null;
  }
  log('[+] Batch lock acquired.');
  return { token, address: coordAddress };
}

async function renewLock(sb, lock) {
  await withRetry('renew lock', async () => {
    const row = await readRow(sb, lock.address);
    const raw = row && row.up_tracks;
    const cur = isObj(raw) && isObj(raw.lock) ? raw.lock : null;
    if (!cur || cur.token !== lock.token) throw new LockLostError('batch lock was taken over by another run');
    const next = { ...raw, lock: { ...cur, expires_at: new Date(Date.now() + CFG.lockTtlMs).toISOString() } };
    const { data, error } = await sb.from('audius').update({ up_tracks: next })
      .eq('address', lock.address).eq('up_tracks->lock->>token', lock.token).select('address');
    if (error) throw new Error(error.message);
    if (!data || data.length !== 1) throw new LockLostError('batch lock was taken over by another run');
  }, { retryIf: e => !(e instanceof LockLostError) });
}

async function releaseLock(sb, lock) {
  if (!lock) return;
  try {
    const row = await readRow(sb, lock.address);
    const raw = row && row.up_tracks;
    if (!isObj(raw) || !isObj(raw.lock) || raw.lock.token !== lock.token) return;
    const { lock: _drop, ...rest } = raw;
    await sb.from('audius').update({ up_tracks: rest }).eq('address', lock.address).eq('up_tracks->lock->>token', lock.token);
    log('[+] Batch lock released.');
  } catch (e) {
    console.error(`[!] Could not release batch lock (it will expire on its own): ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Accounts & selection
// ---------------------------------------------------------------------------
const hasCreds = r => !!(r.user_id && r.app_data && r.app_data.api_key && r.app_data.api_secret && r.app_data.bearer_token);

// The Audius handle is stored in the audius.handle column.
function detectHandle(row) {
  return typeof row.handle === 'string' && row.handle.trim() ? row.handle.trim().replace(/^@/, '') : null;
}

function buildAccounts(rows, ctx) {
  return rows.map(r => {
    const history = normalizeHistory(r.up_tracks, ctx.weekStart);
    const handle = detectHandle(r);
    return {
      address: r.address, user_id: r.user_id, app_data: r.app_data, handle,
      isPriority: !!handle && CFG.priorityHandles.includes(handle.toLowerCase()),
      hasCreds: hasCreds(r), history,
      todayEntry: history.days.find(d => d.date === ctx.today) || null,
      successCount: history.days.filter(d => d.status === 'success').length,
    };
  });
}

const isBatchComplete = accounts => {
  const today = accounts.filter(a => a.todayEntry);
  return today.length >= CFG.accountsPerDay && today.every(a => a.todayEntry.status === 'success' && a.todayEntry.track_id);
};

function makeSdk(acct) {
  return sdkFactory({
    apiKey: acct.app_data.api_key,
    apiSecret: acct.app_data.api_secret,
    bearerToken: acct.app_data.bearer_token,
  });
}

// Safety net: if a priority account's audius.handle is empty, find it via the Audius API.
async function resolvePriorityAccounts(accounts) {
  const found = new Set(accounts.filter(a => a.isPriority).map(a => a.handle.toLowerCase()));
  const missing = CFG.priorityHandles.filter(h => !found.has(h));
  if (!missing.length) return;
  const reader = accounts.find(a => a.hasCreds);
  if (!reader) return;
  const sdk = makeSdk(reader);
  for (const h of missing) {
    try {
      const res = await withRetry(`lookup @${h}`, () => sdk.users.getUserByHandle({ handle: h }));
      const id = res && res.data && res.data.id;
      const acct = id && accounts.find(a => a.user_id === id);
      if (acct) { acct.handle = h; acct.isPriority = true; }
      else log(`[!] Priority handle @${h} was not found among the audius table accounts.`);
    } catch (e) {
      log(`[!] Could not look up priority handle @${h}: ${e.message || e}`);
    }
  }
}

// Priority accounts: 3 days per Saturday-based week, no two adjacent when possible.
// The plan is derived from (week, handle) so it never needs to be stored, and
// missed days are caught up automatically.
function priorityPlan(weekStart, handle) {
  const combos = [];
  for (let a = 0; a < 7; a++) for (let b = a + 2; b < 7; b++) for (let c = b + 2; c < 7; c++) combos.push([a, b, c]);
  const rnd = mulberry32(hashSeed(`${weekStart}|${handle.toLowerCase()}`));
  return combos[Math.floor(rnd() * combos.length)];
}

function isPriorityDue(acct, ctx) {
  if (acct.successCount >= CFG.priorityPerWeek) return false;
  const plan = priorityPlan(ctx.weekStart, acct.handle);
  const plannedRemaining = plan.filter(d => d >= ctx.dayIndex).length;
  return plan.includes(ctx.dayIndex) || acct.successCount + plannedRemaining < CFG.priorityPerWeek;
}

function chooseAccounts(accounts, ctx, need, blocked) {
  const due = accounts.filter(a => a.isPriority && !a.todayEntry && !blocked.has(a.address) && isPriorityDue(a, ctx));
  due.filter(a => !a.hasCreds).forEach(a => log(`[!] Priority account @${a.handle} is due today but has incomplete credentials.`));
  const picked = due.filter(a => a.hasCreds).slice(0, need);
  const slots = need - picked.length;
  const pool = accounts.filter(a => !a.isPriority && a.hasCreds && !a.todayEntry && a.successCount === 0 && !blocked.has(a.address));
  if (pool.length < slots) {
    throw new Error(`Not enough eligible accounts: need ${slots} more but only ${pool.length} have not published this week.`);
  }
  return shuffle([...picked, ...shuffle(pool).slice(0, slots)]);
}

// Balanced random assignment: the first account of each track keeps the original
// title; everyone else gets a fresh title. Always fills the track with the fewest accounts.
function assignNewAccounts(newAccounts, todayAccounts, tracks) {
  const slots = tracks.map(t => t.slot);
  const counts = Object.fromEntries(slots.map(s => [s, 0]));
  const holders = new Set();
  const usedTitles = new Set(tracks.map(t => t.title.toLowerCase()));
  for (const a of todayAccounts) {
    const e = a.todayEntry;
    if (counts[e.generated_track] !== undefined) counts[e.generated_track]++;
    usedTitles.add(String(e.published_title).toLowerCase());
    if (e.published_title === e.original_title) holders.add(e.generated_track);
  }
  return newAccounts.map(acct => {
    const open = slots.filter(s => !holders.has(s));
    let slot, original;
    if (open.length) { slot = pick(open); original = true; holders.add(slot); }
    else {
      const min = Math.min(...slots.map(s => counts[s]));
      slot = pick(slots.filter(s => counts[s] === min));
      original = false;
    }
    counts[slot]++;
    const track = tracks.find(t => t.slot === slot);
    return { acct, slot, track, title: original ? track.title : generateTitle(track.title, usedTitles) };
  });
}

// ---------------------------------------------------------------------------
// Local recovery journal
// ---------------------------------------------------------------------------
function loadJournal() {
  try {
    const j = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    return { date: j.date || null, tracks: Array.isArray(j.tracks) ? j.tracks : [], uploads: isObj(j.uploads) ? j.uploads : {} };
  } catch (_) {
    return { date: null, tracks: [], uploads: {} };
  }
}

function saveJournal(j) {
  const tmp = `${STATE_FILE}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(j, null, 2));
  fs.renameSync(tmp, STATE_FILE);
}

// ---------------------------------------------------------------------------
// Music generation (music/create_track.py, MAX_TRACKS=3)
// ---------------------------------------------------------------------------
function snapshotDir(dir, ext) {
  const m = new Map();
  if (!fs.existsSync(dir)) return m;
  for (const f of fs.readdirSync(dir)) if (f.endsWith(ext)) m.set(f, fs.statSync(path.join(dir, f)).mtimeMs);
  return m;
}

function newFiles(dir, ext, snap) {
  const out = [];
  if (!fs.existsSync(dir)) return out;
  for (const f of fs.readdirSync(dir)) {
    if (!f.endsWith(ext)) continue;
    const st = fs.statSync(path.join(dir, f));
    const prev = snap.get(f);
    if (st.size > 0 && (prev === undefined || st.mtimeMs > prev)) out.push({ name: f, path: path.join(dir, f), mtime: st.mtimeMs });
  }
  return out.sort((a, b) => a.mtime - b.mtime || a.name.localeCompare(b.name));
}

// tracks_dl.py saves {safe_title}.mp3 and images/{safe_title}.jpg, so a complete track
// is an mp3 with a cover of the exact same base name. An mp3 without its cover is skipped
// (the cover is optional in the downloader) and made up by the top-up run.
function pairFiles(mp3s, jpgs) {
  const base = f => f.name.replace(/\.[^.]+$/, '');
  const pairs = [];
  for (const m of mp3s) {
    const j = jpgs.find(x => base(x) === base(m));
    if (j) pairs.push({ mp3: m, jpg: j });
    else log(`[~] ${m.name} has no cover image; not counted as a complete track.`);
  }
  return pairs;
}

function titleFromFilename(audioFilename) {
  const rawTitle = audioFilename.replace('.mp3', '').replace(/-/g, ' ');
  return rawTitle.replace(/\b\w/g, c => c.toUpperCase());
}

function generateTracks() {
  const target = CFG.tracksPerDay;
  const snapA = snapshotDir(MUSIC_DIR, '.mp3');
  const snapJ = snapshotDir(IMAGES_DIR, '.jpg');
  let pairs = [];

  for (let attempt = 0; attempt <= CFG.genTopUpAttempts && pairs.length < target; attempt++) {
    const count = attempt === 0 ? target : target - pairs.length; // top-ups only request what is missing
    log(`[*] Triggering music generation (MAX_TRACKS=${count})...`);
    try {
      execSync('python3 music/create_track.py', {
        cwd: ROOT_DIR, stdio: 'inherit', timeout: CFG.genTimeoutMs,
        env: { ...process.env, MAX_TRACKS: String(count) },
      });
    } catch (e) {
      console.error(`[!] Python script failed: ${e.message}`);
    }
    pairs = pairFiles(newFiles(MUSIC_DIR, '.mp3', snapA), newFiles(IMAGES_DIR, '.jpg', snapJ));
    log(`[*] ${pairs.length}/${target} track(s) (audio + cover) ready.`);
  }

  if (pairs.length < target) throw new Error(`Only ${pairs.length}/${target} tracks were generated`);
  if (pairs.length > target) {
    log(`[~] ${pairs.length} tracks found; using the newest ${target}.`);
    pairs = pairs.slice(-target);
  }
  return pairs.map((p, i) => ({
    slot: `track_${i + 1}`, title: titleFromFilename(p.mp3.name), audioPath: p.mp3.path, coverPath: p.jpg.path,
  }));
}

function ensureTracks(ctx, journal) {
  const ok = journal.date === ctx.today && journal.tracks.length === CFG.tracksPerDay &&
    journal.tracks.every(t => fs.existsSync(t.audioPath) && fs.existsSync(t.coverPath));
  if (ok) {
    log('[*] Reusing the 3 tracks already generated for today.');
    return journal.tracks;
  }
  const tracks = generateTracks();
  journal.date = ctx.today;
  journal.tracks = tracks;
  saveJournal(journal);
  return tracks;
}

// ---------------------------------------------------------------------------
// Audius helpers
// ---------------------------------------------------------------------------
const extractTrackId = r => {
  const id = r?.data?.id || r?.data?.trackId || r?.trackId || r?.id;
  return id ? String(id) : null;
};

async function resolveHandle(sdk, userId) {
  const res = await withRetry('resolve handle', () => sdk.users.getUser({ id: userId }));
  return (res && res.data && res.data.handle) || null;
}

// Looks for a track this user already published with that title (duplicate protection).
async function findExistingTrack(sdk, userId, title, sinceIso) {
  const res = await withRetry('check existing tracks', () => sdk.users.getTracksByUser({ id: userId, limit: 100 }));
  const since = sinceIso ? Date.parse(sinceIso) - 10 * 60000 : 0;
  const hit = ((res && res.data) || []).find(t => {
    if (t.title !== title) return false;
    const ts = Date.parse(t.createdAt || t.created_at || t.releaseDate || '');
    return !(since && Number.isFinite(ts) && ts < since);
  });
  return hit ? String(hit.id) : null;
}

async function verifyTrack(sdk, trackId) {
  for (let i = 1; i <= CFG.verifyTries; i++) {
    try {
      const t = await sdk.tracks.getTrack({ trackId });
      if (t && t.data && String(t.data.id) === String(trackId)) return t.data;
    } catch (_) {}
    if (i < CFG.verifyTries) await sleep(CFG.verifyDelayMs);
  }
  return null;
}

// ---------------------------------------------------------------------------
// Recovery of ambiguous ("publishing") records from an interrupted run
// ---------------------------------------------------------------------------
async function reconcileAmbiguous(sb, accounts, ctx, journal) {
  const blocked = new Set();
  for (const acct of accounts) {
    if (!acct.hasCreds) continue;
    for (const entry of acct.history.days) {
      const jr = journal.uploads[acct.address];
      const journaled = jr && jr.date === entry.date && entry.status !== 'success';
      if (entry.status !== 'publishing' && !journaled) continue;
      const sdk = makeSdk(acct);
      try {
        const known = entry.track_id || (journaled && jr.track_id);
        let id = null;
        if (known) id = (await verifyTrack(sdk, known)) ? String(known) : null;
        if (!id) id = await findExistingTrack(sdk, acct.user_id, entry.published_title, entry.attempt_started_at);
        if (id) {
          await upsertEntry(sb, ctx, acct.address, { ...entry, track_id: id, status: 'success', error: undefined });
          log(`[+] Recovered ${entry.date} publication for ${acct.handle || acct.address}: track ${id} (no re-upload).`);
          delete journal.uploads[acct.address]; saveJournal(journal);
        } else if (entry.date !== ctx.today) {
          await upsertEntry(sb, ctx, acct.address, { ...entry, track_id: null, status: 'failed', error: 'unconfirmed publication; no track found on Audius' });
        } else {
          await upsertEntry(sb, ctx, acct.address, { ...entry, track_id: null, status: 'reserved', attempt_started_at: undefined, error: undefined });
        }
      } catch (e) {
        blocked.add(acct.address);
        log(`[!] Cannot confirm earlier publication for ${acct.handle || acct.address}; skipping it this run: ${e.message || e}`);
      }
    }
  }
  return blocked;
}

// ---------------------------------------------------------------------------
// Upload one account (existing SDK flow, per-account credentials)
// ---------------------------------------------------------------------------
async function publishOne(sb, ctx, acct, entry, track, usedDescriptions, journal) {
  const userId = acct.user_id;
  const sdk = makeSdk(acct); // this account's own api_key / api_secret / bearer_token
  if (!entry.handle) {
    try { entry.handle = await resolveHandle(sdk, userId); } catch (_) {}
  }

  // In Node the SDK expects plain { buffer, name, type } file objects.
  const trackFile = { buffer: fs.readFileSync(track.audioPath), name: path.basename(track.audioPath), type: 'audio/mpeg' };
  const coverArtFile = { buffer: fs.readFileSync(track.coverPath), name: path.basename(track.coverPath), type: 'image/jpeg' };

  // createTrack() only publishes metadata. The files must be uploaded to the
  // storage nodes first, and createTrack gets the resulting CIDs.
  log(`    [*] Uploading audio...`);
  const audio = await withRetry('audio upload', async () => {
    const a = await sdk.uploads.createAudioUpload({ file: trackFile, userId }).start();
    if (!a || !a.trackCid) throw new Error(`audio upload returned no trackCid: ${JSON.stringify(a)}`);
    return a;
  });

  log(`    [*] Uploading cover...`);
  const coverCid = await withRetry('cover upload', async () => {
    const c = await sdk.uploads.createImageUpload({ file: coverArtFile, isBackdrop: false }).start();
    if (!c || typeof c !== 'string') throw new Error(`cover upload returned no CID: ${JSON.stringify(c)}`);
    return c;
  });

  const description = uniqueDescription(entry.published_title, { bpm: audio.bpm, musicalKey: audio.musicalKey }, usedDescriptions);
  log(`    [+] Title: ${entry.published_title}`);
  log(`    [+] Description: ${description}`);

  // Intent marker: if we crash after this point the next run checks Audius before uploading again.
  const attemptAt = new Date().toISOString();
  Object.assign(entry, await upsertEntry(sb, ctx, acct.address, { ...entry, status: 'publishing', attempt_started_at: attemptAt, error: undefined }));

  const payload = {
    userId,
    metadata: {
      title: entry.published_title,
      genre: 'Electronic',
      mood: 'Energizing',
      description,
      // Must be a Date object (the SDK calls .toISOString() on it).
      releaseDate: new Date(),
      isStreamGated: false,
      isDownloadGated: false,
      isUnlisted: false,
      trackCid: audio.trackCid,
      origFileCid: audio.origFileCid,
      origFilename: audio.origFilename,
      duration: audio.duration,
      // Audius stores the artwork CID as coverArtSizes; coverArt is kept as a
      // fallback name. Unknown fields are ignored by the SDK, so this is safe.
      coverArtSizes: coverCid,
      coverArt: coverCid,
    },
  };

  log(`    [*] Publishing track...`);
  let trackId = null;
  for (let attempt = 1; !trackId; attempt++) {
    let resp, createErr = null;
    try { resp = await sdk.tracks.createTrack(payload); } catch (e) { createErr = e; }
    trackId = createErr ? null : extractTrackId(resp);
    if (trackId) break;
    // Not confirmed. Never retry blindly: first check whether the track actually landed.
    await sleep(CFG.verifyDelayMs);
    try {
      trackId = await findExistingTrack(sdk, userId, entry.published_title, attemptAt);
    } catch (lookupErr) {
      lookupErr.ambiguous = true;
      lookupErr.message = `publish outcome unknown (${lookupErr.message}); will be checked on next run`;
      throw lookupErr;
    }
    if (trackId) { log(`    [+] Track had already been published; reusing its ID.`); break; }
    if (!createErr) throw new Error('createTrack returned no track ID and no matching track was found');
    if (attempt >= CFG.retries || !isTransient(createErr)) throw createErr;
    await sleep(CFG.retryBaseMs * 2 ** (attempt - 1));
  }

  // Keep the ID locally first, so a database failure can never lead to a duplicate upload.
  journal.uploads[acct.address] = { date: ctx.today, track_id: trackId, published_title: entry.published_title, handle: entry.handle || null };
  saveJournal(journal);
  entry.track_id = trackId;
  try { await upsertEntry(sb, ctx, acct.address, { ...entry, status: 'publishing', track_id: trackId }); } catch (_) {}

  const verified = await verifyTrack(sdk, trackId);
  if (!verified) {
    const e = new Error(`track ${trackId} was created but could not be verified on Audius yet`);
    e.ambiguous = true;
    throw e;
  }
  // Confirm the cover actually attached (unknown metadata fields are silently
  // dropped by the SDK, which is how a track can publish without artwork).
  const art = verified.artwork;
  log(art && Object.keys(art).length > 0 ? `    [+] Cover art attached.` : `    [!] Track published, but no cover art detected (it may still be indexing).`);

  try {
    await upsertEntry(sb, ctx, acct.address, { ...entry, status: 'success', track_id: trackId, error: undefined });
  } catch (e) {
    e.ambiguous = true;
    e.message = `published on Audius as ${trackId} but the database update failed (${e.message}); saved in the local journal for recovery`;
    throw e;
  }
  delete journal.uploads[acct.address];
  saveJournal(journal);
  return trackId;
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
function printSummary(ctx, accounts, tracksCount, status, failures) {
  const today = accounts.filter(a => a.todayEntry);
  const ok = today.filter(a => a.todayEntry.status === 'success');
  const bad = today.filter(a => a.todayEntry.status !== 'success');
  log('\n==============================');
  log('AUDIUS DAILY PUBLISHING SUMMARY');
  log('==============================');
  log(`Date: ${ctx.today} (${ctx.dayName}, ${ctx.tz})`);
  log(`Week start (Saturday): ${ctx.weekStart}\n`);
  log(`Accounts selected: ${today.length}`);
  log(`Tracks generated: ${tracksCount}`);
  log(`Tracks published: ${ok.length}\n`);
  log(`Successful uploads: ${ok.length}`);
  log(`Failed uploads: ${bad.length}\n`);
  log('Priority accounts:');
  for (const h of CFG.priorityHandles) {
    const a = accounts.find(x => x.handle && x.handle.toLowerCase() === h);
    log(`${h}: ${a ? `${a.successCount}/${CFG.priorityPerWeek} this week` : 'account not found'}`);
  }
  if (bad.length) {
    log('\nFailed accounts:');
    for (const a of bad) {
      log(`- ${a.handle || a.address}: ${(failures.get(a.address)) || a.todayEntry.error || a.todayEntry.status}`);
    }
  }
  log(`\nDaily batch status: ${status}\n`);
}

// ---------------------------------------------------------------------------
// Main flow
// ---------------------------------------------------------------------------
async function runDaily({ now = new Date() } = {}) {
  const ctx = buildContext(now);
  const sb = connectSupabase();
  log(`[*] Publishing date ${ctx.today} (${ctx.dayName}, ${ctx.tz}); week starts Saturday ${ctx.weekStart}.`);

  // 1. Duplicate-run check against the database BEFORE generating or selecting anything.
  let rows = await fetchAllRows(sb);
  if (!rows.length) throw new Error('audius table returned no accounts');
  if (isBatchComplete(buildAccounts(rows, ctx))) {
    log(ALREADY_DONE_MSG);
    return { status: 'ALREADY_COMPLETED' };
  }

  // 2. Take the database-backed batch lock.
  const lock = await acquireLock(sb, rows, ctx);
  if (!lock) return { status: 'LOCKED' };

  try {
    // 3. Re-check under the lock, then handle the weekly reset and interrupted runs.
    rows = await fetchAllRows(sb);
    if (isBatchComplete(buildAccounts(rows, ctx))) {
      log(ALREADY_DONE_MSG);
      return { status: 'ALREADY_COMPLETED' };
    }
    if (await weeklyResetPass(sb, rows, ctx)) rows = await fetchAllRows(sb);

    const journal = loadJournal();
    let accounts = buildAccounts(rows, ctx);
    const blocked = await reconcileAmbiguous(sb, accounts, ctx, journal);
    rows = await fetchAllRows(sb);
    accounts = buildAccounts(rows, ctx);
    if (isBatchComplete(accounts)) {
      log(ALREADY_DONE_MSG);
      return { status: 'ALREADY_COMPLETED' };
    }
    await resolvePriorityAccounts(accounts);

    // 4. Work out who still has to be published today.
    const todayAccounts = accounts.filter(a => a.todayEntry);
    const pending = todayAccounts.filter(a => a.todayEntry.status !== 'success' && !blocked.has(a.address));
    const needMore = CFG.accountsPerDay - todayAccounts.length;
    let selected = [];
    try {
      selected = needMore > 0 ? chooseAccounts(accounts, ctx, needMore, blocked) : [];
    } catch (e) {
      console.error(`[!] ${e.message}`);
      return { status: 'ABORTED' };
    }
    if (todayAccounts.length) {
      log(`[*] Resuming today's batch: ${todayAccounts.length - pending.length - blocked.size} done, ${pending.length} pending, ${selected.length} still to select.`);
    }
    if (!pending.length && !selected.length) {
      log('[!] Nothing left to publish, but the batch could not be confirmed complete (see warnings above).');
      return { status: 'INCOMPLETE' };
    }

    // 5. Generate exactly 3 tracks (or reuse today's) before touching Audius.
    let tracks;
    try {
      tracks = ensureTracks(ctx, journal);
    } catch (e) {
      console.error(`[!] Music generation failed: ${e.message}. Nothing was published; exiting safely.`);
      return { status: 'GENERATION_FAILED' };
    }
    log('\n[+] Generated tracks:');
    tracks.forEach(t => log(`    ${t.slot}: "${t.title}"  (${path.basename(t.audioPath)} + ${path.basename(t.coverPath)})`));

    // 6. Re-point pending records at today's files if the tracks were regenerated.
    for (const a of pending) {
      const e = a.todayEntry;
      const t = tracks.find(x => x.slot === e.generated_track) || tracks[0];
      if (e.original_title !== t.title || e.generated_track !== t.slot) {
        const keepOriginal = e.published_title === e.original_title;
        Object.assign(e, await upsertEntry(sb, ctx, a.address, {
          ...e, generated_track: t.slot, original_title: t.title, published_title: keepOriginal ? t.title : e.published_title,
        }));
      }
    }

    // 7. Assign tracks/titles to newly selected accounts and reserve them in the database.
    const queue = pending.map(a => ({ acct: a, entry: a.todayEntry }));
    if (selected.length) {
      for (const a of selected) {
        if (!a.handle && !a.isPriority) {
          try { a.handle = await resolveHandle(makeSdk(a), a.user_id); } catch (_) {}
        }
      }
      const assignments = assignNewAccounts(selected, todayAccounts, tracks);
      for (const { acct, slot, track, title } of assignments) {
        const entry = await upsertEntry(sb, ctx, acct.address, {
          date: ctx.today, day: ctx.dayName, handle: acct.handle || null, track_id: null,
          original_title: track.title, published_title: title, generated_track: slot, status: 'reserved',
        });
        queue.push({ acct, entry });
      }
    }

    log(`\n[+] Today's accounts to publish (${queue.length}):`);
    queue.forEach(({ acct, entry }, i) => log(
      `    ${String(i + 1).padStart(2)}. ${acct.handle || acct.address}${acct.isPriority ? ' [priority]' : ''}  -> ${entry.generated_track}  "${entry.published_title}"${entry.published_title === entry.original_title ? ' (original)' : ''}`));

    // 8. Publish sequentially, each account with its own credentials.
    const usedDescriptions = new Set();
    const failures = new Map();
    let n = 0;
    for (const { acct, entry } of queue) {
      n++;
      log(`\n[*] (${n}/${queue.length}) Publishing for ${acct.handle || acct.address}...`);
      try {
        await renewLock(sb, lock);
        const track = tracks.find(t => t.slot === entry.generated_track);
        const id = await publishOne(sb, ctx, acct, entry, track, usedDescriptions, journal);
        log(`    [+] SUCCESS! Track ID: ${id}`);
      } catch (err) {
        if (err instanceof LockLostError) throw err;
        const detail = await describeError(err);
        failures.set(acct.address, detail);
        console.error(`    [!] Audius upload failed for ${acct.handle || acct.address}: ${detail}`);
        if (!err.ambiguous) {
          try { await upsertEntry(sb, ctx, acct.address, { ...entry, track_id: null, status: 'failed', error: String(detail).slice(0, 300) }); } catch (_) {}
        }
      }
      if (CFG.uploadDelayMs && n < queue.length) await sleep(CFG.uploadDelayMs);
    }

    // 9. Final verification straight from the database.
    accounts = buildAccounts(await fetchAllRows(sb), ctx);
    const complete = isBatchComplete(accounts);
    printSummary(ctx, accounts, tracks.length, complete ? 'COMPLETED' : 'INCOMPLETE (re-run to recover)', failures);
    return { status: complete ? 'COMPLETED' : 'INCOMPLETE' };
  } finally {
    await releaseLock(sb, lock);
  }
}

async function main() {
  const result = await runDaily();
  if (['INCOMPLETE', 'GENERATION_FAILED', 'ABORTED'].includes(result.status)) process.exitCode = 1;
}

if (require.main === module) {
  main().catch(err => {
    console.error('[!] Fatal error:', err.message || err);
    process.exit(1);
  });
}

module.exports = { runDaily, priorityPlan, buildContext };
