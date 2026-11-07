#!/usr/bin/env node
'use strict';

/**
 * module/follower.js - two daily batches of 50 follower accounts that follow today's
 * published Audius handles (read from audius.up_tracks, never written).
 *
 * Storage (audius.follower, jsonb):
 *   every follower account : { week_start, runs: [ { date, day, run_number, targets: [ { handle, followed_at } ] } ], v }
 *   qringgreen row (coordinator) additionally holds the shared batch state:
 *       batches: [ { date, run_number, accounts: [address...], done } ]   and a temporary  lock
 *   Only successful follows are stored. "v" is a write counter used for safe (compare-and-set) updates.
 *
 * Independent of upload_track.js: nothing is imported from it, and up_tracks is only read.
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');

// --- Safely resolve SDK (same approach as upload_track.js) ---
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
const STATE_FILE = path.join(ROOT_DIR, '.audius_follower_state.json'); // follows done but not yet saved to the DB
const TARGET_ADDRESS = 'qringgreen@gmail.com'; // holds up_tracks (read) and the shared batch state (follower)

const CFG = {
  timezone: process.env.PUBLISH_TIMEZONE || 'UTC',
  accountsPerRun: 50,
  runsPerDay: 2,
  concurrency: 5, // hard cap on simultaneous follow jobs
  lockTtlMs: Number(process.env.LOCK_TTL_MINUTES || 120) * 60000,
  retries: Number(process.env.RETRY_ATTEMPTS || 3),
  retryBaseMs: Number(process.env.RETRY_BASE_MS || 2000),
  verifyTries: 4,
  verifyDelayMs: Number(process.env.VERIFY_DELAY_MS || 2500),
  followDelayMs: Number(process.env.FOLLOW_DELAY_MS || 300),
  scanPages: Number(process.env.FOLLOWING_SCAN_PAGES || 10), // fallback check: pages of 100 followings
};

const BOTH_DONE_MSG = '[!] Both follower runs for today have already been completed. Exiting.';
const HANDLE_RE = /^[A-Za-z0-9_.]{1,30}$/;

// ---------------------------------------------------------------------------
// Generic helpers
// ---------------------------------------------------------------------------
const sleep = ms => new Promise(r => setTimeout(r, ms));
const isObj = v => !!v && typeof v === 'object' && !Array.isArray(v);
const statusOf = e => e && ((e.response && e.response.status) || e.status);
const errMsg = e => `${statusOf(e) ? `HTTP ${statusOf(e)} ` : ''}${(e && e.message) || e}`;

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function isRateLimit(e) {
  return statusOf(e) === 429 || /rate.?limit|too many requests|\b429\b/i.test(String((e && e.message) || e));
}

function isTransient(e) {
  const s = statusOf(e);
  if (s) return s === 408 || s >= 500;
  const msg = String((e && e.message) || e).toLowerCase();
  const code = String((e && (e.code || (e.cause && e.cause.code))) || '').toUpperCase();
  return /ECONNRESET|ETIMEDOUT|ECONNREFUSED|ENOTFOUND|EAI_AGAIN|EPIPE|UND_ERR/.test(code) ||
    /fetch failed|network|timeout|timed out|socket hang up|temporar|bad gateway|service unavailable|gateway/.test(msg);
}

// 'rate' = back off and retry on a later run; 'permanent' = 4xx, retrying will not help; 'retry' = try again later.
function classify(e) {
  if (isRateLimit(e)) return 'rate';
  const s = statusOf(e);
  return s && s >= 400 && s < 500 && s !== 408 ? 'permanent' : 'retry';
}

async function withRetry(fn, retryIf = isTransient, tries = CFG.retries) {
  let lastErr;
  for (let i = 1; i <= tries; i++) {
    try { return await fn(); } catch (e) {
      lastErr = e;
      if (i === tries || !retryIf(e)) break;
      await sleep(CFG.retryBaseMs * 2 ** (i - 1));
    }
  }
  throw lastErr;
}
const readRetry = fn => withRetry(fn, e => !isRateLimit(e) && isTransient(e)); // 429 is never retried

// Fixed-size worker pool: never more than `limit` workers (= jobs) running at once.
async function runPool(items, limit, worker) {
  let next = 0;
  const lanes = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const item = items[next++];
      try { await worker(item); } catch (_) { /* workers handle their own errors */ }
    }
  });
  await Promise.all(lanes);
}

// ---------------------------------------------------------------------------
// Date / week helpers (identical logic to upload_track.js: Saturday-based week)
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
// Supabase access
// ---------------------------------------------------------------------------
class LockLostError extends Error {}

function connectSupabase() {
  if (!fs.existsSync(DB_FILE)) throw new Error(`db.txt not found at: ${DB_FILE}`);
  const dbText = fs.readFileSync(DB_FILE, 'utf8');
  const urlMatch = dbText.match(/SUPABASE_URL\s*=\s*["']([^"']+)["']/);
  const keyMatch = dbText.match(/SUPABASE_KEY\s*=\s*["']([^"']+)["']/);
  if (!urlMatch || !keyMatch) throw new Error('Could not read SUPABASE_URL / SUPABASE_KEY from db.txt');
  return createClient(urlMatch[1], keyMatch[1]);
}

async function fetchRows(sb) {
  const out = [];
  const page = 1000;
  for (let from = 0; ; from += page) {
    const data = await withRetry(async () => {
      const { data, error } = await sb.from('audius').select('address, handle, user_id, app_data, follower')
        .order('address', { ascending: true }).range(from, from + page - 1);
      if (error) {
        const e = new Error(/follower/i.test(error.message) && /column|exist|schema/i.test(error.message)
          ? 'audius.follower column is missing - run the SQL migration first.' : error.message);
        e.fatal = true;
        throw e;
      }
      return data || [];
    }, e => !e.fatal);
    out.push(...data);
    if (data.length < page) break;
  }
  if (!out.some(r => r.address === TARGET_ADDRESS)) throw new Error(`row not found in audius table: ${TARGET_ADDRESS}`);
  return out;
}

// Read-only: the publishing history belongs to upload_track.js.
async function readUpTracks(sb) {
  const { data, error } = await sb.from('audius').select('up_tracks').eq('address', TARGET_ADDRESS).limit(1);
  if (error) throw new Error(`could not read up_tracks: ${error.message}`);
  return data && data[0] ? data[0].up_tracks : null;
}

// Today's successfully published handles (deduplicated, never e-mail addresses).
function todaysHandles(upTracks, today) {
  const days = isObj(upTracks) && Array.isArray(upTracks.days) ? upTracks.days : [];
  const seen = new Map();
  for (const d of days) {
    if (!d || d.date !== today || d.status !== 'success') continue;
    const h = typeof d.handle === 'string' ? d.handle.trim().replace(/^@/, '') : '';
    if (!HANDLE_RE.test(h) || seen.has(h.toLowerCase())) continue;
    seen.set(h.toLowerCase(), { handle: h, address: d.address || null });
  }
  return [...seen.values()];
}

// ---------------------------------------------------------------------------
// follower column: normalisation + compare-and-set updates
// ---------------------------------------------------------------------------
// A document of another week (or the default []) is treated as a fresh week; the lock survives.
function normalizeFollower(raw, weekStart) {
  if (isObj(raw) && raw.week_start === weekStart && Array.isArray(raw.runs)) return JSON.parse(JSON.stringify(raw));
  const h = { week_start: weekStart, runs: [] };
  if (isObj(raw) && raw.lock) h.lock = raw.lock;
  return h;
}

function followedSet(raw, weekStart) {
  const s = new Set();
  if (isObj(raw) && raw.week_start === weekStart && Array.isArray(raw.runs)) {
    for (const r of raw.runs) for (const t of (r.targets || [])) s.add(String(t.handle).toLowerCase());
  }
  return s;
}

let HELD_LOCK = null; // while set, writes to the coordinator row require that we still own the lock

// Read-modify-write of one account's follower document. The write only lands if the document is
// still at the version we read ("v"); otherwise it is re-read and re-applied, so concurrent
// executions can never overwrite each other. mutate() returns false to skip the write.
async function updateFollower(sb, address, weekStart, mutate) {
  let lastErr;
  for (let attempt = 1; attempt <= 8; attempt++) {
    try {
      const { data, error } = await sb.from('audius').select('address, follower').eq('address', address).limit(1);
      if (error) throw new Error(error.message);
      if (!data || !data[0]) throw Object.assign(new Error(`account row not found: ${address}`), { fatal: true });
      const raw = data[0].follower;
      const hist = normalizeFollower(raw, weekStart);
      if (address === TARGET_ADDRESS && HELD_LOCK && !(hist.lock && hist.lock.token === HELD_LOCK.token)) {
        throw new LockLostError('batch lock was taken over by another run');
      }
      if (mutate(hist) === false) return hist;
      const prevV = isObj(raw) && Number.isInteger(raw.v) ? raw.v : null;
      hist.v = (prevV || 0) + 1;
      let q = sb.from('audius').update({ follower: hist }).eq('address', address);
      q = prevV === null ? q.is('follower->>v', null) : q.eq('follower->>v', String(prevV));
      const res = await q.select('address, follower');
      if (res.error) throw new Error(res.error.message);
      if (res.data && res.data.length === 1) return normalizeFollower(res.data[0].follower, weekStart);
      lastErr = new Error(`update conflict for ${address}`); // someone else wrote first: retry on fresh data
      await sleep(100 + Math.random() * 300);
    } catch (e) {
      if (e instanceof LockLostError || e.fatal) throw e;
      lastErr = e;
      await sleep(CFG.retryBaseMs * Math.min(attempt, 3));
    }
  }
  throw lastErr;
}

// Append one successful follow to the follower's own history and verify what the database stored.
async function recordFollow(sb, weekStart, address, f) {
  const key = f.handle.toLowerCase();
  const has = h => h.runs.some(r => (r.targets || []).some(t => String(t.handle).toLowerCase() === key));
  const stored = await updateFollower(sb, address, weekStart, h => {
    if (has(h)) return false; // never record (or follow) the same handle twice
    let run = h.runs.find(r => r.date === f.date && r.run_number === f.run_number);
    if (!run) { run = { date: f.date, day: f.day, run_number: f.run_number, targets: [] }; h.runs.push(run); }
    run.targets.push({ handle: f.handle, followed_at: f.at });
    return true;
  });
  if (!has(stored)) throw new Error(`database verification failed for ${address} -> ${f.handle}`);
}

// ---------------------------------------------------------------------------
// Local recovery journal: verified follows whose database save failed
// ---------------------------------------------------------------------------
let PENDING = [];
function loadPending() {
  try { const j = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); return Array.isArray(j.pending) ? j.pending : []; } catch (_) { return []; }
}
function savePending() {
  const tmp = `${STATE_FILE}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify({ pending: PENDING }));
  fs.renameSync(tmp, STATE_FILE);
}
async function flushPending(sb, ctx) {
  if (!PENDING.length) return;
  const keep = [];
  for (const p of PENDING) {
    if (p.week_start !== ctx.weekStart) continue; // that week's history no longer exists
    try { await recordFollow(sb, ctx.weekStart, p.address, p); } catch (_) { keep.push(p); }
  }
  log(`[+] Recovered ${PENDING.length - keep.length}/${PENDING.length} unsaved follow(s) from the local journal.`);
  PENDING = keep;
  savePending();
}

// ---------------------------------------------------------------------------
// Batch lock (stored in the coordinator's follower document)
// ---------------------------------------------------------------------------
function lockStaleReason(lock) {
  if (!(Date.parse(lock.expires_at) > Date.now())) return 'it expired';
  if (lock.pid && lock.host === os.hostname()) {
    try { process.kill(lock.pid, 0); } catch (e) { if (e.code !== 'EPERM') return 'its owner process is no longer running'; }
  }
  return null;
}

async function acquireLock(sb, ctx) {
  const token = crypto.randomUUID();
  let blocked = null;
  await updateFollower(sb, TARGET_ADDRESS, ctx.weekStart, h => {
    blocked = null;
    if (h.lock && h.lock.token) {
      const why = lockStaleReason(h.lock);
      if (!why) { blocked = h.lock; return false; }
      log(`[~] Taking over batch lock from ${h.lock.acquired_at}: ${why}.`);
    }
    h.lock = {
      token, acquired_at: new Date().toISOString(), expires_at: new Date(Date.now() + CFG.lockTtlMs).toISOString(),
      pid: process.pid, host: os.hostname(),
    };
    return true;
  });
  if (blocked) { log(`[!] Another follower run holds the batch lock (since ${blocked.acquired_at}). Exiting.`); return null; }
  return { token };
}

const renewLock = (sb, ctx) => updateFollower(sb, TARGET_ADDRESS, ctx.weekStart, h => {
  h.lock.expires_at = new Date(Date.now() + CFG.lockTtlMs).toISOString();
  return true;
});

async function releaseLock(sb, ctx, lock) {
  if (!lock) return;
  try {
    await updateFollower(sb, TARGET_ADDRESS, ctx.weekStart, h => {
      if (!h.lock || h.lock.token !== lock.token) return false;
      delete h.lock;
      return true;
    });
  } catch (e) {
    console.error(`[!] Could not release batch lock (it will expire on its own): ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Audius helpers
// ---------------------------------------------------------------------------
const hasCreds = r => !!(r.user_id && r.app_data && r.app_data.api_key && r.app_data.api_secret && r.app_data.bearer_token);

function makeSdk(acct) {
  return sdkFactory({
    apiKey: acct.app_data.api_key,
    apiSecret: acct.app_data.api_secret,
    bearerToken: acct.app_data.bearer_token,
  });
}

async function resolveTargets(published, sdk) {
  const targets = [];
  let unresolved = 0; // transient failures: the run must not be marked complete
  for (const t of published) {
    try {
      const res = await readRetry(() => sdk.users.getUserByHandle({ handle: t.handle }));
      const id = res && res.data && res.data.id;
      if (id) targets.push({ ...t, id: String(id), key: t.handle.toLowerCase() });
      else log(`[!] @${t.handle} was not found on Audius; skipping it.`);
    } catch (e) {
      if (classify(e) === 'permanent') log(`[!] @${t.handle} was not found on Audius; skipping it.`);
      else { unresolved++; log(`[!] Could not look up @${t.handle} (${errMsg(e)}); it will be retried on the next run.`); }
    }
  }
  return { targets, unresolved };
}

// Does followerId currently follow targetId? Uses the relationship flag Audius returns for the
// requesting user; falls back to scanning the follower's "following" list.
async function isFollowing(sdk, followerId, targetId) {
  const res = await readRetry(() => sdk.users.getUser({ id: targetId, userId: followerId }));
  const d = res && res.data;
  const flag = d && (d.doesCurrentUserFollow !== undefined ? d.doesCurrentUserFollow : d.does_current_user_follow);
  if (typeof flag === 'boolean') return flag;
  for (let page = 0; page < CFG.scanPages; page++) {
    const r = await readRetry(() => sdk.users.getFollowing({ id: followerId, limit: 100, offset: page * 100 }));
    const list = (r && r.data) || [];
    if (list.some(u => String(u.id) === String(targetId))) return true;
    if (list.length < 100) break;
  }
  return false;
}

async function verifyFollow(sdk, followerId, targetId) {
  for (let i = 1; i <= CFG.verifyTries; i++) {
    await sleep(CFG.verifyDelayMs);
    if (await isFollowing(sdk, followerId, targetId)) return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Selection
// ---------------------------------------------------------------------------
function selectAccounts(accounts, targets, targetAddrs, usedToday, need) {
  const ids = new Set(targets.map(t => t.id));
  const keys = new Set(targets.map(t => t.key));
  const eligible = accounts.filter(a =>
    a.hasCreds && !targetAddrs.has(a.address) && !ids.has(String(a.user_id)) &&
    !(a.handle && keys.has(a.handle.toLowerCase())) &&
    targets.some(t => !a.followed.has(t.key))); // still has something left to follow
  const fresh = eligible.filter(a => !usedToday.has(a.address));
  const used = eligible.filter(a => usedToday.has(a.address));
  return [...shuffle(fresh), ...shuffle(used)].slice(0, need);
}

function dayState(coord, today) {
  for (let n = 1; n <= CFG.runsPerDay; n++) {
    const b = (coord.batches || []).find(x => x.date === today && x.run_number === n);
    if (!b || !b.done) return { runNumber: n, batch: b || null };
  }
  return null; // every run of the day is completed
}

// ---------------------------------------------------------------------------
// One follow job = one follower account + one target handle
// ---------------------------------------------------------------------------
async function followJob(sb, ctx, run, acct, target) {
  const st = run.stats;
  if (run.abort) return;
  if (run.cool.has(acct.address)) { st.retry++; return; } // rate-limited earlier: leave for the next run
  const label = acct.handle || acct.address;
  let verified = false;
  try {
    let sdk = run.sdks.get(acct.address);
    if (!sdk) { sdk = makeSdk(acct); run.sdks.set(acct.address, sdk); }

    // Already following (e.g. a follow whose save failed earlier)? Then never follow again, just record it.
    let already = await isFollowing(sdk, acct.user_id, target.id);
    if (!already) {
      let followErr = null;
      try { await sdk.users.followUser({ userId: acct.user_id, followeeUserId: target.id }); } catch (e) {
        if (isRateLimit(e)) throw e;
        followErr = e; // may still have landed: verify below
      }
      if (!(await verifyFollow(sdk, acct.user_id, target.id))) throw followErr || new Error('follow could not be verified');
    }
    verified = true;
    already ? st.already++ : st.ok++;
  } catch (e) {
    const kind = classify(e);
    if (kind === 'rate') {
      run.cool.add(acct.address);
      st.rate++;
      log(`[!] Rate limit encountered: ${label} -> ${target.handle}. Skipping for now.`);
    } else {
      st[kind === 'permanent' ? 'perm' : 'retry']++;
      if (st.logged++ < 10) log(`[!] ${label} -> ${target.handle}: ${errMsg(e)}`);
    }
  }
  if (verified) {
    const item = {
      address: acct.address, handle: target.handle, date: ctx.today, day: ctx.dayName, run_number: run.number,
      week_start: ctx.weekStart, at: new Date().toISOString(),
    };
    try { await recordFollow(sb, ctx.weekStart, acct.address, item); } catch (e) {
      PENDING.push(item); savePending(); // the follow happened: keep enough to record it without following again
      if (e instanceof LockLostError) run.abort = e;
      else log(`[!] ${label} -> ${target.handle}: followed, but the database save failed; kept for recovery.`);
    }
    acct.followed.add(target.key);
  }
  if (CFG.followDelayMs) await sleep(CFG.followDelayMs);
}

// ---------------------------------------------------------------------------
// One batch
// ---------------------------------------------------------------------------
async function processRun(sb, ctx, rows, published, coord, { runNumber, batch }) {
  const accounts = rows.map(r => ({
    address: r.address, handle: typeof r.handle === 'string' ? r.handle.trim().replace(/^@/, '') : null,
    user_id: r.user_id, app_data: r.app_data, hasCreds: hasCreds(r), followed: followedSet(r.follower, ctx.weekStart),
  }));
  const byAddr = new Map(accounts.map(a => [a.address, a]));
  const reader = accounts.find(a => a.hasCreds);
  if (!reader) throw new Error('no account with complete credentials');

  const { targets, unresolved } = await resolveTargets(published, makeSdk(reader));
  if (!targets.length) { log("[!] None of today's handles could be resolved on Audius. Exiting."); return { status: 'NO_TARGETS' }; }

  let members;
  if (batch) {
    members = batch.accounts.map(a => byAddr.get(a)).filter(a => a && a.hasCreds);
    log(`[*] Resuming run ${runNumber}: ${members.length} account(s), ${targets.length} handle(s).`);
  } else {
    const targetAddrs = new Set(published.map(p => p.address).filter(Boolean));
    const usedToday = new Set();
    for (const b of coord.batches || []) if (b.date === ctx.today) b.accounts.forEach(a => usedToday.add(a));
    members = selectAccounts(accounts, targets, targetAddrs, usedToday, CFG.accountsPerRun);
    log(`[*] Run ${runNumber}: ${members.length} follower account(s) selected, ${targets.length} handle(s) to follow.`);
    if (members.length < CFG.accountsPerRun) log(`[~] Only ${members.length} eligible account(s) available (wanted ${CFG.accountsPerRun}).`);
    await updateFollower(sb, TARGET_ADDRESS, ctx.weekStart, h => {
      if (!h.batches) h.batches = [];
      h.batches.push({ date: ctx.today, run_number: runNumber, accounts: members.map(m => m.address), done: members.length === 0 && !unresolved });
      return true;
    });
  }

  const run = { number: runNumber, cool: new Set(), abort: null, sdks: new Map(), stats: { ok: 0, already: 0, rate: 0, retry: 0, perm: 0, logged: 0 } };
  const st = run.stats;

  // One handle at a time: every selected account follows handle #1, then handle #2, ... (spreads the load).
  for (const t of targets) {
    const todo = members.filter(a => !a.followed.has(t.key));
    if (!todo.length) continue;
    await renewLock(sb, ctx);
    const before = st.ok + st.already;
    await runPool(todo, CFG.concurrency, a => followJob(sb, ctx, run, a, t));
    if (run.abort) throw run.abort;
    log(`[*] @${t.handle}: ${st.ok + st.already - before}/${todo.length} followed.`);
  }

  const left = st.rate + st.retry + unresolved; // work that a later run should pick up
  if (!left) {
    await updateFollower(sb, TARGET_ADDRESS, ctx.weekStart, h => {
      const b = (h.batches || []).find(x => x.date === ctx.today && x.run_number === runNumber);
      if (!b || b.done) return false;
      b.done = true;
      return true;
    });
  }
  log(`[+] Run ${runNumber}: ${st.ok} followed, ${st.already} already following, ${st.rate} rate-limited, ${st.retry} to retry, ${st.perm} failed. ` +
    (left ? 'INCOMPLETE (the next execution resumes this run).' : 'COMPLETED.'));
  return { status: left ? 'INCOMPLETE' : 'COMPLETED' };
}

// ---------------------------------------------------------------------------
// Main flow
// ---------------------------------------------------------------------------
async function runFollower({ now = new Date() } = {}) {
  const ctx = buildContext(now);
  const sb = connectSupabase();
  PENDING = loadPending();

  // 1. Today's successfully published handles (read-only).
  const published = todaysHandles(await readUpTracks(sb), ctx.today);
  if (!published.length) {
    log('[!] No successfully published handles found for today. Exiting.');
    return { status: 'NO_TARGETS' };
  }

  // 2. Cheap completion check before taking the lock.
  let rows = await fetchRows(sb);
  const coordRaw = rows.find(r => r.address === TARGET_ADDRESS).follower;
  if (isObj(coordRaw) && coordRaw.week_start && coordRaw.week_start !== ctx.weekStart) {
    log(`[*] New week ${ctx.weekStart}: follower history reset.`);
  }
  if (!dayState(normalizeFollower(coordRaw, ctx.weekStart), ctx.today)) {
    log(BOTH_DONE_MSG);
    return { status: 'ALREADY_COMPLETED' };
  }

  // 3. Lock (this also writes the fresh weekly document when the week changed).
  const lock = await acquireLock(sb, ctx);
  if (!lock) return { status: 'LOCKED' };
  HELD_LOCK = lock;

  try {
    await flushPending(sb, ctx);
    rows = await fetchRows(sb);
    const coord = normalizeFollower(rows.find(r => r.address === TARGET_ADDRESS).follower, ctx.weekStart);
    const state = dayState(coord, ctx.today); // re-check under the lock
    if (!state) {
      log(BOTH_DONE_MSG);
      return { status: 'ALREADY_COMPLETED' };
    }
    return await processRun(sb, ctx, rows, published, coord, state);
  } finally {
    HELD_LOCK = null;
    await releaseLock(sb, ctx, lock);
  }
}

if (require.main === module) {
  runFollower().catch(err => {
    console.error('[!] Fatal error:', err.message || err);
    process.exit(1);
  });
}

module.exports = { runFollower, todaysHandles, buildContext };
