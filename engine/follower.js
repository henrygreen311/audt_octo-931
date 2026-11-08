#!/usr/bin/env node
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');

const PROXY_URLS = (process.env.AUDIUS_PROXY_URLS || 'https://audius.qring99.workers.dev,https://audius.qringgreen.workers.dev')
  .split(',').map(x => x.trim().replace(/\/+$/, '')).filter(Boolean);
const PROXY_KEY = process.env.AUDIUS_PROXY_KEY || '';
{
  const realFetch = globalThis.fetch;
  const CONNECT_ERRS = new Set(['ETIMEDOUT', 'ENETUNREACH', 'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'UND_ERR_CONNECT_TIMEOUT']);
  const downUntil = new Map();
  const warned = new Set();
  const ATTEMPTS = Number(process.env.PROXY_ATTEMPTS || 6);
  const pick = () => {
    const now = Date.now();
    const up = PROXY_URLS.filter(u => (downUntil.get(u) || 0) <= now);
    const pool = up.length ? up : PROXY_URLS;
    return pool[Math.floor(Math.random() * pool.length)];
  };
  console.log(`[*] Route: Cloudflare proxy (${PROXY_URLS.length})`);
  globalThis.fetch = async (input, init) => {
    const isReq = typeof input === 'object' && input !== null && 'url' in input;
    let url;
    try { url = new URL(isReq ? input.url : String(input)); } catch (_) { return realFetch(input, init); }
    if (url.hostname !== 'api.audius.co') return realFetch(input, init);
    const headers = new Headers((init && init.headers) || (isReq ? input.headers : undefined));
    if (PROXY_KEY) headers.set('x-proxy-key', PROXY_KEY);
    let lastErr;
    for (let attempt = 1; attempt <= ATTEMPTS; attempt++) {
      const proxy = pick();
      const target = proxy + url.pathname + url.search;
      try {
        return isReq && !init
          ? await realFetch(new Request(target, input.clone()), { headers })
          : await realFetch(target, { ...init, headers });
      } catch (e) {
        const code = e && ((e.cause && e.cause.code) || e.code);
        if (!CONNECT_ERRS.has(code)) throw e;
        lastErr = e;
        downUntil.set(proxy, Date.now() + 20000);
        if (!warned.has(proxy)) { warned.add(proxy); console.log(`[!] Proxy ${new URL(proxy).host} unreachable; using another.`); }
        await new Promise(r => setTimeout(r, 300 * attempt));
      }
    }
    throw lastErr;
  };
}

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

const log = console.log.bind(console);
const VERBOSE = /^(1|true|yes)$/i.test(process.env.VERBOSE || '') || process.argv.includes('--verbose') || process.argv.includes('-v');
let lastBgLog = 0;
process.on('unhandledRejection', e => {
  if (Date.now() - lastBgLog < 30000) return;
  lastBgLog = Date.now();
  const c = e && e.cause && e.cause.code;
  log(`[!] Background network error${c ? ` (${c})` : ''}; continuing.`);
});
for (const level of ['log', 'info', 'debug', 'warn']) {
  const orig = console[level].bind(console);
  console[level] = (...args) => {
    if (typeof args[0] === 'string' && args[0].startsWith('[audius-sdk]')) return;
    orig(...args);
  };
}

const ROOT_DIR = path.resolve(__dirname, '..');
const DB_FILE = path.join(ROOT_DIR, 'db.txt');
const STATE_FILE = path.join(ROOT_DIR, '.audius_follower_state.json');
const TARGET_ADDRESS = 'qringgreen@gmail.com';

const CFG = {
  timezone: process.env.PUBLISH_TIMEZONE || 'UTC',
  accountsPerRun: 50,
  dailyPerHandle: Number(process.env.DAILY_FOLLOWERS_PER_HANDLE || 100),
  runsPerDay: 2,
  lockTtlMs: Number(process.env.LOCK_TTL_MINUTES || 120) * 60000,
  lockStaleMs: Number(process.env.LOCK_STALE_SECONDS || 120) * 1000,
  retries: Number(process.env.RETRY_ATTEMPTS || 3),
  retryBaseMs: Number(process.env.RETRY_BASE_MS || 2000),
  verifyTries: 3,
  verifyDelayMs: Number(process.env.VERIFY_DELAY_MS || 500),
  failStopAfter: Number(process.env.FAIL_STOP_AFTER || 20),
  followDelayMs: Number(process.env.FOLLOW_DELAY_MS || 100),
  concurrency: Number(process.env.CONCURRENCY || 5),
  callTimeoutMs: Number(process.env.CALL_TIMEOUT_SECONDS || 20) * 1000,
  maxRunMs: Number(process.env.MAX_RUN_MINUTES || 0) * 60000,
  scanPages: Number(process.env.FOLLOWING_SCAN_PAGES || 10),
};

const HANDLE_RE = /^[A-Za-z0-9_.]{1,30}$/;

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
function withTimeout(promise, ms = CFG.callTimeoutMs, what = 'request') {
  let t;
  const timeout = new Promise((_, rej) => { t = setTimeout(() => rej(new Error(`timeout: ${what} took longer than ${ms / 1000}s`)), ms); });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}
const readRetry = fn => withRetry(() => withTimeout(fn()), e => !isRateLimit(e) && isTransient(e));

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

async function readUpTracks(sb) {
  const { data, error } = await sb.from('audius').select('up_tracks').eq('address', TARGET_ADDRESS).limit(1);
  if (error) throw new Error(`could not read up_tracks: ${error.message}`);
  return data && data[0] ? data[0].up_tracks : null;
}

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

let HELD_LOCK = null;

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
      lastErr = new Error(`update conflict for ${address}`);
      await sleep(100 + Math.random() * 300);
    } catch (e) {
      if (e instanceof LockLostError || e.fatal) throw e;
      lastErr = e;
      await sleep(CFG.retryBaseMs * Math.min(attempt, 3));
    }
  }
  throw lastErr;
}

async function recordFollow(sb, weekStart, address, f) {
  const key = f.handle.toLowerCase();
  const has = h => h.runs.some(r => (r.targets || []).some(t => String(t.handle).toLowerCase() === key));
  const stored = await updateFollower(sb, address, weekStart, h => {
    if (has(h)) return false;
    let run = h.runs.find(r => r.date === f.date && r.run_number === f.run_number);
    if (!run) { run = { date: f.date, day: f.day, run_number: f.run_number, targets: [] }; h.runs.push(run); }
    run.targets.push({ handle: f.handle, followed_at: f.at });
    return true;
  });
  if (!has(stored)) throw new Error(`database verification failed for ${address} -> ${f.handle}`);
}

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
    if (p.week_start !== ctx.weekStart) continue;
    try { await recordFollow(sb, ctx.weekStart, p.address, p); } catch (_) { keep.push(p); }
  }
  log(`[*] Recovered ${PENDING.length - keep.length}/${PENDING.length} unsaved follows.`);
  PENDING = keep;
  savePending();
}

function lockStaleReason(lock) {
  if (!(Date.parse(lock.expires_at) > Date.now())) return 'it expired';
  const beat = Date.parse(lock.beat || lock.acquired_at);
  if (!Number.isFinite(beat) || Date.now() - beat > CFG.lockStaleMs) return 'its owner stopped responding (cancelled run)';
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
      log('[*] Took over lock from a stopped run.');
    }
    h.lock = {
      token, acquired_at: new Date().toISOString(), beat: new Date().toISOString(),
      expires_at: new Date(Date.now() + CFG.lockTtlMs).toISOString(), pid: process.pid, host: os.hostname(),
    };
    return true;
  });
  if (blocked) { log('[!] Another run is active. Exiting.'); return null; }
  return { token };
}

let LOCK_LOST = null;

function startHeartbeat(sb, ctx) {
  let busy = false;
  const timer = setInterval(async () => {
    if (busy || LOCK_LOST) return;
    busy = true;
    try {
      await updateFollower(sb, TARGET_ADDRESS, ctx.weekStart, h => {
        h.lock.beat = new Date().toISOString();
        h.lock.expires_at = new Date(Date.now() + CFG.lockTtlMs).toISOString();
        return true;
      });
    } catch (e) { if (e instanceof LockLostError) LOCK_LOST = e; } finally { busy = false; }
  }, Math.max(5000, CFG.lockStaleMs / 4));
  timer.unref();
  return () => clearInterval(timer);
}

async function releaseLock(sb, ctx, lock) {
  if (!lock) return;
  try {
    await updateFollower(sb, TARGET_ADDRESS, ctx.weekStart, h => {
      if (!h.lock || h.lock.token !== lock.token) return false;
      delete h.lock;
      return true;
    });
  } catch (e) {
    console.error(`[!] Could not release lock (it expires on its own): ${e.message}`);
  }
}

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
  let unresolved = 0;
  for (const t of published) {
    try {
      const res = await readRetry(() => sdk.users.getUserByHandle({ handle: t.handle }));
      const id = res && res.data && res.data.id;
      if (id) targets.push({ ...t, id: String(id), key: t.handle.toLowerCase() });
      else log(`[!] @${t.handle} not found; skipped.`);
    } catch (e) {
      if (classify(e) === 'permanent') log(`[!] @${t.handle} not found; skipped.`);
      else { unresolved++; log(`[!] @${t.handle} lookup failed (${errMsg(e)}); retry next run.`); }
    }
  }
  return { targets, unresolved };
}

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
    await sleep(CFG.verifyDelayMs * i);
    if (await isFollowing(sdk, followerId, targetId)) return true;
  }
  return false;
}

function handlesBelowCap(rows, published, ctx) {
  const today = countHandles(rows, ctx);
  return published.filter(p => (today.get(p.handle.toLowerCase()) || 0) < CFG.dailyPerHandle);
}

function allDoneMsg(n) {
  return `[*] All ${n} handles reached ${CFG.dailyPerHandle} followers today. Exiting.`;
}

function dayState(coord, today) {
  for (let n = 1; n <= CFG.runsPerDay; n++) {
    const b = (coord.batches || []).find(x => x.date === today && x.run_number === n);
    if (!b || !b.done) return { runNumber: n, batch: b || null };
  }
  return null;
}

const FOLLOW_STYLES = [
  (f, t) => ({ id: t, userId: f, followeeUserId: t }),
  (f, t) => ({ id: f, userId: f, followeeUserId: t }),
];

function reason(e) {
  if (isRateLimit(e)) return '429';
  const code = statusOf(e);
  if (code) return `HTTP ${code}`;
  const m = String((e && e.message) || e);
  if (/timeout|timed out/i.test(m)) return 'timeout';
  return m.slice(0, 50);
}

async function followJob(sb, ctx, run, acct, target) {
  const st = run.stats;
  const who = acct.handle ? `@${acct.handle}` : acct.address;
  const say = text => { if (VERBOSE) log(`[*] ${who} -> @${target.handle} | ${text}`); };
  let verified = false;
  let already = false;
  let outcome;
  try {
    let sdk = run.sdks.get(acct.address);
    if (!sdk) { sdk = makeSdk(acct); run.sdks.set(acct.address, sdk); }

    already = await isFollowing(sdk, acct.user_id, target.id);
    if (!already) {
      let lastErr = null;
      for (const i of run.style === null ? [0, 1] : [run.style]) {
        lastErr = null;
        try { await withTimeout(sdk.users.followUser(FOLLOW_STYLES[i](acct.user_id, target.id)), CFG.callTimeoutMs, 'followUser'); } catch (e) {
          if (isRateLimit(e)) throw e;
          lastErr = e;
          if (!statusOf(e) && /required parameter|invalid/i.test(String(e.message))) continue;
        }
        if (await verifyFollow(sdk, acct.user_id, target.id)) {
          if (run.style === null) run.style = i;
          lastErr = null;
          verified = true;
          break;
        }
      }
      if (!verified) throw lastErr || new Error('follow could not be verified');
    }
    verified = true;
    run.consecFail = 0;
    outcome = already ? 'already' : 'ok';
    already ? st.already++ : st.ok++;
  } catch (e) {
    const kind = classify(e);
    if (kind === 'rate') { outcome = 'rate'; st.rate++; say('429'); }
    else if (kind === 'permanent') { outcome = 'perm'; st.perm++; run.cool.add(acct.address); say(reason(e)); }
    else { outcome = 'retry'; st.retry++; say(reason(e)); }
    if (++run.consecFail >= CFG.failStopAfter && !run.stop) {
      run.stop = true;
      log('[!] Too many failures in a row; stopping. Next run resumes.');
    }
  }
  if (verified) {
    const item = {
      address: acct.address, handle: target.handle, date: ctx.today, day: ctx.dayName, run_number: run.number,
      week_start: ctx.weekStart, at: new Date().toISOString(),
    };
    let saved = true;
    try { await recordFollow(sb, ctx.weekStart, acct.address, item); } catch (e) {
      saved = false;
      PENDING.push(item); savePending();
      if (e instanceof LockLostError) run.abort = e;
    }
    acct.followed.add(target.key);
    acct.today++;
    if (!saved) log(`[!] ${who} -> @${target.handle} | followed, DB save failed (kept locally)`);
    else say(already ? 'already following' : 'successful');
  }
  if (CFG.followDelayMs) await sleep(CFG.followDelayMs);
  return outcome;
}

async function fillHandle(sb, ctx, run, target, need, candidates) {
  let got = 0;
  let inflight = 0;
  let transient = 0;
  let skipped = 0;
  const step = async () => {
    if (run.abort || LOCK_LOST || run.stop) return false;
    if (CFG.maxRunMs && Date.now() - run.startedAt > CFG.maxRunMs) {
      run.stop = true;
      log('[!] Time limit reached; stopping. Next run resumes.');
      return false;
    }
    if (got >= need) return false;
    if (got + inflight >= need) {
      if (!inflight) return false;
      await sleep(50);
      return true;
    }
    const acct = candidates.shift();
    if (!acct) return false;
    inflight++;
    const outcome = await followJob(sb, ctx, run, acct, target);
    inflight--;
    if (outcome === 'ok' || outcome === 'already') got++;
    else skipped++;
    if (outcome === 'rate' || outcome === 'retry') transient++;
    return true;
  };
  for (let n = 0; n < 3 && run.style === null; n++) if (!(await step())) break;
  await Promise.all(Array.from({ length: Math.max(1, CFG.concurrency) }, async () => { while (await step()); }));
  return { got, transient, skipped };
}

function countToday(raw, ctx) {
  let n = 0;
  if (isObj(raw) && raw.week_start === ctx.weekStart && Array.isArray(raw.runs)) {
    for (const r of raw.runs) if (r.date === ctx.today) n += (r.targets || []).length;
  }
  return n;
}

function countHandles(rows, ctx, runNumber) {
  const counts = new Map();
  for (const r of rows) {
    const f = r.follower;
    if (!isObj(f) || f.week_start !== ctx.weekStart || !Array.isArray(f.runs)) continue;
    for (const x of f.runs) {
      if (x.date !== ctx.today || (runNumber !== undefined && x.run_number !== runNumber)) continue;
      for (const t of x.targets || []) {
        const k = String(t.handle).toLowerCase();
        counts.set(k, (counts.get(k) || 0) + 1);
      }
    }
  }
  return counts;
}

async function processRun(sb, ctx, rows, published, { runNumber, batch }) {
  const accounts = rows.map(r => ({
    address: r.address, handle: typeof r.handle === 'string' ? r.handle.trim().replace(/^@/, '') : null,
    user_id: r.user_id, app_data: r.app_data, hasCreds: hasCreds(r), followed: followedSet(r.follower, ctx.weekStart),
    today: countToday(r.follower, ctx),
  }));
  const reader = accounts.find(a => a.hasCreds);
  if (!reader) throw new Error('no account with complete credentials');

  const { targets, unresolved } = await resolveTargets(published, makeSdk(reader));
  if (!targets.length) { log('[!] No handles found on Audius. Exiting.'); return { status: 'NO_TARGETS' }; }

  const todayDone = countHandles(rows, ctx);
  const runDone = countHandles(rows, ctx, runNumber);
  const lastRun = runNumber >= CFG.runsPerDay;
  const need = new Map(targets.map(t => {
    const left = Math.max(0, CFG.dailyPerHandle - (todayDone.get(t.key) || 0));
    const perRun = lastRun ? left : Math.min(left, Math.max(0, CFG.accountsPerRun - (runDone.get(t.key) || 0)));
    return [t.key, perRun];
  }));
  let totalNeed = 0;
  let active = 0;
  for (const n of need.values()) { totalNeed += n; if (n) active++; }
  if (!totalNeed && !unresolved) {
    log(allDoneMsg(targets.length));
    return { status: 'ALREADY_COMPLETED' };
  }
  const atCap = targets.filter(t => (todayDone.get(t.key) || 0) >= CFG.dailyPerHandle).length;
  log(`[*] Run ${runNumber}${batch ? ' (resumed)' : ''}: ${active} handles to do${atCap ? ` (${atCap} already at ${CFG.dailyPerHandle})` : ''}, ${totalNeed} follows.`);

  if (!batch) {
    await updateFollower(sb, TARGET_ADDRESS, ctx.weekStart, h => {
      if (!h.batches) h.batches = [];
      if (h.batches.some(b => b.date === ctx.today && b.run_number === runNumber)) return false;
      h.batches.push({ date: ctx.today, run_number: runNumber, done: false });
      return true;
    });
  }

  const run = { startedAt: Date.now(), number: runNumber, cool: new Set(), abort: null, stop: false, style: null, consecFail: 0, sdks: new Map(), stats: { ok: 0, already: 0, rate: 0, retry: 0, perm: 0 } };
  const st = run.stats;
  let incomplete = false;

  for (const t of targets) {
    const want = need.get(t.key);
    if (!want) continue;
    if (run.stop) { incomplete = true; continue; }
    const pool = accounts.filter(a => a.hasCreds && String(a.user_id) !== String(t.id) && !(a.handle && a.handle.toLowerCase() === t.key) && !a.followed.has(t.key) && !run.cool.has(a.address));
    const candidates = shuffle(pool).sort((x, y) => x.today - y.today);
    const { got, transient, skipped } = await fillHandle(sb, ctx, run, t, want, candidates);
    if (run.abort || LOCK_LOST) throw run.abort || LOCK_LOST;
    const day = (todayDone.get(t.key) || 0) + got;
    if (got < want) {
      if (transient || run.stop) incomplete = true;
      log(`[!] @${t.handle} | ${day}/${CFG.dailyPerHandle} today${transient || run.stop ? '' : ' (no more accounts available)'}`);
    } else {
      log(`[*] @${t.handle} | ${day}/${CFG.dailyPerHandle} today${day >= CFG.dailyPerHandle ? ' (done)' : ''}${skipped ? ` (${skipped} skipped)` : ''}`);
    }
  }

  const left = incomplete || unresolved > 0;
  if (!left) {
    await updateFollower(sb, TARGET_ADDRESS, ctx.weekStart, h => {
      const b = (h.batches || []).find(x => x.date === ctx.today && x.run_number === runNumber);
      if (!b || b.done) return false;
      b.done = true;
      return true;
    });
  }
  const parts = [`${st.ok} followed`];
  if (st.already) parts.push(`${st.already} already following`);
  if (st.rate) parts.push(`${st.rate} rate-limited`);
  if (st.retry) parts.push(`${st.retry} errors`);
  if (st.perm) parts.push(`${st.perm} failed`);
  if (unresolved) parts.push(`${unresolved} handle(s) unresolved`);
  log(`[*] Run ${runNumber} ${left ? 'incomplete' : 'complete'}: ${parts.join(', ')}.${left ? ' Next run resumes.' : ''}`);
  return { status: left ? 'INCOMPLETE' : 'COMPLETED' };
}

async function runFollower({ now = new Date() } = {}) {
  const ctx = buildContext(now);
  const sb = connectSupabase();
  PENDING = loadPending();

  const published = todaysHandles(await readUpTracks(sb), ctx.today);
  if (!published.length) {
    log('[!] No handles published today. Exiting.');
    return { status: 'NO_TARGETS' };
  }

  let rows = await fetchRows(sb);
  const coordRaw = rows.find(r => r.address === TARGET_ADDRESS).follower;
  if (isObj(coordRaw) && coordRaw.week_start && coordRaw.week_start !== ctx.weekStart) {
    log('[*] New week: history reset.');
  }
  if (!handlesBelowCap(rows, published, ctx).length) {
    log(allDoneMsg(published.length));
    return { status: 'ALREADY_COMPLETED' };
  }
  if (!dayState(normalizeFollower(coordRaw, ctx.weekStart), ctx.today)) {
    log('[*] Both runs already done today. Exiting.');
    return { status: 'ALREADY_COMPLETED' };
  }

  const lock = await acquireLock(sb, ctx);
  if (!lock) return { status: 'LOCKED' };
  HELD_LOCK = lock;
  LOCK_LOST = null;
  const stopHeartbeat = startHeartbeat(sb, ctx);

  try {
    await flushPending(sb, ctx);
    rows = await fetchRows(sb);
    const coord = normalizeFollower(rows.find(r => r.address === TARGET_ADDRESS).follower, ctx.weekStart);
    if (!handlesBelowCap(rows, published, ctx).length) {
      log(allDoneMsg(published.length));
      return { status: 'ALREADY_COMPLETED' };
    }
    const state = dayState(coord, ctx.today);
    if (!state) {
      log('[*] Both runs already done today. Exiting.');
      return { status: 'ALREADY_COMPLETED' };
    }
    return await processRun(sb, ctx, rows, published, state);
  } finally {
    stopHeartbeat();
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

module.exports = { runFollower, todaysHandles, buildContext, CFG };
