#!/usr/bin/env node
'use strict';

const STREAM_DURATION_MS = 60000;

const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');

const { AsyncLocalStorage } = require('async_hooks');

const DEFAULT_PROXIES = [
  'https://audius-v2.qringgreen.workers.dev',
  'https://audius-v2.qring99.workers.dev',
  'https://audius.henrygreen311.workers.dev',
  'https://audius-v2.henrygreen311.workers.dev',
  'https://audius.qring99.workers.dev',
  'https://audius.qringgreen.workers.dev',
];
const PROXY_URLS = [...new Set(
  (process.env.AUDIUS_PROXY_URLS || DEFAULT_PROXIES.join(','))
    .split(',').map(x => x.trim().replace(/\/+$/, '')).filter(Boolean)
)];
const PROXY_KEY = process.env.AUDIUS_PROXY_KEY || '';
const ACCOUNTS_PER_PROXY = Math.max(1, Number(process.env.ACCOUNTS_PER_PROXY || 2));

// ---------------------------------------------------------------------------
// Proxy pool: every proxy URL is shared by at most ACCOUNTS_PER_PROXY (2) accounts at a
// time. A running job holds a "lease" on one proxy slot for its whole life (track info +
// the full stream), and all of that job's requests go through the leased proxy. Nothing is
// random: a free slot is always taken from the first healthy proxy (in list order) that
// still has room, so proxies fill in pairs. When a job ends, its slot is released and
// handed to the next waiting job.
// ---------------------------------------------------------------------------
class ProxyPool {
  constructor(urls, perProxy) {
    this.urls = urls;
    this.per = perProxy;
    this.load = new Map(urls.map(u => [u, 0]));
    this.downUntil = new Map();
    this.waiters = [];
  }
  get enabled() { return this.urls.length > 0; }
  get capacity() { return this.urls.length * this.per; }
  healthy() {
    const now = Date.now();
    const up = this.urls.filter(u => (this.downUntil.get(u) || 0) <= now);
    return up.length ? up : this.urls; // all down: keep trying them rather than stall
  }
  markDown(proxy, ms) { this.downUntil.set(proxy, Date.now() + ms); }
  // First healthy proxy with a free slot (skipping `exclude`); reserves the slot.
  take(exclude) {
    for (const u of this.healthy()) {
      if (u !== exclude && this.load.get(u) < this.per) {
        this.load.set(u, this.load.get(u) + 1);
        return u;
      }
    }
    return null;
  }
  give(proxy) {
    this.load.set(proxy, Math.max(0, this.load.get(proxy) - 1));
    for (const wake of this.waiters.splice(0)) wake();
  }
  // Waits (without polling) until some proxy has a free slot.
  async acquire() {
    if (!this.enabled) return { proxy: null, release() {}, move() { return false; } };
    let proxy;
    while (!(proxy = this.take())) await new Promise(resolve => this.waiters.push(resolve));
    const pool = this;
    const lease = {
      proxy,
      released: false,
      release() { if (lease.released) return; lease.released = true; pool.give(lease.proxy); },
      // Proxy unreachable: switch to another proxy that has a free slot (the cap still holds).
      move() {
        const next = pool.take(lease.proxy);
        if (!next) return false;
        pool.give(lease.proxy);
        lease.proxy = next;
        return true;
      },
    };
    return lease;
  }
}
const proxyPool = new ProxyPool(PROXY_URLS, ACCOUNTS_PER_PROXY);
const proxyStore = new AsyncLocalStorage(); // carries the current job's lease through its async calls
const proxyHost = lease => (lease && lease.proxy ? new URL(lease.proxy).host : '');

{
  const realFetch = globalThis.fetch;
  const CONNECT_ERRS = new Set(['ETIMEDOUT', 'ENETUNREACH', 'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'UND_ERR_CONNECT_TIMEOUT']);
  const warned = new Set();
  const ATTEMPTS = Number(process.env.PROXY_ATTEMPTS || 6);
  console.log(proxyPool.enabled
    ? `[*] Route: Cloudflare proxy pool (${PROXY_URLS.length} proxies x ${ACCOUNTS_PER_PROXY} accounts = ${proxyPool.capacity} concurrent)`
    : '[*] Route: direct (no proxies configured)');
  globalThis.fetch = async (input, init) => {
    if (!proxyPool.enabled) return realFetch(input, init);
    const isReq = typeof input === 'object' && input !== null && 'url' in input;
    let url;
    try { url = new URL(isReq ? input.url : String(input)); } catch (_) { return realFetch(input, init); }
    if (url.hostname !== 'api.audius.co') return realFetch(input, init);
    const headers = new Headers((init && init.headers) || (isReq ? input.headers : undefined));
    if (PROXY_KEY) headers.set('x-proxy-key', PROXY_KEY);
    const lease = proxyStore.getStore() || null; // the calling job's proxy slot (none outside a job)
    let lastErr;
    for (let attempt = 1; attempt <= ATTEMPTS; attempt++) {
      const proxy = lease ? lease.proxy : proxyPool.healthy()[0];
      const target = proxy + url.pathname + url.search;
      try {
        return isReq && !init
          ? await realFetch(new Request(target, input.clone()), { headers })
          : await realFetch(target, { ...init, headers });
      } catch (e) {
        const code = e && ((e.cause && e.cause.code) || e.code);
        if (!CONNECT_ERRS.has(code)) throw e;
        lastErr = e;
        proxyPool.markDown(proxy, 20000);
        if (!warned.has(proxy)) { warned.add(proxy); console.log(`[!] Proxy ${new URL(proxy).host} unreachable; moving jobs to another proxy.`); }
        if (lease) lease.move(); // only succeeds if another proxy has a free slot; otherwise retry this one
        await new Promise(r => setTimeout(r, 300 * attempt));
      }
    }
    throw lastErr;
  };
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

const ROOT_DIR = path.resolve(__dirname, '..');
const DB_FILE = path.join(ROOT_DIR, 'db.txt');
const STATE_FILE = path.join(ROOT_DIR, '.audius_stream_state.json');
const TARGET_ADDRESS = 'qringgreen@gmail.com';

const CFG = {
  timezone: process.env.PUBLISH_TIMEZONE || 'UTC',
  dailyPerTrack: Number(process.env.DAILY_STREAMS_PER_TRACK || 1000),
  maxStreamsPerRun: Number(process.env.MAX_STREAMS_PER_RUN || process.env.MAX_STREAMS_PER_RUN_TODAY || 100),
  streamDurationMs: Number(process.env.STREAM_DURATION_MS || STREAM_DURATION_MS),
  fullPlayAt: Number(process.env.FULL_PLAY_AT_STREAM || 100),
  streamDelayMs: Number(process.env.STREAM_DELAY_MS || 100),
  lockTtlMs: Number(process.env.LOCK_TTL_MINUTES || 120) * 60000,
  lockStaleMs: Number(process.env.LOCK_STALE_SECONDS || 120) * 1000,
  retries: Number(process.env.RETRY_ATTEMPTS || 3),
  retryBaseMs: Number(process.env.RETRY_BASE_MS || 2000),
  failStopAfter: Number(process.env.FAIL_STOP_AFTER || 30),
  stallMs: Number(process.env.STREAM_STALL_SECONDS || 15) * 1000,
  bufferMs: Number(process.env.STREAM_BUFFER_SECONDS || 10) * 1000,
  assumedKbps: Number(process.env.STREAM_ASSUMED_KBPS || 320),
  callTimeoutMs: Number(process.env.CALL_TIMEOUT_SECONDS || 30) * 1000,
  maxRunMs: Number(process.env.MAX_RUN_MINUTES || 0) * 60000,
  concurrency: Number(process.env.CONCURRENCY || 12),
};

const TRACK_ID_RE = /^[A-Za-z0-9_-]{1,40}$/;

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
      const { data, error } = await sb.from('audius').select('address, handle, user_id, app_data, stream')
        .order('address', { ascending: true }).range(from, from + page - 1);
      if (error) {
        const e = new Error(/stream/i.test(error.message) && /column|exist|schema/i.test(error.message)
          ? 'audius.stream column is missing - run the SQL migration first.' : error.message);
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

// Only the LAST published batch counts: the newest date in up_tracks that has at least one
// successful upload. Earlier days are ignored completely, and the same tracks stay the
// targets until upload_track.js publishes a newer batch.
function collectTracks(upTracks) {
  const days = isObj(upTracks) && Array.isArray(upTracks.days) ? upTracks.days : [];
  const ok = days.filter(d => d && d.status === 'success' && d.track_id != null && typeof d.date === 'string' &&
    TRACK_ID_RE.test(String(d.track_id).trim()));
  if (!ok.length) return [];
  const latest = ok.reduce((m, d) => (d.date > m ? d.date : m), ok[0].date);
  const seen = new Map();
  for (const d of ok) {
    if (d.date !== latest) continue;
    const id = String(d.track_id).trim();
    if (seen.has(id)) continue;
    seen.set(id, {
      id, key: id, address: d.address || null, handle: typeof d.handle === 'string' ? d.handle : null,
      date: latest, cap: CFG.dailyPerTrack,
      maxPerRun: CFG.maxStreamsPerRun || CFG.dailyPerTrack,
    });
  }
  return [...seen.values()];
}

function normalizeStream(raw, weekStart) {
  if (isObj(raw) && raw.week_start === weekStart && Array.isArray(raw.runs)) return JSON.parse(JSON.stringify(raw));
  const h = { week_start: weekStart, runs: [] };
  if (isObj(raw) && raw.lock) h.lock = raw.lock;
  return h;
}

let HELD_LOCK = null;

async function updateStream(sb, address, weekStart, mutate) {
  let lastErr;
  for (let attempt = 1; attempt <= 8; attempt++) {
    try {
      const { data, error } = await sb.from('audius').select('address, stream').eq('address', address).limit(1);
      if (error) throw new Error(error.message);
      if (!data || !data[0]) throw Object.assign(new Error(`account row not found: ${address}`), { fatal: true });
      const raw = data[0].stream;
      const hist = normalizeStream(raw, weekStart);
      if (address === TARGET_ADDRESS && HELD_LOCK && !(hist.lock && hist.lock.token === HELD_LOCK.token)) {
        throw new LockLostError('batch lock was taken over by another run');
      }
      if (mutate(hist) === false) return hist;
      const prevV = isObj(raw) && Number.isInteger(raw.v) ? raw.v : null;
      hist.v = (prevV || 0) + 1;
      let q = sb.from('audius').update({ stream: hist }).eq('address', address);
      q = prevV === null ? q.is('stream->>v', null) : q.eq('stream->>v', String(prevV));
      const res = await q.select('address, stream');
      if (res.error) throw new Error(res.error.message);
      if (res.data && res.data.length === 1) return normalizeStream(res.data[0].stream, weekStart);
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

async function recordStream(sb, weekStart, address, f) {
  const key = String(f.track_id);
  await updateStream(sb, address, weekStart, h => {
    let run = h.runs.find(r => r.date === f.date && r.run_number === f.run_number);
    if (!run) { run = { date: f.date, day: f.day, run_number: f.run_number, tracks: [] }; h.runs.push(run); }
    const entry = run.tracks.find(t => String(t.track_id) === key && t.status === 'success');
    if (entry) {
      entry.count = (entry.count || 1) + 1;
      entry.streamed_at = f.at;
    } else {
      run.tracks.push({ track_id: key, status: 'success', count: 1, streamed_at: f.at });
    }
    return true;
  });
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
    try { await recordStream(sb, ctx.weekStart, p.address, p); } catch (_) { keep.push(p); }
  }
  log(`[*] Recovered ${PENDING.length - keep.length}/${PENDING.length} unsaved streams.`);
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
  await updateStream(sb, TARGET_ADDRESS, ctx.weekStart, h => {
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
      await updateStream(sb, TARGET_ADDRESS, ctx.weekStart, h => {
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
    await updateStream(sb, TARGET_ADDRESS, ctx.weekStart, h => {
      if (!h.lock || h.lock.token !== lock.token) return false;
      delete h.lock;
      return true;
    });
  } catch (e) {
    console.error(`[!] Could not release lock (it expires on its own): ${e.message}`);
  }
}

const hasCreds = r => !!(r.user_id && r.app_data && r.app_data.api_key && r.app_data.bearer_token);

function makeSdk(acct) {
  return sdkFactory({
    apiKey: acct.app_data.api_key,
    apiSecret: acct.app_data.api_secret,
    bearerToken: acct.app_data.bearer_token,
  });
}

async function getTrackMeta(run, target, sdk) {
  if (!run.meta.has(target.key)) {
    const p = (async () => {
      const res = await withTimeout(
        sdk.tracks.getTrack({ trackId: target.id }),
        CFG.callTimeoutMs,
        'getTrack'
      );
      const d = res && res.data;
      if (!d) throw new Error('track info missing');

      const duration = Number(d.duration);
      
      // Extract the signed cidstream URL generated by the SDK
      const streamUrl = d.stream && d.stream.url;
      if (!streamUrl) throw new Error('track has no stream URL (may be gated)');

      return {
        streamUrl,
        duration: Number.isFinite(duration) && duration > 0 ? duration : 180,
      };
    })();
    run.meta.set(target.key, p);
    p.catch(() => run.meta.delete(target.key));
  }
  return run.meta.get(target.key);
}

function tracksBelowCap(rows, published, ctx) {
  const today = countTracks(rows, ctx);
  return published.filter(p => (today.get(p.key) || 0) < p.cap);
}

function allDoneMsg(n) {
  return `[*] All ${n} tracks reached their daily stream limit. Exiting.`;
}

function dayState(coord, today) {
  const todays = (coord.batches || []).filter(x => x.date === today);
  const open = todays.find(x => !x.done);
  if (open) return { runNumber: open.run_number, batch: open };
  return { runNumber: todays.reduce((m, x) => Math.max(m, x.run_number), 0) + 1, batch: null };
}

function reason(e) {
  if (isRateLimit(e)) return '429';
  const code = statusOf(e);
  if (code) return `HTTP ${code}${e.detail ? ` ${e.detail}` : ''}`;
  const m = String((e && e.message) || e);
  if (/timeout|timed out/i.test(m)) return 'timeout';
  return m.slice(0, 50);
}

async function readChunk(reader, ms) {
  let timer;
  const stall = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`stalled for ${ms / 1000}s`)), ms); });
  try {
    return await Promise.race([reader.read(), stall]);
  } finally {
    clearTimeout(timer);
  }
}

async function playStream(streamUrl, playMs, durationSec) {
  const controller = new AbortController();
  const connectTimer = setTimeout(() => controller.abort(), CFG.callTimeoutMs);
  const overallTimer = setTimeout(() => controller.abort(), playMs + CFG.callTimeoutMs * 2);
  let reader = null;
  let bytes = 0;
  const interrupted = (msg, extra) => Object.assign(new Error(`interrupted: ${msg} after ${Math.round(bytes / 1024)} KB`), { interrupted: true, bytes, ...extra });
  try {
    let res;
    try {
      // The URL provided by the SDK already contains the necessary signatures.
      // We don't need to append query params or headers, just fetch it.
      res = await fetch(streamUrl, {
        headers: { Accept: '*/*' },
        redirect: 'follow',
        signal: controller.signal,
      });
    } catch (e) {
      if (controller.signal.aborted) throw new Error(`timeout: stream request took longer than ${CFG.callTimeoutMs / 1000}s`);
      throw e;
    } finally {
      clearTimeout(connectTimer);
    }
    
    if (!res.ok) {
      let detail = '';
      try { detail = (await res.text()).replace(/\s+/g, ' ').trim().slice(0, 80); } catch (_) {}
      throw Object.assign(new Error(`HTTP ${res.status}`), { status: res.status, detail });
    }
    if (!res.body) throw new Error('empty stream response');

    const length = Number(res.headers.get('content-length')) || 0;
    const rate = length > 0 && durationSec > 0 ? length / durationSec : CFG.assumedKbps * 125;
    const expectedBytes = length || rate * durationSec;
    reader = res.body.getReader();

    let firstAt = 0;
    let ended = false;
    for (;;) {
      const wall = firstAt ? Date.now() - firstAt : 0;
      if (firstAt && wall >= playMs) break;
      let chunk;
      try {
        chunk = await readChunk(reader, firstAt ? CFG.stallMs : CFG.callTimeoutMs);
      } catch (e) {
        const msg = controller.signal.aborted ? `timeout after ${Math.round(playMs / 1000)}s limit` : e.message;
        if (!bytes) throw new Error(`no audio received (${msg})`);
        throw interrupted(msg);
      }
      if (chunk.done) { ended = true; break; }
      if (!chunk.value || !chunk.value.length) continue;
      if (!firstAt) firstAt = Date.now();
      bytes += chunk.value.length;
      const mediaMs = (bytes / rate) * 1000;
      const now = Date.now() - firstAt;
      const ahead = mediaMs - now - CFG.bufferMs;
      if (ahead > 0) await sleep(Math.min(ahead, Math.max(0, playMs - now)));
    }

    if (!bytes) throw new Error('empty stream response');
    const wallMs = Date.now() - firstAt;
    const mediaMs = (bytes / rate) * 1000;
    if (ended) {
      if (bytes < expectedBytes * 0.9) throw interrupted(`stream ended early (${Math.round(bytes / 1024)} of ${Math.round(expectedBytes / 1024)} KB)`, { ended: true });
    } else if (bytes < rate * (playMs / 1000) * 0.9) {
      throw interrupted(`only ${Math.round(bytes / 1024)} of ${Math.round((rate * playMs) / 1000 / 1024)} KB delivered in ${Math.round(wallMs / 1000)}s`);
    }
    return { bytes, wallMs, playbackMs: Math.min(mediaMs, wallMs), ended, status: res.status, contentType: res.headers.get('content-type') || 'unknown' };
  } finally {
    clearTimeout(connectTimer);
    clearTimeout(overallTimer);
    if (reader) await reader.cancel().catch(() => {});
    controller.abort();
  }
}

async function streamJob(sb, ctx, run, acct, target) {
  const st = run.stats;
  const who = acct.handle ? `@${acct.handle}` : acct.address;
  let lease = null;
  const say = text => { if (VERBOSE) log(`[*] ${who} -> track ${target.id}${lease && lease.proxy ? ` via ${proxyHost(lease)}` : ''} | ${text}`); };
  let result = null;
  let outcome;
  let stage = 'trackInfo';

  try {
    const sent = run.started.get(target.key) || 0;
    run.started.set(target.key, sent + 1);
    const ordinal = (run.todayDone.get(target.key) || 0) + sent + 1;

    let sdk = run.sdks.get(acct.address);
    if (!sdk) { sdk = makeSdk(acct); run.sdks.set(acct.address, sdk); }

    // Take a proxy slot (waits if every proxy already has its 2 accounts). Every request
    // this job makes, track info and the stream itself, goes through that proxy.
    lease = await proxyPool.acquire();
    await proxyStore.run(lease, async () => {
      const meta = await getTrackMeta(run, target, sdk);
      let playMs = CFG.streamDurationMs;
      if (ordinal >= CFG.fullPlayAt) {
        playMs = (meta.duration + 5) * 1000;
        say(`playing full track (~${meta.duration}s)`);
      }

      stage = 'play';
      result = await playStream(meta.streamUrl, playMs, meta.duration);
    });
    run.consecFail = 0;
    outcome = 'ok';
    st.ok++;
  } catch (e) {
    const kind = classify(e);
    const why = `${stage}: ${reason(e)}`;
    run.reasons.set(why, (run.reasons.get(why) || 0) + 1);
    if (e && e.interrupted) { outcome = 'interrupted'; st.interrupted++; say(why); }
    else if (kind === 'rate') { outcome = 'rate'; st.rate++; say(why); }
    else if (kind === 'permanent') { outcome = 'perm'; st.perm++; run.cool.add(acct.address); say(why); }
    else { outcome = 'retry'; st.retry++; say(why); }
    if (++run.consecFail >= CFG.failStopAfter && !run.stop) {
      run.stop = true;
      log('[!] Too many failures in a row; stopping. Next run resumes.');
    }
  } finally {
    if (lease) lease.release(); // free the proxy slot for the next waiting account
  }

  if (result) {
    const item = {
      address: acct.address, track_id: target.id, date: ctx.today, day: ctx.dayName, run_number: run.number,
      week_start: ctx.weekStart, at: new Date().toISOString(),
    };
    let saved = true;
    try { await recordStream(sb, ctx.weekStart, acct.address, item); } catch (e) {
      saved = false;
      PENDING.push(item); savePending();
      if (e instanceof LockLostError) run.abort = e;
    }
    acct.today++;
    const detail = `${(result.playbackMs / 1000).toFixed(1)}s played in ${(result.wallMs / 1000).toFixed(1)}s, ${Math.round(result.bytes / 1024)} KB${result.ended ? ', stream ended' : ''}, HTTP ${result.status} ${result.contentType}, user ${acct.user_id}${lease && lease.proxy ? `, proxy ${proxyHost(lease)}` : ''}`;
    if (!saved) log(`[!] ${who} -> track ${target.id} | completed (${detail}), DB save failed (kept locally)`);
    else say(`completed: ${detail}`);
  }

  if (CFG.streamDelayMs) await sleep(CFG.streamDelayMs);
  return outcome;
}

// Concurrent jobs can never exceed the proxy slots (proxies x accounts-per-proxy).
function effectiveConcurrency() {
  const c = Math.max(1, CFG.concurrency);
  return proxyPool.enabled ? Math.min(c, proxyPool.capacity) : c;
}

async function fillTrack(sb, ctx, run, target, attempts, maxOk, candidates) {
  let got = 0;
  let inflight = 0;
  let started = 0;
  const step = async () => {
    if (run.abort || LOCK_LOST || run.stop) return false;
    if (CFG.maxRunMs && Date.now() - run.startedAt > CFG.maxRunMs) {
      run.stop = true;
      log('[!] Time limit reached; stopping. Next run resumes.');
      return false;
    }
    if (got >= maxOk || started >= attempts) return false;
    if (got + inflight >= maxOk) {
      if (!inflight) return false;
      await sleep(50);
      return true;
    }
    let acct = candidates.shift();
    while (acct && run.cool.has(acct.address)) acct = candidates.shift();
    if (!acct) return false;
    inflight++;
    started++;
    const outcome = await streamJob(sb, ctx, run, acct, target);
    inflight--;
    if (outcome === 'ok') got++;
    return true;
  };
  await Promise.all(Array.from({ length: effectiveConcurrency() }, async () => { while (await step()); }));
  return { got, started, skipped: started - got };
}

function countToday(raw, ctx) {
  let n = 0;
  if (isObj(raw) && raw.week_start === ctx.weekStart && Array.isArray(raw.runs)) {
    for (const r of raw.runs) if (r.date === ctx.today) for (const t of (r.tracks || [])) if (t.status === 'success') n += t.count || 1;
  }
  return n;
}

function countTracks(rows, ctx, runNumber) {
  const counts = new Map();
  for (const r of rows) {
    const f = r.stream;
    if (!isObj(f) || f.week_start !== ctx.weekStart || !Array.isArray(f.runs)) continue;
    for (const x of f.runs) {
      if (x.date !== ctx.today || (runNumber !== undefined && x.run_number !== runNumber)) continue;
      for (const t of x.tracks || []) {
        if (t.status !== 'success') continue;
        const k = String(t.track_id);
        counts.set(k, (counts.get(k) || 0) + (t.count || 1));
      }
    }
  }
  return counts;
}

async function processRun(sb, ctx, rows, targets, { runNumber, batch }) {
  const accounts = rows.map(r => ({
    address: r.address, handle: typeof r.handle === 'string' ? r.handle.trim().replace(/^@/, '') : null,
    user_id: r.user_id, app_data: r.app_data, hasCreds: hasCreds(r),
    today: countToday(r.stream, ctx),
  }));
  const reader = accounts.find(a => a.hasCreds);
  if (!reader) throw new Error('no account with complete credentials');

  const targetAddrs = new Set(targets.map(p => p.address).filter(Boolean));
  const excluded = new Set(accounts.filter(a => targetAddrs.has(a.address)).map(a => a.address));

  const todayDone = countTracks(rows, ctx);
  const runDone = countTracks(rows, ctx, runNumber);

  const need = new Map(targets.map(t => {
    const left = Math.max(0, t.cap - (todayDone.get(t.key) || 0));
    const attempts = Math.max(0, t.maxPerRun - (runDone.get(t.key) || 0));
    return [t.key, { attempts: left ? attempts : 0, maxOk: left }];
  }));

  let totalNeed = 0;
  let active = 0;
  for (const t of targets) {
    const n = need.get(t.key);
    if (!n.attempts) continue;
    totalNeed += Math.min(n.attempts, n.maxOk);
    active++;
  }

  if (!totalNeed) {
    if (!tracksBelowCap(rows, targets, ctx).length) {
      log(allDoneMsg(targets.length));
      return { status: 'ALREADY_COMPLETED' };
    }
    await updateStream(sb, TARGET_ADDRESS, ctx.weekStart, h => {
      if (!h.batches) h.batches = [];
      const b = h.batches.find(x => x.date === ctx.today && x.run_number === runNumber);
      if (b && b.done) return false;
      if (b) b.done = true; else h.batches.push({ date: ctx.today, run_number: runNumber, done: true });
      return true;
    });
    log(`[*] Run ${runNumber}: nothing left to attempt in this run.`);
    return { status: 'COMPLETED' };
  }

  const atCap = targets.filter(t => (todayDone.get(t.key) || 0) >= t.cap).length;
  if (proxyPool.enabled) {
    log(`[*] Concurrency: ${effectiveConcurrency()} jobs (${proxyPool.urls.length} proxies x ${proxyPool.per} accounts each).`);
    if (CFG.concurrency > proxyPool.capacity) {
      log(`[!] CONCURRENCY=${CFG.concurrency} is more than the proxy slots available (${proxyPool.capacity}). Add proxy URLs (AUDIUS_PROXY_URLS) or raise ACCOUNTS_PER_PROXY to use all ${CFG.concurrency}.`);
    }
  }
  log(`[*] Run ${runNumber}${batch ? ' (resumed)' : ''}: ${active} tracks to do${atCap ? ` (${atCap} already at daily limit)` : ''}, up to ${totalNeed} streams.`);

  if (!batch) {
    await updateStream(sb, TARGET_ADDRESS, ctx.weekStart, h => {
      if (!h.batches) h.batches = [];
      if (h.batches.some(b => b.date === ctx.today && b.run_number === runNumber)) return false;
      h.batches.push({ date: ctx.today, run_number: runNumber, done: false });
      return true;
    });
  }

  const run = { todayDone, started: new Map(), meta: new Map(), sdks: new Map(), startedAt: Date.now(), number: runNumber, cool: new Set(), abort: null, stop: false, consecFail: 0, reasons: new Map(), stats: { ok: 0, interrupted: 0, rate: 0, retry: 0, perm: 0 } };
  const st = run.stats;
  let incomplete = false;

  for (const t of targets) {
    const n = need.get(t.key);
    if (!n.attempts) continue;
    if (run.stop) { incomplete = true; continue; }
    const pool = accounts.filter(a => a.hasCreds && !excluded.has(a.address) && !run.cool.has(a.address));
    const order = shuffle(pool).sort((x, y) => x.today - y.today);
    const candidates = order.length ? Array.from({ length: n.attempts }, (_, i) => order[i % order.length]) : [];
    const { got, started, skipped } = await fillTrack(sb, ctx, run, t, n.attempts, n.maxOk, candidates);
    if (run.abort || LOCK_LOST) throw run.abort || LOCK_LOST;
    const day = (todayDone.get(t.key) || 0) + got;
    if (started < n.attempts && got < n.maxOk && !run.stop) {
      log(`[!] track ${t.id} | ${day}/${t.cap} today (no more accounts available${skipped ? `, ${skipped} skipped` : ''})`);
    } else {
      log(`[*] track ${t.id} | ${day}/${t.cap} today${day >= t.cap ? ' (done)' : ''}${skipped ? ` (${skipped} skipped)` : ''}`);
    }
    if (run.stop) incomplete = true;
  }

  const left = incomplete;
  if (!left) {
    await updateStream(sb, TARGET_ADDRESS, ctx.weekStart, h => {
      const b = (h.batches || []).find(x => x.date === ctx.today && x.run_number === runNumber);
      if (!b || b.done) return false;
      b.done = true;
      return true;
    });
  }
  if (run.reasons.size) {
    const top = [...run.reasons.entries()].sort((a, b) => b[1] - a[1]).slice(0, 4).map(([k, n]) => `${k} x${n}`);
    log(`[!] Failure reasons: ${top.join(', ')}`);
  }
  const parts = [`${st.ok} completed`];
  if (st.interrupted) parts.push(`${st.interrupted} interrupted`);
  if (st.rate) parts.push(`${st.rate} rate-limited`);
  if (st.retry) parts.push(`${st.retry} errors`);
  if (st.perm) parts.push(`${st.perm} failed`);
  log(`[*] Run ${runNumber} ${left ? 'incomplete' : 'complete'}: ${parts.join(', ')}.${left ? ' Next run resumes.' : ''}`);
  return { status: left ? 'INCOMPLETE' : 'COMPLETED' };
}

async function runStream({ now = new Date() } = {}) {
  const ctx = buildContext(now);
  const sb = connectSupabase();
  PENDING = loadPending();

  const published = collectTracks(await readUpTracks(sb));
  if (!published.length) {
    log('[!] No successfully published tracks found in up_tracks. Exiting.');
    return { status: 'NO_TARGETS' };
  }
  log(`[*] Targeting the last published batch (${published[0].date}): ${published.length} track(s).`);

  let rows = await fetchRows(sb);
  const coordRaw = rows.find(r => r.address === TARGET_ADDRESS).stream;
  if (isObj(coordRaw) && coordRaw.week_start && coordRaw.week_start !== ctx.weekStart) {
    log('[*] New week: history reset.');
  }
  if (!tracksBelowCap(rows, published, ctx).length) {
    log(allDoneMsg(published.length));
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
    const coord = normalizeStream(rows.find(r => r.address === TARGET_ADDRESS).stream, ctx.weekStart);
    if (!tracksBelowCap(rows, published, ctx).length) {
      log(allDoneMsg(published.length));
      return { status: 'ALREADY_COMPLETED' };
    }
    return await processRun(sb, ctx, rows, published, dayState(coord, ctx.today));
  } finally {
    stopHeartbeat();
    HELD_LOCK = null;
    await releaseLock(sb, ctx, lock);
  }
}

if (require.main === module) {
  runStream().catch(err => {
    console.error('[!] Fatal error:', err.message || err);
    process.exit(1);
  });
}

module.exports = { runStream, collectTracks, buildContext, CFG, ProxyPool, proxyPool, proxyStore, effectiveConcurrency };