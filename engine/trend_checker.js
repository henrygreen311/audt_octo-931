#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
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

const CFG = {
  retries: Number(process.env.RETRY_ATTEMPTS || 3),
  retryBaseMs: Number(process.env.RETRY_BASE_MS || 2000),
  callTimeoutMs: Number(process.env.CALL_TIMEOUT_SECONDS || 20) * 1000,
  time: process.env.TREND_TIME || 'week',
  pageSize: 100,
  topDepth: 100,
  bonusDepth: Number(process.env.TREND_BONUS_DEPTH || 1000),
  rateRetries: 3,
  rateWaitMs: Number(process.env.RATE_WAIT_MS || 3000),
};

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

function connectSupabase() {
  if (!fs.existsSync(DB_FILE)) throw new Error(`db.txt not found at: ${DB_FILE}`);
  const dbText = fs.readFileSync(DB_FILE, 'utf8');
  const urlMatch = dbText.match(/SUPABASE_URL\s*=\s*["']([^"']+)["']/);
  const keyMatch = dbText.match(/SUPABASE_KEY\s*=\s*["']([^"']+)["']/);
  if (!urlMatch || !keyMatch) throw new Error('Could not read SUPABASE_URL / SUPABASE_KEY from db.txt');
  return createClient(urlMatch[1], keyMatch[1]);
}

const hasCreds = r => !!(r.user_id && r.app_data && r.app_data.api_key && r.app_data.api_secret && r.app_data.bearer_token);

function makeSdk(acct) {
  return sdkFactory({
    apiKey: acct.app_data.api_key,
    apiSecret: acct.app_data.api_secret,
    bearerToken: acct.app_data.bearer_token,
  });
}

async function fetchAccounts(sb) {
  const out = [];
  const page = 1000;
  for (let from = 0; ; from += page) {
    const data = await withRetry(async () => {
      const { data, error } = await sb.from('audius').select('address, handle, user_id, app_data')
        .order('address', { ascending: true }).range(from, from + page - 1);
      if (error) throw new Error(error.message);
      return data || [];
    });
    out.push(...data);
    if (data.length < page) break;
  }
  return out;
}

const BOARDS = [
  { name: 'Trending', fetch: (sdk, offset, limit) => sdk.tracks.getTrendingTracks({ time: CFG.time, limit, offset }) },
  { name: 'Underground', fetch: (sdk, offset, limit) => sdk.tracks.getUndergroundTrendingTracks({ limit, offset }) },
];

async function loadBoard(sdk, board, depth) {
  const items = [];
  for (let offset = 0; offset < depth; offset += CFG.pageSize) {
    const limit = Math.min(CFG.pageSize, depth - offset);
    let list = null;
    let lastErr;
    for (let i = 0; i <= CFG.rateRetries; i++) {
      try {
        const res = await readRetry(() => board.fetch(sdk, offset, limit));
        list = (res && res.data) || [];
        break;
      } catch (e) {
        lastErr = e;
        if (!isRateLimit(e)) break;
        await sleep(CFG.rateWaitMs * (i + 1));
      }
    }
    if (!list) {
      log(`[!] ${board.name}: stopped at ${items.length} tracks (${isRateLimit(lastErr) ? '429' : errMsg(lastErr).slice(0, 60)}).`);
      break;
    }
    items.push(...list);
    if (list.length < limit) break;
  }
  return items;
}

function indexAccounts(accounts) {
  const byId = new Map();
  const byHandle = new Map();
  for (const a of accounts) {
    if (a.user_id) byId.set(String(a.user_id), a);
    const h = typeof a.handle === 'string' ? a.handle.trim().replace(/^@/, '').toLowerCase() : '';
    if (h) byHandle.set(h, a);
  }
  return track => {
    const u = (track && track.user) || {};
    const id = u.id !== undefined ? u.id : track && track.userId;
    const handle = typeof u.handle === 'string' ? u.handle.toLowerCase() : '';
    return (id !== undefined && byId.get(String(id))) || (handle && byHandle.get(handle)) || null;
  };
}

function describe(hit) {
  const t = hit.track;
  const handle = (t.user && t.user.handle) || hit.acct.handle || hit.acct.address;
  const link = t.permalink ? ` | https://audius.co${t.permalink}` : '';
  return `#${hit.rank} @${String(handle).replace(/^@/, '')} | "${t.title || 'untitled'}" | ${t.id}${link}`;
}

function report(label, hits, from, to) {
  const part = hits.filter(h => h.rank >= from && h.rank <= to);
  if (!part.length) {
    log(`[*] ${label}: none of our accounts.`);
    return 0;
  }
  log(`[*] ${label}: ${part.length} of our tracks found.`);
  for (const h of part) log(`[*]   ${describe(h)}`);
  return part.length;
}

async function runTrendCheck() {
  const sb = connectSupabase();
  const accounts = await fetchAccounts(sb);
  const reader = shuffle(accounts.filter(hasCreds))[0];
  if (!reader) throw new Error('no account with complete credentials');
  const sdk = makeSdk(reader);
  if (!sdk.tracks || typeof sdk.tracks.getTrendingTracks !== 'function' || typeof sdk.tracks.getUndergroundTrendingTracks !== 'function') {
    throw new Error('installed @audius/sdk is missing getTrendingTracks / getUndergroundTrendingTracks');
  }
  const owner = indexAccounts(accounts);
  log(`[*] Checking ${accounts.length} accounts against trending lists.`);

  let topTotal = 0;
  let bonusTotal = 0;
  for (const board of BOARDS) {
    const items = await loadBoard(sdk, board, CFG.bonusDepth);
    const hits = [];
    items.forEach((track, i) => {
      const acct = owner(track);
      if (acct) hits.push({ rank: i + 1, track, acct });
    });
    topTotal += report(`${board.name} Top ${CFG.topDepth}`, hits, 1, CFG.topDepth);
    if (CFG.bonusDepth > CFG.topDepth) {
      bonusTotal += report(`${board.name} Top ${CFG.bonusDepth} (bonus, #${CFG.topDepth + 1}-${CFG.bonusDepth})`, hits, CFG.topDepth + 1, CFG.bonusDepth);
    }
  }
  log(`[*] Trend check complete: ${topTotal} in Top ${CFG.topDepth}, ${bonusTotal} in bonus range.`);
  return { top: topTotal, bonus: bonusTotal };
}

if (require.main === module) {
  runTrendCheck().catch(err => {
    console.error('[!] Fatal error:', err.message || err);
    process.exit(1);
  });
}

module.exports = { runTrendCheck, indexAccounts };
