#!/usr/bin/env node
'use strict';
// engine/name_ch.js - change Audius display names (NOT handles).
//
// Task 1: address ends with @tiffincrane.com -> random name from ../name.txt
//         (if the update fails, another random name is tried)
// Task 2: address ends with @fldubai.com     -> remove the 3 trailing digits (aleli942 -> aleli)
// Any other address is never touched.
//
// Usage:
//   node engine/name_ch.js                # both tasks
//   node engine/name_ch.js --task 1|2     # one task
//   node engine/name_ch.js --dry-run      # show changes, do nothing
//   node engine/name_ch.js --limit 3      # first 3 accounts per task
//   node engine/name_ch.js --force        # redo accounts finished in an earlier run

const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

// ---- same Cloudflare proxy routing as favorite.js ----
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

// ---- Audius SDK loading (same as favorite.js) ----
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
process.on('unhandledRejection', () => {});
for (const level of ['log', 'info', 'debug', 'warn']) {
  const orig = console[level].bind(console);
  console[level] = (...args) => {
    if (typeof args[0] === 'string' && args[0].startsWith('[audius-sdk]')) return;
    orig(...args);
  };
}

const ROOT_DIR = path.resolve(__dirname, '..');
const DB_FILE = path.join(ROOT_DIR, 'db.txt');
const NAME_FILE = path.join(ROOT_DIR, 'name.txt');
const STATE_FILE = path.join(ROOT_DIR, '.audius_name_ch_state.json');

const TASK1_DOMAIN = '@tiffincrane.com';
const TASK2_DOMAIN = '@fldubai.com';
const MAX_NAME_LEN = 32;

const CFG = {
  retries: Number(process.env.RETRY_ATTEMPTS || 3),
  retryBaseMs: Number(process.env.RETRY_BASE_MS || 2000),
  callTimeoutMs: Number(process.env.CALL_TIMEOUT_SECONDS || 30) * 1000,
  concurrency: Number(process.env.CONCURRENCY || 3),
  nameTries: Number(process.env.NAME_TRIES || 5),
  verifyTries: 4,
  verifyDelayMs: Number(process.env.VERIFY_DELAY_MS || 1000),
};

const sleep = ms => new Promise(r => setTimeout(r, ms));
let ABORT = null; // set when a problem makes every further attempt pointless
const statusOf = e => e && ((e.response && e.response.status) || e.status);
const errMsg = e => `${statusOf(e) ? `HTTP ${statusOf(e)} ` : ''}${(e && e.message) || e}`;
const isRateLimit = e => statusOf(e) === 429 || /rate.?limit|too many requests|\b429\b/i.test(String((e && e.message) || e));
const isPermanent = e => { const s = statusOf(e); return !!s && s >= 400 && s < 500 && s !== 408 && s !== 429; };

function withTimeout(promise, ms = CFG.callTimeoutMs, what = 'request') {
  let t;
  const timeout = new Promise((_, rej) => { t = setTimeout(() => rej(new Error(`timeout: ${what} took longer than ${ms / 1000}s`)), ms); });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

async function withRetry(fn, what) {
  let lastErr;
  for (let i = 1; i <= CFG.retries; i++) {
    try { return await withTimeout(fn(), CFG.callTimeoutMs, what); } catch (e) {
      lastErr = e;
      if (i === CFG.retries || isPermanent(e)) break;
      await sleep(CFG.retryBaseMs * i * (isRateLimit(e) ? 3 : 1));
    }
  }
  throw lastErr;
}

// ---- database ----
function connectSupabase() {
  if (!fs.existsSync(DB_FILE)) throw new Error(`db.txt not found at: ${DB_FILE}`);
  const dbText = fs.readFileSync(DB_FILE, 'utf8');
  const urlMatch = dbText.match(/SUPABASE_URL\s*=\s*["']([^"']+)["']/);
  const keyMatch = dbText.match(/SUPABASE_KEY\s*=\s*["']([^"']+)["']/);
  if (!urlMatch || !keyMatch) throw new Error('Could not read SUPABASE_URL / SUPABASE_KEY from db.txt');
  return createClient(urlMatch[1], keyMatch[1]);
}

async function fetchAccounts(sb, domain) {
  const out = [];
  const page = 1000;
  for (let from = 0; ; from += page) {
    const data = await withRetry(async () => {
      const { data, error } = await sb.from('audius').select('address, handle, user_id, app_data')
        .ilike('address', `%${domain}`)
        .order('address', { ascending: true }).range(from, from + page - 1);
      if (error) throw new Error(error.message);
      return data || [];
    }, 'read audius table');
    out.push(...data);
    if (data.length < page) break;
  }
  // exact check too, so no other address can slip through
  return out.filter(r => typeof r.address === 'string' && r.address.trim().toLowerCase().endsWith(domain));
}

const hasCreds = r => !!(r.user_id && r.app_data && r.app_data.api_key && r.app_data.api_secret && r.app_data.bearer_token);
const whoOf = r => (typeof r.handle === 'string' && r.handle.trim() ? `@${r.handle.trim().replace(/^@/, '')}` : r.address);

// ---- state, so a re-run does not rename accounts twice ----
function loadState() {
  try { const j = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); return j && typeof j === 'object' ? j : {}; } catch (_) { return {}; }
}
let STATE = {};
function markDone(task, address, name) {
  (STATE[task] = STATE[task] || {})[address] = name;
  const tmp = `${STATE_FILE}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(STATE, null, 1));
  fs.renameSync(tmp, STATE_FILE);
}

// ---- Audius ----
const sdks = new Map();
function getSdk(acct) {
  let sdk = sdks.get(acct.address);
  if (!sdk) {
    sdk = sdkFactory({ apiKey: acct.app_data.api_key, apiSecret: acct.app_data.api_secret, bearerToken: acct.app_data.bearer_token });
    sdks.set(acct.address, sdk);
  }
  return sdk;
}

async function readUser(acct) {
  const res = await withRetry(() => getSdk(acct).users.getUser({ id: acct.user_id }), 'getUser');
  const d = res && res.data;
  if (!d) throw Object.assign(new Error('user not found'), { status: 404 });
  return d;
}

async function readName(acct) {
  const d = await readUser(acct);
  return typeof d.name === 'string' ? d.name : '';
}

// sets the display name (handle untouched), confirms it changed and that nothing else did
async function setName(acct, name) {
  const sdk = getSdk(acct);
  if (!sdk.users || typeof sdk.users.updateUser !== 'function') {
    throw Object.assign(new Error('installed @audius/sdk has no users.updateUser(); run with --inspect'), { fatal: true });
  }
  const before = await readUser(acct);
  try {
    await withRetry(() => sdk.users.updateUser({ id: acct.user_id, userId: acct.user_id, metadata: { name } }), 'updateUser');
  } catch (e) {
    // wrong call shape is the same for every account, so stop instead of failing 451 times
    if (/required parameter|requestParameters|is not valid|invalid (metadata|body)/i.test(String((e && e.message) || e)) && !statusOf(e)) {
      throw Object.assign(new Error(`updateUser call shape rejected by the SDK: ${e.message}`), { fatal: true });
    }
    throw e;
  }
  for (let i = 1; i <= CFG.verifyTries; i++) {
    await sleep(CFG.verifyDelayMs * i);
    const after = await readUser(acct);
    if (after.name !== name) continue;
    for (const k of ['handle', 'bio', 'location']) {
      if (before[k] !== undefined && after[k] !== before[k]) {
        throw Object.assign(new Error(`updateUser also changed "${k}" on ${whoOf(acct)} (was ${JSON.stringify(before[k])}, now ${JSON.stringify(after[k])}); stopping so nothing else is damaged`), { fatal: true });
      }
    }
    return;
  }
  throw new Error('display name change could not be verified');
}

// ---- names ----
function loadNames() {
  if (!fs.existsSync(NAME_FILE)) throw new Error(`name.txt not found at: ${NAME_FILE}`);
  const seen = new Set();
  const names = [];
  for (const line of fs.readFileSync(NAME_FILE, 'utf8').split(/\r?\n/)) {
    const n = line.trim();
    if (n && n.length <= MAX_NAME_LEN && !seen.has(n.toLowerCase())) { seen.add(n.toLowerCase()); names.push(n); }
  }
  if (!names.length) throw new Error('name.txt has no usable names');
  return names;
}

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// random names, each used once until the list runs out, then it reshuffles
function makeNamePool(names) {
  let left = [];
  return avoid => {
    for (let pass = 0; pass < 2; pass++) {
      if (!left.length) left = shuffle(names);
      const i = left.findIndex(n => !avoid.has(n));
      if (i >= 0) return left.splice(i, 1)[0];
      left = [];
    }
    return null;
  };
}

// ---- tasks ----
async function task1Job(acct, nextName, dry) {
  const who = whoOf(acct);
  const tried = new Set();
  for (let i = 0; i < CFG.nameTries; i++) {
    const name = nextName(tried);
    if (!name) break;
    tried.add(name);
    if (dry) { log(`[dry] ${who} -> "${name}"`); return 'ok'; }
    try {
      await setName(acct, name);
      markDone('task1', acct.address, name);
      log(`[+] ${who} -> "${name}"`);
      return 'ok';
    } catch (e) {
      if (e.fatal) throw e;
      log(`[!] ${who} -> "${name}" failed (${errMsg(e)}); trying another name`);
      if ([401, 403].includes(statusOf(e))) break; // credentials problem, another name won't help
    }
  }
  log(`[x] ${who} | no name could be set`);
  return 'fail';
}

async function task2Job(acct, dry) {
  const who = whoOf(acct);
  let old;
  try { old = await readName(acct); } catch (e) {
    log(`[x] ${who} | could not read current name (${errMsg(e)})`);
    return 'fail';
  }
  if (!/\d{3}$/.test(old)) {
    log(`[-] ${who} | "${old}" has no 3 trailing digits, skipped`);
    if (!dry) markDone('task2', acct.address, old);
    return 'skip';
  }
  const next = old.replace(/\d{3}$/, '').trimEnd();
  if (!next) { log(`[-] ${who} | "${old}" would become empty, skipped`); return 'skip'; }
  if (dry) { log(`[dry] ${who} | "${old}" -> "${next}"`); return 'ok'; }
  try {
    await setName(acct, next);
    markDone('task2', acct.address, next);
    log(`[+] ${who} | "${old}" -> "${next}"`);
    return 'ok';
  } catch (e) {
    if (e.fatal) throw e;
    log(`[x] ${who} | "${old}" -> "${next}" failed (${errMsg(e)})`);
    return 'fail';
  }
}

async function runTask(label, domain, rows, key, opts, job) {
  log(`[*] ${label}: ${rows.length} account(s) with ${domain}`);
  let noCreds = 0;
  let doneBefore = 0;
  let todo = [];
  for (const r of rows) {
    if (!hasCreds(r)) noCreds++;
    else if (!opts.force && STATE[key] && STATE[key][r.address] !== undefined) doneBefore++;
    else todo.push(r);
  }
  if (noCreds) log(`[!] ${noCreds} skipped: incomplete credentials (user_id / api_key / api_secret / bearer_token)`);
  if (doneBefore) log(`[*] ${doneBefore} already done in an earlier run (use --force to redo)`);
  if (opts.limit) todo = todo.slice(0, opts.limit);

  const stats = { ok: 0, skip: 0, fail: 0 };
  await Promise.all(Array.from({ length: Math.max(1, CFG.concurrency) }, async () => {
    while (todo.length && !ABORT) {
      const acct = todo.shift();
      let outcome;
      try { outcome = await job(acct); } catch (e) {
        if (e.fatal) { if (!ABORT) { ABORT = e; log(`[!] Stopping: ${e.message}`); } break; }
        log(`[x] ${whoOf(acct)} | ${errMsg(e)}`); outcome = 'fail';
      }
      stats[outcome]++;
    }
  }));
  if (ABORT) log('[!] Aborted early; nothing else was attempted.');
  log(`[*] ${label} finished: ${stats.ok} changed, ${stats.skip} skipped, ${stats.fail} failed.`);
}

function parseArgs(argv) {
  const opts = { task: 'all', dry: false, limit: 0, force: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--dry-run') opts.dry = true;
    else if (a === '--force') opts.force = true;
    else if (a === '--task') opts.task = argv[++i];
    else if (a === '--limit') opts.limit = Number(argv[++i]) || 0;
    else if (a === '--inspect') opts.inspect = true;
  }
  if (!['all', '1', '2'].includes(opts.task)) throw new Error('--task must be 1, 2 or all');
  return opts;
}

function methodsOf(obj) {
  const names = new Set();
  for (let o = obj; o && o !== Object.prototype; o = Object.getPrototypeOf(o)) {
    for (const k of Object.getOwnPropertyNames(o)) {
      if (k !== 'constructor' && typeof obj[k] === 'function') names.add(k);
    }
  }
  return [...names].sort();
}

async function inspectSdk(sb) {
  let version = 'unknown';
  try {
    let dir = path.dirname(require.resolve('@audius/sdk'));
    for (let i = 0; i < 6; i++, dir = path.dirname(dir)) {
      const f = path.join(dir, 'package.json');
      if (fs.existsSync(f)) { const j = JSON.parse(fs.readFileSync(f, 'utf8')); if (j.name === '@audius/sdk') { version = j.version; break; } }
    }
  } catch (_) {}
  log(`[*] @audius/sdk version: ${version}`);
  const rows = await fetchAccounts(sb, TASK1_DOMAIN);
  const acct = rows.find(hasCreds);
  if (!acct) { log('[!] no account with complete credentials to build an SDK from'); return; }
  const sdk = getSdk(acct);
  log(`[*] sdk keys: ${Object.keys(sdk).join(', ')}`);
  for (const m of ['updateUser', 'updateUserRaw']) {
    if (sdk.users && typeof sdk.users[m] === 'function') log(`[*] ${m} source:\n${String(sdk.users[m]).slice(0, 2500)}`);
  }
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const sb = connectSupabase();
  STATE = loadState();
  if (opts.inspect) return inspectSdk(sb);

  if (opts.task === 'all' || opts.task === '1') {
    const nextName = makeNamePool(loadNames());
    const rows = await fetchAccounts(sb, TASK1_DOMAIN);
    await runTask('Task 1 (random name)', TASK1_DOMAIN, rows, 'task1', opts, a => task1Job(a, nextName, opts.dry));
  }
  if (opts.task === 'all' || opts.task === '2') {
    const rows = await fetchAccounts(sb, TASK2_DOMAIN);
    await runTask('Task 2 (remove 3 digits)', TASK2_DOMAIN, rows, 'task2', opts, a => task2Job(a, opts.dry));
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error('[!] Fatal error:', err.message || err);
    process.exit(1);
  });
}

module.exports = { main };
