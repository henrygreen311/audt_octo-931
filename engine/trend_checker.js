#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const log = console.log.bind(console);
const VERBOSE = /^(1|true|yes)$/i.test(process.env.VERBOSE || '') || process.argv.includes('--verbose') || process.argv.includes('-v');

const ROOT_DIR = path.resolve(__dirname, '..');
const DB_FILE = path.join(ROOT_DIR, 'db.txt');
const API_BASE = process.env.AUDIUS_API_BASE || 'https://api.audius.co';

const CFG = {
  retries: Number(process.env.RETRY_ATTEMPTS || 3),
  retryBaseMs: Number(process.env.RETRY_BASE_MS || 2000),
  timeoutMs: Number(process.env.TREND_TIMEOUT_SECONDS || 30) * 1000,
  time: process.env.TREND_TIME || 'week',
  pageSize: Number(process.env.TREND_PAGE_SIZE || 100),
  minPageSize: 25,
  topDepth: 100,
  bonusDepth: Math.min(300, Number(process.env.TREND_BONUS_DEPTH || 300)),
  rateRetries: 3,
  rateWaitMs: Number(process.env.RATE_WAIT_MS || 3000),
};

const sleep = ms => new Promise(r => setTimeout(r, ms));
const statusOf = e => e && ((e.response && e.response.status) || e.status);
const errMsg = e => {
  const m = String((e && e.message) || e);
  return statusOf(e) && !/^HTTP \d+/.test(m) ? `HTTP ${statusOf(e)} ${m}` : m;
};

function isRateLimit(e) {
  return statusOf(e) === 429 || /rate.?limit|too many requests|\b429\b/i.test(String((e && e.message) || e));
}

function isTransient(e) {
  const s = statusOf(e);
  if (s) return s === 408 || s >= 500;
  const msg = String((e && e.message) || e).toLowerCase();
  const code = String((e && (e.code || (e.cause && e.cause.code))) || '').toUpperCase();
  return /ECONNRESET|ETIMEDOUT|ECONNREFUSED|ENOTFOUND|EAI_AGAIN|EPIPE|UND_ERR/.test(code) ||
    /fetch failed|network|timeout|timed out|aborted|socket hang up|temporar|bad gateway|service unavailable|gateway/.test(msg);
}

function connectSupabase() {
  if (!fs.existsSync(DB_FILE)) throw new Error(`db.txt not found at: ${DB_FILE}`);
  const dbText = fs.readFileSync(DB_FILE, 'utf8');
  const urlMatch = dbText.match(/SUPABASE_URL\s*=\s*["']([^"']+)["']/);
  const keyMatch = dbText.match(/SUPABASE_KEY\s*=\s*["']([^"']+)["']/);
  if (!urlMatch || !keyMatch) throw new Error('Could not read SUPABASE_URL / SUPABASE_KEY from db.txt');
  return createClient(urlMatch[1], keyMatch[1]);
}

async function fetchAccounts(sb) {
  const out = [];
  const page = 1000;
  for (let from = 0; ; from += page) {
    let data;
    for (let i = 1; ; i++) {
      const res = await sb.from('audius').select('address, handle, user_id')
        .order('address', { ascending: true }).range(from, from + page - 1);
      if (!res.error) { data = res.data || []; break; }
      if (i >= CFG.retries) throw new Error(res.error.message);
      await sleep(CFG.retryBaseMs * i);
    }
    out.push(...data);
    if (data.length < page) break;
  }
  return out;
}

async function httpGet(pathname, params) {
  const qs = new URLSearchParams({ ...params, app_name: 'trend_checker' }).toString();
  const res = await fetch(`${API_BASE}${pathname}?${qs}`, {
    headers: { Accept: 'application/json' },
    signal: AbortSignal.timeout(CFG.timeoutMs),
  });
  if (!res.ok) throw Object.assign(new Error(`HTTP ${res.status}`), { status: res.status });
  return res.json();
}

const BOARDS = [
  { name: 'Trending', get: (offset, limit) => httpGet('/v1/tracks/trending', { time: CFG.time, limit, offset }) },
  { name: 'Underground', get: (offset, limit) => httpGet('/v1/tracks/trending/underground', { limit, offset }) },
];

async function loadBoard(board, depth) {
  const items = [];
  let size = CFG.pageSize;
  let offset = 0;
  let rateTries = 0;
  let failTries = 0;
  let lastErr = null;
  let failed = false;
  while (offset < depth) {
    const limit = Math.min(size, depth - offset);
    try {
      const res = await board.get(offset, limit);
      const list = (res && res.data) || [];
      rateTries = 0;
      failTries = 0;
      if (!list.length) break;
      items.push(...list);
      offset += list.length;
      if (VERBOSE) log(`[*] ${board.name}: ${items.length} tracks loaded.`);
    } catch (e) {
      lastErr = e;
      if (isRateLimit(e)) {
        if (++rateTries > CFG.rateRetries) { failed = true; break; }
        await sleep(CFG.rateWaitMs * rateTries);
        continue;
      }
      if (!isTransient(e)) { failed = true; break; }
      if (size > CFG.minPageSize) {
        size = Math.max(CFG.minPageSize, Math.floor(size / 2));
        if (VERBOSE) log(`[*] ${board.name}: slow response, page size now ${size}.`);
        continue;
      }
      if (++failTries >= CFG.retries) { failed = true; break; }
      await sleep(CFG.retryBaseMs * failTries);
    }
  }
  if (failed) log(`[!] ${board.name}: stopped at ${items.length} tracks (${isRateLimit(lastErr) ? '429' : errMsg(lastErr).slice(0, 60)}).`);
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
    const id = u.id !== undefined ? u.id : track && track.user_id;
    const handle = typeof u.handle === 'string' ? u.handle.toLowerCase() : '';
    return (id !== undefined && byId.get(String(id))) || (handle && byHandle.get(handle)) || null;
  };
}

function describe(hit) {
  const t = hit.track;
  const handle = (t.user && t.user.handle) || hit.acct.handle || hit.acct.address;
  const link = VERBOSE && t.permalink ? ` | https://audius.co${t.permalink}` : '';
  return `#${hit.rank} @${String(handle).replace(/^@/, '')} | ${t.title || 'untitled'} | ${t.id}${link}`;
}

function report(label, hits, from, to) {
  const part = hits.filter(h => h.rank >= from && h.rank <= to);
  if (!part.length) {
    log(`[*] ${label}: none`);
    return 0;
  }
  log(`[*] ${label}: ${part.length} found`);
  for (const h of part) log(`[*]   ${describe(h)}`);
  return part.length;
}

async function runTrendCheck() {
  const sb = connectSupabase();
  const accounts = await fetchAccounts(sb);
  const owner = indexAccounts(accounts);
  log('[*] Route: direct');

  let topTotal = 0;
  let bonusTotal = 0;
  const bonusLabel = `#${CFG.topDepth + 1}-${CFG.bonusDepth}`;
  for (const board of BOARDS) {
    const items = await loadBoard(board, CFG.bonusDepth);
    const hits = [];
    items.forEach((track, i) => {
      const acct = owner(track);
      if (acct) hits.push({ rank: i + 1, track, acct });
    });
    topTotal += report(`${board.name} Top ${CFG.topDepth}`, hits, 1, CFG.topDepth);
    if (CFG.bonusDepth > CFG.topDepth) {
      bonusTotal += report(`${board.name} ${bonusLabel}`, hits, CFG.topDepth + 1, CFG.bonusDepth);
    }
  }
  log(`[*] Trend check complete: ${topTotal} in Top ${CFG.topDepth}, ${bonusTotal} in ${bonusLabel}.`);
  return { top: topTotal, bonus: bonusTotal };
}

if (require.main === module) {
  runTrendCheck().catch(err => {
    console.error('[!] Fatal error:', err.message || err);
    process.exit(1);
  });
}

module.exports = { runTrendCheck, indexAccounts };
