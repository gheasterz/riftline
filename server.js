// Riftline multiplayer server: serves the game and relays messages between a room's host and its guests.
// The match itself runs in the host's browser; this process only forwards packets, so a free Replit/Render/Glitch box is plenty.
// It also keeps a public list of open lobbies (host name, mode, players — never the room code) and passes
// "request to join" asks to the host, who accepts or declines them.
// Run:  npm install && npm start     (PORT env var respected, default 3000)
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
let pg = null; try { pg = require('pg'); } catch (e) { }   // optional: only needed for a permanent leaderboard

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'index.html');   // the same file GitHub Pages serves
const REQ_TIMEOUT = 45000;                          // a join request the host never answers expires after this
const MAX_PENDING = 6;                              // open join requests per room

// rooms: code -> { host, guests: Map<cid, ws>, pending: Map<cid, {ws, timer}>, rid, meta, born }
const rooms = new Map();
const CH = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const newCode = () => { for (;;) { let c = ''; for (let i = 0; i < 5; i++) c += CH[Math.floor(Math.random() * CH.length)]; if (!rooms.has(c)) return c; } };
let nextCid = 1;
const send = (ws, o) => { if (ws.readyState === 1) ws.send(typeof o === 'string' ? o : JSON.stringify(o)); };
const str = (v, n) => String(v == null ? '' : v).replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, n);
const int = (v, lo, hi, d) => { v = Math.round(+v); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d; };

// the public lobby list: only what the host chose to show, and never the code
function listRooms() {
  const out = [];
  for (const r of rooms.values()) {
    const m = r.meta; if (!m || !m.open) continue;
    out.push({ rid: r.rid, name: m.ln, host: m.h, mode: m.m, map: m.mp, n: m.n, cap: m.cap, inGame: m.g, born: r.born });
  }
  return out.sort((a, b) => b.born - a.born).slice(0, 60);
}

// ------------------------------------------------------------ GLOBAL LEADERBOARD (Quick Ranked + Standard Ranked)
// Each browser keeps a private random key; the board only ever sees a hash of it (pid), so nobody can post as someone else.
// With DATABASE_URL set (e.g. a free Neon Postgres) the board is permanent; without it, it lives in memory until a restart.
const LB = { pool: null, db: false, mem: new Map(), hits: new Map() };
const LB_MODES = ['rq', 'rs'], LB_TOP = 100, RANK_TOP = 24;
if (process.env.DATABASE_URL && pg) {
  LB.pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 3, idleTimeoutMillis: 10000 });
  LB.pool.on('error', e => console.log('leaderboard db:', e.message));
  LB.pool.query(`create table if not exists rl_lb (pid text not null, mode text not null, name text not null, idx int not null, rr int not null, peak int not null, lv int not null default 1, updated bigint not null, primary key (pid, mode));
    create table if not exists rl_users (uid text primary key, uname text unique not null, name text not null, pass text not null, rec text not null, pid text not null, created bigint not null);
    create table if not exists rl_sessions (tok text primary key, uid text not null, created bigint not null, seen bigint not null);
    create table if not exists rl_saves (uid text primary key, data text not null, sha text not null, at bigint not null, size int not null);
    delete from rl_lb where pid not in (select pid from rl_users);`)   // entries from before accounts are dropped: one entry per account from now on
    .then(() => { LB.db = true; console.log('leaderboard: database ready'); console.log('accounts: database ready'); }).catch(e => console.log('leaderboard: database error, using memory:', e.message));
} else console.log(process.env.DATABASE_URL ? 'leaderboard: run npm install (pg missing), using memory' : 'leaderboard: no DATABASE_URL, using memory');
async function lbSave(pid, mode, e) {
  if (e == null) { if (LB.db) await LB.pool.query('delete from rl_lb where pid = $1 and mode = $2', [pid, mode]); LB.mem.delete(pid + mode); return; }
  if (LB.db) await LB.pool.query(`insert into rl_lb (pid, mode, name, idx, rr, peak, lv, updated) values ($1,$2,$3,$4,$5,$6,$7,$8)
    on conflict (pid, mode) do update set name = excluded.name, idx = excluded.idx, rr = excluded.rr, peak = excluded.peak, lv = excluded.lv, updated = excluded.updated`, [pid, mode, e.n, e.i, e.r, e.k, e.lv, Date.now()]);
  else LB.mem.set(pid + mode, Object.assign({ pid, mode, t: Date.now() }, e));
}
async function lbTop(mode, me) {
  let rows, total, mine = null;
  if (LB.db) {
    rows = (await LB.pool.query('select pid, name as n, idx as i, rr as r, peak as k, lv from rl_lb where mode = $1 order by idx desc, rr desc, updated asc limit $2', [mode, LB_TOP])).rows;
    total = +(await LB.pool.query('select count(*) from rl_lb where mode = $1', [mode])).rows[0].count;
    if (me) { const m = (await LB.pool.query('select name as n, idx as i, rr as r, peak as k, lv, updated from rl_lb where pid = $1 and mode = $2', [me, mode])).rows[0];
      if (m) { const ahead = +(await LB.pool.query('select count(*) from rl_lb where mode = $1 and (idx > $2 or (idx = $2 and rr > $3) or (idx = $2 and rr = $3 and updated < $4))', [mode, m.i, m.r, m.updated])).rows[0].count; mine = { pos: ahead + 1, n: m.n, i: m.i, r: m.r, k: m.k, lv: m.lv }; } }
  } else {
    const all = [...LB.mem.values()].filter(e => e.mode === mode).sort((a, b) => b.i - a.i || b.r - a.r || a.t - b.t);
    total = all.length; rows = all.slice(0, LB_TOP);
    const at = me ? all.findIndex(e => e.pid === me) : -1; if (at >= 0) { const m = all[at]; mine = { pos: at + 1, n: m.n, i: m.i, r: m.r, k: m.k, lv: m.lv }; }
  }
  return { rows: rows.map((e, x) => ({ pos: x + 1, n: e.n, i: e.i, r: e.r, k: e.k, lv: e.lv, me: !!me && e.pid === me })), me: mine, total, persistent: LB.db };
}
function readBody(req, max) {
  return new Promise((ok, no) => { let b = ''; req.on('data', c => { b += c; if (b.length > max) { no(new Error('too big')); req.destroy(); } }); req.on('end', () => ok(b)); req.on('error', no); });
}
function json(res, code, o) { res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(o)); }
async function lbRoute(req, res, url) {
  try {
    if (url === '/lb/submit' && req.method === 'POST') {
      const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
      const now = Date.now(), h = (LB.hits.get(ip) || []).filter(t => now - t < 60000); if (h.length >= 20) return json(res, 429, { err: 'slow down' }); h.push(now); LB.hits.set(ip, h);
      const b = JSON.parse(await readBody(req, 4096) || '{}'), u = await authed(req);
      if (!u) return json(res, 401, { err: 'Sign in to appear on the leaderboard' });
      const pid = u.pid, name = u.name, lv = int(b.lv, 1, 1000, 1);
      for (const mode of LB_MODES) {
        const r = b[mode]; if (r === undefined) continue;
        const i = int(r && r.i, -1, RANK_TOP, -1);   // -1 = unranked / still in placements: not listed
        await lbSave(pid, mode, i < 0 ? null : { n: name, i, r: int(r.r, 0, 99999, 0), k: Math.max(i, int(r.k, 0, RANK_TOP, i)), lv });
      }
      return json(res, 200, { pid });
    }
    if (url === '/lb' && req.method === 'GET') {
      const q = new URL(req.url, 'http://x').searchParams, mode = LB_MODES.includes(q.get('mode')) ? q.get('mode') : 'rq';
      const me = /^[0-9a-f]{20}$/.test(q.get('me') || '') ? q.get('me') : null;
      return json(res, 200, await lbTop(mode, me));
    }
  } catch (e) { return json(res, 500, { err: 'leaderboard unavailable' }); }
  json(res, 404, { err: 'not found' });
}

// ------------------------------------------------------------ ACCOUNTS + CLOUD SAVES
// Username + password (scrypt-hashed), a one-time recovery code shown at sign-up, and session tokens (only their hash is
// stored). Each account keeps one cloud save: the same checksummed file the game's Export save makes. The leaderboard
// lists accounts only, so a player appears once however many devices they use.
const A = { users: new Map(), byName: new Map(), sessions: new Map(), saves: new Map(), fails: new Map(), hits: new Map() };
const SAVE_MAX = 3 * 1024 * 1024, SESSION_DAYS = 120;
const sha = t => crypto.createHash('sha256').update(t).digest('hex');
const scrypt = (pw, salt) => new Promise((ok, no) => crypto.scrypt(pw, salt, 32, { N: 16384, r: 8, p: 1 }, (e, k) => e ? no(e) : ok(k.toString('hex'))));
async function hashPw(pw) { const salt = crypto.randomBytes(16).toString('hex'); return salt + ':' + await scrypt(pw, salt); }
async function checkPw(pw, stored) { const [salt, h] = String(stored).split(':'); if (!salt || !h) return false; const k = await scrypt(pw, salt); return crypto.timingSafeEqual(Buffer.from(k, 'hex'), Buffer.from(h, 'hex')); }
const newRecovery = () => { const C = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; let s = ''; for (let i = 0; i < 12; i++) s += C[crypto.randomInt(C.length)]; return s.slice(0, 4) + '-' + s.slice(4, 8) + '-' + s.slice(8); };
const normRec = c => String(c || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
const USER_RE = /^[A-Za-z0-9_]{3,16}$/;
const db = () => LB.db;
async function userByName(u) { const k = String(u || '').toLowerCase(); if (db()) return (await LB.pool.query('select * from rl_users where uname = $1', [k])).rows[0] || null; const id = A.byName.get(k); return id ? A.users.get(id) : null; }
async function userById(id) { if (db()) return (await LB.pool.query('select * from rl_users where uid = $1', [id])).rows[0] || null; return A.users.get(id) || null; }
async function putUser(u) {
  if (db()) await LB.pool.query(`insert into rl_users (uid, uname, name, pass, rec, pid, created) values ($1,$2,$3,$4,$5,$6,$7) on conflict (uid) do update set pass = excluded.pass, rec = excluded.rec, name = excluded.name`, [u.uid, u.uname, u.name, u.pass, u.rec, u.pid, u.created]);
  else { A.users.set(u.uid, u); A.byName.set(u.uname, u.uid); }
}
async function newSession(uid) {
  const tok = crypto.randomBytes(32).toString('base64url'), h = sha(tok), now = Date.now();
  if (db()) await LB.pool.query('insert into rl_sessions (tok, uid, created, seen) values ($1,$2,$3,$3)', [h, uid, now]); else A.sessions.set(h, { uid, seen: now });
  return tok;
}
async function dropSessions(uid, except) {
  if (db()) await LB.pool.query('delete from rl_sessions where uid = $1 and tok <> $2', [uid, except || '']);
  else for (const [k, v] of A.sessions) if (v.uid === uid && k !== except) A.sessions.delete(k);
}
async function authed(req) {
  const m = /^Bearer ([A-Za-z0-9_-]{20,100})$/.exec(req.headers.authorization || ''); if (!m) return null;
  const h = sha(m[1]), now = Date.now(), old = now - SESSION_DAYS * 864e5; let uid = null;
  if (db()) { const r = (await LB.pool.query('update rl_sessions set seen = $2 where tok = $1 and seen > $3 returning uid', [h, now, old])).rows[0]; uid = r && r.uid; }
  else { const s = A.sessions.get(h); if (s && s.seen > old) { s.seen = now; uid = s.uid; } }
  if (!uid) return null; const u = await userById(uid); return u ? Object.assign({ tokHash: h }, u) : null;
}
async function saveMeta(uid) { if (db()) { const r = (await LB.pool.query('select sha, at, size from rl_saves where uid = $1', [uid])).rows[0]; return r ? { sha: r.sha, at: +r.at, size: r.size } : null; } const s = A.saves.get(uid); return s ? { sha: s.sha, at: s.at, size: s.size } : null; }
async function saveGet(uid) { if (db()) { const r = (await LB.pool.query('select data, sha, at from rl_saves where uid = $1', [uid])).rows[0]; return r ? { data: r.data, sha: r.sha, at: +r.at } : null; } return A.saves.get(uid) || null; }
async function savePut(uid, data, s, at) { if (db()) await LB.pool.query('insert into rl_saves (uid, data, sha, at, size) values ($1,$2,$3,$4,$5) on conflict (uid) do update set data = excluded.data, sha = excluded.sha, at = excluded.at, size = excluded.size', [uid, data, s, at, data.length]); else A.saves.set(uid, { data, sha: s, at, size: data.length }); }
function ipOf(req) { return String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim(); }
function limited(map, key, max, ms) { const now = Date.now(), h = (map.get(key) || []).filter(t => now - t < ms); if (h.length >= max) { map.set(key, h); return true; } h.push(now); map.set(key, h); return false; }
const pub = u => ({ name: u.name, created: +u.created });
// a cloud save must be a real Riftline save file whose checksum matches its data, like Import save requires
function checkSaveFile(text) {
  if (typeof text !== 'string' || !text.length || text.length > SAVE_MAX) return 'The save is too large';
  let f; try { f = JSON.parse(text); } catch (e) { return 'The save is not valid JSON'; }
  if (!f || f.format !== 'riftline-save' || !f.data || typeof f.data !== 'object' || Array.isArray(f.data)) return 'Not a Riftline save';
  if (Object.keys(f.data).some(k => !k.startsWith('riftline.'))) return 'The save contains keys Riftline does not use';
  if (sha(JSON.stringify(f.data)) !== f.sha256) return 'The save checksum does not match';
  if (!f.data['riftline.profile'] || typeof f.data['riftline.profile'] !== 'object') return 'The save has no profile';
  return null;
}
async function acctRoute(req, res, url) {
  try {
    const ip = ipOf(req);
    if (req.method === 'POST' && (url === '/acct/signup' || url === '/acct/login' || url === '/acct/recover')) {
      if (limited(A.hits, ip, 30, 600000)) return json(res, 429, { err: 'Too many attempts. Wait a few minutes and try again.' });
      const b = JSON.parse(await readBody(req, 4096) || '{}'), user = String(b.user || '').trim(), pass = String(b.pass || '');
      if (!USER_RE.test(user)) return json(res, 400, { err: 'Usernames are 3–16 letters, numbers or _' });
      if (pass.length < 6 || pass.length > 128) return json(res, 400, { err: 'Passwords are 6–128 characters' });
      const lower = user.toLowerCase();
      if (url === '/acct/signup') {
        if (await userByName(lower)) return json(res, 409, { err: 'That username is taken' });
        const rec = newRecovery(), uid = crypto.randomBytes(9).toString('hex');
        const u = { uid, uname: lower, name: user, pass: await hashPw(pass), rec: await hashPw(normRec(rec)), pid: sha('acct:' + uid).slice(0, 20), created: Date.now() };
        await putUser(u);
        return json(res, 200, { token: await newSession(uid), user: pub(u), pid: u.pid, recovery: rec, save: null });
      }
      if (limited(A.fails, lower, 8, 900000)) return json(res, 429, { err: 'Too many tries for this account. Wait 15 minutes.' });
      const u = await userByName(lower);
      if (url === '/acct/login') {
        if (!u || !await checkPw(pass, u.pass)) return json(res, 401, { err: 'Wrong username or password' });
        A.fails.delete(lower);
        return json(res, 200, { token: await newSession(u.uid), user: pub(u), pid: u.pid, save: await saveMeta(u.uid) });
      }
      // recover: username + recovery code + new password → new recovery code, other sessions signed out
      if (!u || !await checkPw(normRec(b.code), u.rec)) return json(res, 401, { err: 'That recovery code doesn’t match this username' });
      const rec = newRecovery(); u.pass = await hashPw(pass); u.rec = await hashPw(normRec(rec)); await putUser(u); await dropSessions(u.uid); A.fails.delete(lower);
      return json(res, 200, { token: await newSession(u.uid), user: pub(u), pid: u.pid, recovery: rec, save: await saveMeta(u.uid) });
    }
    const u = await authed(req);
    if (!u) return json(res, 401, { err: 'Signed out' });
    if (url === '/acct/me' && req.method === 'GET') return json(res, 200, { user: pub(u), pid: u.pid, save: await saveMeta(u.uid) });
    if (url === '/acct/logout' && req.method === 'POST') { if (db()) await LB.pool.query('delete from rl_sessions where tok = $1', [u.tokHash]); else A.sessions.delete(u.tokHash); return json(res, 200, { ok: true }); }
    if (url === '/acct/password' && req.method === 'POST') {
      const b = JSON.parse(await readBody(req, 4096) || '{}'), np = String(b.pass || '');
      if (!await checkPw(String(b.old || ''), u.pass)) return json(res, 401, { err: 'Your current password is wrong' });
      if (np.length < 6 || np.length > 128) return json(res, 400, { err: 'Passwords are 6–128 characters' });
      u.pass = await hashPw(np); await putUser(u); await dropSessions(u.uid, u.tokHash); return json(res, 200, { ok: true });
    }
    if (url === '/acct/save' && req.method === 'GET') { const s = await saveGet(u.uid); return json(res, 200, s ? { data: s.data, sha: s.sha, at: s.at } : { data: null }); }
    if (url === '/acct/save' && req.method === 'PUT') {
      if (limited(A.hits, 'save:' + u.uid, 30, 60000)) return json(res, 429, { err: 'Saving too often' });
      const b = JSON.parse(await readBody(req, SAVE_MAX + 4096) || '{}'), bad = checkSaveFile(b.data);
      if (bad) return json(res, 400, { err: bad });
      const cur = await saveMeta(u.uid), base = +b.base || 0, force = !!b.force;
      if (cur && !force && base !== cur.at) return json(res, 409, { err: 'The cloud save changed on another device', at: cur.at, sha: cur.sha });
      const f = JSON.parse(b.data), at = Math.max(Date.now(), cur ? cur.at + 1 : 0);
      await savePut(u.uid, b.data, f.sha256, at);
      return json(res, 200, { at, sha: f.sha256 });
    }
  } catch (e) { console.log('accounts:', e.message); return json(res, 500, { err: 'The account service had a problem. Try again.' }); }
  json(res, 404, { err: 'not found' });
}
// hourly: forget rate-limit buckets
setInterval(() => {
  const now = Date.now(); for (const M of [LB.hits, A.hits, A.fails]) for (const [k, h] of M) if (!h.some(t => now - t < 900000)) M.delete(k);
  if (LB.db) LB.pool.query('delete from rl_sessions where seen < $1', [now - SESSION_DAYS * 864e5]).catch(() => { });
}, 3600000);

const server = http.createServer((req, res) => {
  const url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/lb' || url === '/lb/submit') { lbRoute(req, res, url); return; }
  if (url.startsWith('/acct/')) { acctRoute(req, res, url); return; }
  if (url === '/rl-relay.json') { res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end('{"relay":true,"rooms":true,"lb":true,"acct":true}'); return; }
  if (url === '/rooms') { res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify({ rooms: listRooms() })); return; }
  if (url === '/health') { res.writeHead(200); res.end('ok ' + rooms.size + ' rooms'); return; }
  if (url !== '/' && url !== '/index.html') { res.writeHead(404); res.end('Not found'); return; }
  fs.readFile(INDEX, (err, data) => {
    if (err) { res.writeHead(500); res.end('index.html missing'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

// a guest socket becomes part of a room (joined with the code, or accepted by the host)
function addGuest(room, cid, ws) {
  room.guests.set(cid, ws); send(ws, { t: 'ok', cid }); send(room.host, { t: 'peer', cid });
  ws.on('message', raw2 => { let o; try { o = JSON.parse(raw2); } catch (e) { return; } if (o.t === 'up') send(room.host, { t: 'msg', cid, d: o.d }); });
  ws.on('close', () => { if (room.guests.get(cid) === ws) { room.guests.delete(cid); send(room.host, { t: 'leave', cid }); } });
}
function refuse(ws, msg) { send(ws, { t: 'err', msg }); try { ws.close(); } catch (e) { } }
function dropPending(room, cid, msg) {
  const p = room.pending.get(cid); if (!p) return null;
  room.pending.delete(cid); clearTimeout(p.timer);
  if (msg) refuse(p.ws, msg);
  return p;
}

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 256 * 1024 });
wss.on('connection', ws => {
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.once('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { ws.close(); return; }
    if (m.t === 'host') {
      const code = newCode(), room = { host: ws, guests: new Map(), pending: new Map(), rid: crypto.randomBytes(6).toString('hex'), meta: null, born: Date.now() };
      rooms.set(code, room); send(ws, { t: 'ok', code });
      ws.on('message', raw2 => {
        let o; try { o = JSON.parse(raw2); } catch (e) { return; }
        if (o.t === 'to') { const g = room.guests.get(o.cid); if (g) send(g, JSON.stringify({ t: 'msg', d: o.d })); }
        else if (o.t === 'all') { const pkt = JSON.stringify({ t: 'msg', d: o.d }); for (const [cid, g] of room.guests) if (cid !== o.ex) send(g, pkt); }
        else if (o.t === 'meta') {   // what the lobby browser shows for this room
          const d = o.d || {};
          room.meta = { h: str(d.h, 16) || 'Player', ln: str(d.ln, 28), m: str(d.m, 24), mp: str(d.mp, 24), n: int(d.n, 1, 10, 1), cap: int(d.cap, 1, 10, 10), g: !!d.g, open: !!d.open };
          if (!room.meta.open) for (const cid of [...room.pending.keys()]) { dropPending(room, cid, 'That lobby is no longer listed.'); send(ws, { t: 'reqx', cid }); }
        }
        else if (o.t === 'acc') { const p = dropPending(room, o.cid); if (p && p.ws.readyState === 1) { if (room.guests.size >= 9) refuse(p.ws, 'That room is full.'); else addGuest(room, o.cid, p.ws); } }
        else if (o.t === 'dec') dropPending(room, o.cid, str(o.msg, 120) || 'The host declined your request.');
      });
      ws.on('close', () => {
        rooms.delete(code);
        for (const cid of [...room.pending.keys()]) dropPending(room, cid, 'That lobby closed.');
        for (const g of room.guests.values()) { send(g, { t: 'msg', d: { e: 'bye' } }); g.close(); }
      });
      return;
    }
    if (m.t === 'join') {
      const code = String(m.code || '').toUpperCase(), room = rooms.get(code);
      if (!room) { refuse(ws, 'No game found with code ' + code); return; }
      if (room.guests.size >= 9) { refuse(ws, 'That room is full'); return; }
      addGuest(room, 'c' + (nextCid++), ws);
      return;
    }
    if (m.t === 'req') {   // ask to join a listed lobby: held here until the host answers
      let room = null; for (const r of rooms.values()) if (r.rid === m.rid) { room = r; break; }
      if (!room || !room.meta || !room.meta.open) { refuse(ws, 'That lobby is no longer open.'); return; }
      if (room.meta.g) { refuse(ws, 'That lobby is in a match right now. Try again when it ends.'); return; }
      if (room.guests.size >= 9 || room.meta.n >= room.meta.cap) { refuse(ws, 'That lobby is full.'); return; }
      if (room.pending.size >= MAX_PENDING) { refuse(ws, 'That host has too many requests right now. Try again shortly.'); return; }
      const cid = 'c' + (nextCid++);
      const timer = setTimeout(() => { if (dropPending(room, cid, 'The host didn’t answer your request.')) send(room.host, { t: 'reqx', cid }); }, REQ_TIMEOUT);
      room.pending.set(cid, { ws, timer });
      send(room.host, { t: 'req', cid, name: str(m.name, 16) || 'Player', lv: int(m.lv, 1, 1000, 1), rq: int(m.rq, -1, 99, -1), rs: int(m.rs, -1, 99, -1) });
      ws.on('close', () => { if (dropPending(room, cid)) send(room.host, { t: 'reqx', cid }); });   // they cancelled
      return;
    }
    ws.close();
  });
});
// drop dead sockets (closed laptops, lost wifi) so their slot frees up and a bot takes over
setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch (e) { } } }, 15000);

server.listen(PORT, () => console.log('Riftline server on http://localhost:' + PORT));
