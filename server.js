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

const server = http.createServer((req, res) => {
  const url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/rl-relay.json') { res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end('{"relay":true,"rooms":true}'); return; }
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
