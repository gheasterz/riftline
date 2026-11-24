// Riftline multiplayer server: serves the game and relays messages between a room's host and its guests.
// The match itself runs in the host's browser; this process only forwards packets, so a free Replit/Render/Glitch box is plenty.
// Run:  npm install && npm start     (PORT env var respected, default 3000)
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'index.html');   // the same file GitHub Pages serves

const server = http.createServer((req, res) => {
  const url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/rl-relay.json') { res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end('{"relay":true}'); return; }
  if (url === '/health') { res.writeHead(200); res.end('ok ' + rooms.size + ' rooms'); return; }
  if (url !== '/' && url !== '/index.html') { res.writeHead(404); res.end('Not found'); return; }
  fs.readFile(INDEX, (err, data) => {
    if (err) { res.writeHead(500); res.end('index.html missing'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

// rooms: code -> { host: ws, guests: Map<cid, ws> }
const rooms = new Map();
const CH = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const newCode = () => { for (;;) { let c = ''; for (let i = 0; i < 5; i++) c += CH[Math.floor(Math.random() * CH.length)]; if (!rooms.has(c)) return c; } };
let nextCid = 1;
const send = (ws, o) => { if (ws.readyState === 1) ws.send(typeof o === 'string' ? o : JSON.stringify(o)); };

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 256 * 1024 });
wss.on('connection', ws => {
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.once('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { ws.close(); return; }
    if (m.t === 'host') {
      const code = newCode(), room = { host: ws, guests: new Map() };
      rooms.set(code, room); send(ws, { t: 'ok', code });
      ws.on('message', raw2 => {
        let o; try { o = JSON.parse(raw2); } catch (e) { return; }
        const pkt = JSON.stringify({ t: 'msg', d: o.d });
        if (o.t === 'to') { const g = room.guests.get(o.cid); if (g) send(g, pkt); }
        else if (o.t === 'all') for (const [cid, g] of room.guests) if (cid !== o.ex) send(g, pkt);
      });
      ws.on('close', () => { rooms.delete(code); for (const g of room.guests.values()) { send(g, { t: 'msg', d: { e: 'bye' } }); g.close(); } });
      return;
    }
    if (m.t === 'join') {
      const code = String(m.code || '').toUpperCase(), room = rooms.get(code);
      if (!room) { send(ws, { t: 'err', msg: 'No game found with code ' + code }); ws.close(); return; }
      if (room.guests.size >= 9) { send(ws, { t: 'err', msg: 'That room is full' }); ws.close(); return; }
      const cid = 'c' + (nextCid++);
      room.guests.set(cid, ws); send(ws, { t: 'ok', cid }); send(room.host, { t: 'peer', cid });
      ws.on('message', raw2 => { let o; try { o = JSON.parse(raw2); } catch (e) { return; } if (o.t === 'up') send(room.host, { t: 'msg', cid, d: o.d }); });
      ws.on('close', () => { if (room.guests.get(cid) === ws) { room.guests.delete(cid); send(room.host, { t: 'leave', cid }); } });
      return;
    }
    ws.close();
  });
});
// drop dead sockets (closed laptops, lost wifi) so their slot frees up and a bot takes over
setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch (e) { } } }, 15000);

server.listen(PORT, () => console.log('Riftline server on http://localhost:' + PORT));
