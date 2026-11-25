// Сервер Crust: раздаёт index.html и держит WebSocket-комнаты (?room=1, ?room=pvp1 ...)
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PER_ROOM = +process.env.MAX_PER_ROOM || 40;
const INDEX = path.join(__dirname, 'index.html'); // игра лежит рядом с server.js

const rooms = new Map(); // имя комнаты -> Map(id -> клиент)
let nextId = 1;

const roomCounts = () => {
  const o = {};
  for (const [k, m] of rooms) o[k] = m.size;
  for (const k of ['1', '2', '3', '4', 'pvp1', 'pvp2', 'pvp3', 'pvp4']) if (!(k in o)) o[k] = 0;
  return o;
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  const cors = { 'Access-Control-Allow-Origin': '*' };
  if (url.pathname === '/health') { res.writeHead(200, cors); return res.end('ok'); }
  if (url.pathname === '/rooms') {
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify(roomCounts()));
  }
  if (url.pathname === '/' || url.pathname === '/index.html') {
    return fs.readFile(INDEX, (err, data) => {
      if (err) { res.writeHead(404); return res.end('index.html not found next to server.js'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
  }
  res.writeHead(404); res.end('Not found');
});

const wss = new WebSocketServer({ server, maxPayload: 4096 });

function send(c, o) { if (c.ws.readyState === 1) c.ws.send(JSON.stringify(o)); }
function broadcast(room, o, exceptId) {
  const m = rooms.get(room); if (!m) return;
  const s = JSON.stringify(o);
  for (const c of m.values()) if (c.id !== exceptId && c.ws.readyState === 1) c.ws.send(s);
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://x');
  const room = String(url.searchParams.get('room') || '1').replace(/[^\w-]/g, '').slice(0, 24) || '1';
  if (!rooms.has(room)) rooms.set(room, new Map());
  const members = rooms.get(room);
  if (members.size >= MAX_PER_ROOM) { ws.close(1013, 'room full'); return; }

  const c = { ws, id: nextId++, name: 'Игрок', s: null, joined: false, hp: 0, lastMsg: 0, tokens: 60 };
  members.set(c.id, c);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    // простой лимит сообщений: ~60 в секунду
    const now = Date.now();
    c.tokens = Math.min(60, c.tokens + (now - c.lastMsg) / 1000 * 60); c.lastMsg = now;
    if (--c.tokens < 0) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'join') {
      if (c.joined) return;
      c.joined = true;
      c.name = String(m.name || 'Игрок').slice(0, 24);
      const players = [];
      for (const p of members.values()) if (p.id !== c.id && p.joined) players.push({ id: p.id, name: p.name, s: p.s });
      send(c, { t: 'welcome', id: c.id, players });
      broadcast(room, { t: 'join', id: c.id, name: c.name }, c.id);
      return;
    }
    if (!c.joined) return;

    if (m.t === 's') {               // состояние игрока -> остальным
      const s = { ...m, id: c.id };
      c.s = s;
      broadcast(room, s, c.id);
    } else if (m.t === 'hit') {      // попадание по другому игроку -> урон цели
      const target = members.get(+m.id);
      const d = Math.max(0, Math.min(120, +m.d || 0));
      if (target && target !== c && d > 0) send(target, { t: 'dmg', d, from: c.id });
    } else if (m.t === 'died') {     // смерть -> лента убийств
      const killer = members.get(+m.by);
      broadcast(room, { t: 'feed', k: killer && killer !== c ? killer.name : 'Кто-то', v: c.name, kid: killer ? killer.id : 0, vid: c.id });
    } else if (m.t === 'fx') {       // взрывы и т.п. -> всем остальным
      broadcast(room, { ...m, id: c.id }, c.id);
    }
    // t:'sleep' — клиент закрывает страницу, 'leave' разошлётся при закрытии сокета
  });

  ws.on('close', () => {
    members.delete(c.id);
    if (c.joined) broadcast(room, { t: 'leave', id: c.id });
    if (!members.size) rooms.delete(room);
  });
  ws.on('error', () => {});
});

// выкидываем «мёртвые» соединения и не даём Render усыпить сокеты
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log('Crust server on :' + PORT));
