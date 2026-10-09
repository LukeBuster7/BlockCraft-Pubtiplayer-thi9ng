// Blockcraft multiplayer server: relays player positions, block edits and chat,
// and remembers every block change so new players see the same world.
const { WebSocketServer } = require('ws');
const fs = require('fs');
const crypto = require('crypto');

const PORT = +process.env.PORT || 8080;
const MAX_PLAYERS = +process.env.MAX_PLAYERS || 16;
const SAVE_FILE = process.env.SAVE_FILE || 'world.json';
const PASSWORD = process.env.PASSWORD || '';              // empty = anyone can join
const ORIGIN = process.env.ALLOWED_ORIGIN || '';          // e.g. https://yourname.github.io  (empty = allow all)
const WORLD = {
  name: (process.env.WORLD_NAME || 'Blockcraft Server').slice(0, 32),
  seed: String(process.env.SEED || '12345'),
  type: ['normal', 'flat', 'sky'].includes(process.env.WORLD_TYPE) ? process.env.WORLD_TYPE : 'normal',
  mode: ['creative', 'survival'].includes(process.env.GAME_MODE) ? process.env.GAME_MODE : 'survival',
  day: process.env.ALWAYS_DAY === '1',
};

let edits = {};
try { edits = JSON.parse(fs.readFileSync(SAVE_FILE, 'utf8')); } catch (e) {}
let dirty = false;
setInterval(() => { if (dirty) { dirty = false; fs.writeFile(SAVE_FILE, JSON.stringify(edits), () => {}); } }, 10000);
process.on('SIGTERM', () => { try { fs.writeFileSync(SAVE_FILE, JSON.stringify(edits)); } catch (e) {} process.exit(0); });

const wss = new WebSocketServer({
  port: PORT,
  maxPayload: 32768,
  verifyClient: (info) => !ORIGIN || info.origin === ORIGIN,
});
const h256 = (s) => crypto.createHash('sha256').update(String(s)).digest();
const fails = new Map();                                       // ip -> { n, t } wrong-password attempts
const ipOf = (req) => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
const players = new Map();   // ws -> { id, name, p }
const items = new Map();     // dropped items shared by everyone: u -> { u, id, n, x, y, z, v }
let nextId = 1;

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const broadcast = (o, except) => { for (const ws of players.keys()) if (ws !== except) send(ws, o); };
const clean = (s, n) => String(s).replace(/[^\w .\-]/g, '').trim().slice(0, n);
const num = (v) => typeof v === 'number' && isFinite(v);

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let me = null;
  let tokens = 40, last = Date.now();

  ws.on('message', (raw) => {
    const now = Date.now();                                  // simple rate limit: 40 msgs/sec
    tokens = Math.min(40, tokens + (now - last) / 25); last = now;
    if (--tokens < 0) return;
    if (raw.length > 2048 && !raw.toString('utf8', 0, 12).startsWith('{"t":"skin"')) return;   // only skins may be large
    let m; try { m = JSON.parse(raw); } catch (e) { return; }

    if (!me) {
      if (m.t !== 'join') return;
      if (PASSWORD) {
        const ip = ipOf(req), f = fails.get(ip) || { n: 0, t: 0 };
        if (f.n >= 5 && Date.now() - f.t < 600000) { send(ws, { t: 'badpass', msg: 'Too many wrong passwords. Try again in a few minutes.' }); return ws.close(); }
        if (typeof m.pass !== 'string' || !crypto.timingSafeEqual(h256(m.pass), h256(PASSWORD))) {
          fails.set(ip, { n: f.n + 1, t: Date.now() }); send(ws, { t: 'badpass', msg: 'Wrong password.' }); return ws.close();
        }
        fails.delete(ip);
      }
      if (players.size >= MAX_PLAYERS) { send(ws, { t: 'full', msg: 'Server is full (' + MAX_PLAYERS + ' players).' }); return ws.close(); }
      me = { id: nextId++, name: clean(m.name || 'Player', 16) || 'Player', p: {} };
      players.set(ws, me);
      send(ws, { t: 'welcome', id: me.id, now: Date.now(), world: WORLD, edits, items: [...items.values()],
        peers: [...players.values()].filter(q => q !== me).map(q => ({ id: q.id, name: q.name, p: q.p, skin: q.skin })) });
      broadcast({ t: 'join', id: me.id, name: me.name }, ws);
      console.log(`+ ${me.name} (${players.size}/${MAX_PLAYERS})`);
      return;
    }

    if (m.t === 'pres' && m.p && typeof m.p === 'object') {
      const p = {};
      if (typeof m.p.n === 'string') p.n = clean(m.p.n, 16);
      if (Array.isArray(m.p.p) && m.p.p.length === 3 && m.p.p.every(num)) p.p = m.p.p;
      for (const k of ['y', 't', 'c', 'a', 'e', 'g']) if (num(m.p[k])) p[k] = m.p[k];
      Object.assign(me.p, p);
      broadcast({ t: 'pres', id: me.id, p }, ws);
    } else if (m.t === 'edit' && Array.isArray(m.e) && m.e.length === 4) {
      const [x, y, z, v] = m.e;
      if (![x, y, z, v].every(Number.isInteger) || y < 0 || y > 255 || v < 0 || v > 65535 || Math.abs(x) > 1e7 || Math.abs(z) > 1e7) return;
      edits[x + ',' + y + ',' + z] = v; dirty = true;
      broadcast({ t: 'edit', e: [x, y, z, v] }, ws);
    } else if (m.t === 'skin' && typeof m.img === 'string') {                         // a player's skin (64x64 PNG)
      const nowMs = Date.now();
      if (nowMs - (me.skinT || 0) < 2000 || m.img.length > 24000 || !/^data:image\/png;base64,[A-Za-z0-9+\/=]+$/.test(m.img)) return;
      me.skinT = nowMs; me.skin = m.img;
      broadcast({ t: 'skin', id: me.id, img: m.img }, ws);
    } else if (m.t === 'drop' && m.item && typeof m.item.u === 'string') {          // someone dropped an item
      const q = m.item, u = clean(q.u, 24);
      if (!u || items.has(u) || items.size >= 400) return;
      if (![q.id, q.n].every(Number.isInteger) || q.id < 1 || q.id > 2047 || q.n < 1 || q.n > 64) return;
      if (![q.x, q.y, q.z].every(num) || !Array.isArray(q.v) || q.v.length !== 3 || !q.v.every(num)) return;
      const it = { u, id: q.id, n: q.n, x: q.x, y: q.y, z: q.z, v: q.v.map(a => Math.max(-20, Math.min(20, a))) };
      items.set(u, it); setTimeout(() => items.delete(u), 300000);
      broadcast({ t: 'drop', item: it }, ws);
    } else if (m.t === 'take' && typeof m.u === 'string') {                         // first player to ask gets the item
      const u = clean(m.u, 24);
      if (items.delete(u)) broadcast({ t: 'took', u, by: me.id });
    } else if (m.t === 'hit') {                                                      // player-vs-player damage
      const target = [...players.entries()].find(([w, q]) => q.id === m.to);
      if (!target || target[0] === ws) return;
      const nowMs = Date.now(); me.hits = (me.hits || []).filter(t => nowMs - t < 1000);
      if (me.hits.length >= 8) return; me.hits.push(nowMs);
      const a = me.p.p, b = target[1].p.p;
      if (a && b && Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]) > 12) return;  // too far away
      const dmg = Math.max(1, Math.min(40, Math.round(+m.dmg || 1)));
      const kb = Array.isArray(m.kb) && m.kb.length === 2 && m.kb.every(num) ? m.kb.map(v => Math.max(-1, Math.min(1, v))) : [0, 0];
      send(target[0], { t: 'hit', from: me.id, dmg, kb });
    } else if (m.t === 'died') {
      const by = [...players.values()].find(q => q.id === m.by);
      broadcast({ t: 'chat', d: { n: '', t: by && by !== me ? `${me.name} was slain by ${by.name}` : `${me.name} died` } });
    } else if (m.t === 'chat' && m.d && typeof m.d.t === 'string') {
      const text = m.d.t.replace(/[\u0000-\u001f]/g, '').slice(0, 100);
      if (text) { broadcast({ t: 'chat', d: { n: me.name, t: text } }, ws); console.log(`<${me.name}> ${text}`); }
    }
  });

  ws.on('close', () => {
    if (me) { players.delete(ws); broadcast({ t: 'left', id: me.id }); console.log(`- ${me.name} (${players.size}/${MAX_PLAYERS})`); }
  });
  ws.on('error', () => {});
});

setInterval(() => {                                          // drop dead connections
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); }
}, 30000);

console.log(`Blockcraft server "${WORLD.name}" listening on port ${PORT} (${WORLD.type}, ${WORLD.mode})${PASSWORD ? ' - password protected' : ''}`);
