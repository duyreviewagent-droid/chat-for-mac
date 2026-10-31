// Chat on the web: same rooms, same relay and same messages as the Mac app.
import * as THREE from 'three';
import { RoundedBoxGeometry } from 'three/addons/geometries/RoundedBoxGeometry.js';

const $ = (s) => document.querySelector(s);
const PREFIX = 'chatapp-vx7q/v1';
const BROKERS = ['wss://broker.hivemq.com:8884/mqtt', 'wss://test.mosquitto.org:8081/mqtt'];
const LOBBY = `${PREFIX}/lobby`;
const topic = (code, kind) => `${PREFIX}/r/${code}/${kind}`;

// ---------- looks (same lists as the Mac app) ----------
const skinColors = [0xF6D5BA, 0xE8B48F, 0xC98D5E, 0xA0673F, 0x7A4A2A, 0x4B2E1C];
const hairColors = [0x1E1A18, 0x4A2E1E, 0x8B5A2B, 0xD9B26A, 0xB5432F, 0x9AA0A6, 0xE85DAF, 0x3D7BE0];
const shirtColors = [0xE74C3C, 0xF39C12, 0xF1C40F, 0x2ECC71, 0x1ABC9C, 0x3498DB, 0x9B59B6, 0xEC6FA9, 0xECF0F1, 0x2C3E50];
const pantsColors = [0x2C3E50, 0x3B4A5A, 0x1F4E79, 0x5D4037, 0x7F8C8D, 0x151515];
const hairStyles = ['Short', 'Long', 'Spiky', 'Bun', 'Bald', 'Cap'];
const palettes = { skin: skinColors, hair: hairColors, shirt: shirtColors, pants: pantsColors };

const clampI = (v, n) => Math.min(Math.max(Math.round(Number(v) || 0), 0), n - 1);
function cleanLook(l = {}) {
  return { skin: clampI(l.skin, skinColors.length), hair: clampI(l.hair, hairColors.length), hairStyle: clampI(l.hairStyle, hairStyles.length),
           shirt: clampI(l.shirt, shirtColors.length), pants: clampI(l.pants, pantsColors.length) };
}

const roomStyles = [
  { letter: 'L', key: 'LOUNGE', name: 'Lounge', emoji: '🛋️' }, { letter: 'G', key: 'GAMING', name: 'Gaming', emoji: '🎮' },
  { letter: 'M', key: 'MUSIC', name: 'Music', emoji: '🎧' }, { letter: 'C', key: 'CHILL', name: 'Chill', emoji: '🌙' },
  { letter: 'A', key: 'ART', name: 'Art', emoji: '🎨' }, { letter: 'S', key: 'SPORTS', name: 'Sports', emoji: '⚽' },
];
const styleEmoji = (code) => roomStyles.find((s) => s.letter === code[0])?.emoji ?? '💬';

const themes = {
  LOUNGE: { wall: 0xE9D8C4, trim: 0x6B4A33, floor: 0x9C6B43, kind: 0, rug: 0xA23B3B, couch: 0x3E6E8E, light: 0xFFE2B8, posters: ['☕️', '🛋️', '🌿'], night: false },
  GAMING: { wall: 0x2A2340, trim: 0x7C4DFF, floor: 0x23202E, kind: 2, rug: 0x00C2FF, couch: 0x1B1B24, light: 0xB79CFF, posters: ['🎮', '👾', '🕹️'], night: true },
  MUSIC: { wall: 0x2D4C6E, trim: 0x172B40, floor: 0x6F4A30, kind: 0, rug: 0xF2B134, couch: 0x8E2C2C, light: 0xFFD3A0, posters: ['🎸', '🎧', '🎹'], night: false },
  CHILL: { wall: 0x1F2B45, trim: 0x10172A, floor: 0x3B3F52, kind: 1, rug: 0x6A5ACD, couch: 0x4F6D7A, light: 0x9FB8FF, posters: ['🌙', '✨', '🪐'], night: true },
  ART: { wall: 0xF4F1EA, trim: 0xC9C2B5, floor: 0xC8B79E, kind: 0, rug: 0xF25F5C, couch: 0x70C1B3, light: 0xFFFFFF, posters: ['🎨', '🖼️', '🖌️'], night: false },
  SPORTS: { wall: 0x2F5D3A, trim: 0x1D3A24, floor: 0x7A5233, kind: 0, rug: 0xEDEDED, couch: 0x8B1E1E, light: 0xFFE7C2, posters: ['⚽', '🏀', '🏆'], night: false },
};
function themeFor(code) {
  const s = roomStyles.find((r) => r.letter === code[0]);
  if (s) return themes[s.key];
  const keys = Object.keys(themes).sort();
  let h = 0;
  for (const ch of code) h = (h * 31 + ch.codePointAt(0)) & 0xFFFF;
  return themes[keys[h % keys.length]];
}

// ---------- tiny MQTT 3.1.1 client over WebSocket ----------
const enc = new TextEncoder(), dec = new TextDecoder();
class MQTT {
  constructor() {
    this.id = 'chat-' + Array.from(crypto.getRandomValues(new Uint8Array(7)), (b) => b.toString(16).padStart(2, '0')).join('');
    this.subs = new Set(); this.connected = false; this.broker = 0; this.pid = 1; this.buf = new Uint8Array(0);
    this.onMessage = null; this.onConnected = null;
  }
  connect() { this.open(); setInterval(() => this.send([0xC0, 0]), 25000); }
  open() {
    this.gotAck = false; this.buf = new Uint8Array(0);
    const ws = new WebSocket(BROKERS[this.broker], ['mqtt']);
    ws.binaryType = 'arraybuffer';
    this.ws = ws;
    ws.onopen = () => {
      const vh = [...this.str('MQTT'), 4, 0x02, 0, 60], pl = this.str(this.id);
      this.send([0x10, ...this.len(vh.length + pl.length), ...vh, ...pl]);
    };
    ws.onmessage = (e) => {
      const d = new Uint8Array(e.data), n = new Uint8Array(this.buf.length + d.length);
      n.set(this.buf); n.set(d, this.buf.length); this.buf = n;
      this.parse();
    };
    ws.onclose = () => {
      if (this.ws !== ws) return;
      if (this.connected) { this.connected = false; this.onConnected?.(false); }
      if (!this.gotAck) this.broker = (this.broker + 1) % BROKERS.length;
      setTimeout(() => this.open(), 2000);
    };
  }
  parse() {
    while (this.buf.length >= 2) {
      let mult = 1, len = 0, i = 1;
      for (;;) {
        if (i >= this.buf.length) return;
        const b = this.buf[i++];
        len += (b & 127) * mult; mult *= 128;
        if (!(b & 128)) break;
        if (i > 4) { this.buf = new Uint8Array(0); return; }
      }
      const total = i + len;
      if (this.buf.length < total) return;
      const h = this.buf[0], body = this.buf.slice(i, total);
      this.buf = this.buf.slice(total);
      this.handle(h, body);
    }
  }
  handle(h, body) {
    const type = h >> 4;
    if (type === 2) {
      if (body[1] !== 0) { this.ws.close(); return; }
      this.gotAck = true; this.connected = true;
      for (const t of this.subs) this.sendSub(t);
      this.onConnected?.(true);
    } else if (type === 3) {
      const tl = (body[0] << 8) | body[1];
      const t = dec.decode(body.slice(2, 2 + tl));
      let idx = 2 + tl;
      if ((h >> 1) & 3) idx += 2;
      this.onMessage?.(t, dec.decode(body.slice(idx)));
    }
  }
  str(s) { const u = enc.encode(s); return [u.length >> 8, u.length & 255, ...u]; }
  len(n) { const o = []; do { let b = n % 128; n = Math.floor(n / 128); if (n > 0) b |= 128; o.push(b); } while (n > 0); return o; }
  nextId() { this.pid = this.pid >= 65535 ? 1 : this.pid + 1; return [this.pid >> 8, this.pid & 255]; }
  send(a) { if (this.ws?.readyState === 1) this.ws.send(new Uint8Array(a)); }
  sendSub(t) { const b = [...this.nextId(), ...this.str(t), 0]; this.send([0x82, ...this.len(b.length), ...b]); }
  subscribe(t) { this.subs.add(t); if (this.connected) this.sendSub(t); }
  unsubscribe(t) {
    this.subs.delete(t);
    if (!this.connected) return;
    const b = [...this.nextId(), ...this.str(t)];
    this.send([0xA2, ...this.len(b.length), ...b]);
  }
  publish(t, payload, retain = false) {
    if (!this.connected) return;
    const p = enc.encode(typeof payload === 'string' ? payload : JSON.stringify(payload));
    const b = [...this.str(t), ...p];
    this.send([retain ? 0x31 : 0x30, ...this.len(b.length), ...b]);
  }
}

// ---------- light sounds ----------
let audio;
let soundOn = localStorage.getItem('soundOn') !== '0';
function blip(notes, vol = 0.08) {
  if (!soundOn) return;
  try {
    audio ??= new (window.AudioContext || window.webkitAudioContext)();
    if (audio.state === 'suspended') audio.resume();
    let t = audio.currentTime;
    for (const [f, d] of notes) {
      const o = audio.createOscillator(), g = audio.createGain();
      o.type = 'sine'; o.frequency.setValueAtTime(f, t);
      g.gain.setValueAtTime(0.0001, t);
      g.gain.exponentialRampToValueAtTime(vol, t + 0.01);
      g.gain.exponentialRampToValueAtTime(0.0001, t + d);
      o.connect(g).connect(audio.destination);
      o.start(t); o.stop(t + d + 0.02);
      t += d * 0.6;
    }
  } catch { /* no audio available */ }
}
const sfx = {
  msg: () => blip([[880, 0.09], [1175, 0.12]], 0.05),
  send: () => blip([[1320, 0.07]], 0.04),
  join: () => blip([[523, 0.1], [784, 0.16]], 0.05),
  leave: () => blip([[440, 0.12], [330, 0.16]], 0.03),
  enter: () => blip([[660, 0.08], [880, 0.08], [1320, 0.14]], 0.04),
  emote: () => blip([[740, 0.08], [988, 0.1]], 0.035),
};

// ---------- safety ----------
function clean(s) {
  return String(s)
    .replace(/\+?\d[\d\s().-]{6,}\d/g, '[number hidden]')
    .replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, '[email hidden]');
}
const cleanCode = (s) => String(s).toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);

// ---------- textures ----------
const hexCss = (h) => '#' + h.toString(16).padStart(6, '0');
function canvasTex(w, h, draw, repeat = 1) {
  const c = document.createElement('canvas'); c.width = w; c.height = h;
  draw(c.getContext('2d'), w, h);
  const t = new THREE.CanvasTexture(c);
  t.colorSpace = THREE.SRGBColorSpace;
  t.wrapS = t.wrapT = THREE.RepeatWrapping;
  t.repeat.set(repeat, repeat);
  t.anisotropy = 4;
  return t;
}
function shade(hex, k) {
  const c = new THREE.Color(hex);
  return `rgb(${Math.min(255, c.r * 255 * k) | 0},${Math.min(255, c.g * 255 * k) | 0},${Math.min(255, c.b * 255 * k) | 0})`;
}
function floorTex(t) {
  return canvasTex(512, 512, (g) => {
    if (t.kind === 0) {
      for (let row = 0; row < 8; row++) {
        let x = -(row % 2) * 160;
        while (x < 512) {
          g.fillStyle = shade(t.floor, 0.85 + Math.random() * 0.25); g.fillRect(x, row * 64, 320, 64);
          for (let k = 0; k < 10; k++) {
            g.strokeStyle = `rgba(0,0,0,${0.03 + Math.random() * 0.07})`; g.lineWidth = 0.5 + Math.random();
            const gy = row * 64 + 4 + Math.random() * 56;
            g.beginPath(); g.moveTo(x, gy); g.bezierCurveTo(x + 100, gy + Math.random() * 10 - 5, x + 220, gy + Math.random() * 10 - 5, x + 320, gy + Math.random() * 8 - 4); g.stroke();
          }
          g.fillStyle = 'rgba(0,0,0,.35)'; g.fillRect(x, row * 64, 2, 64);
          x += 320;
        }
        g.fillStyle = 'rgba(0,0,0,.3)'; g.fillRect(0, row * 64, 512, 2);
      }
    } else if (t.kind === 1) {
      for (let i = 0; i < 4; i++) for (let j = 0; j < 4; j++) { g.fillStyle = shade(t.floor, 0.92 + Math.random() * 0.14); g.fillRect(i * 128, j * 128, 128, 128); }
      g.fillStyle = 'rgba(0,0,0,.4)';
      for (let i = 0; i <= 4; i++) { g.fillRect(i * 128 - 2, 0, 4, 512); g.fillRect(0, i * 128 - 2, 512, 4); }
    } else {
      g.fillStyle = hexCss(t.floor); g.fillRect(0, 0, 512, 512);
      for (let i = 0; i < 9000; i++) { g.fillStyle = `rgba(${Math.random() > 0.5 ? 255 : 0},${Math.random() > 0.5 ? 255 : 0},255,${0.02 + Math.random() * 0.05})`; g.fillRect(Math.random() * 512, Math.random() * 512, 2, 2); }
    }
  }, 4);
}
const wallTex = (hex) => canvasTex(256, 256, (g) => {
  g.fillStyle = hexCss(hex); g.fillRect(0, 0, 256, 256);
  for (let i = 0; i < 3000; i++) { g.fillStyle = `rgba(${Math.random() > 0.5 ? 255 : 0},${Math.random() > 0.5 ? 255 : 0},${Math.random() > 0.5 ? 255 : 0},${0.01 + Math.random() * 0.03})`; g.fillRect(Math.random() * 256, Math.random() * 256, 2, 2); }
}, 2);
const rugTex = (hex) => canvasTex(256, 256, (g) => {
  g.fillStyle = hexCss(hex); g.fillRect(0, 0, 256, 256);
  g.strokeStyle = 'rgba(255,255,255,.35)'; g.lineWidth = 6;
  for (let r = 30; r < 128; r += 32) { g.beginPath(); g.arc(128, 128, r, 0, Math.PI * 2); g.stroke(); }
});

// ---------- avatars ----------
const mat = (color, roughness = 0.7, metalness = 0) => new THREE.MeshStandardMaterial({ color, roughness, metalness });

class Avatar extends THREE.Group {
  constructor(id, name, look, isMe, labelLayer) {
    super();
    this.avatarId = id; this.displayName = name; this.isMe = isMe;
    this.phase = 0; this.swing = 0; this.emote = null; this.emoteStart = 0; this.muted = false;
    this.lift = 0;
    this.labelLayer = labelLayer;
    if (labelLayer) {
      this.tag = document.createElement('div');
      this.tag.className = 'tag' + (isMe ? ' me' : '');
      labelLayer.appendChild(this.tag);
    }
    this.rebuild(look);
    this.setName(name);
  }
  rebuild(look) {
    if (this.rig) { this.remove(this.rig); this.rig.traverse((o) => { o.geometry?.dispose(); }); }
    const rig = new THREE.Group(); this.add(rig); this.rig = rig;
    const skin = mat(skinColors[look.skin], 0.55), hair = mat(hairColors[look.hair], 0.6);
    const shirt = mat(shirtColors[look.shirt], 0.85), pants = mat(pantsColors[look.pants], 0.85), shoe = mat(0x1f1f1f, 0.4);
    const part = (geo, m, x, y, z, parent) => { const n = new THREE.Mesh(geo, m); n.position.set(x, y, z); n.castShadow = true; parent.add(n); return n; };
    const capsule = (r, h) => new THREE.CapsuleGeometry(r, Math.max(0.01, h - 2 * r), 6, 12);
    for (const side of [-1, 1]) {
      const p = new THREE.Group(); p.position.set(0.11 * side, 0.82, 0); rig.add(p);
      part(capsule(0.095, 0.78), pants, 0, -0.39, 0, p);
      part(new RoundedBoxGeometry(0.16, 0.09, 0.27, 2, 0.04), shoe, 0, -0.77, 0.04, p);
      if (side < 0) this.legR = p; else this.legL = p;
    }
    part(new RoundedBoxGeometry(0.46, 0.2, 0.27, 3, 0.08), pants, 0, 0.85, 0, rig);
    part(new RoundedBoxGeometry(0.5, 0.6, 0.3, 3, 0.11), shirt, 0, 1.17, 0, rig);
    for (const side of [-1, 1]) {
      const p = new THREE.Group(); p.position.set(0.32 * side, 1.4, 0); rig.add(p);
      part(capsule(0.07, 0.36), shirt, 0, -0.14, 0, p);
      part(capsule(0.06, 0.34), skin, 0, -0.42, 0, p);
      part(new THREE.SphereGeometry(0.075, 14, 10), skin, 0, -0.6, 0, p);
      if (side < 0) this.armR = p; else this.armL = p;
    }
    part(new THREE.CylinderGeometry(0.07, 0.07, 0.12, 12), skin, 0, 1.5, 0, rig);
    const head = part(new THREE.SphereGeometry(0.2, 24, 18), skin, 0, 1.69, 0, rig);
    const eye = mat(0x0d0d0d, 0.2), white = mat(0xffffff, 0.3);
    for (const s of [-1, 1]) {
      part(new THREE.SphereGeometry(0.03, 10, 8), eye, 0.075 * s, 0.03, 0.175, head);
      part(new THREE.SphereGeometry(0.01, 6, 6), white, 0.075 * s + 0.01, 0.045, 0.2, head);
    }
    const mouth = part(capsule(0.012, 0.08), mat(0x7A2E2E), 0, -0.075, 0.18, head);
    mouth.rotation.z = Math.PI / 2;
    const sph = (r) => new THREE.SphereGeometry(r, 24, 16);
    switch (look.hairStyle) {
      case 0: part(sph(0.21), hair, 0, 0.06, -0.025, head).scale.set(1.02, 0.78, 1); break;
      case 1:
        part(sph(0.215), hair, 0, 0.05, -0.03, head).scale.set(1.04, 0.85, 1.02);
        part(new RoundedBoxGeometry(0.42, 0.42, 0.14, 3, 0.07), hair, 0, -0.17, -0.12, head);
        break;
      case 2:
        part(sph(0.2), hair, 0, 0.07, -0.02, head).scale.set(1, 0.7, 1);
        for (let i = 0; i < 7; i++) {
          const a = (i / 7) * Math.PI * 2;
          const c = part(new THREE.ConeGeometry(0.06, 0.16, 10), hair, Math.cos(a) * 0.1, 0.2, Math.sin(a) * 0.1 - 0.02, head);
          c.rotation.set(Math.sin(a) * 0.5, 0, -Math.cos(a) * 0.5);
        }
        break;
      case 3:
        part(sph(0.21), hair, 0, 0.06, -0.025, head).scale.set(1.02, 0.8, 1);
        part(sph(0.09), hair, 0, 0.22, -0.1, head);
        break;
      case 5: {
        const capMat = mat(shirtColors[(look.shirt + 5) % shirtColors.length], 0.8);
        part(sph(0.215), capMat, 0, 0.07, 0, head).scale.set(1.02, 0.72, 1.02);
        part(new RoundedBoxGeometry(0.3, 0.025, 0.2, 2, 0.01), capMat, 0, 0.07, 0.24, head);
        break;
      }
      default: break;
    }
    if (this.isMe) {
      const ring = new THREE.Mesh(new THREE.TorusGeometry(0.38, 0.02, 8, 40), new THREE.MeshStandardMaterial({ color: 0x35D0FF, emissive: 0x35D0FF, emissiveIntensity: 1 }));
      ring.rotation.x = Math.PI / 2; ring.position.y = 0.02;
      rig.add(ring);
    }
  }
  setName(n) {
    this.displayName = n;
    if (this.tag) this.tag.textContent = this.muted ? `${n} 🔇` : n;
  }
  setMuted(m) {
    this.muted = m; this.setName(this.displayName);
    if (m) this.bubble?.remove();
  }
  say(text) {
    if (!this.labelLayer || this.muted) return;
    this.bubble?.remove();
    const b = document.createElement('div');
    b.className = 'bubble'; b.textContent = text;
    this.labelLayer.appendChild(b);
    this.bubble = b;
    clearTimeout(this.bubbleTimer);
    this.bubbleTimer = setTimeout(() => { b.style.opacity = '0'; setTimeout(() => b.remove(), 500); }, 5500);
  }
  playEmote(kind) {
    if (!['wave', 'dance', 'jump'].includes(kind)) return;
    this.emote = kind; this.emoteStart = performance.now() / 1000;
  }
  place(p) { this.position.set(p.x, 0, p.z); }
  face(dx, dz) {
    if (Math.hypot(dx, dz) < 1e-4) return;
    const target = Math.atan2(dx, dz);
    let diff = target - this.rotation.y;
    while (diff > Math.PI) diff -= 2 * Math.PI;
    while (diff < -Math.PI) diff += 2 * Math.PI;
    this.rotation.y += diff * 0.25;
  }
  animate(dt, moving) {
    this.swing += ((moving ? 0.75 : 0) - this.swing) * Math.min(1, dt * 10);
    if (moving) this.phase += dt * 9;
    const s = Math.sin(this.phase) * this.swing;
    this.legL.rotation.set(s, 0, 0); this.legR.rotation.set(-s, 0, 0);
    this.armL.rotation.set(-s * 0.8, 0, 0); this.armR.rotation.set(s * 0.8, 0, 0);
    let lift = Math.abs(Math.sin(this.phase)) * 0.05 * this.swing;
    this.rig.rotation.y = 0;
    if (this.emote) {
      const t = performance.now() / 1000 - this.emoteStart;
      if (this.emote === 'wave' && t < 1.8) {
        this.armR.rotation.set(0, 0, -2.7 + Math.sin(t * 14) * 0.35);
      } else if (this.emote === 'dance' && t < 3.2) {
        lift = Math.abs(Math.sin(t * 9)) * 0.14;
        this.armL.rotation.set(0, 0, 2.3 + Math.sin(t * 9) * 0.5);
        this.armR.rotation.set(0, 0, -2.3 + Math.sin(t * 9) * 0.5);
        this.legL.rotation.set(Math.sin(t * 9) * 0.35, 0, 0);
        this.legR.rotation.set(-Math.sin(t * 9) * 0.35, 0, 0);
        this.rig.rotation.y = Math.sin(t * 4.5) * 0.6;
      } else if (this.emote === 'jump' && t < 0.8) {
        lift = Math.sin((t / 0.8) * Math.PI) * 0.7;
        this.armL.rotation.set(0, 0, 2.6); this.armR.rotation.set(0, 0, -2.6);
      } else {
        this.emote = null;
      }
    }
    this.rig.position.y = lift;
    this.lift = lift;
  }
  dispose() {
    this.tag?.remove(); this.bubble?.remove();
    this.rig?.traverse((o) => o.geometry?.dispose());
  }
}

// ---------- room ----------
function buildRoom(code) {
  const t = themeFor(code);
  const scene = new THREE.Scene();
  scene.background = new THREE.Color(t.night ? 0x07070c : 0x1b1d24);
  const add = (geo, m, x, y, z, parent = scene) => { const n = new THREE.Mesh(geo, m); n.position.set(x, y, z); n.castShadow = true; n.receiveShadow = true; parent.add(n); return n; };

  // floor and walls run past what you can walk on, so the room always fills the screen
  const ft = floorTex(t); ft.repeat.set(4, 8);
  const floor = add(new THREE.BoxGeometry(16, 0.2, 24), new THREE.MeshStandardMaterial({ map: ft, roughness: t.kind === 2 ? 0.95 : 0.6 }), 0, -0.1, 6);
  floor.castShadow = false; floor.name = 'floor';

  const wm = new THREE.MeshStandardMaterial({ map: wallTex(t.wall), roughness: 0.9 });
  add(new THREE.BoxGeometry(16.4, 8, 0.2), wm, 0, 4, -6.1).castShadow = false;
  add(new THREE.BoxGeometry(0.2, 8, 24.2), wm, -8.1, 4, 6).castShadow = false;
  add(new THREE.BoxGeometry(0.2, 8, 24.2), wm, 8.1, 4, 6).castShadow = false;
  const trim = mat(t.trim, 0.5);
  add(new THREE.BoxGeometry(16, 0.24, 0.06), trim, 0, 0.12, -5.98);
  add(new THREE.BoxGeometry(0.06, 0.24, 24), trim, -7.98, 0.12, 6);
  add(new THREE.BoxGeometry(0.06, 0.24, 24), trim, 7.98, 0.12, 6);

  // window
  const sky = canvasTex(256, 160, (g) => {
    const gr = g.createLinearGradient(0, 0, 0, 160);
    gr.addColorStop(0, t.night ? '#0b1030' : '#6ec6ff'); gr.addColorStop(1, t.night ? '#29335c' : '#ffe3b3');
    g.fillStyle = gr; g.fillRect(0, 0, 256, 160);
    g.fillStyle = t.night ? '#f5f3ce' : 'rgba(255,255,255,.9)';
    g.beginPath();
    if (t.night) g.arc(197, 43, 17, 0, Math.PI * 2); else g.ellipse(75, 57, 35, 13, 0, 0, Math.PI * 2);
    g.fill();
  });
  sky.wrapS = sky.wrapT = THREE.ClampToEdgeWrapping;
  add(new THREE.PlaneGeometry(3, 1.8), new THREE.MeshBasicMaterial({ map: sky }), 3.5, 2.3, -5.99).castShadow = false;
  const frame = mat(0xf2f2f2, 0.4);
  for (const [w, h, x, y] of [[3.2, 0.1, 3.5, 3.25], [3.2, 0.1, 3.5, 1.35], [0.1, 2, 1.95, 2.3], [0.1, 2, 5.05, 2.3], [0.06, 1.8, 3.5, 2.3]]) {
    add(new THREE.BoxGeometry(w, h, 0.08), frame, x, y, -5.95);
  }

  // posters
  t.posters.forEach((e, i) => {
    const tex = canvasTex(200, 260, (g) => {
      g.fillStyle = '#f7f7f7'; g.fillRect(0, 0, 200, 260);
      g.fillStyle = hexCss(t.rug) + '59'; g.fillRect(12, 12, 176, 236);
      g.font = '110px "Apple Color Emoji","Segoe UI Emoji","Noto Color Emoji",sans-serif';
      g.textAlign = 'center'; g.textBaseline = 'middle'; g.fillText(e, 100, 135);
    });
    tex.wrapS = tex.wrapT = THREE.ClampToEdgeWrapping;
    const p = add(new THREE.BoxGeometry(1, 1.3, 0.04), new THREE.MeshStandardMaterial({ map: tex, roughness: 0.5 }), 0, 0, 0);
    p.castShadow = false;
    if (i < 2) p.position.set(-5.6 + i * 1.6, 2.5, -5.97);
    else { p.position.set(-7.97, 2.4, -1.5); p.rotation.y = Math.PI / 2; }
  });

  // rug, couch, table, plants, lamp
  const rugT = rugTex(t.rug); rugT.wrapS = rugT.wrapT = THREE.ClampToEdgeWrapping;
  const rug = add(new THREE.CylinderGeometry(2.6, 2.6, 0.02, 64), new THREE.MeshStandardMaterial({ map: rugT, roughness: 1 }), 0, 0.01, 0.5);
  rug.scale.x = 1.4; rug.castShadow = false;

  const couch = new THREE.Group(); couch.position.set(-3, 0, -5.3); scene.add(couch);
  const cm = mat(t.couch, 0.95);
  for (const [w, h, l, x, y, z] of [[4, 0.45, 1.1, 0, 0.3, 0], [4, 0.9, 0.3, 0, 0.75, -0.42], [0.3, 0.7, 1.1, -2, 0.45, 0], [0.3, 0.7, 1.1, 2, 0.45, 0]]) {
    add(new RoundedBoxGeometry(w, h, l, 3, 0.12), cm, x, y, z, couch);
  }
  const cushion = mat(new THREE.Color(t.couch).lerp(new THREE.Color(0xffffff), 0.15), 1);
  for (const x of [-0.95, 0.95]) add(new RoundedBoxGeometry(1.8, 0.18, 0.85, 3, 0.09), cushion, x, 0.6, 0.05, couch);

  const wood = mat(t.trim, 0.45);
  const table = new THREE.Group(); table.position.set(-3, 0, -3.2); scene.add(table);
  add(new THREE.CylinderGeometry(0.75, 0.75, 0.06, 40), wood, 0, 0.45, 0, table);
  add(new THREE.CylinderGeometry(0.08, 0.08, 0.45, 16), wood, 0, 0.22, 0, table);

  for (const px of [-7.2, 7.2]) {
    const plant = new THREE.Group(); plant.position.set(px, 0, -5.2); scene.add(plant);
    add(new THREE.CylinderGeometry(0.32, 0.32, 0.6, 24), mat(0xC26A3D, 0.8), 0, 0.3, 0, plant);
    const leaf = mat(0x3C8D40, 0.8);
    for (let i = 0; i < 7; i++) {
      const a = (i / 7) * Math.PI * 2;
      add(new THREE.SphereGeometry(0.3, 16, 12), leaf, Math.cos(a) * 0.22, 0.95 + (i % 3) * 0.18, Math.sin(a) * 0.22, plant);
    }
  }

  const lamp = new THREE.Group(); lamp.position.set(-6.4, 0, -4.6); scene.add(lamp);
  add(new THREE.CylinderGeometry(0.04, 0.04, 2, 12), mat(0x555555, 0.3, 0.8), 0, 1, 0, lamp);
  const shadeM = new THREE.MeshStandardMaterial({ color: 0xF6E7C8, roughness: 0.9, emissive: t.light, emissiveIntensity: 0.6 });
  add(new THREE.CylinderGeometry(0.2, 0.38, 0.4, 24, 1, true), shadeM, 0, 2.05, 0, lamp).material.side = THREE.DoubleSide;
  const bulb = new THREE.PointLight(t.light, t.night ? 40 : 22, 12, 2);
  bulb.position.set(0, 1.9, 0); lamp.add(bulb);

  // lights
  scene.add(new THREE.AmbientLight(t.night ? 0x6670a0 : 0xffffff, t.night ? 0.55 : 0.95));
  scene.add(new THREE.HemisphereLight(t.night ? 0x334 : 0xddeeff, t.night ? 0x111 : 0x553322, t.night ? 0.3 : 0.5));
  const sun = new THREE.DirectionalLight(t.night ? 0xb8c4ff : t.light, t.night ? 1.1 : 2.3);
  sun.position.set(4, 9, 6);
  sun.castShadow = true;
  const sm = window.innerWidth < 860 ? 1024 : 2048;
  sun.shadow.mapSize.set(sm, sm);
  Object.assign(sun.shadow.camera, { left: -12, right: 12, top: 12, bottom: -12, near: 1, far: 30 });
  sun.shadow.bias = -0.0005; sun.shadow.radius = 3;
  scene.add(sun);

  return { scene, floor };
}

// ---------- app state ----------
const net = new MQTT();
const myID = net.id;
let name = localStorage.getItem('name') || `Guest${100 + Math.floor(Math.random() * 900)}`;
localStorage.setItem('name', name);
let look = cleanLook(JSON.parse(localStorage.getItem('look') || '{"skin":1,"hair":1,"hairStyle":0,"shirt":5,"pants":0}'));
const lobby = new Map();          // code -> {code,name,emoji,count,lastSeen}
let room = null;                  // {code,name,emoji,pub}
let roomScene = null, floorMesh = null;
let me = null;
let myPos = { x: 0, z: 3 }, myTarget = { x: 0, z: 3 };
const keys = new Set();
const peers = new Map();          // id -> {id,name,look,pos,target,lastSeen,node}
const muted = new Set();
let history = [], historyShown = false;
let selectedPeer = null;
let lastBeat = 0, lastLobby = 0, lastSay = 0, lastKeyBeat = 0, keyMoving = false;

const clampRoom = (p) => ({ x: Math.min(Math.max(p.x, -7.2), 7.2), z: Math.min(Math.max(p.z, -4.6), 5.4) });

// ---------- renderer ----------
const canvas = $('#c');
const renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
renderer.shadowMap.enabled = true;
renderer.shadowMap.type = THREE.PCFSoftShadowMap;
renderer.toneMapping = THREE.ACESFilmicToneMapping;
renderer.outputColorSpace = THREE.SRGBColorSpace;
const camera = new THREE.PerspectiveCamera(48, 1, 0.1, 100);
let camPos = new THREE.Vector3(0, 7.2, 12);
const labels = $('#labels');

function resize() {
  const stage = $('#stage');
  const w = stage.clientWidth || 1, h = stage.clientHeight || 1;
  renderer.setSize(w, h, false);
  camera.aspect = w / h;
  camera.fov = camera.aspect < 1 ? 68 : camera.aspect < 1.3 ? 56 : 48;
  camera.updateProjectionMatrix();
}
window.addEventListener('resize', resize);

function camTarget(p) {
  const portrait = camera.aspect < 1;
  return new THREE.Vector3(Math.min(Math.max(p.x, -4), 4) * (portrait ? 1 : 0.8), portrait ? 8 : 7.2, Math.min(Math.max(p.z, -2), 4) * 0.5 + (portrait ? 11.5 : 10.5));
}
function applyCam() {
  camera.position.copy(camPos);
  camera.lookAt(camPos.x * 0.9, 0.6, camPos.z - 10.2);
}

// ---------- preview (main menu) ----------
const prev = (() => {
  const host = $('#preview');
  const r = new THREE.WebGLRenderer({ antialias: true, alpha: false });
  r.setPixelRatio(Math.min(window.devicePixelRatio, 2));
  r.shadowMap.enabled = true;
  r.toneMapping = THREE.ACESFilmicToneMapping;
  r.outputColorSpace = THREE.SRGBColorSpace;
  host.appendChild(r.domElement);
  const s = new THREE.Scene(); s.background = new THREE.Color(0x20232c);
  const disc = new THREE.Mesh(new THREE.CylinderGeometry(0.8, 0.8, 0.04, 48), mat(0x353a48, 0.6)); disc.receiveShadow = true; s.add(disc);
  s.add(new THREE.AmbientLight(0xffffff, 1.0));
  const k = new THREE.DirectionalLight(0xffffff, 2.2); k.position.set(2, 4, 3); k.castShadow = true; s.add(k);
  const cam = new THREE.PerspectiveCamera(35, 1, 0.1, 50); cam.position.set(0, 1.5, 4.6); cam.lookAt(0, 1.0, 0);
  const a = new Avatar('preview', name, look, false, null); s.add(a);
  return { r, s, cam, a, host };
})();

// ---------- networking ----------
function publishState(emote) {
  if (!room) return;
  lastBeat = performance.now();
  const m = { id: myID, name, look, x: myPos.x, z: myPos.z, tx: myTarget.x, tz: myTarget.z, room: room.name, pub: room.pub };
  if (emote) m.emote = emote;
  net.publish(topic(room.code, 's'), m);
}

function handle(t, text) {
  let m;
  try { m = JSON.parse(text); } catch { return; }
  if (!m || typeof m !== 'object') return;
  if (t === LOBBY) {
    const code = cleanCode(m.code || '');
    if (!code) return;
    lobby.set(code, { code, name: String(m.name || code).slice(0, 24), emoji: styleEmoji(code), count: Math.min(Math.max(m.count | 0, 0), 99), lastSeen: Date.now() });
    renderRooms();
    return;
  }
  if (!room) return;
  if (t === topic(room.code, 's') && m.id && m.id !== myID) {
    const nm = String(m.name || 'Someone').slice(0, 16), lk = cleanLook(m.look);
    const pos = { x: +m.x || 0, z: +m.z || 0 }, target = clampRoom({ x: +m.tx || 0, z: +m.tz || 0 });
    let p = peers.get(m.id);
    if (p) {
      if (JSON.stringify(p.look) !== JSON.stringify(lk)) { p.node.rebuild(lk); p.look = lk; }
      if (p.name !== nm) { p.name = nm; p.node.setName(nm); }
      p.target = target;
      if (Math.hypot(p.pos.x - pos.x, p.pos.z - pos.z) > 4) p.pos = clampRoom(pos);
      p.lastSeen = Date.now();
    } else {
      if (peers.size >= 40) return;
      const node = new Avatar(m.id, nm, lk, false, labels);
      node.setMuted(muted.has(m.id));
      roomScene.add(node);
      p = { id: m.id, name: nm, look: lk, pos: clampRoom(pos), target, lastSeen: Date.now(), node };
      peers.set(m.id, p);
      addLine({ sys: true, text: `${nm} joined` });
      sfx.join();
      renderPeople();
      publishState();
    }
    if (m.emote) p.node.playEmote(m.emote);
    if (room.name.startsWith('Room ') && m.room && !String(m.room).startsWith('Room ')) {
      room.name = String(m.room).slice(0, 24); room.pub = !!m.pub; renderRoomBar();
    }
  } else if (t === topic(room.code, 'c') && m.id && m.id !== myID) {
    if (muted.has(m.id)) return;
    const msg = { id: m.id, name: String(m.name || '').slice(0, 16), text: clean(String(m.text || '').slice(0, 200)), t: m.t };
    addLine({ name: msg.name, text: msg.text });
    peers.get(m.id)?.node.say(msg.text);
    remember(msg);
    sfx.msg();
    if (!$('#side').classList.contains('open') && window.innerWidth < 860) $('#unread').classList.remove('hidden');
  } else if (t === topic(room.code, 'h') && Array.isArray(m.items)) {
    m.items.slice(-25).forEach(remember);
    showHistory();
  } else if (t === topic(room.code, 'l') && m.id) {
    removePeer(m.id);
  }
}

function remember(m) {
  const c = { id: String(m.id || ''), name: String(m.name || '').slice(0, 16), text: clean(String(m.text || '').slice(0, 200)), t: typeof m.t === 'number' ? m.t : undefined };
  if (history.some((h) => h.id === c.id && h.text === c.text && h.t === c.t)) return;
  history.push(c);
  history.sort((a, b) => (a.t || 0) - (b.t || 0));
  if (history.length > 25) history = history.slice(-25);
}

function showHistory() {
  if (historyShown) return;
  historyShown = true;
  const shown = new Set([...$('#log').querySelectorAll('.line:not(.sys)')].map((e) => e.dataset.key));
  const old = history.filter((h) => !muted.has(h.id) && !shown.has(`${h.name}|${h.text}`));
  if (!old.length) return;
  const log = $('#log'), first = log.firstChild;
  const items = [{ sys: true, text: 'Earlier messages' }, ...old.map((h) => ({ name: h.name, text: h.text, mine: h.id === myID })), { sys: true, text: '────────' }];
  for (const l of items) log.insertBefore(lineEl(l), first);
}

function removePeer(id) {
  const p = peers.get(id);
  if (!p) return;
  peers.delete(id);
  roomScene?.remove(p.node); p.node.dispose();
  if (selectedPeer === id) { selectedPeer = null; renderPeerCard(); }
  addLine({ sys: true, text: `${p.name} left` });
  sfx.leave();
  renderPeople();
}

// ---------- rooms ----------
function roomList() {
  const list = [...lobby.values()];
  if (room) {
    const mine = list.find((r) => r.code === room.code);
    if (mine) mine.count = peers.size + 1;
    else if (room.pub) list.push({ code: room.code, name: room.name, emoji: room.emoji, count: peers.size + 1 });
  }
  return list.sort((a, b) => (b.count - a.count) || a.name.localeCompare(b.name));
}

function newCode(style) {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c = roomStyles[style].letter;
  for (let i = 0; i < 5; i++) c += chars[Math.floor(Math.random() * chars.length)];
  return c;
}

function join(raw, roomName, pub = true) {
  const code = cleanCode(raw);
  if (!code) return;
  leave(true);
  const known = roomList().find((r) => r.code === code);
  room = { code, name: roomName || known?.name || `Room ${code}`, emoji: known?.emoji || styleEmoji(code), pub: known ? true : pub };
  const built = buildRoom(code);
  roomScene = built.scene; floorMesh = built.floor;
  me = new Avatar(myID, name, look, true, labels);
  roomScene.add(me);
  myPos = { x: Math.random() * 6 - 3, z: 2.5 + Math.random() * 2 };
  myTarget = { ...myPos };
  history = []; historyShown = false;
  for (const k of ['s', 'c', 'l', 'h']) net.subscribe(topic(code, k));
  $('#log').innerHTML = '';
  addLine({ sys: true, text: `You joined ${room.name}. Room code: ${code}` });
  $('#lobby').classList.add('hidden');
  $('#roomScreen').classList.remove('hidden');
  window.history.replaceState(null, '', `?room=${code}`);
  resize();
  camPos = camTarget(myPos); applyCam();
  renderRoomBar(); renderPeople();
  sfx.enter();
  publishState();
}

function leave(silent = false) {
  if (!room) return;
  net.publish(topic(room.code, 'l'), { id: myID });
  for (const k of ['s', 'c', 'l', 'h']) net.unsubscribe(topic(room.code, k));
  for (const p of peers.values()) p.node.dispose();
  peers.clear();
  me?.dispose(); me = null;
  roomScene = null; room = null; selectedPeer = null;
  labels.innerHTML = '';
  keys.clear();
  window.history.replaceState(null, '', location.pathname);
  if (!silent) {
    $('#roomScreen').classList.add('hidden');
    $('#lobby').classList.remove('hidden');
    renderRooms();
  }
}

function joinRandom() {
  const list = roomList().filter((r) => r.code !== room?.code);
  if (list.length) { const p = list[Math.floor(Math.random() * list.length)]; join(p.code, p.name); }
}

// ---------- actions ----------
function say(raw) {
  const text = clean(String(raw).trim().slice(0, 200));
  if (!text || !room || performance.now() - lastSay < 500) return;
  lastSay = performance.now();
  const msg = { id: myID, name, text, t: Date.now() / 1000 };
  net.publish(topic(room.code, 'c'), msg);
  remember(msg);
  net.publish(topic(room.code, 'h'), { items: history }, true);
  addLine({ name, text, mine: true });
  me?.say(text);
  sfx.send();
}

function emote(kind) {
  me?.playEmote(kind);
  publishState(kind);
  sfx.emote();
}

function walkTo(x, z) { myTarget = clampRoom({ x, z }); publishState(); }

function toggleMute(id) {
  if (muted.has(id)) muted.delete(id); else muted.add(id);
  peers.get(id)?.node.setMuted(muted.has(id));
  renderPeople(); renderPeerCard();
}

// ---------- UI ----------
function toast(s) {
  const t = $('#toast'); t.textContent = s; t.classList.remove('hidden');
  clearTimeout(toast.timer); toast.timer = setTimeout(() => t.classList.add('hidden'), 1600);
}

function lineEl(l) {
  const d = document.createElement('div');
  d.className = 'line' + (l.sys ? ' sys' : '') + (l.mine ? ' mine' : '');
  if (l.sys) d.textContent = l.text;
  else {
    const b = document.createElement('b'); b.textContent = `${l.name}: `;
    d.append(b, document.createTextNode(l.text));
    d.dataset.key = `${l.name}|${l.text}`;
  }
  return d;
}
function addLine(l) {
  const log = $('#log');
  log.appendChild(lineEl(l));
  while (log.children.length > 300) log.firstChild.remove();
  log.scrollTop = log.scrollHeight;
}

function renderRooms() {
  const host = $('#roomList');
  const list = roomList();
  $('#randomBtn').disabled = !list.some((r) => r.code !== room?.code);
  host.innerHTML = '';
  if (!list.length) {
    host.innerHTML = '<div class="card empty" style="grid-column:1/-1"><div class="big">🏠</div><b>No rooms open right now</b>Make one with Create room. It\'ll show up here for everyone.</div>';
    return;
  }
  for (const r of list) {
    const b = document.createElement('button');
    b.className = 'roomcard';
    const e = document.createElement('span'); e.className = 'e'; e.textContent = r.emoji;
    const n = document.createElement('span'); n.className = 'n'; n.textContent = r.name;
    const c = document.createElement('span'); c.className = 'c'; c.textContent = `${r.count} here now`;
    b.append(e, n, c);
    b.onclick = () => join(r.code, r.name);
    host.appendChild(b);
  }
}

function renderPerson() {
  for (const host of document.querySelectorAll('.swatches')) {
    const k = host.dataset.k;
    host.innerHTML = '';
    palettes[k].forEach((col, i) => {
      const b = document.createElement('button');
      b.style.background = hexCss(col);
      b.className = look[k] === i ? 'on' : '';
      b.title = `${k} ${i + 1}`;
      b.onclick = () => setLook({ [k]: i });
      host.appendChild(b);
    });
  }
  const hs = $('#hairStyles'); hs.innerHTML = '';
  hairStyles.forEach((s, i) => {
    const b = document.createElement('button');
    b.textContent = s; b.className = look.hairStyle === i ? 'on' : '';
    b.onclick = () => setLook({ hairStyle: i });
    hs.appendChild(b);
  });
}
function setLook(change) {
  look = cleanLook({ ...look, ...change });
  localStorage.setItem('look', JSON.stringify(look));
  prev.a.rebuild(look);
  me?.rebuild(look);
  renderPerson();
}

function renderRoomBar() {
  if (!room) return;
  $('#roomEmoji').textContent = room.emoji;
  $('#roomName').textContent = room.name;
  $('#codeBtn').textContent = `📋 ${room.code}`;
  $('#pubBadge').textContent = room.pub ? 'Public' : 'Private';
}

function renderPeople() {
  $('#peopleTitle').textContent = `People here (${peers.size + 1})`;
  const host = $('#peopleList'); host.innerHTML = '';
  const row = (label, cls, id) => {
    const d = document.createElement('div'); d.className = 'person-row ' + cls;
    const i = document.createElement('i'); const s = document.createElement('span'); s.textContent = label;
    d.append(i, s);
    if (id) {
      const b = document.createElement('button'); b.textContent = muted.has(id) ? 'Unmute' : 'Mute';
      b.onclick = () => toggleMute(id);
      d.appendChild(b);
    }
    host.appendChild(d);
  };
  row(`${name} (you)`, 'me');
  [...peers.values()].sort((a, b) => a.name.localeCompare(b.name)).forEach((p) => row(p.name, muted.has(p.id) ? 'muted' : '', p.id));
  if (!peers.size) { const h = document.createElement('p'); h.className = 'hint'; h.textContent = 'Nobody else yet. Share the room code!'; host.appendChild(h); }
}

function renderPeerCard() {
  const card = $('#peerCard');
  const p = selectedPeer && peers.get(selectedPeer);
  if (!p) { card.classList.add('hidden'); return; }
  card.innerHTML = '';
  const b = document.createElement('b'); b.textContent = p.name;
  const mute = document.createElement('button'); mute.textContent = muted.has(p.id) ? 'Unmute' : 'Mute'; mute.onclick = () => toggleMute(p.id);
  const wave = document.createElement('button'); wave.textContent = '👋 Wave'; wave.onclick = () => emote('wave');
  const x = document.createElement('button'); x.textContent = '✕'; x.onclick = () => { selectedPeer = null; renderPeerCard(); };
  card.append(b, mute, wave, x);
  card.classList.remove('hidden');
}

// lobby controls
$('#nameInput').value = name;
$('#nameInput').addEventListener('input', (e) => {
  name = e.target.value.slice(0, 16) || 'Guest';
  localStorage.setItem('name', name);
  prev.a.setName(name);
});
$('#soundToggle').checked = soundOn;
$('#soundToggle').onchange = (e) => { soundOn = e.target.checked; localStorage.setItem('soundOn', soundOn ? '1' : '0'); if (soundOn) sfx.send(); };
$('#joinBtn').onclick = () => join($('#codeInput').value);
$('#codeInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') join(e.target.value); });
$('#randomBtn').onclick = joinRandom;

// create room sheet
let pickStyle = 0;
function renderStyles() {
  const g = $('#styleGrid'); g.innerHTML = '';
  roomStyles.forEach((s, i) => {
    const b = document.createElement('button');
    b.className = pickStyle === i ? 'on' : '';
    const e = document.createElement('span'); e.textContent = s.emoji;
    b.append(e, document.createTextNode(s.name));
    b.onclick = () => { pickStyle = i; renderStyles(); };
    g.appendChild(b);
  });
}
$('#createBtn').onclick = () => {
  $('#newName').value = ''; $('#newName').placeholder = `${name}'s room`;
  renderStyles();
  $('#createModal').classList.remove('hidden');
  $('#newName').focus();
};
$('#cancelCreate').onclick = () => $('#createModal').classList.add('hidden');
$('#newPublic').onchange = (e) => { $('#pubHint').textContent = e.target.checked ? 'Anyone can find it and join.' : 'Private: only people you give the code to can get in.'; };
$('#doCreate').onclick = () => {
  $('#createModal').classList.add('hidden');
  const n = $('#newName').value.trim().slice(0, 24) || `${name}'s room`;
  join(newCode(pickStyle), n, $('#newPublic').checked);
};
$('#newName').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('#doCreate').click(); });

// room controls
$('#leaveBtn').onclick = () => leave();
$('#roomRandom').onclick = joinRandom;
$('#codeBtn').onclick = async () => {
  const link = `${location.origin}${location.pathname}?room=${room.code}`;
  try { await navigator.clipboard.writeText(link); toast('Invite link copied!'); } catch { toast(`Room code: ${room.code}`); }
};
$('#chatToggle').onclick = () => { $('#side').classList.toggle('open'); $('#unread').classList.add('hidden'); };
const chatInput = $('#chatInput');
const send = () => { say(chatInput.value); chatInput.value = ''; };
$('#sendBtn').onclick = send;
chatInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') { send(); if (window.innerWidth >= 860) chatInput.blur(); } });
document.querySelectorAll('.emote').forEach((b) => { b.onclick = () => emote(b.dataset.e); });
const emojis = [...'😂🤣😭💀🔥💯😎🥺😳🤨😈👀🗿🤡🫡🤯🥳❤️👍👎👏🙏💪🤝👋🎉✨⭐🌈🍕🍔🎮⚽🎧🎨🐐🐶🐱🦄🚀'.matchAll(/\p{Extended_Pictographic}️?/gu)].map((m) => m[0]);
const pop = $('#emojiPop');
for (const e of emojis) {
  const b = document.createElement('button'); b.textContent = e;
  b.onclick = () => { chatInput.value += e; pop.classList.add('hidden'); chatInput.focus(); };
  pop.appendChild(b);
}
$('#emojiBtn').onclick = () => pop.classList.toggle('hidden');

// keyboard walking (when not typing)
const moveKeys = new Set(['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'a', 'd', 'w', 's', 'A', 'D', 'W', 'S']);
window.addEventListener('keydown', (e) => {
  if (!room) return;
  const typing = document.activeElement?.tagName === 'INPUT';
  if (!typing && moveKeys.has(e.key)) { keys.add(e.key.toLowerCase()); e.preventDefault(); }
  else if (!typing && e.key === 'Enter') { chatInput.focus(); e.preventDefault(); }
});
window.addEventListener('keyup', (e) => keys.delete(e.key.toLowerCase()));
window.addEventListener('blur', () => keys.clear());

// tap / click to walk or pick someone
const raycaster = new THREE.Raycaster();
canvas.addEventListener('pointerdown', (e) => {
  if (!roomScene) return;
  pop.classList.add('hidden');
  if (document.activeElement === chatInput && window.innerWidth < 860) chatInput.blur();
  const rect = canvas.getBoundingClientRect();
  const v = new THREE.Vector2(((e.clientX - rect.left) / rect.width) * 2 - 1, -((e.clientY - rect.top) / rect.height) * 2 + 1);
  raycaster.setFromCamera(v, camera);
  const hits = raycaster.intersectObjects(roomScene.children, true);
  for (const h of hits) {
    let o = h.object;
    while (o && !(o instanceof Avatar)) o = o.parent;
    if (o && o !== me) { selectedPeer = selectedPeer === o.avatarId ? null : o.avatarId; renderPeerCard(); return; }
    if (h.object === floorMesh) {
      selectedPeer = null; renderPeerCard();
      walkTo(h.point.x, h.point.z);
      return;
    }
  }
});

// ---------- frame loop ----------
const tmp = new THREE.Vector3();
function placeLabel(node) {
  if (!node.tag) return;
  const w = canvas.clientWidth, h = canvas.clientHeight;
  tmp.set(node.position.x, 2.12 + node.lift, node.position.z).project(camera);
  const visible = tmp.z < 1;
  const x = (tmp.x * 0.5 + 0.5) * w, y = (-tmp.y * 0.5 + 0.5) * h;
  node.tag.style.display = visible ? '' : 'none';
  node.tag.style.transform = `translate(${x}px, ${y}px) translate(-50%, -100%)`;
  if (node.bubble) {
    node.bubble.style.display = visible ? '' : 'none';
    node.bubble.style.transform = `translate(${x}px, ${y - 26}px) translate(-50%, -100%)`;
  }
}

let last = performance.now();
let lastClean = 0;
function loop(now) {
  requestAnimationFrame(loop);
  const dt = Math.min(0.05, (now - last) / 1000);
  last = now;

  if (now - lastClean > 2000) {
    lastClean = now;
    let changed = false;
    for (const [k, r] of lobby) if (Date.now() - r.lastSeen > 14000) { lobby.delete(k); changed = true; }
    if (changed) renderRooms();
  }

  if (!room) {
    // spinning preview on the main menu
    const host = prev.host, w = host.clientWidth, h = host.clientHeight;
    if (w && h) {
      if (prev.r.domElement.width !== Math.floor(w * prev.r.getPixelRatio())) { prev.r.setSize(w, h, false); prev.cam.aspect = w / h; prev.cam.updateProjectionMatrix(); }
      prev.a.rotation.y += dt * 0.9;
      prev.r.render(prev.s, prev.cam);
    }
    return;
  }

  // walking
  let dx = 0, dz = 0;
  if (keys.has('arrowleft') || keys.has('a')) dx -= 1;
  if (keys.has('arrowright') || keys.has('d')) dx += 1;
  if (keys.has('arrowup') || keys.has('w')) dz -= 1;
  if (keys.has('arrowdown') || keys.has('s')) dz += 1;
  const speed = 3.2;
  let moving = false;
  if (dx || dz) {
    const l = Math.hypot(dx, dz), sx = (dx / l) * speed * dt, sz = (dz / l) * speed * dt;
    myPos = clampRoom({ x: myPos.x + sx, z: myPos.z + sz }); myTarget = { ...myPos };
    me.face(sx, sz); moving = true; keyMoving = true;
    if (now - lastKeyBeat > 200) { lastKeyBeat = now; publishState(); }
  } else {
    if (keyMoving) { keyMoving = false; publishState(); }
    const ddx = myTarget.x - myPos.x, ddz = myTarget.z - myPos.z, len = Math.hypot(ddx, ddz);
    if (len > 0.03) {
      const k = Math.min(len, speed * dt) / len;
      myPos = { x: myPos.x + ddx * k, z: myPos.z + ddz * k };
      me.face(ddx, ddz); moving = true;
    }
  }
  me.place(myPos); me.animate(dt, moving);

  for (const [id, p] of peers) {
    if (Date.now() - p.lastSeen > 9000) { removePeer(id); continue; }
    const ddx = p.target.x - p.pos.x, ddz = p.target.z - p.pos.z, len = Math.hypot(ddx, ddz);
    let pm = false;
    if (len > 0.03) {
      const k = Math.min(len, speed * dt) / len;
      p.pos = { x: p.pos.x + ddx * k, z: p.pos.z + ddz * k };
      p.node.face(ddx, ddz); pm = true;
    }
    p.node.place(p.pos); p.node.animate(dt, pm);
  }

  const target = camTarget(myPos);
  camPos.lerp(target, Math.min(1, dt * 3));
  applyCam();
  renderer.render(roomScene, camera);
  placeLabel(me);
  for (const p of peers.values()) placeLabel(p.node);

  if (now - lastBeat > 2000) publishState();
  if (room.pub && now - lastLobby > 4000) {
    lastLobby = now;
    net.publish(LOBBY, { code: room.code, name: room.name, count: peers.size + 1 });
  }
}

// ---------- start ----------
net.onConnected = (c) => {
  const s = $('#status');
  s.classList.toggle('on', c);
  s.querySelector('b').textContent = c ? 'Online' : 'Connecting…';
  if (c) {
    publishState();
    const wanted = new URLSearchParams(location.search).get('room');
    if (wanted && !room && !net.autoJoined) { net.autoJoined = true; join(wanted); }
  }
};
net.onMessage = handle;
net.subscribe(LOBBY);
net.connect();
renderPerson();
renderRooms();
requestAnimationFrame(loop);
window.addEventListener('pagehide', () => leave(true));
