// server.js — Soni Music Proxy
// Deploy: Railway / Render (needs persistent Redis)
// Handles: JioSaavn metadata, Piped fallback, caching, dedup, rate limiting

import express from 'express';
import Redis from 'ioredis';
import fetch from 'node-fetch';
import rateLimit from 'express-rate-limit';
import helmet from 'helmet';
import cors from 'cors';

const app = express();
const PORT = process.env.PORT || 3000;

// ── Config ──────────────────────────────────────────────
// SELF-HOSTED JioSaavn API (Itz-Anya/Saavn-Api — zero keys, DES-decrypts CDN URLs)
// Deploy this yourself: https://github.com/Itz-Anya/Saavn-Api
const JIOSAAVN_API = process.env.JIOSAAVN_API || 'https://your-saavn-api.vercel.app';

// Piped instances for YouTube fallback
const PIPED_INSTANCES = [
  'https://api.piped.private.coffee',
  'https://pipedapi.kavin.rocks',
  'https://pipedapi.adminforge.de',
  'https://pipedapi.leptons.xyz',
  'https://pipedapi.r4fo.com'
];

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

// ── Security ────────────────────────────────────────────
app.use(helmet());
app.use(cors({
  origin: process.env.ALLOWED_ORIGIN || '*', // Set to your domain in prod
  methods: ['GET'],
  allowedHeaders: ['Content-Type', 'X-Requested-With']
}));
app.use(express.json({ limit: '100kb' }));

// Rate limiting — 120 requests/min per IP (enough for 100 users, blocks abuse)
const limiter = rateLimit({
  windowMs: 60 * 1000,
  limit: 120,
  standardHeaders: 'draft-7',
  legacyHeaders: false,
  message: { error: 'Too many requests. Slow down.' }
});
app.use('/api/', limiter);

// ── Redis ───────────────────────────────────────────────
const redis = new Redis(REDIS_URL, {
  maxRetriesPerRequest: 3,
  retryStrategy: (times) => Math.min(times * 100, 3000),
});
redis.on('error', (err) => console.error('Redis error:', err));

// ── Circuit Breaker ─────────────────────────────────────
const breaker = {
  failures: 0, lastFail: 0, threshold: 5, resetAfter: 30000,
  isOpen() {
    if (this.failures < this.threshold) return false;
    if (Date.now() - this.lastFail > this.resetAfter) { this.failures = 0; return false; }
    return true;
  },
  recordFail() { this.failures++; this.lastFail = Date.now(); }
};

// ── In-Flight Deduplication ─────────────────────────────
// If 50 users request the same song at once, only ONE upstream call happens.
const inFlight = new Map();

async function fetchWithDedup(url) {
  if (inFlight.has(url)) return inFlight.get(url);
  const promise = (async () => {
    const res = await fetch(url, {
      headers: { 'User-Agent': 'SoniMusic/1.0' },
      timeout: 10000
    });
    if (!res.ok) throw new Error(`Upstream ${res.status}`);
    return res.json();
  })().finally(() => inFlight.delete(url));
  inFlight.set(url, promise);
  return promise;
}

// ── Cached Fetch ────────────────────────────────────────
async function cachedFetch(path, ttlSeconds, staleTtl = ttlSeconds * 7) {
  const cacheKey = `soni:${path}`;
  const cached = await redis.get(cacheKey);
  if (cached) return { data: JSON.parse(cached), source: 'cache' };

  if (breaker.isOpen()) {
    const stale = await redis.get(`${cacheKey}:stale`);
    if (stale) return { data: JSON.parse(stale), source: 'stale' };
    throw new Error('Upstream unavailable');
  }

  try {
    const data = await fetchWithDedup(`${JIOSAAVN_API}${path}`);
    const fresh = JSON.stringify(data);
    await redis.setex(cacheKey, ttlSeconds, fresh);
    await redis.setex(`${cacheKey}:stale`, staleTtl, fresh);
    breaker.failures = 0;
    return { data, source: 'upstream' };
  } catch (err) {
    breaker.recordFail();
    const stale = await redis.get(`${cacheKey}:stale`);
    if (stale) return { data: JSON.parse(stale), source: 'stale' };
    throw err;
  }
}

// ── Piped Fallback ──────────────────────────────────────
let activePiped = 0;

async function getPipedStream(videoId) {
  for (let i = 0; i < PIPED_INSTANCES.length; i++) {
    const idx = (activePiped + i) % PIPED_INSTANCES.length;
    const host = PIPED_INSTANCES[idx];
    try {
      const res = await fetch(`${host}/streams/${videoId}`, { timeout: 6000 });
      if (!res.ok) continue;
      const data = await res.json();
      const audio = data.audioStreams || [];
      if (audio.length > 0) {
        const med = audio.find(a => a.quality?.includes('128')) || audio[0];
        if (med?.url) { activePiped = idx; return med.url; }
      }
    } catch (e) { continue; }
  }
  return null;
}

async function searchPiped(query) {
  for (let i = 0; i < PIPED_INSTANCES.length; i++) {
    const idx = (activePiped + i) % PIPED_INSTANCES.length;
    const host = PIPED_INSTANCES[idx];
    try {
      const res = await fetch(`${host}/search?q=${encodeURIComponent(query)}&filter=videos`, { timeout: 6000 });
      if (!res.ok) continue;
      const data = await res.json();
      if (data.items?.length) {
        activePiped = idx;
        return data.items.slice(0, 3).map(item => ({
          videoId: item.url?.split('watch?v=')[1]?.split('&')[0],
          title: item.title
        })).filter(v => v.videoId);
      }
    } catch (e) { continue; }
  }
  return [];
}

// ── Routes ──────────────────────────────────────────────

// Search songs — cache 1 hour
app.get('/api/search', async (req, res) => {
  const { query, limit = 20 } = req.query;
  if (!query) return res.status(400).json({ error: 'query required' });

  try {
    const { data, source } = await cachedFetch(
      `/api/search/songs?query=${encodeURIComponent(query)}&limit=${limit}`,
      3600
    );
    res.set('X-Cache', source);
    res.json(data);
  } catch (err) {
    res.status(503).json({ error: 'Search unavailable' });
  }
});

// Get song stream URL — tries JioSaavn CDN, falls back to Piped
app.get('/api/stream/:id', async (req, res) => {
  const { id } = req.params;
  const { title, artist } = req.query;

  try {
    // Try JioSaavn CDN first
    const { data } = await cachedFetch(`/api/songs?id=${id}`, 86400);
    const song = data.data?.[0];
    if (song?.downloadUrl?.length) {
      const url = song.downloadUrl.find(u => u.quality === '320kbps')?.link
        || song.downloadUrl.find(u => u.quality === '160kbps')?.link
        || song.downloadUrl[song.downloadUrl.length - 1]?.link;
      if (url) return res.json({ url, source: 'jiosaavn' });
    }

    // Fallback: Piped/YouTube
    if (title && artist) {
      const query = `${title} ${artist}`.trim();
      const results = await searchPiped(query);
      for (const r of results) {
        const streamUrl = await getPipedStream(r.videoId);
        if (streamUrl) return res.json({ url: streamUrl, source: 'piped' });
      }
    }

    res.status(404).json({ error: 'No stream found' });
  } catch (err) {
    res.status(503).json({ error: 'Stream unavailable' });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ ok: true, breaker: breaker.isOpen() ? 'open' : 'closed', uptime: process.uptime() });
});

app.listen(PORT, () => {
  console.log(`Soni proxy live on :${PORT}`);
  console.log(`JioSaavn API: ${JIOSAAVN_API}`);
});
