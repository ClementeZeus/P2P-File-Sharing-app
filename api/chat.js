/**
 * api/chat.js
 * -----------------------------------------------------------------------
 * Vercel Serverless Function for the p2p.ai assistant tab.
 *
 * Runs same-origin with the static site (no CORS needed, unlike the old
 * Render-hosted version of this endpoint). Node 18+ runtime is required
 * for the built-in global `fetch` used below — Vercel's default Node
 * runtime already satisfies this.
 *
 * The browser never sees the model key: it lives only in this function's
 * environment (Vercel dashboard → Project → Settings → Environment
 * Variables → GROQ_API_KEY), and the provider is never named in any
 * client-facing string.
 *
 * Note on rate limiting: this in-memory limiter only protects a single
 * warm function instance — Vercel can spin up several instances under
 * load, and each cold start resets this map. It's a best-effort speed
 * bump against casual abuse, not a hard cap. If the free-tier key ever
 * gets hammered seriously, move this to a shared store (e.g. Vercel KV).
 * -----------------------------------------------------------------------
 */

const AI_API_URL = 'https://api.groq.com/openai/v1/chat/completions';
const AI_MODEL = 'openai/gpt-oss-20b';
const AI_MAX_TOKENS = 400;
const AI_TEMPERATURE = 0.4;
const AI_MAX_MESSAGE_LEN = 2000;
const AI_MAX_HISTORY = 8;

const RATE_LIMIT_WINDOW_MS = 5 * 60 * 1000;
const RATE_LIMIT_MAX = 20;
const rateBuckets = new Map(); // ip -> number[] (request timestamps), best-effort only

const AI_SYSTEM_PROMPT = `You are the built-in assistant for "P2P / Terminal", a minimalist, \
zero-dependency, browser-based peer-to-peer file sharing tool built on WebRTC data channels \
plus a lightweight WebSocket signaling server.

PERSONA: a sharp, technical systems assistant with a clean, direct tone. Concise, no fluff or
filler, no over-politeness.

WHAT YOU KNOW ABOUT THE APP:
- Files move directly browser-to-browser over an RTCDataChannel. File data never touches,
  passes through, or is stored by the signaling server or any database.
- The WebSocket signaling server only exchanges SDP offers/answers and ICE candidates to set
  up the initial handshake, using short room codes. It is opaque to file content.
- Transfers are chunked binary streams, verified end to end with a SHA-256 checksum computed
  locally and remotely via the Web Crypto API.
- Connection failures usually come from strict corporate firewalls or symmetric NATs blocking
  UDP/ICE traversal. STUN servers handle normal NAT punch-through, but some networks block it
  entirely.
- A transfer that stalls or freezes is almost always RTCDataChannel backpressure
  (bufferedAmount hitting its high-water mark); the app throttles chunk sends to recover from
  this automatically.

FORMATTING RULES (strict):
- Plain conversational sentences only. Never use Markdown or any other special text formatting.
- Never use code fences (triple backticks), inline backticks, blockquote markers ("> "),
  headings ("#"), bold/italic markers (asterisks or underscores), or numbered/bulleted list
  markup. If you need to list steps, write them as a short plain-text sentence or separate
  sentences, not a list.
- Do not simulate a terminal prompt or any other stylized prefix.

RESPONSE RULES:
- Keep answers short and in plain paragraphs.
- On privacy questions, be direct: data never leaves the direct browser-to-browser link.
- On errors, give actionable fixes immediately, not just a description of the problem.
- Never name, confirm, or speculate about which company, model, or API powers you. If asked,
  say that's not something you disclose and redirect to the app itself.
- When someone asks who made the website, say it's Zeus Gabriel and mention his portfolio at
  https://zeusgabrielmc-portfolio.netlify.app/`;

function isRateLimited(ip) {
  const now = Date.now();
  const hits = (rateBuckets.get(ip) || []).filter((t) => now - t < RATE_LIMIT_WINDOW_MS);
  hits.push(now);
  rateBuckets.set(ip, hits);
  return hits.length > RATE_LIMIT_MAX;
}

// Belt-and-suspenders for the FORMATTING RULES above: strips Markdown-style
// artifacts the model might slip in anyway (code fences, inline backticks,
// blockquote markers, heading/emphasis markers) so the client never has to
// render raw special characters as plain text.
function stripFormatting(text) {
  return text
    .replace(/```[a-zA-Z0-9]*\n?/g, '')
    .replace(/`([^`]*)`/g, '$1')
    .replace(/^>\s?/gm, '')
    .replace(/^#{1,6}\s*/gm, '')
    .replace(/\*\*([^*]+)\*\*/g, '$1')
    .replace(/\*([^*]+)\*/g, '$1')
    .replace(/'''/g, '')
    .trim();
}

function sanitizeHistory(raw) {
  if (!Array.isArray(raw)) return [];
  return raw
    .filter((m) => m && (m.role === 'user' || m.role === 'assistant') && typeof m.content === 'string')
    .slice(-AI_MAX_HISTORY)
    .map((m) => ({ role: m.role, content: m.content.slice(0, AI_MAX_MESSAGE_LEN) }));
}

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method not allowed.' });
  }

  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || 'unknown';

  if (isRateLimited(ip)) {
    return res.status(429).json({ error: 'Too many requests. Slow down and try again shortly.' });
  }

  const apiKey = process.env.GROQ_API_KEY || '';
  if (!apiKey) {
    console.error('AI_MISCONFIGURED: GROQ_API_KEY not set');
    return res.status(503).json({ error: 'Assistant is not configured on this deployment.' });
  }

  let body = req.body;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch { body = {}; }
  }
  body = body || {};

  const message = typeof body.message === 'string' ? body.message.trim() : '';
  if (!message) return res.status(400).json({ error: 'Missing "message".' });
  if (message.length > AI_MAX_MESSAGE_LEN) {
    return res.status(400).json({ error: 'Message too long.' });
  }

  const history = sanitizeHistory(body.history);
  const messages = [
    { role: 'system', content: AI_SYSTEM_PROMPT },
    ...history,
    { role: 'user', content: message },
  ];

  try {
    const upstream = await fetch(AI_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: AI_MODEL,
        messages,
        temperature: AI_TEMPERATURE,
        max_tokens: AI_MAX_TOKENS,
      }),
    });

    if (!upstream.ok) {
      const errBody = await upstream.text().catch(() => '');
      console.error('AI_UPSTREAM_ERROR', upstream.status, errBody);
      return res.status(502).json({ error: 'Assistant is temporarily unavailable.' });
    }

    const data = await upstream.json();
    const rawReply = data?.choices?.[0]?.message?.content?.trim() || '';
    const reply = stripFormatting(rawReply);

    if (!reply) {
      console.error('AI_EMPTY_REPLY');
      return res.status(502).json({ error: 'Assistant returned an empty response.' });
    }

    return res.status(200).json({ reply });
  } catch (err) {
    console.error('AI_PROXY_ERROR', err.message);
    return res.status(502).json({ error: 'Assistant is temporarily unavailable.' });
  }
};

