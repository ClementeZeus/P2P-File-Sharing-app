/**
 * api/turn.js
 * -----------------------------------------------------------------------
 * Vercel Serverless Function that hands the client its TURN relay
 * credentials at runtime, instead of having them hardcoded in client.js.
 *
 * Why: client.js is public source in the repo — anything written there
 * literally is the file GitHub shows anyone who opens it. The actual
 * values live only in this function's environment (Vercel dashboard ->
 * Project -> Settings -> Environment Variables -> METERED_TURN_USERNAME /
 * METERED_TURN_CREDENTIAL), same pattern as GROQ_API_KEY in api/chat.js.
 *
 * Caveat that's unavoidable for any client-side WebRTC app: a browser
 * actually using the live site still receives these values (DevTools ->
 * Network -> /api/turn will show them) because the browser is the one
 * that has to authenticate to the TURN relay. This endpoint only keeps
 * them out of the GitHub repo / source code — it does not make them
 * secret from someone inspecting network traffic on the running site.
 * If that visible-at-runtime exposure is a problem, rotate the credential
 * in the Metered dashboard periodically, or switch to minting short-lived
 * per-session credentials via Metered's authenticated REST API instead of
 * reusing one static pair.
 * -----------------------------------------------------------------------
 */

module.exports = async function handler(req, res) {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'Method not allowed.' });
  }

  const username = process.env.METERED_TURN_USERNAME || '';
  const credential = process.env.METERED_TURN_CREDENTIAL || '';

  res.setHeader('Cache-Control', 'no-store');

  if (!username || !credential) {
    // Fail soft: client falls back to STUN-only rather than erroring out.
    console.error('TURN_MISCONFIGURED: METERED_TURN_USERNAME/CREDENTIAL not set');
    return res.status(200).json({ iceServers: [] });
  }

  return res.status(200).json({
    iceServers: [
      { urls: 'turn:global.relay.metered.ca:80', username, credential },
      { urls: 'turn:global.relay.metered.ca:80?transport=tcp', username, credential },
      { urls: 'turn:global.relay.metered.ca:443', username, credential },
      { urls: 'turns:global.relay.metered.ca:443?transport=tcp', username, credential },
    ],
  });
};
