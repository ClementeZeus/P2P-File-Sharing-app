/**
 * client.js
 * -----------------------------------------------------------------------
 * P2P::TERMINAL — client logic.
 *
 * Four jobs, kept in four clear sections:
 *   1. SIGNALING   — talk to server.js over WebSocket to find a peer.
 *   2. WEBRTC      — negotiate RTCPeerConnection + RTCDataChannel.
 *   3. TRANSFER    — chunk, send/receive, backpressure, SHA-256, download.
 *   4. BOOT        — first-paint loading screen.
 *
 * Design note on memory: for simplicity and correctness, the sender reads
 * the whole file into one ArrayBuffer up front (file.arrayBuffer()) so the
 * same buffer can both be hashed (crypto.subtle.digest has no streaming
 * API) and sliced into chunks. That means peak memory use is roughly the
 * size of the file being sent, on both ends. Fine for a terminal utility;
 * for multi-GB files you'd swap this for a streaming hash (e.g. a JS
 * incremental SHA-256) and `file.slice()` reads straight off disk.
 *
 * Fix note (sending progress bar freezing): sendChunks() used to run a
 * single synchronous `while` loop across every chunk of the file, only
 * ever pausing when the data channel's bufferedAmount crossed a 16 MB
 * high-water mark. Queuing a 64 KB chunk is almost instant, and
 * bufferedAmount is updated asynchronously by the browser rather than the
 * instant a chunk is queued — so on a fast link the loop could blast
 * straight through an entire file without that condition ever tripping.
 * A synchronous loop blocks the main thread, and the main thread is also
 * what paints the page, so every progress-bar update got queued but never
 * drawn until the loop finally finished. That's why the receiver's
 * progress bar (driven by separate incoming-message events, each its own
 * turn of the event loop) animated smoothly while the sender's froze on
 * the first computed values. The fix below yields back to the browser on
 * a fixed chunk cadence, independent of bufferedAmount, so the UI thread
 * always gets a chance to repaint.
 * -----------------------------------------------------------------------
 */

(() => {
  'use strict';

  // =========================================================================
  // CONFIG
  // =========================================================================

  // Point this at your deployed Render service, e.g.:
  //   const SIGNALING_URL = 'wss://p2p-terminal-signaling.onrender.com';
  // Render terminates TLS on 443 and proxies internally to your app's PORT,
  // so no :port suffix goes here — just the bare host, scheme wss://.
  // (Only use the localhost fallback below while testing on one machine.)
  const SIGNALING_URL = 'wss://p2p-file-sharing-terminal.onrender.com';
  // const SIGNALING_URL = 'ws://localhost:8080'; // local testing fallback

  // p2p.ai runs as a Vercel Serverless Function alongside this static site
  // (api/chat.js) — same origin as the page, so no CORS and no dependency
  // on the Render signaling server's uptime.
  const AI_ENDPOINT = '/api/chat';
  const AI_HISTORY_LIMIT = 8; // messages (not pairs) kept as context for the assistant

  // STUN alone only works when both peers are behind "easy" NATs. Mobile
  // carrier networks almost always use symmetric/carrier-grade NAT, which
  // STUN can't punch through — that's why phone <-> laptop was failing
  // while WiFi <-> WiFi (or phone <-> itself in a second tab) would've
  // worked. STUN entries carry no secret, so they're fine to keep here —
  // this is the only ICE config the app has; there's no TURN, no
  // credentialed relay, no third-party account of any kind.
  //
  // When a direct WebRTC link doesn't come up within RELAY_FALLBACK_MS,
  // or drops mid-transfer, client.js falls back to shipping file data
  // over the exact same WebSocket connection already open to
  // SIGNALING_URL, with server.js forwarding the raw bytes to the other
  // peer. That path needs no API key, only your own server. See the
  // transport layer in SECTION 2b below.
  const ICE_SERVERS = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
  ];

  // How long to let the direct WebRTC handshake try before giving up on
  // it for this transfer and using the WS relay instead. Generous enough
  // that a slow-but-working STUN negotiation isn't cut off early; short
  // enough that a dead negotiation doesn't leave the user staring at a
  // spinner. If WebRTC *does* come up after this fires, client.js keeps
  // using it anyway (see transportMode()) — the timer only controls how
  // long we wait before treating relay as good enough to proceed with.
  const RELAY_FALLBACK_MS = 6000;

  const CHUNK_SIZE = 64 * 1024;                 // 64 KB per data channel message
  // Chromium's RTCDataChannel has an internal hard cap on queued-but-unsent
  // bytes around 16 MB (stricter still on some mobile/WebView builds). The
  // old HIGH_WATER sat exactly on that ceiling, so one more chunk queued
  // right at the limit could overflow it and throw "send queue is full"
  // instead of pausing cleanly. Keep real headroom under it.
  const BUFFER_HIGH_WATER = 1 * 1024 * 1024;    // pause sending above this
  const BUFFER_LOW_WATER = 256 * 1024;          // resume sending below this
  const UI_THROTTLE_MS = 100;                   // max metrics DOM update rate
  const YIELD_EVERY_N_CHUNKS = 12;              // ~768 KB — keeps the UI thread responsive

  const WS_RECONNECT_MIN_MS = 1000;
  const WS_RECONNECT_MAX_MS = 10000;

  const BOOT_MIN_MS = 650;  // loading screen never flashes away instantly
  const BOOT_MAX_MS = 5000; // ...but never blocks the app indefinitely either

  // =========================================================================
  // DOM REFS
  // =========================================================================

  const $ = (id) => document.getElementById(id);

  const el = {
    heroStatus: $('hero-status'),
    dotWs: $('dot-ws'), txtWs: $('txt-ws'),
    dotPeer: $('dot-peer'), txtPeer: $('txt-peer'),
    linkDiagram: $('link-diagram'),
    btnCreate: $('btn-create'),
    btnJoin: $('btn-join'),
    inputJoinCode: $('input-join-code'),
    roomCodeBox: $('room-code-box'),
    roomCodeValue: $('room-code-value'),
    btnCopy: $('btn-copy'),
    btnCopyLink: $('btn-copy-link'),
    btnQrToggle: $('btn-qr-toggle'),
    qrPanel: $('room-qr-panel'),
    qrCanvas: $('room-qr-canvas'),
    dropzone: $('dropzone'),
    fileInput: $('file-input'),
    log: $('terminal-log'),
    btnClearLog: $('btn-clear-log'),
    transferCard: $('transfer-card'),
    filename: $('stat-filename'),
    direction: $('stat-direction'),
    filesize: $('stat-filesize'),
    filetype: $('stat-filetype'),
    filecount: $('stat-filecount'),
    progressFill: $('progress-fill'),
    pct: $('stat-pct'),
    bytes: $('stat-bytes'),
    speed: $('stat-speed'),
    eta: $('stat-eta'),
    checksum: $('stat-checksum'),
    transport: $('stat-transport'),
    duration: $('stat-duration'),
    avgSpeed: $('stat-avgspeed'),
    started: $('stat-started'),
    hashLocal: $('hash-local'),
    hashRemote: $('hash-remote'),
    btnCopyLocalHash: $('btn-copy-local-hash'),
    btnCopyRemoteHash: $('btn-copy-remote-hash'),
    bootLoader: $('boot-loader'),
    bootStatus: $('boot-status'),
    tabBtnDocs: $('tab-btn-docs'),
    tabBtnApp: $('tab-btn-app'),
    tabBtnAi: $('tab-btn-ai'),
    panelDocs: $('panel-docs'),
    panelApp: $('panel-app'),
    panelAi: $('panel-ai'),
    btnMenu: $('btn-menu'),
    mobileMenu: $('mobile-menu'),
    aiChat: $('ai-chat'),
    aiForm: $('ai-form'),
    aiInput: $('ai-input'),
    aiSend: $('ai-send'),
    aiTyping: $('ai-typing'),
    aiSuggestList: $('ai-suggest-list'),
    tabBtnSpeed: $('tab-btn-speed'),
    panelSpeed: $('panel-speed'),
    speedHeroStatus: $('speed-hero-status'),
    speedDot: $('speed-dot'),
    speedPhaseText: $('speed-phase-text'),
    btnSpeedStart: $('btn-speed-start'),
    btnSpeedStop: $('btn-speed-stop'),
    speedProgressWrap: $('speed-progress-wrap'),
    speedProgressFill: $('speed-progress-fill'),
    speedProgressLabel: $('speed-progress-label'),
    speedLog: $('speed-log'),
    btnSpeedClearLog: $('btn-speed-clear-log'),
    speedStatPing: $('speed-stat-ping'),
    speedStatJitter: $('speed-stat-jitter'),
    speedStatDownload: $('speed-stat-download'),
    speedStatUpload: $('speed-stat-upload'),
  };

  // =========================================================================
  // STATE
  // =========================================================================

  let ws = null;
  let pc = null;
  let dataChannel = null;
  let roomId = null;
  let isHost = false;
  let wsReconnectAttempts = 0;
  let wsReconnectTimer = null;

  // True once the other socket is confirmed in the same room — the WS
  // relay transport needs nothing more than this to be usable, since the
  // room pairing already exists on server.js the moment both are joined.
  let peerPresent = false;
  let relayFallbackTimer = null;
  let relayAnnounced = false; // so the "using relay" log line only fires once per session

  // True once this "teardown episode" (one drop of ICE / connection-state /
  // data-channel, however many of those three events fire for it) has
  // already logged its single relay-fallback line. Without this, a single
  // WebRTC drop that's covered by the relay produces three separate log
  // lines (ice state, connection state, data channel) all saying the same
  // thing. Reset whenever a fresh link attempt starts so the next drop can
  // log its own line again.
  let relayTeardownLogged = false;

  // Shareable join links look like #join=ABC234 — opening one should drop
  // the person straight into the "app" tab and auto-join that room as soon
  // as the signaling socket connects, without them typing the code in.
  const JOIN_LINK_RE = /^#join=([A-Za-z0-9]{6})$/;
  let pendingJoinCode = (() => {
    const m = JOIN_LINK_RE.exec(location.hash);
    return m ? m[1].toUpperCase() : null;
  })();

  function buildJoinLink(code) {
    return `${location.origin}${location.pathname}#join=${code}`;
  }

  /** Receiver-side accumulation state for the file currently inbound. */
  let inbound = null; // { name, size, mime, chunks: [], received, startTime }

  let lastUiTick = 0;

  // Timestamp (performance.now()) of the most recent completed transfer,
  // either direction. Used to soften ICE/connection/data-channel teardown
  // logging right after a transfer finishes.
  //
  // What was happening: nothing in this app calls pc.close() once a
  // transfer is done, so the RTCPeerConnection just sits idle. With no
  // more traffic flowing, a NAT/router on either end can quietly drop its
  // UDP mapping for the ICE candidate pair, or the OS/browser can
  // deprioritize a backgrounded tab's network stack — either way, the
  // browser reports iceConnectionState 'disconnected' and then
  // connectionState 'failed'/'closed' a moment later. That's completely
  // normal post-transfer cleanup, not a real failure: the transfer itself
  // already completed and was already logged as such. The old code logged
  // every one of these as a loud 'err' line ("connection failed"), which
  // reads as if something went wrong mid-transfer even though it didn't.
  // This grace window just downgrades those specific log lines to 'info'
  // when they land shortly after a completed transfer; it doesn't change
  // any actual reconnect/fallback behavior.
  let lastTransferDoneAt = 0;
  const POST_TRANSFER_GRACE_MS = 5000;

  function markTransferDone() {
    lastTransferDoneAt = performance.now();
  }

  function justFinishedTransfer() {
    return lastTransferDoneAt > 0 && (performance.now() - lastTransferDoneAt) < POST_TRANSFER_GRACE_MS;
  }

  // True whenever the WS relay is available as a transport (same check
  // reportLinkDown() uses to decide the status badge). When this is true,
  // a WebRTC/ICE failure doesn't actually take the link down — transportSend()
  // just keeps using the relay — so it isn't something to alarm the user about.
  function hasRelayFallback() {
    return peerPresent && ws && ws.readyState === WebSocket.OPEN;
  }

  // Returns a short explanatory clause for a WebRTC teardown event, or null
  // if there's no good explanation and it really is worth flagging to the
  // user. Centralizes the "is this actually a problem" judgment so the ICE
  // handler, connection-state handler, and data-channel handler all agree.
  function describeLinkTeardown() {
    if (justFinishedTransfer()) return 'transfer already completed, idle link teardown, not an error';
    if (hasRelayFallback()) return 'relay fallback';
    return null;
  }

  // True specifically for the "WebRTC dropped but the relay already has us
  // covered" case, as opposed to the harmless post-transfer idle teardown.
  // The three handlers below collapse this one case into a single log line
  // instead of each logging their own raw state.
  function isRelayCoveredTeardown() {
    return !justFinishedTransfer() && hasRelayFallback();
  }

  // Logs the single "now riding the relay" line for the current teardown
  // episode, once, no matter which of the three handlers notices it first.
  function logRelayTeardownOnce() {
    if (relayTeardownLogged) return;
    relayTeardownLogged = true;
    logLine('ice state: connected via relay', 'ok');
  }

  // =========================================================================
  // BOOT / LOADING SCREEN
  // =========================================================================

  const bootStart = performance.now();
  let bootDone = false;

  function setBootStatus(text) {
    if (el.bootStatus) el.bootStatus.textContent = text;
  }

  function finishBoot() {
    if (bootDone) return;
    const elapsed = performance.now() - bootStart;
    const wait = Math.max(0, BOOT_MIN_MS - elapsed);
    setTimeout(hideBootLoader, wait);
  }

  function hideBootLoader() {
    if (bootDone || !el.bootLoader) return;
    bootDone = true;
    el.bootLoader.classList.add('boot-loader-hide');
    el.bootLoader.addEventListener('transitionend', () => el.bootLoader.remove(), { once: true });
    setTimeout(() => el.bootLoader?.remove(), 700); // fallback if transitionend never fires
  }

  // Hard cap — never let the loading screen block the app, even if the
  // signaling server is completely unreachable.
  setTimeout(finishBoot, BOOT_MAX_MS);

  // =========================================================================
  // LOG + STATUS HELPERS
  // =========================================================================

  function logLine(message, level = 'info', target = el.log) {
    const ts = new Date().toLocaleTimeString('en-GB', { hour12: false });
    const row = document.createElement('div');
    row.className = 'log-line';
    row.innerHTML = `<span class="log-ts">${ts}</span><span class="log-${level}">${escapeHtml(message)}</span>`;
    target.appendChild(row);
    target.scrollTop = target.scrollHeight;
  }

  function escapeHtml(str) {
    return str.replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  function setWsStatus(state) {
    // state: 'off' | 'connecting' | 'on' | 'err'
    el.dotWs.className = 'status-dot ' + (state === 'on' ? 'on' : state === 'err' ? 'err' : state === 'connecting' ? 'warn' : 'off');
    el.txtWs.textContent = { off: 'offline', connecting: 'connecting', on: 'online', err: 'error' }[state];
    // A session can only be started once we have a live signaling link.
    const ready = state === 'on';
    el.btnCreate.disabled = !ready;
    el.btnJoin.disabled = !ready;
  }

  function setPeerStatus(state, detail) {
    // state: 'none' | 'waiting' | 'connected' | 'lost'
    // detail: optional short suffix, e.g. 'relay' when the link is up via
    // the WS fallback transport rather than a direct WebRTC data channel.
    el.dotPeer.className = 'status-dot ' + (state === 'connected' ? 'on' : state === 'waiting' ? 'warn' : state === 'lost' ? 'err' : 'off');
    el.txtPeer.textContent = { none: 'none', waiting: 'waiting', connected: 'linked', lost: 'lost' }[state] + (detail ? ` (${detail})` : '');

    // Drive the hero readout — the single most load-bearing fact on screen.
    el.heroStatus.textContent = ({
      none: 'No active session', waiting: 'Awaiting peer', connected: 'Connected', lost: 'Link lost',
    }[state]) + (detail ? ` (${detail})` : '');
    el.heroStatus.className = 'hero-value ' + (
      state === 'connected' ? 'state-on' : state === 'waiting' ? 'state-wait' : state === 'lost' ? 'state-err' : ''
    );
    el.linkDiagram.className = 'link-diagram ' + (
      state === 'connected' ? 'state-on' : state === 'waiting' ? 'state-wait' : state === 'lost' ? 'state-err' : ''
    );
  }

  function setLinkActivity(active) {
    el.linkDiagram.classList.toggle('is-active', active);
  }

  // =========================================================================
  // FORMATTING UTILITIES
  // =========================================================================

  function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
    return `${(bytes / 1024 ** i).toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
  }

  function formatSpeed(bytesPerSec) {
    return `${formatBytes(bytesPerSec)}/s`;
  }

  function formatEta(seconds) {
    if (!isFinite(seconds) || seconds < 0) return '--';
    if (seconds < 1) return '<1s';
    const m = Math.floor(seconds / 60);
    const s = Math.round(seconds % 60);
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
  }

  // mm:ss readout for the Duration metric. Separate from formatEta() above:
  // that one counts down an estimate, this one counts up an elapsed time,
  // and they're rounded differently (floor here so it doesn't visually
  // tick past the final elapsed value by a second at completion).
  function formatDuration(seconds) {
    if (!isFinite(seconds) || seconds < 0) return '00:00';
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds % 60);
    return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
  }

  function formatClockTime(date) {
    return date.toLocaleTimeString('en-GB', { hour12: false });
  }

  // Human label for whichever transport is actually moving bytes right
  // now, for the Transport metric on the Transfer card. Mirrors the same
  // transportMode() check the send/relay logic itself uses, so the label
  // never drifts out of sync with reality.
  function transportLabel() {
    const mode = transportMode();
    if (mode === 'webrtc') return 'Direct (P2P)';
    if (mode === 'relay') return 'Relay';
    return '--';
  }

  function transportLabelClass() {
    const mode = transportMode();
    if (mode === 'webrtc') return 'ok';
    if (mode === 'relay') return 'wait';
    return '';
  }

  function bufferToHex(buffer) {
    return [...new Uint8Array(buffer)].map((b) => b.toString(16).padStart(2, '0')).join('');
  }

  async function copyToClipboard(text, successMessage) {
    if (!text || text === '--') return;
    try {
      await navigator.clipboard.writeText(text);
      logLine(successMessage, 'ok');
      return true;
    } catch {
      logLine('clipboard unavailable, copy the value manually', 'warn');
      return false;
    }
  }

  function flashCopied(button) {
    button.classList.add('copied');
    clearTimeout(button._copiedTimer);
    button._copiedTimer = setTimeout(() => button.classList.remove('copied'), 1400);
  }

  // =========================================================================
  // TRANSFER UI
  // =========================================================================

  function resetTransferUi(name, size, direction, mime, fileCount = 1) {
    el.transferCard.classList.add('has-transfer');
    el.filename.textContent = name;
    el.filesize.textContent = formatBytes(size);
    el.filetype.textContent = mime || 'unknown type';
    el.direction.textContent = direction;
    el.filecount.textContent = `${fileCount} files`;
    el.filecount.classList.toggle('hidden', fileCount <= 1);
    el.progressFill.style.transform = 'scaleX(0)';
    el.pct.textContent = '0%';
    el.bytes.textContent = `0 B / ${formatBytes(size)}`;
    el.speed.textContent = '0 KB/s';
    el.eta.textContent = '--';
    el.checksum.textContent = 'pending';
    el.checksum.className = 'metric-value wait';
    el.transport.textContent = transportLabel();
    el.transport.className = 'metric-value ' + transportLabelClass();
    el.duration.textContent = '00:00';
    el.avgSpeed.textContent = 'pending';
    el.avgSpeed.className = 'metric-value wait';
    el.started.textContent = formatClockTime(new Date());
    el.hashLocal.textContent = '--';
    el.hashRemote.textContent = '--';
    setLinkActivity(true);
  }

  function updateTransferMetrics(done, total, startTime, force = false) {
    const now = performance.now();
    if (!force && now - lastUiTick < UI_THROTTLE_MS) return;
    lastUiTick = now;

    const pct = total > 0 ? Math.min(100, (done / total) * 100) : 0;
    const elapsedSec = (now - startTime) / 1000;
    const speed = elapsedSec > 0 ? done / elapsedSec : 0;
    const remaining = total - done;
    const etaSec = speed > 0 ? remaining / speed : Infinity;

    el.progressFill.style.transform = `scaleX(${(pct / 100).toFixed(4)})`;
    el.pct.textContent = `${pct.toFixed(1)}%`;
    el.bytes.textContent = `${formatBytes(done)} / ${formatBytes(total)}`;
    el.speed.textContent = formatSpeed(speed);
    el.eta.textContent = formatEta(etaSec);
    el.duration.textContent = formatDuration(elapsedSec);

    // The transport can change mid-transfer (a direct link dropping to
    // relay partway through), so keep this live rather than only setting
    // it once at the start.
    el.transport.textContent = transportLabel();
    el.transport.className = 'metric-value ' + transportLabelClass();

    // done === total marks the finished call every caller makes with
    // force=true, so this is also where the transfer's final, true
    // average speed gets its own distinct readout, separate from the
    // live "Speed" figure above which is only meaningful while a transfer
    // is actually in progress.
    if (force && total > 0 && done >= total) {
      el.avgSpeed.textContent = formatSpeed(speed);
      el.avgSpeed.className = 'metric-value ok';
    }
  }

  // =========================================================================
  // SECTION 1 — SIGNALING (WebSocket to server.js)
  // =========================================================================

  function connectSignaling() {
    clearTimeout(wsReconnectTimer);
    setWsStatus('connecting');
    setBootStatus('Connecting to signaling server…');
    ws = new WebSocket(SIGNALING_URL);
    ws.binaryType = 'arraybuffer'; // relay-mode chunks arrive as ArrayBuffer, matching the data channel

    ws.onopen = () => {
      wsReconnectAttempts = 0;
      setWsStatus('on');
      logLine(`signal channel open: ${SIGNALING_URL}`, 'ok');
      setBootStatus('Connected');
      finishBoot();

      if (pendingJoinCode) {
        const code = pendingJoinCode;
        pendingJoinCode = null; // one-shot: don't re-auto-join on a later reconnect
        el.inputJoinCode.value = code;
        logLine(`joining room ${code} from shared link...`, 'info');
        sendSignalingMessage({ type: 'join', roomId: code });
      }
    };

    ws.onmessage = (event) => {
      // Relay-mode file chunk: binary frame forwarded opaquely by
      // server.js. Feed it through the exact same path a WebRTC data
      // channel message would take, so TRANSFER-section code never needs
      // to know which transport actually carried the bytes.
      if (typeof event.data !== 'string') {
        return handleIncomingMessage(event);
      }

      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return logLine('received malformed signaling message', 'err');
      }

      // File-control chatter (meta/end/verify-result) rides the relay
      // channel wrapped in a 'relay' envelope — see sendFileControl().
      // Unwrap it and hand the inner object to the exact same parser the
      // WebRTC data channel path uses; everything NOT wrapped that way is
      // server.js's own signaling protocol (created/joined/signal/...).
      if (msg.type === 'relay') {
        return handleIncomingMessage({ data: JSON.stringify(msg.data) });
      }
      handleSignalingMessage(msg);
    };

    ws.onerror = () => {
      setWsStatus('err');
      logLine('signal channel error', 'err');
    };

    ws.onclose = () => {
      setWsStatus('off');
      setPeerStatus('none');
      teardownPeerConnection();
      roomId = null;
      isHost = false;
      peerPresent = false;
      relayAnnounced = false;
      relayTeardownLogged = false;
      clearTimeout(relayFallbackTimer);
      el.roomCodeBox.classList.add('hidden');
      setBootStatus('Signaling unavailable, retrying…');
      finishBoot();
      scheduleReconnect();
    };
  }

  function scheduleReconnect() {
    wsReconnectAttempts += 1;
    const delay = Math.min(WS_RECONNECT_MAX_MS, WS_RECONNECT_MIN_MS * 2 ** (wsReconnectAttempts - 1));
    logLine(`signal channel closed, reconnecting in ${Math.round(delay / 1000)}s`, 'warn');
    clearTimeout(wsReconnectTimer);
    wsReconnectTimer = setTimeout(connectSignaling, delay);
  }

  function sendSignalingMessage(payload) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload));
    } else {
      logLine('cannot reach signaling server, check your connection', 'err');
    }
  }

  function handleSignalingMessage(msg) {
    switch (msg.type) {
      case 'created':
        roomId = msg.roomId;
        isHost = true;
        el.roomCodeValue.textContent = roomId;
        el.roomCodeBox.classList.remove('hidden');
        el.qrPanel.classList.add('hidden');
        el.btnQrToggle.setAttribute('aria-expanded', 'false');
        renderJoinQr(buildJoinLink(roomId));
        setPeerStatus('waiting');
        logLine(`room created: code ${roomId}`, 'ok');
        logLine('waiting for peer to join...', 'info');
        break;

      case 'joined':
        roomId = msg.roomId;
        isHost = false;
        peerPresent = true;
        setPeerStatus('waiting');
        logLine(`joined room ${roomId}, negotiating link...`, 'ok');
        // Guest waits passively; host initiates the offer once it sees peer-joined.
        initPeerConnection(false);
        armRelayFallback();
        break;

      case 'peer-joined':
        // Fires on the HOST once the guest joins. Host drives the offer.
        peerPresent = true;
        logLine('peer joined, opening data channel...', 'ok');
        initPeerConnection(true);
        armRelayFallback();
        break;

      case 'peer-left':
        logLine('peer disconnected', 'warn');
        peerPresent = false;
        relayAnnounced = false;
        relayTeardownLogged = false;
        clearTimeout(relayFallbackTimer);
        setPeerStatus('lost');
        teardownPeerConnection();
        break;

      case 'signal':
        handleRemoteSignal(msg.data);
        break;

      case 'error':
        logLine(`server: ${msg.message}`, 'err');
        break;

      default:
        logLine(`unhandled signaling message: ${msg.type}`, 'warn');
    }
  }

  // =========================================================================
  // SECTION 2 — WEBRTC (RTCPeerConnection + RTCDataChannel)
  // =========================================================================

  function initPeerConnection(isInitiator) {
    if (pc) return; // already negotiating/connected

    relayTeardownLogged = false;
    pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

    pc.onicecandidate = (event) => {
      if (event.candidate) {
        sendSignalingMessage({ type: 'signal', data: { candidate: event.candidate } });
      }
    };

    pc.oniceconnectionstatechange = () => {
      if (!pc) return;
      const state = pc.iceConnectionState;
      const teardownState = state === 'disconnected' || state === 'failed' || state === 'closed';

      if (teardownState && isRelayCoveredTeardown()) {
        logRelayTeardownOnce();
      } else {
        const benign = teardownState ? describeLinkTeardown() : null;
        logLine(benign ? `ice state: ${state} (${benign})` : `ice state: ${state}`, 'info');
      }

      if (state === 'failed') {
        reportLinkDown(isRelayCoveredTeardown() ? null : 'ice handshake failed: peer may be behind a restrictive NAT');
      }
      if (state === 'disconnected') {
        reportLinkDown();
      }
    };

    pc.onconnectionstatechange = () => {
      if (!pc) return;
      if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
        if (isRelayCoveredTeardown()) {
          logRelayTeardownOnce();
        } else {
          const benign = describeLinkTeardown();
          if (benign) {
            logLine(`connection ${pc.connectionState} (${benign})`, 'info');
          } else {
            logLine(`connection ${pc.connectionState}`, 'err');
          }
        }
        teardownPeerConnection();
        reportLinkDown();
      }
    };

    if (isInitiator) {
      dataChannel = pc.createDataChannel('fileTransfer', { ordered: true });
      wireDataChannel(dataChannel);
      negotiateOffer();
    } else {
      pc.ondatachannel = (event) => {
        dataChannel = event.channel;
        wireDataChannel(dataChannel);
      };
    }
  }

  async function negotiateOffer() {
    try {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      sendSignalingMessage({ type: 'signal', data: { sdp: pc.localDescription } });
    } catch (err) {
      logLine(`failed to create offer: ${err.message}`, 'err');
    }
  }

  async function handleRemoteSignal(data) {
    if (!pc) {
      // Guest may receive the offer before it has called initPeerConnection —
      // shouldn't happen given message order, but guard anyway.
      initPeerConnection(false);
    }

    try {
      if (data.sdp) {
        await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
        if (data.sdp.type === 'offer') {
          const answer = await pc.createAnswer();
          await pc.setLocalDescription(answer);
          sendSignalingMessage({ type: 'signal', data: { sdp: pc.localDescription } });
        }
      } else if (data.candidate) {
        await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
      }
    } catch (err) {
      logLine(`signal handling error: ${err.message}`, 'err');
    }
  }

  function wireDataChannel(channel) {
    channel.binaryType = 'arraybuffer';
    channel.bufferedAmountLowThreshold = BUFFER_LOW_WATER;

    channel.onopen = () => {
      clearTimeout(relayFallbackTimer);
      relayAnnounced = false;
      relayTeardownLogged = false;
      setPeerStatus('connected');
      logLine('data channel open, link established', 'ok');
    };

    channel.onclose = () => {
      setLinkActivity(false);
      if (isRelayCoveredTeardown()) {
        logRelayTeardownOnce();
      } else {
        const benign = describeLinkTeardown();
        if (benign) {
          logLine(`data channel closed (${benign})`, 'info');
        } else {
          logLine('data channel closed', 'warn');
        }
      }
      reportLinkDown();
    };

    channel.onerror = (event) => {
      logLine(`data channel error: ${event.error ? event.error.message : 'unknown'}`, 'err');
    };

    channel.onmessage = handleIncomingMessage;
  }

  function teardownPeerConnection() {
    setLinkActivity(false);
    if (dataChannel) {
      dataChannel.onmessage = null;
      dataChannel.onbufferedamountlow = null;
      try { dataChannel.close(); } catch { /* already closed */ }
      dataChannel = null;
    }
    if (pc) {
      pc.onicecandidate = null;
      pc.oniceconnectionstatechange = null;
      pc.onconnectionstatechange = null;
      pc.ondatachannel = null;
      try { pc.close(); } catch { /* already closed */ }
      pc = null;
    }
  }

  /**
   * Starts (restarting any previous one) the countdown to declaring the
   * WS relay good enough, called every time a peer shows up in the room.
   * Does nothing itself beyond flipping the UI/log state — transportSend
   * and transportReady() below don't actually consult this timer; they
   * just check "is the data channel open right now", and fall back to
   * relay on their own whenever it isn't. The timer's only job is making
   * sure the user isn't left watching "Awaiting peer" forever if WebRTC
   * never comes up at all.
   */
  /**
   * Called whenever the WebRTC side of the link goes down (ICE failed/
   * disconnected, peer connection failed/closed, or the data channel
   * itself closed). Mid-transfer, this is the difference between "the
   * transfer dies" and "the next chunk just goes out over the relay
   * instead" — transportSend() already falls back on its own per-call,
   * this just keeps the status badge honest about which is true.
   */
  function reportLinkDown(reasonLog) {
    if (reasonLog) logLine(reasonLog, 'err');
    if (peerPresent && ws && ws.readyState === WebSocket.OPEN) {
      relayAnnounced = true;
      setPeerStatus('connected');
    } else {
      setPeerStatus('lost');
    }
  }

  function armRelayFallback() {
    clearTimeout(relayFallbackTimer);
    relayFallbackTimer = setTimeout(() => {
      if (dataChannel && dataChannel.readyState === 'open') return; // WebRTC made it — nothing to announce
      if (!peerPresent) return; // peer already left before this fired
      relayAnnounced = true;
      setPeerStatus('connected');
    }, RELAY_FALLBACK_MS);
  }

  // =========================================================================
  // SECTION 2b — TRANSPORT (picks WebRTC data channel vs. WS relay per send,
  // so TRANSFER code below never has to know which one actually moved the
  // bytes. Preference order: an open data channel first — it's direct,
  // lower-latency, and free of this server's bandwidth — then the relay.)
  // =========================================================================

  function transportMode() {
    if (dataChannel && dataChannel.readyState === 'open') return 'webrtc';
    if (peerPresent && ws && ws.readyState === WebSocket.OPEN) return 'relay';
    return null;
  }

  function transportReady() {
    return transportMode() !== null;
  }

  /** Raw bytes or a JSON string — same contract as dataChannel.send(). */
  function transportSend(data) {
    const mode = transportMode();
    if (mode === 'webrtc') {
      dataChannel.send(data);
    } else if (mode === 'relay') {
      if (!relayAnnounced) {
        relayAnnounced = true;
        clearTimeout(relayFallbackTimer);
        setPeerStatus('connected');
      }
      ws.send(data);
    } else {
      throw new Error('no active peer link');
    }
  }

  /**
   * Sends a file-transfer control message (meta/end/verify-result) over
   * whichever transport is active. A WebRTC data channel only ever
   * carries this app's own messages, so the control object goes out
   * as-is. The WS relay channel is shared with server.js's own signaling
   * protocol (create/join/signal/...), so a bare {type:'meta',...} sent
   * there would hit server.js's `default` case and get rejected instead
   * of relayed — it has to go out wrapped as {type:'relay', data:{...}},
   * the same opaque-passthrough envelope 'signal' already gets. See the
   * matching unwrap in ws.onmessage above.
   */
  function sendFileControl(obj) {
    const payload = transportMode() === 'relay'
      ? JSON.stringify({ type: 'relay', data: obj })
      : JSON.stringify(obj);
    transportSend(payload);
  }

  function transportBufferedAmount() {
    const mode = transportMode();
    if (mode === 'webrtc') return dataChannel.bufferedAmount;
    if (mode === 'relay') return ws.bufferedAmount;
    return 0;
  }

  /**
   * Calls `cb` once bufferedAmount has drained below BUFFER_LOW_WATER.
   * RTCDataChannel fires a real 'bufferedamountlow' event for this; plain
   * WebSocket has no equivalent in browser JS (its bufferedAmount getter
   * exists, but nothing tells you when it drops), so the relay path just
   * polls — same 50ms cadence sendChunks already used as its own
   * no-event-ever-fires fallback, now the only path relay takes.
   */
  function waitForDrain(cb) {
    const mode = transportMode();
    if (mode === 'webrtc') {
      if (dataChannel.bufferedAmount <= BUFFER_LOW_WATER) {
        setTimeout(cb, 50);
        return;
      }
      dataChannel.onbufferedamountlow = () => {
        dataChannel.onbufferedamountlow = null;
        cb();
      };
      return;
    }
    // relay (or mode dropped out entirely, e.g. peer left mid-transfer —
    // let the retry hit transportSend's "no active peer link" throw)
    setTimeout(cb, 50);
  }

  // =========================================================================
  // SECTION 2c — ZIP (pure-JS ZIP writer, "store" method / no compression,
  // used to bundle multiple dropped/selected files into a single archive
  // before handing them to the existing single-file TRANSFER pipeline
  // below. No external library and no network fetch required — this is
  // just the ZIP file format's local/central headers plus CRC-32.)
  // =========================================================================

  const CRC_TABLE = (() => {
    const table = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) {
        c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
      }
      table[n] = c >>> 0;
    }
    return table;
  })();

  function crc32(bytes) {
    let crc = 0xffffffff;
    for (let i = 0; i < bytes.length; i++) {
      crc = CRC_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
    }
    return (crc ^ 0xffffffff) >>> 0;
  }

  function u16(n) { return new Uint8Array([n & 0xff, (n >>> 8) & 0xff]); }
  function u32(n) {
    return new Uint8Array([n & 0xff, (n >>> 8) & 0xff, (n >>> 16) & 0xff, (n >>> 24) & 0xff]);
  }

  function concatBytes(parts) {
    let len = 0;
    for (const p of parts) len += p.length;
    const out = new Uint8Array(len);
    let offset = 0;
    for (const p of parts) { out.set(p, offset); offset += p.length; }
    return out;
  }

  // DOS date/time fields the ZIP format stores instead of a real timestamp.
  function dosDateTime(date) {
    const time = ((date.getHours() & 0x1f) << 11)
      | ((date.getMinutes() & 0x3f) << 5)
      | ((date.getSeconds() >> 1) & 0x1f);
    const dosDate = (((Math.max(0, date.getFullYear() - 1980)) & 0x7f) << 9)
      | (((date.getMonth() + 1) & 0xf) << 5)
      | (date.getDate() & 0x1f);
    return { time, dosDate };
  }

  /**
   * Builds a single ZIP archive (as a Blob) from a list of File objects.
   * Uses the "store" method (no compression) so it's fast and simple —
   * fine for a tool whose whole job is moving bytes to a peer, where the
   * link itself is the bottleneck, not archive size.
   */
  async function buildZip(files, onFile) {
    const encoder = new TextEncoder();
    const usedNames = new Set();

    function uniqueEntryName(rawName, index) {
      let base = (rawName || `file-${index + 1}`).replace(/\\/g, '/').split('/').pop();
      if (!base) base = `file-${index + 1}`;
      let candidate = base;
      let n = 1;
      while (usedNames.has(candidate)) {
        const dot = base.lastIndexOf('.');
        candidate = dot > 0 ? `${base.slice(0, dot)} (${n})${base.slice(dot)}` : `${base} (${n})`;
        n += 1;
      }
      usedNames.add(candidate);
      return candidate;
    }

    const parts = [];
    const centralRecords = [];
    let offset = 0;
    const push = (bytes) => { parts.push(bytes); offset += bytes.length; };

    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      if (onFile) onFile(i, files.length, file.name);

      const nameBytes = encoder.encode(uniqueEntryName(file.name, i));
      const data = new Uint8Array(await file.arrayBuffer());
      const crc = crc32(data);
      const { time, dosDate } = dosDateTime(new Date(file.lastModified || Date.now()));
      const localOffset = offset;

      push(concatBytes([
        u32(0x04034b50), u16(20), u16(0), u16(0),
        u16(time), u16(dosDate), u32(crc),
        u32(data.length), u32(data.length),
        u16(nameBytes.length), u16(0),
      ]));
      push(nameBytes);
      push(data);

      centralRecords.push(concatBytes([
        u32(0x02014b50), u16(20), u16(20), u16(0), u16(0),
        u16(time), u16(dosDate), u32(crc),
        u32(data.length), u32(data.length),
        u16(nameBytes.length), u16(0), u16(0), u16(0), u16(0), u32(0),
        u32(localOffset),
        nameBytes,
      ]));
    }

    const centralOffset = offset;
    for (const rec of centralRecords) push(rec);
    const centralSize = offset - centralOffset;

    push(concatBytes([
      u32(0x06054b50),
      u16(0), u16(0),
      u16(centralRecords.length), u16(centralRecords.length),
      u32(centralSize),
      u32(centralOffset),
      u16(0),
    ]));

    return new Blob(parts, { type: 'application/zip' });
  }

  function zipTimestampTag() {
    const d = new Date();
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}-${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
  }

  /**
   * Entry point for both the file picker and drag-and-drop: a single file
   * goes straight to sendFile() unchanged; two or more are zipped into one
   * archive first, so the peer gets one transfer with one checksum instead
   * of a pile of concurrent/sequential sends.
   */
  async function queueFiles(fileList) {
    const files = Array.from(fileList || []);
    if (files.length === 0) return;

    if (files.length === 1) {
      return sendFile(files[0]);
    }

    if (!transportReady()) {
      logLine('no active peer link, create or join a room first', 'err');
      return;
    }

    logLine(`zipping ${files.length} files into one archive...`, 'info');
    let zipBlob;
    try {
      zipBlob = await buildZip(files, (index, total, name) => {
        logLine(`adding to archive: ${name} (${index + 1}/${total})`, 'info');
      });
    } catch (err) {
      logLine(`failed to build archive: ${err.message}`, 'err');
      return;
    }

    const zipName = `files-${zipTimestampTag()}.zip`;
    const zipFile = new File([zipBlob], zipName, { type: 'application/zip', lastModified: Date.now() });
    logLine(`archive ready: ${zipName} (${formatBytes(zipFile.size)})`, 'ok');
    return sendFile(zipFile, files.length);
  }

  // =========================================================================
  // SECTION 2d — QR (pure-JS QR Code encoder, Byte mode, EC level L,
  // versions 1-10 — implements ISO/IEC 18004 directly: GF(256)
  // Reed-Solomon, BCH format/version info computed algorithmically rather
  // than copied from a table, standard module placement and mask scoring.
  // No external library, no CDN — same "no dependency" approach as the
  // ZIP builder above. Used to render the room's shareable join link as a
  // scannable code next to the room code.
  // =========================================================================

  const QR_TABLE_L = {
    1:  [19,  7, 1, 19, 0, 0],
    2:  [34, 10, 1, 34, 0, 0],
    3:  [55, 15, 1, 55, 0, 0],
    4:  [80, 20, 1, 80, 0, 0],
    5:  [108,26, 1,108, 0, 0],
    6:  [136,18, 2, 68, 0, 0],
    7:  [156,20, 2, 78, 0, 0],
    8:  [194,24, 2, 97, 0, 0],
    9:  [232,30, 2,116, 0, 0],
    10: [274,18, 2, 68, 2, 69],
  };

  const QR_ALIGNMENT_COORDS = {
    2: [6, 18], 3: [6, 22], 4: [6, 26], 5: [6, 30], 6: [6, 34],
    7: [6, 22, 38], 8: [6, 24, 42], 9: [6, 26, 46], 10: [6, 28, 50],
  };

  function qrModuleCount(version) { return 17 + 4 * version; }

  const QR_GF_EXP = new Uint8Array(512);
  const QR_GF_LOG = new Uint8Array(256);
  (function initQrGF() {
    let x = 1;
    for (let i = 0; i < 255; i++) {
      QR_GF_EXP[i] = x;
      QR_GF_LOG[x] = i;
      x <<= 1;
      if (x & 0x100) x ^= 0x11d;
    }
    for (let i = 255; i < 512; i++) QR_GF_EXP[i] = QR_GF_EXP[i - 255];
  })();

  function qrGfMul(a, b) {
    if (a === 0 || b === 0) return 0;
    return QR_GF_EXP[QR_GF_LOG[a] + QR_GF_LOG[b]];
  }

  function qrRsGeneratorPoly(degree) {
    // Big-endian coefficients: index 0 is the leading (always-1) term,
    // built by repeatedly multiplying by (x + alpha^i) in GF(2).
    let poly = [1];
    for (let i = 0; i < degree; i++) {
      const next = new Array(poly.length + 1).fill(0);
      for (let j = 0; j < poly.length; j++) {
        next[j] ^= poly[j];
        next[j + 1] ^= qrGfMul(poly[j], QR_GF_EXP[i]);
      }
      poly = next;
    }
    return poly;
  }

  function qrRsEncode(dataBytes, ecLen) {
    const gen = qrRsGeneratorPoly(ecLen);
    const buf = Array.from(dataBytes).concat(new Array(ecLen).fill(0));
    for (let i = 0; i < dataBytes.length; i++) {
      const coef = buf[i];
      if (coef === 0) continue;
      for (let j = 0; j < gen.length; j++) buf[i + j] ^= qrGfMul(gen[j], coef);
    }
    return buf.slice(dataBytes.length);
  }

  function qrBchDigitCount(n) {
    let d = 0;
    while (n !== 0) { d++; n >>>= 1; }
    return d;
  }

  function qrBchEncode(data, generator) {
    const genDigits = qrBchDigitCount(generator);
    let d = data << (genDigits - 1);
    while (qrBchDigitCount(d) >= genDigits) {
      d ^= generator << (qrBchDigitCount(d) - genDigits);
    }
    return d;
  }

  function qrFormatInfoBits(eccBits, maskPattern) {
    const data = (eccBits << 3) | maskPattern;
    const rem = qrBchEncode(data, 0b10100110111); // x^10+x^8+x^5+x^4+x^2+x+1
    return ((data << 10) | rem) ^ 0b101010000010010;
  }

  function qrVersionInfoBits(version) {
    const rem = qrBchEncode(version, 0b1111100100101);
    return (version << 12) | rem;
  }

  function qrCharCountBits(version) { return version <= 9 ? 8 : 16; }

  function qrPickVersion(byteLen) {
    for (let v = 1; v <= 10; v++) {
      const totalDataCodewords = QR_TABLE_L[v][0];
      const headerBits = 4 + qrCharCountBits(v);
      const capacityBits = totalDataCodewords * 8;
      const neededBits = headerBits + byteLen * 8 + 4; // +4 upper-bound terminator
      if (neededBits <= capacityBits) return v;
    }
    return null; // link too long for this encoder (max ~213 bytes at version 10-L)
  }

  function qrBuildDataCodewords(bytes, version) {
    const totalDataCodewords = QR_TABLE_L[version][0];
    const bits = [];
    const put = (value, length) => { for (let i = length - 1; i >= 0; i--) bits.push((value >>> i) & 1); };
    put(0b0100, 4); // byte mode
    put(bytes.length, qrCharCountBits(version));
    for (const b of bytes) put(b, 8);

    const capacityBits = totalDataCodewords * 8;
    const term = Math.min(4, capacityBits - bits.length);
    if (term > 0) put(0, term);
    while (bits.length % 8 !== 0) bits.push(0);

    const codewords = [];
    for (let i = 0; i < bits.length; i += 8) {
      let byte = 0;
      for (let j = 0; j < 8; j++) byte = (byte << 1) | bits[i + j];
      codewords.push(byte);
    }
    const PAD_BYTES = [0xec, 0x11];
    let p = 0;
    while (codewords.length < totalDataCodewords) codewords.push(PAD_BYTES[p++ % 2]);
    return codewords;
  }

  function qrSplitBlocksAndInterleave(dataCodewords, version) {
    const [, ecLen, g1Count, g1Len, g2Count, g2Len] = QR_TABLE_L[version];
    const blocks = [];
    let offset = 0;
    for (let i = 0; i < g1Count; i++) { blocks.push(dataCodewords.slice(offset, offset + g1Len)); offset += g1Len; }
    for (let i = 0; i < g2Count; i++) { blocks.push(dataCodewords.slice(offset, offset + g2Len)); offset += g2Len; }
    const ecBlocks = blocks.map((blk) => qrRsEncode(blk, ecLen));

    const maxDataLen = Math.max(...blocks.map((b) => b.length));
    const finalData = [];
    for (let i = 0; i < maxDataLen; i++) for (const blk of blocks) if (i < blk.length) finalData.push(blk[i]);
    const finalEc = [];
    for (let i = 0; i < ecLen; i++) for (const ec of ecBlocks) finalEc.push(ec[i]);
    return finalData.concat(finalEc);
  }

  function qrMakeMatrix(version) {
    const n = qrModuleCount(version);
    const matrix = [];
    const reserved = [];
    for (let i = 0; i < n; i++) { matrix.push(new Array(n).fill(0)); reserved.push(new Array(n).fill(false)); }
    return { n, matrix, reserved };
  }

  function qrSetModule(ctx, r, c, val) {
    ctx.matrix[r][c] = val ? 1 : 0;
    ctx.reserved[r][c] = true;
  }

  function qrPlaceFinder(ctx, r0, c0) {
    for (let dr = -1; dr <= 7; dr++) {
      for (let dc = -1; dc <= 7; dc++) {
        const r = r0 + dr, c = c0 + dc;
        if (r < 0 || c < 0 || r >= ctx.n || c >= ctx.n) continue;
        const isBorder = dr === -1 || dr === 7 || dc === -1 || dc === 7;
        const isOuterRing = dr >= 0 && dr <= 6 && dc >= 0 && dc <= 6 && (dr === 0 || dr === 6 || dc === 0 || dc === 6);
        const isCore = dr >= 2 && dr <= 4 && dc >= 2 && dc <= 4;
        qrSetModule(ctx, r, c, !isBorder && (isOuterRing || isCore));
      }
    }
  }

  function qrPlaceAlignmentPatterns(ctx, version) {
    const coords = QR_ALIGNMENT_COORDS[version];
    if (!coords) return;
    const n = ctx.n;
    for (const r of coords) {
      for (const c of coords) {
        if ((r <= 8 && c <= 8) || (r <= 8 && c >= n - 9) || (r >= n - 9 && c <= 8)) continue;
        for (let dr = -2; dr <= 2; dr++) {
          for (let dc = -2; dc <= 2; dc++) {
            const isRing = Math.max(Math.abs(dr), Math.abs(dc)) === 2;
            qrSetModule(ctx, r + dr, c + dc, isRing || (dr === 0 && dc === 0));
          }
        }
      }
    }
  }

  function qrPlaceTimingPatterns(ctx) {
    const n = ctx.n;
    for (let i = 8; i < n - 8; i++) {
      if (!ctx.reserved[6][i]) qrSetModule(ctx, 6, i, i % 2 === 0);
      if (!ctx.reserved[i][6]) qrSetModule(ctx, i, 6, i % 2 === 0);
    }
  }

  function qrReserveFormatAreas(ctx) {
    const n = ctx.n;
    for (let i = 0; i <= 8; i++) { ctx.reserved[8][i] = true; ctx.reserved[i][8] = true; }
    for (let i = 0; i < 8; i++) { ctx.reserved[8][n - 1 - i] = true; ctx.reserved[n - 1 - i][8] = true; }
    qrSetModule(ctx, n - 8, 8, true); // the always-dark module
  }

  function qrReserveVersionAreas(ctx, version) {
    if (version < 7) return;
    const n = ctx.n;
    for (let i = 0; i < 6; i++) {
      for (let j = 0; j < 3; j++) {
        ctx.reserved[i][n - 11 + j] = true;
        ctx.reserved[n - 11 + j][i] = true;
      }
    }
  }

  function qrPlaceFormatInfo(ctx, bits) {
    const n = ctx.n;
    const bitAt = (i) => (bits >>> i) & 1;
    for (let i = 0; i <= 5; i++) qrSetModule(ctx, i, 8, bitAt(i));
    qrSetModule(ctx, 7, 8, bitAt(6));
    qrSetModule(ctx, 8, 8, bitAt(7));
    qrSetModule(ctx, 8, 7, bitAt(8));
    for (let i = 9; i <= 14; i++) qrSetModule(ctx, 8, 14 - i, bitAt(i));
    for (let i = 0; i <= 7; i++) qrSetModule(ctx, 8, n - 1 - i, bitAt(i));
    for (let i = 8; i <= 14; i++) qrSetModule(ctx, n - 15 + i, 8, bitAt(i));
  }

  function qrPlaceVersionInfo(ctx, version) {
    if (version < 7) return;
    const bits = qrVersionInfoBits(version);
    const n = ctx.n;
    const bitAt = (i) => (bits >>> i) & 1;
    for (let i = 0; i < 18; i++) {
      const row = Math.floor(i / 3), col = i % 3;
      qrSetModule(ctx, n - 11 + col, row, bitAt(i));
      qrSetModule(ctx, row, n - 11 + col, bitAt(i));
    }
  }

  function qrMaskFn(pattern, r, c) {
    switch (pattern) {
      case 0: return (r + c) % 2 === 0;
      case 1: return r % 2 === 0;
      case 2: return c % 3 === 0;
      case 3: return (r + c) % 3 === 0;
      case 4: return (Math.floor(r / 2) + Math.floor(c / 3)) % 2 === 0;
      case 5: return (r * c) % 2 + (r * c) % 3 === 0;
      case 6: return ((r * c) % 2 + (r * c) % 3) % 2 === 0;
      case 7: return ((r + c) % 2 + (r * c) % 3) % 2 === 0;
      default: return false;
    }
  }

  function qrPlaceDataBits(ctx, allCodewords) {
    const n = ctx.n;
    const bits = [];
    for (const byte of allCodewords) for (let i = 7; i >= 0; i--) bits.push((byte >>> i) & 1);
    let bitIndex = 0;
    let col = n - 1;
    while (col >= 1) {
      if (col === 6) col = 5; // column 6 is entirely the vertical timing pattern
      const upward = ((col + 1) & 2) === 0;
      for (let i = 0; i < n; i++) {
        const row = upward ? n - 1 - i : i;
        for (const c of [col, col - 1]) {
          if (ctx.reserved[row][c]) continue;
          ctx.matrix[row][c] = bitIndex < bits.length ? bits[bitIndex] : 0;
          bitIndex++;
        }
      }
      col -= 2;
    }
  }

  function qrCollectDataMask(ctx) {
    const n = ctx.n;
    const mask = [];
    for (let r = 0; r < n; r++) {
      mask.push(new Array(n).fill(false));
      for (let c = 0; c < n; c++) mask[r][c] = !ctx.reserved[r][c];
    }
    return mask;
  }

  function qrApplyMask(ctx, dataMask, pattern) {
    const n = ctx.n;
    const out = ctx.matrix.map((row) => row.slice());
    for (let r = 0; r < n; r++) {
      for (let c = 0; c < n; c++) {
        if (dataMask[r][c] && qrMaskFn(pattern, r, c)) out[r][c] ^= 1;
      }
    }
    return out;
  }

  function qrPenaltyScore(m) {
    const n = m.length;
    let score = 0;
    for (let r = 0; r < n; r++) {
      let runColor = m[r][0], runLen = 1;
      for (let c = 1; c < n; c++) {
        if (m[r][c] === runColor) { runLen++; } else { if (runLen >= 5) score += 3 + (runLen - 5); runColor = m[r][c]; runLen = 1; }
      }
      if (runLen >= 5) score += 3 + (runLen - 5);
    }
    for (let c = 0; c < n; c++) {
      let runColor = m[0][c], runLen = 1;
      for (let r = 1; r < n; r++) {
        if (m[r][c] === runColor) { runLen++; } else { if (runLen >= 5) score += 3 + (runLen - 5); runColor = m[r][c]; runLen = 1; }
      }
      if (runLen >= 5) score += 3 + (runLen - 5);
    }
    for (let r = 0; r < n - 1; r++) {
      for (let c = 0; c < n - 1; c++) {
        const v = m[r][c];
        if (v === m[r][c + 1] && v === m[r + 1][c] && v === m[r + 1][c + 1]) score += 3;
      }
    }
    const pattern1 = [1, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0];
    const pattern2 = [0, 0, 0, 0, 1, 0, 1, 1, 1, 0, 1];
    const matchesAt = (arr, idx, pat) => {
      for (let i = 0; i < pat.length; i++) if (arr[idx + i] !== pat[i]) return false;
      return true;
    };
    for (let r = 0; r < n; r++) {
      const row = m[r];
      for (let c = 0; c <= n - 11; c++) {
        if (matchesAt(row, c, pattern1) || matchesAt(row, c, pattern2)) score += 40;
      }
    }
    for (let c = 0; c < n; c++) {
      const col = m.map((row) => row[c]);
      for (let r = 0; r <= n - 11; r++) {
        if (matchesAt(col, r, pattern1) || matchesAt(col, r, pattern2)) score += 40;
      }
    }
    let dark = 0;
    for (let r = 0; r < n; r++) for (let c = 0; c < n; c++) if (m[r][c]) dark++;
    const percent = (dark * 100) / (n * n);
    const prevMultiple = Math.floor(percent / 5) * 5;
    score += Math.min(Math.abs(prevMultiple - 50) / 5, Math.abs(prevMultiple + 5 - 50) / 5) * 10;
    return score;
  }

  /** Encodes `text` (Byte mode, EC level L) into a { version, size, matrix } QR code. */
  function encodeQR(text) {
    const bytes = Array.from(new TextEncoder().encode(text));
    const version = qrPickVersion(bytes.length);
    if (!version) throw new Error('link too long to encode as a QR code');

    const dataCodewords = qrBuildDataCodewords(bytes, version);
    const allCodewords = qrSplitBlocksAndInterleave(dataCodewords, version);

    const ctx = qrMakeMatrix(version);
    qrPlaceFinder(ctx, 0, 0);
    qrPlaceFinder(ctx, 0, ctx.n - 7);
    qrPlaceFinder(ctx, ctx.n - 7, 0);
    qrPlaceAlignmentPatterns(ctx, version);
    qrPlaceTimingPatterns(ctx);
    qrReserveFormatAreas(ctx);
    qrReserveVersionAreas(ctx, version);
    qrPlaceVersionInfo(ctx, version);

    const dataMask = qrCollectDataMask(ctx);
    qrPlaceDataBits(ctx, allCodewords);

    let best = null, bestScore = Infinity, bestPattern = 0;
    for (let p = 0; p < 8; p++) {
      const masked = qrApplyMask(ctx, dataMask, p);
      const s = qrPenaltyScore(masked);
      if (s < bestScore) { bestScore = s; best = masked; bestPattern = p; }
    }

    const finalCtx = { n: ctx.n, matrix: best, reserved: ctx.reserved };
    qrPlaceFormatInfo(finalCtx, qrFormatInfoBits(0b01, bestPattern));
    qrPlaceVersionInfo(finalCtx, version); // version info isn't masked — re-stamp after copying
    return { version, size: ctx.n, matrix: best };
  }

  /** Renders `text` as a QR code onto el.qrCanvas, with a standard 4-module quiet zone. */
  function renderJoinQr(text) {
    let result;
    try {
      result = encodeQR(text);
    } catch (err) {
      logLine(`could not generate QR code: ${err.message}`, 'warn');
      return;
    }
    const { size, matrix } = result;
    const quiet = 4;
    const canvas = el.qrCanvas;
    const px = canvas.width; // square canvas, CSS scales it to fit the panel
    const moduleSize = Math.max(1, Math.floor(px / (size + quiet * 2)));
    const totalSize = moduleSize * (size + quiet * 2);
    const offset = Math.floor((px - totalSize) / 2);

    const ctx2d = canvas.getContext('2d');
    ctx2d.clearRect(0, 0, px, px);
    ctx2d.fillStyle = '#ffffff';
    ctx2d.fillRect(0, 0, px, px);
    ctx2d.fillStyle = '#11151c';
    for (let r = 0; r < size; r++) {
      for (let c = 0; c < size; c++) {
        if (!matrix[r][c]) continue;
        ctx2d.fillRect(
          offset + (c + quiet) * moduleSize,
          offset + (r + quiet) * moduleSize,
          moduleSize,
          moduleSize
        );
      }
    }
  }

  // =========================================================================
  // SECTION 3 — TRANSFER (chunking, backpressure, hashing, download)
  // =========================================================================

  async function sendFile(file, fileCount = 1) {
    if (!transportReady()) {
      logLine('no active peer link, create or join a room first', 'err');
      return;
    }

    const mime = file.type || 'application/octet-stream';
    logLine(`reading ${file.name} into memory (${formatBytes(file.size)})...`, 'info');
    resetTransferUi(file.name, file.size, 'Sending', mime, fileCount);

    let fileBuffer;
    try {
      fileBuffer = await file.arrayBuffer();
    } catch (err) {
      logLine(`failed to read file: ${err.message}`, 'err');
      return;
    }

    // Announce the incoming file so the receiver can prep its buffer.
    sendFileControl({
      type: 'meta',
      name: file.name,
      size: file.size,
      mime,
      count: fileCount,
    });

    logLine('hashing (sha-256)...', 'info');
    const hashBuffer = await crypto.subtle.digest('SHA-256', fileBuffer);
    const hashHex = bufferToHex(hashBuffer);
    el.hashLocal.textContent = hashHex;
    logLine(`local sha-256: ${hashHex}`, 'info');

    logLine(`transmitting ${Math.ceil(file.size / CHUNK_SIZE)} chunks...`, 'info');
    const startTime = performance.now();

    try {
      await sendChunks(fileBuffer, startTime);
    } catch (err) {
      logLine(`transfer failed: ${err.message}`, 'err');
      setLinkActivity(false);
      return;
    }

    sendFileControl({ type: 'end', hash: hashHex });
    updateTransferMetrics(file.size, file.size, startTime, true);
    setLinkActivity(false);
    markTransferDone();
    logLine(`transfer complete: ${file.name} sent`, 'ok');
  }

  /**
   * Sends `buffer` over dataChannel in CHUNK_SIZE pieces.
   *
   * Two separate conditions hand control back to the browser:
   *  - backpressure: bufferedAmount above the high-water mark, resumed by
   *    the 'bufferedamountlow' event (keeps memory flat on large files).
   *  - a fixed chunk cadence (YIELD_EVERY_N_CHUNKS), resumed on the next
   *    animation frame (keeps the tab responsive and the progress bar
   *    actually painting, independent of how fast bufferedAmount happens
   *    to report itself).
   */
  function sendChunks(buffer, startTime) {
    return new Promise((resolve, reject) => {
      const total = buffer.byteLength;
      let offset = 0;
      let chunksSinceYield = 0;

      function pauseForDrain() {
        // waitForDrain already covers the "bufferedAmount is already low
        // but the event will never fire" case for WebRTC, and always
        // polls for relay — see its doc comment in SECTION 2b.
        waitForDrain(pump);
      }

      function pump() {
        try {
          while (offset < total) {
            if (transportBufferedAmount() > BUFFER_HIGH_WATER) {
              pauseForDrain();
              return;
            }

            const end = Math.min(offset + CHUNK_SIZE, total);
            try {
              transportSend(buffer.slice(offset, end));
            } catch (sendErr) {
              // The browser's own internal send queue (independent of our
              // BUFFER_HIGH_WATER check, and not always reflected by
              // bufferedAmount in time) can still overflow under this
              // chunk, e.g. "RTCDataChannel send queue is full" on some
              // mobile builds. Don't abort the whole transfer for a single
              // transient overflow — wait for the channel to drain and
              // retry this exact chunk.
              if (/queue is full/i.test(sendErr.message || '')) {
                pauseForDrain();
                return;
              }
              throw sendErr;
            }
            offset = end;
            chunksSinceYield += 1;
            updateTransferMetrics(offset, total, startTime);

            if (chunksSinceYield >= YIELD_EVERY_N_CHUNKS && offset < total) {
              chunksSinceYield = 0;
              updateTransferMetrics(offset, total, startTime, true); // paint before yielding
              requestAnimationFrame(pump);
              return;
            }
          }
          resolve();
        } catch (err) {
          reject(err);
        }
      }

      pump();
    });
  }

  function handleIncomingMessage(event) {
    if (typeof event.data === 'string') {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return logLine('received malformed control message', 'err');
      }

      if (msg.type === 'meta') {
        inbound = {
          name: msg.name,
          size: msg.size,
          mime: msg.mime,
          chunks: [],
          received: 0,
          startTime: performance.now(),
        };
        resetTransferUi(msg.name, msg.size, 'Receiving', msg.mime, msg.count || 1);
        logLine(`incoming: ${msg.name} (${formatBytes(msg.size)})`, 'info');
      } else if (msg.type === 'end') {
        finalizeInbound(msg.hash);
      } else if (msg.type === 'verify-result') {
        // Sent back by the receiver once it has hashed what it got, so the
        // sender's own UI (which otherwise has no way to know) can show the
        // same verified/mismatch result and remote hash.
        el.hashRemote.textContent = msg.hash;
        el.checksum.textContent = msg.match ? 'verified' : 'mismatch';
        el.checksum.className = 'metric-value ' + (msg.match ? 'ok' : 'err');
        logLine(
          msg.match ? 'peer confirmed checksum match' : 'PEER REPORTED CHECKSUM MISMATCH',
          msg.match ? 'ok' : 'err'
        );
      }
      return;
    }

    // Binary chunk
    if (!inbound) {
      logLine('chunk received with no active transfer, ignoring', 'warn');
      return;
    }
    inbound.chunks.push(event.data);
    inbound.received += event.data.byteLength;
    updateTransferMetrics(inbound.received, inbound.size, inbound.startTime);
  }

  async function finalizeInbound(remoteHash) {
    if (!inbound) return;
    const { name, mime, size, chunks, startTime } = inbound;

    updateTransferMetrics(size, size, startTime, true);
    el.hashRemote.textContent = remoteHash;
    logLine('verifying integrity (sha-256)...', 'info');

    const blob = new Blob(chunks, { type: mime });
    const localHashBuffer = await crypto.subtle.digest('SHA-256', await blob.arrayBuffer());
    const localHash = bufferToHex(localHashBuffer);
    el.hashLocal.textContent = localHash;

    const match = localHash === remoteHash;
    el.checksum.textContent = match ? 'verified' : 'mismatch';
    el.checksum.className = 'metric-value ' + (match ? 'ok' : 'err');
    logLine(
      match ? `checksum verified: ${name} is intact` : `CHECKSUM MISMATCH: ${name} may be corrupted`,
      match ? 'ok' : 'err'
    );

    // Let the sender know too — it has no visibility into what we
    // actually received or computed otherwise, and its own checksum/remote
    // hash fields would sit on "pending" forever without this.
    if (transportReady()) {
      sendFileControl({ type: 'verify-result', hash: localHash, match });
    }

    setLinkActivity(false);
    markTransferDone();
    triggerDownload(blob, name);
    inbound = null;
  }

  function triggerDownload(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 10000);
    logLine(`download triggered: ${filename}`, 'ok');
  }

  // =========================================================================
  // TABS — "Docs" and "Transfer" sections, plus the mobile hamburger menu
  // =========================================================================

  const TABS = {
    docs: { tabBtn: el.tabBtnDocs, panel: el.panelDocs },
    app: { tabBtn: el.tabBtnApp, panel: el.panelApp },
    ai: { tabBtn: el.tabBtnAi, panel: el.panelAi },
    speed: { tabBtn: el.tabBtnSpeed, panel: el.panelSpeed },
  };

  function activateTab(name) {
    if (!TABS[name]) return;

    Object.entries(TABS).forEach(([key, { tabBtn, panel }]) => {
      const isActive = key === name;
      tabBtn.setAttribute('aria-selected', String(isActive));
      tabBtn.tabIndex = isActive ? 0 : -1;
      panel.hidden = !isActive;
    });

    el.mobileMenu.querySelectorAll('.mobile-menu-item').forEach((item) => {
      item.setAttribute('aria-current', String(item.dataset.tab === name));
    });

    closeMobileMenu();

    const hash = `#${name}`;
    if (location.hash !== hash) history.replaceState(null, '', hash);
  }

  function openMobileMenu() {
    el.mobileMenu.hidden = false;
    el.btnMenu.setAttribute('aria-expanded', 'true');
  }

  function closeMobileMenu() {
    el.mobileMenu.hidden = true;
    el.btnMenu.setAttribute('aria-expanded', 'false');
  }

  el.tabBtnDocs.addEventListener('click', () => activateTab('docs'));
  el.tabBtnApp.addEventListener('click', () => activateTab('app'));
  el.tabBtnAi.addEventListener('click', () => activateTab('ai'));
  el.tabBtnSpeed.addEventListener('click', () => activateTab('speed'));

  el.btnMenu.addEventListener('click', () => {
    if (el.mobileMenu.hidden) openMobileMenu(); else closeMobileMenu();
  });

  el.mobileMenu.addEventListener('click', (e) => {
    const item = e.target.closest('.mobile-menu-item');
    if (item) activateTab(item.dataset.tab);
  });

  document.querySelectorAll('[data-tab]').forEach((node) => {
    if (node.classList.contains('tab-link') || node.classList.contains('mobile-menu-item')) return;
    node.addEventListener('click', () => activateTab(node.dataset.tab));
  });

  document.addEventListener('click', (e) => {
    if (!el.mobileMenu.hidden && !el.mobileMenu.contains(e.target) && e.target !== el.btnMenu && !el.btnMenu.contains(e.target)) {
      closeMobileMenu();
    }
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !el.mobileMenu.hidden) closeMobileMenu();
  });

  // Docs is the default landing tab; #app / #transfer and #ai deep-link
  // directly. A #join=XXXXXX shareable link also lands on "app", since
  // that's where the auto-join happens once signaling connects.
  activateTab(
    location.hash === '#app' || location.hash === '#transfer' || JOIN_LINK_RE.test(location.hash) ? 'app'
      : location.hash === '#ai' ? 'ai'
      : location.hash === '#speed' ? 'speed'
      : 'docs'
  );

  // =========================================================================
  // UI EVENT WIRING
  // =========================================================================

  el.btnCreate.addEventListener('click', () => {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return logLine('not connected to signaling server yet', 'err');
    }
    sendSignalingMessage({ type: 'create' });
  });

  el.btnJoin.addEventListener('click', () => {
    const code = el.inputJoinCode.value.trim().toUpperCase();
    if (code.length !== 6) {
      return logLine('enter a valid 6-character room code', 'err');
    }
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return logLine('not connected to signaling server yet', 'err');
    }
    sendSignalingMessage({ type: 'join', roomId: code });
  });

  el.inputJoinCode.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') el.btnJoin.click();
  });

  el.inputJoinCode.addEventListener('input', () => {
    el.inputJoinCode.value = el.inputJoinCode.value.toUpperCase().slice(0, 6);
  });

  el.btnCopy.addEventListener('click', async () => {
    const ok = await copyToClipboard(roomId, 'room code copied to clipboard');
    if (ok) flashCopied(el.btnCopy);
  });

  el.btnCopyLink.addEventListener('click', async () => {
    if (!roomId) return;
    const ok = await copyToClipboard(buildJoinLink(roomId), 'join link copied to clipboard');
    if (ok) flashCopied(el.btnCopyLink);
  });

  el.btnQrToggle.addEventListener('click', () => {
    const showing = !el.qrPanel.classList.contains('hidden');
    el.qrPanel.classList.toggle('hidden', showing);
    el.btnQrToggle.setAttribute('aria-expanded', String(!showing));
    if (!showing && roomId) renderJoinQr(buildJoinLink(roomId));
  });

  el.btnCopyLocalHash.addEventListener('click', async () => {
    const ok = await copyToClipboard(el.hashLocal.textContent, 'local sha-256 copied to clipboard');
    if (ok) flashCopied(el.btnCopyLocalHash);
  });

  el.btnCopyRemoteHash.addEventListener('click', async () => {
    const ok = await copyToClipboard(el.hashRemote.textContent, 'remote sha-256 copied to clipboard');
    if (ok) flashCopied(el.btnCopyRemoteHash);
  });

  el.btnClearLog.addEventListener('click', () => {
    el.log.innerHTML = '';
    logLine('log cleared', 'info');
  });

  el.dropzone.addEventListener('click', () => el.fileInput.click());

  el.dropzone.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      el.fileInput.click();
    }
  });

  el.fileInput.addEventListener('change', () => {
    if (el.fileInput.files.length) queueFiles(el.fileInput.files);
    el.fileInput.value = '';
  });

  ['dragenter', 'dragover'].forEach((evt) =>
    el.dropzone.addEventListener(evt, (e) => {
      e.preventDefault();
      el.dropzone.classList.add('dragover');
    })
  );

  ['dragleave', 'drop'].forEach((evt) =>
    el.dropzone.addEventListener(evt, (e) => {
      e.preventDefault();
      el.dropzone.classList.remove('dragover');
    })
  );

  el.dropzone.addEventListener('drop', (e) => {
    if (e.dataTransfer.files && e.dataTransfer.files.length) queueFiles(e.dataTransfer.files);
  });

  // =========================================================================
  // P2P.AI — assistant chat tab
  // =========================================================================

  let aiHistory = [];   // [{ role: 'user'|'assistant', content }]
  let aiBusy = false;

  // Defense in depth: api/chat.js already strips Markdown-style artifacts
  // from the model's reply before it ever reaches here, but any text that
  // ends up in the chat (including the hardcoded fallback lines below)
  // goes through this too, so a stray code fence, blockquote marker, or
  // emphasis marker never shows up as literal punctuation in the UI.
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

  function appendAiMessage(role, text) {
    const wrap = document.createElement('div');
    wrap.className = `ai-msg ai-msg-${role}`;
    wrap.innerHTML = `
      <span class="ai-msg-tag">${role === 'user' ? 'you' : 'p2p.ai'}</span>
      <p class="ai-msg-text"></p>
    `;
    wrap.querySelector('.ai-msg-text').textContent = role === 'user' ? text : stripFormatting(text);
    el.aiChat.appendChild(wrap);
    el.aiChat.scrollTop = el.aiChat.scrollHeight;
    return wrap;
  }

  function setAiBusy(isBusy) {
    aiBusy = isBusy;
    el.aiInput.disabled = isBusy;
    el.aiSend.disabled = isBusy;
    el.aiTyping.classList.toggle('hidden', !isBusy);
    if (isBusy) el.aiChat.scrollTop = el.aiChat.scrollHeight;
  }

  async function askAi(question) {
    const text = question.trim();
    if (!text || aiBusy) return;

    appendAiMessage('user', text);
    aiHistory.push({ role: 'user', content: text });
    aiHistory = aiHistory.slice(-AI_HISTORY_LIMIT);
    setAiBusy(true);

    try {
      const res = await fetch(AI_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: text, history: aiHistory.slice(0, -1) }),
      });

      if (!res.ok) throw new Error(`status ${res.status}`);
      const data = await res.json();
      const reply = (data && data.reply ? String(data.reply) : '').trim();

      if (!reply) throw new Error('empty reply');

      appendAiMessage('bot', reply);
      aiHistory.push({ role: 'assistant', content: reply });
      aiHistory = aiHistory.slice(-AI_HISTORY_LIMIT);
    } catch (err) {
      appendAiMessage('err', 'assistant unreachable. check your connection and try again.');
    } finally {
      setAiBusy(false);
      el.aiInput.focus();
    }
  }

  el.aiForm.addEventListener('submit', (e) => {
    e.preventDefault();
    const text = el.aiInput.value;
    el.aiInput.value = '';
    askAi(text);
  });

  el.aiSuggestList.addEventListener('click', (e) => {
    const btn = e.target.closest('.ai-suggest');
    if (!btn || aiBusy) return;
    askAi(btn.dataset.q);
  });

  // =========================================================================
  // SECTION 5 — SPEED TEST
  // Client-server HTTPS throughput check against api/speed.js. Separate
  // from the WebRTC link entirely — see the note in the Speed test tab.
  // =========================================================================

  const SPEED_ENDPOINT = '/api/speed';
  const SPEED_PING_SAMPLES = 8;
  const SPEED_DOWNLOAD_MS = 4500;
  const SPEED_DOWNLOAD_STREAMS = 4;
  const SPEED_DOWNLOAD_CHUNK_BYTES = 3 * 1024 * 1024; // per request, well under the 4.5 MB platform cap
  const SPEED_UPLOAD_MS = 4500;
  const SPEED_UPLOAD_STREAMS = 3;
  const SPEED_UPLOAD_CHUNK_BYTES = 1 * 1024 * 1024;
  const SPEED_PROGRESS_TICK_MS = 200;

  const speedState = { running: false, cancelled: false, controller: null };
  let speedUploadPayload = null; // built lazily, reused across the whole test run

  function formatMs(ms) {
    return `${Math.round(ms)} ms`;
  }

  function formatMbps(mbps) {
    if (!isFinite(mbps) || mbps < 0) return '--';
    return `${mbps.toFixed(mbps >= 10 ? 1 : 2)} Mbps`;
  }

  function buildRandomPayload(size) {
    const buf = new Uint8Array(size);
    const MAX_SLICE = 65536; // crypto.getRandomValues' per-call ceiling
    for (let offset = 0; offset < size; offset += MAX_SLICE) {
      crypto.getRandomValues(buf.subarray(offset, Math.min(offset + MAX_SLICE, size)));
    }
    return buf;
  }

  function setSpeedPhase(label, dotState) {
    el.speedPhaseText.textContent = label;
    el.speedDot.className = 'status-dot ' + (dotState || 'off');
  }

  function setSpeedProgress(pct, label) {
    el.speedProgressFill.style.transform = `scaleX(${Math.max(0, Math.min(1, pct / 100)).toFixed(4)})`;
    el.speedProgressLabel.textContent = label;
  }

  function resetSpeedResults() {
    el.speedStatPing.textContent = '--';
    el.speedStatJitter.textContent = '--';
    el.speedStatDownload.textContent = '--';
    el.speedStatUpload.textContent = '--';
    [el.speedStatPing, el.speedStatJitter, el.speedStatDownload, el.speedStatUpload]
      .forEach((n) => { n.className = 'metric-value'; });
  }

  function setSpeedUiRunning(running) {
    el.btnSpeedStart.classList.toggle('hidden', running);
    el.btnSpeedStop.classList.toggle('hidden', !running);
    el.speedProgressWrap.classList.toggle('hidden', !running);
  }

  async function runPingTest(samples) {
    const times = [];
    for (let i = 0; i < samples; i++) {
      if (speedState.cancelled) break;
      const t0 = performance.now();
      try {
        const res = await fetch(`${SPEED_ENDPOINT}?r=${Math.random()}`, { cache: 'no-store' });
        if (!res.ok) continue;
        await res.json();
      } catch {
        continue;
      }
      times.push(performance.now() - t0);
    }
    // Drop the first sample — it usually eats a fresh connection's TLS
    // handshake and isn't representative of steady-state latency.
    const usable = times.slice(1);
    if (usable.length < 2) return { ping: times[0] ?? null, jitter: null };

    const sorted = [...usable].sort((a, b) => a - b);
    const median = sorted[Math.floor(sorted.length / 2)];

    let jitterSum = 0;
    for (let i = 1; i < usable.length; i++) jitterSum += Math.abs(usable[i] - usable[i - 1]);
    const jitter = jitterSum / (usable.length - 1);

    return { ping: median, jitter };
  }

  async function runDownloadTest(durationMs, streamCount, chunkBytes, onProgress) {
    const controller = new AbortController();
    speedState.controller = controller;
    let totalBytes = 0;
    const start = performance.now();
    const durationTimer = setTimeout(() => controller.abort(), durationMs);

 async function worker() {
      while (!controller.signal.aborted) {
        let res;
        try {
          res = await fetch(`${SPEED_ENDPOINT}?dl=1&n=${chunkBytes}&r=${Math.random()}`, {
            signal: controller.signal,
            cache: 'no-store',
          });
        } catch {
          return;
        }
        if (!res.ok) return;
        if (res.body && res.body.getReader) {
          const reader = res.body.getReader();
          for (;;) {
            let step;
            try {
              step = await reader.read();
            } catch {
              return;
            }
            if (step.done) break;
            totalBytes += step.value.length;
          }
        } else {
          // Fallback for a response type without a readable stream.
          const buf = await res.arrayBuffer();
          totalBytes += buf.byteLength;
        }
      }
    }

    const workers = Array.from({ length: streamCount }, worker);
    const progressTimer = setInterval(() => {
      onProgress(totalBytes, (performance.now() - start) / 1000);
    }, SPEED_PROGRESS_TICK_MS);

    await Promise.allSettled(workers);
    clearTimeout(durationTimer);
    clearInterval(progressTimer);
    speedState.controller = null;

    return { bytes: totalBytes, seconds: (performance.now() - start) / 1000 };
  }

  async function runUploadTest(durationMs, streamCount, chunkBytes, onProgress) {
    if (!speedUploadPayload || speedUploadPayload.length !== chunkBytes) {
      speedUploadPayload = buildRandomPayload(chunkBytes);
    }
    const payload = speedUploadPayload;

    const controller = new AbortController();
    speedState.controller = controller;
    let totalBytes = 0;
    const start = performance.now();
    const durationTimer = setTimeout(() => controller.abort(), durationMs);

    async function worker() {
      while (!controller.signal.aborted) {
        try {
          const res = await fetch(SPEED_ENDPOINT, {
            method: 'POST',
            headers: { 'Content-Type': 'application/octet-stream' },
            body: payload,
            signal: controller.signal,
            cache: 'no-store',
          });
          if (!res.ok) return;
          await res.json().catch(() => {});
          totalBytes += payload.length;
        } catch {
          return;
        }
      }
    }

    const workers = Array.from({ length: streamCount }, worker);
    const progressTimer = setInterval(() => {
      onProgress(totalBytes, (performance.now() - start) / 1000);
    }, SPEED_PROGRESS_TICK_MS);

    await Promise.allSettled(workers);
    clearTimeout(durationTimer);
    clearInterval(progressTimer);
    speedState.controller = null;

    return { bytes: totalBytes, seconds: (performance.now() - start) / 1000 };
  }

  async function runSpeedTest() {
    if (speedState.running) return;
    speedState.running = true;
    speedState.cancelled = false;
    setSpeedUiRunning(true);
    resetSpeedResults();
    setSpeedPhase('running', 'warn');
    logLine('starting speed test…', 'info', el.speedLog);

    try {
      // --- ping ------------------------------------------------------
      setSpeedProgress(5, 'Measuring ping…');
      const { ping, jitter } = await runPingTest(SPEED_PING_SAMPLES);
      if (speedState.cancelled) throw new Error('cancelled');

      if (ping != null) {
        el.speedStatPing.textContent = formatMs(ping);
        el.speedStatJitter.textContent = jitter != null ? formatMs(jitter) : '--';
        logLine(`ping ${formatMs(ping)}${jitter != null ? `, jitter ${formatMs(jitter)}` : ''}`, 'ok', el.speedLog);
      } else {
        el.speedStatPing.textContent = 'failed';
        el.speedStatPing.className = 'metric-value err';
        logLine('ping test failed: server unreachable', 'err', el.speedLog);
      }

      // --- download ----------------------------------------------------
      setSpeedProgress(20, 'Testing download…');
      el.speedStatDownload.className = 'metric-value live';
      const dl = await runDownloadTest(
        SPEED_DOWNLOAD_MS, SPEED_DOWNLOAD_STREAMS, SPEED_DOWNLOAD_CHUNK_BYTES,
        (bytes, sec) => {
          const mbps = sec > 0 ? (bytes * 8) / sec / 1e6 : 0;
          el.speedStatDownload.textContent = formatMbps(mbps);
          const frac = Math.min(1, (sec * 1000) / SPEED_DOWNLOAD_MS);
          setSpeedProgress(20 + frac * 35, `Testing download: ${formatMbps(mbps)}`);
        },
      );
      if (speedState.cancelled) throw new Error('cancelled');
      const dlMbps = dl.seconds > 0 ? (dl.bytes * 8) / dl.seconds / 1e6 : 0;
      el.speedStatDownload.textContent = formatMbps(dlMbps);
      el.speedStatDownload.className = 'metric-value ok';
      logLine(`download ${formatMbps(dlMbps)} (${formatBytes(dl.bytes)} in ${dl.seconds.toFixed(1)}s)`, 'ok', el.speedLog);

      // --- upload --------------------------------------------------------
      setSpeedProgress(55, 'Testing upload…');
      el.speedStatUpload.className = 'metric-value live';
      const ul = await runUploadTest(
        SPEED_UPLOAD_MS, SPEED_UPLOAD_STREAMS, SPEED_UPLOAD_CHUNK_BYTES,
        (bytes, sec) => {
          const mbps = sec > 0 ? (bytes * 8) / sec / 1e6 : 0;
          el.speedStatUpload.textContent = formatMbps(mbps);
          const frac = Math.min(1, (sec * 1000) / SPEED_UPLOAD_MS);
          setSpeedProgress(55 + frac * 40, `Testing upload: ${formatMbps(mbps)}`);
        },
      );
      if (speedState.cancelled) throw new Error('cancelled');
      const ulMbps = ul.seconds > 0 ? (ul.bytes * 8) / ul.seconds / 1e6 : 0;
      el.speedStatUpload.textContent = formatMbps(ulMbps);
      el.speedStatUpload.className = 'metric-value ok';
      logLine(`upload ${formatMbps(ulMbps)} (${formatBytes(ul.bytes)} in ${ul.seconds.toFixed(1)}s)`, 'ok', el.speedLog);

      setSpeedProgress(100, 'Done');
      setSpeedPhase('done', 'on');
      logLine('speed test complete', 'ok', el.speedLog);
    } catch (err) {
      if (speedState.cancelled || err.message === 'cancelled') {
        setSpeedPhase('stopped', 'warn');
        setSpeedProgress(0, 'Stopped');
        logLine('speed test stopped', 'warn', el.speedLog);
      } else {
        setSpeedPhase('error', 'err');
        setSpeedProgress(0, 'Failed');
        logLine(`speed test error: ${err.message}`, 'err', el.speedLog);
      }
    } finally {
      speedState.running = false;
      setSpeedUiRunning(false);
    }
  }

  function stopSpeedTest() {
    if (!speedState.running) return;
    speedState.cancelled = true;
    if (speedState.controller) speedState.controller.abort();
    logLine('stopping…', 'warn', el.speedLog);
  }

  el.btnSpeedStart.addEventListener('click', runSpeedTest);
  el.btnSpeedStop.addEventListener('click', stopSpeedTest);
  el.btnSpeedClearLog.addEventListener('click', () => { el.speedLog.innerHTML = ''; });

  // =========================================================================
  // BOOT
  // =========================================================================

  setBootStatus('Initializing interface…');
  logLine('p2p::terminal ready', 'ok');
  connectSignaling();
})();
