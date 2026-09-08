// client.js - P2P tunnel: Gateway creates offer, browser answers.

(function () {
    'use strict';

    const cfg = window.__GATEWAY_CONFIG__ || {};
    const L = window.log || function(){};
    // Sticky target: once P2P is established via /?target=B, later loads
    // without ?target= reuse B (URL param with empty value clears it).
    const LS_KEY = 'p2p-target';
    let pendingTarget = '';
    let userTriggeredConnect = false;
    let autoNavDone = false;
    let pendingNavWindow = null;
    const sessGet = (k) => { try { return sessionStorage.getItem(k) || ''; } catch(e) { return ''; } };
    const sessSet = (k,v) => { try { sessionStorage.setItem(k,v); } catch(e) {} };
    const sessRemove = (k) => { try { sessionStorage.removeItem(k); } catch(e) {} };
    function resolvePageTarget() {
        if (pendingTarget) {
            const t = pendingTarget;
            pendingTarget = '';
            return t;
        }        try {
            const params = new URLSearchParams(location.search);
            if (params.has('target')) {
                const v = params.get('target') || '';
                if (v) {
                    sessSet(LS_KEY, v);
                    return v;
                }
                sessRemove(LS_KEY);
                return '';
            }
            const s = sessGet(LS_KEY);
            if (s) return s;
            return '';
        } catch (e) {
            return new URLSearchParams(location.search).get('target') || '';
        }
    }
    async function probeAndMaybeRedirect(force) {
        if (autoNavDone) return;
        const t = resolvePageTarget() || pageTargetCache || (cfg.target || '');
        if (!t) { if (force && pendingNavWindow) { try { pendingNavWindow.document.body.innerHTML = '<p style="font:14px monospace;padding:20px;color:#c00">No target</p>'; } catch(e) {} } return; }
        const hasParam = new URLSearchParams(location.search).has('target');
        if (!hasParam && !userTriggeredConnect && !force) return;
        if (location.pathname !== '/p2p/' && location.pathname !== '/p2p') return;
        if (!force && sessGet('p2p-auto-nav') !== 'on') return;
        if (force && pendingNavWindow && pendingNavWindow.closed) pendingNavWindow = null;
        let probeStatus = 0; let probeOk = false;
        try {
            const r = await fetch('/?__p2p_probe=' + Date.now(), { cache: 'no-store' });
            probeStatus = r.status;
            probeOk = r.status !== 502 && r.status !== 503 && r.status !== 504;
        } catch(e) { probeOk = false; }
        if (!probeOk) {
            if (force && pendingNavWindow && !pendingNavWindow.closed) {
                try { pendingNavWindow.document.body.innerHTML = '<p style="font:14px monospace;padding:20px">Tunnel probe failed (status '+probeStatus+') — <a href="/?target='+encodeURIComponent(t)+'">click to open anyway</a> (keep this tab open)</p>'; } catch(e) {}
                L('warn','NAV','probe failed status='+probeStatus+' — keep window, manual link shown');
            }
            if (!force) return;
            if (force && pendingNavWindow) {
                const url = '/?target=' + encodeURIComponent(t);
                try { pendingNavWindow.location.href = url; L('ok','NAV','probe fail but force-navigating to target'); } catch(e) {}
                autoNavDone = true; pendingNavWindow = null; return;
            }
            return;
        }
        autoNavDone = true;
        const url = '/?target=' + encodeURIComponent(t);
        if (force && pendingNavWindow && !pendingNavWindow.closed) {
            try { pendingNavWindow.location.href = url; } catch(e) { const w = window.open(url, '_blank'); if (!w) showPopupFallback(url); else pendingNavWindow = w; }
            L('ok','NAV','probe '+probeStatus+' — navigating to target');
            pendingNavWindow = null;
            return;
        }
        const w = window.open(url, '_blank');
        if (!w) { showPopupFallback(url); return; }
        L('ok','NAV','probe '+probeStatus+' — opened target in new tab (keep this /p2p/ tab alive)');
    }
    function showPopupFallback(url) {
        L('warn','NAV','popup blocked — keep /p2p/ alive and open '+url+' manually');
        const a = document.createElement('a');
        a.href = url; a.textContent = '→ Open target (popup blocked)'; a.target = '_blank';
        a.style.cssText = 'display:block;margin:8px 0;color:#06c';
        document.body.prepend(a);
    }

    let pageTargetCache = '';
    function pushTargetToSW(target) {
        if (target) pageTargetCache = target;
        if (!pageTargetCache || !('serviceWorker' in navigator)) {
            return;
        }
        const t = pageTargetCache;
        const send = () => {
            if (navigator.serviceWorker.controller) {
                try { navigator.serviceWorker.controller.postMessage({ type: 'target', target: t }); DIAG('ok','SW','push target '+t); } catch(e) {}
                return true;
            }
            return false;
        };
        if (!send()) {
            navigator.serviceWorker.addEventListener('controllerchange', send, { once: true });
            setTimeout(send, 700);
            setTimeout(send, 2500);
        }
    }
    document.getElementById('target').textContent = resolvePageTarget() || cfg.target || '(url param)';
    document.getElementById('host').textContent = cfg.host || location.host;
    document.getElementById('stun').textContent = cfg.stun || '(none)';

    function setRow(id, text, ok) {
        const el = document.getElementById(id);
        el.textContent = text;
        el.className = ok ? 'val ok' : (ok === false ? 'val bad' : 'val');
    }
    function setStatus(text, ok) {
        const el = document.getElementById('status');
        el.textContent = text;
        el.className = ok ? 'val ok' : (ok === false ? 'val bad' : 'val');
    }

    const SW_VERSION = 'v11-assets';
    const DIAG = (lvl, tag, msg) => { try { L(lvl, tag, msg); } catch(e) {} try { console.log('['+tag+'] '+msg); } catch(e) {} };

    // State declared before boot: without SW support bootP2P() runs
    // synchronously during IIFE evaluation, and touching later-declared
    // let/consts would throw "Cannot access ... before initialization".
    let dc = null;
    let ws = null;
    let pc = null;
    const inflight = new Map();
    const wsStreams = new Map();

    // --- Service Worker ---
    // P2P signaling (WS + DataChannel) does NOT depend on the SW — the SW
    // only intercepts fetch(). Boot P2P immediately on page load so
    // /p2p/?target=X connects without waiting for SW install/ready and
    // without requiring a manual Connect click; the SW catches up in the
    // background. Guarded against double-boot.
    let booted = false;
    function bootP2P() {
        if (booted) {
            return;
        }
        booted = true;
        try {
            startP2P();
        } catch (e) {
            setRow('signal', 'ERR: ' + e.message, false);
            L('err', 'SIGNAL', 'boot failed: ' + e.message);
        }
    }
    if ('serviceWorker' in navigator) {
        L('info', 'SW', 'Registering service worker…');
        navigator.serviceWorker.register('/p2p/sw.js', { scope: '/' })
            .then(reg => {
                if (reg.waiting) {
                    L('warn', 'SW', 'New SW waiting, activating…');
                    reg.waiting.postMessage({ type: 'skip-waiting' });
                }
                reg.addEventListener('updatefound', () => {
                    const sw = reg.installing;
                    if (sw) sw.addEventListener('statechange', () => {
                        if (sw.state === 'installed' && navigator.serviceWorker.controller) {
                            L('warn', 'SW', 'Update found, activating…');
                            sw.postMessage({ type: 'skip-waiting' });
                        }
                    });
                });
                setRow('sw', 'OK (' + SW_VERSION + ')', true);
                L('ok', 'SW', 'Service worker registered');
                return navigator.serviceWorker.ready;
            })
            .then(bootP2P, bootP2P)
            .catch(err => {
                setRow('sw', 'ERR', false);
                L('err', 'SW', 'Registration failed: ' + err.message);
                bootP2P();
            });
        // P2P boots now — do not wait for SW ready (see above).
        bootP2P();
    } else {
        setRow('sw', 'unsupported', false);
        L('err', 'SW', 'Service Workers not supported');
        bootP2P();
    }

    function startP2P() {
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        let signalUrl = proto + '//' + location.host + '/p2p/signal';

        const pageTarget = resolvePageTarget();
        if (pageTarget) {
            signalUrl += '?target=' + encodeURIComponent(pageTarget);
            L('info', 'TARGET', pageTarget);
        } else if (cfg.target) {
            signalUrl += '?target=' + encodeURIComponent(cfg.target);
            L('info', 'TARGET', cfg.target + ' (from config)');
        } else {
            L('warn', 'TARGET', 'No target specified, using host routing');
        }

        // --- WebSocket Signaling ---
        setStatus('Connecting…');
        L('info', 'SIGNAL', 'Connecting to ' + signalUrl);
        pushTargetToSW(pageTarget || cfg.target || '');
        ws = new WebSocket(signalUrl);

        ws.onopen = () => {
            setRow('signal', 'OK', true);
            L('ok', 'SIGNAL', 'WebSocket connected');
            DIAG('ok', 'SIGNAL', 'WS open url=' + signalUrl + ' readyState=' + ws.readyState);
        };
        ws.onclose = (e) => {
            setRow('signal', 'closed', false);
            L('err', 'SIGNAL', 'WebSocket closed (code=' + e.code + ')');
            DIAG('err', 'SIGNAL', 'WS closed code=' + e.code + ' reason=' + (e.reason||'') + ' target=' + (resolvePageTarget()||cfg.target||''));
            setStatus('Disconnected', false);
        };
        ws.onerror = () => {
            setRow('signal', 'error', false);
            L('err', 'SIGNAL', 'WebSocket error');
            DIAG('err', 'SIGNAL', 'WS error url=' + signalUrl);
            setStatus('Signaling error', false);
        };

        let myId = null;

        // --- WebRTC ---
        L('info', 'WEBRTC', 'Creating RTCPeerConnection…');
        pc = new RTCPeerConnection({
            iceServers: [{ urls: cfg.stun || 'stun:stun.miwifi.com:3478' }]
        });

        pc.onicecandidate = (e) => {
            if (e.candidate) {
                const c = e.candidate;
                L('ice', 'ICE', 'Local candidate: ' + c.candidate.substring(0, 60) + '…');
                DIAG('ice', 'ICE', 'local cand ' + c.candidate.substring(0, 80) + '… sdpMid=' + c.sdpMid);
                sendSignal({ type: 'candidate', to: myId, candidate: c });
            } else {
                L('ice', 'ICE', 'Gathering complete');
                DIAG('ice', 'ICE', 'gathering complete pcState=' + pc.connectionState);
            }
        };

        pc.onicegatheringstatechange = () => {
            L('ice', 'ICE', 'Gathering: ' + pc.iceGatheringState);
            DIAG('ice', 'ICE', 'gathering=' + pc.iceGatheringState);
        };

        pc.onconnectionstatechange = () => {
            const s = pc.connectionState;
            setRow('ice', s, s === 'connected');
            L('ice', 'ICE', 'Connection: ' + s);
            DIAG('ice', 'WEBRTC', 'pc state=' + s + ' ice=' + pc.iceConnectionState + ' gathering=' + pc.iceGatheringState);
            if (s === 'connected') {
                setStatus('WebRTC connected');
            } else if (s === 'failed') {
                setStatus('WebRTC failed', false);
                L('err', 'WEBRTC', 'Connection failed — try different STUN/TURN');
                DIAG('err', 'WEBRTC', 'pc failed candidates=' + (pc.getStats ? 'see stats' : 'n/a') + ' stun=' + (cfg.stun||''));
            } else if (s === 'disconnected') {
                setStatus('WebRTC disconnected', false);
            }
        };

        pc.oniceconnectionstatechange = () => {
            L('ice', 'ICE', 'ICE state: ' + pc.iceConnectionState);
            DIAG('ice', 'ICE', 'ICE state=' + pc.iceConnectionState + ' gathering=' + pc.iceGatheringState + ' dc=' + (dc ? dc.readyState : 'none'));
        };

        // Gateway creates DataChannel, browser receives it
        pc.ondatachannel = (event) => {
            dc = event.channel;
            L('ok', 'DC', 'Received DataChannel: ' + dc.label);

            let kaTimer = null;
            let ackSeen = false;
            dc.onopen = () => {
                setRow('dc', 'OPEN', true);
                setStatus('✅ Ready', true);
                L('ok', 'DC', 'DataChannel opened — tunnel ready');
                DIAG('ok', 'DC', 'open label=' + dc.label + ' readyState=' + dc.readyState + ' buffered=' + dc.bufferedAmount + ' pc=' + pc.connectionState);
                const bar = document.getElementById('loading-bar');
                if (bar) { bar.classList.remove('active'); bar.style.width = '100%'; }
                notifySW(true);
                L('info', 'NAV', 'Tunnel ready — ' + (pendingNavWindow ? 'opening target...' : 'use Test or browse, target stays bound'));
                try { if (pendingNavWindow) probeAndMaybeRedirect(true); else probeAndMaybeRedirect(); } catch(e) {}
                try { dc.send(JSON.stringify({ type: 'ready', ts: Date.now() })); DIAG('ok', 'DC', '→ ready'); } catch(e) { DIAG('err','DC','ready send err '+e.message); }
                setTimeout(() => { if (!ackSeen) DIAG('warn','DC','ready-ack not seen in 2s, gateway may be old binary'); }, 2000);
                if (kaTimer) clearInterval(kaTimer);
                kaTimer = setInterval(() => {
                    try { if (dc.readyState === 'open') { dc.send(JSON.stringify({ type: 'ping', ts: Date.now() })); DIAG('ice','DC','→ ping'); } } catch(e) {}
                }, 15000);
            };

            dc.onclose = () => {
                if (kaTimer) { clearInterval(kaTimer); kaTimer = null; }
                setRow('dc', 'CLOSED', false);
                setStatus('DataChannel closed', false);
                L('err', 'DC', 'DataChannel closed');
                DIAG('err','DC','closed buffered=' + (dc ? dc.bufferedAmount : '?'));
                notifySW(false);
            };

            dc.onerror = (e) => {
                L('err', 'DC', 'DataChannel error: ' + (e.error?.message || e));
                DIAG('err','DC','error '+(e.error?.message||e));
            };

            const chunked = new Map();
            dc.onmessage = (event) => {
                let raw = event.data;
                let msg;
                try { msg = JSON.parse(raw); } catch { DIAG('warn','DC','non-json msg len='+(raw&&raw.length)); return; }
                DIAG('ok','DC','← '+msg.type+(msg.type==='response'?' '+msg.status:'')+(msg.type==='response-chunk'?' '+msg.idx:''));
                if (msg.type === 'ping') { try { dc.send(JSON.stringify({ type: 'pong', ts: msg.ts })); DIAG('ice','DC','→ pong'); } catch(e) {} return; }
                if (msg.type === 'pong') return;
                if (msg.type === 'ready') { try { dc.send(JSON.stringify({ type: 'ready-ack', ts: Date.now() })); DIAG('ok','DC','→ ready-ack'); } catch(e) {} L('ok', 'DC', 'ready↔ack, tunnel verified'); return; }
                if (msg.type === 'ready-ack') { ackSeen = true; DIAG('ok','DC','← ready-ack verified'); L('ok', 'DC', 'ready-ack received, tunnel verified'); return; }
                if (msg.type === 'response-start') {
                    if (typeof msg.headers === 'string') { try { msg.headers = JSON.parse(msg.headers); } catch(e) { msg.headers = {}; } }
                    if (!msg.headers || typeof msg.headers !== 'object') msg.headers = {};
                    chunked.set(msg.id, { headers: msg.headers, status: msg.status, chunks: msg.chunks, parts: new Array(msg.chunks), received: 0, port: inflight.get(msg.id) });
                    DIAG('ok','DC','chunked start id='+msg.id+' chunks='+msg.chunks);
                    return;
                }
                if (msg.type === 'response-chunk') {
                    const c = chunked.get(msg.id);
                    if (!c) return;
                    c.parts[msg.idx] = msg.data;
                    c.received++;
                    return;
                }
                if (msg.type === 'response-end') {
                    const c = chunked.get(msg.id);
                    if (!c) return;
                    chunked.delete(msg.id);
                    inflight.delete(msg.id);
                    const body = c.parts.join('');
                    DIAG('ok','DC','chunked done id='+msg.id+' bodyLen='+body.length);
                    const out = { type: 'response', id: msg.id, status: c.status, headers: c.headers, body };
                    const port = c.port;
                    if (!port) return;
                    L('tunnel', 'HTTP', '← ' + out.status + ' ' + (out.headers?.['content-type'] || '') + ' id=' + out.id + ' (chunked)');
                    port.postMessage({ type: 'response', ...out });
                    return;
                }
                if (msg.type === 'response') {
                    const port = inflight.get(msg.id);
                    if (!port) return;
                    inflight.delete(msg.id);
                    L('tunnel', 'HTTP', '← ' + msg.status + ' ' + (msg.headers?.['content-type'] || '') + ' id=' + msg.id);
                    port.postMessage({ type: 'response', ...msg });
                } else if (msg.type === 'ws-open-ok') {
                    const port = wsStreams.get(msg.id);
                    if (!port) return;
                    L('ok', 'WS', 'Upstream connected id=' + msg.id);
                    port.postMessage(msg);
                } else if (msg.type === 'ws-open-err') {
                    const port = wsStreams.get(msg.id);
                    if (!port) return;
                    L('err', 'WS', 'Upstream failed: ' + msg.error + ' id=' + msg.id);
                    port.postMessage(msg);
                    wsStreams.delete(msg.id);
                } else if (msg.type === 'ws-data') {
                    const port = wsStreams.get(msg.id);
                    if (!port) return;
                    const size = msg.data ? Math.round(msg.data.length * 3 / 4) : 0;
                    L('tunnel', 'WS', '← ' + (msg.binary ? 'binary' : 'text') + ' ' + size + 'B id=' + msg.id);
                    port.postMessage(msg);
                } else if (msg.type === 'ws-closed') {
                    const port = wsStreams.get(msg.id);
                    if (!port) return;
                    L('warn', 'WS', 'Closed code=' + msg.code + ' id=' + msg.id);
                    port.postMessage(msg);
                    wsStreams.delete(msg.id);
                }
            };
        };

        function notifySW(open) {
            if (!('serviceWorker' in navigator)) return;
            const target = new URLSearchParams(location.search).get('target') || cfg.target || pageTarget || '';
            const payload = { type: 'dc-state', open, target };
            const trySend = () => {
                if (navigator.serviceWorker.controller) {
                    try { navigator.serviceWorker.controller.postMessage(payload); DIAG(open?'ok':'warn','SW','notify dc='+open+' target='+target); return true; } catch(e) { DIAG('err','SW','notify fail '+e.message); }
                }
                return false;
            };
            if (trySend()) return;
            DIAG('warn','SW','no controller yet, queue notify dc='+open);
            navigator.serviceWorker.addEventListener('controllerchange', trySend, { once: true });
            setTimeout(trySend, 800);
            setTimeout(trySend, 2500);
        }

        // --- SW ↔ DC bridge (SW only; without SW there is no fetch
        // interception, but WS + DataChannel still work) ---
        if ('serviceWorker' in navigator) {
        navigator.serviceWorker.addEventListener('message', (event) => {
            const msg = event.data || {};
            const port = event.ports && event.ports[0];
            if (!port) return;

            if (msg.type === 'tunnel' && msg.payload) {
                const recvAt = Date.now();
                DIAG('ok','HTTP','SW→client id='+msg.payload.id+' '+msg.payload.method+' '+msg.payload.path+' dc='+(dc?dc.readyState:'null'));
                if (!dc || dc.readyState !== 'open') {
                    DIAG('err','HTTP','dc not open for id='+msg.payload.id+' state='+(dc?dc.readyState:'null'));
                    port.postMessage({ type: 'error', error: 'dc not open state='+(dc?dc.readyState:'null') });
                    return;
                }
                inflight.set(msg.payload.id, port);
                L('tunnel', 'HTTP', '→ ' + msg.payload.method + ' ' + msg.payload.path + ' id=' + msg.payload.id);
                try { dc.send(JSON.stringify(msg.payload)); DIAG('ok','HTTP','→ DC id='+msg.payload.id+' '+ (Date.now()-recvAt)+'ms'); } catch(e) { DIAG('err','HTTP','dc send fail id='+msg.payload.id+' '+e.message); try{port.postMessage({type:'error',error:e.message});}catch(_){} inflight.delete(msg.payload.id); return; }
                setTimeout(() => {
                    if (inflight.has(msg.payload.id)) {
                        const p = inflight.get(msg.payload.id);
                        inflight.delete(msg.payload.id);
                        L('err', 'HTTP', 'Timeout id=' + msg.payload.id);
                        DIAG('err','HTTP','client timeout id='+msg.payload.id+' after 8s');
                        try{p.postMessage({ type: 'error', error: 'client timeout 8s' });}catch(_){}
                    }
                }, 8000);
            } else if (msg.type === 'ws-tunnel' && msg.payload) {
                if (!dc || dc.readyState !== 'open') {
                    port.postMessage({ type: 'error', error: 'dc not open' });
                    return;
                }
                wsStreams.set(msg.payload.id, port);
                L('tunnel', 'WS', '→ Open ' + msg.payload.path + ' id=' + msg.payload.id);
                dc.send(JSON.stringify(msg.payload));
            } else if (msg.type === 'ws-data-send' && msg.payload) {
                if (dc && dc.readyState === 'open') {
                    const size = msg.payload.data ? Math.round(msg.payload.data.length * 3 / 4) : 0;
                    L('tunnel', 'WS', '→ Data ' + size + 'B id=' + msg.payload.id);
                    dc.send(JSON.stringify(msg.payload));
                }
            } else if (msg.type === 'ws-close-send' && msg.payload) {
                if (dc && dc.readyState === 'open') {
                    L('tunnel', 'WS', '→ Close id=' + msg.payload.id);
                    dc.send(JSON.stringify(msg.payload));
                    wsStreams.delete(msg.payload.id);
                }
            }
        });
        }

        // --- Signaling messages ---
        // Gateway creates offer, browser answers
        ws.onmessage = async (event) => {
            let msg;
            try { msg = JSON.parse(event.data); } catch { return; }

            if (msg.type === 'join') {
                myId = msg.id;
                L('signal', 'SIGNAL', 'Joined as peer ' + myId);
                DIAG('ok','SIGNAL','join id='+myId);
            } else if (msg.type === 'offer') {
                L('signal', 'SIGNAL', '← Offer received from Gateway');
                DIAG('signal','SIGNAL','← offer sdpType='+(msg.sdp&&msg.sdp.type));
                await pc.setRemoteDescription(msg.sdp);
                L('info', 'WEBRTC', 'Creating SDP answer…');
                const answer = await pc.createAnswer();
                await pc.setLocalDescription(answer);
                L('signal', 'SIGNAL', '→ Answer sent');
                DIAG('signal','SIGNAL','→ answer');
                sendSignal({ type: 'answer', to: myId, sdp: answer });
            } else if (msg.type === 'candidate') {
                try {
                    await pc.addIceCandidate(msg.candidate);
                    const c = msg.candidate;
                    L('ice', 'ICE', '← Remote candidate: ' + (c.candidate || '').substring(0, 60) + '…');
                    DIAG('ice','ICE','← remote cand '+(c.candidate||'').substring(0,80));
                } catch (e) {
                    L('err', 'ICE', 'addIceCandidate failed: ' + e.message);
                    DIAG('err','ICE','addIceCandidate fail '+e.message);
                }
            }
        };

        function sendSignal(obj) {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(obj));
            }
        }

        window.__p2p = { dc, pc };
        L('info', 'BOOT', 'P2P tunnel initialized');
    }

    // --- Target switching (control plane, no page reload) ---
    // switchTarget(t): hot-swap target over existing WS/DC if live
    // (no ICE rebuild), else rebuild tunnel.
    const LS_HIST = 'p2p-targets';
    let histCache = [];
    function escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, (c) => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[c]));
    }
    function loadHistory() {
        try {
            const h = JSON.parse(localStorage.getItem(LS_HIST) || '[]');
            histCache = Array.isArray(h) ? h.filter((x) => typeof x === 'string') : [];
        } catch (e) {
            histCache = [];
        }
        return histCache;
    }
    function renderHistory() {
        const box = document.getElementById('targetHistory');
        if (!box) {
            return;
        }
        loadHistory();
        box.innerHTML = histCache.map((t, i) =>
            '<button class="hist-btn" data-i="' + i + '" title="switch to ' +
            escapeHtml(t) + '">' + escapeHtml(t) + '</button>').join('');
    }
    function switchTarget(t) {
        t = (t || '').trim();
        if (!t) {
            return false;
        }
        userTriggeredConnect = true; autoNavDone = false;
        try {
            const u = new URL(location.href);
            u.searchParams.set('target', t);
            history.replaceState(null, '', u.toString());
        } catch (e) {}
        pendingTarget = t;
        try {
            const w = window.open('about:blank', '_blank');
            if (w) {
                try { w.document.title = 'Connecting ' + t; w.document.body.innerHTML = '<p style="font:14px monospace;padding:20px">Connecting to ' + escapeHtml(t) + ' — probing tunnel...</p>'; } catch(e2) {}
                if (pendingNavWindow && !pendingNavWindow.closed) { try { pendingNavWindow.close(); } catch(e2) {} }
                pendingNavWindow = w;
            }
        } catch(e) {}
        try {
            sessSet(LS_KEY, t);
            const hist = [t].concat(loadHistory().filter((x) => x !== t)).slice(0, 8);
            localStorage.setItem(LS_HIST, JSON.stringify(hist));
        } catch (e) {}
        renderHistory();
        const input = document.getElementById('targetInput');
        if (input) {
            input.value = t;
        }
        document.getElementById('target').textContent = t;
        pushTargetToSW(t);
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
            try {
                const doSend = () => {
                    try {
                        ws.send(JSON.stringify({ type: 'target', target: t }));
                        pendingTarget = '';
                        L('info', 'TARGET', 'hot-swapped to ' + t + ' (no ICE rebuild)');
                        if (dc && dc.readyState === 'open') {
                            setStatus('✅ Ready', true);
                            setTimeout(() => { try { probeAndMaybeRedirect(true); } catch(e) {} }, 400);
                        } else {
                            L('info', 'NAV', 'Target switched — waiting for tunnel to open new tab');
                        }
                    } catch (e2) {}
                };
                if (ws.readyState === WebSocket.OPEN) doSend();
                else ws.addEventListener('open', doSend, { once: true });
                return true;
            } catch (e) {
                L('warn', 'TARGET', 'hot-swap failed, rebuilding: ' + e.message);
            }
        }
        try { if (ws) ws.close(); } catch (e) {}
        try { if (pc) pc.close(); } catch (e) {}
        try { if (window.__p2p && window.__p2p.pc) window.__p2p.pc.close(); } catch (e) {}
        try { inflight.clear(); } catch (e) {}
        try { wsStreams.clear(); } catch (e) {}
        dc = null;
        pc = null;
        setRow('signal', 'switching…');
        setRow('ice', '…');
        setRow('dc', '…');
        setStatus('Switching target…');
        L('info', 'TARGET', 'Rebuilding tunnel for ' + t);
        try {
            startP2P();
        } catch (e) {
            setRow('signal', 'ERR: ' + e.message, false);
        }
        return true;
    }
    window.__switchTarget = switchTarget;

    // --- Test button ---
    // With a controlling SW the fetch goes through the P2P tunnel;
    // without SW support (plain http over LAN) it falls back to a direct
    // /upstream/ request carrying the sticky target.
    renderHistory();
    const histBox = document.getElementById('targetHistory');
    if (histBox && histBox.addEventListener) {
        histBox.addEventListener('click', (event) => {
            const btn = event.target && event.target.closest
                ? event.target.closest('[data-i]') : null;
            if (!btn) {
                return;
            }
            const t = histCache[parseInt(btn.getAttribute('data-i'), 10)];
            if (t) {
                switchTarget(t);
            }
        });
    }
    document.getElementById('btn').addEventListener('click', async () => {
        const out = document.getElementById('out');
        out.textContent = 'fetching…';
        L('tunnel', 'TEST', 'Fetching / via P2P…');
        try {
            const r = await fetch('/?test=' + Date.now());
            const text = await r.text();
            out.textContent = 'HTTP ' + r.status + '\n\n' + text.slice(0, 4000);
            L('ok', 'TEST', 'Got HTTP ' + r.status + ' (' + text.length + ' bytes)');
        } catch (e) {
            out.textContent = 'error: ' + e.message;
            L('err', 'TEST', e.message);
        }
    });
})();
