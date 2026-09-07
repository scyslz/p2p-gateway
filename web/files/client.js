// client.js - P2P tunnel with full logging.

(function () {
    'use strict';

    const cfg = window.__GATEWAY_CONFIG__ || {};
    const L = window.log || function(){};
    document.getElementById('target').textContent = cfg.target || '(url param)';
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

    const SW_VERSION = 'v3';

    // --- Service Worker ---
    if ('serviceWorker' in navigator) {
        L('info', 'SW', 'Registering service worker…');
        navigator.serviceWorker.register('/p2p-sw.js')
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
            .then(() => startP2P())
            .catch(err => {
                setRow('sw', 'ERR', false);
                L('err', 'SW', 'Registration failed: ' + err.message);
            });
    } else {
        setRow('sw', 'unsupported', false);
        L('err', 'SW', 'Service Workers not supported');
    }

    let dc = null;
    let ws = null;
    const inflight = new Map();
    const wsStreams = new Map();

    function startP2P() {
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        let signalUrl = proto + '//' + location.host + '/_signal';

        const urlParam = new URLSearchParams(location.search).get('target');
        if (urlParam) {
            signalUrl += '?target=' + encodeURIComponent(urlParam);
            L('info', 'TARGET', urlParam);
        } else if (cfg.target) {
            signalUrl += '?target=' + encodeURIComponent(cfg.target);
            L('info', 'TARGET', cfg.target + ' (from config)');
        } else {
            L('warn', 'TARGET', 'No target specified, using host routing');
        }

        // --- WebSocket Signaling ---
        setStatus('Connecting…');
        L('info', 'SIGNAL', 'Connecting to ' + signalUrl);
        ws = new WebSocket(signalUrl);

        ws.onopen = () => {
            setRow('signal', 'OK', true);
            L('ok', 'SIGNAL', 'WebSocket connected');
        };
        ws.onclose = (e) => {
            setRow('signal', 'closed', false);
            L('err', 'SIGNAL', 'WebSocket closed (code=' + e.code + ')');
            setStatus('Disconnected', false);
        };
        ws.onerror = () => {
            setRow('signal', 'error', false);
            L('err', 'SIGNAL', 'WebSocket error');
            setStatus('Signaling error', false);
        };

        let myId = null;
        let gatewayId = null;

        // --- WebRTC ---
        L('info', 'WEBRTC', 'Creating RTCPeerConnection…');
        const pc = new RTCPeerConnection({
            iceServers: [{ urls: cfg.stun || 'stun:stun.l.google.com:19302' }]
        });

        pc.onicecandidate = (e) => {
            if (e.candidate) {
                const c = e.candidate;
                L('ice', 'ICE', 'Local candidate: ' + c.candidate.substring(0, 60) + '…');
                sendSignal({ type: 'candidate', to: gatewayId, candidate: c });
            } else {
                L('ice', 'ICE', 'Gathering complete');
            }
        };

        pc.onicegatheringstatechange = () => {
            L('ice', 'ICE', 'Gathering: ' + pc.iceGatheringState);
        };

        pc.onconnectionstatechange = () => {
            const s = pc.connectionState;
            setRow('ice', s, s === 'connected');
            L('ice', 'ICE', 'Connection: ' + s);
            if (s === 'connected') {
                setStatus('WebRTC connected');
            } else if (s === 'failed') {
                setStatus('WebRTC failed', false);
                L('err', 'WEBRTC', 'Connection failed — try different STUN/TURN');
            } else if (s === 'disconnected') {
                setStatus('WebRTC disconnected', false);
            }
        };

        pc.oniceconnectionstatechange = () => {
            L('ice', 'ICE', 'ICE state: ' + pc.iceConnectionState);
        };

        // --- DataChannel ---
        L('info', 'DC', 'Creating DataChannel "http"…');
        dc = pc.createDataChannel('http', { ordered: true });

        dc.onopen = () => {
            setRow('dc', 'OPEN', true);
            setStatus('✅ Ready', true);
            L('ok', 'DC', 'DataChannel opened — tunnel ready');
            const bar = document.getElementById('loading-bar');
            if (bar) { bar.classList.remove('active'); bar.style.width = '100%'; }
            notifySW(true);
        };

        dc.onclose = () => {
            setRow('dc', 'CLOSED', false);
            setStatus('DataChannel closed', false);
            L('err', 'DC', 'DataChannel closed');
            notifySW(false);
        };

        dc.onerror = (e) => {
            L('err', 'DC', 'DataChannel error: ' + (e.error?.message || e));
        };

        dc.onmessage = (event) => {
            let msg;
            try { msg = JSON.parse(event.data); } catch { return; }

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

        function notifySW(open) {
            if (navigator.serviceWorker.controller) {
                navigator.serviceWorker.controller.postMessage({ type: 'dc-state', open });
            }
        }

        // --- SW ↔ DC bridge ---
        navigator.serviceWorker.addEventListener('message', (event) => {
            const msg = event.data || {};
            const port = event.ports && event.ports[0];
            if (!port) return;

            if (msg.type === 'tunnel' && msg.payload) {
                if (dc.readyState !== 'open') {
                    port.postMessage({ type: 'error', error: 'dc not open' });
                    return;
                }
                inflight.set(msg.payload.id, port);
                L('tunnel', 'HTTP', '→ ' + msg.payload.method + ' ' + msg.payload.path + ' id=' + msg.payload.id);
                dc.send(JSON.stringify(msg.payload));
                setTimeout(() => {
                    if (inflight.has(msg.payload.id)) {
                        const p = inflight.get(msg.payload.id);
                        inflight.delete(msg.payload.id);
                        L('err', 'HTTP', 'Timeout id=' + msg.payload.id);
                        p.postMessage({ type: 'error', error: 'timeout' });
                    }
                }, 20000);
            } else if (msg.type === 'ws-tunnel' && msg.payload) {
                if (dc.readyState !== 'open') {
                    port.postMessage({ type: 'error', error: 'dc not open' });
                    return;
                }
                wsStreams.set(msg.payload.id, port);
                L('tunnel', 'WS', '→ Open ' + msg.payload.path + ' id=' + msg.payload.id);
                dc.send(JSON.stringify(msg.payload));
            } else if (msg.type === 'ws-data-send' && msg.payload) {
                if (dc.readyState === 'open') {
                    const size = msg.payload.data ? Math.round(msg.payload.data.length * 3 / 4) : 0;
                    L('tunnel', 'WS', '→ Data ' + size + 'B id=' + msg.payload.id);
                    dc.send(JSON.stringify(msg.payload));
                }
            } else if (msg.type === 'ws-close-send' && msg.payload) {
                if (dc.readyState === 'open') {
                    L('tunnel', 'WS', '→ Close id=' + msg.payload.id);
                    dc.send(JSON.stringify(msg.payload));
                    wsStreams.delete(msg.payload.id);
                }
            }
        });

        // --- Signaling messages ---
        ws.onmessage = async (event) => {
            let msg;
            try { msg = JSON.parse(event.data); } catch { return; }

            if (msg.type === 'join') {
                myId = msg.id;
                L('signal', 'SIGNAL', 'Joined as peer ' + myId);
            } else if (msg.type === 'peer-joined') {
                if (myId && msg.id !== myId) {
                    gatewayId = msg.id;
                    L('signal', 'SIGNAL', 'Gateway peer: ' + gatewayId);
                    L('info', 'WEBRTC', 'Creating SDP offer…');
                    const offer = await pc.createOffer();
                    await pc.setLocalDescription(offer);
                    L('signal', 'SIGNAL', '→ Offer sent');
                    sendSignal({ type: 'offer', to: gatewayId, sdp: offer });
                }
            } else if (msg.type === 'answer') {
                await pc.setRemoteDescription(msg.sdp);
                L('signal', 'SIGNAL', '← Answer received');
            } else if (msg.type === 'candidate') {
                try {
                    await pc.addIceCandidate(msg.candidate);
                    const c = msg.candidate;
                    L('ice', 'ICE', '← Remote candidate: ' + (c.candidate || '').substring(0, 60) + '…');
                } catch (e) {
                    L('err', 'ICE', 'addIceCandidate failed: ' + e.message);
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

    // --- Test button ---
    document.getElementById('btn').addEventListener('click', async () => {
        const out = document.getElementById('out');
        out.textContent = 'fetching…';
        L('tunnel', 'TEST', 'Fetching /…');
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
