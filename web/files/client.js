// client.js - bootstraps the WebRTC P2P tunnel from the browser side.
//
// Connects to /_signal?target=<url> to specify the upstream.
// Supports HTTP and WebSocket tunneling over DataChannel.

(function () {
    'use strict';

    const cfg = window.__GATEWAY_CONFIG__ || {};
    document.getElementById('target').textContent = cfg.target || '(unknown)';
    document.getElementById('host').textContent = cfg.host || location.host;
    document.getElementById('stun').textContent = cfg.stun || '(none)';

    function setRow(id, text, ok) {
        const el = document.getElementById(id);
        el.textContent = text;
        el.className = ok ? 'val ok' : 'val bad';
    }

    function setStatus(text, ok) {
        const el = document.getElementById('status');
        el.textContent = text;
        el.className = ok ? 'val ok' : 'val bad';
    }

    // 1. Register the Service Worker.
    if ('serviceWorker' in navigator) {
        navigator.serviceWorker.register('/p2p-sw.js', { scope: '/' })
            .then(reg => {
                setRow('sw', 'OK', true);
                return navigator.serviceWorker.ready;
            })
            .then(() => startP2P())
            .catch(err => setRow('sw', 'ERR: ' + err.message, false));
    } else {
        setRow('sw', 'unsupported', false);
    }

    let dc = null;
    let ws = null;
    const inflight = new Map(); // id → MessagePort
    const wsStreams = new Map(); // id → MessagePort (for ws-data routing)

    function startP2P() {
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        // Pass target as query param if set.
        let signalUrl = proto + '//' + location.host + '/_signal';
        const urlParam = new URLSearchParams(location.search).get('target');
        if (urlParam) {
            signalUrl += '?target=' + encodeURIComponent(urlParam);
        } else if (cfg.target) {
            signalUrl += '?target=' + encodeURIComponent(cfg.target);
        }
        ws = new WebSocket(signalUrl);

        ws.onopen = () => setRow('signal', 'OK', true);
        ws.onclose = () => {
            setRow('signal', 'closed', false);
            setStatus('Disconnected', false);
        };
        ws.onerror = () => setRow('signal', 'error', false);

        let myId = null;
        let gatewayId = null;

        const pc = new RTCPeerConnection({
            iceServers: [{ urls: cfg.stun || 'stun:stun.l.google.com:19302' }]
        });

        pc.onicecandidate = (e) => {
            if (e.candidate) {
                sendSignal({ type: 'candidate', to: gatewayId, candidate: e.candidate });
            }
        };

        pc.onconnectionstatechange = () => {
            setRow('ice', pc.connectionState, pc.connectionState === 'connected');
        };

        dc = pc.createDataChannel('http', { ordered: true });

        dc.onopen = () => {
            setRow('dc', 'OPEN', true);
            setStatus('Connected', true);
            notifySW(true);
        };

        dc.onclose = () => {
            setRow('dc', 'CLOSED', false);
            setStatus('DataChannel closed', false);
            notifySW(false);
        };

        dc.onmessage = (event) => {
            let msg;
            try { msg = JSON.parse(event.data); } catch { return; }

            if (msg.type === 'response') {
                // HTTP response
                const port = inflight.get(msg.id);
                if (!port) return;
                inflight.delete(msg.id);
                port.postMessage({ type: 'response', ...msg });
            } else if (msg.type === 'ws-open-ok' || msg.type === 'ws-open-err') {
                // WebSocket open result
                const port = wsStreams.get(msg.id);
                if (!port) return;
                port.postMessage(msg);
                if (msg.type === 'ws-open-err') {
                    wsStreams.delete(msg.id);
                }
            } else if (msg.type === 'ws-data' || msg.type === 'ws-closed') {
                // WebSocket data/close
                const port = wsStreams.get(msg.id);
                if (!port) return;
                port.postMessage(msg);
                if (msg.type === 'ws-closed') {
                    wsStreams.delete(msg.id);
                }
            }
        };

        function notifySW(open) {
            if (navigator.serviceWorker.controller) {
                navigator.serviceWorker.controller.postMessage({ type: 'dc-state', open });
            }
        }

        // Receive messages from the SW.
        navigator.serviceWorker.addEventListener('message', (event) => {
            const msg = event.data || {};
            const port = event.ports && event.ports[0];
            if (!port) return;

            if (msg.type === 'tunnel' && msg.payload) {
                // HTTP tunnel
                if (dc.readyState !== 'open') {
                    port.postMessage({ type: 'error', error: 'datachannel not open' });
                    return;
                }
                inflight.set(msg.payload.id, port);
                dc.send(JSON.stringify(msg.payload));
                setTimeout(() => {
                    if (inflight.has(msg.payload.id)) {
                        const p = inflight.get(msg.payload.id);
                        inflight.delete(msg.payload.id);
                        p.postMessage({ type: 'error', error: 'timeout' });
                    }
                }, 20000);
            } else if (msg.type === 'ws-tunnel' && msg.payload) {
                // WebSocket tunnel
                if (dc.readyState !== 'open') {
                    port.postMessage({ type: 'error', error: 'datachannel not open' });
                    return;
                }
                wsStreams.set(msg.payload.id, port);
                dc.send(JSON.stringify(msg.payload));
            } else if (msg.type === 'ws-data-send' && msg.payload) {
                // Forward ws-data from SW to DataChannel
                if (dc.readyState === 'open') {
                    dc.send(JSON.stringify(msg.payload));
                }
            } else if (msg.type === 'ws-close-send' && msg.payload) {
                // Forward ws-close from SW to DataChannel
                if (dc.readyState === 'open') {
                    dc.send(JSON.stringify(msg.payload));
                    wsStreams.delete(msg.payload.id);
                }
            }
        });

        ws.onmessage = async (event) => {
            let msg;
            try { msg = JSON.parse(event.data); } catch { return; }

            if (msg.type === 'join') {
                myId = msg.id;
            } else if (msg.type === 'peer-joined') {
                if (myId && msg.id !== myId) {
                    gatewayId = msg.id;
                    const offer = await pc.createOffer();
                    await pc.setLocalDescription(offer);
                    sendSignal({ type: 'offer', to: gatewayId, sdp: offer });
                }
            } else if (msg.type === 'answer') {
                await pc.setRemoteDescription(msg.sdp);
            } else if (msg.type === 'candidate') {
                try { await pc.addIceCandidate(msg.candidate); }
                catch (e) { console.warn('addIceCandidate failed', e); }
            }
        };

        function sendSignal(obj) {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(obj));
            }
        }

        window.__p2p = { dc, pc };
    }

    // Test button
    document.getElementById('btn').addEventListener('click', async () => {
        const out = document.getElementById('out');
        out.textContent = 'fetching…';
        try {
            const r = await fetch('/?test=' + Date.now());
            const text = await r.text();
            out.textContent = 'HTTP ' + r.status + ' (' + r.headers.get('content-type') + ')\n\n' +
                text.slice(0, 4000);
        } catch (e) {
            out.textContent = 'fetch error: ' + e.message;
        }
    });
})();
