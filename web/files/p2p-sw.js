// p2p-sw.js - Service Worker that tunnels fetch() and WebSocket
// connections over a WebRTC DataChannel.
//
// HTTP protocol (unchanged):
//   request:  {type:"request", id, method, path, headers, body}
//   response: {type:"response", id, status, headers, body}
//
// WebSocket protocol (new):
//   ws-open:  {type:"ws-open", id, path, headers}
//   ← ws-open-ok / ws-open-err
//   ws-data:  {type:"ws-data", id, data:<base64>, binary:<bool>}
//   ws-close: {type:"ws-close", id, code, reason}
//   ← ws-closed

'use strict';

let dcReady = false;
let dcClientId = '';
let pageTarget = '';
const readyByClient = new Map();
const targetByClient = new Map();
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => {
    event.waitUntil(self.clients.claim());
});
self.addEventListener('message', (event) => {
    const msg = event.data || {};
    const src = event.source && event.source.id || '';
    console.log('[p2p-sw] msg ' + msg.type + ' open=' + msg.open + ' target=' + msg.target + ' from=' + src);
    if (msg.type === 'dc-state') {
        dcReady = !!msg.open;
        if (src) { dcClientId = src; if (msg.open) readyByClient.set(src, true); else readyByClient.delete(src); }
        console.log('[p2p-sw] dcReady=' + dcReady + ' target=' + msg.target + ' dcClientId=' + dcClientId);
        if (typeof msg.target === 'string' && msg.target) {
            pageTarget = msg.target;
            if (src) targetByClient.set(src, msg.target);
        }
    } else if (msg.type === 'target') {
        if (typeof msg.target === 'string' && msg.target) {
            pageTarget = msg.target;
            if (src) targetByClient.set(src, msg.target);
            console.log('[p2p-sw] pageTarget=' + pageTarget);
        }
    }
    if (msg.type === 'skip-waiting') {
        self.skipWaiting();
    }
});

// Files that are NEVER tunneled — always served directly by the gateway.
// Covers both the current /p2p/* control plane and legacy paths.
const SKIP = new Set([
    '/_signal',
    '/signal',
    '/p2p/signal',
    '/p2p-sw.js',
    '/p2p/sw.js',
    '/client.js',
    '/index.html',
    '/p2p/',
    '/p2p/status',
    '/upstream',
]);

self.addEventListener('fetch', (event) => {
    const req = event.request;
    const url = new URL(req.url);

    if (SKIP.has(url.pathname) || url.pathname === '/upstream/' || url.pathname.startsWith('/upstream/')) {
        return;
    }

    const clientId = event.clientId || event.resultingClientId || '';
    const myTarget = (clientId && targetByClient.get(clientId)) || pageTarget || '';

    if (!dcReady) {
        if (myTarget && url.pathname === '/' && !url.searchParams.has('target')) {
            event.respondWith(new Response(null, {
                status: 302,
                headers: { 'Location': '/?target=' + encodeURIComponent(myTarget) }
            }));
            return;
        }
        return;
    }

    const upgradeHeader = req.headers.get('upgrade');
    if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
        event.respondWith(wsTunnel(req, clientId));
        return;
    }

    event.respondWith(forward(req, clientId));
});

async function forward(req, clientId) {
    const url = new URL(req.url);
    if (!dcReady) {
        console.log('[p2p-sw] wait dcReady for ' + url.pathname);
        for (let i=0; i<15; i++) {
            await new Promise(r=>setTimeout(r,200));
            if (dcReady) break;
        }
        if (!dcReady) {
            console.warn('[p2p-sw] tunnel not ready after wait ' + url.pathname);
            return new Response('Tunnel not ready (no fallback)', { status: 503, statusText: 'Tunnel not ready' });
        }
    }
    console.log('[p2p-sw] forward via tunnel ' + url.pathname + url.search + ' clientId=' + (clientId||'') + ' dcClientId=' + dcClientId);
    const t0 = Date.now();
    try {
        const r = await p2pFetch(req, clientId);
        console.log('[p2p-sw] tunnel ok ' + url.pathname + ' ' + (Date.now()-t0) + 'ms status=' + r.status);
        return r;
    } catch (e) {
        console.warn('[p2p-sw] tunnel failed ' + url.pathname + ' ' + e.message + ' ' + (Date.now()-t0) + 'ms');
        return new Response('Tunnel failed: ' + e.message, { status: 502, statusText: 'Tunnel failed' });
    }
}

async function pickClient(clientId, id) {
    if (dcClientId) {
        try {
            const c = await self.clients.get(dcClientId);
            if (c) {
                console.log('[p2p-sw] pick by dcClientId id=' + id);
                return c;
            } else {
                console.warn('[p2p-sw] dcClientId gone id=' + id);
            }
        } catch(e) { console.warn('[p2p-sw] get dcClientId fail ' + e.message); }
    }
    if (clientId) {
        try {
            const c = await self.clients.get(clientId);
            if (c) {
                console.log('[p2p-sw] pick by event.clientId id=' + id);
                return c;
            }
        } catch(e) {}
    }
    const cls = await self.clients.matchAll({ includeUncontrolled: true, type: 'window' });
    console.log('[p2p-sw] pick fallback clients=' + cls.length + ' id=' + id);
    return cls[0] || null;
}

// wsTunnel bridges a browser-side WebSocket to the upstream via DataChannel.
function wsTunnel(req, clientId) {
    if (!dcReady) {
        return new Response(null, { status: 503, statusText: 'DataChannel not open' });
    }

    const url = new URL(req.url);
    const id = nextID();
    const ch = new MessageChannel();

    const headers = {};
    req.headers.forEach((v, k) => { headers[k] = v; });

    const openFrame = {
        type: 'tunnel',
        payload: {
            type: 'ws-open',
            id,
            path: url.pathname + url.search,
            headers
        }
    };

    return new Promise((resolve) => {
        const timer = setTimeout(() => {
            ch.port1.close();
            resolve(new Response(null, { status: 504, statusText: 'ws tunnel timeout' }));
        }, 10000);

        ch.port1.onmessage = (event) => {
            const msg = event.data || {};
            if (msg.type === 'ws-open-ok') {
                clearTimeout(timer);
                resolve(new Response(null, {
                    status: 101,
                    statusText: 'Switching Protocols',
                    headers: { 'upgrade': 'websocket', 'connection': 'upgrade' }
                }));
            } else if (msg.type === 'ws-open-err') {
                clearTimeout(timer);
                ch.port1.close();
                resolve(new Response(null, { status: 502, statusText: msg.error || 'ws open failed' }));
            }
        };

        pickClient(clientId, 'ws-'+id).then(client => {
                if (!client) {
                    clearTimeout(timer);
                    ch.port1.close();
                    resolve(new Response(null, { status: 503, statusText: 'no client' }));
                    return;
                }
                try { client.postMessage(openFrame, [ch.port2]); } catch(e) {
                    clearTimeout(timer);
                    ch.port1.close();
                    resolve(new Response(null, { status: 500, statusText: e.message }));
                }
            }).catch(err => {
                clearTimeout(timer);
                ch.port1.close();
                resolve(new Response(null, { status: 500, statusText: err.message }));
            });
    });
}

function p2pFetch(req, clientId) {
    const url = new URL(req.url);
    return new Promise((resolve, reject) => {
        const id = nextID();
        const ch = new MessageChannel();
        const t0 = Date.now();
        console.log('[p2p-sw] p2pFetch start id=' + id + ' ' + req.method + ' ' + url.pathname + url.search + ' dcReady=' + dcReady);

        const timer = setTimeout(() => {
            console.warn('[p2p-sw] p2pFetch TIMEOUT id=' + id + ' ' + url.pathname + ' after ' + (Date.now()-t0) + 'ms');
            ch.port1.close();
            reject(new Error('tunnel timeout 8s id=' + id));
        }, 8000);

        ch.port1.onmessage = (event) => {
            const msg = event.data || {};
            console.log('[p2p-sw] p2pFetch onmessage id=' + id + ' type=' + msg.type + ' elapsed=' + (Date.now()-t0) + 'ms');
            if (msg.type === 'response') {
                clearTimeout(timer);
                ch.port1.close();
                console.log('[p2p-sw] p2pFetch resolve id=' + id + ' status=' + msg.status);
                resolve(buildResponse(msg));
            } else if (msg.type === 'error') {
                clearTimeout(timer);
                ch.port1.close();
                console.warn('[p2p-sw] p2pFetch error id=' + id + ' ' + (msg.error||''));
                reject(new Error(msg.error || 'tunnel error'));
            }
        };

        const headers = {};
        req.headers.forEach((v, k) => { headers[k] = v; });

        const send = (bodyB64) => {
            let reqPath = url.pathname;
            if (reqPath.startsWith('/p2p/')) {
                reqPath = reqPath.substring(4);
            } else if (reqPath === '/p2p') {
                reqPath = '/';
            }
            const cleanPath = reqPath + url.search;
            const frame = {
                type: 'tunnel',
                payload: {
                    type: 'request',
                    id,
                    method: req.method,
                    path: cleanPath,
                    headers,
                    body: bodyB64 || ''
                }
            };
            console.log('[p2p-sw] post to client id=' + id + ' ' + frame.payload.method + ' ' + cleanPath + ' clientId=' + (clientId||''));
            pickClient(clientId, id).then(client => {
                    if (!client) {
                        console.warn('[p2p-sw] no client for id=' + id);
                        ch.port1.postMessage({ type: 'error', error: 'no client' });
                        return;
                    }
                    try {
                        client.postMessage(frame, [ch.port2]);
                        console.log('[p2p-sw] posted to client id=' + id + ' client=' + client.id);
                    } catch(e) {
                        console.warn('[p2p-sw] postMessage fail id=' + id + ' ' + e.message);
                        ch.port1.postMessage({ type: 'error', error: e.message });
                    }
                })
                .catch(err => {
                    console.warn('[p2p-sw] pickClient err id=' + id + ' ' + err.message);
                    ch.port1.postMessage({ type: 'error', error: err.message });
                });
        };

        if (req.method === 'GET' || req.method === 'HEAD') {
            send('');
        } else {
            req.clone().arrayBuffer().then(buf => {
                send(arrayBufferToBase64(buf));
            }).catch(err => {
                clearTimeout(timer);
                reject(err);
            });
        }
    });
}

function arrayBufferToBase64(buf) {
    const bytes = new Uint8Array(buf);
    let bin = '';
    for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return btoa(bin);
}

function base64ToArrayBuffer(b64) {
    const bin = atob(b64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return bytes.buffer;
}

let counter = 0;
function nextID() {
    counter += 1;
    return Date.now().toString(36) + '-' + counter.toString(36);
}

const STATUS_TEXT = {
    200: 'OK', 201: 'Created', 204: 'No Content',
    301: 'Moved Permanently', 302: 'Found', 303: 'See Other',
    307: 'Temporary Redirect', 308: 'Permanent Redirect',
    400: 'Bad Request', 401: 'Unauthorized', 403: 'Forbidden',
    404: 'Not Found', 500: 'Internal Server Error', 502: 'Bad Gateway',
    503: 'Service Unavailable'
};

function buildResponse(msg) {
    const h = new Headers();
    for (const k of Object.keys(msg.headers || {})) {
        try { h.set(k, msg.headers[k]); } catch (e) { /* skip */ }
    }
    let body = null;
    if (msg.body) {
        try {
            body = base64ToArrayBuffer(msg.body);
        } catch(e) {
            console.error('[p2p-sw] base64 decode failed:', e);
        }
    }
    console.log('[p2p-sw] buildResponse status=' + msg.status + ' bodyLen=' + (body ? body.byteLength : 0));
    return new Response(body, {
        status: msg.status || 200,
        statusText: STATUS_TEXT[msg.status] || '',
        headers: h
    });
}
