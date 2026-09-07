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
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => {
    // Take control of all clients immediately.
    event.waitUntil(self.clients.claim());
});
self.addEventListener('message', (event) => {
    const msg = event.data || {};
    if (msg.type === 'dc-state') {
        dcReady = !!msg.open;
    }
    if (msg.type === 'skip-waiting') {
        self.skipWaiting();
    }
});

// Files that are NEVER tunneled — always served directly by the gateway.
const SKIP = new Set([
    '/', '/index.html',
    '/_signal',
    '/p2p-sw.js',
    '/client.js',
]);

self.addEventListener('fetch', (event) => {
    const req = event.request;
    const url = new URL(req.url);

    // Never tunnel gateway's own files.
    if (SKIP.has(url.pathname)) {
        return;
    }

    // If DataChannel is not open, fall back to direct fetch immediately.
    if (!dcReady) {
        return; // browser default — no interception
    }

    // Check if this is a WebSocket upgrade request
    const upgradeHeader = req.headers.get('upgrade');
    if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
        event.respondWith(wsTunnel(req));
        return;
    }

    event.respondWith(forward(req));
});

async function forward(req) {
    if (dcReady) {
        try {
            return await p2pFetch(req);
        } catch (e) {
            console.warn('[p2p-sw] tunnel failed, falling back', e);
        }
    }
    // Fallback: regular fetch against the gateway's reverse-proxy path.
    const directURL = new URL('/upstream' + new URL(req.url).pathname + new URL(req.url).search, location.origin);
    return fetch(directURL.toString(), {
        method: req.method,
        headers: req.headers,
        body: req.method === 'GET' || req.method === 'HEAD' ? undefined : await req.clone().arrayBuffer(),
        redirect: 'manual'
    });
}

// wsTunnel bridges a browser-side WebSocket to the upstream via DataChannel.
function wsTunnel(req) {
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

        self.clients.matchAll({ includeUncontrolled: true, type: 'window' })
            .then(cls => {
                if (!cls.length) {
                    clearTimeout(timer);
                    ch.port1.close();
                    resolve(new Response(null, { status: 503, statusText: 'no client' }));
                    return;
                }
                cls[0].postMessage(openFrame, [ch.port2]);
            })
            .catch(err => {
                clearTimeout(timer);
                ch.port1.close();
                resolve(new Response(null, { status: 500, statusText: err.message }));
            });
    });
}

function p2pFetch(req) {
    const url = new URL(req.url);
    return new Promise((resolve, reject) => {
        const id = nextID();
        const ch = new MessageChannel();

        const timer = setTimeout(() => {
            ch.port1.close();
            reject(new Error('tunnel timeout'));
        }, 20000);

        ch.port1.onmessage = (event) => {
            const msg = event.data || {};
            if (msg.type === 'response') {
                clearTimeout(timer);
                ch.port1.close();
                resolve(buildResponse(msg));
            } else if (msg.type === 'error') {
                clearTimeout(timer);
                ch.port1.close();
                reject(new Error(msg.error || 'tunnel error'));
            }
        };

        const headers = {};
        req.headers.forEach((v, k) => { headers[k] = v; });

        const send = (bodyB64) => {
            const frame = {
                type: 'tunnel',
                payload: {
                    type: 'request',
                    id,
                    method: req.method,
                    path: url.pathname + url.search,
                    headers,
                    body: bodyB64 || ''
                }
            };
            self.clients.matchAll({ includeUncontrolled: true, type: 'window' })
                .then(cls => {
                    if (!cls.length) {
                        ch.port1.postMessage({ type: 'error', error: 'no client' });
                        return;
                    }
                    cls[0].postMessage(frame, [ch.port2]);
                })
                .catch(err => ch.port1.postMessage({ type: 'error', error: err.message }));
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
    const body = msg.body ? base64ToArrayBuffer(msg.body) : null;
    return new Response(body, {
        status: msg.status || 200,
        statusText: STATUS_TEXT[msg.status] || '',
        headers: h
    });
}
