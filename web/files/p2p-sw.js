'use strict';
const SW_VER='v23-noblock';
function normalizeTarget(raw){
    if(!raw) return '';
    raw=String(raw).trim(); if(!raw) return '';
    try{
        let s=raw.includes('://')?raw:'http://'+raw;
        const u=new URL(s);
        let scheme=u.protocol.slice(0,-1).toLowerCase();
        if(scheme!=='http'&&scheme!=='https') scheme='http';
        let host=u.hostname.toLowerCase(); if(!host) return '';
        let port=u.port;
        if((scheme==='http'&&port==='80')||(scheme==='https'&&port==='443')) port='';
        return scheme+'://'+host+(port?':'+port:'');
    }catch(e){ return ''; }
}

let dcReady = false;
let dcClientId = '';
let pageTarget = '';
const readyByClient = new Map();
const targetByClient = new Map();
const readyHostByTarget = new Map();
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => {
    event.waitUntil(self.clients.claim());
});
self.addEventListener('message', (event) => {
    const msg = event.data || {};
    const src = event.source && event.source.id || '';
    console.log('[p2p-sw] msg', msg.type, 'open=', msg.open, 'target=', msg.target, 'src=', src, 'readyHost=', JSON.stringify([...readyHostByTarget.keys()]));
    if (msg.type === 'dc-state') {
        dcReady = !!msg.open;
        if (src) {
            dcClientId = src;
            const tRaw = typeof msg.target === 'string' ? msg.target : '';
            const t = tRaw ? (normalizeTarget(tRaw) || tRaw.trim()) : '';
            if (msg.open && t) {
                readyByClient.set(src, true);
                readyHostByTarget.set(t, src);
                console.log('[p2p-sw] dc-state set', t, '->', src, 'keys', [...readyHostByTarget.keys()]);
            } else if (!msg.open && t) {
                if (readyHostByTarget.get(t) === src) {
                    readyHostByTarget.delete(t);
                    console.log('[p2p-sw] dc-state clear', t, 'keys', [...readyHostByTarget.keys()]);
                }
                let has = false;
                for (const v of readyHostByTarget.values()) if (v === src) has = true;
                if (!has) readyByClient.delete(src);
            } else if (!msg.open && !t) {
                for (const [k, v] of [...readyHostByTarget.entries()]) if (v === src) readyHostByTarget.delete(k);
                readyByClient.delete(src);
            }
        }
        if (typeof msg.target === 'string' && msg.target) {
            const pt = normalizeTarget(msg.target) || msg.target.trim();
            pageTarget = pt;
            console.log('[p2p-sw] dc-state pageTarget', pt);
        }
    } else if (msg.type === 'target') {
        if (typeof msg.target === 'string' && msg.target) {
            const pt = normalizeTarget(msg.target) || msg.target.trim();
            pageTarget = pt;
            if (src) {
                targetByClient.set(src, pt);
                console.log('[p2p-sw] target', pt, 'keys', [...readyHostByTarget.keys()]);
            }
        }
    }
    if (msg.type === 'skip-waiting') self.skipWaiting();
    if (msg.type === 'count-p2p') {
        self.clients.matchAll({includeUncontrolled:true, type:'window'}).then(cls=>{
            const c = cls.filter(cl=>cl.url.includes('/p2p/')).length;
            if(event.ports && event.ports[0]) event.ports[0].postMessage({type:'count-p2p', count:c});
        });
    }
});

const SKIP = new Set([
    '/_signal','/signal','/p2p/signal','/p2p-sw.js','/p2p/sw.js','/client.js','/index.html','/p2p/','/p2p/status','/upstream',
]);

self.addEventListener('fetch', (event) => {
    const req = event.request;
    const url = new URL(req.url);
    if (SKIP.has(url.pathname) || url.pathname === '/upstream/' || url.pathname.startsWith('/upstream/')) return;
    const clientId = event.clientId || event.resultingClientId || '';
    const rawReqTarget = (url.searchParams.get('target') || '').trim();
    const reqTarget = rawReqTarget ? (normalizeTarget(rawReqTarget)||rawReqTarget) : '';
    if (reqTarget && clientId) {
        targetByClient.set(clientId, reqTarget);
        if (readyByClient.has(clientId)) {
            if (!readyHostByTarget.has(reqTarget)) readyHostByTarget.set(reqTarget, clientId);
            console.log('[p2p-sw] fetch set', reqTarget, 'keys', [...readyHostByTarget.keys()]);
        }
    }
    let effectiveTarget = reqTarget;
    if (!effectiveTarget && clientId) effectiveTarget = normalizeTarget(targetByClient.get(clientId)||'') || (targetByClient.get(clientId) || '').trim();
    if (!effectiveTarget) {
        const ref = req.headers.get('referer') || req.headers.get('Referer') || req.referrer || '';
        if (ref) {
            try {
                const ru = new URL(ref, location.origin);
                const rt = (ru.searchParams.get('target') || '').trim();
                const nk = rt ? (normalizeTarget(rt)||rt) : '';
                if (nk) {
                    effectiveTarget = nk;
                    if (clientId && !targetByClient.has(clientId)) targetByClient.set(clientId, nk);
                }
            } catch(e) {}
        }
    }
    if (!effectiveTarget && clientId) {
        event.respondWith((async ()=>{
            try{
                const cl = await self.clients.get(clientId);
                if(cl){
                    const cu = new URL(cl.url, location.origin);
                    const rt2 = (cu.searchParams.get('target')||'').trim();
                    const nk2 = rt2 ? (normalizeTarget(rt2)||rt2) : '';
                    if(nk2){
                        effectiveTarget = nk2;
                        targetByClient.set(clientId, nk2);
                    }
                }
            }catch(e){}
            if(!effectiveTarget && readyHostByTarget.size === 1){
                const onlyKey = [...readyHostByTarget.keys()][0];
                const onlyHost = readyHostByTarget.get(onlyKey);
                if(onlyHost && readyByClient.has(onlyHost)) effectiveTarget = onlyKey;
            }
            const hostId2 = effectiveTarget ? (readyHostByTarget.get(effectiveTarget)||'') : '';
            const tunnelReady2 = !!hostId2 && readyByClient.has(hostId2);
            console.log('[p2p-sw] fetch', url.pathname, 'clientId', clientId, 'reqTarget', reqTarget, 'eff', effectiveTarget, 'hostId', hostId2, 'ready', tunnelReady2, 'keys', [...readyHostByTarget.keys()]);
            if(req.mode === 'navigate' && effectiveTarget && !reqTarget && url.pathname !== '/p2p/' && url.pathname !== '/p2p'){
                const redirectUrl = new URL(url.toString());
                redirectUrl.searchParams.set('target', effectiveTarget);
                return Response.redirect(redirectUrl.toString(), 302);
            }
            if(!tunnelReady2){
                if(effectiveTarget){
                    if(req.mode === 'navigate' && url.pathname !== '/p2p/' && url.pathname !== '/p2p' && !url.pathname.startsWith('/p2p/')){
                        const hid = await new Promise(res=>{
                            let tries=0; const tick=()=>{
                                const h=readyHostByTarget.get(effectiveTarget)||'';
                                if(h && readyByClient.has(h)) return res(h);
                                if(++tries>=20) return res('');
                                setTimeout(tick,150);
                            }; tick();
                        });
                        if(hid && readyByClient.has(hid)){
                            const uh=req.headers.get('upgrade');
                            if(uh && uh.toLowerCase()==='websocket') return wsTunnel(req, hid, effectiveTarget);
                            return forward(req, hid, effectiveTarget);
                        }
                            return new Response('Tunnel not ready for target '+effectiveTarget+' — open /p2p/?target='+encodeURIComponent(effectiveTarget)+' and click Connect', {status:503,statusText:'Tunnel not ready'});
                    }
                    const hid2 = await new Promise(res=>{
                        let tries=0; const tick=()=>{
                            const h=readyHostByTarget.get(effectiveTarget)||'';
                            if(h && readyByClient.has(h)) return res(h);
                            if(++tries>=20) return res('');
                            setTimeout(tick,150);
                        }; tick();
                    });
                    if(hid2 && readyByClient.has(hid2)){
                        const uh2=req.headers.get('upgrade');
                        if(uh2 && uh2.toLowerCase()==='websocket') return wsTunnel(req, hid2, effectiveTarget);
                        return forward(req, hid2, effectiveTarget);
                    }
                    return new Response('Tunnel not ready for target '+effectiveTarget+' — open /p2p/?target='+encodeURIComponent(effectiveTarget), {status:503,statusText:'Tunnel not ready'});
                }
                return fetch(req);
            }
            const uh3=req.headers.get('upgrade');
            if(uh3 && uh3.toLowerCase()==='websocket') return wsTunnel(req, hostId2, effectiveTarget);
            return forward(req, hostId2, effectiveTarget);
        })());
        return;
    }
    if (!effectiveTarget && readyHostByTarget.size === 1) {
        const onlyKey = [...readyHostByTarget.keys()][0];
        const onlyHost = readyHostByTarget.get(onlyKey);
        if (onlyHost && readyByClient.has(onlyHost)) effectiveTarget = onlyKey;
    }
    const hostId = effectiveTarget ? (readyHostByTarget.get(effectiveTarget) || '') : '';
    const tunnelReady = !!hostId && readyByClient.has(hostId);
    console.log('[p2p-sw] fetch', url.pathname, 'clientId', clientId, 'reqTarget', reqTarget, 'eff', effectiveTarget, 'hostId', hostId, 'ready', tunnelReady, 'keys', [...readyHostByTarget.keys()]);
    if (req.mode === 'navigate' && effectiveTarget && !reqTarget && url.pathname !== '/p2p/' && url.pathname !== '/p2p') {
        const redirectUrl = new URL(url.toString());
        redirectUrl.searchParams.set('target', effectiveTarget);
        event.respondWith(Response.redirect(redirectUrl.toString(), 302));
        return;
    }
    if (!tunnelReady) {
        if (effectiveTarget) {
            if (req.mode === 'navigate' && url.pathname !== '/p2p/' && url.pathname !== '/p2p' && !url.pathname.startsWith('/p2p/')) {
                const waitReady = new Promise(resolve => {
                    let tries = 0;
                    const tick = () => {
                        const hid = readyHostByTarget.get(effectiveTarget) || '';
                        if (hid && readyByClient.has(hid)) return resolve(hid);
                        if (++tries >= 20) return resolve('');
                        setTimeout(tick, 150);
                    };
                    tick();
                });
                event.respondWith(waitReady.then(hid => {
                    if (hid && readyByClient.has(hid)) {
                        const upgradeHeader = req.headers.get('upgrade');
                        if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') return wsTunnel(req, hid, effectiveTarget);
                        return forward(req, hid, effectiveTarget);
                    }
                    return new Response('Tunnel not ready for target '+effectiveTarget+' — open /p2p/?target='+encodeURIComponent(effectiveTarget)+' and click Connect', {status:503,statusText:'Tunnel not ready'});
                }));
                return;
            }
            const waitReady = new Promise(resolve => {
                let tries = 0;
                const tick = () => {
                    const hid = readyHostByTarget.get(effectiveTarget) || '';
                    if (hid && readyByClient.has(hid)) return resolve(hid);
                    if (++tries >= 20) return resolve('');
                    setTimeout(tick, 150);
                };
                tick();
            });
            event.respondWith(waitReady.then(hid => {
                if (hid && readyByClient.has(hid)) {
                    const upgradeHeader = req.headers.get('upgrade');
                    if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') return wsTunnel(req, hid, effectiveTarget);
                    return forward(req, hid, effectiveTarget);
                }
                return new Response('Tunnel not ready for target ' + effectiveTarget + ' — open /p2p/?target=' + encodeURIComponent(effectiveTarget), { status: 503, statusText: 'Tunnel not ready' });
            }));
            return;
        }
        return;
    }
    const upgradeHeader = req.headers.get('upgrade');
    if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
        event.respondWith(wsTunnel(req, hostId, effectiveTarget));
        return;
    }
    event.respondWith(forward(req, hostId, effectiveTarget));
});

async function forward(req, hostId, effectiveTarget) {
    const url = new URL(req.url);
    if (!hostId || !readyByClient.has(hostId)) {
        for (let i=0;i<15;i++) {
            await new Promise(r=>setTimeout(r,200));
            if (hostId && readyByClient.has(hostId)) break;
        }
        if (!hostId || !readyByClient.has(hostId)) {
            return new Response('Tunnel not ready for target ' + (effectiveTarget||''), { status: 503, statusText: 'Tunnel not ready' });
        }
    }
    try {
        const r = await p2pFetch(req, hostId, effectiveTarget);
        return r;
    } catch (e) {
        return new Response('Tunnel failed: ' + e.message, { status: 502, statusText: 'Tunnel failed' });
    }
}

async function pickClient(clientId, id) {
    if (clientId) {
        try { const c = await self.clients.get(clientId); if (c) return c; } catch(e) {}
    }
    if (dcClientId) {
        try { const c = await self.clients.get(dcClientId); if (c) return c; } catch(e) {}
    }
    const cls = await self.clients.matchAll({ includeUncontrolled: true, type: 'window' });
    return cls[0] || null;
}

function wsTunnel(req, hostId, effectiveTarget) {
    if (!hostId || !readyByClient.has(hostId)) {
        return new Response(null, { status: 503, statusText: 'Tunnel not ready for target ' + (effectiveTarget||'') });
    }
    const url = new URL(req.url);
    const id = nextID();
    const ch = new MessageChannel();
    const headers = {};
    req.headers.forEach((v,k)=>{ headers[k]=v; });
    const openFrame = { type: 'tunnel', payload: { type: 'ws-open', id, path: url.pathname + url.search, headers, target: effectiveTarget } };
    return new Promise((resolve)=>{
        const timer=setTimeout(()=>{ ch.port1.close(); resolve(new Response(null,{status:504,statusText:'ws tunnel timeout'})); },10000);
        ch.port1.onmessage=(event)=>{
            const msg=event.data||{};
            if(msg.type==='ws-open-ok'){ clearTimeout(timer); resolve(new Response(null,{status:101,statusText:'Switching Protocols',headers:{'upgrade':'websocket','connection':'upgrade'}})); }
            else if(msg.type==='ws-open-err'){ clearTimeout(timer); ch.port1.close(); resolve(new Response(null,{status:502,statusText:msg.error||'ws open failed'})); }
        };
        pickClient(hostId,'ws-'+id).then(client=>{
            if(!client){ clearTimeout(timer); ch.port1.close(); resolve(new Response(null,{status:503,statusText:'no client'})); return; }
            try{ client.postMessage(openFrame,[ch.port2]); }catch(e){ clearTimeout(timer); ch.port1.close(); resolve(new Response(null,{status:500,statusText:e.message})); }
        }).catch(err=>{ clearTimeout(timer); ch.port1.close(); resolve(new Response(null,{status:500,statusText:err.message})); });
    });
}

function p2pFetch(req, clientId, effectiveTarget) {
    const url = new URL(req.url);
    return new Promise((resolve, reject)=>{
        const id=nextID();
        const ch=new MessageChannel();
        const timer=setTimeout(()=>{ ch.port1.close(); reject(new Error('tunnel timeout 30s id='+id)); },30000);
        ch.port1.onmessage=(event)=>{
            const msg=event.data||{};
            if(msg.type==='response'){ clearTimeout(timer); ch.port1.close(); resolve(buildResponse(msg)); }
            else if(msg.type==='error'){ clearTimeout(timer); ch.port1.close(); reject(new Error(msg.error||'tunnel error')); }
        };
        const headers={};
        req.headers.forEach((v,k)=>{ headers[k]=v; });
        const send=(bodyB64)=>{
            let reqPath=url.pathname;
            if(reqPath.startsWith('/p2p/')) reqPath=reqPath.substring(4);
            else if(reqPath==='/p2p') reqPath='/';
            const cleanPath=reqPath+url.search;
            const frame={ type:'tunnel', payload:{ type:'request', id, method:req.method, path:cleanPath, headers, body:bodyB64||'', target: effectiveTarget } };
            pickClient(clientId,id).then(client=>{
                if(!client){ ch.port1.postMessage({type:'error',error:'no client'}); return; }
                try{ client.postMessage(frame,[ch.port2]); }catch(e){ ch.port1.postMessage({type:'error',error:e.message}); }
            }).catch(err=>{ ch.port1.postMessage({type:'error',error:err.message}); });
        };
        if(req.method==='GET'||req.method==='HEAD') send('');
        else req.clone().arrayBuffer().then(buf=>{ send(arrayBufferToBase64(buf)); }).catch(err=>{ clearTimeout(timer); reject(err); });
    });
}

function arrayBufferToBase64(buf){ const bytes=new Uint8Array(buf); let bin=''; for(let i=0;i<bytes.length;i++) bin+=String.fromCharCode(bytes[i]); return btoa(bin); }
function base64ToArrayBuffer(b64){ const bin=atob(b64); const bytes=new Uint8Array(bin.length); for(let i=0;i<bin.length;i++) bytes[i]=bin.charCodeAt(i); return bytes.buffer; }
let counter=0;
function nextID(){ counter+=1; return Date.now().toString(36)+'-'+counter.toString(36); }
const STATUS_TEXT={200:'OK',201:'Created',204:'No Content',301:'Moved Permanently',302:'Found',303:'See Other',307:'Temporary Redirect',308:'Permanent Redirect',400:'Bad Request',401:'Unauthorized',403:'Forbidden',404:'Not Found',500:'Internal Server Error',502:'Bad Gateway',503:'Service Unavailable'};
function buildResponse(msg){
    const h=new Headers();
    for(const k of Object.keys(msg.headers||{})){ try{ h.set(k,msg.headers[k]); }catch(e){} }
    let body=null;
    if(msg.body){ try{ body=base64ToArrayBuffer(msg.body); }catch(e){} }
    return new Response(body,{ status:msg.status||200, statusText:STATUS_TEXT[msg.status]||'', headers:h });
}
