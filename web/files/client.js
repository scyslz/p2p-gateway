// client.js - P2P tunnel: Gateway creates offer, browser answers.
(function () {
    'use strict';
    const cfg = window.__GATEWAY_CONFIG__ || {};
    const L = window.log || function(){};
    let pendingTarget = '';
    let pendingNavWindow=null; let pendingNavTarget='';
    const navigatedKeys=new Set();
    function normalizeTarget(raw){
        if(!raw) return '';
        raw=String(raw).trim(); if(!raw) return '';
        try{
            let hasScheme = raw.includes('://');
            let urlStr = hasScheme ? raw : 'http://' + raw;
            const u = new URL(urlStr);
            let scheme = u.protocol.slice(0,-1).toLowerCase();
            if(scheme!=='http' && scheme!=='https') scheme='http';
            let host = u.hostname.toLowerCase(); if(!host) return '';
            let port = u.port;
            if((scheme==='http' && port==='80') || (scheme==='https' && port==='443')) port='';
            return scheme + '://' + host + (port ? ':'+port : '');
        }catch(e){ return ''; }
    }
    function getUrlTargetRaw(){ try{ return new URLSearchParams(location.search).get('target') || ''; }catch(e){ return ''; } }
    function getUrlTarget(){ const r=getUrlTargetRaw(); return r ? normalizeTarget(r) : ''; }
    function getNextParam(){ try{ return new URLSearchParams(location.search).get('next') || ''; }catch(e){ return ''; } }
    function resolvePageTarget(){
        if(pendingTarget){ const t=pendingTarget; pendingTarget=''; return normalizeTarget(t)||t; }
        const urlT=getUrlTarget(); if(urlT) return urlT;
        try{ if(new URLSearchParams(location.search).has('target')) return ''; }catch(e){}
        return '';
    }
    const myTabId = (()=>{ try{ if(window.crypto&&crypto.randomUUID) return crypto.randomUUID(); }catch(e){} return Math.random().toString(36).slice(2)+Date.now().toString(36); })();
    const LS_TUNNELS = 'p2p-tunnels';
    const LS_KEEPERS = 'p2p-keepers';
    const LS_HIST = 'p2p-targets';
    const KEEPER_TTL = 15000;
    const KEEPER_INTERVAL = 5000;
    const TUNNEL_TTL = 30000;
    const TOUCH_INTERVAL = 10000;
    function loadKeepers(){ try{ const v=localStorage.getItem(LS_KEEPERS); return v?JSON.parse(v):{}; }catch(e){ return {}; } }
    function saveKeepers(m){ try{ localStorage.setItem(LS_KEEPERS, JSON.stringify(m)); }catch(e){} }
    function touchKeeper(){
        try{
            let m=loadKeepers();
            m[myTabId]=Date.now();
            let now=Date.now();
            for(let k in m){ if(now - m[k] > KEEPER_TTL) delete m[k]; }
            saveKeepers(m);
        }catch(e){}
    }
    function hasOtherP2PLocal(){
        try{
            let m=loadKeepers();
            let now=Date.now();
            for(let k in m){ if(k!==myTabId && now - m[k] < KEEPER_TTL) return true; }
            return false;
        }catch(e){ return false; }
    }
    function removeKeeper(){
        try{ let m=loadKeepers(); delete m[myTabId]; saveKeepers(m); }catch(e){}
    }
    touchKeeper();
    setInterval(touchKeeper, KEEPER_INTERVAL);
    window.addEventListener('pagehide', removeKeeper);
    window.addEventListener('beforeunload', removeKeeper);
    document.addEventListener('visibilitychange', ()=>{ if(document.visibilityState==='visible') touchKeeper(); });
    function loadTunnels(){ try{ const v=localStorage.getItem(LS_TUNNELS); const o=v?JSON.parse(v):{}; return o&&typeof o==='object'?o:{}; }catch(e){ return {}; } }
    function saveTunnels(o){ try{ localStorage.setItem(LS_TUNNELS, JSON.stringify(o)); }catch(e){} }
    function isOwnerAlive(tabId){
        if(!tabId) return false;
        try{
            const m=loadKeepers();
            const ts=m[tabId];
            return !!(ts && Date.now()-ts < KEEPER_TTL);
        }catch(e){ return false; }
    }
     function isTunnelAlive(key){
        const k=normalizeTarget(key)||key; if(!k) return false;
        try{
            const ent=tunnels.get(k);
            if(ent && ent.dc && ent.dc.readyState==='open') return true;
            const o=loadTunnels(); const e=o[k];
            if(!e) return false;
            if(!e.ready) return false;
            if(Date.now()-e.ts >= TUNNEL_TTL) return false;
            if(e.ownerTabId && e.ownerTabId!==myTabId && !isOwnerAlive(e.ownerTabId)) return false;
            return true;
        }catch(ex){ return false; }
    }
    function maybeTakeover(key){
        const k=normalizeTarget(key)||key; if(!k) return;
        try{
            let o=loadTunnels(); const cur=o[k];
            if(!cur) return;
            if(cur.ownerTabId===myTabId) return;
            if(Date.now()-cur.ts < TUNNEL_TTL && isOwnerAlive(cur.ownerTabId)) return;
            L('warn','TUNNEL','takeover '+k+' from '+cur.ownerTabId);
            if(!tunnels.has(k)) ensureTunnel(k);
        }catch(e){}
    }
    async function verifyTunnelAlive(key){
        const k=normalizeTarget(key)||key; if(!k) return false;
        try{
            const ctrl=new AbortController(); const to=setTimeout(()=>ctrl.abort(), 2000);
            const r=await fetch('/?__p2p_probe='+Date.now()+'&target='+encodeURIComponent(k), {cache:'no-store', signal:ctrl.signal});
            clearTimeout(to);
            const ok = r && r.status!==502 && r.status!==503 && r.status!==504;
            if(ok) touchTunnel(k, true);
            return ok;
        }catch(e){ return false; }
    }
     const BC_TUNNEL = (()=>{ try{ return new BroadcastChannel('p2p-tunnel-ping'); }catch(e){ return null; }})();
     if(BC_TUNNEL){ BC_TUNNEL.onmessage=(e)=>{
         const m=e.data||{};
         if(m.type==='ping' && m.key){
             const k=normalizeTarget(m.key)||m.key;
             const ent=tunnels.get(k);
             if(ent && ent.dc && ent.dc.readyState==='open'){
                 try{ BC_TUNNEL.postMessage({type:'pong', key:k, from:myTabId, ts: Date.now()}); }catch(ex){}
             }
         }
         if(m.type==='pong' && m.key){
             const k=normalizeTarget(m.key)||m.key;
             // mark that at least one p2p window has alive dc for this key
             try{ let o=loadTunnels(); const cur=o[k]; if(cur){ cur.ts=Date.now(); cur.ready=true; saveTunnels(o); } }catch(ex){}
         }
     }; }
     function touchTunnel(key, ready){
        const k=normalizeTarget(key)||key; if(!k) return;
        try{
            let o=loadTunnels();
            const cur=o[k];
            if(cur && cur.ownerTabId && cur.ownerTabId!==myTabId && isOwnerAlive(cur.ownerTabId) && Date.now()-cur.ts < TUNNEL_TTL) return;
            const willReady = !!(ready || (cur&&cur.ready));
            o[k]={ ownerTabId: myTabId, ts: Date.now(), ready: willReady, sig: (cur&&cur.sig)||'' };
            try{ if(!o[k].sig){ const u=new URL(k); o[k].sig=u.host+(u.port?':'+u.port:''); } }catch(e){}
            saveTunnels(o);
            try{ if(BC_TUNNEL && ready) BC_TUNNEL.postMessage({type:'ping', key:k, from:myTabId}); }catch(e){}
        }catch(e){}
    }
    function setTunnelReady(key){
        const k=normalizeTarget(key)||key; if(!k) return;
        try{ let o=loadTunnels(); o[k]={ ownerTabId: myTabId, ts: Date.now(), ready:true }; saveTunnels(o); }catch(e){}
    }
    function clearTunnel(key, force){
        const k=normalizeTarget(key)||key; if(!k) return;
        try{
            let o=loadTunnels();
            const cur=o[k];
            if(!cur) return;
            if(force || cur.ownerTabId===myTabId || Date.now()-cur.ts >= TUNNEL_TTL || (cur.ownerTabId && !isOwnerAlive(cur.ownerTabId))) { delete o[k]; saveTunnels(o); }
        }catch(e){}
    }
    function pruneStaleTunnels(){
        try{
            const o=loadTunnels(); let changed=false; const now=Date.now();
            for(const k in o){
                const e=o[k];
                const ent=tunnels.get(k);
                const localAlive = !!(ent && ent.dc && ent.dc.readyState==='open');
                const ownerDead = e && e.ownerTabId && e.ownerTabId!==myTabId && !isOwnerAlive(e.ownerTabId);
                if(!e || now - (e.ts||0) > TUNNEL_TTL){
                    if(!localAlive){ delete o[k]; changed=true; L('warn','TUNNEL','prune stale '+k); }
                    else { e.ts=now; changed=true; }
                    continue;
                }
                if(ownerDead && !localAlive){
                    delete o[k]; changed=true; L('warn','TUNNEL','prune owner dead '+k);
                    continue;
                }
                if(e && !e.ready && now - e.ts > 15000 && (!ent || !ent.ws || ent.ws.readyState===WebSocket.CLOSED)){
                    delete o[k]; changed=true; L('warn','TUNNEL','prune pending timeout '+k);
                    continue;
                }
                if(ent && !localAlive && ent.ws && ent.ws.readyState===WebSocket.CLOSED && now - e.ts > 5000){
                    delete o[k]; changed=true; L('warn','TUNNEL','prune dead local '+k);
                    if(tunnels.has(k)){ tunnels.delete(k); }
                    continue;
                }
            }
            if(changed) saveTunnels(o);
        }catch(e){}
        try{
            const keepers=loadKeepers(); const now=Date.now();
            let kc=false;
            for(const id in keepers){ if(now - keepers[id] > KEEPER_TTL){ delete keepers[id]; kc=true; } }
            if(kc) saveKeepers(keepers);
        }catch(e){}
        try{ for(const [k,ent] of tunnels.entries()){ if(!ent.dc || ent.dc.readyState!=='open'){ if(ent.ws && ent.ws.readyState===WebSocket.CLOSED){ const o=loadTunnels(); if(o[k] && o[k].ownerTabId===myTabId){ delete o[k]; saveTunnels(o); } tunnels.delete(k); L('warn','TUNNEL','purge local dead '+k); } } } }catch(e){}
        try{ renderTunnelList(); }catch(e){}
    }
    setInterval(pruneStaleTunnels, 5000);
    setInterval(async ()=>{
        try{
            const o=loadTunnels();
            for(const k in o){
                const e=o[k];
                if(!e||!e.ready) continue;
                if(e.ownerTabId===myTabId) continue;
                if(tunnels.has(k) && tunnels.get(k).dc && tunnels.get(k).dc.readyState==='open') continue;
                if(isOwnerAlive(e.ownerTabId)) continue;
                const ok=await verifyTunnelAlive(k);
                if(!ok){
                    const cur=loadTunnels();
                    if(cur[k]){ delete cur[k]; saveTunnels(cur); L('warn','TUNNEL','probe failed remove '+k); }
                }
            }
        }catch(e){}
    }, 6000);
     window.addEventListener('storage', (e)=>{
        if(e.key===LS_TUNNELS || e.key===LS_KEEPERS){ try{ renderTunnelList(); }catch(ex){} }
        if(e.key===LS_TUNNELS && e.newValue){
            try{
                const o=JSON.parse(e.newValue);
                for(const k in o){
                    const ent=o[k];
                    if(ent && ent.ready && !tunnels.has(k) && Date.now()-ent.ts < 5000){
                        // newly created tunnel by direct /?target open — adopt ping duty
                        L('info','ADOPT','taking over ping for '+k);
                        maybeTakeover(k);
                    }
                }
            }catch(ex){}
        }
    });
    async function countP2PviaSW(){
        if(!navigator.serviceWorker || !navigator.serviceWorker.controller) return null;
        try{
            return await new Promise(resolve=>{
                const ch=new MessageChannel();
                let to=setTimeout(()=>resolve(null), 500);
                ch.port1.onmessage=(e)=>{ clearTimeout(to); resolve(e.data&&typeof e.data.count==='number'?e.data.count:null); };
                try{ navigator.serviceWorker.controller.postMessage({type:'count-p2p'}, [ch.port2]); }catch(ex){ clearTimeout(to); resolve(null); }
            });
        }catch(e){ return null; }
    }
    async function hasOtherP2PAsync(){
        if(hasOtherP2PLocal()) return true;
        const c=await countP2PviaSW();
        if(c!==null) return c>1;
        return false;
    }
    function showPopupFallback(url){
        try{ L('warn','NAV','popup blocked — open '+url+' manually (keep this tab open)'); }catch(e){}
        const a=document.createElement('a');
        a.href=url; a.textContent='→ Open target (popup blocked)'; a.target='_blank';
        a.style.cssText='display:block;margin:8px 0;color:#06c;font-size:13px';
        try{ document.body.prepend(a); }catch(e){}
    }
    async function navigateToNext(nextUrl, key){
        const k=normalizeTarget(key)||key;
        if(navigatedKeys.has(k)) return;
        navigatedKeys.add(k);
        const url = nextUrl || ('/?target='+encodeURIComponent(k));
        if(pendingNavWindow && !pendingNavWindow.closed && pendingNavTarget===k){
            pendingNavWindow.location.href=url;
            L('ok','NAV','pending → '+url);
            pendingNavWindow=null; pendingNavTarget='';
            return;
        }
        const hasNext = !!nextUrl;
        if(hasNext){
            L('ok','NAV','redirect flow href '+url);
            location.href=url;
            return;
        }
        let hasOther = hasOtherP2PLocal();
        if(!hasOther){
            const c=await countP2PviaSW();
            if(c!==null) hasOther = c>1;
        }
        if(hasOther){ location.href=url; return; }
        const w=window.open(url, '_blank');
        if(w) L('ok','NAV','new tab '+url);
        else location.href=url;
    }
    async function handleNextParamOnReady(key){
        const nextRaw=getNextParam();
        let nextUrl='';
        if(nextRaw){
            try{ nextUrl=decodeURIComponent(nextRaw); if(!nextUrl) nextUrl=nextRaw; }catch(e){ nextUrl=nextRaw; }
            // next may be absolute url string already like https://.../?target=... need ensure it's path
            try{
                const u=new URL(nextUrl, location.origin);
                nextUrl=u.pathname + u.search + u.hash;
            }catch(e){}
        } else {
            // requirement: /p2p/ if has target and tunnel ready must jump back
            // if no next but has target, jump to /?target=key
            const t=getUrlTarget();
            if(t) nextUrl='/?target='+encodeURIComponent(t);
            else nextUrl='';
        }
        if(nextUrl) await navigateToNext(nextUrl, key);
    }

    let pageTargetCache = '';
    function pushTargetToSW(target) {
        if (target) pageTargetCache = normalizeTarget(target)||target;
        if (!pageTargetCache || !('serviceWorker' in navigator)) return;
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
            setTimeout(send, 700); setTimeout(send, 2500);
        }
    }
    document.getElementById('target').textContent = resolvePageTarget() || (cfg.target ? normalizeTarget(cfg.target)||cfg.target : '(url param)');
    document.getElementById('host').textContent = cfg.host || location.host;
    document.getElementById('stun').textContent = cfg.stun || '(none)';
    function setRow(id, text, ok) {
        const el = document.getElementById(id);
        if(!el) return;
        el.textContent = text;
        el.className = ok ? 'val ok' : (ok === false ? 'val bad' : 'val');
    }
    function setStatus(text, ok) {
        const el = document.getElementById('status');
        el.textContent = text;
        el.className = ok ? 'val ok' : (ok === false ? 'val bad' : 'val');
    }
    const SW_VERSION = 'v22-fix-includes';
    const DIAG = (lvl, tag, msg) => { try { L(lvl, tag, msg); } catch(e) {} try { console.log('['+tag+'] '+msg); } catch(e) {} };
    const tunnels = new Map();
    let booted = false;
    function bootP2P() {
        if (booted) return;
        booted = true;
        try {
            const r = startP2P();
            if (r && r.catch) r.catch(e=>{ setRow('signal','ERR: '+e.message,false); L('err','SIGNAL','boot failed: '+e.message); });
        } catch (e) {
            setRow('signal', 'ERR: ' + e.message, false);
            L('err', 'SIGNAL', 'boot failed: ' + e.message);
        }
    }
    if ('serviceWorker' in navigator) {
        L('info', 'SW', 'Registering service worker…');
        navigator.serviceWorker.register('/p2p/sw.js?v=v22-fix-includes', { scope: '/' })
            .then(reg => {
                if (reg.waiting) { L('warn', 'SW', 'New SW waiting, activating…'); reg.waiting.postMessage({ type: 'skip-waiting' }); }
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
            .catch(err => { setRow('sw', 'ERR', false); L('err', 'SW', 'Registration failed: ' + err.message); bootP2P(); });
        bootP2P();
    } else {
        setRow('sw', 'unsupported', false);
        L('err', 'SW', 'Service Workers not supported');
        bootP2P();
    }
    function renderTunnelList(){
        const box=document.getElementById('tunnelList');
        if(!box) return;
        const WS_NAMES=['CONNECTING','OPEN','CLOSING','CLOSED'];
        const entries=[...tunnels.entries()].map(([k,v])=> {
            let wsS='-'; let dcS='-'; let st='init';
            if(v.ws!=null && typeof v.ws.readyState==='number') wsS=WS_NAMES[v.ws.readyState]||String(v.ws.readyState);
            else if(v.ws!=null) wsS=String(v.ws.readyState);
            if(v.dc) dcS=String(v.dc.readyState);
            if(v.dc) st=(v.dc.readyState==='open'?'ready':String(v.dc.readyState));
            else if(v.ws!=null) st=wsS;
            return {key:k, state: String(st), ws: String(wsS), dc: String(dcS)};
        });
        // also show persisted tunnels
        const persisted=loadTunnels();
        for(let k in persisted){
            if(!entries.find(e=>e.key===k)){
                const e=persisted[k];
                entries.push({key:k, state: e.ready?'ready(persist)':'pending', ws:'-', dc:'-'});
            }
        }
        if(entries.length===0){ box.innerHTML='<span style="color:#888">no tunnels</span>'; return; }
        try{
        box.innerHTML=entries.map(e=> '<div style="display:flex;gap:8px;align-items:center;font-size:12px;border:1px solid #ddd;padding:4px 6px;border-radius:4px;background:#fff"><span style="font-family:monospace">'+escapeHtml(e.key)+'</span><span style="color:'+(String(e.state).includes('ready')?'#2e7d32':'#888')+'">'+escapeHtml(String(e.state))+'</span><button data-open="'+escapeHtml(e.key)+'" style="margin-left:auto;font-size:11px">Open</button><button data-close="'+escapeHtml(e.key)+'" style="font-size:11px">×</button></div>').join('');
        }catch(ex){ try{ L('err','UI','renderTunnelList '+ex.message);}catch(_){} box.innerHTML='<span style="color:#c00">render error: '+escapeHtml(String(ex.message||ex))+'</span>'; return; }
        box.querySelectorAll('[data-open]').forEach(btn=> btn.addEventListener('click', ()=>{ const k=btn.getAttribute('data-open'); const url='/?target='+encodeURIComponent(k); const hasOther=hasOtherP2PLocal(); if(hasOther){ location.href=url; } else { const w=window.open(url,'_blank'); if(!w) showPopupFallback(url); } }));
        box.querySelectorAll('[data-close]').forEach(btn=> btn.addEventListener('click', ()=>{ const k=btn.getAttribute('data-close'); closeTunnel(k); renderTunnelList(); }));
    }
    function closeTunnel(key){
        const k=normalizeTarget(key)||key;
        const ent=tunnels.get(k);
        if(ent){
            try{ if(ent.ws) ent.ws.close(); }catch(e){}
            try{ if(ent.pc) ent.pc.close(); }catch(e){}
            try{ if(ent.kaTimer) clearInterval(ent.kaTimer); }catch(e){}
            try{ if(ent.touchTimer) clearInterval(ent.touchTimer); }catch(e){}
            tunnels.delete(k);
            clearTunnel(k);
            renderTunnelList();
            L('info','TUNNEL','closed '+k);
        } else {
            clearTunnel(k);
            renderTunnelList();
        }
    }
    setInterval(renderTunnelList, 3000);
    function setConnectLoading(on){
        const b=document.getElementById('applyTarget');
        if(!b) return;
        if(on){
            if(!b.dataset.orig) b.dataset.orig=b.textContent;
            b.disabled=true;
            b.innerHTML='<span class="spin"></span> Connecting…';
        } else {
            b.disabled=false;
            b.textContent=b.dataset.orig||'🔗 Connect';
        }
    }
    async function startP2P() {
        const urlTarget = getUrlTarget();
        const nextParam = getNextParam();
        const effInitial = urlTarget || (cfg.target ? normalizeTarget(cfg.target)||cfg.target : '');
        if(effInitial) L('info','TARGET', effInitial + (nextParam? ' next='+nextParam:''));
        else L('warn','TARGET','No target specified — /p2p/ idle');
        const persisted=loadTunnels();
        const toRestore=Object.keys(persisted).filter(k=>{
            const e=persisted[k];
            return e && e.ready && Date.now()-e.ts < TUNNEL_TTL && k!==urlTarget;
        });
        for(const k of toRestore){
            if(!tunnels.has(k)){
                L('info','RESTORE','restoring '+k);
                ensureTunnel(k);
            }
        }
        if(urlTarget){
            await ensureTunnel(urlTarget);
            renderTunnelList();
            L('info','NAV','tunnel preparing for '+urlTarget+' — click Connect to open');
        } else {
            renderTunnelList();
        }
        return;
    }
    async function ensureTunnel(rawKey){
        const key=normalizeTarget(rawKey)||rawKey; if(!key) return null;
        if(tunnels.has(key)){
            const ent=tunnels.get(key);
            if(ent && ent.dc && ent.dc.readyState==='open'){
                pushTargetToSW(key);
                touchTunnel(key, true);
                return ent;
            }
            if(ent && ent.ws && (ent.ws.readyState===WebSocket.OPEN || ent.ws.readyState===WebSocket.CONNECTING)){
                return ent;
            }
            tunnels.delete(key);
        }
        if(isTunnelAlive(key)){
            const o=loadTunnels(); const e=o[key];
            const aliveKeeper = e && isOwnerAlive(e.ownerTabId);
            const probeOk = aliveKeeper ? await verifyTunnelAlive(key) : false;
            if(aliveKeeper && probeOk){
                pushTargetToSW(key);
                L('ok','TUNNEL','shared tunnel for '+key+' — reuse (keeper alive)');
                return {shared:true, key};
            }
            L('warn','TUNNEL','shared stale for '+key+' — rebuilding');
            try{ delete o[key]; saveTunnels(o); }catch(ex){}
        }
        return await buildTunnel(key);
    }
    async function buildTunnel(key){
        const k=normalizeTarget(key)||key;
        L('info','TUNNEL','building '+k);
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        let signalUrl = proto + '//' + location.host + '/p2p/signal?target=' + encodeURIComponent(k);
        pushTargetToSW(k);
        const ent={ key:k, ws:null, pc:null, dc:null, kaTimer:null, touchTimer:null, inflight:new Map(), wsStreams:new Map(), chunked:new Map(), ackSeen:false };
        tunnels.set(k, ent);
        renderTunnelList();
        touchTunnel(k, false);
        ent.touchTimer=setInterval(()=> touchTunnel(k, ent.dc&&ent.dc.readyState==='open'), TOUCH_INTERVAL);
        const ws = new WebSocket(signalUrl);
        ent.ws=ws;
        ws.onopen = () => {
            setRow('signal', 'OK', true);
            L('ok', 'SIGNAL', 'WebSocket connected for '+k);
            DIAG('ok', 'SIGNAL', 'WS open '+k);
        };
        ws.onclose = (e) => {
            setRow('signal', 'closed', false);
            L('err', 'SIGNAL', 'WebSocket closed for '+k+' code='+e.code);
            DIAG('err', 'SIGNAL', 'WS closed '+k+' code='+e.code);
            setStatus('Disconnected', false);
            if(ent.touchTimer) { clearInterval(ent.touchTimer); ent.touchTimer=null; }
            if(!ent.dc || ent.dc.readyState!=='open'){
                setTimeout(()=>{
                    if(!ent.dc || ent.dc.readyState!=='open'){
                        try{ clearTunnel(k); }catch(ex){}
                        if(tunnels.has(k) && (!tunnels.get(k).dc || tunnels.get(k).dc.readyState!=='open')) tunnels.delete(k);
                        try{ renderTunnelList(); }catch(ex){}
                    }
                }, 3000);
                try{ renderTunnelList(); }catch(ex){}
            }
        };
        ws.onerror = () => {
            setRow('signal', 'error', false);
            L('err', 'SIGNAL', 'WebSocket error for '+k);
            DIAG('err', 'SIGNAL', 'WS error '+k);
            setStatus('Signaling error', false);
        };
        let myId=null;
        const pc = new RTCPeerConnection({ iceServers: [{ urls: cfg.stun || 'stun:stun.miwifi.com:3478' }] });
        ent.pc=pc;
        pc.onicecandidate = (e) => {
            if (e.candidate) {
                L('ice', 'ICE', 'Local candidate for '+k);
                DIAG('ice', 'ICE', 'local cand '+k);
                sendSignal({ type: 'candidate', to: myId, candidate: e.candidate });
            } else { L('ice', 'ICE', 'Gathering complete for '+k); }
        };
        pc.onicegatheringstatechange = () => { L('ice', 'ICE', 'Gathering: '+pc.iceGatheringState+' for '+k); };
        pc.onconnectionstatechange = () => {
            const s=pc.connectionState;
            setRow('ice', s, s==='connected');
            L('ice','ICE','Connection '+k+': '+s);
            if(s==='connected') setStatus('WebRTC connected for '+k);
            else if(s==='failed'){ setStatus('WebRTC failed for '+k, false); L('err','WEBRTC','failed '+k); try{ clearTunnel(k); }catch(e){} tunnels.delete(k); try{ ent.ws&&ent.ws.close(); }catch(e){} try{ renderTunnelList(); }catch(e){} }
            else if(s==='disconnected'){ setStatus('WebRTC disconnected for '+k, false); if(!ent.dc || ent.dc.readyState!=='open'){ setTimeout(()=>{ if(!ent.dc||ent.dc.readyState!=='open'){ try{ clearTunnel(k); }catch(e){} tunnels.delete(k); try{ renderTunnelList(); }catch(e){} } }, 2500); } }
        };
        pc.oniceconnectionstatechange = () => { L('ice','ICE','ICE state '+k+': '+pc.iceConnectionState); };
        pc.ondatachannel = (event) => {
            const dc = event.channel;
            ent.dc=dc;
            L('ok','DC','Received DataChannel '+k+': '+dc.label);
            dc.onopen = () => {
                ent.ackSeen=false;
                ent.pingPending=false;
                ent.pingTs=0;
                ent.failCount=0;
                setRow('dc','OPEN',true);
                setStatus('✅ Ready '+k, true);
                L('ok','DC','DataChannel opened '+k);
                DIAG('ok','DC','open '+k);
                const bar=document.getElementById('loading-bar'); if(bar){ bar.classList.remove('active'); bar.style.width='100%'; }
                notifySWForKey(true, k);
                setTunnelReady(k);
                touchTunnel(k, true);
                renderTunnelList();
                if(pendingNavWindow && !pendingNavWindow.closed && pendingNavTarget===k){
                    if(!navigatedKeys.has(k)){
                        navigatedKeys.add(k);
                        const url='/?target='+encodeURIComponent(k);
                        pendingNavWindow.location.href=url;
                        L('ok','NAV','pending → '+url);
                    }
                    try{ setConnectLoading(false); }catch(e){}
                    pendingNavWindow=null; pendingNavTarget='';
                    renderTunnelList();
                } else {
                    try{ setConnectLoading(false); }catch(e){}
                    L('ok','NAV','Tunnel ready '+k+' — click Open to view, or use Connect again');
                    renderTunnelList();
                }
                try{ dc.send(JSON.stringify({ type:'ready', ts:Date.now() })); }catch(e){}
                setTimeout(()=>{ if(!ent.ackSeen) DIAG('warn','DC','ready-ack not seen '+k); },2000);
                if(ent.kaTimer) clearInterval(ent.kaTimer);
                ent.kaTimer=setInterval(()=>{
                    if(dc.readyState!=='open') return;
                    if(ent.pingPending){
                        ent.failCount=(ent.failCount||0)+1;
                        L('warn','PING','timeout pong '+k+' fail='+ent.failCount);
                        if(ent.failCount>=3){
                            L('err','PING','tunnel dead '+k+' — closing');
                            try{ dc.close(); }catch(e){}
                            try{ ent.pc.close(); }catch(e){}
                            try{ ent.ws.close(); }catch(e){}
                            if(ent.kaTimer){ clearInterval(ent.kaTimer); ent.kaTimer=null; }
                            clearTunnel(k);
                            tunnels.delete(k);
                            renderTunnelList();
                            return;
                        }
                    }
                    ent.pingPending=true;
                    ent.pingTs=Date.now();
                    try{ dc.send(JSON.stringify({ type:'ping', ts: ent.pingTs })); L('ice','PING','→ ping '+k); }catch(e){ ent.pingPending=false; }
                    touchTunnel(k, true);
                },7000);
            };
            dc.onclose = () => {
                if(ent.kaTimer){ clearInterval(ent.kaTimer); ent.kaTimer=null; }
                clearTunnel(k);
                tunnels.delete(k);
                setRow('dc','CLOSED',false);
                setStatus('DataChannel closed '+k, false);
                L('err','DC','DataChannel closed '+k);
                notifySWForKey(false, k);
                try{ ent.ws&&ent.ws.close(); }catch(e){}
                try{ if(ent.touchTimer){ clearInterval(ent.touchTimer); ent.touchTimer=null; } }catch(e){}
                renderTunnelList();
            };
            dc.onerror = (e)=>{ L('err','DC','DataChannel error '+k); };
            dc.onmessage = (event)=>{
                let raw=event.data; let msg;
                try{ msg=JSON.parse(raw); }catch{ return; }
                if(msg.type==='ping'){ try{ dc.send(JSON.stringify({ type:'pong', ts:msg.ts })); L('ice','PING','← ping → pong '+k); touchTunnel(k, true); }catch(e){} return; }
                if(msg.type==='pong'){ ent.pingPending=false; ent.failCount=0; L('ice','PONG','← pong '+k+' rtt='+(Date.now()-msg.ts)+'ms'); touchTunnel(k, true); return; }
                if(msg.type==='ready'){ try{ dc.send(JSON.stringify({ type:'ready-ack', ts:Date.now() })); L('ok','DC','ready↔ack '+k); }catch(e){} return; }
                if(msg.type==='ready-ack'){ ent.ackSeen=true; L('ok','DC','← ready-ack '+k); return; }
                if(msg.type==='response-start'){
                    if(typeof msg.headers==='string'){ try{ msg.headers=JSON.parse(msg.headers); }catch(e){ msg.headers={}; } }
                    if(!msg.headers||typeof msg.headers!=='object') msg.headers={};
                    ent.chunked.set(msg.id, { headers:msg.headers, status:msg.status, chunks:msg.chunks, parts:new Array(msg.chunks), received:0, port: ent.inflight.get(msg.id) });
                    return;
                }
                if(msg.type==='response-chunk'){ const c=ent.chunked.get(msg.id); if(!c) return; c.parts[msg.idx]=msg.data; c.received++; touchTunnel(k, true); return; }
                if(msg.type==='response-end'){
                    const c=ent.chunked.get(msg.id); if(!c) return; ent.chunked.delete(msg.id); ent.inflight.delete(msg.id);
                    const body=c.parts.join(''); const out={ type:'response', id:msg.id, status:c.status, headers:c.headers, body };
                    const port=c.port; if(!port) return; port.postMessage({ type:'response', ...out }); return;
                }
                if(msg.type==='response'){ const port=ent.inflight.get(msg.id); if(!port) return; ent.inflight.delete(msg.id); port.postMessage({ type:'response', ...msg }); }
                else if(msg.type==='ws-open-ok'){ const port=ent.wsStreams.get(msg.id); if(!port) return; port.postMessage(msg); }
                else if(msg.type==='ws-open-err'){ const port=ent.wsStreams.get(msg.id); if(!port) return; port.postMessage(msg); ent.wsStreams.delete(msg.id); }
                else if(msg.type==='ws-data'){ const port=ent.wsStreams.get(msg.id); if(!port) return; port.postMessage(msg); }
                else if(msg.type==='ws-closed'){ const port=ent.wsStreams.get(msg.id); if(!port) return; port.postMessage(msg); ent.wsStreams.delete(msg.id); }
            };
        };
        function notifySWForKey(open, targetKey){
            if(!('serviceWorker' in navigator)) return;
            const k=normalizeTarget(targetKey)||targetKey;
            const payload={ type:'dc-state', open, target: k };
            const trySend=()=>{
                if(navigator.serviceWorker.controller){ try{ navigator.serviceWorker.controller.postMessage(payload); L(open?'ok':'warn','SW', (open?'dc open ':'dc close ')+k); return true; }catch(e){} }
                return false;
            };
            if(trySend()) return;
            navigator.serviceWorker.addEventListener('controllerchange', trySend, {once:true});
            setTimeout(trySend,800); setTimeout(trySend,2500);
        }
        // SW bridge demux by target key — strict per-key, no fallback to avoid cross-routing
        if(!window.__swBridgeInstalled){
            window.__swBridgeInstalled=true;
            if('serviceWorker' in navigator){
                navigator.serviceWorker.addEventListener('message', (event)=>{
                    const msg=event.data||{}; const port=event.ports&&event.ports[0]; if(!port) return;
                    const rawKey = (msg.payload && (msg.payload.target || msg.target)) ? (msg.payload.target || msg.target) : '';
                    const targetKey = rawKey ? (normalizeTarget(rawKey)||rawKey.trim()) : '';
                    let ent=null;
                    if(targetKey) ent=tunnels.get(targetKey);
                    if(!ent){
                        const avail=[...tunnels.keys()].join(', ')||'(none)';
                        L('err','SW','no tunnel for target '+(targetKey||'?')+' avail '+avail);
                        port.postMessage({ type:'error', error:'no tunnel for target '+(targetKey||'?')+' — open /p2p/?target='+encodeURIComponent(targetKey||'') });
                        return;
                    }
                    if(msg.type==='tunnel' && msg.payload){
                        if(!ent.dc || ent.dc.readyState!=='open'){ port.postMessage({ type:'error', error:'dc not open' }); return; }
                        ent.inflight.set(msg.payload.id, port);
                        try{ ent.dc.send(JSON.stringify(msg.payload)); }catch(e){ port.postMessage({type:'error',error:e.message}); ent.inflight.delete(msg.payload.id); return; }
                        setTimeout(()=>{ if(ent.inflight.has(msg.payload.id)){ const p=ent.inflight.get(msg.payload.id); ent.inflight.delete(msg.payload.id); try{p.postMessage({type:'error',error:'client timeout 8s'});}catch(_){} }},8000);
                    } else if(msg.type==='ws-tunnel' && msg.payload){
                        if(!ent.dc || ent.dc.readyState!=='open'){ port.postMessage({type:'error',error:'dc not open'}); return; }
                        ent.wsStreams.set(msg.payload.id, port); try{ ent.dc.send(JSON.stringify(msg.payload)); }catch(e){ port.postMessage({type:'error',error:e.message}); }
                    } else if(msg.type==='ws-data-send' && msg.payload){ if(ent.dc&&ent.dc.readyState==='open') ent.dc.send(JSON.stringify(msg.payload)); }
                    else if(msg.type==='ws-close-send' && msg.payload){ if(ent.dc&&ent.dc.readyState==='open'){ ent.dc.send(JSON.stringify(msg.payload)); ent.wsStreams.delete(msg.payload.id); } }
                });
            }
        }
        ws.onmessage = async (event)=>{
            let msg; try{ msg=JSON.parse(event.data); }catch{ return; }
            if(msg.type==='join'){ myId=msg.id; L('signal','SIGNAL','Joined '+k+' as '+myId); }
            else if(msg.type==='offer'){ await pc.setRemoteDescription(msg.sdp); const answer=await pc.createAnswer(); await pc.setLocalDescription(answer); sendSignal({ type:'answer', to:myId, sdp:answer }); }
            else if(msg.type==='candidate'){ try{ await pc.addIceCandidate(msg.candidate); }catch(e){ L('err','ICE','addIceCandidate failed '+k); } }
        };
        function sendSignal(obj){ if(ws.readyState===WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
        window.__p2p = window.__p2p || {}; window.__p2p[k]=ent;
        return ent;
    }
    let histCache = [];
    function escapeHtml(s){ return String(s).replace(/[&<>"']/g, c=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));}
    function loadHistory(){ try{ const h=JSON.parse(localStorage.getItem(LS_HIST)||'[]'); histCache=Array.isArray(h)?h.filter(x=>typeof x==='string'):[];}catch(e){histCache=[];} return histCache; }
    function renderHistory(){ const box=document.getElementById('targetHistory'); if(!box) return; loadHistory(); box.innerHTML=histCache.map((t,i)=>'<button class="hist-btn" data-i="'+i+'" title="switch to '+escapeHtml(t)+'">'+escapeHtml(t)+'</button>').join(''); }
    function openTargetTab(t){
        const norm=normalizeTarget(t)||t; if(!norm) return null;
        if(pendingNavWindow && !pendingNavWindow.closed && pendingNavTarget===norm) return pendingNavWindow;
        if(navigatedKeys.has(norm)){ L('info','NAV','already opening '+norm); return null; }
        let w=null;
        try{ w=window.open('about:blank','_blank'); }catch(e){}
        if(!w){ showPopupFallback('/?target='+encodeURIComponent(norm)); return null; }
        try{ w.document.title='Connecting '+norm; w.document.body.innerHTML='<p style="font:14px monospace;padding:20px">Connecting to '+escapeHtml(norm)+' — waiting for tunnel...</p>'; }catch(e){}
        pendingNavWindow=w; pendingNavTarget=norm;
        return w;
    }
    function switchTarget(t){
        const norm=normalizeTarget(t); if(!norm) return false; t=norm;
        const existing = tunnels.get(norm);
        const alreadyReady = existing && existing.dc && existing.dc.readyState==='open';
        const alreadyShared = (()=>{ try{ const o=loadTunnels()[norm]; return o && o.ready && Date.now()-o.ts < TUNNEL_TTL && isOwnerAlive(o.ownerTabId); }catch(e){ return false; }})();
        try{ const u=new URL(location.href); u.searchParams.set('target', t); history.replaceState(null,'', u.toString()); }catch(e){}
        pendingTarget=t;
        try{
            const hist=[t].concat(loadHistory().filter(x=>normalizeTarget(x)!==t).map(x=>normalizeTarget(x)||x));
            const uniq=[]; const seen=new Set();
            for(const h of hist){ const k=normalizeTarget(h)||h; if(!seen.has(k)){ seen.add(k); uniq.push(k); } }
            localStorage.setItem(LS_HIST, JSON.stringify(uniq.slice(0,8)));
        }catch(e){}
        renderHistory();
        const input=document.getElementById('targetInput'); if(input) input.value=t;
        document.getElementById('target').textContent=t;
        let spinnerOn=false;
        if(!navigatedKeys.has(norm)){
            const w=openTargetTab(norm);
            if(w) { setConnectLoading(true); spinnerOn=true; }
            else if(alreadyReady||alreadyShared){
                setConnectLoading(true); spinnerOn=true;
            } else {
                setConnectLoading(true); spinnerOn=true;
            }
        }
        ensureTunnel(t).then(async ent=>{
            const ok = ent && ( (ent.dc && ent.dc.readyState==='open') || ent.shared );
            if(ok){
                setStatus('✅ Ready '+t, true);
                if(pendingNavWindow && !pendingNavWindow.closed && pendingNavTarget===norm){
                    if(!navigatedKeys.has(norm)){
                        navigatedKeys.add(norm);
                        pendingNavWindow.location.href='/?target='+encodeURIComponent(norm);
                        L('ok','NAV','pending → /?target='+norm);
                    }
                    pendingNavWindow=null; pendingNavTarget='';
                    setConnectLoading(false);
                    return;
                }
                if(!navigatedKeys.has(norm)){
                    navigatedKeys.add(norm);
                    const url='/?target='+encodeURIComponent(norm);
                    let w=null;
                    try{ w=window.open(url,'_blank'); }catch(e){}
                    if(w) L('ok','NAV','new tab '+url); else { showPopupFallback(url); }
                }
                setConnectLoading(false);
            } else {
                if(spinnerOn){ setTimeout(()=>setConnectLoading(false), 8000); }
            }
        });
        const ent2=tunnels.get(t);
        if(ent2 && ent2.ws && (ent2.ws.readyState===WebSocket.OPEN || ent2.ws.readyState===WebSocket.CONNECTING)){
            try{
                const doSend=()=>{ try{ ent2.ws.send(JSON.stringify({ type:'target', target:t })); pendingTarget=''; L('info','TARGET','hot-swapped '+t); }catch(e2){} };
                if(ent2.ws.readyState===WebSocket.OPEN) doSend(); else ent2.ws.addEventListener('open', doSend,{once:true});
            }catch(e){ L('warn','TARGET','hot-swap failed '+t); }
        }
        renderTunnelList();
        return true;
    }
    window.__switchTarget=switchTarget;
    renderHistory();
    renderTunnelList();
    const histBox=document.getElementById('targetHistory'); if(histBox) histBox.addEventListener('click', (event)=>{ const btn=event.target&&event.target.closest?event.target.closest('[data-i]'):null; if(!btn) return; const t=histCache[parseInt(btn.getAttribute('data-i'),10)]; if(!t) return; const inp=document.getElementById('targetInput'); if(inp) inp.value=t; switchTarget(t); });
    document.getElementById('btn').addEventListener('click', async () => {
        const out=document.getElementById('out'); out.textContent='fetching…'; L('tunnel','TEST','Fetching / via P2P…');
        try{ const r=await fetch('/?test='+Date.now()); const text=await r.text(); out.textContent='HTTP '+r.status+'\n\n'+text.slice(0,4000); L('ok','TEST','Got HTTP '+r.status); }catch(e){ out.textContent='error: '+e.message; L('err','TEST',e.message); }
    });
})();
