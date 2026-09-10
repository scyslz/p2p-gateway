# p2p-gateway

A minimal, single-binary **WebRTC application-layer HTTP tunnel**.

Browser opens `/p2p/?target=<url>`, establishes WebRTC DataChannel via
`/p2p/signal`, then Service Worker (`/p2p/sw.js`) tunnels `fetch()` over
the DataChannel to Go, which proxies to the real upstream. The browser
address bar stays on the gateway host.

```
  browser /p2p/?target=http://127.0.0.1:3000
        |
        |  fetch() intercepted by /p2p/sw.js
        v
  Service Worker -- DataChannel --> Go gateway (this repo)
                                         |
                                         v
                                   http://127.0.0.1:3000
```

HTTP tunnel over WebRTC, not a VPN. Only HTTP/HTTPS through DC.

## Multi-tunnel (current)

Key = `normalizeTarget(raw)` = `scheme://host[:port]`, lowercased,
default ports folded (`http:80/https:443` dropped), path/query/hash
stripped. `http` ≠ `https`. Example:

- `192.168.1.10:3000` → `http://192.168.1.10:3000`
- `https://Example.com/` → `https://example.com`

State:

- `localStorage p2p-tunnels: {key:{ownerTabId,ts,ready}}` + `p2p-keepers:{tabId:ts}` heartbeat `5s/15s TTL`, `TUNNEL_TTL 30s`
- `tunnels Map<key,{ws,pc,dc,kaTimer,touchTimer}>`, `TAB_ID=crypto.randomUUID()`, `BroadcastChannel p2p-tunnel-ping`
- `p2p-targets` history (max 8)

UI (`/p2p/`):

- No auto-jump. Must click **Connect**. Button shows spinner `Connecting…` until `DC open`.
- `switchTarget(t)`: sync `window.open('about:blank')` placeholder + `ensureTunnel(t)`, ready → reuse placeholder `location.href='/?target=key'`, dedup via `navigatedKeys`.
- History click = fill input + `switchTarget` (same as Connect).
- `tunnelList`: per-key `ready/open/Open/×`. `Open` opens `/?target=key`.
- Dead tunnel removed immediately: `dc.onclose/ws.onclose/pc failed/ping fail>=3` → `tunnels.delete + clearTunnel + renderTunnelList`. Background `prune 5s` + `probe 6s` purges stale `localStorage`.

SW (`p2p-sw.js`):

- Per-key `readyHostByTarget Map<key,hostId>`, `targetByClient`, `readyByClient`.
- `fetch` resolves `?target → targetByClient → Referer ?target → client.url ?target → single-live fallback`.
- Not ready: wait `20*150ms`, then `503 Tunnel not ready … open /p2p/?target=… and click Connect`. No auto `302` to `/p2p/?next=`.

## Project layout

```
p2p-gateway/
├── cmd/gateway/      # entry point, HTTP wiring
├── internal/
│   ├── routing/      # host prefix → upstream URL
│   ├── signaling/    # gorilla/websocket offer/answer relay
│   ├── webrtc/       # pion/webrtc PeerConnection lifecycle
│   └── proxy/        # JSON-over-DataChannel HTTP forwarder (per-request Target)
├── web/files/        # index.html, client.js, p2p-sw.js (embedded + disk)
├── config.yaml       # runtime configuration
├── restart.sh        # vet + syntax + build + restart + LISTEN check
└── README.md
```

## Prerequisites

* Go 1.21+
* STUN server (default `stun:stun.miwifi.com:3478`), optional TURN
* Chrome/Firefox with WebRTC + ServiceWorker

## Build & Run

```bash
go vet ./...
node --check web/files/client.js && node --check web/files/p2p-sw.js
go build -o /tmp/p2p-gateway-bin ./cmd/gateway
```

One-key (same as above + restart + check):

```bash
bash restart.sh
# BUILD_OK 15M / LISTEN_OK :62057 / 200
```

Or:

```bash
./p2p-gateway                # uses ./config.yaml
./p2p-gateway /etc/p2p.yaml  # explicit config path
go run ./cmd/gateway ./config.yaml
```

## Configuration

`config.yaml`:

```yaml
listen_addr: ":62057"
base_domain: "localhost"
prefix: "p2p-"
stun_url: "stun:stun.miwifi.com:3478"
turn_url: "turn:user:pass@host:3478?transport=udp"
scheme: "http"
```

* `listen_addr` - e.g. `:62057`
* `stun_url` / `turn_url` - ICE servers
* `scheme` - default upstream scheme for host routing
* `--target <url>` or `target:` - fixed upstream (overrides `?target=`? No: `?target=` wins per-peer)

Signaling target priority: `?target=` > global `Target` > host direct/prefix routing.

## Browser test

1. Open `http://127.0.0.1:62057/p2p/?target=http://127.0.0.1:3000`.
2. Click **Connect** → spinner → log `DC open` → auto reuses placeholder tab to `/?target=<key>`.
3. Click **Fetch / via tunnel** → `HTTP 200`.
4. Second target e.g. `http://192.168.1.5:8080` → Connect again, tunnels coexist, assets route per-key.
5. Direct open `/?target=<key>` with no tunnel → `503`, no auto redirect.
6. Kill upstream / close DC → entry disappears from list within seconds.

Cache bust: files versioned (`/client.js?v=v22…`, `/p2p/sw.js?v=v22…`) + `no-cache,no-store` headers. If UI stale (`e.state.includes is not a function` or old behavior): hard refresh `Ctrl+Shift+R`, DevTools → Application → Service Workers → Unregister → Reload.

## Wire protocol (v2 + Target)

Request (browser → gateway), `target` = normalized key:

```json
{"type":"request","id":"abc","method":"GET","path":"/?x=1","headers":{},"body":"<b64>","target":"http://127.0.0.1:3000"}
```

Response / chunked (gateway → browser):

```json
{"type":"response","id":"abc","status":200,"headers":{},"body":"<b64>"}
{"type":"response-start","id":"abc","status":200,"headers":{},"chunks":N}
{"type":"response-chunk","id":"abc","idx":0,"data":"<b64>"}
{"type":"response-end","id":"abc"}
```

Control: `ws-open/ws-data/ws-closed`, `ping/pong`, `ready/ready-ack`. Client timeout `8s`, SW tunnel timeout `8-10s`.

Fallback `/upstream/` is disabled (`503 P2P-only mode`). Non-browser clients must use WebRTC path.

## Testing

```bash
go test ./...
go vet ./...
```

Covers `internal/routing`, `internal/proxy`.

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `e.state.includes is not a function` / old UI | Stale `client.js`/SW cache → hard refresh + Unregister SW, check `?v=` version |
| `503 Tunnel not ready` | No DC for key → keep `/p2p/?target=` open, click Connect, keep tab alive |
| `Status: Connecting` forever | Signaling WS failed → check listen port, `config.yaml`, gateway log |
| `ICE failed` | Symmetric NAT → add TURN |
| `DC CLOSED` / list entry vanishes | Upstream/peer gone, ping `fail>=3`, owner keeper expired → reconnect |
| `no tunnel for target` in log | SW demux strict per-key, no cross-routing → open `/p2p/?target=<key>` and Connect |
| Popup blocked | Allow popups, or manually open `/?target=<key>`; keep `/p2p/` tab open |

## Files

* `cmd/gateway/main.go` - HTTP server, `/p2p/*` + `/` wiring, no-cache headers
* `internal/signaling/hub.go` - WS room
* `internal/webrtc/peer.go` - pion lifecycle, `SetTarget`
* `internal/proxy/proxy.go` - `Request/WSOpen {Target}`, `forwardHTTP` per `effTarget`
* `web/files/index.html` - UI + spinner CSS
* `web/files/client.js` - tunnels Map, `ensureTunnel/buildTunnel/switchTarget/openTargetTab`, prune/probe
* `web/files/p2p-sw.js` - per-key `readyHostByTarget`, `forward/wsTunnel`, 503 path
* `restart.sh` - one-key verify

## Acknowledgements

* [`andrewmthomas87/web-p2p-tunnel`](https://github.com/andrewmthomas87/web-p2p-tunnel)
* [`ambianic/peerfetch`](https://github.com/ambianic/peerfetch)
