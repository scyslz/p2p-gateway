# p2p-gateway

A minimal, single-binary **WebRTC application-layer HTTP tunnel**.

The gateway terminates HTTPS on a hostname like `p2p-a.example.com`,
exchanges SDP/ICE with the browser over a tiny WebSocket signaling
channel, and forwards every browser request through a WebRTC
DataChannel to a Go reverse-proxy that talks to the real upstream
(`https://a.example.com`). The browser address bar **always** stays on
the gateway host - no redirects, no `Location:` rewrites that escape the
gateway.

```
  browser (https://p2p-a.example.com/)
        |
        |  fetch() intercepted by /p2p-sw.js
        v
  Service Worker -- DataChannel "http" --> Go gateway (this repo)
                                                |
                                                v
                                       https://a.example.com
```

This is an **HTTP tunnel over WebRTC**, not a VPN. Only HTTP/HTTPS
requests go through; other traffic (DNS, raw TCP, QUIC) does not.

## Status

This is the **first working version**. The minimum loop - browser to
upstream over a DataChannel, with HTTPS fallback when WebRTC fails - is
operational. The protocol is intentionally tiny: JSON over a text
DataChannel, with bodies base64-encoded.

## Project layout

```
p2p-gateway/
├── cmd/gateway/      # entry point, HTTP server wiring
├── internal/
│   ├── routing/      # host prefix → upstream URL
│   ├── signaling/    # gorilla/websocket-based offer/answer relay
│   ├── webrtc/       # pion/webrtc PeerConnection lifecycle
│   └── proxy/        # JSON-over-DataChannel HTTP request handler
├── web/files/        # index.html, client.js, p2p-sw.js (embedded)
├── go.mod
├── go.sum
├── config.yaml       # runtime configuration
└── README.md
```

## Prerequisites

* Go 1.21+
* A registered DNS name you control (e.g. `example.com`)
* A TLS certificate covering `p2p-<anything>.example.com`
* A STUN server (the default config uses Google's public STUN, which
  works for NAT-traversal in most cases)

## Build

```bash
go mod tidy
go build -o p2p-gateway ./cmd/gateway
```

The result is a single static binary `p2p-gateway`.

## Configuration

Edit `config.yaml`:

```yaml
listen_addr: ":443"
tls_cert: "/etc/ssl/fullchain.pem"
tls_key: "/etc/ssl/privkey.pem"
base_domain: "example.com"
prefix: "p2p-"
stun_url: "stun:stun.l.google.com:19302"
```

* `listen_addr` - `:443` for production. Anything works for testing.
* `tls_cert` / `tls_key` - PEM-encoded certificate + private key. The
  certificate must include `p2p-<x>.example.com` in the SAN list (or be
  a wildcard for `*.example.com`).
* `base_domain` - the apex domain that hosts the gateway. Subdomains of
  this domain matching `prefix*` are mapped to upstream URLs.
* `prefix` - the subdomain prefix that triggers the tunnel. The default
  `p2p-` maps `p2p-a.example.com` → `https://a.example.com`.
* `stun_url` - a STUN URI the browser will use for ICE. Google and
  Cloudflare both provide free public servers.

Optional fields:

* `scheme: "http"` - talk to upstreams over plain HTTP. **Do not** use in
  production; only for local development.
* `upstream_port: 8000` - append a fixed port to every upstream URL.
  Useful when testing against a local server on a non-default port.

### Host → upstream mapping

Given `base_domain: example.com` and `prefix: p2p-`:

| Gateway host           | Upstream                  |
|------------------------|---------------------------|
| `p2p-a.example.com`    | `https://a.example.com`   |
| `p2p-b.example.com`    | `https://b.example.com`   |
| `p2p-api.example.com`  | `https://api.example.com` |
| `p2p-foo.bar.example.com` | `https://foo.bar.example.com` |

Hosts without the prefix (`example.com`, `other.com`) are rejected with
`400 Bad Request`.

## DNS setup

For each upstream you want to expose, create a DNS A/AAAA record
pointing the `p2p-<name>` subdomain at the gateway server:

```text
p2p-a   IN  A     <gateway.ip>
p2p-b   IN  A     <gateway.ip>
p2p-api IN  CNAME p2p-a.example.com.
```

You can also use a wildcard:

```text
*.p2p-  IN  A     <gateway.ip>
```

This single record covers every gateway host.

## TLS setup

Use any ACME client (certbot, acme.sh, Caddy) to obtain a certificate
that covers the gateway hostnames. For testing:

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout ./certs/privkey.pem \
  -out    ./certs/fullchain.pem \
  -days 365 -nodes \
  -subj "/CN=p2p-a.example.com" \
  -addext "subjectAltName=DNS:p2p-a.example.com,DNS:p2p-b.example.com,DNS:*.example.com"
```

The gateway speaks **plain TLS 1.2+**; HTTP/2 is allowed but
unimportant because the only long-lived connections are WebSockets
(forced to HTTP/1.1 by the browser for the signaling endpoint, then
upgraded in place).

## Run

```bash
./p2p-gateway                # uses ./config.yaml
./p2p-gateway /etc/p2p.yaml  # explicit config path
```

The gateway logs the resolved configuration and the listen address on
startup. SIGINT / SIGTERM trigger a graceful shutdown.

### Development with `go run`

You don't need to build for development:

```bash
go run ./cmd/gateway ./config.yaml
```

## Browser test

1. Open `https://p2p-a.example.com/` in Chrome or Firefox.
2. The page shows a status panel:

   ```
   Target         https://a.example.com
   Gateway Host   p2p-a.example.com
   Service Worker OK
   Signaling      OK
   STUN           stun:stun.l.google.com:19302
   ICE            connected
   DataChannel    OPEN
   Status         Connected
   ```

3. Click **Fetch / via P2P tunnel**. The request goes:

   ```
   browser
   → Service Worker (intercept)
   → WebRTC DataChannel "http"
   → Go gateway (this binary)
   → HTTPS upstream (https://a.example.com)
   ```

   The response flows back the same way. The browser URL bar never
   changes.

4. Try `p2p-b.example.com/` - it should serve `b.example.com`'s content
   through a separate browser session, proving that one binary can host
   many tunnels at once.

### How the data flows

```
┌────────────────────────────────┐
│  index.html  ──  registers ──  │  Service Worker (p2p-sw.js)
│                                │      ▲
│  client.js  ──  opens WS  ──► Gateway WebSocket /_signal
│  client.js  ──  offers   ──► pion/webrtc PeerConnection
│                                │      ▲
│  client.js  ──  sends    ──► DataChannel "http"
│                                │      ▲
└────────────────────────────────┘
                               gateway
                                │
                                ▼
                       ┌─────────────────┐
                       │  JSON frames    │
                       │  ── request     │  proxy.Forward → http.Client
                       │  ── response    │  ← resp body
                       └─────────────────┘
```

### Wire protocol (v1)

Request frame (browser → gateway):

```json
{
  "type": "request",
  "id": "abc123",
  "method": "GET",
  "path": "/foo?x=1",
  "headers": { "accept": "text/html", "cookie": "..." },
  "body": "<base64 of body bytes>"
}
```

Response frame (gateway → browser):

```json
{
  "type": "response",
  "id": "abc123",
  "status": 200,
  "headers": { "content-type": "text/html" },
  "body": "<base64 of body bytes>"
}
```

Bodies are base64 because DataChannel text messages must be valid UTF-8.
Bodies are sent only when non-empty.

## Fallback

If the WebRTC connection drops (or never establishes), the Service
Worker rewrites each `fetch()` to `https://<gateway>/upstream/<path>`
and lets the browser hit the gateway's reverse-proxy handler. That
handler talks directly to the upstream, so the page keeps working -
just without the P2P indirection.

The `/upstream/` prefix is also useful for non-browser clients (e.g.
`curl`) that don't run Service Workers.

## What is *not* implemented

Per the v1 spec, this version deliberately omits:

* Docker / Kubernetes packaging
* Redis / database persistence
* User authentication
* WebSocket proxying
* HTTP/2 / HTTP/3 tunnels
* Caching, CDN integration
* HTML / JavaScript rewriting (the browser sees the upstream's raw
  markup)
* Per-host permissions / multi-tenant UI
* TURN servers (works only if both sides can reach each other directly
  via STUN-derived candidates; for restrictive NATs you will need to
  add a TURN URL to the `iceServers` list in `client.js`)

## Files

* `cmd/gateway/main.go` - HTTP server, config loading, fallback proxy
* `internal/routing/routing.go` - `p2p-a.example.com` → `https://a.example.com`
* `internal/signaling/hub.go` - WebSocket room, peer addressing
* `internal/webrtc/peer.go` - pion PeerConnection lifecycle
* `internal/proxy/proxy.go` - JSON-over-DataChannel HTTP forwarder
* `web/files/index.html` - the bootstrap page
* `web/files/client.js` - registers the SW, opens WS, runs WebRTC
* `web/files/p2p-sw.js` - Service Worker that tunnels fetch()
* `config.yaml` - runtime configuration

## Testing

```bash
go test ./...
```

Currently covers:

* `internal/routing` - host prefix parsing
* `internal/proxy` - GET/POST forward, header handling, error envelopes

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| Page shows "Status: Connecting" forever | Signaling WS failed - check that the gateway is reachable, TLS cert covers the host. |
| `ICE: failed` | Both sides are behind symmetric NAT. Add a TURN server. |
| `DataChannel: CLOSED` | The gateway PeerConnection closed prematurely. Check gateway logs for `[webrtc]` errors. |
| Requests hang in the SW | The DataChannel message timed out. Look at `[webrtc] handler error` lines. |
| `bad host` from `/upstream/` | The Host header does not start with `prefix-` or does not match `base_domain`. |
| `dial tcp: i/o timeout` to upstream | The upstream host is unreachable. The gateway uses Go's default `http.Client`; check DNS / routing. |

Enable verbose logging by setting `LOG_LEVEL=debug` (currently the
gateway uses the standard `log` package; in production, pipe through
`journalctl` or any structured log shipper).

## Acknowledgements

Architecture inspired by:

* [`andrewmthomas87/web-p2p-tunnel`](https://github.com/andrewmthomas87/web-p2p-tunnel) - Service Worker + WebRTC DataChannel + Go reverse proxy.
* [`ambianic/peerfetch`](https://github.com/ambianic/peerfetch) - HTTP over WebRTC DataChannel.

No code was copied from either project.