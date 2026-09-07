// Command gateway is the P2P HTTP tunnel gateway.
//
// It serves three things on a single listener:
//
//  1. Static bootstrap files: /, /client.js, /p2p-sw.js.
//  2. A WebSocket signaling endpoint at /_signal.
//  3. A reverse-proxy fallback at /upstream for when WebRTC is not
//     connected.
//
// Usage:
//
//	gateway                        # host-based routing (p2p-foo.example.com → https://foo.example.com)
//	gateway --target https://foo   # every peer proxies to this target
//	gateway --target ws://foo:8080 # every peer proxies to this WebSocket target
//
// Query param on /_signal: ?target=<url> overrides the global target per-peer.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
	"github.com/example/p2p-gateway/internal/proxy"
	"github.com/example/p2p-gateway/internal/routing"
	"github.com/example/p2p-gateway/internal/signaling"
	wrtc "github.com/example/p2p-gateway/internal/webrtc"
	webui "github.com/example/p2p-gateway/web"
)

var (
	webFS       fs.FS
	gatewayCfg  *Config
)

func init() {
	if sub, err := fs.Sub(os.DirFS("."), "web/files"); err == nil {
		if _, err := fs.ReadFile(sub, "index.html"); err == nil {
			webFS = sub
			return
		}
	}
	webFS = webui.StripPrefix()
}

type Config struct {
	ListenAddr   string `yaml:"listen_addr"`
	TLSCert      string `yaml:"tls_cert"`
	TLSKey       string `yaml:"tls_key"`
	BaseDomain   string `yaml:"base_domain"`
	Prefix       string `yaml:"prefix"`
	STUNURL      string `yaml:"stun_url"`
	Scheme       string `yaml:"scheme"`
	UpstreamPort int    `yaml:"upstream_port"`
	Target       string `yaml:"target"` // fixed upstream target; overrides host routing
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.STUNURL == "" {
		c.STUNURL = "stun:stun.l.google.com:19302"
	}
	if c.ListenAddr == "" {
		c.ListenAddr = ":8080"
	}
	if c.Scheme == "" {
		c.Scheme = "http"
	}
	// CLI --target flag overrides config file.
	for i, arg := range os.Args[1:] {
		if arg == "--target" && i+2 < len(os.Args) {
			c.Target = os.Args[i+2]
		}
	}
	return &c, nil
}

func main() {
	cfgPath := "config.yaml"
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "--") {
		cfgPath = os.Args[1]
	}
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	gatewayCfg = cfg

	targetDesc := cfg.Target
	if targetDesc == "" {
		targetDesc = "(host routing)"
	}
	log.Printf("[boot] config: listen=%s base=%s prefix=%q stun=%s scheme=%s target=%s",
		cfg.ListenAddr, cfg.BaseDomain, cfg.Prefix, cfg.STUNURL, cfg.Scheme, targetDesc)

	resolver := routing.NewResolver(routing.Config{
		BaseDomain: cfg.BaseDomain,
		Prefix:     cfg.Prefix,
		Scheme:     cfg.Scheme,
	})

	hub := signaling.NewHub()
	mgr, err := wrtc.NewManager(wrtc.Config{STUNURL: cfg.STUNURL}, proxyFactory(resolver))
	if err != nil {
		log.Fatalf("init webrtc manager: %v", err)
	}

	mux := http.NewServeMux()

	// Signaling endpoint: supports ?target=<url> to override upstream.
	mux.HandleFunc("/_signal", func(w http.ResponseWriter, r *http.Request) {
		// Resolve target: query param > global config > host routing.
		target := resolveTarget(r)

		hub.ServeWS(w, r, func(p *signaling.Peer) {
			if err := mgr.HandleSignalingPeer(p); err != nil {
				log.Printf("[signal] handle peer: %v", err)
				return
			}
			if pc := mgr.Peer(p.ID); pc != nil {
				pc.SetTarget(target)
			}
		})
	})

	// Reverse-proxy fallback.
	mux.HandleFunc("/upstream/", func(w http.ResponseWriter, r *http.Request) {
		handleDirect(resolver, w, r)
	})
	mux.HandleFunc("/upstream", func(w http.ResponseWriter, r *http.Request) {
		handleDirect(resolver, w, r)
	})

	// Static bootstrap files.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Bypass browser cache — force fresh load.
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		serveStatic(w, r)
	})

	// Graceful shutdown.
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		log.Printf("[boot] shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		log.Printf("[boot] listening on %s (TLS)", cfg.ListenAddr)
		if err := srv.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	} else {
		log.Printf("[boot] listening on %s (HTTP, no TLS)", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}
}

// resolveTarget picks the upstream for a signaling peer.
// Priority: ?target= query param > global config Target > host direct > host p2p-prefix routing.
func resolveTarget(r *http.Request) string {
	// 1. Query param — highest priority, use as-is
	if t := r.URL.Query().Get("target"); t != "" {
		if !strings.Contains(t, "://") {
			t = "http://" + t
		}
		log.Printf("[signal] per-peer target from query: %s", t)
		return t
	}
	// 2. Global config target
	if gatewayCfg.Target != "" {
		return gatewayCfg.Target
	}
	// 3. Derive target from Host header
	host := r.Host
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	// Strip p2p- prefix if present → use the rest as upstream domain
	stripped := strings.TrimPrefix(host, gatewayCfg.Prefix)
	if stripped != host && stripped != "" {
		// Had the prefix → route to the stripped domain
		scheme := gatewayCfg.Scheme
		if scheme == "" {
			scheme = "http"
		}
		target := scheme + "://" + stripped
		log.Printf("[signal] host p2p routing: %s → %s", host, target)
		return target
	}
	// 4. No prefix — use the host directly as the upstream domain.
	//    Works for IPs (192.168.x.x), any domain, or localhost.
	scheme := gatewayCfg.Scheme
	if scheme == "" {
		scheme = "http"
	}
	target := scheme + "://" + host
	log.Printf("[signal] host direct: %s → %s", host, target)
	return target
}

func proxyFactory(resolver *routing.Resolver) wrtc.RequestHandler {
	return func(p *wrtc.Peer, raw []byte) ([]byte, error) {
		target, _ := p.Target()
		if target == "" {
			return nil, fmt.Errorf("no target for peer")
		}
		return proxy.Handler(target)(p, raw)
	}
}

func handleDirect(resolver *routing.Resolver, w http.ResponseWriter, r *http.Request) {
	// Use the same target resolution logic as signaling.
	target := resolveTarget(r)
	if target == "" {
		http.Error(w, "no target resolved", http.StatusBadRequest)
		return
	}

	path := r.URL.Path
	if strings.HasPrefix(path, "/upstream") {
		path = strings.TrimPrefix(path, "/upstream")
		if path == "" {
			path = "/"
		}
	}

	u, _ := url.Parse(target)
	rel, _ := url.Parse(path)
	u.Path = singleSlash(u.Path, rel.Path)
	u.RawQuery = r.URL.RawQuery

	req, err := http.NewRequestWithContext(r.Context(), r.Method, u.String(), r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for k, vs := range r.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Host = u.Hostname()

	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return nil
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vs := range resp.Header {
		if strings.EqualFold(k, "location") {
			vs = rewriteLocation(vs, r.Host, target)
		}
		if strings.EqualFold(k, "set-cookie") {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func rewriteLocation(values []string, gatewayHost, upstream string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://") {
			if u, err := url.Parse(v); err == nil && (u.Host == "" || strings.HasSuffix(u.Host, gatewayHost[strings.IndexByte(gatewayHost, '.')+1:])) {
				out = append(out, u.RequestURI())
				continue
			}
		}
		out = append(out, v)
	}
	return out
}

func serveStatic(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}

	b, err := fs.ReadFile(webFS, path)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	if path == "index.html" {
		page := string(b)
		target, _ := resolveFromHost(r.Host)
		stun := gatewayCfg.STUNURL
		cfgJSON, _ := json.Marshal(map[string]string{
			"host":   r.Host,
			"target": target,
			"stun":   stun,
		})
		page = strings.Replace(page, "/*__P2P_CONFIG__*/null", string(cfgJSON), 1)
		b = []byte(page)
	}

	switch {
	case strings.HasSuffix(path, ".html"):
		w.Header().Set("content-type", "text/html; charset=utf-8")
	case strings.HasSuffix(path, ".js"):
		w.Header().Set("content-type", "application/javascript")
	case strings.HasSuffix(path, ".css"):
		w.Header().Set("content-type", "text/css")
	}
	_, _ = w.Write(b)
}

func resolveFromHost(host string) (string, error) {
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if gatewayCfg == nil {
		cfgPath := "config.yaml"
		if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "--") {
			cfgPath = os.Args[1]
		}
		cfg, err := loadConfig(cfgPath)
		if err != nil {
			return "", err
		}
		gatewayCfg = cfg
	}
	if gatewayCfg.Target != "" {
		return gatewayCfg.Target, nil
	}
	// Strip p2p- prefix
	stripped := strings.TrimPrefix(host, gatewayCfg.Prefix)
	if stripped != host && stripped != "" {
		scheme := gatewayCfg.Scheme
		if scheme == "" {
			scheme = "http"
		}
		return scheme + "://" + stripped, nil
	}
	// Direct host
	scheme := gatewayCfg.Scheme
	if scheme == "" {
		scheme = "http"
	}
	return scheme + "://" + host, nil
}

func singleSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	default:
		return a + b
	}
}
