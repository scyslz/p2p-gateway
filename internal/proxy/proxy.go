// Package proxy serves HTTP and WebSocket requests received over a
// WebRTC DataChannel and writes responses back on the same channel.
//
// Protocol v2 (JSON over text DataChannel):
//
//	HTTP request:
//	  {"type":"request","id":"...","method":"GET","path":"/x",
//	   "headers":{...},"body":"<base64>"}
//	HTTP response:
//	  {"type":"response","id":"...","status":200,
//	   "headers":{...},"body":"<base64>"}
//
//	WebSocket:
//	  {"type":"ws-open","id":"...","path":"/ws","headers":{...}}
//	  ← {"type":"ws-open-ok","id":"..."} | {"type":"ws-open-err","id":"...","error":"..."}
//	  {"type":"ws-data","id":"...","data":"<base64>","binary":false}
//	  {"type":"ws-close","id":"...","code":1000,"reason":""}
//	  ← {"type":"ws-closed","id":"...","code":1000}
package proxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	wrtc "github.com/example/p2p-gateway/internal/webrtc"
)

// Handler builds a RequestHandler that dispatches to the given target.
// The target can be http(s) or ws(s).
func Handler(target string) wrtc.RequestHandler {
	return func(p *wrtc.Peer, raw []byte) ([]byte, error) {
		client := testClient()
		// Disable automatic redirect following — we handle redirects manually
		// in forwardHTTP with origin and scheme validation.
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, fmt.Errorf("decode envelope: %w", err)
		}

		switch envelope.Type {
		case "request":
			var req Request
			if err := json.Unmarshal(raw, &req); err != nil {
				return nil, fmt.Errorf("decode request: %w", err)
			}
			log.Printf("[proxy] %s %s → %s", req.Method, req.Path, target)
			resp, err := forwardHTTP(client, target, &req)
			if err != nil {
				log.Printf("[proxy] forward error: %v", err)
				return json.Marshal(Response{
					Type:    "response",
					ID:      req.ID,
					Status:  http.StatusBadGateway,
					Headers: map[string]string{"content-type": "text/plain"},
					BodyB64: base64.StdEncoding.EncodeToString([]byte("gateway error: " + err.Error())),
				})
			}
			return json.Marshal(resp)

		case "ws-open":
			var msg WSOpen
			if err := json.Unmarshal(raw, &msg); err != nil {
				return nil, fmt.Errorf("decode ws-open: %w", err)
			}
			go handleWSOpen(p, target, &msg)
			return nil, nil

		case "ws-data":
			var msg WSData
			if err := json.Unmarshal(raw, &msg); err != nil {
				return nil, fmt.Errorf("decode ws-data: %w", err)
			}
			p.RouteWSData(msg.ID, msg.Data, msg.Binary)
			return nil, nil

		case "ws-close":
			var msgWSClose WSClose
			if err := json.Unmarshal(raw, &msgWSClose); err != nil {
				return nil, fmt.Errorf("decode ws-close: %w", err)
			}
			p.CloseWSStream(msgWSClose.ID, msgWSClose.Code, msgWSClose.Reason)
			return nil, nil

		default:
			return nil, fmt.Errorf("unknown type %q", envelope.Type)
		}
	}
}

type Request struct {
	Type    string            `json:"type"`
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	BodyB64 string            `json:"body"`
}

type Response struct {
	Type    string            `json:"type"`
	ID      string            `json:"id"`
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	BodyB64 string            `json:"body"`
}

type WSOpen struct {
	Type    string            `json:"type"`
	ID      string            `json:"id"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
}

type WSData struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Data   string `json:"data"`
	Binary bool   `json:"binary"`
}

type WSClose struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Code   int    `json:"code"`
	Reason string `json:"reason"`
}

const maxRedirects = 10

// forwardHTTP proxies an HTTP request to the target, automatically following
// redirects (301/302/303/307/308) within the same origin. Cross-origin redirects
// and HTTPS→HTTP downgrades are blocked.
func forwardHTTP(client *http.Client, target string, in *Request) (*Response, error) {
	if !strings.HasPrefix(target, "https://") && !strings.HasPrefix(target, "http://") {
		return nil, fmt.Errorf("target must be http(s) for HTTP requests")
	}

	targetURL, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("parse target: %w", err)
	}
	targetHost := strings.ToLower(targetURL.Hostname())
	targetScheme := strings.ToLower(targetURL.Scheme)

	// Build the initial request URL.
	rel, err := url.Parse(in.Path)
	if err != nil {
		return nil, fmt.Errorf("parse path: %w", err)
	}
	// Strip internal query params (target, etc.) — don't forward to upstream.
	q := rel.Query()
	q.Del("target")
	rel.RawQuery = q.Encode()
	reqURL := *targetURL
	reqURL.Path = singleSlash(reqURL.Path, rel.Path)
	reqURL.RawQuery = rel.RawQuery
	log.Printf("[proxy] resolved URL: %s", reqURL.String())

	// Parse body once — may be reused across redirects.
	var bodyBytes []byte
	if in.BodyB64 != "" {
		bodyBytes, err = base64.StdEncoding.DecodeString(in.BodyB64)
		if err != nil {
			return nil, fmt.Errorf("decode body: %w", err)
		}
	}

	originalMethod := strings.ToUpper(in.Method)
	if originalMethod == "" {
		originalMethod = "GET"
	}
	currentMethod := originalMethod

	skip := map[string]struct{}{
		"host": {}, "connection": {}, "upgrade": {}, "keep-alive": {},
		"proxy-authenticate": {}, "proxy-authorization": {}, "te": {},
		"trailers": {}, "transfer-encoding": {},
	}

	// Follow redirects manually with origin + scheme validation.
	var lastResp *http.Response
	for redirectCount := 0; redirectCount <= maxRedirects; redirectCount++ {

		var bodyReader io.Reader
		if len(bodyBytes) > 0 {
			bodyReader = strings.NewReader(string(bodyBytes))
		}

		req, err := http.NewRequestWithContext(context.Background(), currentMethod, reqURL.String(), bodyReader)
		if err != nil {
			return nil, fmt.Errorf("new request: %w", err)
		}

		for k, v := range in.Headers {
			lk := strings.ToLower(k)
			if _, drop := skip[lk]; drop {
				continue
			}
			req.Header.Set(k, v)
		}
		req.Host = reqURL.Host

		if lastResp != nil {
			lastResp.Body.Close()
		}
		log.Printf("[proxy] >>> %s %s Host=%s", currentMethod, reqURL.String(), req.Host)
		lastResp, err = client.Do(req)
		if err != nil {
			log.Printf("[proxy] <<< err=%v", err)
			return nil, fmt.Errorf("upstream do: %w", err)
		}
		log.Printf("[proxy] <<< status=%d", lastResp.StatusCode)

		// Only auto-follow redirect status codes.
		if lastResp.StatusCode != 301 && lastResp.StatusCode != 302 &&
			lastResp.StatusCode != 303 && lastResp.StatusCode != 307 &&
			lastResp.StatusCode != 308 {
				break
		}

		locHeader := lastResp.Header.Get("Location")
		if locHeader == "" {
			break
		}

		locURL, err := url.Parse(locHeader)
		if err != nil {
			break
		}

		// Resolve relative Location against current request URL.
		nextURL := reqURL.ResolveReference(locURL)

		// --- Security checks ---
		nextHost := strings.ToLower(nextURL.Hostname())
		nextScheme := strings.ToLower(nextURL.Scheme)

		// 1. Block cross-origin redirect.
		if nextHost != targetHost {
			lastResp.Body.Close()
				log.Printf("[proxy] blocked cross-origin redirect from=%s to=%s", reqURL.String(), nextURL.String())
			return errorResponse(in.ID, http.StatusBadGateway, "blocked cross-origin redirect"), nil
		}

		// 2. Block HTTPS → HTTP downgrade.
		if targetScheme == "https" && nextScheme == "http" {
			lastResp.Body.Close()
				log.Printf("[proxy] blocked insecure redirect from=%s to=%s", reqURL.String(), nextURL.String())
			return errorResponse(in.ID, http.StatusBadGateway, "blocked insecure redirect"), nil
		}

		log.Printf("[proxy] redirect %d: %s → %s", redirectCount, reqURL.String(), nextURL.String())

		// 3. For 301/302/303: change POST to GET per HTTP spec.
		if lastResp.StatusCode == 301 || lastResp.StatusCode == 302 || lastResp.StatusCode == 303 {
			currentMethod = "GET"
			bodyBytes = nil // No body for GET.
		}
		// 307/308: preserve original method and body.

		reqURL = *nextURL
	}

	if lastResp == nil {
		return nil, fmt.Errorf("no response from upstream")
	}
	defer lastResp.Body.Close()

	// If the final response is still a redirect, we hit the limit.
	if lastResp.StatusCode == 301 || lastResp.StatusCode == 302 ||
		lastResp.StatusCode == 303 || lastResp.StatusCode == 307 ||
		lastResp.StatusCode == 308 {
		lastResp.Body.Close()
		log.Printf("[proxy] redirect limit exceeded (max %d)", maxRedirects)
		return errorResponse(in.ID, http.StatusBadGateway, "redirect limit exceeded"), nil
	}

	body, err := io.ReadAll(lastResp.Body)
	lastResp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	out := &Response{
		Type:    "response",
		ID:      in.ID,
		Status:  lastResp.StatusCode,
		Headers: make(map[string]string, len(lastResp.Header)),
		BodyB64: base64.StdEncoding.EncodeToString(body),
	}
	for k, vs := range lastResp.Header {
		if len(vs) == 0 {
			continue
		}
		// Rewrite Location header to strip target origin.
		if strings.EqualFold(k, "location") {
			for _, v := range vs {
				if resolved, err2 := url.Parse(v); err2 == nil {
					abs := lastResp.Request.URL.ResolveReference(resolved)
					if strings.ToLower(abs.Hostname()) == targetHost {
						v = abs.RequestURI()
					}
				}
				out.Headers[k] = v
			}
			continue
		}
		out.Headers[k] = strings.Join(vs, ", ")
	}

	// Inject <base> tag for HTML responses so relative URLs resolve to upstream.
	ct := lastResp.Header.Get("Content-Type")
	if strings.Contains(ct, "text/html") {
		baseTag := `<base href="` + target + `">`
		html := string(body)
		if strings.Contains(html, "<head") {
			html = strings.Replace(html, "<head", "<head>"+baseTag, 1)
		} else if strings.Contains(html, "<HEAD") {
			html = strings.Replace(html, "<HEAD", "<HEAD>"+baseTag, 1)
		} else {
			html = baseTag + html
		}
		out.BodyB64 = base64.StdEncoding.EncodeToString([]byte(html))
	}

	return out, nil
}

// handleWSOpen dials the upstream WebSocket and bridges it to the DataChannel.
func handleWSOpen(p *wrtc.Peer, target string, msg *WSOpen) {
	// Convert http(s) target to ws(s) if needed.
	wsTarget := target
	if strings.HasPrefix(target, "http://") {
		wsTarget = "ws://" + target[len("http://"):]
	} else if strings.HasPrefix(target, "https://") {
		wsTarget = "wss://" + target[len("https://"):]
	}

	if !strings.HasPrefix(wsTarget, "ws://") && !strings.HasPrefix(wsTarget, "wss://") {
		sendWSError(p, msg.ID, "target must be ws(s) or http(s)")
		return
	}

	u, err := url.Parse(wsTarget)
	if err != nil {
		sendWSError(p, msg.ID, "parse target: "+err.Error())
		return
	}
	rel, err := url.Parse(msg.Path)
	if err != nil {
		sendWSError(p, msg.ID, "parse path: "+err.Error())
		return
	}
	u.Path = singleSlash(u.Path, rel.Path)
	u.RawQuery = rel.RawQuery

	header := http.Header{}
	for k, v := range msg.Headers {
		lk := strings.ToLower(k)
		if lk == "host" || lk == "connection" || lk == "upgrade" {
			continue
		}
		header.Set(k, v)
	}

	dialer := &websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(u.String(), header)
	if err != nil {
		sendWSError(p, msg.ID, "dial: "+err.Error())
		return
	}

	log.Printf("[proxy] ws-open id=%s target=%s", msg.ID, u.String())

	p.RegisterWSStream(msg.ID, conn)

	// Send OK back.
	p.SendDC(mustJSON(map[string]interface{}{
		"type": "ws-open-ok",
		"id":   msg.ID,
	}))

	// Read pump: upstream → DataChannel
	for {
		mt, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[proxy] ws upstream read id=%s err=%v", msg.ID, err)
			code := websocket.CloseNormalClosure
			reason := ""
			if ce, ok := err.(*websocket.CloseError); ok {
				code = ce.Code
				reason = ce.Text
			}
			p.SendDC(mustJSON(map[string]interface{}{
				"type":   "ws-closed",
				"id":     msg.ID,
				"code":   code,
				"reason": reason,
			}))
			p.UnregisterWSStream(msg.ID)
			return
		}

		isBinary := mt == websocket.BinaryMessage
		dataB64 := base64.StdEncoding.EncodeToString(message)

		p.SendDC(mustJSON(map[string]interface{}{
			"type":   "ws-data",
			"id":     msg.ID,
			"data":   dataB64,
			"binary": isBinary,
		}))
	}
}

func sendWSError(p *wrtc.Peer, id, errMsg string) {
	p.SendDC(mustJSON(map[string]interface{}{
		"type":  "ws-open-err",
		"id":    id,
		"error": errMsg,
	}))
}

func mustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
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

func errorResponse(id string, status int, msg string) *Response {
	return &Response{
		Type:    "response",
		ID:      id,
		Status:  status,
		Headers: map[string]string{"content-type": "text/plain"},
		BodyB64: base64.StdEncoding.EncodeToString([]byte(msg)),
	}
}

var testClientOverride *http.Client

func testClient() *http.Client {
	if testClientOverride != nil {
		return testClientOverride
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func SetTestClient(c *http.Client) { testClientOverride = c }
