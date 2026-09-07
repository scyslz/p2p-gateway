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
	client := testClient()
	return func(p *wrtc.Peer, raw []byte) ([]byte, error) {
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

// forwardHTTP proxies an HTTP request to the target.
func forwardHTTP(client *http.Client, target string, in *Request) (*Response, error) {
	if !strings.HasPrefix(target, "https://") && !strings.HasPrefix(target, "http://") {
		return nil, fmt.Errorf("target must be http(s) for HTTP requests")
	}

	u, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("parse target: %w", err)
	}
	rel, err := url.Parse(in.Path)
	if err != nil {
		return nil, fmt.Errorf("parse path: %w", err)
	}
	u.Path = singleSlash(u.Path, rel.Path)
	u.RawQuery = rel.RawQuery

	var bodyReader io.Reader
	if in.BodyB64 != "" {
		b, err := base64.StdEncoding.DecodeString(in.BodyB64)
		if err != nil {
			return nil, fmt.Errorf("decode body: %w", err)
		}
		bodyReader = strings.NewReader(string(b))
	}

	method := strings.ToUpper(in.Method)
	if method == "" {
		method = "GET"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, u.String(), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}

	skip := map[string]struct{}{
		"host": {}, "connection": {}, "upgrade": {}, "keep-alive": {},
		"proxy-authenticate": {}, "proxy-authorization": {}, "te": {},
		"trailers": {}, "transfer-encoding": {},
	}
	for k, v := range in.Headers {
		lk := strings.ToLower(k)
		if _, drop := skip[lk]; drop {
			continue
		}
		req.Header.Set(k, v)
	}
	req.Host = u.Hostname()

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("upstream do: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	out := &Response{
		Type:    "response",
		ID:      in.ID,
		Status:  resp.StatusCode,
		Headers: make(map[string]string, len(resp.Header)),
		BodyB64: base64.StdEncoding.EncodeToString(body),
	}
	for k, vs := range resp.Header {
		if len(vs) == 0 {
			continue
		}
		out.Headers[k] = strings.Join(vs, ", ")
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

var testClientOverride *http.Client

func testClient() *http.Client {
	if testClientOverride != nil {
		return testClientOverride
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func SetTestClient(c *http.Client) { testClientOverride = c }
