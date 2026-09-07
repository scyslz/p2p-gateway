package proxy

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mock peer just satisfies the type that Handler returns from.
type mockPeer struct{}

func TestForward(t *testing.T) {
	// Mock upstream: returns "ok" for GET /, echoes POST body for /echo.
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "yes")
		switch r.URL.Path {
		case "/":
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "hello upstream")
		case "/echo":
			b, _ := io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(b)
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	// http.Client that trusts the upstream's self-signed cert.
	client := upstream.Client()

	// GET /
	req := &Request{
		Type:    "request",
		ID:      "1",
		Method:  "GET",
		Path:    "/",
		Headers: map[string]string{"X-Foo": "bar"},
		BodyB64: "",
	}
	// We don't go through the wrapper - call forward() directly.
	resp, err := forwardHTTP(client, upstream.URL, req)
	if err != nil {
		t.Fatalf("forward GET /: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d", resp.Status)
	}
	body, _ := base64.StdEncoding.DecodeString(resp.BodyB64)
	if string(body) != "hello upstream" {
		t.Errorf("body = %q", string(body))
	}
	if resp.Headers["X-Upstream"] != "yes" {
		t.Errorf("missing X-Upstream header: %v", resp.Headers)
	}

	// POST /echo with body
	postBody := []byte("payload=1")
	req2 := &Request{
		Type:    "request",
		ID:      "2",
		Method:  "POST",
		Path:    "/echo",
		Headers: map[string]string{"Content-Type": "text/plain"},
		BodyB64: base64.StdEncoding.EncodeToString(postBody),
	}
	resp2, err := forwardHTTP(client, upstream.URL, req2)
	if err != nil {
		t.Fatalf("forward POST: %v", err)
	}
	got, _ := base64.StdEncoding.DecodeString(resp2.BodyB64)
	if string(got) != string(postBody) {
		t.Errorf("echo body = %q, want %q", string(got), string(postBody))
	}
}

func TestHandlerJSONShape(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	defer upstream.Close()

	// We need Handler to use a client that trusts upstream's self-signed
	// cert.  Patch the package via a setter hook.
	SetTestClient(upstream.Client())
	defer SetTestClient(nil)

	h := Handler(upstream.URL)
	rawReq, _ := json.Marshal(Request{
		Type:    "request",
		ID:      "abc",
		Method:  "GET",
		Path:    "/",
		Headers: map[string]string{},
	})
	out, err := h(nil, rawReq)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	var resp Response
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("response not json: %v: %s", err, string(out))
	}
	if resp.Type != "response" || resp.ID != "abc" || resp.Status != 204 {
		t.Errorf("bad fields: %+v", resp)
	}
	if !strings.HasPrefix(resp.BodyB64, "") {
		// empty body still base64-decodes to empty string; ensure non-nil.
		b, _ := base64.StdEncoding.DecodeString(resp.BodyB64)
		if len(b) != 0 {
			t.Errorf("expected empty body, got %q", string(b))
		}
	}
}
func TestRedirectFollow(t *testing.T) {
	// Test: 302 redirect within same origin is followed automatically.
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login":
			http.Redirect(w, r, "/dashboard", http.StatusFound)
		case "/dashboard":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(w, "welcome")
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	SetTestClient(upstream.Client())
	defer SetTestClient(nil)

	h := Handler(upstream.URL)
	rawReq, _ := json.Marshal(Request{
		Type: "request", ID: "r1", Method: "GET",
		Path: "/login", Headers: map[string]string{},
	})
	out, err := h(nil, rawReq)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	var resp Response
	_ = json.Unmarshal(out, &resp)
	if resp.Status != 200 {
		t.Errorf("expected 200 after redirect, got %d", resp.Status)
	}
	body, _ := base64.StdEncoding.DecodeString(resp.BodyB64)
	if string(body) != "welcome" {
		t.Errorf("body = %q, want %q", string(body), "welcome")
	}
}

func TestRedirectCrossOriginBlocked(t *testing.T) {
	// Test: redirect to different origin is blocked.
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://evil.com/steal", http.StatusFound)
	}))
	defer upstream.Close()

	SetTestClient(upstream.Client())
	defer SetTestClient(nil)

	h := Handler(upstream.URL)
	rawReq, _ := json.Marshal(Request{
		Type: "request", ID: "r2", Method: "GET",
		Path: "/redirect", Headers: map[string]string{},
	})
	out, err := h(nil, rawReq)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	var resp Response
	_ = json.Unmarshal(out, &resp)
	if resp.Status != 502 {
		t.Errorf("expected 502 for cross-origin redirect, got %d", resp.Status)
	}
}

func TestRedirectHTTPDowngradeBlocked(t *testing.T) {
	// Test: HTTPS → HTTP redirect is blocked.
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.com/login", http.StatusFound)
	}))
	defer upstream.Close()

	SetTestClient(upstream.Client())
	defer SetTestClient(nil)

	h := Handler(upstream.URL)
	rawReq, _ := json.Marshal(Request{
		Type: "request", ID: "r3", Method: "GET",
		Path: "/downgrade", Headers: map[string]string{},
	})
	out, err := h(nil, rawReq)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	var resp Response
	_ = json.Unmarshal(out, &resp)
	if resp.Status != 502 {
		t.Errorf("expected 502 for HTTP downgrade redirect, got %d", resp.Status)
	}
}

func TestRedirectLoopBlocked(t *testing.T) {
	// Test: redirect loop triggers max redirect limit.
	loopCount := 0
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loopCount++
		if r.URL.Path == "/a" {
			http.Redirect(w, r, "/b", http.StatusFound)
		} else {
			http.Redirect(w, r, "/a", http.StatusFound)
		}
	}))
	defer upstream.Close()

	SetTestClient(upstream.Client())
	defer SetTestClient(nil)

	h := Handler(upstream.URL)
	rawReq, _ := json.Marshal(Request{
		Type: "request", ID: "r4", Method: "GET",
		Path: "/a", Headers: map[string]string{},
	})
	out, err := h(nil, rawReq)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	var resp Response
	_ = json.Unmarshal(out, &resp)
	// Should fail with 502 after maxRedirects (10) iterations.
	if resp.Status != 502 {
		t.Errorf("expected 502 for redirect loop, got %d", resp.Status)
	}
	if loopCount > 11 {
		t.Errorf("too many redirects: %d", loopCount)
	}
}

func TestRedirectPostPreserved307(t *testing.T) {
	// Test: 307 preserves POST method.
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/test" && r.Method == "POST" {
			http.Redirect(w, r, "/result", http.StatusTemporaryRedirect)
		} else if r.URL.Path == "/result" && r.Method == "POST" {
			b, _ := io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write(b)
		} else {
			w.WriteHeader(405)
		}
	}))
	defer upstream.Close()

	SetTestClient(upstream.Client())
	defer SetTestClient(nil)

	h := Handler(upstream.URL)
	rawReq, _ := json.Marshal(Request{
		Type: "request", ID: "r5", Method: "POST",
		Path: "/test", Headers: map[string]string{"Content-Type": "text/plain"},
		BodyB64: base64.StdEncoding.EncodeToString([]byte("data")),
	})
	out, err := h(nil, rawReq)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	var resp Response
	_ = json.Unmarshal(out, &resp)
	if resp.Status != 200 {
		t.Errorf("expected 200, got %d", resp.Status)
	}
	body, _ := base64.StdEncoding.DecodeString(resp.BodyB64)
	if string(body) != "data" {
		t.Errorf("body = %q, want %q", string(body), "data")
	}
}
