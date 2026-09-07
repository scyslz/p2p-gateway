package proxy

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// TestRealProxyLoop is a partial end-to-end test: it simulates a frame
// arriving from the DataChannel and verifies the gateway-to-upstream
// round trip.
//
//  1. Start a mock upstream HTTPS server.
//  2. Run the same code path the DataChannel handler runs: decode JSON
//     request → forward → encode JSON response.
func TestRealProxyLoop(t *testing.T) {
	var (
		mu      sync.Mutex
		got     []string
		gotBody []byte
	)

	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, r.Method+" "+r.URL.RequestURI())
		b, _ := io.ReadAll(r.Body)
		gotBody = append(gotBody, b...)
		mu.Unlock()

		w.Header().Set("X-Upstream", "yes")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello from upstream"))
	}))
	defer upstream.Close()
	SetTestClient(upstream.Client())
	defer SetTestClient(nil)

	// Simulate a frame arriving from the DataChannel.
	in := []byte(`{"type":"request","id":"x1","method":"GET","path":"/hello?q=1","headers":{"X-Foo":"bar"},"body":""}`)

	out, err := Handler(upstream.URL)(nil, in)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	var resp Response
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("response: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d", resp.Status)
	}
	body, err := base64.StdEncoding.DecodeString(resp.BodyB64)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if string(body) != "hello from upstream" {
		t.Errorf("body = %q", body)
	}
	if resp.Headers["X-Upstream"] != "yes" {
		t.Errorf("missing X-Upstream: %v", resp.Headers)
	}

	// Confirm the upstream saw the request.
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "GET /hello?q=1" {
		t.Errorf("upstream got %v, want [GET /hello?q=1]", got)
	}
}

// TestErrorResponse verifies that an upstream error still produces a
// well-formed JSON response (so the Service Worker does not hang).
func TestErrorResponse(t *testing.T) {
	// Use an invalid URL that will fail to dial.
	SetTestClient(&http.Client{Timeout: 1})
	defer SetTestClient(nil)

	in := []byte(`{"type":"request","id":"e1","method":"GET","path":"/","headers":{},"body":""}`)
	out, err := Handler("http://this-domain-does-not-exist.invalid")(nil, in)
	if err != nil {
		t.Fatalf("handler returned err: %v", err)
	}
	var resp Response
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("response: %v", err)
	}
	if resp.Type != "response" || resp.ID != "e1" {
		t.Errorf("bad fields: %+v", resp)
	}
	if resp.Status == 200 {
		t.Errorf("expected non-200 status, got %d", resp.Status)
	}
}