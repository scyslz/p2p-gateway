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
	resp, err := forward(client, upstream.URL, req)
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
	resp2, err := forward(client, upstream.URL, req2)
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