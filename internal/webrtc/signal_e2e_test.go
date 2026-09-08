package webrtc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"

	"github.com/example/p2p-gateway/internal/signaling"
)

// End-to-end: gateway is the offerer (creates offer + DataChannel).
// Browser-side client answers over the same WebSocket, DataChannel opens,
// and an HTTP request frame gets a proxied response.
func TestSinglePeerOfferAnswerE2E(t *testing.T) {
	hub := signaling.NewHub()
	mgr, err := NewManager(Config{}, func(p *Peer, raw []byte) ([]byte, error) {
		var req struct {
			Type string `json:"type"`
			ID   string `json:"id"`
			Path string `json:"path"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
		body := base64.StdEncoding.EncodeToString([]byte("upstream-ok:" + req.Path))
		return json.Marshal(map[string]interface{}{
			"type": "response", "id": req.ID, "status": 200,
			"headers": map[string]string{"content-type": "text/plain"}, "body": body,
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.ServeWS(w, r, func(p *signaling.Peer) {
			if err := mgr.HandleSignalingPeer(p); err != nil {
				t.Logf("handle peer: %v", err)
				return
			}
			if pc := mgr.Peer(p.ID); pc != nil {
				pc.SetTarget("http://127.0.0.1:9") // unused by mock handler
			}
		})
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/_signal"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial signal: %v", err)
	}
	defer conn.Close()

	// Browser-side peer (answerer).
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	dcOpen := make(chan struct{})
	var dcOpened bool
	gotResp := make(chan []byte, 1)

	// Need the DataChannel object for sending: capture via channel.
	dcCh := make(chan *webrtc.DataChannel, 1)
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnOpen(func() {
			if !dcOpened {
				dcOpened = true
				close(dcOpen)
			}
		})
		dc.OnMessage(func(m webrtc.DataChannelMessage) {
			var env struct{ Type string `json:"type"` }
			_ = json.Unmarshal(m.Data, &env)
			if env.Type == "ready" || env.Type == "ready-ack" || env.Type == "ping" || env.Type == "pong" {
				return
			}
			select {
			case gotResp <- m.Data:
			default:
			}
		})
		select {
		case dcCh <- dc:
		default:
		}
	})

	sendSig := func(v interface{}) {
		b, _ := json.Marshal(v)
		_ = conn.WriteMessage(websocket.TextMessage, b)
	}
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		sendSig(map[string]interface{}{"type": "candidate", "candidate": c.ToJSON()})
	})

	// Pump signaling until DC opens.
	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case <-dcOpen:
			goto OPEN
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for DataChannel open")
		}
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			continue // timeout: re-check dcOpen
		}
		var env struct {
			Type      string          `json:"type"`
			SDP       json.RawMessage `json:"sdp"`
			Candidate json.RawMessage `json:"candidate"`
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			continue
		}
		switch env.Type {
		case "join":
			// nothing to do
		case "offer":
			var offer webrtc.SessionDescription
			if err := json.Unmarshal(env.SDP, &offer); err != nil {
				t.Fatalf("bad offer: %v", err)
			}
			if err := pc.SetRemoteDescription(offer); err != nil {
				t.Fatalf("set remote: %v", err)
			}
			answer, err := pc.CreateAnswer(nil)
			if err != nil {
				t.Fatalf("create answer: %v", err)
			}
			if err := pc.SetLocalDescription(answer); err != nil {
				t.Fatalf("set local: %v", err)
			}
			sendSig(map[string]interface{}{"type": "answer", "sdp": answer})
		case "candidate":
			var c webrtc.ICECandidateInit
			if err := json.Unmarshal(env.Candidate, &c); err != nil {
				continue
			}
			_ = pc.AddICECandidate(c)
		}
	}
OPEN:
	var dc *webrtc.DataChannel
	select {
	case dc = <-dcCh:
	case <-time.After(5 * time.Second):
		t.Fatal("no DataChannel object captured")
	}
	time.Sleep(300 * time.Millisecond)
	drain := func() {
		for {
			select {
			case <-gotResp:
			default:
				return
			}
		}
	}
	drain()
	req := fmt.Sprintf(`{"type":"request","id":"r1","method":"GET","path":"/hello?x=1","headers":{},"body":""}`)
	if err := dc.SendText(req); err != nil {
		t.Fatalf("dc send: %v", err)
	}
	deadline2 := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline2) {
			t.Fatal("timeout waiting for DC response")
		}
		select {
		case raw := <-gotResp:
			var resp struct {
				Type    string `json:"type"`
				ID      string `json:"id"`
				Status  int    `json:"status"`
				BodyB64 string `json:"body"`
			}
			if err := json.Unmarshal(raw, &resp); err != nil {
				continue
			}
			if resp.Type != "response" {
				continue
			}
			body, _ := base64.StdEncoding.DecodeString(resp.BodyB64)
			if resp.Status != 200 || !strings.Contains(string(body), "upstream-ok:/hello") {
				t.Fatalf("unexpected proxied body: status=%d body=%q", resp.Status, body)
			}
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}
