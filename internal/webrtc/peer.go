// Package webrtc wires pion/webrtc to the signaling hub and the proxy.
//
// The gateway acts as the WebRTC "answerer".  When a browser sends an
// SDP offer via the signaling channel, we create a PeerConnection,
// reply with an SDP answer, and once the DataChannel "http" opens we
// start serving HTTP and WebSocket requests over it.
package webrtc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"

	"github.com/example/p2p-gateway/internal/signaling"
)

// Peer is one browser-side WebRTC peer attached to the gateway.
type Peer struct {
	signaling *signaling.Peer
	pc        *webrtc.PeerConnection
	dc        *webrtc.DataChannel

	inbox  chan []byte
	target string

	mu     sync.Mutex
	closed bool
	onReq  RequestHandler

	// WebSocket stream multiplexing: id → upstream conn
	wsMu      sync.Mutex
	wsStreams map[string]*websocket.Conn
}

// SetTarget records the upstream URL on the peer.
func (p *Peer) SetTarget(t string) { p.target = t }

// Target returns the upstream URL.
func (p *Peer) Target() (string, error) {
	if p.target == "" {
		return "", fmt.Errorf("no target configured")
	}
	return p.target, nil
}

// RequestHandler is invoked for every HTTP request received over the
// DataChannel.  Implementations should write a JSON response back to
// the channel.
type RequestHandler func(p *Peer, req []byte) ([]byte, error)

// Config for creating PeerConnections.
type Config struct {
	STUNURL string
}

// Manager owns the API.
type Manager struct {
	api     *webrtc.API
	cfg     Config
	handler RequestHandler

	mu    sync.Mutex
	peers map[string]*Peer
}

// NewManager builds a webrtc Manager.
func NewManager(cfg Config, handler RequestHandler) (*Manager, error) {
	api := webrtc.NewAPI()
	return &Manager{api: api, cfg: cfg, handler: handler, peers: make(map[string]*Peer)}, nil
}

// Peer returns the active peer with the given signaling ID, or nil.
func (m *Manager) Peer(id string) *Peer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peers[id]
}

func (m *Manager) addPeer(p *Peer) {
	m.mu.Lock()
	m.peers[p.signaling.ID] = p
	m.mu.Unlock()
}

func (m *Manager) removePeer(id string) {
	m.mu.Lock()
	delete(m.peers, id)
	m.mu.Unlock()
}

// HandleSignalingPeer attaches to a new signaling peer and waits for an offer.
func (m *Manager) HandleSignalingPeer(sp *signaling.Peer) error {
	pc, err := m.api.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{m.cfg.STUNURL}},
		},
	})
	if err != nil {
		return fmt.Errorf("new peer connection: %w", err)
	}

	p := &Peer{
		signaling: sp,
		pc:        pc,
		onReq:     m.handler,
		inbox:     make(chan []byte, 64),
		wsStreams: make(map[string]*websocket.Conn),
	}

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		init := c.ToJSON()
		msg, _ := json.Marshal(map[string]interface{}{
			"type":      "candidate",
			"to":        sp.ID,
			"candidate": init,
		})
		select {
		case sp.Send <- msg:
		default:
		}
	})

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Printf("[webrtc] connection state: %s", s)
		if s == webrtc.PeerConnectionStateFailed ||
			s == webrtc.PeerConnectionStateClosed ||
			s == webrtc.PeerConnectionStateDisconnected {
			p.Close()
		}
	})

	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if dc.Label() != "http" {
			log.Printf("[webrtc] ignoring data channel %q", dc.Label())
			return
		}
		p.dc = dc
		log.Printf("[webrtc] data channel %q opened", dc.Label())

		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if !msg.IsString {
				log.Printf("[webrtc] ignoring non-string message")
				return
			}
			resp, err := p.onReq(p, msg.Data)
			if err != nil {
				log.Printf("[webrtc] handler error: %v", err)
				return
			}
			if resp != nil {
				if err := dc.SendText(string(resp)); err != nil {
					log.Printf("[webrtc] send response: %v", err)
				}
			}
		})

		dc.OnClose(func() {
			log.Printf("[webrtc] data channel closed")
		})
	})

	go p.runSignalingLoop(m)
	m.addPeer(p)
	return nil
}

func (p *Peer) runSignalingLoop(m *Manager) {
	defer func() {
		if m != nil {
			m.removePeer(p.signaling.ID)
		}
		p.Close()
	}()
	for raw := range p.inbox {
		var env struct {
			Type string          `json:"type"`
			SDP  json.RawMessage `json:"sdp"`
			Cand json.RawMessage `json:"candidate"`
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			log.Printf("[webrtc] bad signaling msg: %v", err)
			continue
		}
		switch env.Type {
		case "offer":
			offer := webrtc.SessionDescription{}
			if err := json.Unmarshal(env.SDP, &offer); err != nil {
				log.Printf("[webrtc] bad offer: %v", err)
				continue
			}
			if err := p.pc.SetRemoteDescription(offer); err != nil {
				log.Printf("[webrtc] set remote description: %v", err)
				continue
			}
			answer, err := p.pc.CreateAnswer(nil)
			if err != nil {
				log.Printf("[webrtc] create answer: %v", err)
				continue
			}
			if err := p.pc.SetLocalDescription(answer); err != nil {
				log.Printf("[webrtc] set local description: %v", err)
				continue
			}
			out, _ := json.Marshal(map[string]interface{}{
				"type": "answer",
				"to":   p.signaling.ID,
				"sdp":  answer,
			})
			select {
			case p.signaling.Send <- out:
			default:
				log.Printf("[webrtc] signaling.Send full, dropping answer")
			}

		case "candidate":
			c := webrtc.ICECandidateInit{}
			if err := json.Unmarshal(env.Cand, &c); err != nil {
				log.Printf("[webrtc] bad candidate: %v", err)
				continue
			}
			if err := p.pc.AddICECandidate(c); err != nil {
				log.Printf("[webrtc] add ice candidate: %v", err)
			}
		}
	}
}

func (p *Peer) Inbox() chan []byte { return p.inbox }

func (p *Peer) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.inbox)
	pc := p.pc
	sig := p.signaling
	p.mu.Unlock()

	// Close all WebSocket streams
	p.closeAllWSStreams()

	if pc != nil {
		_ = pc.Close()
	}
	if sig != nil {
		sig.Close()
	}
}

func (p *Peer) DataChannel() *webrtc.DataChannel { return p.dc }

// SendDC sends a message through the DataChannel if it's open.
func (p *Peer) SendDC(data []byte) {
	p.mu.Lock()
	dc := p.dc
	p.mu.Unlock()
	if dc != nil && dc.ReadyState() == webrtc.DataChannelStateOpen {
		dc.SendText(string(data))
	}
}

// --- WebSocket stream multiplexing ---

// RegisterWSStream registers an upstream WebSocket connection for a given stream ID.
func (p *Peer) RegisterWSStream(id string, conn *websocket.Conn) {
	p.wsMu.Lock()
	p.wsStreams[id] = conn
	p.wsMu.Unlock()
}

// UnregisterWSStream removes and closes a WebSocket stream.
func (p *Peer) UnregisterWSStream(id string) {
	p.wsMu.Lock()
	if c, ok := p.wsStreams[id]; ok {
		delete(p.wsStreams, id)
		p.wsMu.Unlock()
		c.Close()
		return
	}
	p.wsMu.Unlock()
}

// RouteWSData forwards base64 data from the DataChannel to the upstream WS.
func (p *Peer) RouteWSData(id, dataB64 string, binary bool) {
	p.wsMu.Lock()
	conn, ok := p.wsStreams[id]
	p.wsMu.Unlock()
	if !ok {
		return
	}
	data, err := base64Decode(dataB64)
	if err != nil {
		return
	}
	mt := websocket.TextMessage
	if binary {
		mt = websocket.BinaryMessage
	}
	conn.WriteMessage(mt, data)
}

// CloseWSStream sends a close frame to the upstream WS and removes it.
func (p *Peer) CloseWSStream(id string, code int, reason string) {
	p.wsMu.Lock()
	conn, ok := p.wsStreams[id]
	delete(p.wsStreams, id)
	p.wsMu.Unlock()
	if !ok {
		return
	}
	if code == 0 {
		code = websocket.CloseNormalClosure
	}
	conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(code, reason))
	conn.Close()
}

func (p *Peer) closeAllWSStreams() {
	p.wsMu.Lock()
	streams := p.wsStreams
	p.wsStreams = make(map[string]*websocket.Conn)
	p.wsMu.Unlock()
	for _, c := range streams {
		c.Close()
	}
}

func base64Decode(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}
