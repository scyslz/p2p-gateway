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
	"strings"
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

func (p *Peer) SetTarget(t string) { p.target = t }

func (p *Peer) Target() (string, error) {
	if p.target == "" {
		return "", fmt.Errorf("no target configured")
	}
	return p.target, nil
}

type RequestHandler func(p *Peer, req []byte) ([]byte, error)

// Config for creating PeerConnections.
type Config struct {
	STUNURL string // comma-separated STUN URLs
	TURNURL string // optional TURN URL
}

// Manager owns the API.
type Manager struct {
	api     *webrtc.API
	cfg     Config
	handler RequestHandler

	mu    sync.Mutex
	peers map[string]*Peer
}

func NewManager(cfg Config, handler RequestHandler) (*Manager, error) {
	api := webrtc.NewAPI()
	return &Manager{api: api, cfg: cfg, handler: handler, peers: make(map[string]*Peer)}, nil
}

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

// buildICEServers parses the comma-separated STUN URLs and optional TURN URL
// into []webrtc.ICEServer for the peer connection.
func (m *Manager) buildICEServers() []webrtc.ICEServer {
	var servers []webrtc.ICEServer

	// STUN servers (comma-separated)
	if m.cfg.STUNURL != "" {
		for _, s := range strings.Split(m.cfg.STUNURL, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				servers = append(servers, webrtc.ICEServer{
					URLs: []string{s},
				})
			}
		}
	}

	// Optional TURN server
	if m.cfg.TURNURL != "" {
		for _, s := range strings.Split(m.cfg.TURNURL, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			ices := webrtc.ICEServer{
				URLs: []string{s},
			}
			// Parse turn:user:pass@host format
			if strings.HasPrefix(s, "turn:") || strings.HasPrefix(s, "turns:") {
				// Extract credentials from URL if present
				// Format: turn:user:password@host:port
				atIdx := strings.LastIndex(s, "@")
				if atIdx > 0 {
					beforeAt := s[:atIdx]
					// turn:user:pass → username=user, credential=pass
					idx := strings.Index(beforeAt, "://")
					if idx >= 0 {
						cred := beforeAt[idx+3:]
						colonIdx := strings.Index(cred, ":")
						if colonIdx >= 0 {
							ices.Username = cred[:colonIdx]
							ices.Credential = cred[colonIdx+1:]
							ices.URLs = []string{s}
						}
					}
				}
			}
			servers = append(servers, ices)
		}
	}

	if len(servers) == 0 {
		servers = []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		}
	}

	log.Printf("[webrtc] ICE servers: %v", servers)
	return servers
}

func (m *Manager) HandleSignalingPeer(sp *signaling.Peer) error {
	pc, err := m.api.NewPeerConnection(webrtc.Configuration{
		ICEServers: m.buildICEServers(),
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

	p.closeAllWSStreams()

	if pc != nil {
		_ = pc.Close()
	}
	if sig != nil {
		sig.Close()
	}
}

func (p *Peer) DataChannel() *webrtc.DataChannel { return p.dc }

func (p *Peer) SendDC(data []byte) {
	p.mu.Lock()
	dc := p.dc
	p.mu.Unlock()
	if dc != nil && dc.ReadyState() == webrtc.DataChannelStateOpen {
		dc.SendText(string(data))
	}
}

// --- WebSocket stream multiplexing ---

func (p *Peer) RegisterWSStream(id string, conn *websocket.Conn) {
	p.wsMu.Lock()
	p.wsStreams[id] = conn
	p.wsMu.Unlock()
}

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

func (p *Peer) RouteWSData(id, dataB64 string, binary bool) {
	p.wsMu.Lock()
	conn, ok := p.wsStreams[id]
	p.wsMu.Unlock()
	if !ok {
		return
	}
	data, err := base64.StdEncoding.DecodeString(dataB64)
	if err != nil {
		return
	}
	mt := websocket.TextMessage
	if binary {
		mt = websocket.BinaryMessage
	}
	conn.WriteMessage(mt, data)
}

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
