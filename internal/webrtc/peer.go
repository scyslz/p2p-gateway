package webrtc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"

	"github.com/example/p2p-gateway/internal/signaling"
)

type Peer struct {
	signaling  *signaling.Peer
	pc         *webrtc.PeerConnection
	dc         *webrtc.DataChannel
	onReq      RequestHandler
	inbox      chan []byte
	wsStreams  map[string]*websocket.Conn
	mu         sync.Mutex
	sendMu     sync.Mutex
	closed     bool
	target     string
}
type RequestHandler func(p *Peer, raw []byte) ([]byte, error)

type Config struct {
	STUNURL string
	TURNURL string
}

type Manager struct {
	api     *webrtc.API
	cfg     Config
	handler RequestHandler

	mu    sync.Mutex
	peers map[string]*Peer
}

func NewManager(cfg Config, handler RequestHandler) (*Manager, error) {
	m := &Manager{cfg: cfg, handler: handler, peers: make(map[string]*Peer)}

	// Set up MediaEngine and SettingEngine
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("register codecs: %w", err)
	}

	var settingEngine webrtc.SettingEngine
	m.api = webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine), webrtc.WithSettingEngine(settingEngine))

	return m, nil
}

// HandleSignalingPeer creates a PeerConnection for the connecting browser,
// then sends an OFFER to it. The browser answers and the DataChannel carries HTTP.
func (m *Manager) HandleSignalingPeer(sp *signaling.Peer) error {
	iceServers := parseICEServers(m.cfg.STUNURL, m.cfg.TURNURL)
	log.Printf("[webrtc] ICE servers: %+v", iceServers)

	pc, err := m.api.NewPeerConnection(webrtc.Configuration{
		ICEServers: iceServers,
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

	// Route all WebSocket messages from browser directly to webrtc inbox.
	sp.OnMessage = func(msg []byte) {
		select {
		case p.inbox <- msg:
		default:
			log.Printf("[webrtc] webrtc inbox full for peer %s", sp.ID)
		}
	}

	// Gateway creates DataChannel (as offerer)
	dc, err := pc.CreateDataChannel("http", &webrtc.DataChannelInit{Ordered: boolPtr(true)})
	if err != nil {
		pc.Close()
		return fmt.Errorf("create data channel: %w", err)
	}
	p.dc = dc

	dc.OnOpen(func() {
		log.Printf("[webrtc] DataChannel %q opened for peer %s", dc.Label(), sp.ID)
		if b, _ := json.Marshal(map[string]interface{}{"type": "ready", "ts": time.Now().UnixMilli()}); b != nil {
			p.SendDC(b)
		}
		go p.keepaliveLoop()
	})
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		p.handleDCMessage(msg)
	})

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			log.Printf("[webrtc] ICE gathering complete for peer %s", sp.ID)
			return
		}
		init := c.ToJSON()
		log.Printf("[webrtc] local ICE candidate for %s: %s", sp.ID, init.Candidate)
		msg, _ := json.Marshal(map[string]interface{}{
			"type":      "candidate",
			"to":        sp.ID,
			"candidate": init,
		})
		select {
		case sp.Send <- msg:
		default:
			log.Printf("[webrtc] signaling Send full, dropping candidate")
		}
	})

	pc.OnICEGatheringStateChange(func(s webrtc.ICEGathererState) {
		log.Printf("[webrtc] ICE gathering state for %s: %s", sp.ID, s)
	})

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Printf("[webrtc] connection state for %s: %s", sp.ID, s)
		if s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed || s == webrtc.PeerConnectionStateDisconnected {
			p.Close()
			m.removePeer(sp.ID)
		}
	})

	// Create offer as Gateway (offerer)
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		pc.Close()
		return fmt.Errorf("create offer: %w", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		pc.Close()
		return fmt.Errorf("set local description: %w", err)
	}

	// Send offer to browser via signaling
	out, _ := json.Marshal(map[string]interface{}{
		"type": "offer", "to": sp.ID, "sdp": offer,
	})
	select {
	case sp.Send <- out:
		log.Printf("[webrtc] sent offer to browser %s", sp.ID)
	default:
		log.Printf("[webrtc] signaling Send full, dropping offer")
	}

	// Start background signaling loop — reads from signaling inbox
	// to handle browser's answer and ICE candidates
	go p.runGatewaySignalingLoop(m)

	m.addPeer(sp.ID, p)
	return nil
}

func (p *Peer) runGatewaySignalingLoop(m *Manager) {
	defer func() {
		if m != nil {
			m.removePeer(p.signaling.ID)
		}
		p.Close()
	}()

	for raw := range p.inbox {
		var env struct {
			Type   string          `json:"type"`
			SDP    json.RawMessage `json:"sdp"`
			Cand   json.RawMessage `json:"candidate"`
			Target string          `json:"target"`
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			continue
		}
		switch env.Type {
		case "target":
			// Live target switch: no ICE rebuild needed, the same
			// DataChannel keeps serving with the new upstream.
			t := strings.TrimSpace(env.Target)
			if t != "" {
				if !strings.Contains(t, "://") {
					t = "http://" + t
				}
				p.SetTarget(t)
				log.Printf("[webrtc] peer %s switched target to %s", p.signaling.ID, t)
			}
			continue
		case "answer":
			log.Printf("[webrtc] received answer from browser %s", p.signaling.ID)
			answer := webrtc.SessionDescription{}
			if err := json.Unmarshal(env.SDP, &answer); err != nil {
				log.Printf("[webrtc] bad answer SDP: %v", err)
				continue
			}
			if err := p.pc.SetRemoteDescription(answer); err != nil {
				log.Printf("[webrtc] set remote description: %v", err)
				continue
			}
		case "candidate":
			log.Printf("[webrtc] received candidate from browser %s", p.signaling.ID)
			c := webrtc.ICECandidateInit{}
			if err := json.Unmarshal(env.Cand, &c); err != nil {
				continue
			}
			p.pc.AddICECandidate(c)
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
	pc, sig := p.pc, p.signaling
	p.mu.Unlock()
	p.closeAllWSStreams()
	if pc != nil {
		pc.Close()
	}
	if sig != nil {
		sig.Close()
	}
}

func (p *Peer) DataChannel() *webrtc.DataChannel { return p.dc }

func (p *Peer) SendDC(data []byte) {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	p.mu.Lock()
	dc := p.dc
	p.mu.Unlock()
	if dc != nil && dc.ReadyState() == webrtc.DataChannelStateOpen {
		_ = dc.SendText(string(data))
	}
}

func (p *Peer) keepaliveLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		p.mu.Lock()
		closed := p.closed
		dc := p.dc
		p.mu.Unlock()
		if closed || dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
			return
		}
		ping, _ := json.Marshal(map[string]interface{}{"type": "ping", "ts": time.Now().UnixMilli()})
		if err := dc.Send(ping); err != nil {
			log.Printf("[webrtc] keepalive ping failed for %s: %v", p.signaling.ID, err)
			return
		}
	}
}

func (p *Peer) handleDCMessage(msg webrtc.DataChannelMessage) {
	raw := msg.Data
	var envelope struct {
		Type    string `json:"type"`
		StreamID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return
	}
	switch envelope.Type {
	case "ping":
		log.Printf("[webrtc] ping from peer %s → pong", p.signaling.ID)
		pong, _ := json.Marshal(map[string]interface{}{"type": "pong", "ts": time.Now().UnixMilli()})
		p.SendDC(pong)
		log.Printf("[webrtc] pong sent to peer %s", p.signaling.ID)
		return
	case "pong":
		log.Printf("[webrtc] pong from peer %s", p.signaling.ID)
		return
	case "ready":
		if b, _ := json.Marshal(map[string]interface{}{"type": "ready-ack", "ts": time.Now().UnixMilli()}); b != nil {
			p.SendDC(b)
		}
		log.Printf("[webrtc] ready↔ack with peer %s", p.signaling.ID)
		return
	case "ready-ack":
		log.Printf("[webrtc] ready-ack from peer %s", p.signaling.ID)
		return
	case "request":
		if p.onReq != nil {
			rawCopy := append([]byte(nil), raw...)
			go func() {
				resp, err := p.onReq(p, rawCopy)
				if err != nil {
					log.Printf("[proxy] handler error: %v", err)
				}
				if resp != nil {
					p.SendDC(resp)
				}
			}()
		}
	case "ws-open":
		p.handleWSOpen(raw, envelope.StreamID)
	case "ws-data":
		p.handleWSData(raw, envelope.StreamID)
	case "ws-close":
		p.handleWSClose(envelope.StreamID)
	default:
		if p.onReq != nil {
			rawCopy := append([]byte(nil), raw...)
			go func() {
				resp, err := p.onReq(p, rawCopy)
				if err != nil {
					log.Printf("[proxy] handler error: %v", err)
				}
				if resp != nil {
					p.SendDC(resp)
				}
			}()
		}
	}
}

func (p *Peer) handleWSOpen(raw []byte, streamID string) {
	var msg struct {
		Type   string `json:"type"`
		StreamID string `json:"id"`
		Path   string `json:"path"`
		Host   string `json:"host"`
		Proto  string `json:"protocols"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if p.onReq == nil {
		p.sendWSOpenErr(streamID, "no handler")
		return
	}
	resp, err := p.onReq(p, raw)
	if err != nil {
		p.sendWSOpenErr(streamID, err.Error())
		return
	}
	_ = resp
}

func (p *Peer) sendWSOpenErr(streamID, errMsg string) {
	msg, _ := json.Marshal(map[string]interface{}{
		"type": "ws-open-err", "id": streamID, "error": errMsg,
	})
	p.SendDC(msg)
}

func (p *Peer) handleWSData(raw []byte, streamID string) {
	if p.onReq != nil {
		resp, err := p.onReq(p, raw)
		if err != nil {
			log.Printf("[proxy] ws-data error: %v", err)
		}
		if resp != nil {
			p.SendDC(resp)
		}
	}
}

func (p *Peer) handleWSClose(streamID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ws, ok := p.wsStreams[streamID]; ok {
		ws.Close()
		delete(p.wsStreams, streamID)
	}
}

func (p *Peer) closeAllWSStreams() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ws := range p.wsStreams {
		ws.Close()
	}
	p.wsStreams = make(map[string]*websocket.Conn)
}

// parseICEServers parses comma-separated STUN/TURN URLs into pion ICEServer config.
func parseICEServers(stunRaw, turnRaw string) []webrtc.ICEServer {
	var servers []webrtc.ICEServer

	// Parse STUN servers
	for _, s := range strings.Split(stunRaw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		servers = append(servers, webrtc.ICEServer{URLs: []string{s}})
	}

	// Parse TURN server
	turnRaw = strings.TrimSpace(turnRaw)
	if turnRaw != "" {
		for _, s := range strings.Split(turnRaw, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			ices := webrtc.ICEServer{}
			isTurn := strings.HasPrefix(s, "turn:") || strings.HasPrefix(s, "turns:")
			if isTurn {
				prefix := "turn:"
				if strings.HasPrefix(s, "turns:") {
					prefix = "turns:"
				}
				rest := s[len(prefix):] // user:pass@host:port?transport=udp
				atIdx := strings.LastIndex(rest, "@")
				if atIdx > 0 {
					credPart := rest[:atIdx] // user:pass
					hostPart := rest[atIdx+1:] // host:port?transport=udp
					colonIdx := strings.Index(credPart, ":")
					if colonIdx > 0 {
						ices.Username = credPart[:colonIdx]
						ices.Credential = credPart[colonIdx+1:]
						// Reconstruct URL without credentials
						cleanURL := prefix + hostPart
						ices.URLs = []string{cleanURL}
						log.Printf("[webrtc] TURN: user=%q url=%q", ices.Username, cleanURL)
					}
				}
				if ices.Username == "" {
					ices.URLs = []string{s}
				}
			} else {
				ices.URLs = []string{s}
			}
			servers = append(servers, ices)
		}
	}

	if len(servers) == 0 {
		servers = []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		}
	}
	return servers
}

func (m *Manager) addPeer(id string, p *Peer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers[id] = p
}

func (m *Manager) removePeer(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.peers, id)
}

func boolPtr(b bool) *bool { return &b }

func encodeDCMessage(raw []byte) string {
	return base64.StdEncoding.EncodeToString(raw)
}

func (p *Peer) RegisterWSStream(id string, ws *websocket.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wsStreams[id] = ws
}

func (p *Peer) UnregisterWSStream(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.wsStreams, id)
}

func (p *Peer) RouteWSData(id string, data string, binary bool) {
	// Forward to DC
	msg, _ := json.Marshal(map[string]interface{}{
		"type": "ws-data", "id": id, "data": data,
	})
	p.SendDC(msg)
}

func (p *Peer) CloseWSStream(id string, code int, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ws, ok := p.wsStreams[id]; ok {
		ws.Close()
		delete(p.wsStreams, id)
	}
}

func (p *Peer) SetTarget(t string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = t
}

func (p *Peer) Target() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.target, p.target != ""
}

func (m *Manager) Peer(id string) *Peer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peers[id]
}
