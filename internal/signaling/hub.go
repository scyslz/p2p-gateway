// Package signaling implements a minimal WebSocket signaling server.
//
// The signaling protocol is intentionally trivial: every connected peer is
// assigned a random ID, and any message addressed to another peer in the
// same room is forwarded verbatim, with "from" injected and "to" stripped.
//
// The gateway's WebRTC side is the answerer, so the page always sends
// offers and the gateway answers.
package signaling

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// Peer represents a single WebSocket connection.  It exposes Inbox, a
// channel that other components can read to receive signaling messages
// addressed to this peer (offer, candidate, ...).
type Peer struct {
	ID   string
	Conn *websocket.Conn

	// Outbox: we own the Send channel.
	Send chan []byte

	// Inbox: written by the readPump goroutine whenever a message
	// addressed to this peer arrives.
	Inbox chan []byte

	Room string

	mu     sync.Mutex
	closed bool
}

// Close marks the peer as closed and closes the underlying connection.
func (p *Peer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	if p.Send != nil {
		close(p.Send)
	}
	if p.Conn != nil {
		_ = p.Conn.Close()
	}
}

// Room is a set of peers connected to the same gateway host.
type Room struct {
	Name  string
	Peers map[string]*Peer
	mu    sync.Mutex
}

func (r *Room) add(p *Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Peers[p.ID] = p
}

func (r *Room) remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.Peers, id)
}

// Hub keeps track of every room.
type Hub struct {
	rooms map[string]*Room
	mu    sync.Mutex
}

// NewHub returns a new signaling Hub.
func NewHub() *Hub {
	return &Hub{rooms: make(map[string]*Room)}
}

func (h *Hub) room(name string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[name]
	if !ok {
		r = &Room{Name: name, Peers: make(map[string]*Peer)}
		h.rooms[name] = r
	}
	return r
}

// Upgrader is shared.  CheckOrigin always returns true because we
// authenticate via the gateway host header rather than origin.
var Upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// OnPeer is invoked once a peer has been created and assigned to a
// room, before any signaling traffic flows.  Use it to wire the peer
// into the WebRTC layer.
type OnPeer func(p *Peer)

// ServeWS handles a single WebSocket connection.  The "room" is the
// gateway host the browser connected to (r.Host).
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request, onPeer OnPeer) {
	roomName := r.Host
	if roomName == "" {
		http.Error(w, "missing host", http.StatusBadRequest)
		return
	}

	conn, err := Upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[signal] upgrade error: %v", err)
		return
	}

	peer := &Peer{
		ID:   newID(),
		Conn: conn,
		Send: make(chan []byte, 64),
		// Inbox is unbuffered so that slow WebRTC layers naturally apply
		// backpressure to the signaling read goroutine.
		Inbox: make(chan []byte),
		Room:  roomName,
	}

	room := h.room(roomName)
	room.add(peer)

	log.Printf("[signal] join room=%s peer=%s", roomName, peer.ID)

	// Tell the new peer its own id.
	joinMsg, _ := json.Marshal(map[string]string{"type": "join", "id": peer.ID})
	peer.Send <- joinMsg

	// Notify others.
	notice, _ := json.Marshal(map[string]string{"type": "peer-joined", "id": peer.ID})
	room.broadcast(notice, peer.ID, peer)

	if onPeer != nil {
		onPeer(peer)
	}

	go writePump(peer)
	readPump(room, peer)
}

func writePump(p *Peer) {
	defer p.Close()
	for msg := range p.Send {
		if err := p.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

// broadcast sends msg to every peer in the room except `except`.
func (r *Room) broadcast(msg []byte, exceptID string, exceptPeer *Peer) {
	r.mu.Lock()
	peers := make([]*Peer, 0, len(r.Peers))
	for id, p := range r.Peers {
		if id == exceptID {
			continue
		}
		peers = append(peers, p)
	}
	r.mu.Unlock()
	for _, p := range peers {
		select {
		case p.Send <- msg:
		default:
			// Drop slow peer.
			go p.Close()
		}
	}
}

func readPump(room *Room, p *Peer) {
	defer func() {
		room.remove(p.ID)
		notice, _ := json.Marshal(map[string]string{"type": "peer-left", "id": p.ID})
		room.broadcast(notice, p.ID, p)
		log.Printf("[signal] leave room=%s peer=%s", p.Room, p.ID)
		// Closing the peer also closes Inbox so WebRTC loops unblock.
		p.Close()
	}()

	for {
		_, msg, err := p.Conn.ReadMessage()
		if err != nil {
			return
		}

		var env struct {
			Type string `json:"type"`
			To   string `json:"to"`
		}
		if err := json.Unmarshal(msg, &env); err != nil {
			log.Printf("[signal] bad json: %v", err)
			continue
		}

		if env.To == "" {
			// Broadcast.
			room.broadcast(msg, p.ID, p)
			continue
		}

		// Forward to the named peer in the same room.
		room.mu.Lock()
		target, ok := room.Peers[env.To]
		room.mu.Unlock()
		if !ok {
			continue
		}

		// Replace "to" with "from" so the receiver can identify the
		// sender.
		var m map[string]interface{}
		_ = json.Unmarshal(msg, &m)
		m["from"] = p.ID
		delete(m, "to")
		out, _ := json.Marshal(m)

		select {
		case target.Inbox <- out:
		default:
			// Slow consumer: drop.
			log.Printf("[signal] inbox full for peer=%s", target.ID)
		}
	}
}