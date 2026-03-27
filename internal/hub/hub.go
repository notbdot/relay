package hub

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	TypeStreamStatus = "stream_status"
	TypeChatMessage  = "chat_message"
	TypeChatClear    = "chat_clear"
	TypeChatBan      = "chat_ban"
	TypeChatDelete   = "chat_delete"
	TypeAdminEvent   = "admin_event"
)

type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type StreamStatusPayload struct {
	Online      bool    `json:"online"`
	Bitrate     float64 `json:"bitrate_kbps"`
	UptimeS     int64   `json:"uptime_s"`
	ViewerCount int     `json:"viewer_count"`
	Title       string  `json:"title"`
	Scene       string  `json:"scene"` // "live" | "starting_soon" | "brb" | "ending"
}

type ChatMessagePayload struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
	Color     string `json:"color"`
}

type ChatBanPayload struct {
	Username string `json:"username"`
}

type ChatDeletePayload struct {
	ID int64 `json:"id"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	send      chan []byte
	isAdmin   bool
	username  string
	lastMsg   time.Time
	rateCount int
}

type Hub struct {
	mu          sync.RWMutex
	clients     map[*Client]struct{}
	broadcast   chan []byte
	adminOnly   chan []byte
	register    chan *Client
	unregister  chan *Client
	viewerCount atomic.Int32
	statusCache []byte
}

func New() *Hub {
	h := &Hub{
		clients:    make(map[*Client]struct{}),
		broadcast:  make(chan []byte, 256),
		adminOnly:  make(chan []byte, 64),
		register:   make(chan *Client, 32),
		unregister: make(chan *Client, 32),
	}
	return h
}

func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			h.clients[c] = struct{}{}
			h.mu.Unlock()
			h.viewerCount.Add(1)
			// Send cached status on join
			if h.statusCache != nil {
				select {
				case c.send <- h.statusCache:
				default:
				}
			}

		case c := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
			h.mu.Unlock()
			h.viewerCount.Add(-1)

		case msg := <-h.broadcast:
			h.mu.RLock()
			for c := range h.clients {
				select {
				case c.send <- msg:
				default:
					// slow client — drop
				}
			}
			h.mu.RUnlock()

		case msg := <-h.adminOnly:
			h.mu.RLock()
			for c := range h.clients {
				if c.isAdmin {
					select {
					case c.send <- msg:
					default:
					}
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *Hub) ViewerCount() int {
	return int(h.viewerCount.Load())
}

func (h *Hub) BroadcastMessage(msgType string, payload any) {
	data, err := marshalEnvelope(msgType, payload)
	if err != nil {
		return
	}
	if msgType == TypeStreamStatus {
		h.statusCache = data
	}
	h.broadcast <- data
}

func (h *Hub) BroadcastAdmin(msgType string, payload any) {
	data, err := marshalEnvelope(msgType, payload)
	if err != nil {
		return
	}
	h.adminOnly <- data
}

func (h *Hub) SendHistory(c *Client, msgs []ChatMessagePayload) {
	for _, m := range msgs {
		data, err := marshalEnvelope(TypeChatMessage, m)
		if err != nil {
			continue
		}
		select {
		case c.send <- data:
		default:
		}
	}
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request, isAdmin bool, username string, history []ChatMessagePayload) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &Client{
		hub:      h,
		conn:     conn,
		send:     make(chan []byte, 256),
		isAdmin:  isAdmin,
		username: username,
	}
	h.register <- c

	if len(history) > 0 {
		h.SendHistory(c, history)
	}

	go c.writePump()
	go c.readPump()
}

func (c *Client) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

type incomingMsg struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type chatPayload struct {
	Username string `json:"username"`
	Message  string `json:"message"`
}

// OnChatMessage is called by the hub's owner when a chat message arrives from a client.
// It's a hook for the server to persist and broadcast.
var OnChatMessage func(c *Client, username, message string)

// OnAdminCommand is called for admin-only commands.
var OnAdminCommand func(c *Client, msgType string, payload json.RawMessage)

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(4096)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("ws client %q closed unexpectedly: %v", c.username, err)
			}
			return
		}
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		var msg incomingMsg
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "chat_message":
			var p chatPayload
			if err := json.Unmarshal(msg.Payload, &p); err != nil {
				continue
			}
			// rate limit: 1 msg/sec
			now := time.Now()
			if now.Sub(c.lastMsg) < time.Second {
				continue
			}
			c.lastMsg = now

			if len(p.Message) > 300 {
				p.Message = p.Message[:300]
			}
			if p.Message == "" || p.Username == "" {
				continue
			}
			if OnChatMessage != nil {
				OnChatMessage(c, p.Username, p.Message)
			}

		default:
			if c.isAdmin && OnAdminCommand != nil {
				OnAdminCommand(c, msg.Type, msg.Payload)
			}
		}
	}
}

func marshalEnvelope(msgType string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Message{Type: msgType, Payload: raw})
}
