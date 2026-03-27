package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/notbdot/sluice/internal/db"
	"github.com/notbdot/sluice/internal/hub"
	"github.com/notbdot/sluice/internal/ingest"
)

// Deps holds everything the server needs.
type Deps struct {
	DB          *db.DB
	Hub         *hub.Hub
	Ingest      *ingest.Manager
	SegmentsDir string
	// embed fs for static files
	ViewerHTML []byte
	AdminHTML  []byte
}

type Server struct {
	deps    *Deps
	mux     *http.ServeMux
	httpSrv *http.Server
	addr    string

	mu         sync.RWMutex
	bitrateHistory []float64 // last 300 samples (5min @ 1s)
}

func New(addr string, deps *Deps) *Server {
	s := &Server{
		deps:           deps,
		mux:            http.NewServeMux(),
		addr:           addr,
		bitrateHistory: make([]float64, 0, 300),
	}
	s.routes()

	// install hub callbacks
	hub.OnChatMessage = s.handleChatMessage
	hub.OnAdminCommand = s.handleAdminCommand

	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /", s.viewerHandler)
	s.mux.HandleFunc("GET /hls/", s.hlsHandler)
	s.mux.HandleFunc("GET /admin", s.adminHandler)
	s.mux.HandleFunc("GET /ws", s.wsHandler)
	s.mux.HandleFunc("GET /api/status", s.apiStatus)
	s.mux.HandleFunc("GET /api/bitrate-history", s.apiBitrateHistory)
	s.mux.HandleFunc("POST /api/admin/config", s.requireAdmin(s.apiAdminConfig))
	s.mux.HandleFunc("POST /api/admin/chat/clear", s.requireAdmin(s.apiChatClear))
	s.mux.HandleFunc("POST /api/admin/chat/ban", s.requireAdmin(s.apiChatBan))
	s.mux.HandleFunc("POST /api/admin/chat/delete", s.requireAdmin(s.apiChatDelete))
	s.mux.HandleFunc("POST /api/admin/stream/restart", s.requireAdmin(s.apiStreamRestart))
	s.mux.HandleFunc("GET /api/admin/sessions", s.requireAdmin(s.apiSessions))
	s.mux.HandleFunc("GET /api/admin/chat/all", s.requireAdmin(s.apiAllChat))
}

func (s *Server) Start(ctx context.Context) error {
	s.httpSrv = &http.Server{
		Addr:    s.addr,
		Handler: s.mux,
	}

	// Poll ingest stats and broadcast
	go s.statsLoop(ctx)

	// Watch for ingest status changes
	go s.watchIngest(ctx)

	log.Printf("server: listening on %s", s.addr)
	errCh := make(chan error, 1)
	go func() { errCh <- s.httpSrv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpSrv.Shutdown(shutCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) statsLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := s.deps.Ingest.Stats()
			if stats.Online {
				s.mu.Lock()
				if len(s.bitrateHistory) >= 300 {
					s.bitrateHistory = s.bitrateHistory[1:]
				}
				s.bitrateHistory = append(s.bitrateHistory, stats.CurrentKbps)
				s.mu.Unlock()
			}

			title, _ := s.deps.DB.GetConfig("stream_title")
			payload := hub.StreamStatusPayload{
				Online:      stats.Online,
				Bitrate:     stats.CurrentKbps,
				UptimeS:     s.deps.Ingest.UptimeSeconds(),
				ViewerCount: s.deps.Hub.ViewerCount(),
				Title:       title,
			}
			s.deps.Hub.BroadcastMessage(hub.TypeStreamStatus, payload)
		}
	}
}

func (s *Server) watchIngest(ctx context.Context) {
	var currentSessionID int64
	for {
		select {
		case <-ctx.Done():
			return
		case change := <-s.deps.Ingest.StatusCh:
			if change.Online {
				id, err := s.deps.DB.StartSession()
				if err != nil {
					log.Printf("server: start session error: %v", err)
				} else {
					currentSessionID = id
					log.Printf("server: stream session %d started", id)
				}
			} else {
				if currentSessionID > 0 {
					peak := s.deps.Hub.ViewerCount()
					if err := s.deps.DB.EndSession(currentSessionID, peak, "disconnect"); err != nil {
						log.Printf("server: end session error: %v", err)
					}
					log.Printf("server: stream session %d ended", currentSessionID)
					currentSessionID = 0
					// clear bitrate history
					s.mu.Lock()
					s.bitrateHistory = s.bitrateHistory[:0]
					s.mu.Unlock()
				}
			}
		}
	}
}

// Handlers

func (s *Server) viewerHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(s.deps.ViewerHTML)
}

func (s *Server) hlsHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/hls/")
	path = filepath.Clean(path)

	// Prevent path traversal
	if strings.Contains(path, "..") {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	fullPath := filepath.Join(s.deps.SegmentsDir, path)

	// Set appropriate content types
	if strings.HasSuffix(path, ".m3u8") {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	} else if strings.HasSuffix(path, ".ts") {
		w.Header().Set("Content-Type", "video/mp2t")
		w.Header().Set("Cache-Control", "max-age=60")
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	http.ServeFile(w, r, fullPath)
}

func (s *Server) adminHandler(w http.ResponseWriter, r *http.Request) {
	// Admin auth via cookie or query param
	if !s.isAdminAuthed(r) {
		token := r.URL.Query().Get("token")
		if token == "" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprintf(w, `<!doctype html><html><body style="background:#282a36;color:#f8f8f2;font-family:monospace;padding:2rem">
<h2>Admin Login</h2>
<form method="GET">
<input name="token" type="password" placeholder="Admin token" style="background:#44475a;color:#f8f8f2;border:1px solid #6272a4;padding:8px;font-family:monospace" autofocus>
<button type="submit" style="background:#bd93f9;color:#282a36;border:none;padding:8px 16px;font-family:monospace;cursor:pointer;margin-left:8px">→ Enter</button>
</form></body></html>`)
			return
		}
		// Validate token from query param and set cookie
		adminToken, _ := s.deps.DB.GetConfig("admin_token")
		if token != adminToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		http.SetCookie(w, &http.Cookie{
			Name:     "sluice_admin",
			Value:    token,
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
		})
		http.Redirect(w, r, "/admin", http.StatusFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(s.deps.AdminHTML)
}

func (s *Server) wsHandler(w http.ResponseWriter, r *http.Request) {
	isAdmin := s.isAdminAuthed(r)

	// Load chat history
	msgs, err := s.deps.DB.GetRecentMessages(50)
	if err != nil {
		log.Printf("server: get recent messages error: %v", err)
	}

	var history []hub.ChatMessagePayload
	bannedUsers, _ := s.deps.DB.GetBannedUsers()
	bannedSet := make(map[string]bool, len(bannedUsers))
	for _, u := range bannedUsers {
		bannedSet[u] = true
	}

	for _, m := range msgs {
		if bannedSet[m.Username] {
			continue
		}
		history = append(history, hub.ChatMessagePayload{
			ID:        m.ID,
			Username:  m.Username,
			Message:   m.Message,
			Timestamp: m.Timestamp.Unix(),
			Color:     usernameColor(m.Username),
		})
	}

	username := r.URL.Query().Get("username")
	if username == "" {
		username = "viewer"
	}

	s.deps.Hub.ServeWS(w, r, isAdmin, username, history)
}

func (s *Server) apiStatus(w http.ResponseWriter, r *http.Request) {
	stats := s.deps.Ingest.Stats()
	title, _ := s.deps.DB.GetConfig("stream_title")
	jsonResp(w, map[string]any{
		"online":       stats.Online,
		"bitrate_kbps": stats.CurrentKbps,
		"uptime_s":     s.deps.Ingest.UptimeSeconds(),
		"viewer_count": s.deps.Hub.ViewerCount(),
		"title":        title,
	})
}

func (s *Server) apiBitrateHistory(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	hist := make([]float64, len(s.bitrateHistory))
	copy(hist, s.bitrateHistory)
	s.mu.RUnlock()
	jsonResp(w, hist)
}

func (s *Server) apiAdminConfig(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StreamTitle  string `json:"stream_title"`
		StreamKey    string `json:"stream_key"`
		FFmpegFlags  string `json:"ffmpeg_flags"`
		SRTPort      string `json:"srt_port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	needsRestart := false

	if body.StreamTitle != "" {
		_ = s.deps.DB.SetConfig("stream_title", body.StreamTitle)
	}
	if body.StreamKey != "" {
		_ = s.deps.DB.SetConfig("stream_key", body.StreamKey)
		needsRestart = true
	}
	if body.FFmpegFlags != "" {
		_ = s.deps.DB.SetConfig("ffmpeg_flags", body.FFmpegFlags)
		needsRestart = true
	}
	if body.SRTPort != "" {
		_ = s.deps.DB.SetConfig("srt_port", body.SRTPort)
		// Note: actual port change requires process restart
	}

	if needsRestart {
		s.deps.Ingest.Restart()
	}

	jsonResp(w, map[string]string{"status": "ok"})
}

func (s *Server) apiChatClear(w http.ResponseWriter, r *http.Request) {
	if err := s.deps.DB.ClearChat(); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	s.deps.Hub.BroadcastMessage(hub.TypeChatClear, map[string]any{})
	jsonResp(w, map[string]string{"status": "ok"})
}

func (s *Server) apiChatBan(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Username == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := s.deps.DB.BanUser(body.Username, "admin"); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	s.deps.Hub.BroadcastMessage(hub.TypeChatBan, hub.ChatBanPayload{Username: body.Username})
	jsonResp(w, map[string]string{"status": "ok"})
}

func (s *Server) apiChatDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ID == 0 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := s.deps.DB.DeleteMessage(body.ID); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	s.deps.Hub.BroadcastMessage(hub.TypeChatDelete, hub.ChatDeletePayload{ID: body.ID})
	jsonResp(w, map[string]string{"status": "ok"})
}

func (s *Server) apiStreamRestart(w http.ResponseWriter, r *http.Request) {
	s.deps.Ingest.Restart()
	jsonResp(w, map[string]string{"status": "ok"})
}

func (s *Server) apiSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := s.deps.DB.GetRecentSessions(20)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	type sessionResp struct {
		ID          int64  `json:"id"`
		StartedAt   int64  `json:"started_at"`
		EndedAt     *int64 `json:"ended_at"`
		PeakViewers int    `json:"peak_viewers"`
		EndReason   string `json:"end_reason"`
		DurationS   int64  `json:"duration_s"`
	}

	var resp []sessionResp
	for _, sess := range sessions {
		sr := sessionResp{
			ID:          sess.ID,
			StartedAt:   sess.StartedAt.Unix(),
			PeakViewers: sess.PeakViewers,
			EndReason:   sess.EndReason,
		}
		if sess.EndedAt != nil {
			ts := sess.EndedAt.Unix()
			sr.EndedAt = &ts
			sr.DurationS = ts - sess.StartedAt.Unix()
		}
		resp = append(resp, sr)
	}
	jsonResp(w, resp)
}

func (s *Server) apiAllChat(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}
	msgs, err := s.deps.DB.GetAllMessages(limit)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	type msgResp struct {
		ID        int64  `json:"id"`
		Username  string `json:"username"`
		Message   string `json:"message"`
		Timestamp int64  `json:"timestamp"`
		IsDeleted bool   `json:"is_deleted"`
		IsBanned  bool   `json:"is_banned"`
		Color     string `json:"color"`
	}

	var resp []msgResp
	for _, m := range msgs {
		resp = append(resp, msgResp{
			ID:        m.ID,
			Username:  m.Username,
			Message:   m.Message,
			Timestamp: m.Timestamp.Unix(),
			IsDeleted: m.IsDeleted,
			IsBanned:  m.IsBanned,
			Color:     usernameColor(m.Username),
		})
	}
	jsonResp(w, resp)
}

// handleChatMessage is called by the hub when a client sends a chat message.
func (s *Server) handleChatMessage(c *hub.Client, username, message string) {
	banned, err := s.deps.DB.IsBanned(username)
	if err != nil || banned {
		return
	}

	id, err := s.deps.DB.SaveMessage(username, message)
	if err != nil {
		log.Printf("server: save message error: %v", err)
		return
	}

	payload := hub.ChatMessagePayload{
		ID:        id,
		Username:  username,
		Message:   message,
		Timestamp: time.Now().Unix(),
		Color:     usernameColor(username),
	}
	s.deps.Hub.BroadcastMessage(hub.TypeChatMessage, payload)
}

// handleAdminCommand is called by the hub for admin-only WebSocket commands.
func (s *Server) handleAdminCommand(c *hub.Client, msgType string, payload json.RawMessage) {
	// Admin can send commands via WS too; not used for now, handled via REST
}

// Auth helpers

func (s *Server) isAdminAuthed(r *http.Request) bool {
	adminToken, err := s.deps.DB.GetConfig("admin_token")
	if err != nil || adminToken == "" {
		return false
	}

	// Check cookie
	if cookie, err := r.Cookie("sluice_admin"); err == nil {
		if cookie.Value == adminToken {
			return true
		}
	}

	// Check Authorization header
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		if strings.TrimPrefix(auth, "Bearer ") == adminToken {
			return true
		}
	}

	// Check query param
	if token := r.URL.Query().Get("token"); token == adminToken {
		return true
	}

	return false
}

func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.isAdminAuthed(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// Helpers

func jsonResp(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// usernameColor deterministically maps a username to a Dracula accent color.
func usernameColor(username string) string {
	colors := []string{"#bd93f9", "#ff79c6", "#8be9fd", "#ffb86c", "#f1fa8c", "#50fa7b"}
	var h uint32
	for _, c := range username {
		h = h*31 + uint32(c)
	}
	return colors[h%uint32(len(colors))]
}
