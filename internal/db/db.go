// Package db provides persistent storage using JSON files.
// This keeps the binary dependency-free for the database layer.
package db

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DB is a simple JSON-file backed data store.
type DB struct {
	mu   sync.RWMutex
	dir  string
	data *store
}

type store struct {
	Config   map[string]string    `json:"config"`
	Messages []chatMessageRow     `json:"messages"`
	Sessions []streamSessionRow   `json:"sessions"`
	Banned   map[string]banRecord `json:"banned"`
	NextMsgID  int64              `json:"next_msg_id"`
	NextSessID int64              `json:"next_sess_id"`
}

type chatMessageRow struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
	IsDeleted bool   `json:"is_deleted"`
	IsBanned  bool   `json:"is_banned"`
}

type streamSessionRow struct {
	ID          int64  `json:"id"`
	StartedAt   int64  `json:"started_at"`
	EndedAt     int64  `json:"ended_at"` // 0 = not ended
	PeakViewers int    `json:"peak_viewers"`
	EndReason   string `json:"end_reason"`
}

type banRecord struct {
	BannedAt int64  `json:"banned_at"`
	BannedBy string `json:"banned_by"`
}

// Public types used by other packages.

type ChatMessage struct {
	ID        int64
	Username  string
	Message   string
	Timestamp time.Time
	IsDeleted bool
	IsBanned  bool
}

type StreamSession struct {
	ID          int64
	StartedAt   time.Time
	EndedAt     *time.Time
	PeakViewers int
	EndReason   string
}

const maxMessages = 10000
const dataFile = "relay.json"

func Open(path string) (*DB, error) {
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	// The path arg can be either a dir or a file path — use the directory.
	// We store everything in a single sluice.json next to the given path.
	dataPath := filepath.Join(dir, dataFile)
	if path != "" && !isDir(path) && filepath.Ext(path) != "" {
		// path looks like a file (e.g. ./sluice.db), use its directory
		dataPath = filepath.Join(filepath.Dir(path), dataFile)
	}

	d := &DB{dir: filepath.Dir(dataPath)}
	if err := d.load(dataPath); err != nil {
		return nil, err
	}
	return d, nil
}

func isDir(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

func (d *DB) dataPath() string {
	return filepath.Join(d.dir, dataFile)
}

func (d *DB) load(path string) error {
	d.data = &store{
		Config:  make(map[string]string),
		Banned:  make(map[string]banRecord),
		NextMsgID:  1,
		NextSessID: 1,
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // first run
		}
		return err
	}
	return json.Unmarshal(data, d.data)
}

func (d *DB) save() error {
	data, err := json.Marshal(d.data)
	if err != nil {
		return err
	}
	tmp := d.dataPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, d.dataPath())
}

func (d *DB) Close() error { return nil }

// Config operations

func (d *DB) GetConfig(key string) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.data.Config[key], nil
}

func (d *DB) SetConfig(key, value string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data.Config[key] = value
	return d.save()
}

func (d *DB) InitDefaults() (streamKey string, isNew bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	streamKey = d.data.Config["stream_key"]

	if streamKey == "" {
		isNew = true
		streamKey = randomKey()
		d.data.Config["stream_key"] = streamKey
		defaults := map[string]string{
			"stream_title": "Live Stream",
			"srt_port":     "9999",
			"ffmpeg_flags": "",
		}
		for k, v := range defaults {
			if _, ok := d.data.Config[k]; !ok {
				d.data.Config[k] = v
			}
		}
		err = d.save()
	}
	return
}

// Chat operations

func (d *DB) SaveMessage(username, message string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	id := d.data.NextMsgID
	d.data.NextMsgID++

	row := chatMessageRow{
		ID:        id,
		Username:  username,
		Message:   message,
		Timestamp: time.Now().Unix(),
	}
	d.data.Messages = append(d.data.Messages, row)

	// trim old messages
	if len(d.data.Messages) > maxMessages {
		d.data.Messages = d.data.Messages[len(d.data.Messages)-maxMessages:]
	}

	return id, d.save()
}

func (d *DB) GetRecentMessages(limit int) ([]ChatMessage, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var result []ChatMessage
	msgs := d.data.Messages
	for i := len(msgs) - 1; i >= 0 && len(result) < limit; i-- {
		m := msgs[i]
		if m.IsDeleted || m.IsBanned {
			continue
		}
		result = append(result, toChatMessage(m))
	}
	// reverse to get oldest first
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result, nil
}

func (d *DB) GetAllMessages(limit int) ([]ChatMessage, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	msgs := d.data.Messages
	start := 0
	if len(msgs) > limit {
		start = len(msgs) - limit
	}

	result := make([]ChatMessage, 0, len(msgs)-start)
	for _, m := range msgs[start:] {
		result = append(result, toChatMessage(m))
	}
	return result, nil
}

func toChatMessage(m chatMessageRow) ChatMessage {
	return ChatMessage{
		ID:        m.ID,
		Username:  m.Username,
		Message:   m.Message,
		Timestamp: time.Unix(m.Timestamp, 0),
		IsDeleted: m.IsDeleted,
		IsBanned:  m.IsBanned,
	}
}

func (d *DB) DeleteMessage(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.data.Messages {
		if d.data.Messages[i].ID == id {
			d.data.Messages[i].IsDeleted = true
			break
		}
	}
	return d.save()
}

func (d *DB) ClearChat() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.data.Messages {
		d.data.Messages[i].IsDeleted = true
	}
	return d.save()
}

func (d *DB) BanUser(username, bannedBy string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data.Banned[username] = banRecord{BannedAt: time.Now().Unix(), BannedBy: bannedBy}
	for i := range d.data.Messages {
		if d.data.Messages[i].Username == username {
			d.data.Messages[i].IsBanned = true
		}
	}
	return d.save()
}

func (d *DB) UnbanUser(username string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.data.Banned, username)
	return d.save()
}

func (d *DB) IsBanned(username string) (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.data.Banned[username]
	return ok, nil
}

func (d *DB) GetBannedUsers() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	users := make([]string, 0, len(d.data.Banned))
	for u := range d.data.Banned {
		users = append(users, u)
	}
	return users, nil
}

// Stream session operations

func (d *DB) StartSession() (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id := d.data.NextSessID
	d.data.NextSessID++
	d.data.Sessions = append(d.data.Sessions, streamSessionRow{
		ID:        id,
		StartedAt: time.Now().Unix(),
	})
	return id, d.save()
}

func (d *DB) EndSession(id int64, peakViewers int, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.data.Sessions {
		if d.data.Sessions[i].ID == id {
			d.data.Sessions[i].EndedAt = time.Now().Unix()
			d.data.Sessions[i].PeakViewers = peakViewers
			d.data.Sessions[i].EndReason = reason
			break
		}
	}
	return d.save()
}

func (d *DB) GetRecentSessions(limit int) ([]StreamSession, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	sessions := d.data.Sessions
	start := 0
	if len(sessions) > limit {
		start = len(sessions) - limit
	}

	result := make([]StreamSession, 0, len(sessions)-start)
	for i := len(sessions) - 1; i >= start; i-- {
		s := sessions[i]
		sess := StreamSession{
			ID:          s.ID,
			StartedAt:   time.Unix(s.StartedAt, 0),
			PeakViewers: s.PeakViewers,
			EndReason:   s.EndReason,
		}
		if s.EndedAt != 0 {
			t := time.Unix(s.EndedAt, 0)
			sess.EndedAt = &t
		}
		result = append(result, sess)
	}
	return result, nil
}

func (d *DB) UpdateSessionPeakViewers(id int64, peak int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.data.Sessions {
		if d.data.Sessions[i].ID == id && d.data.Sessions[i].PeakViewers < peak {
			d.data.Sessions[i].PeakViewers = peak
			break
		}
	}
	return d.save()
}

// randomKey generates a short, easy-to-type stream key like "A3KF-9XM2-P7BV".
// Uses Crockford-style charset (no 0/O, 1/I/L ambiguity).
func randomKey() string {
	const chars = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("randomKey: %v", err))
	}
	out := make([]byte, 14) // 12 chars + 2 dashes
	for i, j := 0, 0; i < 12; i++ {
		out[j] = chars[int(b[i])%len(chars)]
		j++
		if i == 3 || i == 7 {
			out[j] = '-'
			j++
		}
	}
	return string(out)
}

