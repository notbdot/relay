package ingest

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Stats holds live stream statistics parsed from FFmpeg stderr.
type Stats struct {
	Online      bool
	StartedAt   time.Time
	CurrentKbps float64
}

// StatusChange is sent on the channel when stream goes online or offline.
type StatusChange struct {
	Online    bool
	SessionID int64
}

// Manager manages the FFmpeg SRT ingest process.
type Manager struct {
	mu          sync.Mutex
	ffmpegPath  string
	srtPort     int
	segmentsDir string
	hlsTime     int
	hlsListSize int
	extraFlags  string

	getStreamKey  func() string
	getExtraFlags func() string

	cmd         *exec.Cmd
	cancel      context.CancelFunc
	statsAtomic atomic.Pointer[Stats]

	StatusCh chan StatusChange
}

func New(
	ffmpegPath string,
	srtPort int,
	segmentsDir string,
	hlsTime, hlsListSize int,
	extraFlags string,
	getStreamKey func() string,
	getExtraFlagsFunc func() string,
) *Manager {
	mgr := &Manager{
		ffmpegPath:    ffmpegPath,
		srtPort:       srtPort,
		segmentsDir:   segmentsDir,
		hlsTime:       hlsTime,
		hlsListSize:   hlsListSize,
		extraFlags:    extraFlags,
		getStreamKey:  getStreamKey,
		getExtraFlags: getExtraFlagsFunc,
		StatusCh:      make(chan StatusChange, 8),
	}
	mgr.statsAtomic.Store(&Stats{})
	return mgr
}

// Start begins the ingest loop. Blocks until ctx is cancelled.
func (mgr *Manager) Start(ctx context.Context) {
	if err := os.MkdirAll(mgr.segmentsDir, 0755); err != nil {
		log.Printf("ingest: failed to create segments dir: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			mgr.stopCurrentProcess()
			return
		default:
		}

		log.Printf("ingest: starting FFmpeg SRT listener on :%d", mgr.srtPort)
		mgr.runFFmpeg(ctx)

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

var bitrateRe = regexp.MustCompile(`bitrate=\s*([\d.]+)\s*kbits/s`)
var connectedRe = regexp.MustCompile(`Input #0|SRT source opened|Opening .* for reading`)

func (mgr *Manager) runFFmpeg(ctx context.Context) {
	streamKey := mgr.getStreamKey()

	srtURL := fmt.Sprintf("srt://0.0.0.0:%d?mode=listener", mgr.srtPort)
	if streamKey != "" {
		srtURL = fmt.Sprintf("srt://0.0.0.0:%d?mode=listener&passphrase=%s", mgr.srtPort, streamKey)
	}

	playlist := filepath.Join(mgr.segmentsDir, "live.m3u8")
	segPattern := filepath.Join(mgr.segmentsDir, "live%03d.ts")

	extraFlags := mgr.extraFlags
	if mgr.getExtraFlags != nil {
		if f := mgr.getExtraFlags(); f != "" {
			extraFlags = f
		}
	}

	args := []string{
		"-y",
		"-i", srtURL,
		"-c:v", "copy",
		"-c:a", "aac",
		"-f", "hls",
		"-hls_time", strconv.Itoa(mgr.hlsTime),
		"-hls_list_size", strconv.Itoa(mgr.hlsListSize),
		"-hls_flags", "delete_segments+append_list",
		"-hls_segment_filename", segPattern,
	}

	if extraFlags != "" {
		parts := strings.Fields(extraFlags)
		args = append(args, parts...)
	}

	args = append(args, playlist)

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, mgr.ffmpegPath, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("ingest: stderr pipe error: %v", err)
		return
	}

	mgr.mu.Lock()
	mgr.cmd = cmd
	mgr.cancel = cancel
	mgr.mu.Unlock()

	if err := cmd.Start(); err != nil {
		log.Printf("ingest: failed to start ffmpeg: %v", err)
		return
	}
	log.Printf("ingest: FFmpeg pid=%d started, waiting for SRT connection...", cmd.Process.Pid)

	connected := false

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			mgr.parseLine(line, &connected)
		}
	}()

	if err := cmd.Wait(); err != nil && cmdCtx.Err() == nil {
		log.Printf("ingest: FFmpeg exited: %v", err)
	}

	if connected {
		mgr.statsAtomic.Store(&Stats{})
		select {
		case mgr.StatusCh <- StatusChange{Online: false}:
		default:
		}
		log.Printf("ingest: stream disconnected")
	}

	mgr.mu.Lock()
	mgr.cmd = nil
	mgr.cancel = nil
	mgr.mu.Unlock()
}

func (mgr *Manager) parseLine(line string, connected *bool) {
	log.Printf("ffmpeg: %s", line)
	if !*connected {
		if connectedRe.MatchString(line) {
			*connected = true
			s := &Stats{
				Online:    true,
				StartedAt: time.Now(),
			}
			mgr.statsAtomic.Store(s)
			log.Printf("ingest: stream connected")
			select {
			case mgr.StatusCh <- StatusChange{Online: true}:
			default:
			}
		}
	}

	if matches := bitrateRe.FindStringSubmatch(line); matches != nil {
		if v, err := strconv.ParseFloat(matches[1], 64); err == nil {
			old := mgr.statsAtomic.Load()
			if old != nil && old.Online {
				updated := *old
				updated.CurrentKbps = v
				mgr.statsAtomic.Store(&updated)
			}
		}
	}
}

func (mgr *Manager) stopCurrentProcess() {
	mgr.mu.Lock()
	cancel := mgr.cancel
	mgr.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Restart kills the current FFmpeg process so it restarts with fresh settings.
func (mgr *Manager) Restart() {
	mgr.mu.Lock()
	cmd := mgr.cmd
	mgr.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

// Stats returns current stream stats (safe for concurrent use).
func (mgr *Manager) Stats() Stats {
	if s := mgr.statsAtomic.Load(); s != nil {
		return *s
	}
	return Stats{}
}

// UptimeSeconds returns the stream uptime in seconds, or 0 if offline.
func (mgr *Manager) UptimeSeconds() int64 {
	s := mgr.Stats()
	if !s.Online {
		return 0
	}
	return int64(time.Since(s.StartedAt).Seconds())
}

// parseLine is also used from io.Reader for testing
func ParseStderrLine(line string) (kbps float64, isConnected bool) {
	if connectedRe.MatchString(line) {
		isConnected = true
	}
	if matches := bitrateRe.FindStringSubmatch(line); matches != nil {
		kbps, _ = strconv.ParseFloat(matches[1], 64)
	}
	return
}

