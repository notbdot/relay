package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/notbdot/relay/internal/config"
	"github.com/notbdot/relay/internal/db"
	"github.com/notbdot/relay/internal/hub"
	"github.com/notbdot/relay/internal/ingest"
	"github.com/notbdot/relay/internal/server"
	"github.com/notbdot/relay/web"
)

const usage = `Relay — SRT live streaming server

Commands:
  relay serve   Start the streaming server
  relay help    Show this help

Configuration is loaded from relay.yaml (if present), with environment
variables taking precedence. See relay.yaml.example for all options.
`

func main() {
	log.SetPrefix("[relay] ")
	log.SetFlags(log.Ltime)

	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	switch cmd {
	case "serve":
		runServe()
	case "help", "--help", "-h":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %q\n\n%s", cmd, usage)
		os.Exit(1)
	}
}

func runServe() {
	cfg, err := config.Load("relay.yaml")
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	database, err := db.Open(cfg.DB.Path)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer database.Close()

	streamKey, isNew, err := database.InitDefaults()
	if err != nil {
		log.Fatalf("db init: %v", err)
	}

	adminPW := cfg.Server.AdminPassword
	if adminPW == "" {
		adminPW = "admin"
	}

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	if isNew {
		fmt.Println("  First run — stream key generated")
		fmt.Printf("  Stream key : %s\n", streamKey)
	}
	fmt.Printf("  Admin password : %s\n", adminPW)
	fmt.Printf("  Viewer → http://localhost:%d/\n", cfg.Server.Port)
	fmt.Printf("  Admin  → http://localhost:%d/admin\n", cfg.Server.Port)
	fmt.Printf("  SRT (OBS) → srt://localhost:%d  (stream key in Stream ID)\n", cfg.SRT.Port)
	fmt.Printf("  SRT (cam) → srt://localhost:%d  (stream key in Stream ID)\n", cfg.SRT.CameraPort)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	h := hub.New()
	go h.Run()

	getStreamKey := func() string {
		k, _ := database.GetConfig("stream_key")
		if k == "" {
			return streamKey
		}
		return k
	}

	getExtraFlags := func() string {
		f, _ := database.GetConfig("ffmpeg_flags")
		return f
	}

	// Main ingest: OBS → /hls/live.m3u8 — this is what viewers see.
	mgr := ingest.New(
		cfg.FFmpeg.Path,
		cfg.SRT.Port,
		cfg.HLS.SegmentsDir,
		cfg.HLS.HLSTime,
		cfg.HLS.HLSListSize,
		cfg.FFmpeg.ExtraFlags,
		getStreamKey,
		getExtraFlags,
		func() string { v, _ := database.GetConfig("quality_preset"); return v },
	)
	go mgr.Start(ctx)

	// Camera ingest: raw camera feed → /hls/camera/live.m3u8 — admin preview only.
	// Always source passthrough (no transcoding). Uses same stream key.
	cameraMgr := ingest.New(
		cfg.FFmpeg.Path,
		cfg.SRT.CameraPort,
		filepath.Join(cfg.HLS.SegmentsDir, "camera"),
		cfg.HLS.HLSTime,
		cfg.HLS.HLSListSize,
		"",
		getStreamKey,
		nil,
		func() string { return "source" },
	)
	go cameraMgr.Start(ctx)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := server.New(addr, &server.Deps{
		DB:            database,
		Hub:           h,
		Ingest:        mgr,
		SegmentsDir:   cfg.HLS.SegmentsDir,
		AdminPassword:   cfg.Server.AdminPassword,
		OBSWebSocketURL: cfg.Server.OBSWebSocketURL,
		ViewerHTML:    web.ViewerHTML,
		AdminHTML:     web.AdminHTML,
		OverlayHTML:   web.OverlayHTML,
	})

	if err := srv.Start(ctx); err != nil && err.Error() != "http: Server closed" {
		log.Printf("server: %v", err)
	}
	log.Println("shutdown complete")
}

