package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/notbdot/sluice/internal/config"
	"github.com/notbdot/sluice/internal/db"
	"github.com/notbdot/sluice/internal/hub"
	"github.com/notbdot/sluice/internal/ingest"
	"github.com/notbdot/sluice/internal/server"
	"github.com/notbdot/sluice/web"
)

const usage = `Sluice — SRT live streaming server

Commands:
  sluice serve              Start the streaming server
  sluice reset-admin-token  Regenerate and print the admin token
  sluice help               Show this help

Configuration is loaded from sluice.yaml (if present), with environment
variables taking precedence. See sluice.yaml.example for all options.
`

func main() {
	log.SetPrefix("[sluice] ")
	log.SetFlags(log.Ltime)

	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	switch cmd {
	case "serve":
		runServe()
	case "reset-admin-token":
		runResetAdminToken()
	case "help", "--help", "-h":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %q\n\n%s", cmd, usage)
		os.Exit(1)
	}
}

func runServe() {
	cfg, err := config.Load("sluice.yaml")
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	database, err := db.Open(cfg.DB.Path)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer database.Close()

	streamKey, _, isNew, err := database.InitDefaults()
	if err != nil {
		log.Fatalf("db init: %v", err)
	}

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	if isNew {
		fmt.Println("  First run — stream key generated")
		fmt.Printf("  Stream key  : %s\n", streamKey)
	}
	fmt.Printf("  Viewer  → http://localhost:%d/\n", cfg.Server.Port)
	fmt.Printf("  Admin   → http://localhost:%d/admin\n", cfg.Server.Port)
	fmt.Printf("  SRT in  → srt://localhost:%d  (stream key goes in Stream ID field)\n", cfg.SRT.Port)
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

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := server.New(addr, &server.Deps{
		DB:          database,
		Hub:         h,
		Ingest:      mgr,
		SegmentsDir: cfg.HLS.SegmentsDir,
		ViewerHTML:  web.ViewerHTML,
		AdminHTML:   web.AdminHTML,
		OverlayHTML: web.OverlayHTML,
	})

	if err := srv.Start(ctx); err != nil && err.Error() != "http: Server closed" {
		log.Printf("server: %v", err)
	}
	log.Println("shutdown complete")
}

func runResetAdminToken() {
	cfg, err := config.Load("sluice.yaml")
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	database, err := db.Open(cfg.DB.Path)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer database.Close()

	token, err := database.RegenerateAdminToken()
	if err != nil {
		log.Fatalf("regenerate: %v", err)
	}

	fmt.Printf("New admin token : %s\n", token)
	fmt.Printf("Admin URL       : http://localhost:%d/admin?token=%s\n", cfg.Server.Port, token)
}
