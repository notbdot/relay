<img src="brand/logo.svg" alt="Relay" width="148"><br><br>

[![CI](https://img.shields.io/github/actions/workflow/status/notbdot/relay/docker.yml?label=build&logo=github)](https://github.com/notbdot/relay/actions/workflows/docker.yml)
[![Docker Image](https://img.shields.io/badge/ghcr.io-notbdot%2Frelay-blue?logo=docker)](https://github.com/notbdot/relay/pkgs/container/relay)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/notbdot/relay)](https://goreportcard.com/report/github.com/notbdot/relay)

Self-hosted SRT live streaming server. Accepts an OBS stream over SRT, transcodes to HLS, and serves a viewer page with live chat. Built as a single Go binary with no database dependencies.

> **Built by GitHub Actions** — multi-arch (amd64, arm64) Docker images are automatically built and pushed to [`ghcr.io/notbdot/relay`](https://github.com/notbdot/relay/pkgs/container/relay) on every push to `main`.

## Quick start (Docker)

Pull the pre-built image and run it — no build step required:

```bash
docker run -d \
  --name relay \
  --network host \
  --restart unless-stopped \
  -v $(pwd)/relay.yaml:/etc/relay.yaml:ro \
  -v $(pwd)/segments:/segments \
  ghcr.io/notbdot/relay:latest
```

## Features

- **SRT ingest** — accepts streams from OBS (or any SRT source) on a configurable port
- **Dual inputs** — separate SRT port for a camera feed; shown in the admin monitor, not broadcast to viewers
- **HLS output** — adaptive-quality transcoding via FFmpeg (1080p / 720p / 480p / 360p / source pass-through)
- **Live chat** — WebSocket-based chat with rate limiting, message history, and admin moderation (ban, delete, clear)
- **Admin panel** — token or password auth, real-time bitrate sparkline, dual preview monitors, stream config
- **Scene system** — switch between Live / Starting Soon / BRB / Ending scenes from the admin panel
- **Chat overlay** — dedicated `/overlay` endpoint for use as an OBS browser source
- **Persistent storage** — JSON file, no database required
- **Single binary** — ships as a self-contained binary; Docker image includes FFmpeg

## Quick start (Docker Compose)

Use the docker-compose file for the standard setup:

```bash
# 1. grab the compose file
curl -O https://raw.githubusercontent.com/notbdot/relay/main/docker-compose.yml

# 2. create a config (optional — defaults work out of the box)
curl -O https://raw.githubusercontent.com/notbdot/relay/main/relay.yaml.example
cp relay.yaml.example relay.yaml

# 3. start
docker compose up -d
```

On first run, relay prints the generated stream key:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  First run — stream key generated
  Stream key     : XXXX-XXXX-XXXX
  Admin password : admin
  Viewer → http://localhost:2935/
  Admin  → http://localhost:2935/admin
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

The stream key goes in OBS. The default admin password is `admin` — set `admin_password` in `relay.yaml` before exposing to the internet.

## Updating

```bash
docker compose pull && docker compose up -d
```

## OBS setup

1. Settings → Stream → Service: **Custom**
2. Server: `srt://your-server-ip:9999`
3. Stream Key: leave blank
4. Open Settings → Output → Advanced → set **Stream ID** to your stream key

The stream key is validated via the SRT stream ID field, not the OBS "Stream Key" field.

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 2935 | TCP | HTTP — viewer, admin, API |
| 9999 | UDP | SRT — OBS / main stream input |
| 9998 | UDP | SRT — camera feed (admin preview only) |

> **Note:** relay uses `network_mode: host` in Docker because bridge NAT silently drops SRT/UDP stream data. This means it runs on Linux only — Docker Desktop for Mac/Windows does not support host networking.

## Configuration

Configuration is loaded from `relay.yaml` (if present). Environment variables override file values.

```yaml
server:
  host: "0.0.0.0"
  port: 2935              # RELAY_SERVER_PORT
  admin_password: "admin"  # RELAY_ADMIN_PASSWORD — change before exposing to the internet

srt:
  port: 9999              # RELAY_SRT_PORT
  camera_port: 9998       # RELAY_SRT_CAMERA_PORT

hls:
  segments_dir: "./segments"   # RELAY_SEGMENTS_DIR
  hls_time: 2                  # segment duration in seconds
  hls_list_size: 6             # segments kept in playlist

db:
  path: "./relay.db"           # RELAY_DB_PATH

ffmpeg:
  path: "ffmpeg"               # RELAY_FFMPEG_PATH
  extra_flags: ""              # appended to ffmpeg args, e.g. "-vf scale=1280:720"
```

## CLI

```
relay serve   Start the streaming server (default)
relay help    Show help
```

## Building from source

Requires Go 1.22+ and FFmpeg installed.

```bash
git clone https://github.com/notbdot/relay
cd relay
go build -o relay .
./relay serve
```

## Behind a reverse proxy

relay does not handle TLS. Run it behind nginx or Caddy for HTTPS.

**Caddy example:**
```
stream.example.com {
    reverse_proxy localhost:2935
}
```

**nginx example:**
```nginx
server {
    listen 443 ssl;
    server_name stream.example.com;

    location / {
        proxy_pass http://localhost:2935;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
    }
}
```

WebSocket (`/ws`) requires the `Upgrade` headers above.

## Architecture

```
OBS  ──SRT:9999──▶  FFmpeg  ──HLS──▶  /segments/live.m3u8  ──▶  viewers
Cam  ──SRT:9998──▶  FFmpeg  ──HLS──▶  /segments/camera/     ──▶  admin only

Browser  ──WS:/ws──▶  Hub  ──broadcast──▶  all clients
Admin    ──REST──▶   Server
```

- Stream key authentication happens at the SRT layer via the stream ID field before FFmpeg receives any data
- The WebSocket hub fans out stream status, chat messages, bans, and deletes to all connected clients
- All persistent data (chat history, sessions, config, banned users) lives in a single `relay.json` file

## License

MIT
