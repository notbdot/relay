package config

import (
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server  ServerConfig  `yaml:"server"`
	SRT     SRTConfig     `yaml:"srt"`
	HLS     HLSConfig     `yaml:"hls"`
	DB      DBConfig      `yaml:"db"`
	FFmpeg  FFmpegConfig  `yaml:"ffmpeg"`
}

type ServerConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type SRTConfig struct {
	Port       int `yaml:"port"`
	CameraPort int `yaml:"camera_port"`
}

type HLSConfig struct {
	SegmentsDir string `yaml:"segments_dir"`
	HLSTime     int    `yaml:"hls_time"`
	HLSListSize int    `yaml:"hls_list_size"`
}

type DBConfig struct {
	Path string `yaml:"path"`
}

type FFmpegConfig struct {
	Path       string `yaml:"path"`
	ExtraFlags string `yaml:"extra_flags"`
}

func Load(path string) (*Config, error) {
	cfg := &Config{
		Server: ServerConfig{Host: "0.0.0.0", Port: 2935},
		SRT:    SRTConfig{Port: 9999, CameraPort: 9998},
		HLS: HLSConfig{
			SegmentsDir: "./segments",
			HLSTime:     2,
			HLSListSize: 6,
		},
		DB:     DBConfig{Path: "./relay.db"},
		FFmpeg: FFmpegConfig{Path: "ffmpeg"},
	}

	data, err := os.ReadFile(path)
	if err == nil {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, err
		}
	}

	// env overrides
	if v := os.Getenv("RELAY_SERVER_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.Server.Port = p
		}
	}
	if v := os.Getenv("RELAY_SRT_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.SRT.Port = p
		}
	}
	if v := os.Getenv("RELAY_SRT_CAMERA_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.SRT.CameraPort = p
		}
	}
	if v := os.Getenv("RELAY_DB_PATH"); v != "" {
		cfg.DB.Path = v
	}
	if v := os.Getenv("RELAY_SEGMENTS_DIR"); v != "" {
		cfg.HLS.SegmentsDir = v
	}
	if v := os.Getenv("RELAY_FFMPEG_PATH"); v != "" {
		cfg.FFmpeg.Path = v
	}

	return cfg, nil
}
