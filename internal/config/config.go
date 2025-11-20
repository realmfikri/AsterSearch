package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
	"gopkg.in/yaml.v3"
)

// AppConfig captures configuration for the server, storage, and index defaults.
type AppConfig struct {
	Server        ServerConfig        `toml:"server" yaml:"server"`
	Paths         PathsConfig         `toml:"paths" yaml:"paths"`
	IndexDefaults IndexDefaultsConfig `toml:"index_defaults" yaml:"index_defaults"`
	Logging       LoggingConfig       `toml:"logging" yaml:"logging"`
	Metrics       MetricsConfig       `toml:"metrics" yaml:"metrics"`
}

// ServerConfig controls network settings.
type ServerConfig struct {
	Listen string `toml:"listen" yaml:"listen"`
}

// PathsConfig configures the on-disk layout.
type PathsConfig struct {
	IndexDir string `toml:"index_dir" yaml:"index_dir"`
}

// IndexDefaultsConfig provides baseline settings used when new indexes are created.
type IndexDefaultsConfig struct {
	Tokenizer      string        `toml:"tokenizer" yaml:"tokenizer"`
	BM25           BM25Config    `toml:"bm25" yaml:"bm25"`
	MergeInterval  time.Duration `toml:"merge_interval" yaml:"merge_interval"`
	MergeThreshold int           `toml:"merge_threshold" yaml:"merge_threshold"`
	FlushMaxDocs   int           `toml:"flush_max_documents" yaml:"flush_max_documents"`
	FlushMaxPosts  int           `toml:"flush_max_postings" yaml:"flush_max_postings"`
}

// BM25Config mirrors the scoring parameters exposed by the index package.
type BM25Config struct {
	K1 float64 `toml:"k1" yaml:"k1"`
	B  float64 `toml:"b" yaml:"b"`
}

// LoggingConfig toggles observability around requests.
type LoggingConfig struct {
	RequestLogs *bool `toml:"request_logs" yaml:"request_logs"`
}

// MetricsConfig enables counters/telemetry endpoints.
type MetricsConfig struct {
	Enabled *bool `toml:"enabled" yaml:"enabled"`
}

// DefaultConfig returns the baseline configuration used when no file is supplied.
func DefaultConfig() AppConfig {
	return AppConfig{
		Server: ServerConfig{Listen: ":8080"},
		Paths:  PathsConfig{IndexDir: "data/indexes"},
		IndexDefaults: IndexDefaultsConfig{
			Tokenizer:      "standard",
			BM25:           BM25Config{K1: 1.2, B: 0.75},
			MergeInterval:  30 * time.Second,
			MergeThreshold: 4,
			FlushMaxDocs:   512,
			FlushMaxPosts:  50000,
		},
		Logging: LoggingConfig{RequestLogs: boolPtr(true)},
		Metrics: MetricsConfig{Enabled: boolPtr(true)},
	}
}

// Load reads the provided config path, merging it onto the defaults.
func Load(path string) (AppConfig, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return AppConfig{}, fmt.Errorf("read config: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(path))
	var fileCfg AppConfig
	switch ext {
	case ".toml":
		if err := toml.Unmarshal(content, &fileCfg); err != nil {
			return AppConfig{}, fmt.Errorf("parse toml: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(content, &fileCfg); err != nil {
			return AppConfig{}, fmt.Errorf("parse yaml: %w", err)
		}
	default:
		return AppConfig{}, errors.New("config file must be .toml, .yaml, or .yml")
	}

	merged := mergeConfig(cfg, fileCfg)
	return merged, nil
}

func mergeConfig(base, override AppConfig) AppConfig {
	if override.Server.Listen != "" {
		base.Server.Listen = override.Server.Listen
	}
	if override.Paths.IndexDir != "" {
		base.Paths.IndexDir = override.Paths.IndexDir
	}

	if override.IndexDefaults.Tokenizer != "" {
		base.IndexDefaults.Tokenizer = override.IndexDefaults.Tokenizer
	}
	if override.IndexDefaults.BM25.K1 != 0 {
		base.IndexDefaults.BM25.K1 = override.IndexDefaults.BM25.K1
	}
	if override.IndexDefaults.BM25.B != 0 {
		base.IndexDefaults.BM25.B = override.IndexDefaults.BM25.B
	}
	if override.IndexDefaults.MergeInterval != 0 {
		base.IndexDefaults.MergeInterval = override.IndexDefaults.MergeInterval
	}
	if override.IndexDefaults.MergeThreshold != 0 {
		base.IndexDefaults.MergeThreshold = override.IndexDefaults.MergeThreshold
	}
	if override.IndexDefaults.FlushMaxDocs != 0 {
		base.IndexDefaults.FlushMaxDocs = override.IndexDefaults.FlushMaxDocs
	}
	if override.IndexDefaults.FlushMaxPosts != 0 {
		base.IndexDefaults.FlushMaxPosts = override.IndexDefaults.FlushMaxPosts
	}

	if override.Logging.RequestLogs != nil {
		base.Logging.RequestLogs = override.Logging.RequestLogs
	}

	if override.Metrics.Enabled != nil {
		base.Metrics.Enabled = override.Metrics.Enabled
	}

	return base
}

// ToBM25 converts the config BM25 representation into the value expected by the index package.
func (cfg AppConfig) ToBM25() (float64, float64) {
	return cfg.IndexDefaults.BM25.K1, cfg.IndexDefaults.BM25.B
}

func boolPtr(v bool) *bool {
	return &v
}
