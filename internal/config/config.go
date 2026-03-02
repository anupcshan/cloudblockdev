// Package config handles device configuration.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the configuration for a cloud block device.
type Config struct {
	// Device configuration
	Device DeviceConfig `yaml:"device"`

	// S3 backend configuration
	S3 S3Config `yaml:"s3"`

	// Server configuration
	Server ServerConfig `yaml:"server"`

	// NBD kernel device configuration
	NBD NBDConfig `yaml:"nbd"`

	// GC configuration
	GC GCConfig `yaml:"gc"`

	// Metrics configuration
	Metrics MetricsConfig `yaml:"metrics"`

	// FileStore configuration (alternative to S3, for testing)
	FileStore FileStoreConfig `yaml:"filestore"`
}

// DeviceConfig holds device-specific settings.
type DeviceConfig struct {
	// Size is the device size (e.g., "10G", "100M", or bytes as int).
	Size string `yaml:"size"`
	// ExportName is the NBD export name. Default: "cloudblockdev".
	ExportName string `yaml:"export_name,omitempty"`
	// MaxWriteBuffer limits the total bytes of sealed data awaiting upload to S3.
	// When this limit is reached, new writes block until uploads complete,
	// providing backpressure proportional to network throughput.
	// Uses size format (e.g., "128M", "1G"). Default: "128M".
	MaxWriteBuffer string `yaml:"max_write_buffer,omitempty"`
}

// S3Config holds S3 connection settings.
type S3Config struct {
	// Bucket is the S3 bucket name (required).
	Bucket string `yaml:"bucket"`
	// Prefix is the key prefix for all objects (e.g., "mydevice/").
	Prefix string `yaml:"prefix,omitempty"`
	// Endpoint is the S3 endpoint URL (for S3-compatible services).
	Endpoint string `yaml:"endpoint,omitempty"`
	// Region is the AWS region. Default: "us-east-1".
	Region string `yaml:"region,omitempty"`
	// AccessKey is the AWS access key (optional, can use environment).
	AccessKey string `yaml:"access_key,omitempty"`
	// SecretKey is the AWS secret key (optional, can use environment).
	SecretKey string `yaml:"secret_key,omitempty"`
}

// ServerConfig holds server settings.
type ServerConfig struct {
	// Listen is the address to listen on. Default: ":10809".
	// Set to empty string to disable the external TCP server.
	Listen string `yaml:"listen,omitempty"`
}

// NBDConfig holds kernel NBD device settings.
type NBDConfig struct {
	// Enabled controls whether to manage a kernel NBD device directly.
	// When enabled, cloudblockdev sets up /dev/nbdX using the netlink interface.
	// Default: false (use external nbd-client).
	Enabled bool `yaml:"enabled,omitempty"`
	// Index is the NBD device index (0 = /dev/nbd0).
	// Set to -1 for auto-assign. Default: 0.
	Index *int `yaml:"index,omitempty"`
	// DeadConnTimeout is how long the kernel waits for reconnection
	// after cloudblockdev exits/crashes before failing I/O.
	// This enables zero-downtime restarts. Default: "120s".
	DeadConnTimeout string `yaml:"dead_conn_timeout,omitempty"`
}

// GCConfig holds garbage collection settings.
type GCConfig struct {
	// Enabled controls whether GC is enabled. Default: true.
	Enabled *bool `yaml:"enabled,omitempty"`
	// Interval is how often to run GC (e.g., "60s", "5m"). Default: "60s".
	Interval string `yaml:"interval,omitempty"`
}

// FileStoreConfig holds filesystem-backed store settings.
// When Dir is set, the filestore is used instead of S3.
type FileStoreConfig struct {
	// Dir is the directory to store blobs in.
	Dir string `yaml:"dir,omitempty"`
	// Latency is the minimum latency for each read/write operation (e.g., "50ms").
	// Used to simulate S3 latency for testing. Default: 0 (no delay).
	Latency string `yaml:"latency,omitempty"`
}

// MetricsConfig holds Prometheus metrics endpoint settings.
type MetricsConfig struct {
	// Listen is the address for the Prometheus metrics endpoint.
	// Set to empty string to disable. Default: ":9090".
	Listen string `yaml:"listen,omitempty"`
}

// LoadConfig loads configuration from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	// Validate required fields
	if cfg.Device.Size == "" {
		return nil, fmt.Errorf("device.size is required")
	}
	if cfg.S3.Bucket == "" && cfg.FileStore.Dir == "" {
		return nil, fmt.Errorf("s3.bucket is required (or set filestore.dir)")
	}

	// Apply defaults
	if cfg.Device.ExportName == "" {
		cfg.Device.ExportName = "cloudblockdev"
	}
	if cfg.Device.MaxWriteBuffer == "" {
		cfg.Device.MaxWriteBuffer = "128M"
	}
	if cfg.S3.Region == "" {
		cfg.S3.Region = "us-east-1"
	}
	if cfg.Server.Listen == "" {
		cfg.Server.Listen = ":10809"
	}

	// GC defaults
	if cfg.GC.Enabled == nil {
		enabled := true
		cfg.GC.Enabled = &enabled
	}
	if cfg.GC.Interval == "" {
		cfg.GC.Interval = "60s"
	}

	// NBD defaults
	if cfg.NBD.Index == nil {
		index := 0
		cfg.NBD.Index = &index
	}
	if cfg.NBD.DeadConnTimeout == "" {
		cfg.NBD.DeadConnTimeout = "120s"
	}

	// Metrics defaults
	if cfg.Metrics.Listen == "" {
		cfg.Metrics.Listen = "127.0.0.1:9090"
	}

	return &cfg, nil
}

// ExampleConfig is a commented YAML config showing all options with defaults.
// Used by the CLI help text to keep documentation in sync with actual defaults.
const ExampleConfig = `  device:
    size: 10G
    export_name: mydev   # optional, default: cloudblockdev

  s3:
    bucket: my-bucket
    prefix: mydevice/    # optional
    endpoint: ""         # optional, for S3-compatible services
    region: us-east-1    # optional, default: us-east-1

  server:
    listen: :10809       # optional, default: :10809, empty to disable

  nbd:
    enabled: true            # optional, manage kernel NBD device directly
    index: 0                 # optional, /dev/nbd0, -1 for auto-assign
    dead_conn_timeout: 120s  # optional, kernel waits for reconnect

  # Alternative to S3, for testing
  filestore:
    dir: /tmp/cloudblockdev  # optional, use local filesystem instead of S3

  gc:
    enabled: true        # optional, default: true
    interval: 60s        # optional, default: 60s

  metrics:
    listen: 127.0.0.1:9090  # optional, Prometheus metrics endpoint
`

// ParseSize parses a size string like "10G" or "100M" into bytes.
func ParseSize(s string) (int64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("empty size")
	}

	var size int64
	var unit byte

	n, err := fmt.Sscanf(s, "%d%c", &size, &unit)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("invalid size format: %s", s)
	}

	if n == 1 {
		// Plain number, assume bytes
		return size, nil
	}

	switch unit {
	case 'b', 'B':
		// bytes
	case 'k', 'K':
		size *= 1024
	case 'm', 'M':
		size *= 1024 * 1024
	case 'g', 'G':
		size *= 1024 * 1024 * 1024
	case 't', 'T':
		size *= 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size unit: %c", unit)
	}

	return size, nil
}

// ParseDuration parses a duration string like "120s" or "5m".
func ParseDuration(s string) (time.Duration, error) {
	return time.ParseDuration(s)
}
