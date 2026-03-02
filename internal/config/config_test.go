package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestExampleConfigParseable(t *testing.T) {
	var cfg Config
	if err := yaml.Unmarshal([]byte(ExampleConfig), &cfg); err != nil {
		t.Fatalf("ExampleConfig is not valid YAML: %v", err)
	}

	// Verify required fields are populated in the example.
	if cfg.Device.Size == "" {
		t.Error("ExampleConfig should set device.size")
	}
	if cfg.S3.Bucket == "" {
		t.Error("ExampleConfig should set s3.bucket")
	}
}
