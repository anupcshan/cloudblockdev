package e2e

import (
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// CloudInitConfig contains configuration for cloud-init.
type CloudInitConfig struct {
	SSHPublicKey string
	Hostname     string
	Username     string
	InstanceID   string // Unique ID to force cloud-init to re-run
}

// DefaultCloudInitConfig returns a default configuration.
func DefaultCloudInitConfig(sshPubKey string) CloudInitConfig {
	return CloudInitConfig{
		SSHPublicKey: strings.TrimSpace(sshPubKey),
		Hostname:     "cloudblockdev-test",
		Username:     "test",
		InstanceID:   generateInstanceID(),
	}
}

// generateInstanceID creates a unique instance ID.
func generateInstanceID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("cloudblockdev-test-%x", b)
}

// userDataTemplate is the cloud-init user-data configuration.
const userDataTemplate = `#cloud-config
hostname: {{.Hostname}}

users:
  - name: {{.Username}}
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    lock_passwd: true
    ssh_authorized_keys:
      - {{.SSHPublicKey}}

# Install ZFS tools and test utilities
package_update: true
packages:
  - zfsutils-linux
  - fio

# Disable cloud-init on subsequent boots for faster restarts
runcmd:
  - touch /etc/cloud/cloud-init.disabled

# Reduce boot time by disabling unnecessary services
bootcmd:
  - systemctl mask apt-daily.service apt-daily-upgrade.service

# Output boot logs to console for debugging
output:
  all: '| tee -a /var/log/cloud-init-output.log'
`

// metaDataTemplate is the cloud-init meta-data.
const metaDataTemplate = `instance-id: {{.InstanceID}}
local-hostname: {{.Hostname}}
`

// CreateCloudInitISO creates a cloud-init NoCloud ISO image.
// Returns the path to the created ISO file.
func CreateCloudInitISO(dir string, config CloudInitConfig) (string, error) {
	// Create temporary directory for cloud-init files
	ciDir := filepath.Join(dir, "cidata")
	if err := os.MkdirAll(ciDir, 0755); err != nil {
		return "", fmt.Errorf("creating cloud-init directory: %w", err)
	}

	// Write user-data
	userData := replaceTemplateVars(userDataTemplate, config)
	userDataPath := filepath.Join(ciDir, "user-data")
	if err := os.WriteFile(userDataPath, []byte(userData), 0644); err != nil {
		return "", fmt.Errorf("writing user-data: %w", err)
	}

	// Write meta-data
	metaData := replaceTemplateVars(metaDataTemplate, config)
	metaDataPath := filepath.Join(ciDir, "meta-data")
	if err := os.WriteFile(metaDataPath, []byte(metaData), 0644); err != nil {
		return "", fmt.Errorf("writing meta-data: %w", err)
	}

	// Find ISO generator
	isoGen, err := FindISOGenerator()
	if err != nil {
		return "", err
	}

	// Create ISO
	isoPath := filepath.Join(dir, "cloud-init.iso")
	cmd := exec.Command(isoGen,
		"-output", isoPath,
		"-volid", "cidata",
		"-joliet",
		"-rock",
		userDataPath,
		metaDataPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("creating ISO with %s: %w\noutput: %s", isoGen, err, output)
	}

	return isoPath, nil
}

// replaceTemplateVars performs simple template variable replacement.
func replaceTemplateVars(template string, config CloudInitConfig) string {
	result := template
	result = strings.ReplaceAll(result, "{{.Hostname}}", config.Hostname)
	result = strings.ReplaceAll(result, "{{.Username}}", config.Username)
	result = strings.ReplaceAll(result, "{{.SSHPublicKey}}", config.SSHPublicKey)
	result = strings.ReplaceAll(result, "{{.InstanceID}}", config.InstanceID)
	return result
}
