// Package e2e provides end-to-end tests for cloudblockdev using QEMU VMs.
package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
)

// Prereq represents a prerequisite command that must be available.
type Prereq struct {
	Name        string // Human-readable name
	Command     string // Command to check (looked up in PATH)
	Description string // What it's used for
}

// Prerequisites lists all required external tools for e2e tests.
var Prerequisites = []Prereq{
	{
		Name:        "QEMU",
		Command:     "qemu-system-x86_64",
		Description: "VM execution",
	},
	{
		Name:        "QEMU Image Tool",
		Command:     "qemu-img",
		Description: "Creating overlay images",
	},
	{
		Name:        "Docker",
		Command:     "docker",
		Description: "Running MinIO container",
	},
}

// ISOGenerators lists possible commands for creating cloud-init ISOs.
// At least one must be available.
var ISOGenerators = []string{"genisoimage", "mkisofs"}

// CheckPrerequisites verifies all required tools are installed.
// Returns an error describing missing prerequisites.
func CheckPrerequisites(ctx context.Context) error {
	var missing []string

	for _, prereq := range Prerequisites {
		if _, err := exec.LookPath(prereq.Command); err != nil {
			missing = append(missing, fmt.Sprintf("%s (%s) - needed for %s",
				prereq.Name, prereq.Command, prereq.Description))
		}
	}

	// Check for at least one ISO generator
	hasISOGen := false
	for _, cmd := range ISOGenerators {
		if _, err := exec.LookPath(cmd); err == nil {
			hasISOGen = true
			break
		}
	}
	if !hasISOGen {
		missing = append(missing, fmt.Sprintf("ISO generator (one of: %v) - needed for cloud-init", ISOGenerators))
	}

	// Check Docker is running
	if _, err := exec.LookPath("docker"); err == nil {
		cmd := exec.CommandContext(ctx, "docker", "info")
		if err := cmd.Run(); err != nil {
			missing = append(missing, "Docker daemon not running - needed for MinIO container")
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing prerequisites:\n  - %s", joinLines(missing))
	}

	return nil
}

// RequirePrerequisites calls t.Fatal if prerequisites are not met.
func RequirePrerequisites(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*1e9) // 10 seconds
	defer cancel()

	if err := CheckPrerequisites(ctx); err != nil {
		t.Fatalf("e2e test prerequisites not met: %v", err)
	}
}

// FindISOGenerator returns the path to an available ISO generator command.
func FindISOGenerator() (string, error) {
	for _, cmd := range ISOGenerators {
		if path, err := exec.LookPath(cmd); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("no ISO generator found (tried: %v)", ISOGenerators)
}

func joinLines(lines []string) string {
	result := ""
	for i, line := range lines {
		if i > 0 {
			result += "\n  - "
		}
		result += line
	}
	return result
}
