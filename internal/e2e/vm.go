package e2e

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// NBDDrive describes a single NBD-backed drive to attach to the VM.
type NBDDrive struct {
	Server string // address of the NBD server (e.g., "localhost:10809")
	Export string // NBD export name
}

// VMConfig contains configuration for a test VM.
type VMConfig struct {
	// RootImage is the path to the boot disk image (qcow2).
	RootImage string

	// NBDServer is the address of the NBD server (e.g., "localhost:10809").
	// Deprecated: use NBDDrives instead.
	NBDServer string

	// NBDExport is the NBD export name.
	// Deprecated: use NBDDrives instead.
	NBDExport string

	// NBDDrives is a list of NBD-backed drives to attach to the VM.
	// Each drive appears as a virtio block device (/dev/vdb, /dev/vdc, ...).
	NBDDrives []NBDDrive

	// CloudInitISO is the path to the cloud-init ISO.
	CloudInitISO string

	// SSHKey is the SSH key pair for authentication.
	SSHKey *SSHKeyPair

	// Memory is the VM memory in megabytes.
	Memory int

	// CPUs is the number of virtual CPUs.
	CPUs int

	// TempDir is the directory for temporary files.
	TempDir string
}

// DefaultVMConfig returns a VMConfig with sensible defaults.
func DefaultVMConfig() VMConfig {
	return VMConfig{
		Memory: 2048,
		CPUs:   2,
	}
}

// VM represents a running QEMU virtual machine.
type VM struct {
	config  VMConfig
	cmd     *exec.Cmd
	sshPort int
	ssh     *SSHClient
	t       *testing.T

	// Log output
	logMu  sync.Mutex
	logBuf []string
}

// NewVM creates a new VM instance.
func NewVM(t *testing.T, config VMConfig) *VM {
	return &VM{
		config: config,
		t:      t,
	}
}

// Start boots the VM and waits for SSH to become available.
func (vm *VM) Start(ctx context.Context) error {
	// Find a free port for SSH
	sshPort, err := findFreePort()
	if err != nil {
		return fmt.Errorf("finding free port: %w", err)
	}
	vm.sshPort = sshPort

	// Build QEMU command
	args := []string{
		"-m", fmt.Sprintf("%d", vm.config.Memory),
		"-smp", fmt.Sprintf("%d", vm.config.CPUs),
		"-nographic",
		"-serial", "mon:stdio",

		// Try KVM first, fall back to TCG
		"-accel", "kvm",
		"-accel", "tcg",

		// Root disk (overlay image)
		"-drive", fmt.Sprintf("file=%s,if=virtio,format=qcow2", vm.config.RootImage),
	}

	// Add NBD drives. Support both the legacy single-drive field and the new list.
	nbdDrives := vm.config.NBDDrives
	if len(nbdDrives) == 0 && vm.config.NBDServer != "" {
		nbdDrives = []NBDDrive{{Server: vm.config.NBDServer, Export: vm.config.NBDExport}}
	}
	for _, d := range nbdDrives {
		args = append(args, "-drive", fmt.Sprintf("file=nbd://%s/%s,if=virtio,format=raw,cache=none", d.Server, d.Export))
	}

	// Network with SSH port forwarding
	args = append(args,
		"-netdev", fmt.Sprintf("user,id=net0,hostfwd=tcp::%d-:22", vm.sshPort),
		"-device", "virtio-net,netdev=net0",
	)

	// Only add cloud-init ISO if specified (not needed for golden image)
	if vm.config.CloudInitISO != "" {
		args = append(args, "-cdrom", vm.config.CloudInitISO)
	}

	vm.cmd = exec.CommandContext(ctx, "qemu-system-x86_64", args...)

	// Capture stdout and stderr
	stdout, err := vm.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("creating stdout pipe: %w", err)
	}
	stderr, err := vm.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("creating stderr pipe: %w", err)
	}

	// Start log capture goroutines
	go vm.captureOutput("stdout", stdout)
	go vm.captureOutput("stderr", stderr)

	// Start QEMU
	vm.t.Logf("Starting QEMU VM (SSH port: %d)", vm.sshPort)
	if err := vm.cmd.Start(); err != nil {
		return fmt.Errorf("starting QEMU: %w", err)
	}

	// Wait for SSH - shorter timeout for golden image (no package install needed)
	sshTimeout := 2 * time.Minute
	if vm.config.CloudInitISO != "" {
		sshTimeout = 5 * time.Minute // Longer for cloud-init
	}
	sshCtx, cancel := context.WithTimeout(ctx, sshTimeout)
	defer cancel()

	vm.t.Log("Waiting for SSH to become available...")
	sshAddr := fmt.Sprintf("localhost:%d", vm.sshPort)
	sshClient, err := WaitForSSH(sshCtx, sshAddr, vm.config.SSHKey.Signer, "test")
	if err != nil {
		vm.dumpLogs()
		return fmt.Errorf("waiting for SSH: %w", err)
	}
	vm.ssh = sshClient

	// Only wait for cloud-init if ISO was provided
	if vm.config.CloudInitISO != "" {
		vm.t.Log("Waiting for cloud-init to complete...")
		ciCtx, ciCancel := context.WithTimeout(ctx, 5*time.Minute)
		defer ciCancel()
		if err := vm.waitForCloudInit(ciCtx); err != nil {
			vm.dumpLogs()
			return fmt.Errorf("waiting for cloud-init: %w", err)
		}
	}

	vm.t.Log("VM is ready")
	return nil
}

// waitForCloudInit waits for cloud-init to finish.
func (vm *VM) waitForCloudInit(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Check if cloud-init has finished
			stdout, _, err := vm.ssh.Run(ctx, "cloud-init status 2>/dev/null || echo 'waiting'")
			if err != nil {
				// SSH might have disconnected during reboot, retry
				continue
			}
			// "done" = normal completion, "disabled" = already ran and disabled itself
			if strings.Contains(stdout, "done") || strings.Contains(stdout, "disabled") {
				return nil
			}
			if strings.Contains(stdout, "error") {
				return fmt.Errorf("cloud-init failed: %s", stdout)
			}
			vm.t.Logf("Cloud-init status: %s", strings.TrimSpace(stdout))
		}
	}
}

// truncateForLog truncates a string for logging if it exceeds maxLen.
func truncateForLog(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + fmt.Sprintf("... (%d bytes total)", len(s))
}

// Run executes a command in the VM and returns stdout.
func (vm *VM) Run(ctx context.Context, cmd string) (string, error) {
	vm.t.Logf("VM exec: %s", truncateForLog(cmd, 200))
	stdout, stderr, err := vm.ssh.Run(ctx, cmd)
	if stdout != "" {
		vm.t.Logf("  stdout: %s", truncateForLog(stdout, 500))
	}
	if stderr != "" {
		vm.t.Logf("  stderr: %s", truncateForLog(stderr, 500))
	}
	if err != nil {
		return stdout, fmt.Errorf("command %q failed: %w\nstderr: %s", truncateForLog(cmd, 200), err, stderr)
	}
	return stdout, nil
}

// RunSudo executes a command with sudo in the VM.
func (vm *VM) RunSudo(ctx context.Context, cmd string) (string, error) {
	return vm.Run(ctx, "sudo "+cmd)
}

// Shutdown gracefully shuts down the VM.
func (vm *VM) Shutdown(ctx context.Context) error {
	if vm.ssh != nil {
		// Try graceful shutdown
		vm.t.Log("Shutting down VM...")
		_, _, _ = vm.ssh.Run(ctx, "sudo poweroff")
		vm.ssh.Close()
		vm.ssh = nil
	}

	if vm.cmd != nil && vm.cmd.Process != nil {
		// Wait for process to exit with timeout
		done := make(chan error, 1)
		go func() {
			done <- vm.cmd.Wait()
		}()

		select {
		case <-time.After(30 * time.Second):
			vm.t.Log("VM did not shut down gracefully, killing...")
			vm.cmd.Process.Kill()
		case <-done:
			vm.t.Log("VM shut down")
		}
	}

	return nil
}

// captureOutput reads from a pipe and logs the output.
func (vm *VM) captureOutput(name string, r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		vm.logMu.Lock()
		vm.logBuf = append(vm.logBuf, fmt.Sprintf("[%s] %s", name, line))
		vm.logMu.Unlock()
	}
}

// dumpLogs outputs captured VM logs for debugging.
func (vm *VM) dumpLogs() {
	vm.logMu.Lock()
	defer vm.logMu.Unlock()

	vm.t.Log("=== VM Log Output ===")
	for _, line := range vm.logBuf {
		vm.t.Log(line)
	}
	vm.t.Log("=== End VM Log ===")
}

// findFreePort finds an available TCP port.
func findFreePort() (int, error) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// SetupTestVM creates all necessary files and returns a configured VM.
// Uses a pre-configured golden image with ZFS already installed for fast startup.
// This is a convenience function that handles:
// - Ensuring the golden image exists (creating it if needed)
// - Creating an overlay image for this test
// - Loading the cached SSH keys
func SetupTestVM(t *testing.T, nbdServer, nbdExport string) (*VM, string, error) {
	t.Helper()

	// Create temp directory for this test
	tempDir, err := os.MkdirTemp("", "cloudblockdev-e2e-*")
	if err != nil {
		return nil, "", fmt.Errorf("creating temp directory: %w", err)
	}

	// Ensure golden image exists (this will create it on first run)
	goldenImage, sshKey, err := EnsureGoldenImage()
	if err != nil {
		return nil, tempDir, fmt.Errorf("ensuring golden image: %w", err)
	}

	// Create overlay image for this test
	overlayPath := filepath.Join(tempDir, "root.qcow2")
	if err := CreateOverlayImage(goldenImage, overlayPath); err != nil {
		return nil, tempDir, fmt.Errorf("creating overlay image: %w", err)
	}

	// Create VM config - no cloud-init needed since golden image is pre-configured
	config := DefaultVMConfig()
	config.RootImage = overlayPath
	config.NBDServer = nbdServer
	config.NBDExport = nbdExport
	config.SSHKey = sshKey
	config.TempDir = tempDir
	// CloudInitISO left empty - not needed for golden image

	return NewVM(t, config), tempDir, nil
}

// CleanupTestVM removes temporary files created by SetupTestVM.
func CleanupTestVM(tempDir string) error {
	if tempDir != "" {
		return os.RemoveAll(tempDir)
	}
	return nil
}
