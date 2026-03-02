package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	// UbuntuImageURL is the URL for Ubuntu 25.10 (Questing) cloud image.
	UbuntuImageURL = "https://cloud-images.ubuntu.com/questing/current/questing-server-cloudimg-amd64.img"

	// UbuntuImageName is the filename for the cached image.
	UbuntuImageName = "ubuntu-25.10-cloudimg-amd64.img"

	// GoldenImageName is the filename for the pre-configured image with ZFS.
	GoldenImageName = "ubuntu-25.10-zfs-golden.qcow2"
)

// GetCacheDir returns the directory for caching test images.
// Uses CLOUDBLOCKDEV_TEST_CACHE env var if set, otherwise ~/.cache/cloudblockdev-test/
func GetCacheDir() (string, error) {
	if dir := os.Getenv("CLOUDBLOCKDEV_TEST_CACHE"); dir != "" {
		return dir, nil
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("getting home directory: %w", err)
	}

	return filepath.Join(homeDir, ".cache", "cloudblockdev-test"), nil
}

// EnsureUbuntuImage downloads the Ubuntu cloud image if not already cached.
// Returns the path to the cached image.
func EnsureUbuntuImage() (string, error) {
	cacheDir, err := GetCacheDir()
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return "", fmt.Errorf("creating cache directory: %w", err)
	}

	imagePath := filepath.Join(cacheDir, UbuntuImageName)

	// Check if image already exists
	if _, err := os.Stat(imagePath); err == nil {
		return imagePath, nil
	}

	// Download the image
	fmt.Printf("Downloading Ubuntu cloud image (this may take a while)...\n")
	if err := downloadFile(UbuntuImageURL, imagePath); err != nil {
		// Clean up partial download
		os.Remove(imagePath)
		return "", fmt.Errorf("downloading Ubuntu image: %w", err)
	}

	fmt.Printf("Ubuntu cloud image cached at: %s\n", imagePath)
	return imagePath, nil
}

// CreateOverlayImage creates a qcow2 overlay image backed by the base image.
// This allows multiple VMs to share the same base image while having
// independent writes.
func CreateOverlayImage(baseImage, overlayPath string) error {
	// qemu-img create -f qcow2 -b <base> -F qcow2 <overlay>
	cmd := exec.Command("qemu-img", "create",
		"-f", "qcow2",
		"-b", baseImage,
		"-F", "qcow2",
		overlayPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("creating overlay image: %w\noutput: %s", err, output)
	}
	return nil
}

// EnsureGoldenImage ensures a pre-configured image with ZFS is available.
// This image has ZFS installed and a test user configured, eliminating the
// need for cloud-init and network access during tests.
// Returns the path to the golden image and the SSH key pair to use.
func EnsureGoldenImage() (string, *SSHKeyPair, error) {
	cacheDir, err := GetCacheDir()
	if err != nil {
		return "", nil, err
	}

	goldenPath := filepath.Join(cacheDir, GoldenImageName)
	sshKeyPath := filepath.Join(cacheDir, "test_ssh_key")

	// Check if golden image and SSH key already exist
	if _, err := os.Stat(goldenPath); err == nil {
		if _, err := os.Stat(sshKeyPath); err == nil {
			// Load existing SSH key
			sshKey, err := LoadSSHKeyPair(sshKeyPath)
			if err != nil {
				return "", nil, fmt.Errorf("loading SSH key: %w", err)
			}
			return goldenPath, sshKey, nil
		}
	}

	// Need to create the golden image
	fmt.Println("Creating golden image with ZFS pre-installed (one-time setup)...")

	// Ensure base image exists
	baseImage, err := EnsureUbuntuImage()
	if err != nil {
		return "", nil, err
	}

	// Generate and save SSH key pair
	sshKey, err := GenerateSSHKeyPair()
	if err != nil {
		return "", nil, fmt.Errorf("generating SSH key: %w", err)
	}
	if err := SaveSSHKeyPair(sshKey, sshKeyPath); err != nil {
		return "", nil, fmt.Errorf("saving SSH key: %w", err)
	}

	// Create the golden image
	if err := createGoldenImage(baseImage, goldenPath, sshKey); err != nil {
		os.Remove(goldenPath)
		os.Remove(sshKeyPath)
		os.Remove(sshKeyPath + ".pub")
		return "", nil, fmt.Errorf("creating golden image: %w", err)
	}

	fmt.Printf("Golden image created at: %s\n", goldenPath)
	return goldenPath, sshKey, nil
}

// createGoldenImage boots the base image, installs ZFS, and saves the result.
func createGoldenImage(baseImage, goldenPath string, sshKey *SSHKeyPair) error {
	// Create a working directory for golden image creation
	workDir, err := os.MkdirTemp("", "cloudblockdev-golden-*")
	if err != nil {
		return fmt.Errorf("creating work directory: %w", err)
	}
	defer os.RemoveAll(workDir)

	// Create overlay on base image (this will become our golden image)
	overlayPath := filepath.Join(workDir, "overlay.qcow2")
	if err := CreateOverlayImage(baseImage, overlayPath); err != nil {
		return err
	}

	// Resize to 20GB so there's room for build-essential and kernel builds.
	resizeCmd := exec.Command("qemu-img", "resize", overlayPath, "20G")
	if output, err := resizeCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("resizing overlay: %w\noutput: %s", err, output)
	}

	// Create cloud-init ISO for setup
	ciConfig := DefaultCloudInitConfig(sshKey.PublicKey)
	ciISO, err := CreateCloudInitISO(workDir, ciConfig)
	if err != nil {
		return fmt.Errorf("creating cloud-init ISO: %w", err)
	}

	// Find a free port for SSH
	sshPort, err := findFreePort()
	if err != nil {
		return fmt.Errorf("finding free port: %w", err)
	}

	// Boot the VM to install ZFS
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	fmt.Println("  Booting VM to install ZFS...")
	qemuArgs := []string{
		"-m", "2048",
		"-smp", "2",
		"-nographic",
		"-serial", "mon:stdio",
		"-accel", "kvm",
		"-accel", "tcg",
		"-drive", fmt.Sprintf("file=%s,if=virtio,format=qcow2", overlayPath),
		"-cdrom", ciISO,
		"-netdev", fmt.Sprintf("user,id=net0,hostfwd=tcp::%d-:22", sshPort),
		"-device", "virtio-net,netdev=net0",
	}

	cmd := exec.CommandContext(ctx, "qemu-system-x86_64", qemuArgs...)
	// Discard output to avoid noise
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting QEMU: %w", err)
	}

	// Wait for SSH to become available
	fmt.Println("  Waiting for SSH...")
	sshAddr := fmt.Sprintf("localhost:%d", sshPort)
	sshClient, err := WaitForSSH(ctx, sshAddr, sshKey.Signer, "test")
	if err != nil {
		cmd.Process.Kill()
		return fmt.Errorf("waiting for SSH: %w", err)
	}

	// Wait for cloud-init to complete
	fmt.Println("  Waiting for cloud-init to complete (installing ZFS)...")
	if err := waitForCloudInitComplete(ctx, sshClient); err != nil {
		sshClient.Close()
		cmd.Process.Kill()
		return fmt.Errorf("waiting for cloud-init: %w", err)
	}

	// Grow partition to fill resized disk.
	fmt.Println("  Growing root partition...")
	sshClient.Run(ctx, "sudo growpart /dev/vda 1")
	sshClient.Run(ctx, "sudo resize2fs /dev/vda1")

	// Verify ZFS is installed
	fmt.Println("  Verifying ZFS installation...")
	stdout, _, err := sshClient.Run(ctx, "which zpool && zpool version")
	if err != nil {
		sshClient.Close()
		cmd.Process.Kill()
		return fmt.Errorf("ZFS not installed correctly: %w", err)
	}
	fmt.Printf("  ZFS installed: %s\n", strings.Split(stdout, "\n")[1])

	// Install build tools (gcc, make, etc.) so tests can compile software.
	fmt.Println("  Installing build-essential and kernel build deps...")
	_, stderr, err := sshClient.Run(ctx, "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential flex bison bc libelf-dev libssl-dev")
	if err != nil {
		sshClient.Close()
		cmd.Process.Kill()
		return fmt.Errorf("installing build-essential: %w\nstderr: %s", err, stderr)
	}

	// Clean up cloud-init so it doesn't run again
	fmt.Println("  Cleaning up cloud-init state...")
	sshClient.Run(ctx, "sudo cloud-init clean --logs")
	sshClient.Run(ctx, "sudo rm -rf /var/lib/cloud/instances")

	// Clear bash history and logs for smaller image
	sshClient.Run(ctx, "sudo rm -f /home/test/.bash_history")
	sshClient.Run(ctx, "sudo truncate -s 0 /var/log/*.log 2>/dev/null || true")

	// Shutdown cleanly
	fmt.Println("  Shutting down VM...")
	sshClient.Run(ctx, "sudo poweroff")
	sshClient.Close()

	// Wait for QEMU to exit
	cmd.Wait()

	// Convert overlay to standalone qcow2 (flatten it)
	fmt.Println("  Flattening image...")
	convertCmd := exec.Command("qemu-img", "convert",
		"-O", "qcow2",
		"-c", // compress
		overlayPath,
		goldenPath,
	)
	if output, err := convertCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("converting image: %w\noutput: %s", err, output)
	}

	return nil
}

// waitForCloudInitComplete waits for cloud-init to finish.
func waitForCloudInitComplete(ctx context.Context, ssh *SSHClient) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			stdout, _, err := ssh.Run(ctx, "cloud-init status 2>/dev/null || echo 'waiting'")
			if err != nil {
				continue
			}
			if strings.Contains(stdout, "done") || strings.Contains(stdout, "disabled") {
				return nil
			}
			if strings.Contains(stdout, "error") {
				return fmt.Errorf("cloud-init failed: %s", stdout)
			}
		}
	}
}

// downloadFile downloads a URL to a local file with progress indication.
func downloadFile(url, filepath string) error {
	// Create temporary file
	tmpPath := filepath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Download
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Copy with progress
	counter := &progressWriter{Total: resp.ContentLength}
	_, err = io.Copy(out, io.TeeReader(resp.Body, counter))
	if err != nil {
		return err
	}
	fmt.Println() // New line after progress

	// Rename temp file to final path
	return os.Rename(tmpPath, filepath)
}

// progressWriter tracks download progress.
type progressWriter struct {
	Total      int64
	Downloaded int64
	lastPct    int
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n := len(p)
	pw.Downloaded += int64(n)

	if pw.Total > 0 {
		pct := int(100 * pw.Downloaded / pw.Total)
		if pct != pw.lastPct && pct%10 == 0 {
			fmt.Printf("  %d%% (%d MB / %d MB)\n", pct, pw.Downloaded/(1024*1024), pw.Total/(1024*1024))
			pw.lastPct = pct
		}
	}

	return n, nil
}
