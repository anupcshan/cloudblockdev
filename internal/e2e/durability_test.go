package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/anupcshan/cloudblockdev/internal/cloudblock"
)

const (
	// ColdStartDeviceSize is the device size for cold start recovery tests.
	ColdStartDeviceSize = 1 * 1024 * 1024 * 1024 // 1GB

	// ColdStartDataSizeMB is how much random data to write during the test.
	ColdStartDataSizeMB = 100
)

// blockTransport abstracts the protocol-specific setup for connecting a
// cloudblock device to a kernel block device.
type blockTransport interface {
	// Connect starts the transport server and kernel driver.
	// Returns the block device path (e.g., /dev/nbd0).
	Connect(device *cloudblock.Device, size int64) (devPath string, err error)

	// Disconnect tears down the kernel driver and transport server.
	Disconnect() error
}

// coldStartRecoveryTest runs a cold-start durability test using the given transport.
// It writes data through a kernel block device, syncs, tears down the device,
// recreates it from S3 only (no local cache), and verifies data integrity.
//
// This test runs INSIDE the VM (CLOUDBLOCKDEV_IN_VM=1).
func coldStartRecoveryTest(t *testing.T, transport blockTransport) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// Get config from environment
	s3Endpoint := os.Getenv("S3_ENDPOINT")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	bucket := os.Getenv("S3_BUCKET")
	s3Prefix := os.Getenv("S3_PREFIX")

	if s3Endpoint == "" || bucket == "" {
		t.Fatal("S3_ENDPOINT and S3_BUCKET must be set")
	}
	if s3Prefix == "" {
		s3Prefix = "cold-start-test/"
	}

	t.Logf("Running cold start recovery test with S3 endpoint: %s", s3Endpoint)

	// Create S3 blob store (shared across both phases)
	newStore := func() (cloudblock.BlobStore, error) {
		return cloudblock.NewS3BlobStore(ctx, cloudblock.S3BlobStoreConfig{
			Bucket:    bucket,
			Prefix:    s3Prefix,
			Endpoint:  s3Endpoint,
			Region:    "us-east-1",
			AccessKey: accessKey,
			SecretKey: secretKey,
		})
	}

	// ========== PHASE 1: Write data and sync ==========
	t.Log("=== PHASE 1: Write data and sync ===")

	store, err := newStore()
	if err != nil {
		t.Fatalf("Failed to create S3 blob store: %v", err)
	}

	device, err := cloudblock.NewDevice(cloudblock.DeviceConfig{
		Store: store,
		Size:  ColdStartDeviceSize,
	})
	if err != nil {
		t.Fatalf("Failed to create device: %v", err)
	}
	if err := device.Start(); err != nil {
		t.Fatalf("Failed to start device: %v", err)
	}

	devPath, err := transport.Connect(device, ColdStartDeviceSize)
	if err != nil {
		device.Close()
		t.Fatalf("Failed to connect transport: %v", err)
	}
	t.Logf("Block device: %s", devPath)

	// Verify device size
	sizeOut, err := runCommand("blockdev", "--getsize64", devPath)
	if err != nil {
		t.Fatalf("Failed to get device size: %v", err)
	}
	t.Logf("Device size: %s bytes", strings.TrimSpace(sizeOut))

	// Create ZFS pool
	t.Log("Creating ZFS pool...")
	if output, err := runCommand("zpool", "create", "testpool", devPath); err != nil {
		t.Fatalf("Failed to create zpool: %v\nOutput: %s", err, output)
	}

	// Set recordsize to 1M to produce more individual blobs
	if _, err := runCommand("zfs", "set", "recordsize=1M", "testpool"); err != nil {
		t.Fatalf("Failed to set recordsize: %v", err)
	}

	// Write random data
	t.Logf("Writing %dMB of random data...", ColdStartDataSizeMB)
	writeCmd := fmt.Sprintf("dd if=/dev/urandom of=/testpool/testfile bs=1M count=%d conv=fdatasync status=progress", ColdStartDataSizeMB)
	if _, err := runCommand("sh", "-c", writeCmd); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	// Record checksum
	origChecksum, err := runCommand("md5sum", "/testpool/testfile")
	if err != nil {
		t.Fatalf("Failed to calculate checksum: %v", err)
	}
	t.Logf("Original checksum: %s", strings.TrimSpace(origChecksum))

	// Sync everything — this is the critical durability barrier.
	// If the transport correctly delivers flush commands, all data will be in S3.
	t.Log("Syncing data...")
	if _, err := runCommand("sync"); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}
	if _, err := runCommand("zpool", "sync", "testpool"); err != nil {
		t.Fatalf("Failed to sync pool: %v", err)
	}

	// Export pool (writes ZFS metadata to device, then syncs again)
	t.Log("Exporting ZFS pool...")
	if _, err := runCommand("zpool", "export", "testpool"); err != nil {
		t.Fatalf("Failed to export pool: %v", err)
	}

	// Tear down transport and device
	t.Log("Disconnecting transport...")
	if err := transport.Disconnect(); err != nil {
		t.Logf("Warning: transport disconnect: %v", err)
	}

	t.Log("Closing device...")
	device.Close()

	// ========== PHASE 2: Cold start from S3 only ==========
	t.Log("=== PHASE 2: Cold start from S3 (no local cache) ===")

	store2, err := newStore()
	if err != nil {
		t.Fatalf("Failed to create S3 blob store (phase 2): %v", err)
	}

	device2, err := cloudblock.NewDevice(cloudblock.DeviceConfig{
		Store: store2,
		Size:  ColdStartDeviceSize,
	})
	if err != nil {
		t.Fatalf("Failed to create device (phase 2): %v", err)
	}
	if err := device2.Start(); err != nil {
		t.Fatalf("Failed to start device (phase 2): %v", err)
	}

	// Recover index from S3
	t.Log("Recovering from S3...")
	if err := device2.Recover(ctx); err != nil {
		t.Fatalf("Failed to recover from S3: %v", err)
	}
	t.Log("Recovery completed")

	devPath2, err := transport.Connect(device2, ColdStartDeviceSize)
	if err != nil {
		device2.Close()
		t.Fatalf("Failed to connect transport (phase 2): %v", err)
	}
	t.Logf("Block device (phase 2): %s", devPath2)

	// ========== PHASE 3: Verify data integrity ==========
	t.Log("=== PHASE 3: Verify data integrity ===")

	// Import pool
	t.Log("Importing ZFS pool...")
	if _, err := runCommand("zpool", "import", "testpool"); err != nil {
		t.Fatalf("Failed to import pool: %v", err)
	}

	// Verify checksum
	t.Log("Verifying data checksum...")
	newChecksum, err := runCommand("md5sum", "/testpool/testfile")
	if err != nil {
		t.Fatalf("Failed to calculate checksum after recovery: %v", err)
	}
	t.Logf("Checksum after recovery: %s", strings.TrimSpace(newChecksum))

	origMD5 := strings.Fields(origChecksum)[0]
	newMD5 := strings.Fields(newChecksum)[0]
	if origMD5 != newMD5 {
		t.Errorf("Checksum mismatch: original=%s, after_recovery=%s", origMD5, newMD5)
	}

	// Run scrub to verify ZFS block-level integrity
	t.Log("Running ZFS scrub...")
	if err := localScrubAndVerify(t, "testpool"); err != nil {
		t.Fatalf("Scrub after cold start failed: %v", err)
	}

	// Cleanup
	t.Log("Cleaning up...")
	runCommand("zpool", "destroy", "testpool")
	transport.Disconnect()
	device2.Close()

	t.Log("Cold start recovery test passed!")
}

// localScrubAndVerify runs a ZFS scrub and verifies no errors, calling
// commands directly (for use inside the VM, not via SSH).
func localScrubAndVerify(t *testing.T, poolName string) error {
	t.Helper()

	// Sync before scrub
	runCommand("sync")
	runCommand("zpool", "sync", poolName)

	// Start scrub
	if _, err := runCommand("zpool", "scrub", poolName); err != nil {
		return fmt.Errorf("failed to start scrub: %w", err)
	}

	// Wait for scrub to complete
	t.Log("Waiting for scrub to complete...")
	for i := 0; i < 120; i++ {
		status, err := runCommand("zpool", "status", poolName)
		if err != nil {
			return fmt.Errorf("failed to get pool status: %w", err)
		}
		if !strings.Contains(status, "scrub in progress") {
			t.Log("Scrub completed")
			t.Logf("Pool status:\n%s", status)

			if strings.Contains(status, "DEGRADED") || strings.Contains(status, "FAULTED") {
				return fmt.Errorf("pool is degraded or faulted")
			}

			// Check device lines for checksum errors
			for _, line := range strings.Split(status, "\n") {
				fields := strings.Fields(line)
				if len(fields) >= 5 && fields[1] == "ONLINE" {
					cksum := fields[4]
					if cksum != "0" {
						return fmt.Errorf("checksum errors detected on device %s: %s errors", fields[0], cksum)
					}
				}
			}

			if strings.Contains(status, "errors:") && !strings.Contains(status, "No known data errors") {
				return fmt.Errorf("data errors detected in pool")
			}

			return nil
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("scrub did not complete within timeout")
}

// runTestInVM handles the host-side orchestration for running a test inside a VM.
// It starts MinIO, boots a VM, copies the test binary, loads kernel modules,
// and re-executes the named test inside the VM.
func runTestInVM(t *testing.T, testName string, modules []string, s3Prefix string) {
	t.Helper()

	RequirePrerequisites(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to get executable path: %v", err)
	}
	t.Logf("Test binary: %s", testBinary)

	// Start MinIO
	pool, minio, s3Endpoint := startMinIO(t)
	defer pool.Purge(minio)

	// Create bucket
	s3Client, err := createS3Client(ctx, s3Endpoint)
	if err != nil {
		t.Fatalf("Failed to create S3 client: %v", err)
	}
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(TestBucket),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// VM S3 endpoint (via QEMU NAT gateway)
	hostIP := getHostIP(t)
	minioPort := minio.GetPort("9000/tcp")
	vmS3Endpoint := fmt.Sprintf("http://%s:%s", hostIP, minioPort)
	t.Logf("VM will access MinIO at: %s", vmS3Endpoint)

	// Boot VM
	vm, tempDir, err := setupVMWithoutNBD(t)
	if err != nil {
		t.Fatalf("Failed to set up VM: %v", err)
	}
	defer CleanupTestVM(tempDir)
	defer vm.Shutdown(ctx)

	if err := vm.Start(ctx); err != nil {
		t.Fatalf("Failed to start VM: %v", err)
	}

	// Copy test binary
	t.Log("Copying test binary to VM...")
	if err := copyFileToVM(ctx, vm, testBinary, "/tmp/e2e.test"); err != nil {
		t.Fatalf("Failed to copy binary: %v", err)
	}
	if _, err := vm.RunSudo(ctx, "chmod +x /tmp/e2e.test"); err != nil {
		t.Fatalf("Failed to chmod binary: %v", err)
	}

	// Load kernel modules
	for _, mod := range modules {
		t.Logf("Loading kernel module: %s", mod)
		if _, err := vm.RunSudo(ctx, "modprobe "+mod); err != nil {
			t.Logf("Warning: failed to load module %s: %v (may already be loaded)", mod, err)
		}
	}

	// Run test inside VM
	t.Logf("Running %s inside VM...", testName)
	cmd := fmt.Sprintf(
		"CLOUDBLOCKDEV_IN_VM=1 S3_ENDPOINT=%s S3_ACCESS_KEY=%s S3_SECRET_KEY=%s S3_BUCKET=%s S3_PREFIX=%s "+
			"/tmp/e2e.test -test.run ^%s$ -test.v -test.timeout 10m",
		vmS3Endpoint, MinIOAccessKey, MinIOSecretKey, TestBucket, s3Prefix, testName,
	)
	output, err := vm.RunSudo(ctx, cmd)
	if err != nil {
		t.Fatalf("Test failed in VM: %v\nOutput:\n%s", err, output)
	}
	t.Logf("VM test output:\n%s", output)
}
