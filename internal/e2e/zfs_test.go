package e2e

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ory/dockertest/v3"

	"github.com/anupcshan/cloudblockdev/internal/cloudblock"
	"github.com/anupcshan/cloudblockdev/internal/nbd"
)

const (
	// DeviceSize is the size of the NBD device (10GB, sparse)
	DeviceSize = 10 * 1024 * 1024 * 1024

	// ExportName is the NBD export name
	ExportName = "cloudblockdev"

	// MinIOAccessKey is the access key for MinIO
	MinIOAccessKey = "minioadmin"

	// MinIOSecretKey is the secret key for MinIO
	MinIOSecretKey = "minioadmin"

	// TestBucket is the S3 bucket name for tests
	TestBucket = "cloudblockdev-test"
)

// testEnv holds the test environment resources.
type testEnv struct {
	pool       *dockertest.Pool
	minio      *dockertest.Resource
	s3Client   *s3.Client
	s3Endpoint string
	device     *cloudblock.Device
	nbdServer  *nbd.Server
	nbdAddr    string
	listener   net.Listener
}

// setupTestEnv sets up the test environment with MinIO and cloudblockdev.
func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	ctx := context.Background()
	env := &testEnv{}

	// Create dockertest pool
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not construct docker pool: %v", err)
	}
	env.pool = pool

	// Set reasonable timeout for docker operations
	pool.MaxWait = 2 * time.Minute

	// Start MinIO container
	t.Log("Starting MinIO container...")
	minio, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		Env: []string{
			"MINIO_ROOT_USER=" + MinIOAccessKey,
			"MINIO_ROOT_PASSWORD=" + MinIOSecretKey,
		},
	})
	if err != nil {
		t.Fatalf("Could not start MinIO container: %v", err)
	}
	env.minio = minio

	// Get MinIO endpoint
	env.s3Endpoint = fmt.Sprintf("http://localhost:%s", minio.GetPort("9000/tcp"))
	t.Logf("MinIO endpoint: %s", env.s3Endpoint)

	// Wait for MinIO to be ready
	if err := pool.Retry(func() error {
		client, err := createS3Client(ctx, env.s3Endpoint)
		if err != nil {
			return err
		}
		_, err = client.ListBuckets(ctx, &s3.ListBucketsInput{})
		return err
	}); err != nil {
		env.cleanup(t)
		t.Fatalf("Could not connect to MinIO: %v", err)
	}
	t.Log("MinIO is ready")

	// Create S3 client
	env.s3Client, err = createS3Client(ctx, env.s3Endpoint)
	if err != nil {
		env.cleanup(t)
		t.Fatalf("Could not create S3 client: %v", err)
	}

	// Create test bucket
	_, err = env.s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(TestBucket),
	})
	if err != nil {
		env.cleanup(t)
		t.Fatalf("Could not create bucket: %v", err)
	}
	t.Logf("Created bucket: %s", TestBucket)

	// Create S3 blob store
	store, err := createS3BlobStore(ctx, env.s3Endpoint)
	if err != nil {
		env.cleanup(t)
		t.Fatalf("Could not create S3 blob store: %v", err)
	}

	// Create cloud block device
	env.device, err = cloudblock.NewDevice(cloudblock.DeviceConfig{
		Store: store,
		Size:  DeviceSize,
	})
	if err != nil {
		env.cleanup(t)
		t.Fatalf("Could not create device: %v", err)
	}

	// Start the device (background goroutines)
	if err := env.device.Start(); err != nil {
		env.cleanup(t)
		t.Fatalf("Could not start device: %v", err)
	}

	// Start NBD server
	env.nbdServer = nbd.NewServer(nbd.Config{
		Device:     env.device,
		ExportName: ExportName,
	})

	// Find a free port and start listening
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		env.cleanup(t)
		t.Fatalf("Could not start listener: %v", err)
	}
	env.listener = listener
	env.nbdAddr = listener.Addr().String()
	t.Logf("NBD server listening on: %s", env.nbdAddr)

	// Start NBD server in background
	go func() {
		if err := env.nbdServer.Serve(listener); err != nil {
			// Ignore errors during shutdown
			t.Logf("NBD server stopped: %v", err)
		}
	}()

	return env
}

// cleanup releases all test resources.
func (env *testEnv) cleanup(t *testing.T) {
	t.Helper()

	if env.nbdServer != nil {
		env.nbdServer.Shutdown()
	}
	if env.listener != nil {
		env.listener.Close()
	}
	if env.device != nil {
		env.device.Close()
	}
	if env.minio != nil {
		if err := env.pool.Purge(env.minio); err != nil {
			t.Logf("Warning: could not purge MinIO container: %v", err)
		}
	}
}

// createS3Client creates a raw S3 client for MinIO (used for test setup like bucket creation).
func createS3Client(ctx context.Context, endpoint string) (*s3.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(MinIOAccessKey, MinIOSecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return client, nil
}

// createS3BlobStore creates an S3 blob store for MinIO.
func createS3BlobStore(ctx context.Context, endpoint string) (cloudblock.BlobStore, error) {
	return cloudblock.NewS3BlobStore(ctx, cloudblock.S3BlobStoreConfig{
		Bucket:    TestBucket,
		Prefix:    "test/",
		Endpoint:  endpoint,
		Region:    "us-east-1",
		AccessKey: MinIOAccessKey,
		SecretKey: MinIOSecretKey,
	})
}

// runScrubAndVerify runs a ZFS scrub and verifies no errors occurred.
// Returns an error if the scrub finds any checksum errors.
func runScrubAndVerify(ctx context.Context, t *testing.T, vm *VM, poolName string) error {
	t.Helper()

	// Ensure all data is flushed to backend before scrubbing
	t.Log("Syncing data before scrub...")
	if _, err := vm.RunSudo(ctx, "sync"); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}
	if _, err := vm.RunSudo(ctx, "zpool sync "+poolName); err != nil {
		return fmt.Errorf("failed to sync pool: %w", err)
	}

	t.Log("Starting ZFS scrub...")
	if _, err := vm.RunSudo(ctx, "zpool scrub "+poolName); err != nil {
		return fmt.Errorf("failed to start scrub: %w", err)
	}

	// Wait for scrub to complete
	t.Log("Waiting for scrub to complete...")
	for i := 0; i < 120; i++ { // Max 4 minutes
		status, err := vm.RunSudo(ctx, "zpool status "+poolName)
		if err != nil {
			return fmt.Errorf("failed to get pool status: %w", err)
		}
		if !strings.Contains(status, "scrub in progress") {
			t.Log("Scrub completed")

			// Check for errors
			t.Logf("Pool status:\n%s", status)

			// Check CKSUM column for errors (look for non-zero values)
			if strings.Contains(status, "DEGRADED") || strings.Contains(status, "FAULTED") {
				return fmt.Errorf("pool is degraded or faulted")
			}

			// Parse the status to check for checksum errors
			// Look for lines with device status that have non-zero CKSUM
			lines := strings.Split(status, "\n")
			for _, line := range lines {
				// Device lines look like: "  vdb       ONLINE       0     0     8"
				fields := strings.Fields(line)
				if len(fields) >= 5 && fields[1] == "ONLINE" {
					cksum := fields[4]
					if cksum != "0" {
						return fmt.Errorf("checksum errors detected on device %s: %s errors", fields[0], cksum)
					}
				}
			}

			// Also check "errors:" line
			if strings.Contains(status, "errors:") && !strings.Contains(status, "No known data errors") {
				return fmt.Errorf("data errors detected in pool")
			}

			return nil
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("scrub did not complete within timeout")
}

// TestZFSBasicOperations tests basic ZFS pool operations.
func TestZFSBasicOperations(t *testing.T) {
	RequirePrerequisites(t)

	env := setupTestEnv(t)
	defer env.cleanup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Set up VM
	vm, tempDir, err := SetupTestVM(t, env.nbdAddr, ExportName)
	if err != nil {
		t.Fatalf("Failed to set up VM: %v", err)
	}
	defer CleanupTestVM(tempDir)

	// Start VM
	if err := vm.Start(ctx); err != nil {
		t.Fatalf("Failed to start VM: %v", err)
	}
	defer vm.Shutdown(ctx)

	// Test basic ZFS operations
	t.Log("Creating ZFS pool...")
	if _, err := vm.RunSudo(ctx, "zpool create testpool /dev/vdb"); err != nil {
		t.Fatalf("Failed to create zpool: %v", err)
	}

	t.Log("Writing test file...")
	if _, err := vm.RunSudo(ctx, "bash -c 'echo \"hello cloudblockdev\" > /testpool/test.txt'"); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	t.Log("Syncing...")
	if _, err := vm.RunSudo(ctx, "sync"); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	t.Log("Reading test file...")
	output, err := vm.RunSudo(ctx, "cat /testpool/test.txt")
	if err != nil {
		t.Fatalf("Failed to read test file: %v", err)
	}

	if !strings.Contains(output, "hello cloudblockdev") {
		t.Errorf("Expected 'hello cloudblockdev' in output, got: %s", output)
	}

	// Run scrub and verify no errors
	if err := runScrubAndVerify(ctx, t, vm, "testpool"); err != nil {
		t.Fatalf("Scrub verification failed: %v", err)
	}

	t.Log("Destroying ZFS pool...")
	if _, err := vm.RunSudo(ctx, "zpool destroy testpool"); err != nil {
		t.Fatalf("Failed to destroy zpool: %v", err)
	}

	t.Log("Basic ZFS operations test passed!")
}

// TestZFSSnapshotRollback tests ZFS snapshot and rollback.
func TestZFSSnapshotRollback(t *testing.T) {
	RequirePrerequisites(t)

	env := setupTestEnv(t)
	defer env.cleanup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Set up VM
	vm, tempDir, err := SetupTestVM(t, env.nbdAddr, ExportName)
	if err != nil {
		t.Fatalf("Failed to set up VM: %v", err)
	}
	defer CleanupTestVM(tempDir)

	// Start VM
	if err := vm.Start(ctx); err != nil {
		t.Fatalf("Failed to start VM: %v", err)
	}
	defer vm.Shutdown(ctx)

	// Create pool and initial file
	t.Log("Creating ZFS pool...")
	if _, err := vm.RunSudo(ctx, "zpool create testpool /dev/vdb"); err != nil {
		t.Fatalf("Failed to create zpool: %v", err)
	}

	t.Log("Writing original content...")
	if _, err := vm.RunSudo(ctx, "bash -c 'echo \"original content\" > /testpool/data.txt'"); err != nil {
		t.Fatalf("Failed to write original content: %v", err)
	}
	if _, err := vm.RunSudo(ctx, "sync"); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Create snapshot
	t.Log("Creating snapshot...")
	if _, err := vm.RunSudo(ctx, "zfs snapshot testpool@snap1"); err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Modify file
	t.Log("Modifying content...")
	if _, err := vm.RunSudo(ctx, "bash -c 'echo \"modified content\" > /testpool/data.txt'"); err != nil {
		t.Fatalf("Failed to modify content: %v", err)
	}
	if _, err := vm.RunSudo(ctx, "sync"); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Verify modified content
	output, err := vm.RunSudo(ctx, "cat /testpool/data.txt")
	if err != nil {
		t.Fatalf("Failed to read modified content: %v", err)
	}
	if !strings.Contains(output, "modified content") {
		t.Errorf("Expected 'modified content', got: %s", output)
	}

	// Rollback to snapshot
	t.Log("Rolling back to snapshot...")
	if _, err := vm.RunSudo(ctx, "zfs rollback testpool@snap1"); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify original content is restored
	t.Log("Verifying rollback...")
	output, err = vm.RunSudo(ctx, "cat /testpool/data.txt")
	if err != nil {
		t.Fatalf("Failed to read after rollback: %v", err)
	}
	if !strings.Contains(output, "original content") {
		t.Errorf("Expected 'original content' after rollback, got: %s", output)
	}

	// Run scrub and verify no errors
	if err := runScrubAndVerify(ctx, t, vm, "testpool"); err != nil {
		t.Fatalf("Scrub verification failed: %v", err)
	}

	// Cleanup
	t.Log("Destroying ZFS pool...")
	if _, err := vm.RunSudo(ctx, "zpool destroy testpool"); err != nil {
		t.Fatalf("Failed to destroy zpool: %v", err)
	}

	t.Log("Snapshot/rollback test passed!")
}

// TestRawBlockIntegrity tests raw block device read/write without ZFS.
// This helps isolate whether issues are with the block device or ZFS.
func TestRawBlockIntegrity(t *testing.T) {
	RequirePrerequisites(t)

	env := setupTestEnv(t)
	defer env.cleanup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Set up VM
	vm, tempDir, err := SetupTestVM(t, env.nbdAddr, ExportName)
	if err != nil {
		t.Fatalf("Failed to set up VM: %v", err)
	}
	defer CleanupTestVM(tempDir)

	// Start VM
	if err := vm.Start(ctx); err != nil {
		t.Fatalf("Failed to start VM: %v", err)
	}
	defer vm.Shutdown(ctx)

	// Write a known pattern directly to the block device
	t.Log("Writing test pattern to raw block device...")
	// Write "TESTPATTERN" repeated, at offset 1MB to avoid any partition table area
	if _, err := vm.RunSudo(ctx, "bash -c 'for i in $(seq 1 1000); do echo -n TESTPATTERN; done > /tmp/pattern.bin'"); err != nil {
		t.Fatalf("Failed to create pattern file: %v", err)
	}

	// Check pattern file size
	patternSize, err := vm.RunSudo(ctx, "stat -c %s /tmp/pattern.bin")
	if err != nil {
		t.Fatalf("Failed to get pattern size: %v", err)
	}
	t.Logf("Pattern file size: %s bytes", strings.TrimSpace(patternSize))

	// Write pattern to block device at offset 1MB
	if _, err := vm.RunSudo(ctx, "dd if=/tmp/pattern.bin of=/dev/vdb bs=512 seek=2048 conv=fdatasync"); err != nil {
		t.Fatalf("Failed to write to block device: %v", err)
	}

	// Sync to ensure all data is flushed
	if _, err := vm.RunSudo(ctx, "sync"); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Calculate checksum of what we wrote
	origSum, err := vm.RunSudo(ctx, "md5sum /tmp/pattern.bin")
	if err != nil {
		t.Fatalf("Failed to calculate original checksum: %v", err)
	}
	t.Logf("Original checksum: %s", origSum)

	// Read back and compare
	t.Log("Reading back and comparing...")
	// Use 4K block size for direct I/O alignment
	// Pattern is at offset 1MB (2048*512 = 1048576 = 256*4096)
	patternSizeInt := 11000                       // "TESTPATTERN" * 1000 = 11000 bytes
	countBlocks := (patternSizeInt + 4095) / 4096 // Round up to 4K blocks
	t.Logf("Reading %d blocks (4096 bytes each)", countBlocks)
	if _, err := vm.RunSudo(ctx, fmt.Sprintf("dd if=/dev/vdb of=/tmp/readback.bin bs=4096 skip=256 count=%d", countBlocks)); err != nil {
		t.Fatalf("Failed to read from block device: %v", err)
	}

	// Truncate readback to exact pattern size for comparison
	if _, err := vm.RunSudo(ctx, fmt.Sprintf("truncate -s %d /tmp/readback.bin", patternSizeInt)); err != nil {
		t.Fatalf("Failed to truncate readback: %v", err)
	}

	readSum, err := vm.RunSudo(ctx, "md5sum /tmp/readback.bin")
	if err != nil {
		t.Fatalf("Failed to calculate readback checksum: %v", err)
	}
	t.Logf("Readback checksum: %s", readSum)

	origMD5 := strings.Fields(origSum)[0]
	readMD5 := strings.Fields(readSum)[0]
	if origMD5 != readMD5 {
		t.Errorf("Checksum mismatch: original=%s, readback=%s", origMD5, readMD5)
	}

	// Now do a stress test: write random data multiple times and verify each time
	t.Log("Stress testing with random data...")
	for i := 0; i < 10; i++ {
		// Generate random data (100KB = 25 * 4K blocks)
		offset := 512 + i*128 // Different 4K-block offset for each iteration (in 4K units)
		if _, err := vm.RunSudo(ctx, fmt.Sprintf("dd if=/dev/urandom of=/tmp/rand%d.bin bs=4K count=25", i)); err != nil {
			t.Fatalf("Iteration %d: failed to generate random data: %v", i, err)
		}

		// Write to block device (using 4K blocks for alignment)
		if _, err := vm.RunSudo(ctx, fmt.Sprintf("dd if=/tmp/rand%d.bin of=/dev/vdb bs=4K seek=%d conv=fdatasync", i, offset)); err != nil {
			t.Fatalf("Iteration %d: failed to write: %v", i, err)
		}

		// Read back immediately (using 4K blocks)
		if _, err := vm.RunSudo(ctx, fmt.Sprintf("dd if=/dev/vdb of=/tmp/verify%d.bin bs=4K skip=%d count=25", i, offset)); err != nil {
			t.Fatalf("Iteration %d: failed to read back: %v", i, err)
		}

		// Compare
		origSum, _ := vm.RunSudo(ctx, fmt.Sprintf("md5sum /tmp/rand%d.bin", i))
		verifySum, _ := vm.RunSudo(ctx, fmt.Sprintf("md5sum /tmp/verify%d.bin", i))
		if strings.Fields(origSum)[0] != strings.Fields(verifySum)[0] {
			t.Errorf("Iteration %d: checksum mismatch at offset %d", i, offset)
		}
	}

	t.Log("Raw block integrity test passed!")
}

// TestZFSDataIntegrity tests ZFS data integrity with I/O stress.
func TestZFSDataIntegrity(t *testing.T) {
	RequirePrerequisites(t)

	env := setupTestEnv(t)
	defer env.cleanup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Set up VM
	vm, tempDir, err := SetupTestVM(t, env.nbdAddr, ExportName)
	if err != nil {
		t.Fatalf("Failed to set up VM: %v", err)
	}
	defer CleanupTestVM(tempDir)

	// Start VM
	if err := vm.Start(ctx); err != nil {
		t.Fatalf("Failed to start VM: %v", err)
	}
	defer vm.Shutdown(ctx)

	// Create pool
	t.Log("Creating ZFS pool...")
	if _, err := vm.RunSudo(ctx, "zpool create testpool /dev/vdb"); err != nil {
		t.Fatalf("Failed to create zpool: %v", err)
	}

	// I/O stress with dd - write random data
	t.Log("Running I/O stress test with dd (50MB random writes)...")
	if _, err := vm.RunSudo(ctx, "dd if=/dev/urandom of=/testpool/stress1 bs=1M count=50 conv=fdatasync status=progress"); err != nil {
		t.Fatalf("Failed dd stress test 1: %v", err)
	}

	t.Log("Running I/O stress test with dd (small block writes)...")
	if _, err := vm.RunSudo(ctx, "dd if=/dev/urandom of=/testpool/stress2 bs=4k count=5000 conv=fdatasync status=progress"); err != nil {
		t.Fatalf("Failed dd stress test 2: %v", err)
	}

	// Calculate checksums before scrub
	t.Log("Calculating checksums...")
	checksum1, err := vm.RunSudo(ctx, "md5sum /testpool/stress1")
	if err != nil {
		t.Fatalf("Failed to calculate checksum: %v", err)
	}
	checksum2, err := vm.RunSudo(ctx, "md5sum /testpool/stress2")
	if err != nil {
		t.Fatalf("Failed to calculate checksum: %v", err)
	}

	// Run scrub and verify no errors
	if err := runScrubAndVerify(ctx, t, vm, "testpool"); err != nil {
		t.Fatalf("Scrub verification failed: %v", err)
	}

	// Verify checksums after scrub
	t.Log("Verifying checksums after scrub...")
	newChecksum1, err := vm.RunSudo(ctx, "md5sum /testpool/stress1")
	if err != nil {
		t.Fatalf("Failed to calculate checksum after scrub: %v", err)
	}
	newChecksum2, err := vm.RunSudo(ctx, "md5sum /testpool/stress2")
	if err != nil {
		t.Fatalf("Failed to calculate checksum after scrub: %v", err)
	}

	// Compare checksums
	if strings.Fields(checksum1)[0] != strings.Fields(newChecksum1)[0] {
		t.Errorf("Checksum mismatch for stress1: before=%s after=%s", checksum1, newChecksum1)
	}
	if strings.Fields(checksum2)[0] != strings.Fields(newChecksum2)[0] {
		t.Errorf("Checksum mismatch for stress2: before=%s after=%s", checksum2, newChecksum2)
	}

	// Cleanup
	t.Log("Destroying ZFS pool...")
	if _, err := vm.RunSudo(ctx, "zpool destroy testpool"); err != nil {
		t.Fatalf("Failed to destroy zpool: %v", err)
	}

	t.Log("Data integrity test passed!")
}
