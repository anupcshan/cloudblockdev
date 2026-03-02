package e2e

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ory/dockertest/v3"

	"github.com/anupcshan/cloudblockdev/internal/cloudblock"
	"github.com/anupcshan/cloudblockdev/internal/nbd"
	"github.com/anupcshan/cloudblockdev/internal/nbdclient"
)

const (
	// KernelNBDDeviceSize is smaller for faster kernel NBD tests
	KernelNBDDeviceSize = 1 * 1024 * 1024 * 1024 // 1GB
)

// TestKernelNBDBasic tests the kernel NBD device management.
// This test runs cloudblockdev INSIDE the VM to use the kernel's NBD module.
func TestKernelNBDBasic(t *testing.T) {
	// Check if running inside the VM
	if os.Getenv("CLOUDBLOCKDEV_IN_VM") == "1" {
		runInVMKernelNBDBasic(t)
		return
	}

	// --- Host orchestration ---
	RequirePrerequisites(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Get path to ourselves (the test binary)
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to get executable path: %v", err)
	}
	t.Logf("Test binary: %s", testBinary)

	// Start MinIO container
	pool, minio, s3Endpoint := startMinIO(t)
	defer pool.Purge(minio)

	// Create test bucket
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

	// Get host IP for MinIO access from VM
	hostIP := getHostIP(t)
	minioPort := minio.GetPort("9000/tcp")
	vmS3Endpoint := fmt.Sprintf("http://%s:%s", hostIP, minioPort)
	t.Logf("VM will access MinIO at: %s", vmS3Endpoint)

	// Set up VM (without NBD connection - we'll use kernel NBD inside)
	vm, tempDir, err := setupVMWithoutNBD(t)
	if err != nil {
		t.Fatalf("Failed to set up VM: %v", err)
	}
	defer CleanupTestVM(tempDir)
	defer vm.Shutdown(ctx)

	if err := vm.Start(ctx); err != nil {
		t.Fatalf("Failed to start VM: %v", err)
	}

	// Copy test binary to VM
	t.Log("Copying test binary to VM...")
	if err := copyFileToVM(ctx, vm, testBinary, "/tmp/e2e.test"); err != nil {
		t.Fatalf("Failed to copy binary: %v", err)
	}
	if _, err := vm.RunSudo(ctx, "chmod +x /tmp/e2e.test"); err != nil {
		t.Fatalf("Failed to chmod binary: %v", err)
	}

	// Load NBD kernel module
	t.Log("Loading NBD kernel module...")
	if _, err := vm.RunSudo(ctx, "modprobe nbd"); err != nil {
		t.Fatalf("Failed to load nbd module: %v", err)
	}

	// Run the test inside the VM
	t.Log("Running kernel NBD test inside VM...")
	cmd := fmt.Sprintf(
		"CLOUDBLOCKDEV_IN_VM=1 S3_ENDPOINT=%s S3_ACCESS_KEY=%s S3_SECRET_KEY=%s S3_BUCKET=%s "+
			"/tmp/e2e.test -test.run ^TestKernelNBDBasic$ -test.v -test.timeout 10m",
		vmS3Endpoint, MinIOAccessKey, MinIOSecretKey, TestBucket,
	)
	output, err := vm.RunSudo(ctx, cmd)
	if err != nil {
		t.Fatalf("Test failed in VM: %v\nOutput:\n%s", err, output)
	}
	t.Logf("VM test output:\n%s", output)

	t.Log("Kernel NBD basic test passed!")
}

// runInVMKernelNBDBasic runs inside the VM with kernel NBD.
func runInVMKernelNBDBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Get config from environment
	s3Endpoint := os.Getenv("S3_ENDPOINT")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	bucket := os.Getenv("S3_BUCKET")

	if s3Endpoint == "" || bucket == "" {
		t.Fatal("S3_ENDPOINT and S3_BUCKET must be set")
	}

	t.Logf("Running in VM with S3 endpoint: %s", s3Endpoint)

	// Create S3 blob store
	store, err := cloudblock.NewS3BlobStore(ctx, cloudblock.S3BlobStoreConfig{
		Bucket:    bucket,
		Prefix:    "kernel-nbd-test/",
		Endpoint:  s3Endpoint,
		Region:    "us-east-1",
		AccessKey: accessKey,
		SecretKey: secretKey,
	})
	if err != nil {
		t.Fatalf("Failed to create S3 blob store: %v", err)
	}

	// Create cloudblock device
	device, err := cloudblock.NewDevice(cloudblock.DeviceConfig{
		Store: store,
		Size:  KernelNBDDeviceSize,
	})
	if err != nil {
		t.Fatalf("Failed to create device: %v", err)
	}
	if err := device.Start(); err != nil {
		t.Fatalf("Failed to start device: %v", err)
	}
	defer device.Close()

	// Set up kernel NBD device
	dm := nbdclient.NewDeviceManager(nbdclient.DeviceConfig{
		Index:           0,
		Size:            KernelNBDDeviceSize,
		DeadConnTimeout: 120 * time.Second,
	})
	if err := dm.Setup(); err != nil {
		t.Fatalf("Failed to setup NBD device: %v", err)
	}
	defer dm.Disconnect()

	t.Logf("NBD device ready at %s", dm.DevicePath())

	// Create NBD server and start handling in background
	nbdServer := nbd.NewServer(nbd.Config{
		Device:     device,
		ExportName: "test",
	})

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- nbdServer.HandleDirectConnection(dm.ServerConn())
	}()

	// Verify device size
	sizeOut, err := runCommand("blockdev", "--getsize64", dm.DevicePath())
	if err != nil {
		t.Fatalf("Failed to get device size: %v", err)
	}
	t.Logf("Device size: %s bytes", strings.TrimSpace(sizeOut))

	// Create ZFS pool
	t.Log("Creating ZFS pool...")
	if _, err := runCommand("zpool", "create", "testpool", dm.DevicePath()); err != nil {
		t.Fatalf("Failed to create zpool: %v", err)
	}

	// Write test data
	t.Log("Writing test data...")
	testData := "kernel nbd test data - " + time.Now().String()
	if err := os.WriteFile("/testpool/test.txt", []byte(testData), 0644); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	// Sync
	if _, err := runCommand("sync"); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}
	if _, err := runCommand("zpool", "sync", "testpool"); err != nil {
		t.Fatalf("Failed to sync pool: %v", err)
	}

	// Read back and verify
	readData, err := os.ReadFile("/testpool/test.txt")
	if err != nil {
		t.Fatalf("Failed to read test data: %v", err)
	}
	if string(readData) != testData {
		t.Errorf("Data mismatch: got %q, want %q", string(readData), testData)
	}
	t.Log("Data verified successfully")

	// Export pool for restart test
	t.Log("Exporting ZFS pool...")
	if _, err := runCommand("zpool", "export", "testpool"); err != nil {
		t.Fatalf("Failed to export pool: %v", err)
	}

	// --- Simulate restart ---
	t.Log("Simulating restart (closing server connection)...")

	// Shutdown NBD server (this closes the connection, simulating process exit)
	nbdServer.Shutdown()

	// Wait for server to finish
	select {
	case err := <-serverDone:
		t.Logf("Server stopped: %v", err)
	case <-time.After(5 * time.Second):
		t.Log("Server did not stop in time, continuing...")
	}

	// Close device manager (keeps kernel device waiting for reconnect)
	dm.Close()

	// Wait a moment
	time.Sleep(2 * time.Second)

	// --- Reconnect ---
	t.Log("Reconnecting (testing NBD_CMD_RECONFIGURE)...")

	// Create new device manager (will use RECONFIGURE since device exists)
	dm2 := nbdclient.NewDeviceManager(nbdclient.DeviceConfig{
		Index:           0,
		Size:            KernelNBDDeviceSize,
		DeadConnTimeout: 120 * time.Second,
	})
	if err := dm2.Setup(); err != nil {
		t.Fatalf("Failed to reconnect NBD device: %v", err)
	}
	defer dm2.Disconnect()

	// Start new server
	nbdServer2 := nbd.NewServer(nbd.Config{
		Device:     device,
		ExportName: "test",
	})
	go nbdServer2.HandleDirectConnection(dm2.ServerConn())
	defer nbdServer2.Shutdown()

	// Verify device is still available
	sizeOut2, err := runCommand("blockdev", "--getsize64", dm2.DevicePath())
	if err != nil {
		t.Fatalf("Device not available after restart: %v", err)
	}
	t.Logf("Device size after restart: %s bytes", strings.TrimSpace(sizeOut2))

	// Re-import pool
	t.Log("Re-importing ZFS pool...")
	if _, err := runCommand("zpool", "import", "testpool"); err != nil {
		t.Fatalf("Failed to import pool: %v", err)
	}

	// Verify data after restart
	readData2, err := os.ReadFile("/testpool/test.txt")
	if err != nil {
		t.Fatalf("Failed to read after restart: %v", err)
	}
	if string(readData2) != testData {
		t.Errorf("Data mismatch after restart: got %q, want %q", string(readData2), testData)
	}
	t.Log("Data verified after restart")

	// Cleanup
	t.Log("Cleaning up...")
	runCommand("zpool", "destroy", "testpool")

	t.Log("Kernel NBD basic test completed successfully!")
}

// TestKernelNBDForceReconnect tests the ForceReconnect option for size changes.
func TestKernelNBDForceReconnect(t *testing.T) {
	// Check if running inside the VM
	if os.Getenv("CLOUDBLOCKDEV_IN_VM") == "1" {
		runInVMKernelNBDForceReconnect(t)
		return
	}

	// --- Host orchestration ---
	RequirePrerequisites(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to get executable path: %v", err)
	}

	// Start MinIO
	pool, minio, s3Endpoint := startMinIO(t)
	defer pool.Purge(minio)

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

	hostIP := getHostIP(t)
	minioPort := minio.GetPort("9000/tcp")
	vmS3Endpoint := fmt.Sprintf("http://%s:%s", hostIP, minioPort)

	// Set up VM
	vm, tempDir, err := setupVMWithoutNBD(t)
	if err != nil {
		t.Fatalf("Failed to set up VM: %v", err)
	}
	defer CleanupTestVM(tempDir)
	defer vm.Shutdown(ctx)

	if err := vm.Start(ctx); err != nil {
		t.Fatalf("Failed to start VM: %v", err)
	}

	// Copy binary and load NBD module
	if err := copyFileToVM(ctx, vm, testBinary, "/tmp/e2e.test"); err != nil {
		t.Fatalf("Failed to copy binary: %v", err)
	}
	vm.RunSudo(ctx, "chmod +x /tmp/e2e.test")
	vm.RunSudo(ctx, "modprobe nbd")

	// Run the test inside the VM
	t.Log("Running force-reconnect test inside VM...")
	cmd := fmt.Sprintf(
		"CLOUDBLOCKDEV_IN_VM=1 S3_ENDPOINT=%s S3_ACCESS_KEY=%s S3_SECRET_KEY=%s S3_BUCKET=%s "+
			"/tmp/e2e.test -test.run ^TestKernelNBDForceReconnect$ -test.v -test.timeout 10m",
		vmS3Endpoint, MinIOAccessKey, MinIOSecretKey, TestBucket,
	)
	output, err := vm.RunSudo(ctx, cmd)
	if err != nil {
		t.Fatalf("Test failed in VM: %v\nOutput:\n%s", err, output)
	}
	t.Logf("VM test output:\n%s", output)

	t.Log("Force reconnect test passed!")
}

// runInVMKernelNBDForceReconnect runs inside the VM testing size changes.
func runInVMKernelNBDForceReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	s3Endpoint := os.Getenv("S3_ENDPOINT")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	bucket := os.Getenv("S3_BUCKET")

	if s3Endpoint == "" || bucket == "" {
		t.Fatal("S3_ENDPOINT and S3_BUCKET must be set")
	}

	size1GB := int64(1 * 1024 * 1024 * 1024)
	size2GB := int64(2 * 1024 * 1024 * 1024)

	// Create S3 store
	store, err := cloudblock.NewS3BlobStore(ctx, cloudblock.S3BlobStoreConfig{
		Bucket:    bucket,
		Prefix:    "force-reconnect-test/",
		Endpoint:  s3Endpoint,
		Region:    "us-east-1",
		AccessKey: accessKey,
		SecretKey: secretKey,
	})
	if err != nil {
		t.Fatalf("Failed to create S3 blob store: %v", err)
	}

	// Create 1GB device
	device, err := cloudblock.NewDevice(cloudblock.DeviceConfig{
		Store: store,
		Size:  size1GB,
	})
	if err != nil {
		t.Fatalf("Failed to create device: %v", err)
	}
	if err := device.Start(); err != nil {
		t.Fatalf("Failed to start device: %v", err)
	}
	defer device.Close()

	// Set up kernel NBD with 1GB
	t.Log("Setting up 1GB NBD device...")
	dm := nbdclient.NewDeviceManager(nbdclient.DeviceConfig{
		Index:           0,
		Size:            uint64(size1GB),
		DeadConnTimeout: 120 * time.Second,
	})
	if err := dm.Setup(); err != nil {
		t.Fatalf("Failed to setup NBD device: %v", err)
	}

	// Start server
	nbdServer := nbd.NewServer(nbd.Config{
		Device:     device,
		ExportName: "test",
	})
	go nbdServer.HandleDirectConnection(dm.ServerConn())

	// Verify 1GB size
	sizeOut, err := runCommand("blockdev", "--getsize64", dm.DevicePath())
	if err != nil {
		t.Fatalf("Failed to get device size: %v", err)
	}
	t.Logf("Initial size: %s", strings.TrimSpace(sizeOut))
	if !strings.Contains(sizeOut, "1073741824") {
		t.Errorf("Expected 1GB (1073741824 bytes), got: %s", sizeOut)
	}

	// Stop server and close device manager
	nbdServer.Shutdown()
	dm.Close()
	time.Sleep(1 * time.Second)

	// Try to reconnect with 2GB WITHOUT force (should fail)
	t.Log("Trying to reconnect with 2GB (without force, should fail)...")
	dm2 := nbdclient.NewDeviceManager(nbdclient.DeviceConfig{
		Index:           0,
		Size:            uint64(size2GB),
		DeadConnTimeout: 120 * time.Second,
		ForceReconnect:  false,
	})
	err = dm2.Setup()
	if err == nil {
		dm2.Disconnect()
		t.Fatal("Expected size mismatch error, but setup succeeded")
	}
	if !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("Expected size mismatch error, got: %v", err)
	}
	t.Logf("Got expected error: %v", err)

	// Now reconnect with 2GB WITH force
	t.Log("Reconnecting with 2GB (with ForceReconnect)...")
	dm3 := nbdclient.NewDeviceManager(nbdclient.DeviceConfig{
		Index:           0,
		Size:            uint64(size2GB),
		DeadConnTimeout: 120 * time.Second,
		ForceReconnect:  true,
	})
	if err := dm3.Setup(); err != nil {
		t.Fatalf("Failed to force-reconnect with 2GB: %v", err)
	}
	defer dm3.Disconnect()

	// Start new server (with 2GB device)
	device2, err := cloudblock.NewDevice(cloudblock.DeviceConfig{
		Store: store,
		Size:  size2GB,
	})
	if err != nil {
		t.Fatalf("Failed to create 2GB device: %v", err)
	}
	if err := device2.Start(); err != nil {
		t.Fatalf("Failed to start 2GB device: %v", err)
	}
	defer device2.Close()

	nbdServer2 := nbd.NewServer(nbd.Config{
		Device:     device2,
		ExportName: "test",
	})
	go nbdServer2.HandleDirectConnection(dm3.ServerConn())
	defer nbdServer2.Shutdown()

	// Verify 2GB size
	sizeOut2, err := runCommand("blockdev", "--getsize64", dm3.DevicePath())
	if err != nil {
		t.Fatalf("Failed to get device size after force-reconnect: %v", err)
	}
	t.Logf("New size: %s", strings.TrimSpace(sizeOut2))
	if !strings.Contains(sizeOut2, "2147483648") {
		t.Errorf("Expected 2GB (2147483648 bytes), got: %s", sizeOut2)
	}

	t.Log("Force reconnect test completed successfully!")
}

// runCommand executes a command and returns its output.
func runCommand(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

// startMinIO starts a MinIO container and returns the pool, resource, and endpoint.
func startMinIO(t *testing.T) (*dockertest.Pool, *dockertest.Resource, string) {
	t.Helper()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not construct docker pool: %v", err)
	}
	pool.MaxWait = 2 * time.Minute

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
		t.Fatalf("Could not start MinIO: %v", err)
	}

	endpoint := fmt.Sprintf("http://localhost:%s", minio.GetPort("9000/tcp"))

	// Wait for MinIO to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pool.Retry(func() error {
		client, err := createS3Client(ctx, endpoint)
		if err != nil {
			return err
		}
		_, err = client.ListBuckets(ctx, &s3.ListBucketsInput{})
		return err
	}); err != nil {
		pool.Purge(minio)
		t.Fatalf("MinIO not ready: %v", err)
	}

	t.Logf("MinIO ready at %s", endpoint)
	return pool, minio, endpoint
}

// getHostIP returns the host IP address accessible from the VM.
func getHostIP(t *testing.T) string {
	t.Helper()
	// For QEMU user networking, the host is typically at 10.0.2.2
	return "10.0.2.2"
}

// setupVMWithoutNBD sets up a VM without an NBD drive (for kernel NBD testing).
func setupVMWithoutNBD(t *testing.T) (*VM, string, error) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "cloudblockdev-nbd-e2e-*")
	if err != nil {
		return nil, "", fmt.Errorf("create temp dir: %w", err)
	}

	goldenImage, sshKey, err := EnsureGoldenImage()
	if err != nil {
		return nil, tempDir, fmt.Errorf("ensure golden image: %w", err)
	}

	overlayPath := filepath.Join(tempDir, "root.qcow2")
	if err := CreateOverlayImage(goldenImage, overlayPath); err != nil {
		return nil, tempDir, fmt.Errorf("create overlay: %w", err)
	}

	// Create VM without NBD drive
	vm := &VM{
		config: VMConfig{
			RootImage: overlayPath,
			SSHKey:    sshKey,
			Memory:    2048,
			CPUs:      2,
			TempDir:   tempDir,
		},
		t: t,
	}

	return vm, tempDir, nil
}

// TestKernelNBDExt4 tests creating an ext4 filesystem on the kernel NBD device.
func TestKernelNBDExt4(t *testing.T) {
	// Check if running inside the VM
	if os.Getenv("CLOUDBLOCKDEV_IN_VM") == "1" {
		runInVMKernelNBDExt4(t)
		return
	}

	// --- Host orchestration ---
	RequirePrerequisites(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to get executable path: %v", err)
	}

	// Start MinIO
	pool, minio, s3Endpoint := startMinIO(t)
	defer pool.Purge(minio)

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

	hostIP := getHostIP(t)
	minioPort := minio.GetPort("9000/tcp")
	vmS3Endpoint := fmt.Sprintf("http://%s:%s", hostIP, minioPort)

	// Set up VM
	vm, tempDir, err := setupVMWithoutNBD(t)
	if err != nil {
		t.Fatalf("Failed to set up VM: %v", err)
	}
	defer CleanupTestVM(tempDir)
	defer vm.Shutdown(ctx)

	if err := vm.Start(ctx); err != nil {
		t.Fatalf("Failed to start VM: %v", err)
	}

	// Copy binary and load NBD module
	if err := copyFileToVM(ctx, vm, testBinary, "/tmp/e2e.test"); err != nil {
		t.Fatalf("Failed to copy binary: %v", err)
	}
	vm.RunSudo(ctx, "chmod +x /tmp/e2e.test")
	vm.RunSudo(ctx, "modprobe nbd")

	// Run the test inside the VM
	t.Log("Running ext4 test inside VM...")
	cmd := fmt.Sprintf(
		"CLOUDBLOCKDEV_IN_VM=1 S3_ENDPOINT=%s S3_ACCESS_KEY=%s S3_SECRET_KEY=%s S3_BUCKET=%s "+
			"/tmp/e2e.test -test.run ^TestKernelNBDExt4$ -test.v -test.timeout 10m",
		vmS3Endpoint, MinIOAccessKey, MinIOSecretKey, TestBucket,
	)
	output, err := vm.RunSudo(ctx, cmd)
	if err != nil {
		t.Fatalf("Test failed in VM: %v\nOutput:\n%s", err, output)
	}
	t.Logf("VM test output:\n%s", output)

	t.Log("Ext4 test passed!")
}

// runInVMKernelNBDExt4 runs inside the VM testing ext4 filesystem.
func runInVMKernelNBDExt4(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	s3Endpoint := os.Getenv("S3_ENDPOINT")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	bucket := os.Getenv("S3_BUCKET")

	if s3Endpoint == "" || bucket == "" {
		t.Fatal("S3_ENDPOINT and S3_BUCKET must be set")
	}

	t.Logf("Running in VM with S3 endpoint: %s", s3Endpoint)

	// Create S3 blob store
	store, err := cloudblock.NewS3BlobStore(ctx, cloudblock.S3BlobStoreConfig{
		Bucket:    bucket,
		Prefix:    "ext4-test/",
		Endpoint:  s3Endpoint,
		Region:    "us-east-1",
		AccessKey: accessKey,
		SecretKey: secretKey,
	})
	if err != nil {
		t.Fatalf("Failed to create S3 blob store: %v", err)
	}

	// Create cloudblock device
	device, err := cloudblock.NewDevice(cloudblock.DeviceConfig{
		Store: store,
		Size:  KernelNBDDeviceSize,
	})
	if err != nil {
		t.Fatalf("Failed to create device: %v", err)
	}
	if err := device.Start(); err != nil {
		t.Fatalf("Failed to start device: %v", err)
	}
	defer device.Close()

	// Set up kernel NBD device
	dm := nbdclient.NewDeviceManager(nbdclient.DeviceConfig{
		Index:           0,
		Size:            KernelNBDDeviceSize,
		DeadConnTimeout: 120 * time.Second,
	})
	if err := dm.Setup(); err != nil {
		t.Fatalf("Failed to setup NBD device: %v", err)
	}
	defer dm.Disconnect()

	t.Logf("NBD device ready at %s", dm.DevicePath())

	// Create NBD server and start handling in background
	nbdServer := nbd.NewServer(nbd.Config{
		Device:     device,
		ExportName: "test",
	})

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- nbdServer.HandleDirectConnection(dm.ServerConn())
	}()

	// Verify device size
	sizeOut, err := runCommand("blockdev", "--getsize64", dm.DevicePath())
	if err != nil {
		t.Fatalf("Failed to get device size: %v", err)
	}
	t.Logf("Device size: %s bytes", strings.TrimSpace(sizeOut))

	// Create ext4 filesystem
	t.Log("Creating ext4 filesystem...")
	if _, err := runCommand("mkfs.ext4", "-F", dm.DevicePath()); err != nil {
		t.Fatalf("Failed to create ext4 filesystem: %v", err)
	}

	// Create mount point and mount
	mountPoint := "/mnt/ext4test"
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		t.Fatalf("Failed to create mount point: %v", err)
	}

	t.Log("Mounting ext4 filesystem...")
	if _, err := runCommand("mount", dm.DevicePath(), mountPoint); err != nil {
		t.Fatalf("Failed to mount ext4 filesystem: %v", err)
	}

	// Write test data
	t.Log("Writing test data...")
	testData := "ext4 kernel nbd test data - " + time.Now().String()
	testFile := filepath.Join(mountPoint, "test.txt")
	if err := os.WriteFile(testFile, []byte(testData), 0644); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	// Sync
	if _, err := runCommand("sync"); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Read back and verify
	readData, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read test data: %v", err)
	}
	if string(readData) != testData {
		t.Errorf("Data mismatch: got %q, want %q", string(readData), testData)
	}
	t.Log("Data verified successfully")

	// Unmount for restart test
	t.Log("Unmounting ext4 filesystem...")
	if _, err := runCommand("umount", mountPoint); err != nil {
		t.Fatalf("Failed to unmount: %v", err)
	}

	// --- Simulate restart ---
	t.Log("Simulating restart (closing server connection)...")

	// Shutdown NBD server
	nbdServer.Shutdown()

	// Wait for server to finish
	select {
	case err := <-serverDone:
		t.Logf("Server stopped: %v", err)
	case <-time.After(5 * time.Second):
		t.Log("Server did not stop in time, continuing...")
	}

	// Close device manager
	dm.Close()

	// Wait a moment
	time.Sleep(2 * time.Second)

	// --- Reconnect ---
	t.Log("Reconnecting (testing NBD_CMD_RECONFIGURE)...")

	// Create new device manager
	dm2 := nbdclient.NewDeviceManager(nbdclient.DeviceConfig{
		Index:           0,
		Size:            KernelNBDDeviceSize,
		DeadConnTimeout: 120 * time.Second,
	})
	if err := dm2.Setup(); err != nil {
		t.Fatalf("Failed to reconnect NBD device: %v", err)
	}
	defer dm2.Disconnect()

	// Start new server
	nbdServer2 := nbd.NewServer(nbd.Config{
		Device:     device,
		ExportName: "test",
	})
	go nbdServer2.HandleDirectConnection(dm2.ServerConn())
	defer nbdServer2.Shutdown()

	// Verify device is still available
	sizeOut2, err := runCommand("blockdev", "--getsize64", dm2.DevicePath())
	if err != nil {
		t.Fatalf("Device not available after restart: %v", err)
	}
	t.Logf("Device size after restart: %s bytes", strings.TrimSpace(sizeOut2))

	// Re-mount ext4 filesystem
	t.Log("Re-mounting ext4 filesystem...")
	if _, err := runCommand("mount", dm2.DevicePath(), mountPoint); err != nil {
		t.Fatalf("Failed to re-mount ext4 filesystem: %v", err)
	}

	// Verify data after restart
	readData2, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read after restart: %v", err)
	}
	if string(readData2) != testData {
		t.Errorf("Data mismatch after restart: got %q, want %q", string(readData2), testData)
	}
	t.Log("Data verified after restart")

	// Cleanup
	t.Log("Cleaning up...")
	runCommand("umount", mountPoint)

	t.Log("Ext4 kernel NBD test completed successfully!")
}

// nbdTransport implements blockTransport for the kernel NBD driver.
type nbdTransport struct {
	dm         *nbdclient.DeviceManager
	server     *nbd.Server
	serverDone chan error
}

func (t *nbdTransport) Connect(device *cloudblock.Device, size int64) (string, error) {
	t.dm = nbdclient.NewDeviceManager(nbdclient.DeviceConfig{
		Index:           0,
		Size:            uint64(size),
		DeadConnTimeout: 120 * time.Second,
	})
	if err := t.dm.Setup(); err != nil {
		return "", fmt.Errorf("NBD device setup: %w", err)
	}

	t.server = nbd.NewServer(nbd.Config{
		Device:     device,
		ExportName: "test",
	})

	t.serverDone = make(chan error, 1)
	go func() {
		t.serverDone <- t.server.HandleDirectConnection(t.dm.ServerConn())
	}()

	return t.dm.DevicePath(), nil
}

func (t *nbdTransport) Disconnect() error {
	if t.server != nil {
		t.server.Shutdown()
		t.server = nil
	}
	if t.serverDone != nil {
		select {
		case <-t.serverDone:
		case <-time.After(5 * time.Second):
		}
		t.serverDone = nil
	}
	if t.dm != nil {
		t.dm.Disconnect()
		t.dm = nil
	}
	time.Sleep(1 * time.Second)
	return nil
}

// TestKernelNBDColdStartRecovery tests data durability across a cold restart using kernel NBD.
// It writes data, syncs (which must flush to S3), tears down the device,
// recreates it from S3 only, and verifies data integrity.
func TestKernelNBDColdStartRecovery(t *testing.T) {
	if os.Getenv("CLOUDBLOCKDEV_IN_VM") == "1" {
		coldStartRecoveryTest(t, &nbdTransport{})
		return
	}
	runTestInVM(t, "TestKernelNBDColdStartRecovery", []string{"nbd"}, "nbd-cold-start/")
}

// copyFileToVM copies a file to the VM using base64 encoding over SSH.
func copyFileToVM(ctx context.Context, vm *VM, localPath, remotePath string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("read local file: %w", err)
	}

	// Encode as base64 and decode on VM
	encoded := base64.StdEncoding.EncodeToString(data)

	// Write in chunks to avoid command line limits
	chunkSize := 50000 // ~50KB per command
	for i := 0; i < len(encoded); i += chunkSize {
		end := i + chunkSize
		if end > len(encoded) {
			end = len(encoded)
		}
		chunk := encoded[i:end]

		var cmd string
		if i == 0 {
			cmd = fmt.Sprintf("echo -n '%s' > /tmp/b64tmp", chunk)
		} else {
			cmd = fmt.Sprintf("echo -n '%s' >> /tmp/b64tmp", chunk)
		}

		if _, err := vm.RunSudo(ctx, cmd); err != nil {
			return fmt.Errorf("write chunk: %w", err)
		}
	}

	// Decode base64 to final file
	if _, err := vm.RunSudo(ctx, fmt.Sprintf("base64 -d /tmp/b64tmp > %s && rm /tmp/b64tmp", remotePath)); err != nil {
		return fmt.Errorf("decode file: %w", err)
	}

	return nil
}
