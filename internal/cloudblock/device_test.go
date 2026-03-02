package cloudblock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/anupcshan/cloudblockdev/internal/nbd"

)

// Test helpers for async operations

func mustStart(t *testing.T, device *Device) {
	t.Helper()
	if err := device.Start(); err != nil {
		t.Fatalf("device.Start: %v", err)
	}
	t.Cleanup(func() {
		device.Stop(5 * time.Second)
	})
}

func writeAt(t *testing.T, device *Device, data []byte, offset int64) int {
	t.Helper()
	result := <-device.SubmitWrite(0, data, offset)
	if result.Err != nil {
		t.Fatalf("WriteAt offset %d: %v", offset, result.Err)
	}
	return result.N
}

func readAt(t *testing.T, device *Device, buf []byte, offset int64) int {
	t.Helper()
	result := <-device.SubmitRead(0, len(buf), offset)
	if result.Err != nil {
		t.Fatalf("ReadAt offset %d: %v", offset, result.Err)
	}
	copy(buf, result.Data)
	return result.N
}

func flush(t *testing.T, device *Device) {
	t.Helper()
	result := <-device.SubmitFlush(0)
	if result.Err != nil {
		t.Fatalf("Flush: %v", result.Err)
	}
}

func waitForSegments(t *testing.T, ctx context.Context, store BlobStore, n int) {
	t.Helper()
	for range 100 {
		keys, _ := store.ListPrefix(ctx, "index/segment-")
		if len(keys) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d segments", n)
}

func submitRead(device *Device, length int, offset int64) nbd.OpResult {
	return <-device.SubmitRead(0, length, offset)
}

func TestDeviceWriteReadSmall(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024, // 1MB
		LargeWriteThreshold: 512 * 1024,  // 512KB
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write small data (will be buffered)
	data := []byte("hello world")
	n := writeAt(t, device, data, 100)
	if n != len(data) {
		t.Errorf("WriteAt returned %d, want %d", n, len(data))
	}

	// Read back (from buffer, before flush)
	buf := make([]byte, len(data))
	n = readAt(t, device, buf, 100)
	if n != len(data) {
		t.Errorf("ReadAt returned %d, want %d", n, len(data))
	}
	if !bytes.Equal(buf, data) {
		t.Errorf("ReadAt: got %q, want %q", buf, data)
	}

	// Flush to store
	flush(t, device)

	// Verify blob was written to store
	blobs, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(blobs) != 1 {
		t.Errorf("Expected 1 blob, got %d", len(blobs))
	}

	// Read back (from store)
	buf = make([]byte, len(data))
	readAt(t, device, buf, 100)
	if !bytes.Equal(buf, data) {
		t.Errorf("ReadAt after flush: got %q, want %q", buf, data)
	}
}

func TestDeviceWriteReadLarge(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                10 * 1024 * 1024, // 10MB
		LargeWriteThreshold: 1024,             // 1KB threshold for testing
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write large data (will go directly to store)
	data := make([]byte, 2048)
	for i := range data {
		data[i] = byte(i % 256)
	}

	n := writeAt(t, device, data, 0)
	if n != len(data) {
		t.Errorf("WriteAt returned %d, want %d", n, len(data))
	}

	// Flush to ensure upload completes
	flush(t, device)

	// Verify blob was written
	blobs, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(blobs) == 0 {
		t.Error("Expected at least 1 blob")
	}

	// Read back
	buf := make([]byte, len(data))
	readAt(t, device, buf, 0)
	if !bytes.Equal(buf, data) {
		t.Errorf("ReadAt: data mismatch")
	}
}

func TestDeviceMultipleWrites(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write multiple small ranges
	writes := []struct {
		offset int64
		data   []byte
	}{
		{0, []byte("AAAA")},
		{100, []byte("BBBB")},
		{200, []byte("CCCC")},
	}

	for _, w := range writes {
		writeAt(t, device, w.data, w.offset)
	}

	// Flush all writes
	flush(t, device)

	// Verify all writes can be read back
	for _, w := range writes {
		buf := make([]byte, len(w.data))
		readAt(t, device, buf, w.offset)
		if !bytes.Equal(buf, w.data) {
			t.Errorf("ReadAt offset %d: got %q, want %q", w.offset, buf, w.data)
		}
	}

	// Read a range that spans a gap (should be zero-filled)
	buf := make([]byte, 50)
	readAt(t, device, buf, 0)
	// First 4 bytes should be "AAAA"
	if !bytes.Equal(buf[:4], []byte("AAAA")) {
		t.Errorf("First 4 bytes: got %q, want %q", buf[:4], "AAAA")
	}
	// Bytes 4-49 should be zeros (gap)
	for i := 4; i < 50; i++ {
		if buf[i] != 0 {
			t.Errorf("Byte %d should be zero, got %d", i, buf[i])
			break
		}
	}

	_ = ctx // silence unused warning
}

func TestDeviceOverwrite(t *testing.T) {
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write initial data
	initial := []byte("AAAABBBBCCCC")
	writeAt(t, device, initial, 0)
	flush(t, device)

	// Overwrite middle portion
	overwrite := []byte("XXXX")
	writeAt(t, device, overwrite, 4)
	flush(t, device)

	// Read back full range
	buf := make([]byte, 12)
	readAt(t, device, buf, 0)

	expected := []byte("AAAAXXXXCCCC")
	if !bytes.Equal(buf, expected) {
		t.Errorf("After overwrite: got %q, want %q", buf, expected)
	}
}

func TestDeviceLoadIndex(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	// Create device and write some data
	device1, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	if err := device1.Start(); err != nil {
		t.Fatalf("device1.Start: %v", err)
	}

	data := []byte("persistent data")
	writeAt(t, device1, data, 500)
	flush(t, device1)
	device1.Stop(5 * time.Second)

	// Create a new device instance (simulating restart)
	device2, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice 2: %v", err)
	}
	mustStart(t, device2)

	// Load index from store
	if err := device2.LoadIndex(ctx); err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}

	// Read back data
	buf := make([]byte, len(data))
	readAt(t, device2, buf, 500)
	if !bytes.Equal(buf, data) {
		t.Errorf("After LoadIndex: got %q, want %q", buf, data)
	}
}

func TestDeviceSparseRead(t *testing.T) {
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Read from unwritten region (should be zeros)
	buf := make([]byte, 100)
	for i := range buf {
		buf[i] = 0xFF // Fill with non-zero to verify it gets zeroed
	}

	result := submitRead(device, 100, 1000)
	if result.Err != nil {
		t.Fatalf("ReadAt: %v", result.Err)
	}
	if result.N != 100 {
		t.Errorf("ReadAt returned %d, want 100", result.N)
	}

	for i, b := range result.Data {
		if b != 0 {
			t.Errorf("Byte %d should be zero, got %d", i, b)
			break
		}
	}
}

func TestDeviceReadYourWrites(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write data but don't flush
	data := []byte("buffered data")
	writeAt(t, device, data, 0)

	// Verify store is empty (not flushed yet)
	blobs, _ := store.List(ctx)
	if len(blobs) != 0 {
		t.Errorf("Expected 0 blobs before flush, got %d", len(blobs))
	}

	// Read should still return the buffered data
	buf := make([]byte, len(data))
	readAt(t, device, buf, 0)
	if !bytes.Equal(buf, data) {
		t.Errorf("Read-your-writes: got %q, want %q", buf, data)
	}
}

// TestDeviceConcurrentWriteRead tests that a read submitted immediately after
// a write sees the written data, even if the write hasn't been processed yet.
// This tests the race condition between write sequencer and read workers.
func TestDeviceConcurrentWriteRead(t *testing.T) {
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Run multiple iterations to increase chance of hitting the race
	for i := 0; i < 100; i++ {
		offset := int64(i * 1024)
		data := []byte(fmt.Sprintf("test data iteration %d", i))

		// Submit write - don't wait for result yet
		writeCh := device.SubmitWrite(0, data, offset)

		// Immediately submit read - this may race with write processing
		readCh := device.SubmitRead(0, len(data), offset)

		// Now wait for both results
		writeResult := <-writeCh
		if writeResult.Err != nil {
			t.Fatalf("Write error: %v", writeResult.Err)
		}

		readResult := <-readCh
		if readResult.Err != nil {
			t.Fatalf("Read error: %v", readResult.Err)
		}

		// The read MUST see the written data
		if !bytes.Equal(readResult.Data, data) {
			t.Errorf("Iteration %d: read-after-write failed: got %q, want %q",
				i, readResult.Data, data)
		}
	}
}

// TestDeviceConcurrentLargeWriteRead tests read-after-write for large writes
// that bypass the buffer and go directly to blob creation.
func TestDeviceConcurrentLargeWriteRead(t *testing.T) {
	store := NewMemoryBlobStore()

	// Use a small threshold to make writes "large"
	largeThreshold := 1024

	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                10 * 1024 * 1024, // 10MB
		LargeWriteThreshold: largeThreshold,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Run multiple iterations
	for i := 0; i < 20; i++ {
		offset := int64(i * largeThreshold * 2)
		// Create data larger than threshold
		data := bytes.Repeat([]byte{byte('A' + i%26)}, largeThreshold+100)

		// Submit write - don't wait for result yet
		writeCh := device.SubmitWrite(0, data, offset)

		// Immediately submit read - this may race with write processing
		readCh := device.SubmitRead(0, len(data), offset)

		// Now wait for both results
		writeResult := <-writeCh
		if writeResult.Err != nil {
			t.Fatalf("Write error: %v", writeResult.Err)
		}

		readResult := <-readCh
		if readResult.Err != nil {
			t.Fatalf("Read error: %v", readResult.Err)
		}

		// The read MUST see the written data
		if !bytes.Equal(readResult.Data, data) {
			t.Errorf("Iteration %d: large read-after-write failed: got %d bytes, want %d bytes",
				i, len(readResult.Data), len(data))
			// Show first difference
			for j := 0; j < len(data) && j < len(readResult.Data); j++ {
				if data[j] != readResult.Data[j] {
					t.Errorf("First difference at byte %d: got %d, want %d", j, readResult.Data[j], data[j])
					break
				}
			}
		}
	}
}

func TestDeviceSparseWritesInBuffer(t *testing.T) {
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024 * 1024, // 1GB device
		LargeWriteThreshold: 512 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write at two widely separated offsets (this would OOM with naive coalescing)
	data1 := []byte("START")
	data2 := []byte("END")
	offset1 := int64(0)
	offset2 := int64(500 * 1024 * 1024) // 500MB apart

	writeAt(t, device, data1, offset1)
	writeAt(t, device, data2, offset2)

	// Flush should NOT allocate 500MB of memory
	flush(t, device)

	// Verify both writes are readable
	buf1 := make([]byte, len(data1))
	readAt(t, device, buf1, offset1)
	if !bytes.Equal(buf1, data1) {
		t.Errorf("Read 1: got %q, want %q", buf1, data1)
	}

	buf2 := make([]byte, len(data2))
	readAt(t, device, buf2, offset2)
	if !bytes.Equal(buf2, data2) {
		t.Errorf("Read 2: got %q, want %q", buf2, data2)
	}
}

func TestDeviceOverlappingWritesInBuffer(t *testing.T) {
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512 * 1024, // High threshold so all writes are buffered
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write initial data: "AAAABBBBCCCCDDDD" at offset 0
	writeAt(t, device, []byte("AAAABBBBCCCCDDDD"), 0)

	// Overwrite middle portion with "XXXX" at offset 4 (should result in "AAAAXXXXCCCCDDDD")
	writeAt(t, device, []byte("XXXX"), 4)

	// Read back BEFORE flush (from buffer) - should see the overwrite
	buf := make([]byte, 16)
	readAt(t, device, buf, 0)
	expected := []byte("AAAAXXXXCCCCDDDD")
	if !bytes.Equal(buf, expected) {
		t.Errorf("Before flush: got %q, want %q", buf, expected)
	}

	// Flush to store - this is where overlapping writes in buffer get combined into one blob
	flush(t, device)

	// Read back AFTER flush (from store) - should still see the correct data
	buf = make([]byte, 16)
	readAt(t, device, buf, 0)
	if !bytes.Equal(buf, expected) {
		t.Errorf("After flush: got %q, want %q", buf, expected)
	}

	// Also test reading a subset
	buf = make([]byte, 4)
	readAt(t, device, buf, 4)
	if !bytes.Equal(buf, []byte("XXXX")) {
		t.Errorf("Subset read: got %q, want %q", buf, "XXXX")
	}
}

func TestDeviceRandomWriteReadPattern(t *testing.T) {
	store := NewMemoryBlobStore()

	deviceSize := int64(1024 * 1024) // 1MB
	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                deviceSize,
		LargeWriteThreshold: 512 * 1024, // High threshold so all writes are buffered
		MaxBufferSize:       64 * 1024,  // Force frequent flushes
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Keep a reference copy of what we expect the device to contain
	reference := make([]byte, deviceSize)

	// Do many random writes at 4KB aligned offsets with 4KB blocks (like ZFS)
	blockSize := 4096
	numWrites := 100

	for i := 0; i < numWrites; i++ {
		// Random 4KB-aligned offset within the first 256KB
		offset := int64((i*7)%64) * int64(blockSize) // Deterministic pseudo-random

		// Write pattern includes the iteration number for uniqueness
		data := make([]byte, blockSize)
		for j := range data {
			data[j] = byte(i + j)
		}

		// Write to device
		writeAt(t, device, data, offset)

		// Update reference
		copy(reference[offset:offset+int64(blockSize)], data)
	}

	// Flush any remaining buffered data
	flush(t, device)

	// Verify all data by reading the entire range we wrote to
	buf := make([]byte, 256*1024) // 256KB
	readAt(t, device, buf, 0)

	// Compare with reference
	if !bytes.Equal(buf, reference[:256*1024]) {
		// Find first difference
		for i := 0; i < len(buf); i++ {
			if buf[i] != reference[i] {
				t.Errorf("First difference at offset %d: got %d, want %d", i, buf[i], reference[i])
				break
			}
		}
	}
}

func TestDeviceInterleavedWritesAndReads(t *testing.T) {
	store := NewMemoryBlobStore()

	deviceSize := int64(1024 * 1024) // 1MB
	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                deviceSize,
		LargeWriteThreshold: 512 * 1024,
		MaxBufferSize:       16 * 1024, // Small buffer to trigger frequent flushes
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Keep a reference copy
	reference := make([]byte, deviceSize)

	// Interleave writes and reads - this mimics ZFS behavior
	blockSize := 4096
	numOps := 200

	for i := 0; i < numOps; i++ {
		offset := int64((i*13)%64) * int64(blockSize) // Pseudo-random offset

		if i%3 == 0 {
			// Read operation - verify data matches reference
			result := submitRead(device, blockSize, offset)
			if result.Err != nil {
				t.Fatalf("ReadAt %d: %v", i, result.Err)
			}
			if !bytes.Equal(result.Data, reference[offset:offset+int64(blockSize)]) {
				t.Errorf("Op %d: Read mismatch at offset %d", i, offset)
				for j := 0; j < blockSize; j++ {
					if result.Data[j] != reference[offset+int64(j)] {
						t.Errorf("  First diff at byte %d: got %d, want %d", j, result.Data[j], reference[offset+int64(j)])
						break
					}
				}
			}
		} else {
			// Write operation
			data := make([]byte, blockSize)
			for j := range data {
				data[j] = byte(i ^ j)
			}
			writeAt(t, device, data, offset)
			copy(reference[offset:offset+int64(blockSize)], data)
		}

		// Occasional flush (like ZFS txg commits)
		if i%20 == 0 {
			flush(t, device)
		}
	}

	// Final flush and full verification
	flush(t, device)

	// Read entire written area and compare
	buf := make([]byte, 256*1024)
	readAt(t, device, buf, 0)

	if !bytes.Equal(buf, reference[:256*1024]) {
		t.Error("Final verification failed")
		for i := 0; i < len(buf); i++ {
			if buf[i] != reference[i] {
				t.Errorf("First diff at offset %d: got %d, want %d", i, buf[i], reference[i])
				break
			}
		}
	}
}

// TestDeviceUncachedRead verifies that reads work correctly from the blob store.
// This exercises the readRangesFromStore path which reads directly from the blob store.
func TestDeviceUncachedRead(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	// Create device, write data, flush to store
	device1, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	if err := device1.Start(); err != nil {
		t.Fatalf("device1.Start: %v", err)
	}

	// Write data at multiple offsets to test range handling
	data1 := []byte("first block of data for uncached read test")
	data2 := []byte("second block at different offset")
	writeAt(t, device1, data1, 0)
	writeAt(t, device1, data2, 4096)
	flush(t, device1)
	device1.Stop(5 * time.Second)

	// Verify blobs were written
	blobs, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(blobs) == 0 {
		t.Fatal("Expected at least 1 blob")
	}

	// Create new device and load index
	device2, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512 * 1024,
		// Reads go through readRangesFromStore path
	})
	if err != nil {
		t.Fatalf("NewDevice 2: %v", err)
	}
	mustStart(t, device2)

	if err := device2.LoadIndex(ctx); err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}

	// Read back first block
	buf1 := make([]byte, len(data1))
	readAt(t, device2, buf1, 0)
	if !bytes.Equal(buf1, data1) {
		t.Errorf("Read at offset 0: got %q, want %q", buf1, data1)
	}

	// Read back second block
	buf2 := make([]byte, len(data2))
	readAt(t, device2, buf2, 4096)
	if !bytes.Equal(buf2, data2) {
		t.Errorf("Read at offset 4096: got %q, want %q", buf2, data2)
	}

	// Test reading a range that spans both blocks (with zeros in between)
	spanBuf := make([]byte, 4096+len(data2))
	readAt(t, device2, spanBuf, 0)
	if !bytes.Equal(spanBuf[:len(data1)], data1) {
		t.Errorf("Span read data1 portion mismatch")
	}
	// Middle should be zeros
	for i := len(data1); i < 4096; i++ {
		if spanBuf[i] != 0 {
			t.Errorf("Span read byte %d: got %d, want 0", i, spanBuf[i])
			break
		}
	}
	if !bytes.Equal(spanBuf[4096:], data2) {
		t.Errorf("Span read data2 portion mismatch")
	}
}

func TestDeviceSegmentWriting(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	// Create device with low segment threshold for testing
	device, err := NewDevice(DeviceConfig{
		Store:               store,
		Size:                10 * 1024 * 1024, // 10MB
		SegmentThreshold:    3,                 // Write segment after 3 blobs
		LargeWriteThreshold: 1,                 // Make every write go directly to blob
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	if err := device.Start(); err != nil {
		t.Fatalf("device.Start: %v", err)
	}

	// Write 4 blobs (should trigger one segment write after 3)
	for i := 0; i < 4; i++ {
		data := []byte(fmt.Sprintf("blob data %d", i))
		writeAt(t, device, data, int64(i*1000))
		flush(t, device)
	}

	// Give async segment writer time to complete
	time.Sleep(100 * time.Millisecond)

	device.Stop(5 * time.Second)

	// Check that a segment was written
	keys, err := store.ListPrefix(ctx, SegmentPrefix)
	if err != nil {
		t.Fatalf("ListPrefix: %v", err)
	}

	// Should have at least one segment
	if len(keys) == 0 {
		t.Error("Expected at least one segment to be written")
	}
}

func TestDeviceLoadIndexFromSegments(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	// Write some blobs directly to store
	for i := 0; i < 3; i++ {
		blobID := GenerateBlobID()
		blocks := []BlockEntry{{Offset: uint64(i * 1000), Length: 100}}
		data := make([]byte, 100)
		for j := range data {
			data[j] = byte(i)
		}
		blob, err := NewBlob(blocks, [][]byte{data})
		if err != nil {
			t.Fatalf("NewBlob: %v", err)
		}
		// Override the auto-generated ID to use our sequential one
		blob.ID = blobID
		blobData, _ := blob.Marshal()
		if err := store.Put(ctx, blob.ID, bytes.NewReader(blobData), int64(len(blobData))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Create a segment containing these blobs
	blobs, _ := store.List(ctx)
	var entries []IndexSegmentEntry
	var maxBlobTS int64
	for _, blob := range blobs {
		data := make([]byte, BlobHeaderMax)
		n, _ := store.Get(ctx, blob.ID, data, 0)
		header, _ := UnmarshalBlobHeader(data[:n])
		entries = append(entries, IndexSegmentEntry{
			ID:     blob.ID,
			Header: *header,
		})
		if ts, err := ParseBlobIDTimestamp(blob.ID); err == nil && int64(ts) > maxBlobTS {
			maxBlobTS = int64(ts)
		}
	}
	segment := NewIndexSegment(0, maxBlobTS, entries)
	segmentData, _ := segment.Marshal()
	if err := store.PutKey(ctx, segment.Key(), segmentData); err != nil {
		t.Fatalf("PutKey: %v", err)
	}

	// Create device and load index from S3 segments
	device, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	if err := device.Start(); err != nil {
		t.Fatalf("device.Start: %v", err)
	}

	if err := device.LoadIndex(ctx); err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}

	// Index should have entries
	if device.index.Len() == 0 {
		t.Error("Index should have entries after segment loading")
	}

	device.Stop(5 * time.Second)
}

func TestDeviceLoadIndexWithOverlappingSegments(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	// Create blobs at different timestamps writing to the SAME offset
	// This simulates overwrites where later blobs should win
	var allEntries []IndexSegmentEntry

	// Blob 1: writes "AAAA" to offset 0 at T=100
	blob1ID := "0000000000100-blob1"
	blob1, _ := NewBlob([]BlockEntry{{Offset: 0, Length: 4}}, [][]byte{[]byte("AAAA")})
	blob1.ID = blob1ID
	blob1Data, _ := blob1.Marshal()
	store.Put(ctx, blob1ID, bytes.NewReader(blob1Data), int64(len(blob1Data)))
	header1, _ := UnmarshalBlobHeader(blob1Data)
	allEntries = append(allEntries, IndexSegmentEntry{ID: blob1ID, Header: *header1})

	// Blob 2: writes "BBBB" to offset 0 at T=200 (should overwrite blob1)
	blob2ID := "0000000000200-blob2"
	blob2, _ := NewBlob([]BlockEntry{{Offset: 0, Length: 4}}, [][]byte{[]byte("BBBB")})
	blob2.ID = blob2ID
	blob2Data, _ := blob2.Marshal()
	store.Put(ctx, blob2ID, bytes.NewReader(blob2Data), int64(len(blob2Data)))
	header2, _ := UnmarshalBlobHeader(blob2Data)
	allEntries = append(allEntries, IndexSegmentEntry{ID: blob2ID, Header: *header2})

	// Blob 3: writes "CCCC" to offset 0 at T=150 (between blob1 and blob2)
	blob3ID := "0000000000150-blob3"
	blob3, _ := NewBlob([]BlockEntry{{Offset: 0, Length: 4}}, [][]byte{[]byte("CCCC")})
	blob3.ID = blob3ID
	blob3Data, _ := blob3.Marshal()
	store.Put(ctx, blob3ID, bytes.NewReader(blob3Data), int64(len(blob3Data)))
	header3, _ := UnmarshalBlobHeader(blob3Data)
	allEntries = append(allEntries, IndexSegmentEntry{ID: blob3ID, Header: *header3})

	// Create two OVERLAPPING segments:
	// Segment 1: covers T=100-150 (blob1, blob3)
	seg1 := &IndexSegment{
		StartTimestampExclusive: 100,
		EndTimestampInclusive:   150,
		Blobs:          []IndexSegmentEntry{allEntries[0], allEntries[2]}, // blob1, blob3
	}
	seg1Data, _ := seg1.Marshal()
	store.PutKey(ctx, seg1.Key(), seg1Data)

	// Segment 2: covers T=100-200 (all blobs) - this COVERS segment 1
	seg2 := &IndexSegment{
		StartTimestampExclusive: 100,
		EndTimestampInclusive:   200,
		Blobs:          allEntries, // all blobs
	}
	seg2Data, _ := seg2.Marshal()
	store.PutKey(ctx, seg2.Key(), seg2Data)

	// Verify we have 2 segments
	keys, _ := store.ListPrefix(ctx, SegmentPrefix)
	if len(keys) != 2 {
		t.Fatalf("Expected 2 segments, got %d", len(keys))
	}

	// Create device and load index
	device, _ := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	device.Start()

	if err := device.LoadIndex(ctx); err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}

	// The key test: read offset 0 - should get "BBBB" (blob2, T=200)
	// NOT "CCCC" (blob3, T=150) or "AAAA" (blob1, T=100)
	buf := make([]byte, 4)
	result := <-device.SubmitRead(0, 4, 0)
	if result.Err != nil {
		t.Fatalf("Read failed: %v", result.Err)
	}
	copy(buf, result.Data)

	if string(buf) != "BBBB" {
		t.Errorf("Expected 'BBBB' (latest write), got %q", string(buf))
	}

	// Verify index has all 3 blobs
	if !device.index.HasBlob(blob1ID) || !device.index.HasBlob(blob2ID) || !device.index.HasBlob(blob3ID) {
		t.Error("Index should have all 3 blobs")
	}

	device.Stop(5 * time.Second)
}

func TestDeviceLoadIndexWithEpochPrecedence(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	// Create blobs
	blob1ID := "0000000000100-blob1"
	blob1, _ := NewBlob([]BlockEntry{{Offset: 0, Length: 4}}, [][]byte{[]byte("AAAA")})
	blob1.ID = blob1ID
	blob1Data, _ := blob1.Marshal()
	store.Put(ctx, blob1ID, bytes.NewReader(blob1Data), int64(len(blob1Data)))
	header1, _ := UnmarshalBlobHeader(blob1Data)
	entry1 := IndexSegmentEntry{ID: blob1ID, Header: *header1}

	blob2ID := "0000000000200-blob2"
	blob2, _ := NewBlob([]BlockEntry{{Offset: 0, Length: 4}}, [][]byte{[]byte("BBBB")})
	blob2.ID = blob2ID
	blob2Data, _ := blob2.Marshal()
	store.Put(ctx, blob2ID, bytes.NewReader(blob2Data), int64(len(blob2Data)))
	header2, _ := UnmarshalBlobHeader(blob2Data)
	entry2 := IndexSegmentEntry{ID: blob2ID, Header: *header2}

	// Create two segments with SAME time range but different epochs
	// Lower epoch (0) segment has both blobs
	seg0 := &IndexSegment{
		Epoch:          0,
		StartTimestampExclusive: 100,
		EndTimestampInclusive:   200,
		Blobs:          []IndexSegmentEntry{entry1, entry2},
	}
	seg0Data, _ := seg0.Marshal()
	store.PutKey(ctx, seg0.Key(), seg0Data)

	// Higher epoch (1) segment has only blob1 (simulating compaction that removed blob2)
	seg1 := &IndexSegment{
		Epoch:          1,
		StartTimestampExclusive: 100,
		EndTimestampInclusive:   200,
		Blobs:          []IndexSegmentEntry{entry1}, // Only blob1
	}
	seg1Data, _ := seg1.Marshal()
	store.PutKey(ctx, seg1.Key(), seg1Data)

	// Verify we have 2 segments
	keys, _ := store.ListPrefix(ctx, SegmentPrefix)
	if len(keys) != 2 {
		t.Fatalf("Expected 2 segments, got %d", len(keys))
	}

	// Create device and load index
	device, _ := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	device.Start()

	if err := device.LoadIndex(ctx); err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}

	// The higher epoch segment should win, so we should only have blob1
	// and reading should return "AAAA" not "BBBB"
	buf := make([]byte, 4)
	result := <-device.SubmitRead(0, 4, 0)
	if result.Err != nil {
		t.Fatalf("Read failed: %v", result.Err)
	}
	copy(buf, result.Data)

	if string(buf) != "AAAA" {
		t.Errorf("Expected 'AAAA' (from higher epoch segment), got %q", string(buf))
	}

	// Verify only blob1 is in index (from the higher epoch segment)
	if !device.index.HasBlob(blob1ID) {
		t.Error("Index should have blob1")
	}
	if device.index.HasBlob(blob2ID) {
		t.Error("Index should NOT have blob2 (superseded by higher epoch)")
	}

	device.Stop(5 * time.Second)
}

// GC Tests

func TestGCDeletesUnreferencedBlobs(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:         store,
		Size:          1024 * 1024, // 1MB
		MaxBufferSize:    4096,        // Small buffer to force frequent flushes
		SegmentThreshold: 1,           // Write segments immediately for GC
		GCEnabled:        true,
		GCInterval:    100 * time.Millisecond, // Fast GC for testing
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write data to offset 0, creating blob A
	data1 := bytes.Repeat([]byte("A"), 1000)
	writeAt(t, device, data1, 0)
	flush(t, device)

	// Get list of blobs before overwrite
	blobsBefore, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(blobsBefore) != 1 {
		t.Fatalf("Expected 1 blob before overwrite, got %d", len(blobsBefore))
	}
	blobA := blobsBefore[0]

	// Overwrite the same location, creating blob B
	data2 := bytes.Repeat([]byte("B"), 1000)
	writeAt(t, device, data2, 0)
	flush(t, device)

	// Now we should have 2 blobs, but only blob B is referenced
	blobsAfter, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(blobsAfter) != 2 {
		t.Fatalf("Expected 2 blobs after overwrite, got %d", len(blobsAfter))
	}

	// Wait for GC to run
	time.Sleep(300 * time.Millisecond)

	// Blob A should be deleted, blob B should remain
	blobsAfterGC, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(blobsAfterGC) != 1 {
		t.Errorf("Expected 1 blob after GC, got %d", len(blobsAfterGC))
	}

	// The remaining blob should NOT be blob A
	for _, b := range blobsAfterGC {
		if b == blobA {
			t.Errorf("Blob A should have been deleted by GC")
		}
	}

	// Data should still be readable (from blob B)
	buf := make([]byte, 1000)
	readAt(t, device, buf, 0)
	if !bytes.Equal(buf, data2) {
		t.Errorf("Data mismatch after GC: got %q, want %q", buf[:10], data2[:10])
	}
}

func TestGCPreservesReferencedBlobs(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:         store,
		Size:          1024 * 1024,
		MaxBufferSize:    4096,
		SegmentThreshold: 1,
		GCEnabled:        true,
		GCInterval:       100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write data to two different locations
	data1 := bytes.Repeat([]byte("A"), 1000)
	data2 := bytes.Repeat([]byte("B"), 1000)
	writeAt(t, device, data1, 0)
	writeAt(t, device, data2, 2000)
	flush(t, device)

	// Get blob count
	blobsBefore, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	countBefore := len(blobsBefore)

	// Wait for GC to run
	time.Sleep(300 * time.Millisecond)

	// All blobs should still exist (both are referenced)
	blobsAfter, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(blobsAfter) != countBefore {
		t.Errorf("GC deleted referenced blobs: had %d, now have %d", countBefore, len(blobsAfter))
	}

	// Data should still be readable
	buf1 := make([]byte, 1000)
	buf2 := make([]byte, 1000)
	readAt(t, device, buf1, 0)
	readAt(t, device, buf2, 2000)

	if !bytes.Equal(buf1, data1) {
		t.Errorf("Data1 mismatch after GC")
	}
	if !bytes.Equal(buf2, data2) {
		t.Errorf("Data2 mismatch after GC")
	}
}

func TestGCDisabled(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:         store,
		Size:          1024 * 1024,
		MaxBufferSize: 4096,
		GCEnabled:     false, // Disabled
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write and overwrite
	data1 := bytes.Repeat([]byte("A"), 1000)
	writeAt(t, device, data1, 0)
	flush(t, device)

	data2 := bytes.Repeat([]byte("B"), 1000)
	writeAt(t, device, data2, 0)
	flush(t, device)

	// Wait a bit
	time.Sleep(200 * time.Millisecond)

	// Both blobs should still exist (GC is disabled)
	blobs, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(blobs) != 2 {
		t.Errorf("Expected 2 blobs (GC disabled), got %d", len(blobs))
	}
}

func TestGCPartialOverwrite(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:         store,
		Size:          1024 * 1024,
		MaxBufferSize:    4096,
		SegmentThreshold: 1,
		GCEnabled:        true,
		GCInterval:       100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write data spanning 0-1000
	data1 := bytes.Repeat([]byte("A"), 1000)
	writeAt(t, device, data1, 0)
	flush(t, device)

	blobsBefore, _ := store.List(ctx)
	blobA := blobsBefore[0]

	// Partially overwrite (only 500-1000)
	data2 := bytes.Repeat([]byte("B"), 500)
	writeAt(t, device, data2, 500)
	flush(t, device)

	// Wait for GC
	time.Sleep(300 * time.Millisecond)

	// Blob A should still exist because 0-500 still references it
	blobsAfter, _ := store.List(ctx)

	foundA := false
	for _, b := range blobsAfter {
		if b == blobA {
			foundA = true
			break
		}
	}

	if !foundA {
		t.Errorf("Blob A should NOT be deleted - it's still partially referenced")
	}

	// Data should be correct: AAAAA...BBBBB...
	buf := make([]byte, 1000)
	readAt(t, device, buf, 0)

	if !bytes.Equal(buf[:500], bytes.Repeat([]byte("A"), 500)) {
		t.Errorf("First 500 bytes should be A")
	}
	if !bytes.Equal(buf[500:], bytes.Repeat([]byte("B"), 500)) {
		t.Errorf("Last 500 bytes should be B")
	}
}

func TestRunGCCycleDirectly(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:         store,
		Size:          1024 * 1024,
		MaxBufferSize:    4096,
		SegmentThreshold: 1,
		GCEnabled:        false, // Don't run background GC
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write and overwrite
	data1 := bytes.Repeat([]byte("A"), 1000)
	writeAt(t, device, data1, 0)
	flush(t, device)

	blobsBefore, _ := store.List(ctx)
	blobA := blobsBefore[0]

	data2 := bytes.Repeat([]byte("B"), 1000)
	writeAt(t, device, data2, 0)
	flush(t, device)

	// Wait for segment writes to complete (async after upload).
	// With SegmentThreshold=1, each flush produces one segment.
	waitForSegments(t, ctx, store, 2)

	// Manually run GC
	stats := device.runGCCycle(ctx)

	// BlobsScanned counts unreferenced blobs (candidates for deletion)
	if stats.BlobsScanned != 1 {
		t.Errorf("Expected 1 blob scanned (unreferenced), got %d", stats.BlobsScanned)
	}
	if stats.BlobsDeleted != 1 {
		t.Errorf("Expected 1 blob deleted, got %d", stats.BlobsDeleted)
	}
	if stats.Errors != 0 {
		t.Errorf("Expected 0 errors, got %d", stats.Errors)
	}

	// Verify blob A was deleted
	blobsAfter, _ := store.List(ctx)
	for _, b := range blobsAfter {
		if b == blobA {
			t.Errorf("Blob A should have been deleted")
		}
	}
}

// TRIM/Hole tests

func trimAt(t *testing.T, device *Device, offset, length int64) {
	t.Helper()
	result := <-device.SubmitTrim(0, offset, length)
	if result.Err != nil {
		t.Fatalf("TrimAt offset %d length %d: %v", offset, length, result.Err)
	}
}

func TestDeviceTrimReturnsZeros(t *testing.T) {
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write data
	data := []byte("hello world, this is test data")
	writeAt(t, device, data, 100)

	// Verify data is there
	buf := make([]byte, len(data))
	readAt(t, device, buf, 100)
	if !bytes.Equal(buf, data) {
		t.Errorf("Before TRIM: got %q, want %q", buf, data)
	}

	// TRIM the region
	trimAt(t, device, 100, int64(len(data)))

	// Read should return zeros
	zeros := make([]byte, len(data))
	readAt(t, device, buf, 100)
	if !bytes.Equal(buf, zeros) {
		t.Errorf("After TRIM: got %q, want zeros", buf)
	}
}

func TestDeviceTrimPartialOverwrite(t *testing.T) {
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write 100 bytes of 'A' at offset 0
	data := bytes.Repeat([]byte("A"), 100)
	writeAt(t, device, data, 0)

	// TRIM middle portion (offset 25-75)
	trimAt(t, device, 25, 50)

	// Read entire region
	buf := make([]byte, 100)
	readAt(t, device, buf, 0)

	// First 25 bytes should be 'A'
	for i := 0; i < 25; i++ {
		if buf[i] != 'A' {
			t.Errorf("Byte %d: got %q, want 'A'", i, buf[i])
		}
	}
	// Middle 50 bytes should be zeros
	for i := 25; i < 75; i++ {
		if buf[i] != 0 {
			t.Errorf("Byte %d: got %d, want 0", i, buf[i])
		}
	}
	// Last 25 bytes should be 'A'
	for i := 75; i < 100; i++ {
		if buf[i] != 'A' {
			t.Errorf("Byte %d: got %q, want 'A'", i, buf[i])
		}
	}
}

func TestDeviceDataOverwritesHole(t *testing.T) {
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Create a hole at offset 100
	trimAt(t, device, 100, 50)

	// Verify it reads as zeros
	buf := make([]byte, 50)
	readAt(t, device, buf, 100)
	for i, b := range buf {
		if b != 0 {
			t.Errorf("Before overwrite: byte %d = %d, want 0", i, b)
		}
	}

	// Overwrite with data
	data := bytes.Repeat([]byte("X"), 50)
	writeAt(t, device, data, 100)

	// Read should return the data
	readAt(t, device, buf, 100)
	if !bytes.Equal(buf, data) {
		t.Errorf("After overwrite: got %q, want %q", buf, data)
	}
}

func TestDeviceTrimSurvivesFlush(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write data and flush
	data := bytes.Repeat([]byte("D"), 100)
	writeAt(t, device, data, 0)
	flush(t, device)

	// TRIM and flush
	trimAt(t, device, 0, 100)
	flush(t, device)

	// Verify blob was written with hole
	blobs, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(blobs) < 2 {
		t.Fatalf("Expected at least 2 blobs, got %d", len(blobs))
	}

	// Read should still return zeros
	buf := make([]byte, 100)
	readAt(t, device, buf, 0)
	for i, b := range buf {
		if b != 0 {
			t.Errorf("After flush: byte %d = %d, want 0", i, b)
		}
	}
}

func TestDeviceTrimSurvivesRecovery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	// First device: write data, TRIM, flush
	device1, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	if err := device1.Start(); err != nil {
		t.Fatalf("device1.Start: %v", err)
	}

	// Write data
	data := bytes.Repeat([]byte("R"), 100)
	writeAt(t, device1, data, 0)
	flush(t, device1)

	// TRIM
	trimAt(t, device1, 0, 100)
	flush(t, device1)

	device1.Stop(5 * time.Second)

	// Second device: recover and verify zeros
	device2, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice 2: %v", err)
	}
	if err := device2.Start(); err != nil {
		t.Fatalf("device2.Start: %v", err)
	}
	defer device2.Stop(5 * time.Second)

	if err := device2.LoadIndex(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Read should return zeros
	buf := make([]byte, 100)
	readAt(t, device2, buf, 0)
	for i, b := range buf {
		if b != 0 {
			t.Errorf("After recovery: byte %d = %d, want 0", i, b)
		}
	}
}

func TestDeviceTrimSurvivesColdRecovery(t *testing.T) {
	// Cold recovery: no segments - just S3 listing
	ctx := context.Background()
	store := NewMemoryBlobStore()

	// First device: write data, TRIM, flush
	device1, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	if err := device1.Start(); err != nil {
		t.Fatalf("device1.Start: %v", err)
	}

	// Write data
	data := bytes.Repeat([]byte("C"), 100)
	writeAt(t, device1, data, 0)
	flush(t, device1)

	// TRIM
	trimAt(t, device1, 0, 100)
	flush(t, device1)

	device1.Stop(5 * time.Second)

	// Second device: cold recovery (will fall back to S3 listing)
	device2, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice 2: %v", err)
	}
	if err := device2.Start(); err != nil {
		t.Fatalf("device2.Start: %v", err)
	}
	defer device2.Stop(5 * time.Second)

	if err := device2.LoadIndex(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Read should return zeros (hole should survive cold recovery)
	buf := make([]byte, 100)
	readAt(t, device2, buf, 0)
	for i, b := range buf {
		if b != 0 {
			t.Errorf("After cold recovery: byte %d = %d, want 0", i, b)
		}
	}
}

func TestDeviceReadSpanningHolesAndData(t *testing.T) {
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Create pattern: data(A) | hole | data(B) | hole | data(C)
	// 0-100: A, 100-200: hole, 200-300: B, 300-400: hole, 400-500: C
	writeAt(t, device, bytes.Repeat([]byte("A"), 100), 0)
	trimAt(t, device, 100, 100)
	writeAt(t, device, bytes.Repeat([]byte("B"), 100), 200)
	trimAt(t, device, 300, 100)
	writeAt(t, device, bytes.Repeat([]byte("C"), 100), 400)

	// Read entire range
	buf := make([]byte, 500)
	readAt(t, device, buf, 0)

	// Verify pattern
	for i := 0; i < 100; i++ {
		if buf[i] != 'A' {
			t.Errorf("Byte %d: got %q, want 'A'", i, buf[i])
		}
	}
	for i := 100; i < 200; i++ {
		if buf[i] != 0 {
			t.Errorf("Byte %d: got %d, want 0 (hole)", i, buf[i])
		}
	}
	for i := 200; i < 300; i++ {
		if buf[i] != 'B' {
			t.Errorf("Byte %d: got %q, want 'B'", i, buf[i])
		}
	}
	for i := 300; i < 400; i++ {
		if buf[i] != 0 {
			t.Errorf("Byte %d: got %d, want 0 (hole)", i, buf[i])
		}
	}
	for i := 400; i < 500; i++ {
		if buf[i] != 'C' {
			t.Errorf("Byte %d: got %q, want 'C'", i, buf[i])
		}
	}
}

func TestGCWithHoles(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:         store,
		Size:          1024 * 1024,
		MaxBufferSize:    4096,
		SegmentThreshold: 1,
		GCEnabled:        true,
		GCInterval:       100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write data at offset 0
	data := bytes.Repeat([]byte("G"), 1000)
	writeAt(t, device, data, 0)
	flush(t, device)

	blobsBefore, _ := store.List(ctx)
	if len(blobsBefore) != 1 {
		t.Fatalf("Expected 1 blob before TRIM, got %d", len(blobsBefore))
	}
	dataBlob := blobsBefore[0].ID

	// TRIM the entire region (makes the data blob unreferenced)
	trimAt(t, device, 0, 1000)
	flush(t, device)

	// Should now have 2 blobs (data + hole)
	blobsAfterTrim, _ := store.List(ctx)
	if len(blobsAfterTrim) != 2 {
		t.Fatalf("Expected 2 blobs after TRIM, got %d", len(blobsAfterTrim))
	}

	// Find the hole blob
	var holeBlob string
	for _, b := range blobsAfterTrim {
		if b.ID != dataBlob {
			holeBlob = b.ID
			break
		}
	}

	// Wait for GC
	time.Sleep(200 * time.Millisecond)

	// The original data blob should be deleted by GC
	// The hole blob should be KEPT (needed for recovery)
	blobsAfter, _ := store.List(ctx)

	dataFound := false
	holeFound := false
	for _, b := range blobsAfter {
		if b.ID == dataBlob {
			dataFound = true
		}
		if b.ID == holeBlob {
			holeFound = true
		}
	}

	if dataFound {
		t.Errorf("Data blob %s should have been GC'd after TRIM", dataBlob)
	}
	if !holeFound {
		t.Errorf("Hole blob %s should NOT be GC'd (needed for recovery)", holeBlob)
	}

	// Verify read still returns zeros
	buf := make([]byte, 1000)
	readAt(t, device, buf, 0)
	for i, b := range buf {
		if b != 0 {
			t.Errorf("Byte %d = %d, want 0", i, b)
		}
	}
}

func TestGCDeletesOverwrittenHoleBlob(t *testing.T) {
	// Test that a hole blob can be GC'd once it's fully overwritten by new data
	// Use manual GC to avoid race conditions
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:         store,
		Size:          1024 * 1024,
		MaxBufferSize:    4096,
		SegmentThreshold: 1,
		GCEnabled:        false, // Disable automatic GC
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Step 1: Write initial data
	data1 := bytes.Repeat([]byte("A"), 1000)
	writeAt(t, device, data1, 0)
	flush(t, device)

	blobsStep1, _ := store.List(ctx)
	if len(blobsStep1) != 1 {
		t.Fatalf("Step 1: Expected 1 blob, got %d", len(blobsStep1))
	}
	dataBlob := blobsStep1[0].ID

	// Step 2: TRIM to create a hole (data blob becomes unreferenced)
	trimAt(t, device, 0, 1000)
	flush(t, device)

	blobsStep2, _ := store.List(ctx)
	if len(blobsStep2) != 2 {
		t.Fatalf("Step 2: Expected 2 blobs, got %d", len(blobsStep2))
	}
	var holeBlob string
	for _, b := range blobsStep2 {
		if b.ID != dataBlob {
			holeBlob = b.ID
			break
		}
	}

	// Wait for segment writes before GC (segments are written async after upload)
	waitForSegments(t, ctx, store, 2)

	// Run GC manually - should delete data blob, keep hole blob
	stats := device.runGCCycle(ctx)
	if stats.BlobsDeleted != 1 {
		t.Fatalf("GC 1: Expected 1 blob deleted, got %d", stats.BlobsDeleted)
	}

	blobsAfterGC1, _ := store.List(ctx)
	if len(blobsAfterGC1) != 1 {
		t.Fatalf("After GC 1: Expected 1 blob (hole), got %d", len(blobsAfterGC1))
	}
	if blobsAfterGC1[0].ID != holeBlob {
		t.Fatalf("After GC 1: Expected hole blob %s, got %s", holeBlob, blobsAfterGC1[0].ID)
	}

	// Step 3: Overwrite the hole with new data
	data2 := bytes.Repeat([]byte("B"), 1000)
	writeAt(t, device, data2, 0)
	flush(t, device)

	blobsStep3, _ := store.List(ctx)
	if len(blobsStep3) != 2 {
		t.Fatalf("Step 3: Expected 2 blobs (hole + new data), got %d", len(blobsStep3))
	}
	var newDataBlob string
	for _, b := range blobsStep3 {
		if b.ID != holeBlob {
			newDataBlob = b.ID
			break
		}
	}

	// Wait for segment writes before GC
	waitForSegments(t, ctx, store, 3)

	// Run GC manually - should delete hole blob (now unreferenced), keep new data blob
	stats = device.runGCCycle(ctx)
	if stats.BlobsDeleted != 1 {
		t.Fatalf("GC 2: Expected 1 blob deleted, got %d", stats.BlobsDeleted)
	}

	blobsAfterGC2, _ := store.List(ctx)
	if len(blobsAfterGC2) != 1 {
		t.Fatalf("After GC 2: Expected 1 blob (new data), got %d", len(blobsAfterGC2))
	}
	if blobsAfterGC2[0].ID != newDataBlob {
		t.Fatalf("After GC 2: Expected new data blob %s, got %s", newDataBlob, blobsAfterGC2[0].ID)
	}

	// Verify read returns new data
	buf := make([]byte, 1000)
	readAt(t, device, buf, 0)
	if !bytes.Equal(buf, data2) {
		t.Errorf("Read returned wrong data")
	}
}

func TestGCWithMixedDataAndHoles(t *testing.T) {
	// Test GC with a blob containing both data and hole entries
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:         store,
		Size:          1024 * 1024,
		MaxBufferSize:    4096,
		SegmentThreshold: 1,
		GCEnabled:        true,
		GCInterval:       100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Write data at offset 0
	data := bytes.Repeat([]byte("M"), 500)
	writeAt(t, device, data, 0)

	// TRIM at offset 500 (same buffer, will create mixed blob)
	trimAt(t, device, 500, 500)

	// Flush - creates one blob with data[0-500] and hole[500-1000]
	flush(t, device)

	blobsBefore, _ := store.List(ctx)
	if len(blobsBefore) != 1 {
		t.Fatalf("Expected 1 blob with mixed data+hole, got %d", len(blobsBefore))
	}
	mixedBlob := blobsBefore[0].ID

	// Verify read works (data + zeros)
	buf := make([]byte, 1000)
	readAt(t, device, buf, 0)
	for i := 0; i < 500; i++ {
		if buf[i] != 'M' {
			t.Errorf("Byte %d = %q, want 'M'", i, buf[i])
		}
	}
	for i := 500; i < 1000; i++ {
		if buf[i] != 0 {
			t.Errorf("Byte %d = %d, want 0", i, buf[i])
		}
	}

	// Overwrite entire region with new data
	newData := bytes.Repeat([]byte("N"), 1000)
	writeAt(t, device, newData, 0)
	flush(t, device)

	// Wait for GC - mixed blob should be deleted
	time.Sleep(200 * time.Millisecond)

	blobsAfter, _ := store.List(ctx)
	for _, b := range blobsAfter {
		if b.ID == mixedBlob {
			t.Errorf("Mixed blob %s should have been GC'd", mixedBlob)
		}
	}

	// Verify read returns new data
	readAt(t, device, buf, 0)
	if !bytes.Equal(buf, newData) {
		t.Error("Read returned wrong data after GC")
	}
}

// TestGCDoesNotDeleteSupersededBlobWhileReplacementUploading verifies that a blob
// is not garbage collected while its replacement is still uploading to S3.
// This tests the critical invariant that InsertBlob() is only called AFTER
// store.Put() succeeds, ensuring liveness only changes after the new blob is safely stored.
func TestGCDoesNotDeleteSupersededBlobWhileReplacementUploading(t *testing.T) {
	ctx := context.Background()
	baseStore := &MemoryBlobStore{blobs: make(map[string][]byte)}
	blockingStore := newBlockingBlobStore(NewConcurrentBlobStore(baseStore, DefaultMaxConcurrentFetches))

	device, err := NewDevice(DeviceConfig{
		Store:         blockingStore,
		Size:          1024 * 1024,
		MaxBufferSize:    4096,
		SegmentThreshold: 1,
		GCEnabled:        false, // We'll run GC manually
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Step 1: Write data A and flush (need to unblock the upload)
	dataA := bytes.Repeat([]byte("A"), 1000)
	writeAt(t, device, dataA, 0)

	// Start flush in goroutine since it will block
	flushADone := make(chan struct{})
	go func() {
		result := <-device.SubmitFlush(0)
		if result.Err != nil {
			t.Errorf("Flush A failed: %v", result.Err)
		}
		close(flushADone)
	}()

	// Wait for blob A's Put to start, then unblock it
	blobAID := blockingStore.waitForPut(t)
	t.Logf("Blob A (%s) upload started, unblocking", blobAID)
	blockingStore.unblock(blobAID)

	// Wait for flush A to complete
	select {
	case <-flushADone:
		t.Log("Blob A upload completed")
	case <-time.After(5 * time.Second):
		t.Fatal("Flush A timed out")
	}

	// Verify blob A exists in store
	blobsAfterA, _ := baseStore.List(ctx)
	if len(blobsAfterA) != 1 {
		t.Fatalf("Expected 1 blob after A, got %d", len(blobsAfterA))
	}
	if blobsAfterA[0].ID != blobAID {
		t.Fatalf("Expected blob A (%s), got %s", blobAID, blobsAfterA[0].ID)
	}

	// Step 2: Write data B (supersedes A) but block the upload
	dataB := bytes.Repeat([]byte("B"), 1000)
	writeAt(t, device, dataB, 0)

	// Start flush in goroutine - this will block on Put
	flushBDone := make(chan struct{})
	go func() {
		result := <-device.SubmitFlush(0)
		if result.Err != nil {
			t.Errorf("Flush B failed: %v", result.Err)
		}
		close(flushBDone)
	}()

	// Wait for blob B's Put to start (now it's blocked)
	blobBID := blockingStore.waitForPut(t)
	t.Logf("Blob B (%s) upload started but blocked", blobBID)

	// Wait for segment covering blob A before running GC
	waitForSegments(t, ctx, baseStore, 1)

	// Step 3: Run GC while blob B is still uploading
	// At this point:
	// - Blob B's Put has started but is blocked (not in S3 yet)
	// - InsertBlob(B) has NOT been called (happens after Put succeeds)
	// - Blob A should still be referenced in the index
	// - GC should NOT delete blob A
	stats := device.runGCCycle(ctx)

	if stats.BlobsDeleted != 0 {
		t.Errorf("GC deleted %d blobs while replacement was uploading, expected 0", stats.BlobsDeleted)
	}

	// Verify blob A still exists in S3
	blobsBeforeUnblock, _ := baseStore.List(ctx)
	foundA := false
	for _, b := range blobsBeforeUnblock {
		if b.ID == blobAID {
			foundA = true
			break
		}
	}
	if !foundA {
		t.Fatalf("Blob A (%s) was deleted while blob B was still uploading", blobAID)
	}
	t.Log("Blob A still exists (correctly protected while B uploads)")

	// Step 4: Unblock blob B's upload and wait for completion
	blockingStore.unblock(blobBID)
	select {
	case <-flushBDone:
		t.Log("Blob B upload completed")
	case <-time.After(5 * time.Second):
		t.Fatal("Flush B timed out")
	}

	// Wait for segment covering blob B before running GC
	waitForSegments(t, ctx, baseStore, 2)

	// Step 5: Run GC again - now blob A should be deleted
	stats = device.runGCCycle(ctx)

	if stats.BlobsDeleted != 1 {
		t.Errorf("GC deleted %d blobs after replacement completed, expected 1", stats.BlobsDeleted)
	}

	// Verify blob A is gone and blob B remains
	blobsAfterGC, _ := baseStore.List(ctx)
	if len(blobsAfterGC) != 1 {
		t.Errorf("Expected 1 blob after GC, got %d", len(blobsAfterGC))
	}
	if len(blobsAfterGC) > 0 && blobsAfterGC[0].ID != blobBID {
		t.Errorf("Expected blob B (%s) to remain, got %s", blobBID, blobsAfterGC[0].ID)
	}

	// Verify data is correct (should be B's data)
	buf := make([]byte, 1000)
	readAt(t, device, buf, 0)
	if !bytes.Equal(buf, dataB) {
		t.Errorf("Data mismatch: expected B's data")
	}
}

// TestGCTargetedProtection verifies that GC can delete blobs superseded by
// uploaded blobs, even while there's buffered data for an UNRELATED range.
// This tests the targeted protection mechanism vs blanket GC disabling.
//
// Timeline:
// 1. Blob A uploaded at offset 0
// 2. Blob B uploaded at offset 0 (supersedes A)
// 3. Data C written at offset 10000 (stays in buffer, NOT flushed)
// 4. GC runs while C is buffered
// 5. A should be deleted (superseded by uploaded B, C is unrelated)
func TestGCTargetedProtection(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryBlobStore()

	device, err := NewDevice(DeviceConfig{
		Store:         store,
		Size:          1024 * 1024,
		MaxBufferSize:    4096,
		SegmentThreshold: 1,
		GCEnabled:        false, // Manual GC
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Step 1: Write and upload blob A at offset 0
	dataA := bytes.Repeat([]byte("A"), 1000)
	writeAt(t, device, dataA, 0)
	flush(t, device)
	blobsAfterA, _ := store.List(ctx)
	if len(blobsAfterA) != 1 {
		t.Fatalf("Expected 1 blob after A, got %d", len(blobsAfterA))
	}
	blobAID := blobsAfterA[0].ID
	t.Logf("Blob A (%s) uploaded at offset 0", blobAID)

	// Step 2: Write and upload blob B at offset 0 (supersedes A)
	dataB := bytes.Repeat([]byte("B"), 1000)
	writeAt(t, device, dataB, 0)
	flush(t, device)
	blobsAfterB, _ := store.List(ctx)
	if len(blobsAfterB) != 2 {
		t.Fatalf("Expected 2 blobs after B, got %d", len(blobsAfterB))
	}
	var blobBID string
	for _, b := range blobsAfterB {
		if b.ID != blobAID {
			blobBID = b.ID
			break
		}
	}
	t.Logf("Blob B (%s) uploaded at offset 0 (supersedes A)", blobBID)

	// Step 3: Write data C at offset 10000 but DON'T flush - stays in buffer
	dataC := bytes.Repeat([]byte("C"), 1000)
	writeAt(t, device, dataC, 10000)
	t.Log("Data C written at offset 10000, staying in buffer (not flushed)")

	// Wait for segment writes before GC
	waitForSegments(t, ctx, store, 2)

	// At this point:
	// - Blob A is unreferenced (superseded by uploaded blob B)
	// - Blob B is referenced at offset 0
	// - Buffer entry exists at offset 10000 (C's data, not yet uploaded)
	// - currentBufferProtections should be empty (C doesn't supersede any real blob)
	// - A should be deletable because it was superseded by B (uploaded),
	//   not by any buffer entry

	// Run GC - should delete blob A
	stats := device.runGCCycle(ctx)

	if stats.BlobsDeleted != 1 {
		t.Errorf("GC deleted %d blobs, expected 1 (blob A)", stats.BlobsDeleted)
	}

	// Verify blob A is gone
	blobsAfterGC, _ := store.List(ctx)
	if len(blobsAfterGC) != 1 {
		t.Errorf("Expected 1 blob after GC, got %d", len(blobsAfterGC))
	}
	for _, b := range blobsAfterGC {
		if b.ID == blobAID {
			t.Errorf("Blob A should have been deleted by GC")
		}
	}
	t.Log("GC correctly deleted blob A while unrelated buffer entry exists")
}

func TestIndexHoleBlobsAreReferenced(t *testing.T) {
	idx := NewRangeIndex()

	// Insert a data range and mark blob as known
	idx.Insert(RangeEntry{
		Offset:     0,
		Length:     100,
		BlobID:     "data-blob",
		BlobOffset: 0,
		IsHole:     false,
	})
	idx.mu.Lock()
	idx.knownBlobs["data-blob"] = true
	idx.mu.Unlock()

	// Insert a hole range - hole blobs must be kept for recovery
	idx.Insert(RangeEntry{
		Offset:     100,
		Length:     100,
		BlobID:     "hole-blob",
		BlobOffset: 0,
		IsHole:     true,
	})
	idx.mu.Lock()
	idx.knownBlobs["hole-blob"] = true
	idx.mu.Unlock()

	// Both should be referenced - GetUnreferencedBlobs should return empty
	unreferenced := idx.GetUnreferencedBlobs()
	if len(unreferenced) != 0 {
		t.Errorf("Expected 0 unreferenced blobs, got %d: %v", len(unreferenced), unreferenced)
	}

	// Overwrite the data blob completely
	idx.Insert(RangeEntry{
		Offset:     0,
		Length:     100,
		BlobID:     "new-blob",
		BlobOffset: 0,
	})
	idx.mu.Lock()
	idx.knownBlobs["new-blob"] = true
	idx.mu.Unlock()

	// Now data-blob should be unreferenced, but hole-blob should still be referenced
	unreferenced = idx.GetUnreferencedBlobs()
	if len(unreferenced) != 1 {
		t.Errorf("Expected 1 unreferenced blob, got %d: %v", len(unreferenced), unreferenced)
	}
	if len(unreferenced) > 0 && unreferenced[0] != "data-blob" {
		t.Errorf("Expected 'data-blob' to be unreferenced, got %v", unreferenced)
	}
}

// blockingBlobStore wraps a BlobStore and blocks Put operations until explicitly unblocked.
type blockingBlobStore struct {
	BlobStore
	mu        sync.Mutex
	blockers  map[string]chan struct{} // blob ID -> channel that blocks until closed
	doneChans map[string]chan struct{} // blob ID -> closed when Put returns
	puts      chan string              // notifies when a Put starts (sends blob ID)
}

func newBlockingBlobStore(base BlobStore) *blockingBlobStore {
	return &blockingBlobStore{
		BlobStore: base,
		blockers:  make(map[string]chan struct{}),
		doneChans: make(map[string]chan struct{}),
		puts:      make(chan string, 100),
	}
}

func (s *blockingBlobStore) Put(ctx context.Context, id string, body io.ReadSeeker, length int64) error {
	s.mu.Lock()
	blocker, exists := s.blockers[id]
	if !exists {
		// Create a new blocker for this blob
		blocker = make(chan struct{})
		s.blockers[id] = blocker
	}
	done := make(chan struct{})
	s.doneChans[id] = done
	s.mu.Unlock()

	// Notify that Put started
	select {
	case s.puts <- id:
	default:
	}

	// Block until unblocked or context cancelled
	select {
	case <-blocker:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Rewind the reader since it may have been partially read before blocking
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return err
	}

	err := s.BlobStore.Put(ctx, id, body, length)
	close(done) // Signal that Put has returned
	return err
}

// unblock allows a specific blob's Put to proceed
func (s *blockingBlobStore) unblock(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, exists := s.blockers[id]; exists {
		close(ch)
		delete(s.blockers, id)
	}
}

// unblockAll allows all pending Puts to proceed
func (s *blockingBlobStore) unblockAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, ch := range s.blockers {
		close(ch)
		delete(s.blockers, id)
	}
}

// waitForPutDone waits for a specific blob's Put to complete (return).
func (s *blockingBlobStore) waitForPutDone(t *testing.T, id string) {
	t.Helper()
	s.mu.Lock()
	done := s.doneChans[id]
	s.mu.Unlock()

	if done == nil {
		t.Fatalf("No done channel for blob %s", id)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for Put %s to complete", id)
	}
}

// waitForPut waits for a Put to start and returns the blob ID
func (s *blockingBlobStore) waitForPut(t *testing.T) string {
	t.Helper()
	select {
	case id := <-s.puts:
		return id
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for Put to start")
		return ""
	}
}

// TestFUADoesNotWaitForPriorUploads verifies that FUA writes complete
// as soon as THIS write is uploaded, without waiting for prior writes.
func TestFUADoesNotWaitForPriorUploads(t *testing.T) {
	baseStore := &MemoryBlobStore{blobs: make(map[string][]byte)}
	blockingStore := newBlockingBlobStore(NewConcurrentBlobStore(baseStore, DefaultMaxConcurrentFetches))

	device, err := NewDevice(DeviceConfig{
		Store:               blockingStore,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512, // Force each write to be "large" and create a blob
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Submit a regular write - this returns immediately (doesn't wait for upload)
	regularData := bytes.Repeat([]byte("A"), 1024)
	regularResult := <-device.SubmitWrite(1, regularData, 0)
	if regularResult.Err != nil {
		t.Fatalf("Regular write failed: %v", regularResult.Err)
	}

	// Wait for the regular write's Put to start (so it's blocked in upload)
	regularBlobID := blockingStore.waitForPut(t)
	t.Logf("Regular write blob %s is blocked on upload", regularBlobID)

	// Submit a FUA write - this should wait for its own upload
	fuaData := bytes.Repeat([]byte("B"), 1024)
	fuaWriteCh := device.SubmitWriteFUA(2, fuaData, 4096)

	// Wait for the FUA write's Put to start
	fuaBlobID := blockingStore.waitForPut(t)
	t.Logf("FUA write blob %s is blocked on upload", fuaBlobID)

	// Verify FUA is not yet complete (still blocked on its upload)
	select {
	case <-fuaWriteCh:
		t.Fatal("FUA completed before its blob was unblocked")
	default:
		// Good - FUA is still waiting for its upload
	}

	// Unblock ONLY the FUA blob - the regular write's blob is still blocked
	blockingStore.unblock(fuaBlobID)

	// FUA should now complete (it only waited for its own upload)
	select {
	case fuaResult := <-fuaWriteCh:
		if fuaResult.Err != nil {
			t.Fatalf("FUA write failed: %v", fuaResult.Err)
		}
		t.Log("FUA write completed after its blob was unblocked")
	case <-time.After(5 * time.Second):
		t.Fatal("FUA write did not complete after unblocking its blob")
	}

	// Verify the regular blob upload is still blocked (proves FUA didn't wait for it)
	blockingStore.mu.Lock()
	_, regularStillBlocked := blockingStore.blockers[regularBlobID]
	blockingStore.mu.Unlock()
	if !regularStillBlocked {
		t.Error("Regular blob upload should still be blocked")
	} else {
		t.Log("Regular blob upload is still blocked (FUA didn't wait for it)")
	}

	// Clean up: unblock the regular write's upload
	blockingStore.unblock(regularBlobID)
}

// TestFUADoesNotBlockSubsequentWrites verifies that a pending FUA write
// does not block subsequent non-FUA writes from completing.
func TestFUADoesNotBlockSubsequentWrites(t *testing.T) {
	baseStore := &MemoryBlobStore{blobs: make(map[string][]byte)}
	blockingStore := newBlockingBlobStore(NewConcurrentBlobStore(baseStore, DefaultMaxConcurrentFetches))

	device, err := NewDevice(DeviceConfig{
		Store:               blockingStore,
		Size:                1024 * 1024,
		LargeWriteThreshold: 512, // Force each write to be "large" and create a blob
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Submit a FUA write - this will block waiting for its upload
	fuaData := bytes.Repeat([]byte("A"), 1024)
	fuaWriteCh := device.SubmitWriteFUA(1, fuaData, 0)

	// Wait for the FUA write's Put to start (so it's blocked)
	fuaBlobID := blockingStore.waitForPut(t)
	t.Logf("FUA write blob %s is blocked on upload", fuaBlobID)

	// Verify FUA is blocked
	select {
	case <-fuaWriteCh:
		t.Fatal("FUA completed before its blob was unblocked")
	default:
		// Good - FUA is waiting
	}

	// Submit a regular write - this should complete immediately, not blocked by FUA
	regularData := bytes.Repeat([]byte("B"), 1024)
	regularResult := <-device.SubmitWrite(2, regularData, 4096)
	if regularResult.Err != nil {
		t.Fatalf("Regular write failed: %v", regularResult.Err)
	}
	t.Log("Regular write completed immediately (not blocked by pending FUA)")

	// FUA should still be blocked
	select {
	case <-fuaWriteCh:
		t.Fatal("FUA completed unexpectedly")
	default:
		t.Log("FUA write is still blocked (as expected)")
	}

	// Clean up: unblock the FUA and regular blob uploads
	blockingStore.unblockAll()
	<-fuaWriteCh
}

// TestFUAWithAutoSealBug exposes the bug where FUA returns early when Add() auto-seals.
// When a write pushes the buffer over the threshold, Add() seals and enqueues the buffer.
// Then SealAndEnqueue() sees an empty buffer and returns nil.
// The buggy code returns success immediately without waiting for the upload.
func TestFUAWithAutoSealBug(t *testing.T) {
	baseStore := &MemoryBlobStore{blobs: make(map[string][]byte)}
	blockingStore := newBlockingBlobStore(NewConcurrentBlobStore(baseStore, DefaultMaxConcurrentFetches))

	// Use a small MaxBufferSize to trigger auto-seal in Add()
	device, err := NewDevice(DeviceConfig{
		Store:               blockingStore,
		Size:                1024 * 1024,
		MaxBufferSize:       512,         // Small buffer - auto-seals when exceeded
		LargeWriteThreshold: 1024 * 1024, // Large threshold so writes go through buffering
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Submit a regular write to partially fill the buffer (400 bytes)
	regularData := bytes.Repeat([]byte("A"), 400)
	regularResult := <-device.SubmitWrite(1, regularData, 0)
	if regularResult.Err != nil {
		t.Fatalf("Regular write failed: %v", regularResult.Err)
	}

	// Submit a FUA write that pushes over the threshold (200 bytes)
	// This will cause Add() to auto-seal the buffer (400 + 200 = 600 > 512)
	fuaData := bytes.Repeat([]byte("B"), 200)
	fuaWriteCh := device.SubmitWriteFUA(2, fuaData, 4096)

	// Wait for the blob upload to start
	blobID := blockingStore.waitForPut(t)
	t.Logf("Blob %s is blocked on upload (auto-sealed by Add)", blobID)

	// BUG: With the current code, FUA completes immediately because
	// SealAndEnqueue() returns nil (buffer was already sealed by Add)
	select {
	case <-fuaWriteCh:
		t.Fatal("BUG: FUA completed before its blob was uploaded!")
	default:
		t.Log("Good: FUA is correctly waiting for upload")
	}

	// Unblock the upload
	blockingStore.unblock(blobID)

	// FUA should now complete
	select {
	case fuaResult := <-fuaWriteCh:
		if fuaResult.Err != nil {
			t.Fatalf("FUA write failed: %v", fuaResult.Err)
		}
		t.Log("FUA completed after blob was uploaded")
	case <-time.After(5 * time.Second):
		t.Fatal("FUA did not complete after unblocking blob")
	}
}

// TestOutOfOrderBlobUploads verifies that when two blobs affecting the same
// offset complete uploading out of order, the index reflects the NEWER data.
//
// Timeline:
// 1. Write A at offset 0, flush (blob A starts uploading but blocked)
// 2. Write B at offset 0, flush (blob B starts uploading but blocked)
// 3. Unblock blob B first -> InsertBlob(B) runs
// 4. Unblock blob A -> InsertBlob(A) runs
// 5. Read offset 0 -> should return data B (newer), not data A (older)
//
// Note: Flush is a barrier, so flush B waits for both A and B. We unblock B first
// so that InsertBlob(B) runs before InsertBlob(A), then unblock A so both complete.
func TestOutOfOrderBlobUploads(t *testing.T) {
	baseStore := &MemoryBlobStore{blobs: make(map[string][]byte)}
	blockingStore := newBlockingBlobStore(NewConcurrentBlobStore(baseStore, DefaultMaxConcurrentFetches))

	device, err := NewDevice(DeviceConfig{
		Store:         blockingStore,
		Size:          1024 * 1024,
		MaxBufferSize: 4096,
		GCEnabled:     false,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	mustStart(t, device)

	// Step 1: Write data A and start flush (will block)
	dataA := bytes.Repeat([]byte("A"), 1000)
	writeAt(t, device, dataA, 0)

	flushADone := make(chan struct{})
	go func() {
		result := <-device.SubmitFlush(0)
		if result.Err != nil {
			t.Errorf("Flush A failed: %v", result.Err)
		}
		close(flushADone)
	}()

	blobAID := blockingStore.waitForPut(t)
	t.Logf("Blob A (%s) upload started but blocked", blobAID)

	// Step 2: Write data B and start flush (will also block)
	dataB := bytes.Repeat([]byte("B"), 1000)
	writeAt(t, device, dataB, 0)

	flushBDone := make(chan struct{})
	go func() {
		result := <-device.SubmitFlush(0)
		if result.Err != nil {
			t.Errorf("Flush B failed: %v", result.Err)
		}
		close(flushBDone)
	}()

	blobBID := blockingStore.waitForPut(t)
	t.Logf("Blob B (%s) upload started but blocked", blobBID)

	// Step 3: Unblock blob B FIRST (out of order!)
	// This causes InsertBlob(B) to run before InsertBlob(A)
	t.Log("Unblocking blob B first (out of order)")
	blockingStore.unblock(blobBID)

	// Wait for B's Put to complete (and InsertBlob(B) to run)
	blockingStore.waitForPutDone(t, blobBID)

	// Step 4: Unblock blob A - this causes InsertBlob(A) to run AFTER InsertBlob(B)
	t.Log("Unblocking blob A")
	blockingStore.unblock(blobAID)

	// Both flushes should now complete
	select {
	case <-flushADone:
		t.Log("Flush A completed")
	case <-time.After(5 * time.Second):
		t.Fatal("Flush A timed out")
	}

	select {
	case <-flushBDone:
		t.Log("Flush B completed")
	case <-time.After(5 * time.Second):
		t.Fatal("Flush B timed out")
	}

	// Step 5: Read offset 0 - should get data B (newer write)
	result := <-device.SubmitRead(0, 1000, 0)
	if result.Err != nil {
		t.Fatalf("Read failed: %v", result.Err)
	}

	if bytes.Equal(result.Data, dataA) {
		t.Errorf("Read returned data A (older) instead of data B (newer) - OUT OF ORDER BUG!")
	} else if bytes.Equal(result.Data, dataB) {
		t.Log("Read correctly returned data B (newer)")
	} else {
		t.Errorf("Read returned unexpected data: first byte is %c", result.Data[0])
	}
}
