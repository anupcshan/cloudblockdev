package cloudblock

// Baseline benchmark results (2s benchtime, AMD Ryzen 5 3600):
//
// BenchmarkSequentialReads4KB        586,740      4,079 ns/op  1,004.21 MB/s    12 allocs/op
// BenchmarkSequentialReads128KB       78,342     30,069 ns/op  4,359.06 MB/s    12 allocs/op
// BenchmarkSequentialWrites1B      1,578,681      1,512 ns/op      0.66 MB/s     9 allocs/op
// BenchmarkRandomWrites1B          1,000,000      2,905 ns/op      0.34 MB/s     9 allocs/op
// BenchmarkRandomReads1B             682,362      3,565 ns/op      0.28 MB/s    12 allocs/op
import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkSequentialReads4KB(b *testing.B) {
	benchmarkSequentialReads(b, 4*1024)
}

func BenchmarkSequentialReads128KB(b *testing.B) {
	benchmarkSequentialReads(b, 128*1024)
}

func BenchmarkSequentialWrites1B(b *testing.B) {
	b.ReportAllocs()
	store := NewMemoryBlobStore()

	deviceSize := int64(32 * 1024 * 1024) // 32MB device
	device, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  deviceSize,
	})
	if err != nil {
		b.Fatalf("NewDevice: %v", err)
	}
	if err := device.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer device.Stop(5 * time.Second)

	// 1 byte blocks with 2 byte stride to prevent coalescing
	blockSize := 1
	stride := 2

	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	offset := int64(0)
	for i := 0; i < b.N; i++ {
		data := []byte{0x42} // fresh allocation per iteration (SubmitWrite takes ownership)
		result := <-device.SubmitWrite(0, data, offset)
		if result.Err != nil {
			b.Fatalf("Write at %d: %v", offset, result.Err)
		}
		offset = (offset + int64(stride)) % deviceSize
	}
}

func BenchmarkRandomWrites1B(b *testing.B) {
	b.ReportAllocs()
	store := NewMemoryBlobStore()

	deviceSize := int64(32 * 1024 * 1024) // 32MB device
	device, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  deviceSize,
	})
	if err != nil {
		b.Fatalf("NewDevice: %v", err)
	}
	if err := device.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer device.Stop(5 * time.Second)

	blockSize := 1

	// Deterministic random source for reproducibility
	rng := rand.New(rand.NewSource(42))

	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := []byte{0x42} // fresh allocation per iteration (SubmitWrite takes ownership)
		offset := rng.Int63n(deviceSize)
		result := <-device.SubmitWrite(0, data, offset)
		if result.Err != nil {
			b.Fatalf("Write at %d: %v", offset, result.Err)
		}
	}
}

func BenchmarkRandomReads1B(b *testing.B) {
	b.ReportAllocs()
	store := NewMemoryBlobStore()

	deviceSize := int64(128 * 1024) // 128KB device (small for fast setup)
	device, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  deviceSize,
	})
	if err != nil {
		b.Fatalf("NewDevice: %v", err)
	}
	if err := device.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer device.Stop(5 * time.Second)

	// Pre-populate device with data (every 2 bytes to prevent coalescing)
	blockSize := 1
	stride := 2
	for offset := int64(0); offset < deviceSize; offset += int64(stride) {
		data := []byte{0x42} // fresh allocation (SubmitWrite takes ownership)
		result := <-device.SubmitWrite(0, data, offset)
		if result.Err != nil {
			b.Fatalf("Write at %d: %v", offset, result.Err)
		}
	}

	// Flush to ensure data is in the store
	if result := <-device.SubmitFlush(0); result.Err != nil {
		b.Fatalf("Flush: %v", result.Err)
	}

	// Deterministic random source for reproducibility
	rng := rand.New(rand.NewSource(42))

	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Random offset aligned to stride (so we read valid data)
		offset := rng.Int63n(deviceSize/int64(stride)) * int64(stride)
		result := <-device.SubmitRead(0, blockSize, offset)
		if result.Err != nil {
			b.Fatalf("Read at %d: %v", offset, result.Err)
		}
	}
}

func benchmarkSequentialReads(b *testing.B, blockSize int) {
	b.ReportAllocs()
	store := NewMemoryBlobStore()

	// Use 2x stride to prevent coalescing of adjacent writes
	stride := blockSize * 2

	deviceSize := int64(256 * 1024 * 1024) // 256MB
	device, err := NewDevice(DeviceConfig{
		Store: store,
		Size:  deviceSize,
	})
	if err != nil {
		b.Fatalf("NewDevice: %v", err)
	}
	if err := device.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer device.Stop(5 * time.Second)

	// Pre-populate with data using the specified block size and stride
	for offset := int64(0); offset < deviceSize; offset += int64(stride) {
		data := make([]byte, blockSize) // fresh allocation (SubmitWrite takes ownership)
		for i := range data {
			data[i] = byte(i % 256)
		}
		result := <-device.SubmitWrite(0, data, offset)
		if result.Err != nil {
			b.Fatalf("Write at %d: %v", offset, result.Err)
		}
	}

	// Flush to store
	result := <-device.SubmitFlush(0)
	if result.Err != nil {
		b.Fatalf("Flush: %v", result.Err)
	}

	// Benchmark sequential reads
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	offset := int64(0)
	for i := 0; i < b.N; i++ {
		result := <-device.SubmitRead(0, blockSize, offset)
		if result.Err != nil {
			b.Fatalf("Read at %d: %v", offset, result.Err)
		}
		offset = (offset + int64(stride)) % deviceSize
	}
}
