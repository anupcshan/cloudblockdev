package nbd

import (
	"os"
	"sync"
	"time"
)

// FileDevice is a simple block device backed directly by a file.
// Unlike cloudblock.Device, it doesn't use chunking - it reads/writes
// directly at the requested offsets.
type FileDevice struct {
	file *os.File
	size int64
	mu   sync.RWMutex
}

// FileDeviceConfig holds configuration for creating a FileDevice.
type FileDeviceConfig struct {
	// Path is the path to the backing file.
	Path string
	// Size is the device size in bytes. If the file doesn't exist,
	// it will be created as a sparse file with this size.
	Size int64
}

// NewFileDevice creates a new file-backed block device.
func NewFileDevice(cfg FileDeviceConfig) (*FileDevice, error) {
	var file *os.File
	var err error

	// Check if file exists
	info, statErr := os.Stat(cfg.Path)
	if os.IsNotExist(statErr) {
		// Create new sparse file
		file, err = os.OpenFile(cfg.Path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		// Truncate to desired size (creates a sparse file)
		if err := file.Truncate(cfg.Size); err != nil {
			file.Close()
			return nil, err
		}
		return &FileDevice{file: file, size: cfg.Size}, nil
	}

	// Open existing file
	file, err = os.OpenFile(cfg.Path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Use existing file size
	return &FileDevice{file: file, size: info.Size()}, nil
}

// Size returns the device size in bytes.
func (d *FileDevice) Size() int64 {
	return d.size
}

// Start is a no-op for FileDevice (no background goroutines needed).
func (d *FileDevice) Start() error {
	return nil
}

// Stop is a no-op for FileDevice.
func (d *FileDevice) Stop(timeout time.Duration) error {
	return nil
}

// SubmitRead queues a read operation.
// For FileDevice, this executes synchronously and returns immediately.
func (d *FileDevice) SubmitRead(handle uint64, length int, offset int64) <-chan OpResult {
	result := make(chan OpResult, 1)

	go func() {
		d.mu.RLock()
		defer d.mu.RUnlock()

		data := make([]byte, length)
		n, err := d.file.ReadAt(data, offset)
		result <- OpResult{Data: data, N: n, Err: err}
	}()

	return result
}

// SubmitWrite queues a write operation.
// For FileDevice, this executes synchronously and returns immediately.
func (d *FileDevice) SubmitWrite(handle uint64, data []byte, offset int64) <-chan OpResult {
	result := make(chan OpResult, 1)

	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		n, err := d.file.WriteAt(data, offset)
		result <- OpResult{N: n, Err: err}
	}()

	return result
}

// SubmitWriteFUA queues a write with Force Unit Access.
// For FileDevice, this writes and then syncs to ensure durability.
func (d *FileDevice) SubmitWriteFUA(handle uint64, data []byte, offset int64) <-chan OpResult {
	result := make(chan OpResult, 1)

	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		n, err := d.file.WriteAt(data, offset)
		if err != nil {
			result <- OpResult{N: n, Err: err}
			return
		}
		// Sync to ensure durability
		err = d.file.Sync()
		result <- OpResult{N: n, Err: err}
	}()

	return result
}

// SubmitFlush queues a flush operation.
// For FileDevice, this syncs the file to disk.
func (d *FileDevice) SubmitFlush(handle uint64) <-chan OpResult {
	result := make(chan OpResult, 1)

	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		err := d.file.Sync()
		result <- OpResult{Err: err}
	}()

	return result
}

// SubmitTrim queues a trim/discard operation.
// For FileDevice, this is a no-op (could use fallocate PUNCH_HOLE on Linux).
func (d *FileDevice) SubmitTrim(handle uint64, offset, length int64) <-chan OpResult {
	result := make(chan OpResult, 1)
	result <- OpResult{}
	return result
}

// SubmitTrimFUA queues a trim with Force Unit Access.
// For FileDevice, this syncs to ensure any previous writes are durable.
func (d *FileDevice) SubmitTrimFUA(handle uint64, offset, length int64) <-chan OpResult {
	result := make(chan OpResult, 1)

	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		// Sync to ensure durability
		err := d.file.Sync()
		result <- OpResult{Err: err}
	}()

	return result
}

// Close closes the backing file.
func (d *FileDevice) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}
