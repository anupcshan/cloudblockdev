package cloudblock

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// FileBlobStore implements BlobStore using the local filesystem.
// All files are stored in a single flat directory.
// This is an alternative to S3BlobStore for testing and trace recording.
type FileBlobStore struct {
	dir     string
	latency time.Duration
}

// FileBlobStoreConfig holds configuration for FileBlobStore.
type FileBlobStoreConfig struct {
	Dir     string        // directory to store files in
	Latency time.Duration // injected delay per read/write op (0 = none)
}

// NewFileBlobStore creates a new filesystem-backed blob store.
// Creates the directory if it doesn't exist.
// The returned store is wrapped with concurrency limiting.
func NewFileBlobStore(cfg FileBlobStoreConfig) (BlobStore, error) {
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("create filestore dir: %w", err)
	}
	return NewConcurrentBlobStore(&FileBlobStore{
		dir:     cfg.Dir,
		latency: cfg.Latency,
	}, DefaultMaxConcurrentFetches), nil
}

// waitLatency waits until the timer expires or the context is cancelled.
// Returns ctx.Err() if the context was cancelled, nil otherwise.
func (f *FileBlobStore) waitLatency(ctx context.Context, timer *time.Timer) error {
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	}
}

// blobPath returns the filesystem path for a blob ID.
func (f *FileBlobStore) blobPath(id string) string {
	return filepath.Join(f.dir, id+".blob")
}

// keyPath returns the filesystem path for a raw key.
// Slashes in the key are replaced with underscores to keep the directory flat.
func (f *FileBlobStore) keyPath(key string) string {
	return filepath.Join(f.dir, strings.ReplaceAll(key, "/", "_"))
}

func (f *FileBlobStore) Put(ctx context.Context, id string, body io.ReadSeeker, length int64) error {
	var timer *time.Timer
	if f.latency > 0 {
		timer = time.NewTimer(f.latency)
		defer timer.Stop()
	}

	data, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	if err := os.WriteFile(f.blobPath(id), data, 0644); err != nil {
		return fmt.Errorf("write blob %s: %w", id, err)
	}

	if timer != nil {
		return f.waitLatency(ctx, timer)
	}
	return nil
}

func (f *FileBlobStore) Get(ctx context.Context, id string, p []byte, off int64) (int, error) {
	var timer *time.Timer
	if f.latency > 0 {
		timer = time.NewTimer(f.latency)
		defer timer.Stop()
	}

	file, err := os.Open(f.blobPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrBlobNotFound
		}
		return 0, fmt.Errorf("open blob %s: %w", id, err)
	}
	defer file.Close()

	n, err := file.ReadAt(p, off)
	if err != nil && err != io.EOF {
		return n, fmt.Errorf("read blob %s: %w", id, err)
	}

	if timer != nil {
		if waitErr := f.waitLatency(ctx, timer); waitErr != nil {
			return n, waitErr
		}
	}
	return n, nil
}

func (f *FileBlobStore) List(ctx context.Context) ([]BlobInfo, error) {
	return f.ListAfter(ctx, 0)
}

func (f *FileBlobStore) ListAfter(ctx context.Context, afterTimestamp int64) ([]BlobInfo, error) {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}

	var infos []BlobInfo
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".blob") {
			continue
		}
		id := strings.TrimSuffix(name, ".blob")

		if afterTimestamp > 0 {
			ts, err := ParseBlobIDTimestamp(id)
			if err != nil || int64(ts) <= afterTimestamp {
				continue
			}
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}
		infos = append(infos, BlobInfo{ID: id, Size: info.Size()})
	}

	sort.Slice(infos, func(i, j int) bool { return infos[i].ID < infos[j].ID })
	return infos, nil
}

func (f *FileBlobStore) Delete(ctx context.Context, id string) error {
	if err := os.Remove(f.blobPath(id)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete blob %s: %w", id, err)
	}
	return nil
}

func (f *FileBlobStore) PutKey(ctx context.Context, key string, data []byte) error {
	var timer *time.Timer
	if f.latency > 0 {
		timer = time.NewTimer(f.latency)
		defer timer.Stop()
	}

	if err := os.WriteFile(f.keyPath(key), data, 0644); err != nil {
		return fmt.Errorf("write key %s: %w", key, err)
	}

	if timer != nil {
		return f.waitLatency(ctx, timer)
	}
	return nil
}

func (f *FileBlobStore) GetKey(ctx context.Context, key string) ([]byte, error) {
	var timer *time.Timer
	if f.latency > 0 {
		timer = time.NewTimer(f.latency)
		defer timer.Stop()
	}

	data, err := os.ReadFile(f.keyPath(key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrBlobNotFound
		}
		return nil, fmt.Errorf("read key %s: %w", key, err)
	}

	if timer != nil {
		if err := f.waitLatency(ctx, timer); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (f *FileBlobStore) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	flatPrefix := strings.ReplaceAll(prefix, "/", "_")

	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}

	var keys []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, flatPrefix) {
			// Convert back from flat filename to original key format
			key := strings.ReplaceAll(name, "_", "/")
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)
	return keys, nil
}
