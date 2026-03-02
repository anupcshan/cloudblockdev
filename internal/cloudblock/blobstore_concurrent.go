package cloudblock

import (
	"context"
	"io"
)

// DefaultMaxConcurrentFetches is the default limit for concurrent blob fetches.
const DefaultMaxConcurrentFetches = 512

// ConcurrentBlobStore wraps a BlobStore with concurrency limiting.
// This prevents overwhelming the underlying store with too many concurrent requests.
type ConcurrentBlobStore struct {
	store BlobStore
	sem   chan struct{}
}

// NewConcurrentBlobStore wraps a BlobStore with concurrency limiting.
// maxConcurrent limits the number of concurrent Get operations.
func NewConcurrentBlobStore(store BlobStore, maxConcurrent int) *ConcurrentBlobStore {
	return &ConcurrentBlobStore{
		store: store,
		sem:   make(chan struct{}, maxConcurrent),
	}
}

// acquire blocks until a slot is available in the semaphore.
func (c *ConcurrentBlobStore) acquire() {
	c.sem <- struct{}{}
}

// release returns a slot to the semaphore.
func (c *ConcurrentBlobStore) release() {
	<-c.sem
}

// Get reads len(p) bytes starting at offset off from the blob into p.
func (c *ConcurrentBlobStore) Get(ctx context.Context, id string, p []byte, off int64) (int, error) {
	c.acquire()
	defer c.release()
	return c.store.Get(ctx, id, p, off)
}

// Put stores a blob. Pass-through to underlying store (no concurrency limit).
func (c *ConcurrentBlobStore) Put(ctx context.Context, id string, body io.ReadSeeker, length int64) error {
	return c.store.Put(ctx, id, body, length)
}

// List returns all blobs with their sizes. Pass-through to underlying store.
func (c *ConcurrentBlobStore) List(ctx context.Context) ([]BlobInfo, error) {
	return c.store.List(ctx)
}

// Delete removes a blob. Pass-through to underlying store.
func (c *ConcurrentBlobStore) Delete(ctx context.Context, id string) error {
	return c.store.Delete(ctx, id)
}

// PutKey stores data at a raw key. Pass-through to underlying store.
func (c *ConcurrentBlobStore) PutKey(ctx context.Context, key string, data []byte) error {
	return c.store.PutKey(ctx, key, data)
}

// GetKey retrieves data from a raw key with concurrency limiting.
func (c *ConcurrentBlobStore) GetKey(ctx context.Context, key string) ([]byte, error) {
	c.acquire()
	defer c.release()
	return c.store.GetKey(ctx, key)
}

// ListPrefix returns all keys with the given prefix. Pass-through to underlying store.
func (c *ConcurrentBlobStore) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	return c.store.ListPrefix(ctx, prefix)
}

// ListAfter returns blobs with timestamps > afterTimestamp. Pass-through to underlying store.
func (c *ConcurrentBlobStore) ListAfter(ctx context.Context, afterTimestamp int64) ([]BlobInfo, error) {
	return c.store.ListAfter(ctx, afterTimestamp)
}
