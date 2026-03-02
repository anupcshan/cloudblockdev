package cloudblock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ErrBlobNotFound is returned when a blob does not exist.
var ErrBlobNotFound = errors.New("blob not found")

// BlobInfo contains metadata about a blob.
type BlobInfo struct {
	ID   string
	Size int64
}

// BlobStore is the interface for blob storage backends.
type BlobStore interface {
	// Put stores a blob from a seekable reader of known length.
	// The reader must support seeking for S3 retry support.
	Put(ctx context.Context, id string, body io.ReadSeeker, length int64) error

	// Get reads len(p) bytes starting at offset off from the blob into p.
	// Returns the number of bytes read and any error.
	// Returns ErrBlobNotFound if the blob does not exist.
	Get(ctx context.Context, id string, p []byte, off int64) (int, error)

	// List returns all blobs with their sizes.
	List(ctx context.Context) ([]BlobInfo, error)

	// ListAfter returns blobs with timestamps > afterTimestamp.
	// Uses S3 StartAfter for efficient incremental listing.
	ListAfter(ctx context.Context, afterTimestamp int64) ([]BlobInfo, error)

	// Delete removes a blob.
	Delete(ctx context.Context, id string) error

	// PutKey stores data at a raw key (without blob path transformation).
	// Used for storing index segments and other non-blob data.
	PutKey(ctx context.Context, key string, data []byte) error

	// GetKey retrieves data from a raw key (without blob path transformation).
	GetKey(ctx context.Context, key string) ([]byte, error)

	// ListPrefix returns all keys with the given prefix (without blob path transformation).
	// Used for listing index segments.
	ListPrefix(ctx context.Context, prefix string) ([]string, error)
}

// MemoryBlobStore is an in-memory implementation of BlobStore for testing.
type MemoryBlobStore struct {
	mu    sync.RWMutex
	blobs map[string][]byte
}

// NewMemoryBlobStore creates a new in-memory blob store.
// The returned store is wrapped with concurrency limiting.
func NewMemoryBlobStore() BlobStore {
	return NewConcurrentBlobStore(&MemoryBlobStore{
		blobs: make(map[string][]byte),
	}, DefaultMaxConcurrentFetches)
}

func (s *MemoryBlobStore) Put(ctx context.Context, id string, body io.ReadSeeker, length int64) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.blobs[id] = data
	return nil
}

func (s *MemoryBlobStore) Get(ctx context.Context, id string, p []byte, off int64) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.blobs[id]
	if !ok {
		return 0, ErrBlobNotFound
	}

	if int(off) >= len(data) {
		return 0, nil
	}

	n := copy(p, data[off:])
	return n, nil
}

func (s *MemoryBlobStore) List(ctx context.Context) ([]BlobInfo, error) {
	return s.ListAfter(ctx, 0)
}

func (s *MemoryBlobStore) ListAfter(ctx context.Context, afterTimestamp int64) ([]BlobInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	infos := make([]BlobInfo, 0, len(s.blobs))
	for id, data := range s.blobs {
		ts, err := ParseBlobIDTimestamp(id)
		if err != nil {
			continue // Skip non-blob entries (e.g., segment index keys)
		}
		if afterTimestamp > 0 && int64(ts) <= afterTimestamp {
			continue
		}
		infos = append(infos, BlobInfo{ID: id, Size: int64(len(data))})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].ID < infos[j].ID })
	return infos, nil
}

func (s *MemoryBlobStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.blobs, id)
	return nil
}

func (s *MemoryBlobStore) PutKey(ctx context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Make a copy
	copied := make([]byte, len(data))
	copy(copied, data)
	s.blobs[key] = copied
	return nil
}

func (s *MemoryBlobStore) GetKey(ctx context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.blobs[key]
	if !ok {
		return nil, ErrBlobNotFound
	}

	// Return a copy
	copied := make([]byte, len(data))
	copy(copied, data)
	return copied, nil
}

func (s *MemoryBlobStore) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []string
	for key := range s.blobs {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// S3BlobStore implements BlobStore using S3.
type S3BlobStore struct {
	client *s3.Client
	bucket string
	prefix string
}

// S3BlobStoreConfig holds configuration for S3BlobStore.
type S3BlobStoreConfig struct {
	Bucket    string
	Prefix    string // e.g., "mydevice/"
	Endpoint  string // Optional: custom S3 endpoint for S3-compatible services
	Region    string // AWS region (defaults to "us-east-1")
	AccessKey string // Optional: explicit credentials
	SecretKey string // Optional: explicit credentials
}

// NewS3BlobStore creates a new S3-backed blob store.
// The returned store is wrapped with concurrency limiting.
func NewS3BlobStore(ctx context.Context, cfg S3BlobStoreConfig) (BlobStore, error) {
	client, err := createS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return NewConcurrentBlobStore(&S3BlobStore{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, DefaultMaxConcurrentFetches), nil
}

// createS3Client creates an S3 client with optimized HTTP transport settings.
func createS3Client(ctx context.Context, cfg S3BlobStoreConfig) (*s3.Client, error) {
	// Custom HTTP transport with optimized connection pooling for S3.
	// Need enough connections to support concurrent workers (512 read + 512 upload).
	transport := &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // S3 objects are often already compressed
		ForceAttemptHTTP2:   true, // HTTP/2 multiplexing can help
	}
	httpClient := &http.Client{Transport: transport}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(region))
	opts = append(opts, awsconfig.WithHTTPClient(httpClient))

	// Configure SDK retryer with higher max attempts for handling rate limiting.
	// The SDK's standard retryer already handles 503 SlowDown, 500 InternalError,
	// 503 ServiceUnavailable, and connection errors. We increase max attempts
	// from the default (3) to handle sustained rate limiting during recovery.
	opts = append(opts, awsconfig.WithRetryer(func() aws.Retryer {
		return retry.AddWithMaxAttempts(retry.NewStandard(), 10)
	}))

	// Use explicit credentials if provided
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	// Create S3 client with optional custom endpoint
	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true // Required for most S3-compatible services
		})
	}

	return s3.NewFromConfig(awsCfg, s3Opts...), nil
}

func (s *S3BlobStore) key(id string) string {
	partition := BlobPartition(id)
	return s.prefix + BlobPrefix + partition + "/" + id + ".blob"
}

func (s *S3BlobStore) Put(ctx context.Context, id string, body io.ReadSeeker, length int64) error {
	key := s.key(id)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(length),
	})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}

	return nil
}

func (s *S3BlobStore) Get(ctx context.Context, id string, p []byte, off int64) (int, error) {
	key := s.key(id)

	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return 0, ErrBlobNotFound
		}
		return 0, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer resp.Body.Close()

	n, err := io.ReadFull(resp.Body, p)
	if err != nil {
		return n, fmt.Errorf("s3 read body %s: %w", key, err)
	}

	return n, nil
}

func (s *S3BlobStore) List(ctx context.Context) ([]BlobInfo, error) {
	return s.ListAfter(ctx, 0)
}

// listPartitionConcurrency limits concurrent partition listings.
const listPartitionConcurrency = 16

func (s *S3BlobStore) ListAfter(ctx context.Context, afterTimestamp int64) ([]BlobInfo, error) {
	// List all partitions with limited concurrency
	type partitionResult struct {
		infos []BlobInfo
		err   error
	}
	results := make(chan partitionResult, NumBlobPartitions)
	sem := make(chan struct{}, listPartitionConcurrency)

	for i := 0; i < NumBlobPartitions; i++ {
		partition := fmt.Sprintf("%02d", i)
		go func(partition string) {
			sem <- struct{}{}        // acquire
			defer func() { <-sem }() // release
			infos, err := s.listPartitionAfter(ctx, partition, afterTimestamp)
			results <- partitionResult{infos, err}
		}(partition)
	}

	// Collect results
	var allInfos []BlobInfo
	for i := 0; i < NumBlobPartitions; i++ {
		result := <-results
		if result.err != nil {
			return nil, result.err
		}
		allInfos = append(allInfos, result.infos...)
	}

	// Sort by blob ID (timestamp order)
	sort.Slice(allInfos, func(i, j int) bool {
		return allInfos[i].ID < allInfos[j].ID
	})

	return allInfos, nil
}

// listPartitionAfter lists blobs in a single partition after the given timestamp.
func (s *S3BlobStore) listPartitionAfter(ctx context.Context, partition string, afterTimestamp int64) ([]BlobInfo, error) {
	prefix := s.prefix + BlobPrefix + partition + "/"
	var infos []BlobInfo

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}

	// Use StartAfter to skip blobs at or before afterTimestamp
	if afterTimestamp > 0 {
		startAfter := fmt.Sprintf("%s%013d.blob", prefix, afterTimestamp)
		input.StartAfter = aws.String(startAfter)
	}

	paginator := s3.NewListObjectsV2Paginator(s.client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list partition %s: %w", partition, err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasSuffix(key, ".blob") {
				continue
			}
			// Extract blob ID from: {prefix}blobs/{partition}/{id}.blob
			id := strings.TrimPrefix(key, prefix)
			id = strings.TrimSuffix(id, ".blob")
			infos = append(infos, BlobInfo{ID: id, Size: aws.ToInt64(obj.Size)})
		}
	}

	return infos, nil
}

func (s *S3BlobStore) Delete(ctx context.Context, id string) error {
	key := s.key(id)

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 delete %s: %w", key, err)
	}
	return nil
}

func (s *S3BlobStore) PutKey(ctx context.Context, key string, data []byte) error {
	fullKey := s.prefix + key

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(fullKey),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", fullKey, err)
	}

	return nil
}

func (s *S3BlobStore) GetKey(ctx context.Context, key string) ([]byte, error) {
	fullKey := s.prefix + key

	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, ErrBlobNotFound
		}
		return nil, fmt.Errorf("s3 get %s: %w", fullKey, err)
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (s *S3BlobStore) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := s.prefix + prefix
	var keys []string

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			// Strip the store's prefix to return relative keys
			key = strings.TrimPrefix(key, s.prefix)
			keys = append(keys, key)
		}
	}

	return keys, nil
}
