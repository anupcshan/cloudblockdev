package cloudblock

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/anupcshan/cloudblockdev/internal/nbd"
)

// Device configuration constants
const (
	DefaultLargeWriteThreshold = 512 * 1024       // 512KB
	DefaultMaxBufferSize       = 16 * 1024 * 1024 // 16MB
	DefaultUploadWorkers       = 512
	DefaultSegmentThreshold    = 256 // minimum blobs per segment
	BlobPrefix                 = "blobs/"
)

// Default GC interval
const DefaultGCInterval = 60 * time.Second

// DeviceConfig holds configuration for creating a Device.
type DeviceConfig struct {
	// Store is the blob storage backend
	Store BlobStore
	// Device size in bytes
	Size int64
	// Large write threshold - writes >= this size bypass buffer
	LargeWriteThreshold int
	// Max buffer size - seal buffer when exceeded
	MaxBufferSize int
	// SegmentThreshold is the minimum number of blobs per segment (default 256)
	SegmentThreshold int
	// UploadWorkers is the number of concurrent upload workers (default 32)
	UploadWorkers int
	// GCEnabled controls whether garbage collection is enabled (default true)
	GCEnabled bool
	// GCInterval is how often to run garbage collection (default 60s)
	GCInterval time.Duration
	// MaxOutstandingWriteBytes limits the total bytes of sealed data awaiting
	// upload. When this limit is reached, new writes block until uploads
	// complete. 0 = unlimited. Default: 128MB.
	MaxOutstandingWriteBytes int64
}

// blobQueueEntry tracks a sealed blob's upload status for segment writing.
type blobQueueEntry struct {
	timestamp int64
	header    *BlobHeader // nil until upload succeeds
}

// Device is a block device backed by blob storage.
// It supports concurrent read/write operations with the following model:
//   - Writes are serialized through a single sequencer goroutine
//   - Reads are parallel via a worker pool
//   - Uploads to S3 are parallel via a separate worker pool
//   - Flush acts as a barrier, waiting for prior writes to be uploaded
type Device struct {
	store               BlobStore
	size                int64
	largeWriteThreshold int

	index     *RangeIndex
	bufferMgr *BufferManager
	opTracker *OpTracker

	// Channel for async operations (unified queue for reads, writes, flush, trim)
	opCh chan *Operation

	// Worker management
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	// Configuration
	uploadWorkers    int
	segmentThreshold int

	// mu protects buffer manager state and segment writing state.
	mu sync.Mutex

	// Blob queue: ordered list of sealed blob IDs, appended at seal time
	// (sequencer), marked complete by upload workers. Entries are popped
	// from the head when complete and added to segmentBatch.
	blobQueue     []string                    // ordered blob IDs
	blobQueueData map[string]*blobQueueEntry  // blob ID -> entry

	// Segment batch: blobs popped from the queue, awaiting segment write.
	// Once len(segmentBatch) >= segmentThreshold, a segment is written.
	segmentBatch   []IndexSegmentEntry
	segmentPending bool  // true if a segment write is in progress
	lastSegmentEnd int64 // end timestamp of last successful segment

	// Upload queue for sealed buffers
	uploadQueue chan *SealedBuffer

	// Write backpressure: limits total bytes of sealed data awaiting upload.
	// Uses a separate mutex from d.mu because the sequencer blocks here
	// waiting for upload workers, which need d.mu to complete uploads.
	outstandingMu    sync.Mutex
	outstandingCond  *sync.Cond
	outstandingBytes int64
	maxOutstanding   int64
	bpClosing        bool // set during shutdown to unblock waiters

	// GC configuration
	gcEnabled  bool
	gcInterval time.Duration
	gcStop     chan struct{}

}

// NewDevice creates a new blob-backed block device.
// Call Start() to begin processing operations.
func NewDevice(cfg DeviceConfig) (*Device, error) {
	if cfg.Store == nil {
		return nil, errors.New("store is required")
	}
	if cfg.Size <= 0 {
		return nil, errors.New("size must be positive")
	}

	largeWriteThreshold := cfg.LargeWriteThreshold
	if largeWriteThreshold <= 0 {
		largeWriteThreshold = DefaultLargeWriteThreshold
	}

	maxBufferSize := cfg.MaxBufferSize
	if maxBufferSize <= 0 {
		maxBufferSize = DefaultMaxBufferSize
	}

	uploadWorkers := cfg.UploadWorkers
	if uploadWorkers <= 0 {
		uploadWorkers = DefaultUploadWorkers
	}

	segmentThreshold := cfg.SegmentThreshold
	if segmentThreshold <= 0 {
		segmentThreshold = DefaultSegmentThreshold
	}

	gcInterval := cfg.GCInterval
	if gcInterval <= 0 {
		gcInterval = DefaultGCInterval
	}

	d := &Device{
		store:               cfg.Store,
		size:                cfg.Size,
		largeWriteThreshold: largeWriteThreshold,
		index:               NewRangeIndex(),
		bufferMgr:           NewBufferManager(BufferManagerConfig{Threshold: maxBufferSize}),
		opTracker:           NewOpTracker(),
		uploadWorkers:       uploadWorkers,
		segmentThreshold:    segmentThreshold,
		blobQueueData:       make(map[string]*blobQueueEntry),
		maxOutstanding:      cfg.MaxOutstandingWriteBytes,
		gcEnabled:           cfg.GCEnabled,
		gcInterval:          gcInterval,
	}
	d.outstandingCond = sync.NewCond(&d.outstandingMu)

	return d, nil
}

// Size returns the device size in bytes.
func (d *Device) Size() int64 {
	return d.size
}

// Start begins the background goroutines for processing operations.
// Must be called before submitting any operations.
func (d *Device) Start() error {
	if d.ctx != nil {
		return errors.New("device already started")
	}

	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.opCh = make(chan *Operation, 256)
	d.uploadQueue = make(chan *SealedBuffer, d.uploadWorkers*2)

	// Start unified sequencer (single goroutine for all operations)
	d.wg.Add(1)
	go d.sequencer()

	// Start upload workers
	for i := 0; i < d.uploadWorkers; i++ {
		d.wg.Add(1)
		go d.uploadWorker()
	}

	// Start GC worker if enabled
	if d.gcEnabled {
		d.gcStop = make(chan struct{})
		d.wg.Add(1)
		go d.gcWorker()
		log.Printf("Device started: %d upload workers, GC enabled (interval: %v)",
			d.uploadWorkers, d.gcInterval)
	} else {
		log.Printf("Device started: %d upload workers, GC disabled",
			d.uploadWorkers)
	}

	return nil
}

// Stop gracefully shuts down the device, waiting for pending operations.
func (d *Device) Stop(timeout time.Duration) error {
	if d.cancel == nil {
		return nil
	}

	// Signal shutdown
	d.cancel()

	// Stop GC worker
	if d.gcStop != nil {
		close(d.gcStop)
	}

	// Close operation channel and upload queue
	close(d.opCh)

	// Unblock any goroutine waiting in enqueueUpload
	if d.maxOutstanding > 0 {
		d.outstandingMu.Lock()
		d.bpClosing = true
		d.outstandingMu.Unlock()
		d.outstandingCond.Broadcast()
	}
	close(d.uploadQueue)

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("Device stopped gracefully")
		return nil
	case <-time.After(timeout):
		log.Printf("Device stop timed out after %v", timeout)
		return errors.New("shutdown timed out")
	}
}

// SubmitWrite queues a write operation.
// Takes ownership of data — the caller must not use data after this call.
// Returns a channel that will receive the result.
func (d *Device) SubmitWrite(handle uint64, data []byte, offset int64) <-chan nbd.OpResult {
	result := make(chan nbd.OpResult, 1)

	if offset < 0 || offset >= d.size {
		result <- nbd.OpResult{Err: errors.New("offset out of bounds")}
		return result
	}

	// Clamp to device size
	length := len(data)
	if offset+int64(length) > d.size {
		length = int(d.size - offset)
		data = data[:length]
	}

	seq := d.opTracker.NextSeq()
	d.opTracker.TrackWrite(seq)

	op := &Operation{
		Seq:    seq,
		Type:   OpWrite,
		Handle: handle,
		Offset: offset,
		Length: length,
		Data:   data,
		Done:   result,
	}

	select {
	case d.opCh <- op:
	case <-d.ctx.Done():
		d.opTracker.CompleteWrite(seq)
		result <- nbd.OpResult{Err: d.ctx.Err()}
	}

	return result
}

// SubmitWriteFUA queues a write with Force Unit Access.
// Takes ownership of data — the caller must not use data after this call.
// The write is immediately sealed and uploaded to S3 before the result is sent.
// Unlike Flush, this only waits for THIS write - not prior pending writes.
func (d *Device) SubmitWriteFUA(handle uint64, data []byte, offset int64) <-chan nbd.OpResult {
	result := make(chan nbd.OpResult, 1)

	if offset < 0 || offset >= d.size {
		result <- nbd.OpResult{Err: errors.New("offset out of bounds")}
		return result
	}

	// Clamp to device size
	length := len(data)
	if offset+int64(length) > d.size {
		length = int(d.size - offset)
		data = data[:length]
	}

	seq := d.opTracker.NextSeq()
	d.opTracker.TrackWrite(seq)

	op := &Operation{
		Seq:    seq,
		Type:   OpWriteFUA,
		Handle: handle,
		Offset: offset,
		Length: length,
		Data:   data,
		Done:   result,
	}

	select {
	case d.opCh <- op:
	case <-d.ctx.Done():
		d.opTracker.CompleteWrite(seq)
		result <- nbd.OpResult{Err: d.ctx.Err()}
	}

	return result
}

// SubmitRead queues a read operation.
// Returns a channel that will receive the result.
func (d *Device) SubmitRead(handle uint64, length int, offset int64) <-chan nbd.OpResult {
	result := make(chan nbd.OpResult, 1)

	if offset < 0 || offset >= d.size {
		result <- nbd.OpResult{Data: make([]byte, length), N: 0}
		return result
	}

	// Clamp to device size
	if offset+int64(length) > d.size {
		length = int(d.size - offset)
	}

	seq := d.opTracker.NextSeq()
	// Note: reads are not tracked for flush barriers

	op := &Operation{
		Seq:    seq,
		Type:   OpRead,
		Handle: handle,
		Offset: offset,
		Length: length,
		Done:   result,
	}

	select {
	case d.opCh <- op:
	case <-d.ctx.Done():
		result <- nbd.OpResult{Err: d.ctx.Err()}
	}

	return result
}

// SubmitFlush queues a flush operation.
// Flush acts as a barrier - it waits for all prior writes to be uploaded to S3.
func (d *Device) SubmitFlush(handle uint64) <-chan nbd.OpResult {
	result := make(chan nbd.OpResult, 1)

	seq := d.opTracker.NextSeq()

	op := &Operation{
		Seq:    seq,
		Type:   OpFlush,
		Handle: handle,
		Done:   result,
	}

	select {
	case d.opCh <- op:
	case <-d.ctx.Done():
		result <- nbd.OpResult{Err: d.ctx.Err()}
	}

	return result
}

// SubmitTrim queues a trim operation.
// Trim goes through the sequencer to maintain ordering.
func (d *Device) SubmitTrim(handle uint64, offset, length int64) <-chan nbd.OpResult {
	result := make(chan nbd.OpResult, 1)

	seq := d.opTracker.NextSeq()
	d.opTracker.TrackWrite(seq)

	op := &Operation{
		Seq:    seq,
		Type:   OpTrim,
		Handle: handle,
		Offset: offset,
		Length: int(length),
		Done:   result,
	}

	select {
	case d.opCh <- op:
	case <-d.ctx.Done():
		d.opTracker.CompleteWrite(seq)
		result <- nbd.OpResult{Err: d.ctx.Err()}
	}

	return result
}

// SubmitTrimFUA queues a trim with Force Unit Access (for write-zeroes with FUA).
// The trim must be durable (uploaded to S3) before the result is sent.
func (d *Device) SubmitTrimFUA(handle uint64, offset, length int64) <-chan nbd.OpResult {
	result := make(chan nbd.OpResult, 1)

	seq := d.opTracker.NextSeq()
	d.opTracker.TrackWrite(seq)

	op := &Operation{
		Seq:    seq,
		Type:   OpTrimFUA,
		Handle: handle,
		Offset: offset,
		Length: int(length),
		Done:   result,
	}

	select {
	case d.opCh <- op:
	case <-d.ctx.Done():
		d.opTracker.CompleteWrite(seq)
		result <- nbd.OpResult{Err: d.ctx.Err()}
	}

	return result
}

// sequencer is the single goroutine that handles all operations.
// This ensures ordering is preserved for writes while allowing async I/O for reads and flushes.
func (d *Device) sequencer() {
	defer d.wg.Done()

	for op := range d.opCh {
		switch op.Type {
		case OpWrite:
			d.handleWrite(op)
			d.opTracker.CompleteWrite(op.Seq)
		case OpWriteFUA:
			d.handleWriteFUA(op)
			d.opTracker.CompleteWrite(op.Seq)
		case OpRead:
			d.handleRead(op)
		case OpFlush:
			d.handleFlush(op)
		case OpTrim:
			d.handleTrim(op)
			d.opTracker.CompleteWrite(op.Seq)
		case OpTrimFUA:
			d.handleTrimFUA(op)
			d.opTracker.CompleteWrite(op.Seq)
		}
	}
}

// handleWrite processes a write operation.
func (d *Device) handleWrite(op *Operation) {
	// Large write - create blob directly and enqueue
	if op.Length >= d.largeWriteThreshold {
		d.handleLargeWrite(op)
		return
	}

	// Small write - add to buffer under lock
	d.mu.Lock()
	sb := d.bufferMgr.TakeWrite(op.Seq, uint64(op.Offset), op.Data)
	if sb != nil {
		d.enqueueBlobQueue(sb)
	}
	d.mu.Unlock()
	op.Data = nil // ownership transferred

	if sb != nil {
		d.enqueueUpload(sb)
	}
	op.Done <- nbd.OpResult{N: op.Length}
}

// handleLargeWrite handles a write that bypasses coalescing.
// It adds to the current buffer and immediately seals it.
func (d *Device) handleLargeWrite(op *Operation) {
	d.mu.Lock()
	d.bufferMgr.TakeWrite(op.Seq, uint64(op.Offset), op.Data)
	sb, _, _ := d.bufferMgr.Seal(SealTriggerLargeWrite)
	if sb != nil {
		d.enqueueBlobQueue(sb)
	}
	d.mu.Unlock()
	op.Data = nil // ownership transferred

	if sb != nil {
		d.enqueueUpload(sb)
	}
	op.Done <- nbd.OpResult{N: op.Length}
}

// handleWriteFUA handles a Force Unit Access write.
// The write is immediately sealed and we wait for THIS buffer's upload to complete.
// This does NOT wait for prior pending uploads - only this write.
func (d *Device) handleWriteFUA(op *Operation) {
	d.mu.Lock()
	// TakeWrite may auto-seal if buffer exceeds threshold
	sbFromAdd := d.bufferMgr.TakeWrite(op.Seq, uint64(op.Offset), op.Data)
	if sbFromAdd != nil {
		d.enqueueBlobQueue(sbFromAdd)
	}
	sbFromSeal, _, _ := d.bufferMgr.Seal(SealTriggerFUA)
	if sbFromSeal != nil {
		d.enqueueBlobQueue(sbFromSeal)
	}
	d.mu.Unlock()
	op.Data = nil // ownership transferred

	if sbFromAdd != nil {
		d.enqueueUpload(sbFromAdd)
	}
	if sbFromSeal != nil {
		d.enqueueUpload(sbFromSeal)
	}

	// Wait for whichever sealed buffer contains our data
	sb := sbFromSeal
	if sb == nil {
		sb = sbFromAdd
	}

	// Wait for this specific sealed buffer to be uploaded
	go func() {
		if sb != nil {
			<-sb.uploaded
			if sb.err != nil {
				op.Done <- nbd.OpResult{Err: sb.err}
				return
			}
		}
		op.Done <- nbd.OpResult{N: op.Length}
	}()
}

// handleTrim processes a trim operation.
// Trim creates a hole in the device - reads to this region will return zeros.
// The hole is buffered and flushed to S3 like regular writes.
func (d *Device) handleTrim(op *Operation) {
	d.mu.Lock()
	sb := d.bufferMgr.AddHole(op.Seq, uint64(op.Offset), uint64(op.Length))
	if sb != nil {
		d.enqueueBlobQueue(sb)
	}
	d.mu.Unlock()

	if sb != nil {
		d.enqueueUpload(sb)
	}
	op.Done <- nbd.OpResult{}
}

// handleTrimFUA handles a Force Unit Access trim (for write-zeroes with FUA).
// The hole is immediately sealed and we wait for THIS buffer's upload to complete.
func (d *Device) handleTrimFUA(op *Operation) {
	d.mu.Lock()
	// AddHole may auto-seal if buffer exceeds block count limit
	sbFromAdd := d.bufferMgr.AddHole(op.Seq, uint64(op.Offset), uint64(op.Length))
	if sbFromAdd != nil {
		d.enqueueBlobQueue(sbFromAdd)
	}
	sbFromSeal, _, _ := d.bufferMgr.Seal(SealTriggerFUA)
	if sbFromSeal != nil {
		d.enqueueBlobQueue(sbFromSeal)
	}
	d.mu.Unlock()

	if sbFromAdd != nil {
		d.enqueueUpload(sbFromAdd)
	}
	if sbFromSeal != nil {
		d.enqueueUpload(sbFromSeal)
	}

	// Wait for whichever sealed buffer contains our data
	sb := sbFromSeal
	if sb == nil {
		sb = sbFromAdd
	}

	// Wait for this specific sealed buffer to be uploaded
	go func() {
		if sb != nil {
			<-sb.uploaded
			if sb.err != nil {
				op.Done <- nbd.OpResult{Err: sb.err}
				return
			}
		}
		op.Done <- nbd.OpResult{}
	}()
}

// handleRead processes a read operation.
// It copies data from buffers synchronously, then dispatches S3 fetches async if needed.
func (d *Device) handleRead(op *Operation) {
	p := make([]byte, op.Length)

	// Copy from buffers (newest first) and get remaining gaps
	d.mu.Lock()
	gaps := d.bufferMgr.CopyAndReturnGaps(p, uint64(op.Offset), uint64(op.Length))
	d.mu.Unlock()

	if len(gaps) == 0 {
		// Fully satisfied from memory
		op.Done <- nbd.OpResult{Data: p, N: op.Length}
		return
	}

	// Query index for gaps to get blob ranges
	var blobRanges []RangeEntry
	for _, gap := range gaps {
		entries := d.index.Lookup(gap.Offset, gap.Length)
		for _, e := range entries {
			if e.BlobID != "" && !e.IsHole {
				blobRanges = append(blobRanges, e)
			}
			// Holes: leave zeros (already in p)
			// BlobID=="": should have been covered by buffers
		}
	}

	if len(blobRanges) == 0 {
		// No S3 data needed (all zeros or covered by buffers)
		op.Done <- nbd.OpResult{Data: p, N: op.Length}
		return
	}

	// Dispatch S3 fetches async to avoid blocking sequencer
	go func() {
		err := d.fetchBlobRanges(d.ctx, p, op.Offset, op.Seq, blobRanges)
		if err != nil {
			op.Done <- nbd.OpResult{Err: err}
		} else {
			op.Done <- nbd.OpResult{Data: p, N: op.Length}
		}
	}()
}

// handleFlush processes a flush operation.
// It seals the current buffer and spawns a goroutine to wait for uploads.
func (d *Device) handleFlush(op *Operation) {
	d.mu.Lock()
	sb, marker, hasUploads := d.bufferMgr.Seal(SealTriggerFlush)
	if sb != nil {
		d.enqueueBlobQueue(sb)
	}
	// Snapshot sealed buffers to wait for while holding the lock
	var toWait []*SealedBuffer
	if hasUploads {
		toWait = d.bufferMgr.SealedBuffers(marker)
	}
	d.mu.Unlock()

	if sb != nil {
		d.enqueueUpload(sb)
	}

	go func() {
		err := d.waitForUploads(toWait)
		op.Done <- nbd.OpResult{Err: err}
	}()
}

// enqueueBlobQueue appends a sealed buffer to the blob queue for segment tracking.
// Called at seal time on the sequencer, so blob IDs are in monotonic timestamp order.
// Caller must hold d.mu.
func (d *Device) enqueueBlobQueue(sb *SealedBuffer) {
	ts, err := ParseBlobIDTimestamp(sb.blobID)
	if err != nil {
		return
	}
	d.blobQueue = append(d.blobQueue, sb.blobID)
	d.blobQueueData[sb.blobID] = &blobQueueEntry{
		timestamp: int64(ts),
	}
}

// enqueueUpload sends a sealed buffer to the upload queue.
// Blocks if the outstanding byte limit is exceeded or the queue is full.
// Must NOT hold d.mu (upload workers need d.mu to complete).
func (d *Device) enqueueUpload(sb *SealedBuffer) {
	if d.maxOutstanding > 0 {
		bufSize := int64(sb.buffer.Size())
		d.outstandingMu.Lock()
		// Wait until outstanding bytes are under the limit.
		// Allow the first buffer through even if it exceeds the limit
		// (outstandingBytes == 0) to avoid deadlock when a single
		// buffer is larger than maxOutstanding.
		for !d.bpClosing && d.outstandingBytes > 0 &&
			d.outstandingBytes+bufSize > d.maxOutstanding {
			d.outstandingCond.Wait()
		}
		if !d.bpClosing {
			d.outstandingBytes += bufSize
			OutstandingWriteBytes.Set(float64(d.outstandingBytes))
		}
		d.outstandingMu.Unlock()
	}
	d.uploadQueue <- sb
}

// releaseBackpressure decrements outstanding bytes after an upload completes.
func (d *Device) releaseBackpressure(sb *SealedBuffer) {
	if d.maxOutstanding > 0 {
		bufSize := int64(sb.buffer.Size())
		d.outstandingMu.Lock()
		d.outstandingBytes -= bufSize
		OutstandingWriteBytes.Set(float64(d.outstandingBytes))
		d.outstandingMu.Unlock()
		d.outstandingCond.Broadcast()
	}
}

// waitForUploads blocks until all given sealed buffers are uploaded.
func (d *Device) waitForUploads(toWait []*SealedBuffer) error {
	for _, sb := range toWait {
		<-sb.uploaded
		if sb.err != nil {
			return sb.err
		}
	}
	return nil
}

// fetchBlobRanges fetches data from pending buffers or S3 for the given blob ranges.
func (d *Device) fetchBlobRanges(ctx context.Context, p []byte, baseOffset int64, seq uint64, ranges []RangeEntry) error {
	// Group ranges by blob for efficient fetching
	blobRanges := make(map[string][]RangeEntry)
	for _, r := range ranges {
		blobRanges[r.BlobID] = append(blobRanges[r.BlobID], r)
	}

	// Fetch data from each blob
	for blobID, blobEntries := range blobRanges {
		// Check if blob is still pending upload (data in memory)
		d.mu.Lock()
		sb := d.bufferMgr.GetPendingBlob(blobID)
		d.mu.Unlock()

		if sb != nil {
			// Read from the sealed buffer's WriteBuffer using device offsets
			for _, entry := range blobEntries {
				if entry.IsHole {
					continue // Holes are already zeros in p
				}
				// Calculate overlap between entry and requested range
				entryEnd := entry.Offset + entry.Length
				reqEnd := uint64(baseOffset) + uint64(len(p))
				overlapStart := max(entry.Offset, uint64(baseOffset))
				overlapEnd := min(entryEnd, reqEnd)
				if overlapStart >= overlapEnd {
					continue
				}
				overlapLen := overlapEnd - overlapStart
				dstStart := overlapStart - uint64(baseOffset)
				sb.buffer.ReadInto(p[dstStart:dstStart+overlapLen], overlapStart, overlapLen)
			}
			continue
		}

		// Blob is uploaded (or upload failed and was removed) - read from S3
		if err := d.readFromBlob(ctx, p, baseOffset, len(p), blobID, seq, blobEntries); err != nil {
			return err
		}
	}

	return nil
}

// readFromBlob reads data from a specific blob for the given ranges.
func (d *Device) readFromBlob(ctx context.Context, p []byte, offset int64, length int, blobID string, seq uint64, ranges []RangeEntry) error {
	return d.readRangesFromStore(ctx, p, offset, length, blobID, seq, ranges)
}

// readRangesFromStore reads the needed ranges directly from the blob store into p.
func (d *Device) readRangesFromStore(ctx context.Context, p []byte, offset int64, length int, blobID string, seq uint64, ranges []RangeEntry) error {
	for _, r := range ranges {
		// Calculate overlap with requested range
		rEnd := r.Offset + uint64(r.Length)
		reqEnd := uint64(offset) + uint64(length)

		overlapStart := max(r.Offset, uint64(offset))
		overlapEnd := min(rEnd, reqEnd)

		if overlapStart >= overlapEnd {
			continue
		}

		// Source: blob at BlobOffset + (overlapStart - r.Offset)
		srcOffset := int64(r.BlobOffset) + int64(overlapStart-r.Offset)
		// Destination: p at (overlapStart - offset)
		dstOffset := int(overlapStart - uint64(offset))
		copyLen := int(overlapEnd - overlapStart)

		// Read directly from store into output buffer
		n, err := d.store.Get(ctx, blobID, p[dstOffset:dstOffset+copyLen], srcOffset)
		if err != nil {
			return err
		}
		if n != copyLen {
			return fmt.Errorf("blob %s: short read from store (got %d, want %d)", blobID, n, copyLen)
		}
	}
	return nil
}

// uploadWorker processes sealed buffers from the upload queue.
func (d *Device) uploadWorker() {
	defer d.wg.Done()

	for sb := range d.uploadQueue {
		err := d.uploadBlob(d.ctx, sb)

		sb.err = err
		close(sb.uploaded)

		d.mu.Lock()
		if sb.header != nil && err == nil {
			// Upload succeeded - add index entries.
			// The timestamp-based comparison in insertLocked ensures older uploads
			// don't overwrite newer ones, handling out-of-order completion safely.
			d.index.InsertBlobEntries(sb.blobID, sb.header)

			// Mark blob as uploaded in the queue and drain completed head entries
			d.blobQueueData[sb.blobID].header = sb.header
			d.drainBlobQueue()
		}
		d.bufferMgr.MarkUploaded(sb)
		d.mu.Unlock()

		d.releaseBackpressure(sb)
	}
}

// uploadBlob uploads a sealed buffer to the store.
// The blob is streamed directly from the WriteBuffer's data chunks to S3 via
// a SegmentReader — no intermediate copies are made.
func (d *Device) uploadBlob(ctx context.Context, sb *SealedBuffer) error {
	if sb.buffer == nil || sb.buffer.IsEmpty() {
		return nil
	}

	// Build block entries and collect data slices from the sealed WriteBuffer.
	// The WriteBuffer is immutable after sealing (a new active buffer was
	// created), so this is safe without locks.
	blocks, dataSlices := sb.buffer.ToBlobParts()
	if blocks == nil {
		return nil
	}

	// Build the BlobHeader and serialize it.
	header, err := NewBlobHeader(blocks)
	if err != nil {
		return fmt.Errorf("new blob header: %w", err)
	}
	headerBytes, err := header.Marshal()
	if err != nil {
		return fmt.Errorf("marshal header: %w", err)
	}

	// Store header on SealedBuffer for upload completion to access.
	sb.header = header

	// Build a SegmentReader that streams [headerBytes, data0, data1, ...] to S3
	// without allocating a contiguous buffer. Only non-hole data slices are included.
	segments := make([][]byte, 0, 1+len(dataSlices))
	segments = append(segments, headerBytes)
	for _, ds := range dataSlices {
		if ds != nil {
			segments = append(segments, ds)
		}
	}
	reader := NewSegmentReader(segments...)

	// Stream to store — no contiguous []byte allocation.
	if err := d.store.Put(ctx, sb.blobID, reader, reader.Size()); err != nil {
		return fmt.Errorf("store put: %w", err)
	}

	return nil
}

// drainBlobQueue pops completed entries from the head of the blob queue
// and adds successful uploads to the segment batch. If the batch reaches
// the segment threshold, a segment write is triggered.
// Caller must hold d.mu.
func (d *Device) drainBlobQueue() {
	// Pop all uploaded entries from the head
	popped := 0
	for popped < len(d.blobQueue) {
		blobID := d.blobQueue[popped]
		entry := d.blobQueueData[blobID] // must exist; panic if invariant violated
		if entry.header == nil {
			break // not yet uploaded
		}
		d.segmentBatch = append(d.segmentBatch, IndexSegmentEntry{
			ID:     blobID,
			Header: *entry.header,
		})
		delete(d.blobQueueData, blobID)
		popped++
	}
	if popped > 0 {
		d.blobQueue = d.blobQueue[popped:]
	}

	// Check if we should write a segment
	if len(d.segmentBatch) >= d.segmentThreshold && !d.segmentPending {
		batch := d.segmentBatch
		d.segmentBatch = nil
		d.segmentPending = true
		d.wg.Add(1)
		go d.writeSegment(batch)
	}
}

// writeSegment writes an index segment to S3 containing the given batch of blobs.
func (d *Device) writeSegment(batch []IndexSegmentEntry) {
	defer d.wg.Done()
	defer func() {
		d.mu.Lock()
		d.segmentPending = false
		// Check if more entries accumulated while we were writing
		d.drainBlobQueue()
		d.mu.Unlock()
	}()

	// Determine segment range
	d.mu.Lock()
	startTS := d.lastSegmentEnd
	d.mu.Unlock()

	// Find max timestamp in batch for endTS
	var endTS int64
	for _, entry := range batch {
		if ts, err := ParseBlobIDTimestamp(entry.ID); err == nil && int64(ts) > endTS {
			endTS = int64(ts)
		}
	}

	// Marshal and upload segment (no lock held during I/O)
	segment := NewIndexSegment(startTS, endTS, batch)
	data, err := segment.Marshal()
	if err != nil {
		log.Printf("Warning: failed to marshal segment: %v", err)
		return
	}

	ctx := context.Background()
	if err := d.store.PutKey(ctx, segment.Key(), data); err != nil {
		log.Printf("Warning: failed to upload segment %s: %v", segment.Key(), err)
		return
	}

	log.Printf("Wrote index segment %s with %d blobs", segment.Key(), segment.Len())

	// Update lastSegmentEnd on success
	d.mu.Lock()
	d.lastSegmentEnd = endTS
	d.mu.Unlock()
}

// fetchResult holds the result of a parallel fetch operation.
type fetchResult[R any] struct {
	data R
	err  error
}

// fetchParallelApplyOrdered fetches items in parallel but applies results in order.
// This enables maximum fetch parallelism while maintaining correct application order.
// Returns on first error encountered (either fetch or apply).
func fetchParallelApplyOrdered[T any, R any](
	ctx context.Context,
	items []T,
	fetch func(context.Context, T) (R, error),
	apply func(R) error,
) error {
	if len(items) == 0 {
		return nil
	}

	// One channel per item for ordered completion
	results := make([]chan fetchResult[R], len(items))
	for i := range results {
		results[i] = make(chan fetchResult[R], 1)
	}

	// Spawn parallel fetchers
	for i, item := range items {
		go func(i int, item T) {
			data, err := fetch(ctx, item)
			results[i] <- fetchResult[R]{data, err}
		}(i, item)
	}

	// Apply in order (blocks until each slot is ready)
	for i := range results {
		res := <-results[i]
		if res.err != nil {
			return res.err
		}
		if err := apply(res.data); err != nil {
			return err
		}
	}
	return nil
}

// LoadIndex rebuilds the in-memory index from S3 segments and blob headers.
// This is the normal startup flow, not just for failure recovery.
//
// Flow:
// 1. List segments from S3 and filter superseded ones
// 2. Fetch segments (parallel fetch, ordered apply)
// 3. List blobs after max segment timestamp from S3
// 4. Fetch blob headers (parallel fetch, ordered apply)
// 5. Set blob ID floor
func (d *Device) LoadIndex(ctx context.Context) error {
	log.Printf("Loading index from S3...")

	// 1. List segments from S3 and parse metadata
	s3Keys, err := d.store.ListPrefix(ctx, SegmentPrefix)
	if err != nil {
		return fmt.Errorf("list segments: %w", err)
	}

	var segments []segmentMeta
	for _, key := range s3Keys {
		epoch, start, end, err := ParseSegmentKey(key)
		if err != nil {
			log.Printf("Warning: invalid segment key %s: %v", key, err)
			continue
		}
		segments = append(segments, segmentMeta{key: key, epoch: epoch, start: start, end: end})
	}

	// Filter out superseded segments
	activeSegments, supersededSegments := d.filterSupersededSegments(segments)
	if len(supersededSegments) > 0 {
		log.Printf("Filtered %d superseded segments, %d active segments remain",
			len(supersededSegments), len(activeSegments))
	}

	// Compute maxSegmentTS from segment keys (before fetching)
	var maxSegmentTS int64
	for _, seg := range activeSegments {
		if seg.end > maxSegmentTS {
			maxSegmentTS = seg.end
		}
	}

	// Sort segments by end timestamp for ordered application
	sort.Slice(activeSegments, func(i, j int) bool {
		return activeSegments[i].end < activeSegments[j].end
	})

	// 2. Fetch segments in parallel, apply in timestamp order
	var segmentBlobCount int
	if len(activeSegments) > 0 {
		log.Printf("Loading %d index segments...", len(activeSegments))
		err = fetchParallelApplyOrdered(ctx, activeSegments,
			func(ctx context.Context, seg segmentMeta) (*IndexSegment, error) {
				data, err := d.store.GetKey(ctx, seg.key)
				if err != nil {
					return nil, fmt.Errorf("get segment %s: %w", seg.key, err)
				}
				return UnmarshalSegment(data)
			},
			func(segment *IndexSegment) error {
				// Apply segment: insert blobs into index in order
				blobIDs := make([]string, 0, len(segment.Blobs))
				blobMap := make(map[string]*BlobHeader, len(segment.Blobs))
				for _, entry := range segment.Blobs {
					header := entry.Header // copy
					blobIDs = append(blobIDs, entry.ID)
					blobMap[entry.ID] = &header
				}
				sort.Strings(blobIDs)
				for _, blobID := range blobIDs {
					d.index.InsertBlob(blobID, blobMap[blobID])
				}
				segmentBlobCount += len(segment.Blobs)
				return nil
			},
		)
		if err != nil {
			return fmt.Errorf("load segments: %w", err)
		}
		d.lastSegmentEnd = maxSegmentTS
	}

	// 3. List blobs after maxSegmentTS from S3
	blobs, err := d.store.ListAfter(ctx, maxSegmentTS)
	if err != nil {
		return fmt.Errorf("list blobs after %d: %w", maxSegmentTS, err)
	}

	// Filter out blobs we already have in our index
	var newBlobs []BlobInfo
	for _, blob := range blobs {
		if !d.index.HasBlob(blob.ID) {
			newBlobs = append(newBlobs, blob)
		}
	}

	// Sort by blob ID (timestamp order) for ordered application
	sort.Slice(newBlobs, func(i, j int) bool {
		return newBlobs[i].ID < newBlobs[j].ID
	})

	// 4. Fetch blob headers in parallel, apply in order
	var maxBlobTS int64
	var headerCount int
	if len(newBlobs) > 0 {
		log.Printf("Loading %d blob headers...", len(newBlobs))

		type blobHeaderResult struct {
			blobID string
			header *BlobHeader
		}

		err = fetchParallelApplyOrdered(ctx, newBlobs,
			func(ctx context.Context, blob BlobInfo) (blobHeaderResult, error) {
				header, err := d.readBlobHeader(ctx, blob.ID, blob.Size)
				if err != nil {
					return blobHeaderResult{}, fmt.Errorf("read blob %s header: %w", blob.ID, err)
				}
				return blobHeaderResult{blobID: blob.ID, header: header}, nil
			},
			func(result blobHeaderResult) error {
				d.index.InsertBlob(result.blobID, result.header)
				headerCount++

				// Track max blob timestamp
				if ts, err := ParseBlobIDTimestamp(result.blobID); err == nil && int64(ts) > maxBlobTS {
					maxBlobTS = int64(ts)
				}
				return nil
			},
		)
		if err != nil {
			return fmt.Errorf("load blob headers: %w", err)
		}
	}

	// 5. Set blob ID floor
	maxTS := maxSegmentTS
	if maxBlobTS > maxTS {
		maxTS = maxBlobTS
	}
	if maxTS > 0 {
		SetBlobIDFloor(maxTS)
	}

	log.Printf("Index loaded: %d from segments + %d from blob headers, %d total ranges",
		segmentBlobCount, headerCount, d.index.Len())

	return nil
}

// Recover is an alias for LoadIndex for backward compatibility.
// Deprecated: Use LoadIndex instead.
func (d *Device) Recover(ctx context.Context) error {
	return d.LoadIndex(ctx)
}

// segmentMeta holds parsed segment key metadata.
type segmentMeta struct {
	key   string
	epoch int
	start int64
	end   int64
}

// filterSupersededSegments separates active segments from superseded ones.
// A segment is superseded if another segment covers it with a higher epoch,
// or covers a strictly larger range at the same epoch.
func (d *Device) filterSupersededSegments(segments []segmentMeta) (active, superseded []segmentMeta) {
	for _, seg := range segments {
		isSuperseded := false
		for _, other := range segments {
			if other.key == seg.key {
				continue
			}
			// Check if other covers seg's range
			if other.start <= seg.start && other.end >= seg.end {
				if other.epoch > seg.epoch {
					// Higher epoch supersedes (compacted segment)
					isSuperseded = true
					break
				}
				// Same epoch: only supersede if other is strictly larger
				if other.epoch == seg.epoch && (other.start < seg.start || other.end > seg.end) {
					isSuperseded = true
					break
				}
			}
		}
		if isSuperseded {
			superseded = append(superseded, seg)
		} else {
			active = append(active, seg)
		}
	}
	return active, superseded
}

// readBlobHeader reads just the header of a blob from the store.
// blobSize is the total blob size (from List); we read min(blobSize, BlobHeaderMax) bytes.
func (d *Device) readBlobHeader(ctx context.Context, blobID string, blobSize int64) (*BlobHeader, error) {
	readSize := blobSize
	if readSize > BlobHeaderMax {
		readSize = BlobHeaderMax
	}
	headerData := make([]byte, readSize)
	n, err := d.store.Get(ctx, blobID, headerData, 0)
	if err != nil {
		return nil, err
	}
	return UnmarshalBlobHeader(headerData[:n])
}

// GCStats holds statistics from a GC cycle.
type GCStats struct {
	BlobsScanned int
	BlobsDeleted int
	Errors       int
	Duration     time.Duration
}

// gcWorker runs periodic garbage collection.
func (d *Device) gcWorker() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.gcStop:
			return
		case <-ticker.C:
			stats := d.runGCCycle(d.ctx)
			if stats.BlobsDeleted > 0 || stats.Errors > 0 {
				log.Printf("GC: scanned %d blobs, deleted %d, errors %d, took %v",
					stats.BlobsScanned, stats.BlobsDeleted, stats.Errors, stats.Duration)
			}
		}
	}
}

// runGCCycle performs a single garbage collection cycle.
// It deletes blobs that are no longer referenced by the index.
func (d *Device) runGCCycle(ctx context.Context) GCStats {
	start := time.Now()
	stats := GCStats{}

	// 1. Get lastSegmentEnd - only delete blobs with timestamp <= this value.
	// All such blobs are in a segment and confirmed uploaded to S3.
	d.mu.Lock()
	safeTS := d.lastSegmentEnd
	d.mu.Unlock()

	if safeTS <= 0 {
		// No segments written yet, nothing to GC
		return stats
	}

	// 2. Get unreferenced blobs (no S3 listing needed!)
	unreferenced := d.index.GetUnreferencedBlobs()
	stats.BlobsScanned = len(unreferenced)

	// 3. Delete unreferenced blobs
	for _, blobID := range unreferenced {
		// Parse timestamp from blob ID
		ts, err := ParseBlobIDTimestamp(blobID)
		if err != nil {
			// Can't parse timestamp, skip to be safe
			continue
		}

		// Only delete if blob is in a written segment (timestamp <= lastSegmentEnd)
		if int64(ts) > safeTS {
			continue
		}

		// Safe to delete
		if err := d.store.Delete(ctx, blobID); err != nil {
			log.Printf("GC: failed to delete blob %s: %v", blobID, err)
			stats.Errors++
			continue
		}

		stats.BlobsDeleted++

		// Remove from index tracking
		d.index.RemoveBlob(blobID)
	}

	stats.Duration = time.Since(start)
	return stats
}

// Close releases resources.
func (d *Device) Close() error {
	var stopErr error
	if d.cancel != nil {
		stopErr = d.Stop(30 * time.Second)
	}
	return stopErr
}
