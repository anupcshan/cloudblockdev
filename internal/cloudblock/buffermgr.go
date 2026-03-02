package cloudblock

// SealedBuffer represents a buffer that has been sealed and is pending upload.
type SealedBuffer struct {
	maxSeq   uint64       // highest write seq in this buffer
	buffer   *WriteBuffer // the actual buffer data
	blobID   string       // pre-generated at seal time for GC protection scoping
	trigger  string       // what caused the seal (flush, fua, large_write, threshold_size, threshold_blocks)
	header   *BlobHeader  // set by Device after building the blob
	uploaded chan struct{} // closed when upload complete
	err      error        // upload error, if any
}

// BufferManager is a passive data structure that manages write buffers.
// It maintains an active buffer for incoming writes and a list of sealed
// buffers pending upload. All methods assume the caller holds the
// appropriate external lock (Device.mu).
type BufferManager struct {
	active       *WriteBuffer
	activeMinSeq uint64          // seq of first write in active buffer
	activeMaxSeq uint64          // seq of last write in active buffer
	sealed       []*SealedBuffer // ordered by seal time
	threshold    int             // seal when buffer size exceeds this

	// pendingBlobs tracks sealed buffers by blob ID for memory reads.
	// Reads check this map to read from memory instead of S3 for pending uploads.
	pendingBlobs map[string]*SealedBuffer
}

// BufferManagerConfig holds configuration for BufferManager.
type BufferManagerConfig struct {
	Threshold int // seal buffer when size exceeds this (bytes)
}

// NewBufferManager creates a new buffer manager.
func NewBufferManager(cfg BufferManagerConfig) *BufferManager {
	if cfg.Threshold <= 0 {
		cfg.Threshold = DefaultMaxBufferSize
	}

	return &BufferManager{
		active:       NewWriteBuffer(),
		sealed:       make([]*SealedBuffer, 0),
		threshold:    cfg.Threshold,
		pendingBlobs: make(map[string]*SealedBuffer),
	}
}

// TakeWrite takes ownership of data and adds it to the active buffer.
// The caller must not read, write, or reference data after this call returns.
// If the buffer exceeds the threshold, it is automatically sealed.
// Returns the sealed buffer if auto-seal occurred, nil otherwise.
// Caller must hold Device.mu.
func (m *BufferManager) TakeWrite(seq uint64, offset uint64, data []byte) *SealedBuffer {
	// Track sequence range in active buffer
	if m.active.IsEmpty() {
		m.activeMinSeq = seq
	}
	m.activeMaxSeq = seq

	// Add to buffer (ownership of data transfers to WriteBuffer)
	m.active.TakeWrite(offset, data)

	// Note: We intentionally do NOT insert a buffer entry into the index here.
	// Buffer data is found via CopyAndReturnGaps() which checks buffers directly.
	// Old blob entries stay in the index until InsertBlob() replaces them,
	// providing implicit GC protection without explicit tracking.

	// Check if we should seal (either size threshold or block count limit)
	if m.active.Size() >= m.threshold {
		return m.seal(SealTriggerThresholdSize)
	}
	if m.active.BlockCount() >= MaxBlocksPerBlob {
		return m.seal(SealTriggerThresholdBlocks)
	}
	return nil
}

// AddHole adds a hole (trim/zero) to the active buffer.
// If the buffer exceeds the block count limit, it is automatically sealed.
// Returns the sealed buffer if auto-seal occurred, nil otherwise.
// Caller must hold Device.mu.
func (m *BufferManager) AddHole(seq uint64, offset uint64, length uint64) *SealedBuffer {
	// Track sequence range in active buffer
	if m.active.IsEmpty() {
		m.activeMinSeq = seq
	}
	m.activeMaxSeq = seq

	// Add hole to buffer
	m.active.AddHole(offset, length)

	// Note: We intentionally do NOT insert a hole entry into the index here.
	// Buffer holes are found via CopyAndReturnGaps() which checks buffers directly.
	// Old blob entries stay in the index until InsertBlob() replaces them.

	// Check if we should seal (block count limit - holes don't add to size)
	if m.active.BlockCount() >= MaxBlocksPerBlob {
		return m.seal(SealTriggerThresholdBlocks)
	}
	return nil
}

// Seal seals the active buffer and returns it.
// Returns (nil, marker, hasUploads) if the active buffer is empty.
// The marker is the maxSeq of the last sealed buffer, used for flush barriers.
// Caller must hold Device.mu.
func (m *BufferManager) Seal(trigger string) (*SealedBuffer, uint64, bool) {
	// Get marker for any existing sealed buffers
	var marker uint64
	hasUploads := len(m.sealed) > 0
	if hasUploads {
		marker = m.sealed[len(m.sealed)-1].maxSeq
	}

	if m.active.IsEmpty() {
		return nil, marker, hasUploads
	}

	sb := m.seal(trigger)
	marker = sb.maxSeq
	hasUploads = true
	return sb, marker, hasUploads
}

// seal seals the active buffer and creates a new one.
// The blob is NOT built here — it is created lazily during upload to avoid holding
// a duplicate copy of the data in memory while the sealed buffer waits in the queue.
// Caller must hold Device.mu.
func (m *BufferManager) seal(trigger string) *SealedBuffer {
	blobID := GenerateBlobID()

	sb := &SealedBuffer{
		maxSeq:   m.activeMaxSeq,
		buffer:   m.active,
		blobID:   blobID,
		trigger:  trigger,
		uploaded: make(chan struct{}),
	}

	SealsTotal.WithLabelValues(trigger).Inc()

	m.sealed = append(m.sealed, sb)
	m.pendingBlobs[blobID] = sb
	m.active = NewWriteBuffer()
	m.activeMinSeq = 0
	m.activeMaxSeq = 0

	return sb
}

// MarkUploaded removes a sealed buffer from the sealed list and pendingBlobs map.
// Caller must hold Device.mu.
func (m *BufferManager) MarkUploaded(sb *SealedBuffer) {
	delete(m.pendingBlobs, sb.blobID)

	for i, s := range m.sealed {
		if s == sb {
			m.sealed = append(m.sealed[:i], m.sealed[i+1:]...)
			break
		}
	}
}

// SealedBuffers returns a snapshot of sealed buffers with maxSeq <= marker.
// Used by flush to find buffers to wait on.
// Caller must hold Device.mu.
func (m *BufferManager) SealedBuffers(marker uint64) []*SealedBuffer {
	var result []*SealedBuffer
	for _, sb := range m.sealed {
		if sb.maxSeq <= marker {
			result = append(result, sb)
		}
	}
	return result
}

// CopyAndReturnGaps copies data from the active buffer and sealed buffers,
// returning unfilled gaps that must be read from the index/blobs.
// Caller must hold Device.mu.
func (m *BufferManager) CopyAndReturnGaps(dst []byte, offset uint64, length uint64) []Range {
	gaps := []Range{{Offset: offset, Length: length}}

	// Check sealed buffers first (newest data)
	// Iterate in reverse order so most recent seals take precedence
	for i := len(m.sealed) - 1; i >= 0; i-- {
		if m.sealed[i].buffer != nil {
			gaps = m.copyFromBufferAndComputeGaps(dst, offset, m.sealed[i].buffer, gaps)
		}
	}

	// Then check active buffer (newest writes)
	gaps = m.copyFromBufferAndComputeGaps(dst, offset, m.active, gaps)

	return gaps
}

// copyFromBufferAndComputeGaps copies data from buffer for needed ranges and returns remaining gaps.
func (m *BufferManager) copyFromBufferAndComputeGaps(dst []byte, baseOffset uint64, buf *WriteBuffer, needed []Range) []Range {
	var remaining []Range
	for _, r := range needed {
		// Read from buffer for this range
		dstStart := r.Offset - baseOffset
		covered := buf.ReadInto(dst[dstStart:dstStart+r.Length], r.Offset, r.Length)

		// Compute gaps (what wasn't covered)
		gaps := SubtractRanges(r, covered)
		remaining = append(remaining, gaps...)
	}
	return remaining
}

// GetPendingBlob returns the sealed buffer for a blob that is still pending upload.
// Returns nil if the blob is not pending (either already uploaded or unknown).
// Caller must hold Device.mu.
func (m *BufferManager) GetPendingBlob(blobID string) *SealedBuffer {
	return m.pendingBlobs[blobID]
}

// ActiveBufferSize returns the size of the active buffer.
// Caller must hold Device.mu.
func (m *BufferManager) ActiveBufferSize() int {
	return m.active.Size()
}

// SealedCount returns the number of sealed buffers pending upload.
// Caller must hold Device.mu.
func (m *BufferManager) SealedCount() int {
	return len(m.sealed)
}
