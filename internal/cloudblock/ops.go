package cloudblock

import (
	"sync"

	"github.com/anupcshan/cloudblockdev/internal/nbd"
)

// OpType represents the type of operation.
type OpType int

const (
	OpRead OpType = iota
	OpWrite
	OpWriteFUA // Force Unit Access - must be durable before reply
	OpFlush
	OpTrim
	OpTrimFUA // Force Unit Access for trim/write-zeroes
)

// Operation represents an async operation submitted to the device.
type Operation struct {
	Seq    uint64
	Type   OpType
	Handle uint64 // NBD handle for reply correlation
	Offset int64
	Length int
	Data   []byte            // for writes
	Done   chan nbd.OpResult // result channel
}

// OpTracker tracks in-flight write operations for flush barriers.
// It assigns sequence numbers and allows waiting for writes to complete.
type OpTracker struct {
	mu      sync.Mutex
	nextSeq uint64
	pending map[uint64]struct{} // in-flight write seqs
	cond    *sync.Cond
}

// NewOpTracker creates a new operation tracker.
func NewOpTracker() *OpTracker {
	t := &OpTracker{
		pending: make(map[uint64]struct{}),
	}
	t.cond = sync.NewCond(&t.mu)
	return t
}

// NextSeq returns the next sequence number.
func (t *OpTracker) NextSeq() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	seq := t.nextSeq
	t.nextSeq++
	return seq
}

// TrackWrite records that a write with the given seq is in progress.
func (t *OpTracker) TrackWrite(seq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending[seq] = struct{}{}
}

// CompleteWrite marks a write as complete and signals any waiters.
func (t *OpTracker) CompleteWrite(seq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pending, seq)
	t.cond.Broadcast()
}

// WaitForWritesUntil blocks until all writes with seq < targetSeq are complete.
// This is used by flush to ensure all prior writes are in buffers.
func (t *OpTracker) WaitForWritesUntil(targetSeq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for {
		// Check if any pending write has seq < targetSeq
		hasPending := false
		for seq := range t.pending {
			if seq < targetSeq {
				hasPending = true
				break
			}
		}
		if !hasPending {
			return
		}
		t.cond.Wait()
	}
}

// PendingCount returns the number of in-flight writes (for testing/debugging).
func (t *OpTracker) PendingCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}
