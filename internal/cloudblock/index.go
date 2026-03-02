package cloudblock

import (
	"errors"
	"slices"
	"sync"

	"github.com/tidwall/btree"
)

// ErrUnknownBlob is returned when an operation references a blob not in the index.
var ErrUnknownBlob = errors.New("unknown blob")

// isNewerBlob returns true if blobA is newer than blobB based on blob ID timestamps.
// Blob IDs are monotonically increasing timestamps, so higher = newer.
// Returns false if either ID is empty (buffer entries) or can't be parsed.
func isNewerBlob(blobA, blobB string) bool {
	if blobA == "" || blobB == "" {
		return false
	}
	tsA, errA := ParseBlobIDTimestamp(blobA)
	tsB, errB := ParseBlobIDTimestamp(blobB)
	if errA != nil || errB != nil {
		return false
	}
	return tsA > tsB
}

// LiveRange describes the contiguous range of live data within a blob.
type LiveRange struct {
	Start     uint64 // Start offset within blob (from first live byte)
	End       uint64 // End offset within blob (after last live byte)
	LiveBytes int64  // Total live bytes (may be less than End-Start if holes in middle)
}

// RangeEntry describes where a byte range is stored.
type RangeEntry struct {
	Offset     uint64 // Start offset in device
	Length     uint64 // Length in bytes
	BlobID     string // Blob containing this data (empty for buffered writes)
	BlobOffset uint32 // Offset within blob's data section
	IsHole     bool   // If true, this range is a hole (returns zeros on read)
}

// End returns the end offset (exclusive) of the range.
func (r RangeEntry) End() uint64 {
	return r.Offset + r.Length
}

// rangeEntryLess compares RangeEntry by Offset for B-tree ordering.
func rangeEntryLess(a, b *RangeEntry) bool {
	return a.Offset < b.Offset
}

// RangeIndex tracks which byte ranges are stored in which blobs.
// It maintains a B-tree of non-overlapping ranges sorted by offset and a reverse
// index from blob ID to its ranges for efficient blob-centric queries.
//
// GC Safety: Old blob entries stay in the index until InsertBlob() replaces them
// with new blob entries. Since InsertBlob() is only called after successful S3 upload,
// this provides implicit GC protection - blobs remain "referenced" until their
// replacement is safely stored.
type RangeIndex struct {
	mu         sync.RWMutex
	ranges     *btree.BTreeG[*RangeEntry] // sorted by Offset, non-overlapping
	blobRanges map[string][]*RangeEntry   // reverse index: blobID -> ranges
	knownBlobs map[string]bool            // tracks which blob IDs have been indexed
}

// NewRangeIndex creates a new empty range index.
func NewRangeIndex() *RangeIndex {
	return &RangeIndex{
		ranges:     btree.NewBTreeGOptions(rangeEntryLess, btree.Options{NoLocks: true}),
		blobRanges: make(map[string][]*RangeEntry),
		knownBlobs: make(map[string]bool),
	}
}

// Insert adds a new range to the index.
// If the new range overlaps with existing ranges, the existing ranges are
// split or removed as necessary. The new range takes precedence.
func (idx *RangeIndex) Insert(entry RangeEntry) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.insertLocked(&entry)
}

// insertLocked is the internal insert implementation (caller must hold lock).
// Uses timestamp-based comparison: newer blobs always win. If an overlapping
// entry is from a newer blob (higher timestamp), it is preserved and the new
// entry is split around it. This handles out-of-order blob uploads correctly.
func (idx *RangeIndex) insertLocked(entry *RangeEntry) {
	newEnd := entry.End()

	// Collect ALL overlapping ranges (we'll filter by timestamp later)
	var allOverlapping []*RangeEntry

	// Check if there's a range starting before entry.Offset that overlaps.
	pivot := &RangeEntry{Offset: entry.Offset}
	idx.ranges.Descend(pivot, func(r *RangeEntry) bool {
		if r.Offset < entry.Offset && r.End() > entry.Offset {
			allOverlapping = append(allOverlapping, r)
		}
		return false // stop after checking the first
	})

	// Find all ranges starting at or after entry.Offset but before newEnd.
	idx.ranges.Ascend(pivot, func(r *RangeEntry) bool {
		if r.Offset >= newEnd {
			return false // past our range
		}
		allOverlapping = append(allOverlapping, r)
		return true
	})

	// Separate overlapping ranges into older (to be replaced) and newer (to be kept)
	var olderOverlapping []*RangeEntry
	var newerOverlapping []*RangeEntry
	for _, r := range allOverlapping {
		if isNewerBlob(r.BlobID, entry.BlobID) {
			newerOverlapping = append(newerOverlapping, r)
		} else {
			olderOverlapping = append(olderOverlapping, r)
		}
	}

	// If completely covered by newer blobs, skip the insert entirely
	if len(newerOverlapping) > 0 && len(olderOverlapping) == 0 {
		// Check if newer blobs completely cover the new entry
		covered := uint64(0)
		for _, r := range newerOverlapping {
			overlapStart := max(entry.Offset, r.Offset)
			overlapEnd := min(newEnd, r.End())
			if overlapEnd > overlapStart {
				covered += overlapEnd - overlapStart
			}
		}
		if covered >= entry.Length {
			return // Completely covered by newer blobs, nothing to insert
		}
	}

	// Remove only the OLDER overlapping ranges from tree and reverse index
	var firstOlderOverlap, lastOlderOverlap *RangeEntry
	for _, r := range olderOverlapping {
		idx.ranges.Delete(r)
		idx.removeFromReverseIndex(r)
		if firstOlderOverlap == nil || r.Offset < firstOlderOverlap.Offset {
			firstOlderOverlap = r
		}
		if lastOlderOverlap == nil || r.End() > lastOlderOverlap.End() {
			lastOlderOverlap = r
		}
	}

	// Create left fragment if firstOlderOverlap starts before new entry
	if firstOlderOverlap != nil && firstOlderOverlap.Offset < entry.Offset {
		leftFrag := &RangeEntry{
			Offset:     firstOlderOverlap.Offset,
			Length:     entry.Offset - firstOlderOverlap.Offset,
			BlobID:     firstOlderOverlap.BlobID,
			BlobOffset: firstOlderOverlap.BlobOffset,
			IsHole:     firstOlderOverlap.IsHole,
		}
		idx.ranges.Set(leftFrag)
		idx.addToReverseIndex(leftFrag)
	}

	// Add the new entry, but split around any newer overlapping ranges
	if len(newerOverlapping) == 0 {
		// No newer overlaps - add the entire entry
		idx.ranges.Set(entry)
		idx.addToReverseIndex(entry)
	} else {
		// Split new entry around newer overlaps
		idx.insertAroundNewerBlobs(entry, newerOverlapping)
	}

	// Create right fragment if lastOlderOverlap ends after new entry
	if lastOlderOverlap != nil && lastOlderOverlap.End() > newEnd {
		rightFrag := &RangeEntry{
			Offset:     newEnd,
			Length:     lastOlderOverlap.End() - newEnd,
			BlobID:     lastOlderOverlap.BlobID,
			BlobOffset: lastOlderOverlap.BlobOffset + uint32(newEnd-lastOlderOverlap.Offset),
			IsHole:     lastOlderOverlap.IsHole,
		}
		idx.ranges.Set(rightFrag)
		idx.addToReverseIndex(rightFrag)
	}
}

// insertAroundNewerBlobs inserts parts of entry that don't overlap with newer blobs.
// newerOverlapping must be sorted by offset for correct gap calculation.
func (idx *RangeIndex) insertAroundNewerBlobs(entry *RangeEntry, newerOverlapping []*RangeEntry) {
	// Sort newer overlaps by offset
	slices.SortFunc(newerOverlapping, func(a, b *RangeEntry) int {
		if a.Offset < b.Offset {
			return -1
		}
		if a.Offset > b.Offset {
			return 1
		}
		return 0
	})

	// Insert fragments in gaps between newer blobs
	current := entry.Offset
	entryEnd := entry.End()
	dataOffset := entry.BlobOffset

	for _, newer := range newerOverlapping {
		if newer.Offset > current && current < entryEnd {
			// Gap before this newer blob - insert fragment
			fragEnd := min(newer.Offset, entryEnd)
			frag := &RangeEntry{
				Offset:     current,
				Length:     fragEnd - current,
				BlobID:     entry.BlobID,
				BlobOffset: dataOffset,
				IsHole:     entry.IsHole,
			}
			idx.ranges.Set(frag)
			idx.addToReverseIndex(frag)
		}
		// Skip past this newer blob
		if newer.End() > current {
			skipped := newer.End() - max(current, newer.Offset)
			if !entry.IsHole {
				dataOffset += uint32(skipped)
			}
			current = newer.End()
		}
	}

	// Insert final fragment after all newer blobs
	if current < entryEnd {
		frag := &RangeEntry{
			Offset:     current,
			Length:     entryEnd - current,
			BlobID:     entry.BlobID,
			BlobOffset: dataOffset,
			IsHole:     entry.IsHole,
		}
		idx.ranges.Set(frag)
		idx.addToReverseIndex(frag)
	}
}

// addToReverseIndex adds an entry to the reverse index.
// Caller must hold the lock.
func (idx *RangeIndex) addToReverseIndex(entry *RangeEntry) {
	if entry.BlobID == "" {
		return // buffered writes don't go in reverse index
	}
	idx.blobRanges[entry.BlobID] = append(idx.blobRanges[entry.BlobID], entry)
}

// removeFromReverseIndex removes an entry from the reverse index.
// Caller must hold the lock.
func (idx *RangeIndex) removeFromReverseIndex(entry *RangeEntry) {
	if entry.BlobID == "" {
		return
	}
	ranges := idx.blobRanges[entry.BlobID]
	for i, r := range ranges {
		if r == entry { // pointer comparison
			idx.blobRanges[entry.BlobID] = slices.Delete(ranges, i, i+1)
			break
		}
	}
	// Clean up empty entries
	if len(idx.blobRanges[entry.BlobID]) == 0 {
		delete(idx.blobRanges, entry.BlobID)
	}
}

// InsertHole marks a range as a hole (trimmed/zeroed region).
// Reads to this range will return zeros.
func (idx *RangeIndex) InsertHole(offset uint64, length uint64) {
	idx.Insert(RangeEntry{
		Offset: offset,
		Length: length,
		BlobID: "", // Empty = in buffer
		IsHole: true,
	})
}

// Lookup finds all ranges that overlap with the given range.
func (idx *RangeIndex) Lookup(offset uint64, length uint64) []RangeEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.ranges.Len() == 0 {
		return nil
	}

	end := offset + length
	var result []RangeEntry

	// Check if there's a range starting before offset that overlaps.
	// Descend from offset finds ranges with Offset <= offset (highest first).
	// We only consider entries with Offset < offset here; entries starting at
	// offset will be handled by Ascend below.
	pivot := &RangeEntry{Offset: offset}
	idx.ranges.Descend(pivot, func(r *RangeEntry) bool {
		if r.Offset < offset && r.End() > offset {
			result = append(result, *r)
		}
		return false // stop after checking the first
	})

	// Find all ranges starting at or after offset but before end.
	idx.ranges.Ascend(pivot, func(r *RangeEntry) bool {
		if r.Offset >= end {
			return false // past our range
		}
		result = append(result, *r)
		return true
	})

	return result
}

// FindGaps returns ranges within [offset, offset+length) that are NOT covered
// by any entries in the index.
func (idx *RangeIndex) FindGaps(offset uint64, length uint64) []Range {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	end := offset + length
	var gaps []Range
	current := offset

	// Check if there's a range starting before offset that covers part of our range.
	pivot := &RangeEntry{Offset: offset}
	idx.ranges.Descend(pivot, func(r *RangeEntry) bool {
		if r.End() > current {
			current = r.End()
		}
		return false // only check one
	})

	// Scan ranges starting at or after offset
	idx.ranges.Ascend(pivot, func(r *RangeEntry) bool {
		if r.Offset >= end {
			return false // past our range
		}

		// Gap before this range?
		if r.Offset > current {
			gapEnd := min(r.Offset, end)
			gaps = append(gaps, Range{
				Offset: current,
				Length: gapEnd - current,
			})
		}

		// Move current past this range
		if r.End() > current {
			current = r.End()
		}
		return true
	})

	// Gap after last range?
	if current < end {
		gaps = append(gaps, Range{
			Offset: current,
			Length: end - current,
		})
	}

	return gaps
}

// All returns all ranges in the index, sorted by offset.
func (idx *RangeIndex) All() []RangeEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([]RangeEntry, 0, idx.ranges.Len())
	idx.ranges.Scan(func(r *RangeEntry) bool {
		result = append(result, *r)
		return true
	})
	return result
}

// Len returns the number of ranges in the index.
func (idx *RangeIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.ranges.Len()
}

// Clear removes all ranges from the index.
func (idx *RangeIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.ranges.Clear()
	idx.blobRanges = make(map[string][]*RangeEntry)
	idx.knownBlobs = make(map[string]bool)
}

// InsertBlobEntries adds all blocks from a blob to the index.
// Holes are tracked but don't contribute to data offset calculations.
// Does NOT release GC protections - call ReleaseBlobProtections after upload succeeds.
func (idx *RangeIndex) InsertBlobEntries(blobID string, header *BlobHeader) {
	dataOffset := uint32(0)
	for _, block := range header.Blocks {
		idx.Insert(RangeEntry{
			Offset:     block.Offset,
			Length:     block.Length,
			BlobID:     blobID,
			BlobOffset: header.HeaderSize + dataOffset,
			IsHole:     block.IsHole,
		})
		// Only advance data offset for non-hole blocks
		if !block.IsHole {
			dataOffset += uint32(block.Length)
		}
	}
	// Track that we've indexed this blob
	idx.mu.Lock()
	idx.knownBlobs[blobID] = true
	idx.mu.Unlock()
}

// InsertBlob adds all blocks from a blob to the index and releases GC protections.
// This is a convenience wrapper for recovery paths where entries and protections
// should be handled together.
func (idx *RangeIndex) InsertBlob(blobID string, header *BlobHeader) {
	idx.InsertBlobEntries(blobID, header)
	idx.ReleaseBlobProtections(blobID)
}

// ReleaseBlobProtections is a no-op kept for API compatibility.
// GC protection is now implicit: old blob entries stay in the index
// until InsertBlob() replaces them with new entries.
func (idx *RangeIndex) ReleaseBlobProtections(blobID string) {
	// No-op: protection is implicit via index entries
}

// RemoveBlobEntries removes all index entries for a blob.
// Called when an upload fails and the index entries need to be rolled back.
func (idx *RangeIndex) RemoveBlobEntries(blobID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Remove all entries for this blob from the btree
	entries := idx.blobRanges[blobID]
	for _, entry := range entries {
		idx.ranges.Delete(entry)
	}

	// Clean up tracking
	delete(idx.knownBlobs, blobID)
	delete(idx.blobRanges, blobID)
}

// HasBlob returns true if the blob ID has been indexed.
func (idx *RangeIndex) HasBlob(blobID string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.knownBlobs[blobID]
}

// RemoveBlob removes a blob from the index's tracking.
// Called after a blob has been deleted from storage.
func (idx *RangeIndex) RemoveBlob(blobID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.knownBlobs, blobID)
	delete(idx.blobRanges, blobID)
}

// GetLiveRange returns the contiguous range of live data for a blob.
// The returned range covers from the first live byte to the last live byte,
// allowing a single S3 range request to fetch all needed data.
// Returns ErrUnknownBlob if the blob is not in the index.
func (idx *RangeIndex) GetLiveRange(blobID string) (LiveRange, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.knownBlobs[blobID] {
		return LiveRange{}, ErrUnknownBlob
	}

	ranges := idx.blobRanges[blobID]
	if len(ranges) == 0 {
		// Blob is known but has no live ranges (all overwritten)
		return LiveRange{}, nil
	}

	// Find min/max blob offsets and sum live bytes
	var minOffset, maxOffset uint64 = ^uint64(0), 0
	var liveBytes int64

	for _, r := range ranges {
		// BlobOffset is where this range starts in the blob
		start := uint64(r.BlobOffset)
		end := start + r.Length

		if start < minOffset {
			minOffset = start
		}
		if end > maxOffset {
			maxOffset = end
		}
		if !r.IsHole {
			liveBytes += int64(r.Length)
		}
	}

	return LiveRange{
		Start:     minOffset,
		End:       maxOffset,
		LiveBytes: liveBytes,
	}, nil
}

// GetUnreferencedBlobs returns blob IDs that are known but no longer referenced.
// These blobs can be safely deleted from storage.
//
// GC safety is ensured because old blob entries stay in the index until
// InsertBlob() replaces them. Since InsertBlob() is only called after
// successful S3 upload, blobs remain "referenced" until their replacement
// is safely stored.
func (idx *RangeIndex) GetUnreferencedBlobs() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Build set of referenced blobs from reverse index
	referenced := make(map[string]bool, len(idx.blobRanges))
	for blobID := range idx.blobRanges {
		referenced[blobID] = true
	}

	// Find known blobs that are not referenced
	var unreferenced []string
	for blobID := range idx.knownBlobs {
		if !referenced[blobID] {
			unreferenced = append(unreferenced, blobID)
		}
	}

	return unreferenced
}
