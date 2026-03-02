package cloudblock

import (
	"slices"
	"sort"
)

// WriteBuffer accumulates small writes and holes before flushing to S3.
// Writes are stored as non-overlapping regions; new writes/holes overwrite
// any overlapping parts of earlier writes/holes.
//
// WriteBuffer is NOT thread-safe. Synchronization is handled by BufferManager.
type WriteBuffer struct {
	writes []BufferedWrite
	size   int // Total buffered data bytes (holes excluded)
}

// BufferedWrite represents a single buffered write or hole.
type BufferedWrite struct {
	Offset uint64
	Length uint64 // Length of this region
	Data   []byte // nil for holes
	IsHole bool   // If true, this is a hole (no data)
}

// End returns the end offset (exclusive) of the write.
func (w BufferedWrite) End() uint64 {
	return w.Offset + w.Length
}

// NewWriteBuffer creates a new write buffer.
func NewWriteBuffer() *WriteBuffer {
	return &WriteBuffer{}
}

// Add adds a write to the buffer.
// If the new write overlaps with existing writes/holes, they are
// split or removed as necessary. The new write takes precedence.
//
// TakeWrite takes ownership of data. The caller must not read, write, or
// reference data after this call returns.
func (b *WriteBuffer) TakeWrite(offset uint64, data []byte) {
	newWrite := BufferedWrite{
		Offset: offset,
		Length: uint64(len(data)),
		Data:   data,
		IsHole: false,
	}

	b.insertWrite(newWrite)
}

// AddHole adds a hole (trim/zero) to the buffer.
// If the hole overlaps with existing writes/holes, they are
// split or removed as necessary. The hole takes precedence.
func (b *WriteBuffer) AddHole(offset uint64, length uint64) {
	newWrite := BufferedWrite{
		Offset: offset,
		Length: length,
		Data:   nil,
		IsHole: true,
	}

	b.insertWrite(newWrite)
}

// insertWrite handles the common logic for adding writes or holes.
func (b *WriteBuffer) insertWrite(newWrite BufferedWrite) {
	newEnd := newWrite.End()

	// Fast path: empty buffer
	if len(b.writes) == 0 {
		b.writes = []BufferedWrite{newWrite}
		if !newWrite.IsHole {
			b.size = len(newWrite.Data)
		}
		return
	}

	// Binary search to find insertion point by offset
	insertPos := sort.Search(len(b.writes), func(i int) bool {
		return b.writes[i].Offset >= newWrite.Offset
	})

	// Check if previous write overlaps (at most 1 can overlap due to non-overlapping invariant)
	first := insertPos
	if first > 0 && b.writes[first-1].End() > newWrite.Offset {
		first--
	}

	// Scan forward to find last overlapping write
	last := insertPos
	for last < len(b.writes) && b.writes[last].Offset < newEnd {
		last++
	}

	// Calculate size delta from removed writes
	removedSize := 0
	for i := first; i < last; i++ {
		if !b.writes[i].IsHole {
			removedSize += len(b.writes[i].Data)
		}
	}

	// Build replacement entries for the [first, last) range
	var replacement []BufferedWrite
	addedSize := 0

	// Keep left fragment if first overlapping write starts before new write
	if first < last && b.writes[first].Offset < newWrite.Offset {
		existing := b.writes[first]
		leftLen := newWrite.Offset - existing.Offset
		if existing.IsHole {
			replacement = append(replacement, BufferedWrite{
				Offset: existing.Offset,
				Length: leftLen,
				Data:   nil,
				IsHole: true,
			})
		} else {
			leftData := make([]byte, leftLen)
			copy(leftData, existing.Data[:leftLen])
			replacement = append(replacement, BufferedWrite{
				Offset: existing.Offset,
				Length: leftLen,
				Data:   leftData,
				IsHole: false,
			})
			addedSize += int(leftLen)
		}
	}

	// Add the new write
	replacement = append(replacement, newWrite)
	if !newWrite.IsHole {
		addedSize += len(newWrite.Data)
	}

	// Keep right fragment if last overlapping write ends after new write
	if first < last && b.writes[last-1].End() > newEnd {
		existing := b.writes[last-1]
		rightStart := newEnd - existing.Offset
		rightLen := existing.End() - newEnd
		if existing.IsHole {
			replacement = append(replacement, BufferedWrite{
				Offset: newEnd,
				Length: rightLen,
				Data:   nil,
				IsHole: true,
			})
		} else {
			rightData := make([]byte, rightLen)
			copy(rightData, existing.Data[rightStart:])
			replacement = append(replacement, BufferedWrite{
				Offset: newEnd,
				Length: rightLen,
				Data:   rightData,
				IsHole: false,
			})
			addedSize += int(rightLen)
		}
	}

	// Splice: writes[:first] + replacement + writes[last:]
	b.writes = slices.Replace(b.writes, first, last, replacement...)
	b.size = b.size - removedSize + addedSize
}

// Size returns the total buffered data bytes (holes excluded).
func (b *WriteBuffer) Size() int {
	return b.size
}

// IsEmpty returns true if the buffer has no writes or holes.
func (b *WriteBuffer) IsEmpty() bool {
	return len(b.writes) == 0
}

// BlockCount returns the number of non-overlapping regions (writes + holes).
// This is the maximum number of blocks that could be created (before merging adjacent regions).
func (b *WriteBuffer) BlockCount() int {
	return len(b.writes)
}

// ReadInto copies data from the buffer directly into dst for the given range.
// Holes are skipped (dst already contains zeros for those regions).
// Returns a list of sub-ranges that were found and copied.
func (b *WriteBuffer) ReadInto(dst []byte, offset uint64, length uint64) []Range {
	var found []Range
	rEnd := offset + length

	for _, w := range b.writes {
		wEnd := w.End()

		// Check for overlap
		if w.Offset >= rEnd || wEnd <= offset {
			continue // No overlap
		}

		// Skip holes - dst already contains zeros
		if w.IsHole {
			continue
		}

		// Calculate overlap region
		overlapStart := max(w.Offset, offset)
		overlapEnd := min(wEnd, rEnd)

		// Copy overlapping data directly to dst
		srcStart := overlapStart - w.Offset
		dstStart := overlapStart - offset
		copyLen := overlapEnd - overlapStart

		copy(dst[dstStart:dstStart+copyLen], w.Data[srcStart:srcStart+copyLen])

		found = append(found, Range{
			Offset: overlapStart,
			Length: copyLen,
		})
	}

	return found
}

// Range represents a byte range.
type Range struct {
	Offset uint64
	Length uint64
}

// ToBlob converts buffered writes and holes to a blob.
// The buffer is NOT cleared; call Clear() after successful upload.
// Since Add()/AddHole() maintains non-overlapping writes, this just needs to
// merge adjacent regions and create the blob.
func (b *WriteBuffer) ToBlob() (*Blob, error) {
	if len(b.writes) == 0 {
		return nil, nil
	}

	blocks, dataSlices := b.mergeBlocks()
	return NewBlob(blocks, dataSlices)
}

// ToBlobParts returns block entries and their data slices for streaming to S3.
// Adjacent writes of the same type are merged at the BlockEntry level, but
// data slices are kept separate (not concatenated) to avoid large allocations.
// The SegmentReader streams the individual data slices directly to S3,
// so there is no need for them to be contiguous in memory.
// Returns nil, nil if the buffer is empty.
func (b *WriteBuffer) ToBlobParts() ([]BlockEntry, [][]byte) {
	if len(b.writes) == 0 {
		return nil, nil
	}

	// Sort writes by offset (they're already non-overlapping)
	sorted := make([]BufferedWrite, len(b.writes))
	copy(sorted, b.writes)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Offset < sorted[j].Offset
	})

	// Build block entries and data slices. Adjacent writes of the same type
	// are merged into a single BlockEntry, but data slices remain separate.
	blocks := make([]BlockEntry, 0, len(sorted))
	dataSlices := make([][]byte, 0, len(sorted))

	for _, w := range sorted {
		// Try to merge with the last block if adjacent and same type
		if len(blocks) > 0 {
			last := &blocks[len(blocks)-1]
			if w.IsHole == last.IsHole && w.Offset == last.Offset+last.Length {
				last.Length += w.Length
				if !w.IsHole {
					dataSlices = append(dataSlices, w.Data)
				}
				continue
			}
		}

		blocks = append(blocks, BlockEntry{
			Offset: w.Offset,
			Length: w.Length,
			IsHole: w.IsHole,
		})
		if w.IsHole {
			dataSlices = append(dataSlices, nil)
		} else {
			dataSlices = append(dataSlices, w.Data)
		}
	}

	return blocks, dataSlices
}

// mergeBlocks sorts writes by offset and merges adjacent regions of the same type.
// Returns the merged block entries and their corresponding data slices (nil for holes).
func (b *WriteBuffer) mergeBlocks() ([]BlockEntry, [][]byte) {
	// Sort writes by offset (they're already non-overlapping)
	sorted := make([]BufferedWrite, len(b.writes))
	copy(sorted, b.writes)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Offset < sorted[j].Offset
	})

	// Merge adjacent regions of the same type
	var merged []BufferedWrite
	for _, w := range sorted {
		if len(merged) == 0 {
			merged = append(merged, BufferedWrite{
				Offset: w.Offset,
				Length: w.Length,
				Data:   w.Data,
				IsHole: w.IsHole,
			})
			continue
		}

		last := &merged[len(merged)-1]
		lastEnd := last.End()

		// Can only merge if adjacent AND same type (both holes or both data)
		if w.Offset == lastEnd && w.IsHole == last.IsHole {
			if w.IsHole {
				// Merge adjacent holes
				last.Length += w.Length
			} else {
				// Merge adjacent data writes
				last.Length += w.Length
				last.Data = append(last.Data, w.Data...)
			}
		} else {
			// Gap or different type - start a new region
			merged = append(merged, BufferedWrite{
				Offset: w.Offset,
				Length: w.Length,
				Data:   w.Data,
				IsHole: w.IsHole,
			})
		}
	}

	// Convert merged writes to blocks and data
	blocks := make([]BlockEntry, len(merged))
	dataSlices := make([][]byte, len(merged))
	for i, w := range merged {
		blocks[i] = BlockEntry{
			Offset: w.Offset,
			Length: w.Length,
			IsHole: w.IsHole,
		}
		if w.IsHole {
			dataSlices[i] = nil
		} else {
			dataSlices[i] = w.Data
		}
	}

	return blocks, dataSlices
}

// Clear empties the buffer.
func (b *WriteBuffer) Clear() {
	b.writes = nil
	b.size = 0
}

// Writes returns a copy of all buffered writes, sorted by offset.
func (b *WriteBuffer) Writes() []BufferedWrite {
	result := make([]BufferedWrite, len(b.writes))
	copy(result, b.writes)

	sort.Slice(result, func(i, j int) bool {
		return result[i].Offset < result[j].Offset
	})

	return result
}

// SubtractRanges returns parts of `from` not covered by `covered`.
// Both `from` and `covered` are assumed to be sorted by offset.
func SubtractRanges(from Range, covered []Range) []Range {
	if len(covered) == 0 {
		return []Range{from}
	}

	var gaps []Range
	current := from.Offset
	end := from.Offset + from.Length

	for _, c := range covered {
		cEnd := c.Offset + c.Length

		// Skip if covered range is before current position
		if cEnd <= current {
			continue
		}

		// If covered range starts after current, there's a gap
		if c.Offset > current {
			gapEnd := min(c.Offset, end)
			if gapEnd > current {
				gaps = append(gaps, Range{Offset: current, Length: gapEnd - current})
			}
		}

		// Move current past the covered range
		current = max(current, cEnd)

		// If we've passed the end of `from`, we're done
		if current >= end {
			break
		}
	}

	// If there's remaining uncovered space at the end
	if current < end {
		gaps = append(gaps, Range{Offset: current, Length: end - current})
	}

	return gaps
}
