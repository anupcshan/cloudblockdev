package cloudblock

import (
	"io"
)

// SegmentReader implements io.ReadSeeker over multiple contiguous byte slices
// (segments). It reads sequentially across segments without copying or
// concatenating them, enabling zero-copy streaming to S3.
type SegmentReader struct {
	segments [][]byte
	size     int64 // total size across all segments
	pos      int64 // current absolute read position
}

// NewSegmentReader creates a new SegmentReader over the given byte slices.
// The segments are read in order as if they were one contiguous buffer.
// Nil or empty segments are skipped.
func NewSegmentReader(segments ...[]byte) *SegmentReader {
	// Filter out nil/empty segments and compute total size
	var filtered [][]byte
	var size int64
	for _, s := range segments {
		if len(s) > 0 {
			filtered = append(filtered, s)
			size += int64(len(s))
		}
	}
	return &SegmentReader{
		segments: filtered,
		size:     size,
	}
}

// Size returns the total number of bytes across all segments.
func (r *SegmentReader) Size() int64 {
	return r.size
}

// Read implements io.Reader.
func (r *SegmentReader) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}

	// Find which segment and offset within it corresponds to r.pos
	remaining := r.pos
	totalRead := 0

	for _, seg := range r.segments {
		segLen := int64(len(seg))
		if remaining >= segLen {
			remaining -= segLen
			continue
		}

		// Read from this segment starting at 'remaining' offset
		available := seg[remaining:]
		n := copy(p[totalRead:], available)
		totalRead += n
		r.pos += int64(n)
		remaining = 0

		if totalRead == len(p) {
			return totalRead, nil
		}
	}

	if totalRead == 0 {
		return 0, io.EOF
	}
	return totalRead, nil
}

// Seek implements io.Seeker.
func (r *SegmentReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, io.ErrNoProgress
	}
	if abs < 0 {
		return 0, io.ErrNoProgress
	}
	r.pos = abs
	return abs, nil
}

// Len returns the number of unread bytes. This is used by the AWS SDK
// to determine the content length without seeking.
func (r *SegmentReader) Len() int {
	unread := r.size - r.pos
	if unread < 0 {
		return 0
	}
	return int(unread)
}
