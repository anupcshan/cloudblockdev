package cloudblock

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/anupcshan/cloudblockdev/internal/cloudblock/pb"
	"google.golang.org/protobuf/proto"
)

// SegmentPrefix is the S3 key prefix for index segments.
const SegmentPrefix = "index/"

// IndexSegment represents a batch of blob headers stored in S3.
// Segments cover a range of blob timestamps and are used for
// cold-start recovery without full blob listing.
type IndexSegment struct {
	// Epoch is the compaction epoch. Epoch 0 is for normal segments.
	// Higher epochs are for compacted segments that supersede lower-epoch ones.
	Epoch int
	// StartTimestampExclusive is the minimum timestamp (inclusive) of blobs in this segment.
	StartTimestampExclusive int64
	// EndTimestampInclusive is the maximum timestamp (inclusive) of blobs in this segment.
	EndTimestampInclusive int64
	// Blobs contains the blob entries in this segment, sorted by timestamp.
	Blobs []IndexSegmentEntry
}

// IndexSegmentEntry represents a single blob's metadata in a segment.
type IndexSegmentEntry struct {
	ID     string
	Header BlobHeader
}

// SegmentKey generates the S3 key for a segment with the given epoch and timestamp range.
// Format: index/segment-{epoch}-{start_ts}-{end_ts}.idx
// Epoch 0 is for normal segments. Higher epochs are for compacted segments.
func SegmentKey(epoch int, start, end int64) string {
	return fmt.Sprintf("%ssegment-%03d-%013d-%013d.idx", SegmentPrefix, epoch, start, end)
}

// ParseSegmentKey extracts the epoch and timestamp range from a segment key.
// Returns an error if the key format is invalid.
// Also handles legacy format without epoch (treats as epoch 0).
func ParseSegmentKey(key string) (epoch int, start, end int64, err error) {
	// Remove prefix
	if !strings.HasPrefix(key, SegmentPrefix) {
		return 0, 0, 0, fmt.Errorf("invalid segment key: missing prefix")
	}
	name := strings.TrimPrefix(key, SegmentPrefix)

	// Parse "segment-{epoch}-{start}-{end}.idx" or legacy "segment-{start}-{end}.idx"
	if !strings.HasPrefix(name, "segment-") || !strings.HasSuffix(name, ".idx") {
		return 0, 0, 0, fmt.Errorf("invalid segment key format: %s", key)
	}

	name = strings.TrimPrefix(name, "segment-")
	name = strings.TrimSuffix(name, ".idx")

	parts := strings.Split(name, "-")
	if len(parts) == 2 {
		// Legacy format: segment-{start}-{end}.idx (epoch 0)
		epoch = 0
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid start timestamp: %w", err)
		}
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid end timestamp: %w", err)
		}
	} else if len(parts) == 3 {
		// New format: segment-{epoch}-{start}-{end}.idx
		epochVal, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid epoch: %w", err)
		}
		epoch = int(epochVal)
		start, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid start timestamp: %w", err)
		}
		end, err = strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid end timestamp: %w", err)
		}
	} else {
		return 0, 0, 0, fmt.Errorf("invalid segment key format: %s", key)
	}

	return epoch, start, end, nil
}

// NewIndexSegment creates a new segment covering the range (startTS, endTS].
// startTS and endTS define the coverage range, not the blob timestamps.
// startTS is the end of the previous segment (0 if this is the first).
func NewIndexSegment(startTS, endTS int64, entries []IndexSegmentEntry) *IndexSegment {
	return &IndexSegment{
		StartTimestampExclusive: startTS,
		EndTimestampInclusive:   endTS,
		Blobs:          entries,
	}
}

// toProto converts IndexSegment to its protobuf representation.
func (s *IndexSegment) toProto() *pb.IndexSegment {
	blobs := make([]*pb.IndexSegmentEntry, len(s.Blobs))
	for i, b := range s.Blobs {
		blobs[i] = pb.IndexSegmentEntry_builder{
			Id:     b.ID,
			Header: b.Header.toProto(),
		}.Build()
	}
	return pb.IndexSegment_builder{
		Epoch:          int32(s.Epoch),
		StartTimestampExclusive: s.StartTimestampExclusive,
		EndTimestampInclusive:   s.EndTimestampInclusive,
		Blobs:          blobs,
	}.Build()
}

// fromProto populates IndexSegment from its protobuf representation.
func (s *IndexSegment) fromProto(pbSeg *pb.IndexSegment) {
	s.Epoch = int(pbSeg.GetEpoch())
	s.StartTimestampExclusive = pbSeg.GetStartTimestampExclusive()
	s.EndTimestampInclusive = pbSeg.GetEndTimestampInclusive()
	s.Blobs = make([]IndexSegmentEntry, len(pbSeg.GetBlobs()))
	for i, b := range pbSeg.GetBlobs() {
		s.Blobs[i].ID = b.GetId()
		s.Blobs[i].Header.fromProto(b.GetHeader())
		// Compute HeaderSize from proto size (not stored in proto to avoid chicken-and-egg)
		if protoData, err := proto.Marshal(b.GetHeader()); err == nil {
			s.Blobs[i].Header.HeaderSize = uint32(BlobHeaderFixed + len(protoData))
		}
	}
}

// Marshal serializes the segment to protobuf.
func (s *IndexSegment) Marshal() ([]byte, error) {
	return proto.Marshal(s.toProto())
}

// UnmarshalSegment deserializes a segment from protobuf.
func UnmarshalSegment(data []byte) (*IndexSegment, error) {
	var pbSeg pb.IndexSegment
	if err := proto.Unmarshal(data, &pbSeg); err != nil {
		return nil, fmt.Errorf("unmarshal segment: %w", err)
	}
	segment := &IndexSegment{}
	segment.fromProto(&pbSeg)
	return segment, nil
}

// Key returns the S3 key for this segment.
func (s *IndexSegment) Key() string {
	return SegmentKey(s.Epoch, s.StartTimestampExclusive, s.EndTimestampInclusive)
}

// Len returns the number of blobs in the segment.
func (s *IndexSegment) Len() int {
	return len(s.Blobs)
}
