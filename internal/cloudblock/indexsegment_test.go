package cloudblock

import (
	"testing"
)

func TestSegmentKey(t *testing.T) {
	// Test epoch 0
	key := SegmentKey(0, 1000, 2000)
	expected := "index/segment-000-0000000001000-0000000002000.idx"
	if key != expected {
		t.Errorf("SegmentKey: got %q, want %q", key, expected)
	}

	// Test non-zero epoch
	key = SegmentKey(5, 1000, 2000)
	expected = "index/segment-005-0000000001000-0000000002000.idx"
	if key != expected {
		t.Errorf("SegmentKey with epoch: got %q, want %q", key, expected)
	}
}

func TestParseSegmentKey(t *testing.T) {
	tests := []struct {
		key       string
		wantEpoch int
		wantStart int64
		wantEnd   int64
		wantErr   bool
	}{
		{
			// Legacy format (no epoch) - treated as epoch 0
			key:       "index/segment-0000000001000-0000000002000.idx",
			wantEpoch: 0,
			wantStart: 1000,
			wantEnd:   2000,
			wantErr:   false,
		},
		{
			// New format with epoch
			key:       "index/segment-000-0000000001000-0000000002000.idx",
			wantEpoch: 0,
			wantStart: 1000,
			wantEnd:   2000,
			wantErr:   false,
		},
		{
			// New format with non-zero epoch
			key:       "index/segment-005-1234567890123-1234567890456.idx",
			wantEpoch: 5,
			wantStart: 1234567890123,
			wantEnd:   1234567890456,
			wantErr:   false,
		},
		{
			key:     "blobs/something.blob",
			wantErr: true,
		},
		{
			key:     "index/invalid.idx",
			wantErr: true,
		},
		{
			key:     "index/segment-abc-def.idx",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		epoch, start, end, err := ParseSegmentKey(tt.key)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseSegmentKey(%q): expected error", tt.key)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSegmentKey(%q): %v", tt.key, err)
			continue
		}
		if epoch != tt.wantEpoch {
			t.Errorf("ParseSegmentKey(%q): epoch = %d, want %d", tt.key, epoch, tt.wantEpoch)
		}
		if start != tt.wantStart {
			t.Errorf("ParseSegmentKey(%q): start = %d, want %d", tt.key, start, tt.wantStart)
		}
		if end != tt.wantEnd {
			t.Errorf("ParseSegmentKey(%q): end = %d, want %d", tt.key, end, tt.wantEnd)
		}
	}
}

func TestSegmentKeyRoundTrip(t *testing.T) {
	epoch := 3
	start := int64(1234567890123)
	end := int64(1234567890456)

	key := SegmentKey(epoch, start, end)
	gotEpoch, gotStart, gotEnd, err := ParseSegmentKey(key)
	if err != nil {
		t.Fatalf("ParseSegmentKey: %v", err)
	}
	if gotEpoch != epoch {
		t.Errorf("Epoch: got %d, want %d", gotEpoch, epoch)
	}
	if gotStart != start {
		t.Errorf("Start: got %d, want %d", gotStart, start)
	}
	if gotEnd != end {
		t.Errorf("End: got %d, want %d", gotEnd, end)
	}
}

func TestNewIndexSegment(t *testing.T) {
	entries := []IndexSegmentEntry{
		{
			ID:     "0000000001000-00000001",
			Header: BlobHeader{Timestamp: 1000, HeaderSize: 22},
		},
		{
			ID:     "0000000003000-00000003",
			Header: BlobHeader{Timestamp: 3000, HeaderSize: 22},
		},
		{
			ID:     "0000000002000-00000002",
			Header: BlobHeader{Timestamp: 2000, HeaderSize: 22},
		},
	}

	segment := NewIndexSegment(0, 3000, entries)

	if segment.StartTimestampExclusive != 0 {
		t.Errorf("StartTimestampExclusive: got %d, want 0", segment.StartTimestampExclusive)
	}
	if segment.EndTimestampInclusive != 3000 {
		t.Errorf("EndTimestampInclusive: got %d, want 3000", segment.EndTimestampInclusive)
	}
	if segment.Len() != 3 {
		t.Errorf("Len: got %d, want 3", segment.Len())
	}
}

func TestIndexSegmentMarshalUnmarshal(t *testing.T) {
	segment := &IndexSegment{
		StartTimestampExclusive: 1000,
		EndTimestampInclusive:   2000,
		Blobs: []IndexSegmentEntry{
			{
				ID: "0000000001000-00000001",
				Header: BlobHeader{
					Timestamp:  1000,
					HeaderSize: 34,
					Blocks: []BlockEntry{
						{Offset: 0, Length: 100},
						{Offset: 1000, Length: 200},
					},
				},
			},
			{
				ID: "0000000002000-00000002",
				Header: BlobHeader{
					Timestamp:  2000,
					HeaderSize: 22,
					Blocks:     []BlockEntry{{Offset: 500, Length: 50}},
				},
			},
		},
	}

	// Marshal
	data, err := segment.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Unmarshal
	got, err := UnmarshalSegment(data)
	if err != nil {
		t.Fatalf("UnmarshalSegment: %v", err)
	}

	// Verify
	if got.StartTimestampExclusive != segment.StartTimestampExclusive {
		t.Errorf("StartTimestampExclusive: got %d, want %d", got.StartTimestampExclusive, segment.StartTimestampExclusive)
	}
	if got.EndTimestampInclusive != segment.EndTimestampInclusive {
		t.Errorf("EndTimestampInclusive: got %d, want %d", got.EndTimestampInclusive, segment.EndTimestampInclusive)
	}
	if got.Len() != segment.Len() {
		t.Errorf("Len: got %d, want %d", got.Len(), segment.Len())
	}

	// Verify first blob
	if got.Blobs[0].ID != segment.Blobs[0].ID {
		t.Errorf("Blobs[0].ID: got %q, want %q", got.Blobs[0].ID, segment.Blobs[0].ID)
	}
	if len(got.Blobs[0].Header.Blocks) != 2 {
		t.Errorf("Blobs[0].Header.Blocks: got %d blocks, want 2", len(got.Blobs[0].Header.Blocks))
	}
}

func TestIndexSegmentKey(t *testing.T) {
	segment := &IndexSegment{
		Epoch:          0,
		StartTimestampExclusive: 1234567890123,
		EndTimestampInclusive:   1234567890456,
	}

	key := segment.Key()
	expected := "index/segment-000-1234567890123-1234567890456.idx"
	if key != expected {
		t.Errorf("Key: got %q, want %q", key, expected)
	}

	// Test with non-zero epoch
	segment.Epoch = 2
	key = segment.Key()
	expected = "index/segment-002-1234567890123-1234567890456.idx"
	if key != expected {
		t.Errorf("Key with epoch: got %q, want %q", key, expected)
	}
}

func TestIndexSegmentWithHoles(t *testing.T) {
	segment := &IndexSegment{
		StartTimestampExclusive: 1000,
		EndTimestampInclusive:   1000,
		Blobs: []IndexSegmentEntry{
			{
				ID: "0000000001000-00000001",
				Header: BlobHeader{
					Timestamp:  1000,
					HeaderSize: 34,
					Blocks: []BlockEntry{
						{Offset: 0, Length: 100, IsHole: false},
						{Offset: 100, Length: 200, IsHole: true}, // hole
						{Offset: 300, Length: 50, IsHole: false},
					},
				},
			},
		},
	}

	// Marshal
	data, err := segment.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Unmarshal
	got, err := UnmarshalSegment(data)
	if err != nil {
		t.Fatalf("UnmarshalSegment: %v", err)
	}

	// Verify hole field is preserved
	blocks := got.Blobs[0].Header.Blocks
	if len(blocks) != 3 {
		t.Fatalf("expected 3 blocks, got %d", len(blocks))
	}
	if blocks[0].IsHole {
		t.Errorf("blocks[0].IsHole: got true, want false")
	}
	if !blocks[1].IsHole {
		t.Errorf("blocks[1].IsHole: got false, want true")
	}
	if blocks[2].IsHole {
		t.Errorf("blocks[2].IsHole: got true, want false")
	}
}

func TestEmptySegment(t *testing.T) {
	segment := NewIndexSegment(0, 0, nil)
	if segment.Len() != 0 {
		t.Errorf("Len: got %d, want 0", segment.Len())
	}

	// Should still marshal/unmarshal
	data, err := segment.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := UnmarshalSegment(data)
	if err != nil {
		t.Fatalf("UnmarshalSegment: %v", err)
	}
	if got.Len() != 0 {
		t.Errorf("Len after unmarshal: got %d, want 0", got.Len())
	}
}
