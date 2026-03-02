package cloudblock

import (
	"testing"
)

func TestBlobIDGeneration(t *testing.T) {
	id1 := GenerateBlobID()
	id2 := GenerateBlobID()

	if id1 == id2 {
		t.Error("Generated IDs should be unique")
	}

	// Verify format: 13 digits timestamp
	if len(id1) != 13 {
		t.Errorf("Expected ID length 13, got %d: %s", len(id1), id1)
	}

	ts, err := ParseBlobIDTimestamp(id1)
	if err != nil {
		t.Errorf("Failed to parse timestamp: %v", err)
	}
	if ts == 0 {
		t.Error("Timestamp should be non-zero")
	}

	// Verify IDs are monotonically increasing
	ts1, _ := ParseBlobIDTimestamp(id1)
	ts2, _ := ParseBlobIDTimestamp(id2)
	if ts2 <= ts1 {
		t.Errorf("IDs should be monotonically increasing: %d <= %d", ts2, ts1)
	}
}

func TestBlobPartition(t *testing.T) {
	tests := []struct {
		id        string
		partition string
	}{
		{"1234567890100", "00"},
		{"1234567890101", "01"},
		{"1234567890199", "99"},
		{"1234567890123", "23"},
		{"00", "00"},
		{"1", "00"}, // edge case: short ID returns fallback
	}

	for _, tc := range tests {
		got := BlobPartition(tc.id)
		if got != tc.partition {
			t.Errorf("BlobPartition(%q) = %q, want %q", tc.id, got, tc.partition)
		}
	}
}

func TestBlobHeaderMarshalUnmarshal(t *testing.T) {
	blocks := []BlockEntry{
		{Offset: 4096, Length: 1024},
		{Offset: 0, Length: 512},
		{Offset: 8192, Length: 2048},
	}

	header, err := NewBlobHeader(blocks)
	if err != nil {
		t.Fatalf("NewBlobHeader: %v", err)
	}

	// Verify blocks are sorted
	for i := 1; i < len(header.Blocks); i++ {
		if header.Blocks[i].Offset <= header.Blocks[i-1].Offset {
			t.Errorf("Blocks not sorted: %d at offset %d, %d at offset %d",
				i-1, header.Blocks[i-1].Offset, i, header.Blocks[i].Offset)
		}
	}

	// Marshal
	data, err := header.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Verify header is at least the fixed size (protobuf adds variable-length data)
	if len(data) < BlobHeaderFixed {
		t.Errorf("Marshaled size %d is less than minimum %d", len(data), BlobHeaderFixed)
	}

	// Unmarshal
	parsed, err := UnmarshalBlobHeader(data)
	if err != nil {
		t.Fatalf("UnmarshalBlobHeader: %v", err)
	}

	// Verify fields match
	if parsed.Timestamp != header.Timestamp {
		t.Errorf("Timestamp mismatch: %d != %d", parsed.Timestamp, header.Timestamp)
	}
	if len(parsed.Blocks) != len(header.Blocks) {
		t.Fatalf("Block count mismatch: %d != %d", len(parsed.Blocks), len(header.Blocks))
	}
	for i, b := range parsed.Blocks {
		if b.Offset != header.Blocks[i].Offset || b.Length != header.Blocks[i].Length {
			t.Errorf("Block %d mismatch: got {%d, %d}, want {%d, %d}",
				i, b.Offset, b.Length, header.Blocks[i].Offset, header.Blocks[i].Length)
		}
	}
}

func TestBlobCreation(t *testing.T) {
	blocks := []BlockEntry{
		{Offset: 1000, Length: 5},
		{Offset: 0, Length: 3},
	}
	data := [][]byte{
		[]byte("hello"),
		[]byte("abc"),
	}

	blob, err := NewBlob(blocks, data)
	if err != nil {
		t.Fatalf("NewBlob: %v", err)
	}

	// Blocks should be sorted by offset
	if blob.Header.Blocks[0].Offset != 0 || blob.Header.Blocks[1].Offset != 1000 {
		t.Error("Blocks not sorted correctly")
	}

	// Data should be reordered to match sorted blocks
	// Block at offset 0 has data "abc", block at offset 1000 has "hello"
	block0Data := blob.GetBlockData(0)
	if string(block0Data) != "abc" {
		t.Errorf("Block 0 data: got %q, want %q", block0Data, "abc")
	}

	block1Data := blob.GetBlockData(1)
	if string(block1Data) != "hello" {
		t.Errorf("Block 1 data: got %q, want %q", block1Data, "hello")
	}
}

func TestBlobMarshal(t *testing.T) {
	blocks := []BlockEntry{
		{Offset: 0, Length: 4},
		{Offset: 100, Length: 5},
	}
	data := [][]byte{
		[]byte("test"),
		[]byte("hello"),
	}

	blob, err := NewBlob(blocks, data)
	if err != nil {
		t.Fatalf("NewBlob: %v", err)
	}

	marshaled, err := blob.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	expectedSize := int(blob.Header.HeaderSize) + 4 + 5
	if len(marshaled) != expectedSize {
		t.Errorf("Marshaled size: got %d, want %d", len(marshaled), expectedSize)
	}

	// Parse header from marshaled data
	parsed, err := UnmarshalBlobHeader(marshaled)
	if err != nil {
		t.Fatalf("UnmarshalBlobHeader: %v", err)
	}

	if len(parsed.Blocks) != 2 {
		t.Errorf("Parsed block count: got %d, want 2", len(parsed.Blocks))
	}
}

func TestTooManyBlocks(t *testing.T) {
	blocks := make([]BlockEntry, MaxBlocksPerBlob+1)
	for i := range blocks {
		blocks[i] = BlockEntry{Offset: uint64(i * 4096), Length: 4096}
	}

	_, err := NewBlobHeader(blocks)
	if err != ErrTooManyBlocks {
		t.Errorf("Expected ErrTooManyBlocks, got %v", err)
	}
}

func TestInvalidMagic(t *testing.T) {
	// Create valid-length header with invalid magic
	data := make([]byte, BlobHeaderFixed)
	copy(data[0:4], "XXXX") // Invalid magic
	_, err := UnmarshalBlobHeader(data)
	if err != ErrInvalidMagic {
		t.Errorf("Expected ErrInvalidMagic, got %v", err)
	}
}
