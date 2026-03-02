package cloudblock

import (
	"bytes"
	"io"
	"testing"
)

func TestWriteBufferLaterWriteLowerOffset(t *testing.T) {
	// This test reproduces a bug where a later write at a lower offset
	// was incorrectly overwritten by an earlier write at a higher offset.
	//
	// The bug was in ToBlob() which sorted writes by offset before merging.
	// This caused earlier writes (higher offset) to be processed after
	// later writes (lower offset), incorrectly overwriting the newer data.

	buf := NewWriteBuffer()

	// Write A at offset 100, length 100 (first write, idx=0)
	// Covers bytes 100-199
	buf.TakeWrite(100, bytes.Repeat([]byte{'A'}, 100))

	// Write B at offset 50, length 60 (second write, idx=1, LATER)
	// Covers bytes 50-109, overlapping with A at 100-109
	buf.TakeWrite(50, bytes.Repeat([]byte{'B'}, 60))

	// Expected result:
	//   50-99:   B's data (60 bytes, but only first 50 are non-overlapping)
	//   100-109: B's data (B was written LATER, so B wins the overlap)
	//   110-199: A's data (90 bytes, non-overlapping part of A)

	blob, err := buf.ToBlob()
	if err != nil {
		t.Fatalf("ToBlob() error: %v", err)
	}
	if blob == nil {
		t.Fatal("ToBlob() returned nil")
	}

	// The blob should have the merged data
	// Check that we have the right blocks
	if len(blob.Header.Blocks) == 0 {
		t.Fatal("Expected at least one block")
	}

	// Verify the data content
	// The merged region should be 50-199 (150 bytes total)
	totalLen := 0
	for _, block := range blob.Header.Blocks {
		totalLen += int(block.Length)
	}
	if totalLen != 150 {
		t.Errorf("Expected total length 150, got %d", totalLen)
	}

	// Check the actual data
	// Bytes 0-49 (offset 50-99): should be 'B'
	// Bytes 50-59 (offset 100-109): should be 'B' (B wins overlap!)
	// Bytes 60-149 (offset 110-199): should be 'A'

	// The data in the blob should be:
	// - Offset 50-109: 'B' (60 bytes, B was written later so it wins)
	// - Offset 110-199: 'A' (90 bytes, remaining part of A)
	// Since these are adjacent, they should be merged into one block
	if len(blob.Header.Blocks) != 1 {
		t.Fatalf("Expected 1 merged block, got %d", len(blob.Header.Blocks))
	}

	block := blob.Header.Blocks[0]
	if block.Offset != 50 {
		t.Errorf("Expected block offset 50, got %d", block.Offset)
	}
	if block.Length != 150 {
		t.Errorf("Expected block length 150, got %d", block.Length)
	}

	// Use blob.Data directly (already available after ToBlob)
	data := blob.Data
	if len(data) != 150 {
		t.Fatalf("Expected data length 150, got %d", len(data))
	}

	// Bytes 0-59 (offset 50-109) should be 'B'
	for i := 0; i < 60; i++ {
		if data[i] != 'B' {
			t.Errorf("Byte at data index %d = '%c', expected 'B'", i, data[i])
		}
	}

	// Bytes 60-149 (offset 110-199) should be 'A'
	for i := 60; i < 150; i++ {
		if data[i] != 'A' {
			t.Errorf("Byte at data index %d = '%c', expected 'A'", i, data[i])
		}
	}
}

func TestWriteBufferOverlappingSameOffset(t *testing.T) {
	// Test that writes at the same offset are handled correctly
	// (later write wins)

	buf := NewWriteBuffer()

	buf.TakeWrite(100, bytes.Repeat([]byte{'A'}, 50))
	buf.TakeWrite(100, bytes.Repeat([]byte{'B'}, 50))

	blob, err := buf.ToBlob()
	if err != nil {
		t.Fatalf("ToBlob() error: %v", err)
	}

	blobBytes, err := blob.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	// All data should be 'B'
	data := blobBytes[blob.Header.HeaderSize:]
	for i := 0; i < 50; i++ {
		if data[i] != 'B' {
			t.Errorf("Byte %d = '%c', expected 'B'", i, data[i])
		}
	}
}

func TestWriteBufferMultipleOverlaps(t *testing.T) {
	// Test complex overlapping scenario

	buf := NewWriteBuffer()

	// Write 1: offset 0, length 100 (covers 0-99)
	buf.TakeWrite(0, bytes.Repeat([]byte{'1'}, 100))

	// Write 2: offset 50, length 100 (covers 50-149, overlaps 50-99)
	buf.TakeWrite(50, bytes.Repeat([]byte{'2'}, 100))

	// Write 3: offset 25, length 50 (covers 25-74, overlaps with both)
	buf.TakeWrite(25, bytes.Repeat([]byte{'3'}, 50))

	// Expected:
	//   0-24:   '1' (only write 1)
	//   25-74:  '3' (write 3 is latest for this region)
	//   75-99:  '2' (write 2, after write 1 but write 3 doesn't cover this)
	//   100-149: '2' (only write 2)

	blob, err := buf.ToBlob()
	if err != nil {
		t.Fatalf("ToBlob() error: %v", err)
	}

	blobBytes, err := blob.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	data := blobBytes[blob.Header.HeaderSize:]

	// Verify each region
	testCases := []struct {
		start, end int
		expected   byte
		desc       string
	}{
		{0, 25, '1', "region 0-24 should be '1'"},
		{25, 75, '3', "region 25-74 should be '3' (latest write)"},
		{75, 100, '2', "region 75-99 should be '2'"},
		{100, 150, '2', "region 100-149 should be '2'"},
	}

	for _, tc := range testCases {
		for i := tc.start; i < tc.end; i++ {
			if data[i] != tc.expected {
				t.Errorf("%s: byte %d = '%c', expected '%c'", tc.desc, i, data[i], tc.expected)
				break // Only report first error per region
			}
		}
	}
}

func TestWriteBufferHole(t *testing.T) {
	// Test that holes work correctly
	buf := NewWriteBuffer()

	// Write data at offset 0
	buf.TakeWrite(0, bytes.Repeat([]byte{'A'}, 100))

	// Add a hole at offset 50 (overwrites part of the data)
	buf.AddHole(50, 30)

	// The result should be:
	// - offset 0-49: 'A' (data)
	// - offset 50-79: hole
	// - offset 80-99: 'A' (data)

	blob, err := buf.ToBlob()
	if err != nil {
		t.Fatalf("ToBlob() error: %v", err)
	}

	// Should have 3 blocks: data, hole, data
	if len(blob.Header.Blocks) != 3 {
		t.Fatalf("Expected 3 blocks, got %d", len(blob.Header.Blocks))
	}

	// Verify blocks
	if blob.Header.Blocks[0].Offset != 0 || blob.Header.Blocks[0].Length != 50 || blob.Header.Blocks[0].IsHole {
		t.Errorf("Block 0: expected data at 0-49, got offset=%d length=%d isHole=%v",
			blob.Header.Blocks[0].Offset, blob.Header.Blocks[0].Length, blob.Header.Blocks[0].IsHole)
	}
	if blob.Header.Blocks[1].Offset != 50 || blob.Header.Blocks[1].Length != 30 || !blob.Header.Blocks[1].IsHole {
		t.Errorf("Block 1: expected hole at 50-79, got offset=%d length=%d isHole=%v",
			blob.Header.Blocks[1].Offset, blob.Header.Blocks[1].Length, blob.Header.Blocks[1].IsHole)
	}
	if blob.Header.Blocks[2].Offset != 80 || blob.Header.Blocks[2].Length != 20 || blob.Header.Blocks[2].IsHole {
		t.Errorf("Block 2: expected data at 80-99, got offset=%d length=%d isHole=%v",
			blob.Header.Blocks[2].Offset, blob.Header.Blocks[2].Length, blob.Header.Blocks[2].IsHole)
	}

	// Data should only contain the non-hole bytes (50 + 20 = 70 bytes)
	if len(blob.Data) != 70 {
		t.Errorf("Expected data length 70, got %d", len(blob.Data))
	}
}

func TestWriteBufferHoleOverwritesData(t *testing.T) {
	// Test that a hole completely overwrites data
	buf := NewWriteBuffer()

	buf.TakeWrite(100, bytes.Repeat([]byte{'A'}, 50))
	buf.AddHole(100, 50)

	blob, err := buf.ToBlob()
	if err != nil {
		t.Fatalf("ToBlob() error: %v", err)
	}

	// Should have 1 block (hole)
	if len(blob.Header.Blocks) != 1 {
		t.Fatalf("Expected 1 block, got %d", len(blob.Header.Blocks))
	}

	if !blob.Header.Blocks[0].IsHole {
		t.Error("Expected hole block")
	}

	// No data bytes
	if len(blob.Data) != 0 {
		t.Errorf("Expected no data, got %d bytes", len(blob.Data))
	}
}

func TestWriteBufferDataOverwritesHole(t *testing.T) {
	// Test that data overwrites a hole
	buf := NewWriteBuffer()

	buf.AddHole(100, 50)
	buf.TakeWrite(100, bytes.Repeat([]byte{'B'}, 50))

	blob, err := buf.ToBlob()
	if err != nil {
		t.Fatalf("ToBlob() error: %v", err)
	}

	// Should have 1 block (data)
	if len(blob.Header.Blocks) != 1 {
		t.Fatalf("Expected 1 block, got %d", len(blob.Header.Blocks))
	}

	if blob.Header.Blocks[0].IsHole {
		t.Error("Expected data block, got hole")
	}

	// Data should be all 'B'
	if len(blob.Data) != 50 {
		t.Errorf("Expected 50 bytes of data, got %d", len(blob.Data))
	}
	for i, b := range blob.Data {
		if b != 'B' {
			t.Errorf("Byte %d = '%c', expected 'B'", i, b)
			break
		}
	}
}

func TestToBlobParts(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(buf *WriteBuffer)
		wantBlocks []BlockEntry
		wantData   io.Reader
	}{
		{
			name: "empty",
		},
		{
			name: "single write",
			setup: func(buf *WriteBuffer) {
				buf.TakeWrite(500, bytes.Repeat([]byte{'X'}, 64))
			},
			wantBlocks: []BlockEntry{{Offset: 500, Length: 64}},
			wantData:   bytes.NewReader(bytes.Repeat([]byte{'X'}, 64)),
		},
		{
			name: "adjacent data writes merge",
			setup: func(buf *WriteBuffer) {
				buf.TakeWrite(0, bytes.Repeat([]byte{'A'}, 100))
				buf.TakeWrite(100, bytes.Repeat([]byte{'B'}, 100))
				buf.TakeWrite(200, bytes.Repeat([]byte{'C'}, 100))
			},
			wantBlocks: []BlockEntry{{Offset: 0, Length: 300}},
			wantData: io.MultiReader(
				bytes.NewReader(bytes.Repeat([]byte{'A'}, 100)),
				bytes.NewReader(bytes.Repeat([]byte{'B'}, 100)),
				bytes.NewReader(bytes.Repeat([]byte{'C'}, 100)),
			),
		},
		{
			name: "non-adjacent data writes stay separate",
			setup: func(buf *WriteBuffer) {
				buf.TakeWrite(0, bytes.Repeat([]byte{'A'}, 100))
				buf.TakeWrite(200, bytes.Repeat([]byte{'B'}, 100))
			},
			wantBlocks: []BlockEntry{
				{Offset: 0, Length: 100},
				{Offset: 200, Length: 100},
			},
			wantData: io.MultiReader(
				bytes.NewReader(bytes.Repeat([]byte{'A'}, 100)),
				bytes.NewReader(bytes.Repeat([]byte{'B'}, 100)),
			),
		},
		{
			name: "adjacent holes merge",
			setup: func(buf *WriteBuffer) {
				buf.AddHole(0, 100)
				buf.AddHole(100, 100)
			},
			wantBlocks: []BlockEntry{{Offset: 0, Length: 200, IsHole: true}},
			wantData:   bytes.NewReader(nil),
		},
		{
			name: "data-hole-data not merged across hole",
			setup: func(buf *WriteBuffer) {
				buf.TakeWrite(0, bytes.Repeat([]byte{'A'}, 100))
				buf.AddHole(100, 50)
				buf.TakeWrite(150, bytes.Repeat([]byte{'B'}, 100))
			},
			wantBlocks: []BlockEntry{
				{Offset: 0, Length: 100},
				{Offset: 100, Length: 50, IsHole: true},
				{Offset: 150, Length: 100},
			},
			wantData: io.MultiReader(
				bytes.NewReader(bytes.Repeat([]byte{'A'}, 100)),
				bytes.NewReader(bytes.Repeat([]byte{'B'}, 100)),
			),
		},
		{
			name: "out-of-order writes merge",
			setup: func(buf *WriteBuffer) {
				buf.TakeWrite(200, bytes.Repeat([]byte{'C'}, 100))
				buf.TakeWrite(100, bytes.Repeat([]byte{'B'}, 100))
				buf.TakeWrite(0, bytes.Repeat([]byte{'A'}, 100))
			},
			wantBlocks: []BlockEntry{{Offset: 0, Length: 300}},
			wantData: io.MultiReader(
				bytes.NewReader(bytes.Repeat([]byte{'A'}, 100)),
				bytes.NewReader(bytes.Repeat([]byte{'B'}, 100)),
				bytes.NewReader(bytes.Repeat([]byte{'C'}, 100)),
			),
		},
		{
			name: "partially overlapping writes",
			// A at 0-99, B at 50-149 (B wins 50-99)
			// After resolution: {0-49: A}, {50-149: B} — adjacent, merged
			setup: func(buf *WriteBuffer) {
				buf.TakeWrite(0, bytes.Repeat([]byte{'A'}, 100))
				buf.TakeWrite(50, bytes.Repeat([]byte{'B'}, 100))
			},
			wantBlocks: []BlockEntry{{Offset: 0, Length: 150}},
			wantData: io.MultiReader(
				bytes.NewReader(bytes.Repeat([]byte{'A'}, 50)),
				bytes.NewReader(bytes.Repeat([]byte{'B'}, 100)),
			),
		},
		{
			name: "out-of-order overlapping writes",
			// B at 100-199, A at 50-149 (A wins 100-149), C at 150-249 (overwrites B fragment)
			// Result: {50-149: A}, {150-249: C} — adjacent, merged
			setup: func(buf *WriteBuffer) {
				buf.TakeWrite(100, bytes.Repeat([]byte{'B'}, 100))
				buf.TakeWrite(50, bytes.Repeat([]byte{'A'}, 100))
				buf.TakeWrite(150, bytes.Repeat([]byte{'C'}, 100))
			},
			wantBlocks: []BlockEntry{{Offset: 50, Length: 200}},
			wantData: io.MultiReader(
				bytes.NewReader(bytes.Repeat([]byte{'A'}, 100)),
				bytes.NewReader(bytes.Repeat([]byte{'C'}, 100)),
			),
		},
		{
			name: "complex triple overlap",
			// 1 at 0-99, 2 at 50-149, 3 at 25-74
			// After resolution: {0-24: 1}, {25-74: 3}, {75-149: 2} — all adjacent
			setup: func(buf *WriteBuffer) {
				buf.TakeWrite(0, bytes.Repeat([]byte{'1'}, 100))
				buf.TakeWrite(50, bytes.Repeat([]byte{'2'}, 100))
				buf.TakeWrite(25, bytes.Repeat([]byte{'3'}, 50))
			},
			wantBlocks: []BlockEntry{{Offset: 0, Length: 150}},
			wantData: io.MultiReader(
				bytes.NewReader(bytes.Repeat([]byte{'1'}, 25)),
				bytes.NewReader(bytes.Repeat([]byte{'3'}, 50)),
				bytes.NewReader(bytes.Repeat([]byte{'2'}, 75)),
			),
		},
		{
			name: "hole punched into data",
			// Data at 0-199, hole at 50-79 splits into data/hole/data
			setup: func(buf *WriteBuffer) {
				buf.TakeWrite(0, bytes.Repeat([]byte{'A'}, 200))
				buf.AddHole(50, 30)
			},
			wantBlocks: []BlockEntry{
				{Offset: 0, Length: 50},
				{Offset: 50, Length: 30, IsHole: true},
				{Offset: 80, Length: 120},
			},
			wantData: io.MultiReader(
				bytes.NewReader(bytes.Repeat([]byte{'A'}, 50)),
				bytes.NewReader(bytes.Repeat([]byte{'A'}, 120)),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := NewWriteBuffer()
			if tt.setup != nil {
				tt.setup(buf)
			}

			blocks, dataSlices := buf.ToBlobParts()

			if tt.wantBlocks == nil {
				if blocks != nil || dataSlices != nil {
					t.Fatalf("Expected nil, nil for empty buffer, got %v, %v", blocks, dataSlices)
				}
				return
			}

			assertBlocksEqual(t, blocks, tt.wantBlocks)
			assertDataSlicesEqual(t, dataSlices, tt.wantData)
		})
	}
}

// assertBlocksEqual compares block entries against expected values.
func assertBlocksEqual(t *testing.T, got []BlockEntry, want []BlockEntry) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("Expected %d blocks, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Block %d: got {Offset:%d Length:%d IsHole:%v}, want {Offset:%d Length:%d IsHole:%v}",
				i, got[i].Offset, got[i].Length, got[i].IsHole,
				want[i].Offset, want[i].Length, want[i].IsHole)
		}
	}
}

// assertDataSlicesEqual builds a SegmentReader from dataSlices and compares
// its output against the expected reader.
func assertDataSlicesEqual(t *testing.T, dataSlices [][]byte, want io.Reader) {
	t.Helper()

	got, err := io.ReadAll(NewSegmentReader(dataSlices...))
	if err != nil {
		t.Fatalf("SegmentReader read error: %v", err)
	}

	expected, err := io.ReadAll(want)
	if err != nil {
		t.Fatalf("Expected reader read error: %v", err)
	}

	if !bytes.Equal(got, expected) {
		n := min(len(got), len(expected))
		for i := 0; i < n; i++ {
			if got[i] != expected[i] {
				t.Errorf("Data mismatch at byte %d: got '%c', want '%c'", i, got[i], expected[i])
				return
			}
		}
		t.Errorf("Data length mismatch: got %d bytes, want %d bytes", len(got), len(expected))
	}
}

func TestWriteBufferReadIntoSkipsHoles(t *testing.T) {
	// Test that ReadInto doesn't copy data from holes
	buf := NewWriteBuffer()

	// Add data then hole then data
	buf.TakeWrite(0, bytes.Repeat([]byte{'A'}, 50))
	buf.AddHole(50, 50)
	buf.TakeWrite(100, bytes.Repeat([]byte{'B'}, 50))

	// Create output buffer filled with 'X'
	out := bytes.Repeat([]byte{'X'}, 150)

	// ReadInto should copy data but leave holes as-is (which are 'X')
	buf.ReadInto(out, 0, 150)

	// Check results
	for i := 0; i < 50; i++ {
		if out[i] != 'A' {
			t.Errorf("Byte %d = '%c', expected 'A'", i, out[i])
		}
	}
	for i := 50; i < 100; i++ {
		if out[i] != 'X' {
			t.Errorf("Byte %d = '%c', expected 'X' (hole should not be copied)", i, out[i])
		}
	}
	for i := 100; i < 150; i++ {
		if out[i] != 'B' {
			t.Errorf("Byte %d = '%c', expected 'B'", i, out[i])
		}
	}
}
