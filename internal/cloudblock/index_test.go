package cloudblock

import (
	"testing"
)

func TestRangeIndexInsertNonOverlapping(t *testing.T) {
	idx := NewRangeIndex()

	idx.Insert(RangeEntry{Offset: 0, Length: 100, BlobID: "a", BlobOffset: 0})
	idx.Insert(RangeEntry{Offset: 200, Length: 100, BlobID: "b", BlobOffset: 0})
	idx.Insert(RangeEntry{Offset: 100, Length: 100, BlobID: "c", BlobOffset: 0})

	ranges := idx.All()
	if len(ranges) != 3 {
		t.Fatalf("Expected 3 ranges, got %d", len(ranges))
	}

	// Verify sorted order
	if ranges[0].Offset != 0 || ranges[1].Offset != 100 || ranges[2].Offset != 200 {
		t.Error("Ranges not sorted correctly")
	}
}

func TestRangeIndexInsertOverlapping(t *testing.T) {
	idx := NewRangeIndex()

	// Insert initial range
	idx.Insert(RangeEntry{Offset: 0, Length: 100, BlobID: "a", BlobOffset: 0})

	// Insert overlapping range in the middle
	idx.Insert(RangeEntry{Offset: 25, Length: 50, BlobID: "b", BlobOffset: 0})

	ranges := idx.All()
	if len(ranges) != 3 {
		t.Fatalf("Expected 3 ranges, got %d", len(ranges))
	}

	// [0-25) from a, [25-75) from b, [75-100) from a
	if ranges[0].Offset != 0 || ranges[0].Length != 25 || ranges[0].BlobID != "a" {
		t.Errorf("Range 0: got %+v, want offset=0 length=25 blob=a", ranges[0])
	}
	if ranges[1].Offset != 25 || ranges[1].Length != 50 || ranges[1].BlobID != "b" {
		t.Errorf("Range 1: got %+v, want offset=25 length=50 blob=b", ranges[1])
	}
	if ranges[2].Offset != 75 || ranges[2].Length != 25 || ranges[2].BlobID != "a" {
		t.Errorf("Range 2: got %+v, want offset=75 length=25 blob=a", ranges[2])
	}
	// Verify the right part has correct BlobOffset (75-0=75 bytes into original blob)
	if ranges[2].BlobOffset != 75 {
		t.Errorf("Range 2 BlobOffset: got %d, want 75", ranges[2].BlobOffset)
	}
}

func TestRangeIndexInsertFullOverwrite(t *testing.T) {
	idx := NewRangeIndex()

	idx.Insert(RangeEntry{Offset: 100, Length: 50, BlobID: "a", BlobOffset: 0})
	idx.Insert(RangeEntry{Offset: 0, Length: 200, BlobID: "b", BlobOffset: 0})

	ranges := idx.All()
	if len(ranges) != 1 {
		t.Fatalf("Expected 1 range after full overwrite, got %d", len(ranges))
	}
	if ranges[0].BlobID != "b" {
		t.Errorf("Expected blob b, got %s", ranges[0].BlobID)
	}
}

func TestRangeIndexLookup(t *testing.T) {
	idx := NewRangeIndex()

	idx.Insert(RangeEntry{Offset: 0, Length: 100, BlobID: "a", BlobOffset: 0})
	idx.Insert(RangeEntry{Offset: 100, Length: 100, BlobID: "b", BlobOffset: 0})
	idx.Insert(RangeEntry{Offset: 300, Length: 100, BlobID: "c", BlobOffset: 0})

	// Lookup that spans multiple ranges
	results := idx.Lookup(50, 100) // [50, 150)
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// Lookup with no overlap
	results = idx.Lookup(200, 50) // [200, 250) - gap between b and c
	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestRangeIndexFindGaps(t *testing.T) {
	idx := NewRangeIndex()

	idx.Insert(RangeEntry{Offset: 100, Length: 100, BlobID: "a", BlobOffset: 0})
	idx.Insert(RangeEntry{Offset: 300, Length: 100, BlobID: "b", BlobOffset: 0})

	// Find gaps in [0, 500)
	gaps := idx.FindGaps(0, 500)
	if len(gaps) != 3 {
		t.Fatalf("Expected 3 gaps, got %d: %+v", len(gaps), gaps)
	}

	// [0-100), [200-300), [400-500)
	if gaps[0].Offset != 0 || gaps[0].Length != 100 {
		t.Errorf("Gap 0: got %+v, want offset=0 length=100", gaps[0])
	}
	if gaps[1].Offset != 200 || gaps[1].Length != 100 {
		t.Errorf("Gap 1: got %+v, want offset=200 length=100", gaps[1])
	}
	if gaps[2].Offset != 400 || gaps[2].Length != 100 {
		t.Errorf("Gap 2: got %+v, want offset=400 length=100", gaps[2])
	}
}

func TestRangeIndexInsertBlob(t *testing.T) {
	idx := NewRangeIndex()

	header := &BlobHeader{
		Timestamp:  12345,
		HeaderSize: 46, // Fixed header + 2 blocks
		Blocks: []BlockEntry{
			{Offset: 0, Length: 100},
			{Offset: 1000, Length: 200},
		},
	}

	idx.InsertBlob("blob-1", header)

	ranges := idx.All()
	if len(ranges) != 2 {
		t.Fatalf("Expected 2 ranges, got %d", len(ranges))
	}

	// First block: data at HeaderSize
	if ranges[0].BlobOffset != 46 {
		t.Errorf("Range 0 BlobOffset: got %d, want 46", ranges[0].BlobOffset)
	}
	// Second block: data at HeaderSize + first block length
	if ranges[1].BlobOffset != 146 {
		t.Errorf("Range 1 BlobOffset: got %d, want 146", ranges[1].BlobOffset)
	}
}

func TestRangeIndexPartialOverlapLeft(t *testing.T) {
	idx := NewRangeIndex()

	// Insert [100, 200)
	idx.Insert(RangeEntry{Offset: 100, Length: 100, BlobID: "a", BlobOffset: 0})

	// Insert [50, 150) - overlaps left part
	idx.Insert(RangeEntry{Offset: 50, Length: 100, BlobID: "b", BlobOffset: 0})

	ranges := idx.All()
	if len(ranges) != 2 {
		t.Fatalf("Expected 2 ranges, got %d: %+v", len(ranges), ranges)
	}

	// [50-150) from b, [150-200) from a
	if ranges[0].Offset != 50 || ranges[0].Length != 100 || ranges[0].BlobID != "b" {
		t.Errorf("Range 0: got %+v, want offset=50 length=100 blob=b", ranges[0])
	}
	if ranges[1].Offset != 150 || ranges[1].Length != 50 || ranges[1].BlobID != "a" {
		t.Errorf("Range 1: got %+v, want offset=150 length=50 blob=a", ranges[1])
	}
	// Check BlobOffset for the surviving right part of 'a'
	// Original 'a' was at offset 100, we're keeping [150-200), so BlobOffset should be 50
	if ranges[1].BlobOffset != 50 {
		t.Errorf("Range 1 BlobOffset: got %d, want 50", ranges[1].BlobOffset)
	}
}

func TestRangeIndexHasBlob(t *testing.T) {
	idx := NewRangeIndex()

	// Create a simple header
	header := &BlobHeader{
		Blocks:     []BlockEntry{{Offset: 0, Length: 100}},
		HeaderSize: 64,
	}

	// Should not have blob before insert
	if idx.HasBlob("blob-1") {
		t.Error("HasBlob returned true before InsertBlob")
	}

	// Insert blob
	idx.InsertBlob("blob-1", header)

	// Should have blob after insert
	if !idx.HasBlob("blob-1") {
		t.Error("HasBlob returned false after InsertBlob")
	}

	// Should not have unknown blob
	if idx.HasBlob("blob-2") {
		t.Error("HasBlob returned true for unknown blob")
	}

	// Clear should reset
	idx.Clear()
	if idx.HasBlob("blob-1") {
		t.Error("HasBlob returned true after Clear")
	}
}

func TestRangeIndexGetLiveRange(t *testing.T) {
	idx := NewRangeIndex()

	// Create a blob with multiple ranges
	header := &BlobHeader{
		Timestamp:  12345,
		HeaderSize: 100, // simplify calculation
		Blocks: []BlockEntry{
			{Offset: 0, Length: 100},    // BlobOffset = 100
			{Offset: 1000, Length: 200}, // BlobOffset = 200
			{Offset: 2000, Length: 300}, // BlobOffset = 400
		},
	}

	idx.InsertBlob("blob-1", header)

	// GetLiveRange for unknown blob should return error
	_, err := idx.GetLiveRange("unknown")
	if err != ErrUnknownBlob {
		t.Errorf("Expected ErrUnknownBlob, got %v", err)
	}

	// GetLiveRange for known blob
	lr, err := idx.GetLiveRange("blob-1")
	if err != nil {
		t.Fatalf("GetLiveRange failed: %v", err)
	}

	// Should span from first range start (100) to last range end (400+300=700)
	if lr.Start != 100 {
		t.Errorf("LiveRange.Start: got %d, want 100", lr.Start)
	}
	if lr.End != 700 {
		t.Errorf("LiveRange.End: got %d, want 700", lr.End)
	}
	// Total live bytes = 100 + 200 + 300 = 600
	if lr.LiveBytes != 600 {
		t.Errorf("LiveRange.LiveBytes: got %d, want 600", lr.LiveBytes)
	}
}

func TestRangeIndexGetLiveRangeAfterOverwrite(t *testing.T) {
	idx := NewRangeIndex()

	// Insert initial range for blob-a
	idx.Insert(RangeEntry{Offset: 0, Length: 100, BlobID: "blob-a", BlobOffset: 50})

	// Mark blob-a as known
	idx.mu.Lock()
	idx.knownBlobs["blob-a"] = true
	idx.mu.Unlock()

	lr, err := idx.GetLiveRange("blob-a")
	if err != nil {
		t.Fatalf("GetLiveRange failed: %v", err)
	}
	if lr.LiveBytes != 100 {
		t.Errorf("LiveBytes before overwrite: got %d, want 100", lr.LiveBytes)
	}

	// Overwrite part of blob-a with blob-b
	idx.Insert(RangeEntry{Offset: 25, Length: 50, BlobID: "blob-b", BlobOffset: 0})

	// blob-a should now have two fragments: [0,25) and [75,100)
	lr, err = idx.GetLiveRange("blob-a")
	if err != nil {
		t.Fatalf("GetLiveRange after overwrite failed: %v", err)
	}
	// LiveBytes = 25 + 25 = 50
	if lr.LiveBytes != 50 {
		t.Errorf("LiveBytes after overwrite: got %d, want 50", lr.LiveBytes)
	}
}

// TestGCProtectionsScopedPerBuffer verifies that GC protections are scoped
// to each buffer independently. When buffer A's upload completes, it should
// only affect blobs superseded by buffer A, not blobs superseded by buffer B.
//
// Scenario:
// 1. Blobs X and Y exist at different offsets
// 2. Write to buffer A supersedes X, buffer A seals
// 3. Write to buffer B supersedes Y (new active buffer)
// 4. Buffer A uploads and completes
// 5. Y should still be protected (buffer B hasn't uploaded yet)
func TestGCProtectionsScopedPerBuffer(t *testing.T) {
	store := NewMemoryBlobStore()
	device, err := NewDevice(DeviceConfig{
		Store:         store,
		Size:          1 << 20, // 1MB
		MaxBufferSize: 100,     // Small threshold to trigger seals easily
		UploadWorkers: 1,
	})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	if err := device.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer device.Close()

	// Setup: create blobs X and Y at different offsets
	headerX := &BlobHeader{HeaderSize: 50, Blocks: []BlockEntry{{Offset: 0, Length: 100}}}
	headerY := &BlobHeader{HeaderSize: 50, Blocks: []BlockEntry{{Offset: 1000, Length: 100}}}
	device.index.InsertBlob("X", headerX)
	device.index.InsertBlob("Y", headerY)

	// Write data at offset 0 (supersedes X) - enough to trigger auto-seal and upload
	dataA := make([]byte, 150) // Exceeds threshold
	for i := range dataA {
		dataA[i] = 'A'
	}
	result := <-device.SubmitWrite(0, dataA, 0)
	if result.Err != nil {
		t.Fatalf("Write A failed: %v", result.Err)
	}

	// Flush to ensure buffer A is uploaded
	result = <-device.SubmitFlush(0)
	if result.Err != nil {
		t.Fatalf("Flush failed: %v", result.Err)
	}

	// Write at offset 1000 (overlaps Y) - stays in active buffer (below threshold).
	// Y's index entry should remain until this buffer is uploaded and its blob
	// entries replace Y's in the index.
	dataB := make([]byte, 50) // Below threshold so it stays in active buffer
	for i := range dataB {
		dataB[i] = 'B'
	}
	result = <-device.SubmitWrite(0, dataB, 1000)
	if result.Err != nil {
		t.Fatalf("Write B failed: %v", result.Err)
	}

	// Y should still be referenced - buffer B hasn't been uploaded so Y's
	// index entry at offset 1000-1100 has not been replaced yet
	unreferenced := device.index.GetUnreferencedBlobs()

	for _, id := range unreferenced {
		if id == "Y" {
			t.Errorf("Y should remain protected until buffer B uploads, but it's listed as unreferenced")
		}
	}
}

func TestRangeIndexGetUnreferencedBlobs(t *testing.T) {
	idx := NewRangeIndex()

	// Insert ranges for blob-a at offset 0 and blob-b at offset 1000
	headerA := &BlobHeader{
		HeaderSize: 50,
		Blocks:     []BlockEntry{{Offset: 0, Length: 100}},
	}
	headerB := &BlobHeader{
		HeaderSize: 50,
		Blocks:     []BlockEntry{{Offset: 1000, Length: 100}},
	}
	idx.InsertBlob("blob-a", headerA)
	idx.InsertBlob("blob-b", headerB)

	// Both blobs should be referenced initially
	unreferenced := idx.GetUnreferencedBlobs()
	if len(unreferenced) != 0 {
		t.Errorf("Expected 0 unreferenced blobs, got %d: %v", len(unreferenced), unreferenced)
	}

	// Overwrite blob-a completely with blob-c
	idx.Insert(RangeEntry{Offset: 0, Length: 100, BlobID: "blob-c", BlobOffset: 0})
	idx.mu.Lock()
	idx.knownBlobs["blob-c"] = true
	idx.mu.Unlock()

	// Now only blob-a should be unreferenced (blob-b is at different offset)
	unreferenced = idx.GetUnreferencedBlobs()
	if len(unreferenced) != 1 || unreferenced[0] != "blob-a" {
		t.Errorf("Expected [blob-a] unreferenced, got %v", unreferenced)
	}

	// Mark blob-a as deleted
	idx.RemoveBlob("blob-a")

	// Now nothing should be unreferenced
	unreferenced = idx.GetUnreferencedBlobs()
	if len(unreferenced) != 0 {
		t.Errorf("Expected 0 unreferenced blobs after delete, got %d: %v", len(unreferenced), unreferenced)
	}
}
