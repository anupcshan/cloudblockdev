// Package cloudblock implements an S3-backed block device.
package cloudblock

//go:generate protoc -I../.. --go_out=../.. --go_opt=module=github.com/anupcshan/cloudblockdev --go_opt=default_api_level=API_OPAQUE proto/blob.proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/anupcshan/cloudblockdev/internal/cloudblock/pb"
	"google.golang.org/protobuf/proto"
)

// blobIDState tracks the last timestamp used for blob ID generation
// to ensure monotonically increasing IDs.
var blobIDState struct {
	mu            sync.Mutex
	lastTimestamp int64
}

// Blob format constants
const (
	BlobMagic   = "CBDV"
	BlobVersion = 2 // Version 2 uses protobuf encoding

	// BlobHeaderFixed is the fixed portion of the blob header:
	// Magic(4) + Version(2) + HeaderLen(4) = 10 bytes
	BlobHeaderFixed = 10

	// MaxBlocksPerBlob limits the number of blocks in a single blob.
	// With protobuf's varint encoding, this is a soft limit for sanity.
	MaxBlocksPerBlob = 1000

	// BlobHeaderMax is the maximum size of a blob header.
	// Used for buffer allocation when reading headers.
	BlobHeaderMax = 64 * 1024 // 64KB should be plenty for protobuf headers
)

var (
	ErrInvalidMagic   = errors.New("invalid blob magic")
	ErrInvalidVersion = errors.New("unsupported blob version")
	ErrTooManyBlocks  = errors.New("too many blocks in blob")
)

// BlockEntry describes a single block of data within a blob.
type BlockEntry struct {
	Offset uint64 // Start offset in the block device
	Length uint64 // Length of this block in bytes
	IsHole bool   // If true, this block is a hole (no data stored)
}

// BlobHeader contains metadata for a blob.
type BlobHeader struct {
	Timestamp  uint64       // Milliseconds since epoch
	Blocks     []BlockEntry // Sorted by offset
	HeaderSize uint32       // Total header size in bytes (computed during marshal)
}

// GenerateBlobID creates a new blob identifier.
// Format: {timestamp_ms} (13 digits, zero-padded)
// The timestamp is guaranteed to be monotonically increasing, even if the
// system clock goes backward (e.g., NTP adjustment, reboot).
// Single-writer only - concurrent writers would need additional uniqueness.
func GenerateBlobID() string {
	blobIDState.mu.Lock()
	now := time.Now().UnixMilli()
	// Ensure monotonically increasing: use max(now, lastTimestamp+1)
	if now <= blobIDState.lastTimestamp {
		now = blobIDState.lastTimestamp + 1
	}
	blobIDState.lastTimestamp = now
	blobIDState.mu.Unlock()

	return fmt.Sprintf("%013d", now)
}

// SetBlobIDFloor sets the minimum timestamp for future blob IDs.
// This should be called after recovery to ensure new blob IDs are
// greater than all existing blobs.
func SetBlobIDFloor(ts int64) {
	blobIDState.mu.Lock()
	if ts > blobIDState.lastTimestamp {
		blobIDState.lastTimestamp = ts
	}
	blobIDState.mu.Unlock()
}

// GetBlobIDFloor returns the current blob ID timestamp floor.
// Useful for determining the low water mark for incremental sync.
func GetBlobIDFloor() int64 {
	blobIDState.mu.Lock()
	defer blobIDState.mu.Unlock()
	return blobIDState.lastTimestamp
}

// ParseBlobIDTimestamp extracts the timestamp from a blob ID.
func ParseBlobIDTimestamp(id string) (uint64, error) {
	var ts uint64
	n, err := fmt.Sscanf(id, "%d", &ts)
	if err != nil || n != 1 {
		return 0, fmt.Errorf("invalid blob ID format: %s", id)
	}
	return ts, nil
}

// NumBlobPartitions is the number of partitions for blob storage (00-99).
const NumBlobPartitions = 100

// BlobPartition extracts the partition (00-99) from a blob ID.
// Uses the last 2 digits of the timestamp for natural round-robin distribution.
func BlobPartition(id string) string {
	if len(id) < 2 {
		return "00"
	}
	return id[len(id)-2:]
}

// NewBlobHeader creates a new blob header with the given blocks.
// Blocks are sorted by offset.
func NewBlobHeader(blocks []BlockEntry) (*BlobHeader, error) {
	if len(blocks) > MaxBlocksPerBlob {
		return nil, ErrTooManyBlocks
	}

	// Sort blocks by offset
	sorted := make([]BlockEntry, len(blocks))
	copy(sorted, blocks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Offset < sorted[j].Offset
	})

	return &BlobHeader{
		Timestamp: uint64(time.Now().UnixMilli()),
		Blocks:    sorted,
		// HeaderSize will be computed during Marshal
	}, nil
}

// toProto converts BlobHeader to its protobuf representation.
// Note: HeaderSize is NOT included in the proto because it depends on the
// serialized proto size (chicken-and-egg problem). It's computed separately.
func (h *BlobHeader) toProto() *pb.BlobHeader {
	blocks := make([]*pb.BlockEntry, len(h.Blocks))
	for i, b := range h.Blocks {
		blocks[i] = pb.BlockEntry_builder{
			Offset: b.Offset,
			Length: b.Length,
			IsHole: b.IsHole,
		}.Build()
	}
	return pb.BlobHeader_builder{
		Timestamp: h.Timestamp,
		Blocks:    blocks,
	}.Build()
}

// fromProto populates BlobHeader from its protobuf representation.
// Note: HeaderSize is NOT set from the proto - it must be computed separately
// based on the context (binary format or segment).
func (h *BlobHeader) fromProto(pbHeader *pb.BlobHeader) {
	h.Timestamp = pbHeader.GetTimestamp()
	h.Blocks = make([]BlockEntry, len(pbHeader.GetBlocks()))
	for i, b := range pbHeader.GetBlocks() {
		h.Blocks[i] = BlockEntry{
			Offset: b.GetOffset(),
			Length: b.GetLength(),
			IsHole: b.GetIsHole(),
		}
	}
}

// Marshal serializes the blob header to bytes.
// Format: Magic(4) + Version(2) + HeaderLen(4) + ProtoHeader(HeaderLen)
func (h *BlobHeader) Marshal() ([]byte, error) {
	// Serialize the protobuf header
	protoData, err := proto.Marshal(h.toProto())
	if err != nil {
		return nil, fmt.Errorf("marshal protobuf: %w", err)
	}

	// Build the full header
	buf := new(bytes.Buffer)

	// Magic (4 bytes)
	buf.WriteString(BlobMagic)

	// Version (2 bytes)
	binary.Write(buf, binary.BigEndian, uint16(BlobVersion))

	// HeaderLen (4 bytes) - length of protobuf data
	binary.Write(buf, binary.BigEndian, uint32(len(protoData)))

	// Protobuf header
	buf.Write(protoData)

	result := buf.Bytes()
	h.HeaderSize = uint32(len(result))
	return result, nil
}

// UnmarshalBlobHeader deserializes a blob header from bytes.
func UnmarshalBlobHeader(data []byte) (*BlobHeader, error) {
	if len(data) < BlobHeaderFixed {
		return nil, fmt.Errorf("header too short: %d bytes", len(data))
	}

	r := bytes.NewReader(data)

	// Magic (4 bytes)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	if string(magic) != BlobMagic {
		return nil, ErrInvalidMagic
	}

	// Version (2 bytes)
	var version uint16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("read version: %w", err)
	}
	if version != BlobVersion {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrInvalidVersion, version, BlobVersion)
	}

	// HeaderLen (4 bytes)
	var headerLen uint32
	if err := binary.Read(r, binary.BigEndian, &headerLen); err != nil {
		return nil, fmt.Errorf("read header length: %w", err)
	}

	// Validate we have enough data
	totalHeaderSize := BlobHeaderFixed + int(headerLen)
	if len(data) < totalHeaderSize {
		return nil, fmt.Errorf("header data too short: have %d, need %d", len(data), totalHeaderSize)
	}

	// Read and parse protobuf
	protoData := data[BlobHeaderFixed:totalHeaderSize]
	var pbHeader pb.BlobHeader
	if err := proto.Unmarshal(protoData, &pbHeader); err != nil {
		return nil, fmt.Errorf("unmarshal protobuf: %w", err)
	}

	header := &BlobHeader{}
	header.fromProto(&pbHeader)
	// Override HeaderSize with the computed value from the binary format,
	// as the proto's header_size may be stale or zero (it's set after marshaling).
	header.HeaderSize = uint32(totalHeaderSize)
	return header, nil
}

// DataOffset returns the byte offset where block i's data starts within the blob.
// Holes are skipped (they have no data in the blob).
func (h *BlobHeader) DataOffset(blockIndex int) uint32 {
	offset := h.HeaderSize
	for i := 0; i < blockIndex; i++ {
		if !h.Blocks[i].IsHole {
			offset += uint32(h.Blocks[i].Length)
		}
	}
	return offset
}

// TotalSize returns the total size of the blob (header + all data).
// Holes don't contribute to the data size.
func (h *BlobHeader) TotalSize() uint64 {
	size := uint64(h.HeaderSize)
	for _, b := range h.Blocks {
		if !b.IsHole {
			size += b.Length
		}
	}
	return size
}

// DataSize returns the total size of the data section (excluding header).
// Holes don't contribute to the data size.
func (h *BlobHeader) DataSize() uint64 {
	var size uint64
	for _, b := range h.Blocks {
		if !b.IsHole {
			size += b.Length
		}
	}
	return size
}

// Blob represents a complete blob with header and data.
type Blob struct {
	ID     string
	Header *BlobHeader
	Data   []byte // Concatenated block data (holes excluded)
}

// NewBlob creates a new blob from blocks and their data.
// For hole blocks, the corresponding data entry should be nil.
func NewBlob(blocks []BlockEntry, data [][]byte) (*Blob, error) {
	if len(blocks) != len(data) {
		return nil, errors.New("blocks and data length mismatch")
	}

	// Validate data lengths match block lengths
	for i, b := range blocks {
		if b.IsHole {
			if len(data[i]) != 0 {
				return nil, fmt.Errorf("block %d: hole should have nil/empty data", i)
			}
		} else {
			if uint64(len(data[i])) != b.Length {
				return nil, fmt.Errorf("block %d: data length %d != declared length %d",
					i, len(data[i]), b.Length)
			}
		}
	}

	header, err := NewBlobHeader(blocks)
	if err != nil {
		return nil, err
	}

	// Since we sorted blocks, we need to reorder data to match
	// Create a mapping from original index to sorted index
	type indexedBlock struct {
		originalIndex int
		block         BlockEntry
	}
	indexed := make([]indexedBlock, len(blocks))
	for i, b := range blocks {
		indexed[i] = indexedBlock{i, b}
	}
	sort.Slice(indexed, func(i, j int) bool {
		return indexed[i].block.Offset < indexed[j].block.Offset
	})

	// Concatenate data in sorted order, skipping holes
	var concatenated []byte
	for _, ib := range indexed {
		if !ib.block.IsHole {
			concatenated = append(concatenated, data[ib.originalIndex]...)
		}
	}

	return &Blob{
		ID:     GenerateBlobID(),
		Header: header,
		Data:   concatenated,
	}, nil
}

// Marshal serializes the entire blob (header + data).
func (b *Blob) Marshal() ([]byte, error) {
	header, err := b.Header.Marshal()
	if err != nil {
		return nil, err
	}
	result := make([]byte, len(header)+len(b.Data))
	copy(result, header)
	copy(result[len(header):], b.Data)
	return result, nil
}

// GetBlockData returns the data for a specific block by index.
// Returns nil for holes.
func (b *Blob) GetBlockData(blockIndex int) []byte {
	if blockIndex < 0 || blockIndex >= len(b.Header.Blocks) {
		return nil
	}
	block := b.Header.Blocks[blockIndex]
	if block.IsHole {
		return nil
	}
	start := b.Header.DataOffset(blockIndex) - b.Header.HeaderSize
	end := start + uint32(block.Length)
	return b.Data[start:end]
}
