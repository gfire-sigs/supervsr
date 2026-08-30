package replication

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var (
	ErrInvalidBlock = errors.New("replication: invalid block")
	ErrBlockMissing = errors.New("replication: block does not match requested reference")
)

type BlockStore struct {
	storage Storage
	cluster ClusterConfig
	group   protocol.GroupID
	release protocol.Release
	scratch []byte
	mu      sync.Mutex
}

type BlockReadResult struct {
	Metadata [96]byte
	Snapshot uint64
	Type     protocol.BlockType
	BodySize uint32
}

func NewBlockStore(storage Storage, cluster ClusterConfig, group protocol.GroupID, release protocol.Release) (*BlockStore, error) {
	if storage == nil || group.IsZero() || release == 0 || cluster.BlockSize < protocol.HeaderSize || cluster.BlockSize > cluster.MessageSizeMax {
		return nil, ErrInvalidConfiguration
	}
	scratch, err := NewAlignedBuffer(cluster.BlockSize, SectorSize)
	if err != nil {
		return nil, err
	}
	return &BlockStore{storage: storage, cluster: cluster, group: group, release: release, scratch: scratch}, nil
}

func (store *BlockStore) Write(address, snapshot uint64, blockType protocol.BlockType, metadata [96]byte, body []byte) (BlockReference, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.validAddress(address) || uint64(len(body))+protocol.HeaderSize > store.cluster.BlockSize || len(body) == 0 {
		return BlockReference{}, ErrInvalidBlock
	}
	offset, _ := store.cluster.BlockOffset(address)
	if err := ValidateBlockMetadata(blockType, metadata, body); err != nil {
		return BlockReference{}, err
	}
	clear(store.scratch)
	copy(store.scratch[protocol.HeaderSize:], body)
	header := protocol.Header{Group: store.group, Release: store.release, Protocol: protocol.ProtocolVersion, Command: protocol.CommandBlock, Author: 0}
	copy(header.Fields[:96], metadata[:])
	binary.LittleEndian.PutUint64(header.Fields[96:104], address)
	binary.LittleEndian.PutUint64(header.Fields[104:112], snapshot)
	header.Fields[112] = byte(blockType)
	frame := store.scratch[:protocol.HeaderSize+len(body)]
	if err := protocol.SealFrame(frame, &header); err != nil {
		return BlockReference{}, errors.Join(ErrInvalidBlock, err)
	}
	if err := store.storage.WriteAt(store.scratch, offset); err != nil {
		return BlockReference{}, err
	}
	if err := store.storage.Sync(); err != nil {
		return BlockReference{}, err
	}
	return BlockReference{Checksum: header.HeaderChecksum, Address: address}, nil
}

func (store *BlockStore) Read(reference BlockReference, blockType protocol.BlockType, destination []byte) (BlockReadResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if reference.Address == 0 || reference.Checksum.IsZero() || !store.validAddress(reference.Address) {
		return BlockReadResult{}, ErrBlockMissing
	}
	offset, _ := store.cluster.BlockOffset(reference.Address)
	if err := store.storage.ReadAt(store.scratch, offset); err != nil {
		return BlockReadResult{}, err
	}
	header, reason := protocol.DecodeHeader(store.scratch[:protocol.HeaderSize], store.group, uint32(store.cluster.BlockSize), 1)
	if reason != protocol.RejectNone || header.Command != protocol.CommandBlock || header.HeaderChecksum != reference.Checksum {
		return BlockReadResult{}, ErrBlockMissing
	}
	if header.Size > uint32(store.cluster.BlockSize) || !allZeroBytes(store.scratch[header.Size:]) {
		return BlockReadResult{}, ErrInvalidBlock
	}
	header, body, reason := protocol.DecodeFrame(store.scratch[:header.Size], store.group, uint32(store.cluster.BlockSize), 1)
	if reason != protocol.RejectNone || header.Release == 0 || header.Release > store.release {
		return BlockReadResult{}, ErrInvalidBlock
	}
	address := binary.LittleEndian.Uint64(header.Fields[96:104])
	actualType := protocol.BlockType(header.Fields[112])
	if address != reference.Address || actualType != blockType || !allZeroBytes(header.Fields[113:]) {
		return BlockReadResult{}, ErrBlockMissing
	}
	var metadata [96]byte
	copy(metadata[:], header.Fields[:96])
	if err := ValidateBlockMetadata(actualType, metadata, body); err != nil {
		return BlockReadResult{}, err
	}
	if len(destination) < len(body) {
		return BlockReadResult{}, ErrInvalidBlock
	}
	copy(destination, body)
	return BlockReadResult{Metadata: metadata, Snapshot: binary.LittleEndian.Uint64(header.Fields[104:112]), Type: actualType, BodySize: uint32(len(body))}, nil
}

func (store *BlockStore) validAddress(address uint64) bool {
	_, ok := store.cluster.BlockOffset(address)
	return ok
}

func ValidateBlockMetadata(blockType protocol.BlockType, metadata [96]byte, body []byte) error {
	switch blockType {
	case protocol.BlockFreeSet, protocol.BlockClientSessions:
		if !validTrailerMetadata(metadata) {
			return ErrInvalidBlock
		}
	case protocol.BlockManifest:
		if !validManifestMetadata(metadata) {
			return ErrInvalidBlock
		}
	case protocol.BlockIndex:
		values := binary.LittleEndian.Uint32(metadata[:4])
		maximum := binary.LittleEndian.Uint32(metadata[4:8])
		keySize := binary.LittleEndian.Uint32(metadata[8:12])
		if maximum == 0 || values > maximum || keySize == 0 || !allZeroBytes(metadata[14:]) {
			return ErrInvalidBlock
		}
	case protocol.BlockValue:
		maximum := binary.LittleEndian.Uint32(metadata[:4])
		count := binary.LittleEndian.Uint32(metadata[4:8])
		valueSize := binary.LittleEndian.Uint32(metadata[8:12])
		if maximum == 0 || count > maximum || valueSize == 0 || !allZeroBytes(metadata[14:]) {
			return ErrInvalidBlock
		}
		expected, ok := checkedMul(uint64(count), uint64(valueSize))
		if !ok || expected != uint64(len(body)) {
			return ErrInvalidBlock
		}
	default:
		return ErrInvalidBlock
	}
	return nil
}

func validTrailerMetadata(metadata [96]byte) bool {
	previousAddress := binary.LittleEndian.Uint64(metadata[32:40])
	var previous protocol.Checksum
	copy(previous[:], metadata[:16])
	return (previousAddress == 0) == previous.IsZero() && allZeroBytes(metadata[16:32]) && allZeroBytes(metadata[40:])
}

func validManifestMetadata(metadata [96]byte) bool {
	previousAddress := binary.LittleEndian.Uint64(metadata[32:40])
	var previous protocol.Checksum
	copy(previous[:], metadata[:16])
	return (previousAddress == 0) == previous.IsZero() &&
		allZeroBytes(metadata[16:32]) &&
		binary.LittleEndian.Uint32(metadata[40:44]) > 0 &&
		allZeroBytes(metadata[44:])
}
