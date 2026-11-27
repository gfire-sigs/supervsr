package replication

import (
	"context"
	"errors"
	"fmt"

	"z.gfire.dev/supervsr/replication/protocol"
)

var (
	ErrStorageNotEmpty             = errors.New("replication: format target is not empty")
	ErrHardwareChecksumUnavailable = errors.New("replication: hardware AES checksum unavailable")
)

// FormatConfig supplies the immutable cluster parameters used to initialize a
// fresh replica storage file. Setting RequireHardwareChecksum rejects formatting
// on CPUs without hardware AES support.
type FormatConfig struct {
	Group                   protocol.GroupID
	Membership              Membership
	Cluster                 ClusterConfig
	CurrentRelease          protocol.Release
	RequireHardwareChecksum bool
}

// FormatDependencies injects the target storage for Format.
type FormatDependencies struct {
	Storage Storage
}

// Format validates configuration and writes the initial superblock copies, root
// prepare slot, and header ring to empty storage.
func Format(ctx context.Context, cfg FormatConfig, dependencies FormatDependencies) error {
	if err := validateFormatConfig(cfg, dependencies); err != nil {
		return err
	}
	return formatAtView(ctx, cfg, dependencies.Storage, 0, 0)
}

func formatAtView(ctx context.Context, cfg FormatConfig, storage Storage, view, logView protocol.View) error {
	size, err := storage.Size()
	if err != nil {
		return err
	}
	if size != 0 {
		return ErrStorageNotEmpty
	}
	layout, ok := deriveWALLayout(cfg.Cluster)
	if !ok {
		return ErrInvalidConfiguration
	}
	if err := storage.Resize(layout.BlockBase); err != nil {
		return err
	}
	if err := zeroStorage(ctx, storage, layout.BlockBase); err != nil {
		return err
	}

	memberCount := cfg.Membership.ActiveCount + cfg.Membership.StandbyCount
	wal, err := newWAL(storage, cfg.Cluster, cfg.Group, memberCount)
	if err != nil {
		return err
	}
	root, err := rootPrepareHeader(cfg.Group, memberCount, uint32(cfg.Cluster.MessageSizeMax), cfg.Cluster.ChecksumAlgorithm)
	if err != nil {
		return err
	}
	wal.slots[0].Authoritative = root
	wal.slots[0].Redundant = root
	copy(wal.headerRing[:protocol.HeaderSize], root[:])
	clear(wal.prepareBuffer)
	copy(wal.prepareBuffer[:protocol.HeaderSize], root[:])
	if err := storage.WriteAt(wal.prepareBuffer, layout.PrepareBase); err != nil {
		return fmt.Errorf("%w: root prepare: %w", ErrStorage, err)
	}
	if err := storage.Sync(); err != nil {
		return err
	}
	if err := storage.WriteAt(wal.headerRing, layout.HeaderBase); err != nil {
		return fmt.Errorf("%w: header ring: %w", ErrStorage, err)
	}
	if err := storage.Sync(); err != nil {
		return err
	}

	emptyChecksum, err := protocol.ChecksumBytes(cfg.Cluster.ChecksumAlgorithm, nil)
	if err != nil {
		return err
	}
	checkpoint := CheckpointState{
		Header:                    root,
		AcquiredAggregateChecksum: emptyChecksum,
		ReleasedAggregateChecksum: emptyChecksum,
		SessionAggregateChecksum:  emptyChecksum,
		LogicalStorageSize:        layout.BlockBase,
		Release:                   cfg.CurrentRelease,
	}
	superblock := superblockRecord{
		FormatVersion: protocol.FormatVersion,
		Release:       cfg.CurrentRelease,
		Sequence:      1,
		Group:         cfg.Group,
		State: durableReplicaState{
			Checkpoint:   checkpoint,
			LocalMember:  cfg.Membership.LocalMember,
			Members:      cfg.Membership.Members,
			LogView:      logView,
			View:         view,
			ActiveCount:  cfg.Membership.ActiveCount,
			StandbyCount: cfg.Membership.StandbyCount,
		},
		ViewHeaderCount:       1,
		ConfigurationChecksum: cfg.Cluster.Fingerprint(),
		ProtocolVersion:       protocol.ProtocolVersion,
	}
	superblock.ViewHeaders[0] = root
	validation := superblockValidation{
		Group:                 cfg.Group,
		Membership:            cfg.Membership,
		ConfigurationChecksum: cfg.Cluster.Fingerprint(),
		Cluster:               cfg.Cluster,
	}
	writeCopies, _ := superblockWriteCopies(cfg.Cluster.SuperblockCopies)
	buffer, err := NewAlignedBuffer(SuperblockBytes, SectorSize)
	if err != nil {
		return err
	}
	for physicalIndex := range writeCopies {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := superblock.Encode(buffer, uint16(physicalIndex), validation); err != nil {
			return err
		}
		if err := storage.WriteAt(buffer, uint64(physicalIndex)*SuperblockBytes); err != nil {
			return err
		}
		if err := storage.Sync(); err != nil {
			return err
		}
	}
	if err := storage.SyncParent(); err != nil {
		return err
	}
	return nil
}

func validateFormatConfig(cfg FormatConfig, dependencies FormatDependencies) error {
	if dependencies.Storage == nil {
		return &ConfigError{Field: "Storage", Reason: "must be supplied"}
	}
	if cfg.RequireHardwareChecksum && !protocol.HardwareChecksumAvailable() {
		return ErrHardwareChecksumUnavailable
	}
	if cfg.Group.IsZero() || cfg.CurrentRelease == 0 {
		return ErrInvalidConfiguration
	}
	if err := cfg.Membership.Validate(); err != nil {
		return errors.Join(ErrInvalidConfiguration, err)
	}
	return cfg.Cluster.Validate(cfg.Membership.ActiveCount, cfg.Membership.StandbyCount)
}

func zeroStorage(ctx context.Context, storage Storage, size uint64) error {
	const zeroChunkSize = 1 << 20
	bufferSize := min(size, uint64(zeroChunkSize))
	buffer, err := NewAlignedBuffer(bufferSize, SectorSize)
	if err != nil {
		return err
	}
	for offset := uint64(0); offset < size; offset += uint64(len(buffer)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		length := min(uint64(len(buffer)), size-offset)
		if err := storage.WriteAt(buffer[:length], offset); err != nil {
			return err
		}
	}
	return storage.Sync()
}

func rootPrepareHeader(group protocol.GroupID, memberCount uint8, messageSizeMax uint32, algorithm ChecksumAlgorithm) ([protocol.HeaderSize]byte, error) {
	var encoded [protocol.HeaderSize]byte
	header := protocol.Header{
		Group:    group,
		Protocol: protocol.ProtocolVersion,
		Command:  protocol.CommandPrepare,
		Author:   0,
	}
	header.Fields[124] = byte(protocol.OperationRoot)
	if err := protocol.SealFrame(encoded[:], &header, algorithm); err != nil {
		return [protocol.HeaderSize]byte{}, err
	}
	if _, reason := protocol.DecodeHeader(encoded[:], group, messageSizeMax, memberCount, algorithm); reason != protocol.RejectNone {
		return [protocol.HeaderSize]byte{}, ErrInvalidWAL
	}
	return encoded, nil
}
