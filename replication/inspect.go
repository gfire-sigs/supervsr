package replication

import (
	"errors"

	"z.gfire.dev/supervsr/replication/protocol"
)

// InspectConfig identifies the expected cluster identity and layout used to
// verify a replica storage file during read-only inspection.
type InspectConfig struct {
	Group      GroupID
	Membership Membership
	Cluster    ClusterConfig
}

// StorageInspection summarizes the durable superblock and checkpoint metadata
// recovered from a replica storage file without mutating disk state.
type StorageInspection struct {
	FormatVersion         uint16
	ProtocolVersion       uint16
	Release               Release
	Sequence              uint64
	Group                 GroupID
	ConfigurationChecksum Checksum
	LocalMember           MemberID
	Members               [MembersMax]MemberID
	ActiveCount           uint8
	StandbyCount          uint8
	View                  View
	LogView               View
	CommitMax             Op
	SyncMin               Op
	SyncMax               Op
	CheckpointOp          Op
	CheckpointRelease     Release
	CheckpointID          protocol.CheckpointID
	ViewHeaderCount       uint32
	FileSize              uint64
	ValidCopies           uint8
	TotalCopies           uint8
	MisdirectedCopies     uint8
}

// InspectStorage reads and validates the superblock copies of storage against
// cfg without modifying or repairing the underlying file.
func InspectStorage(storage Storage, cfg InspectConfig) (StorageInspection, error) {
	if storage == nil || cfg.Group.IsZero() {
		return StorageInspection{}, ErrInvalidConfiguration
	}
	if err := cfg.Membership.Validate(); err != nil {
		return StorageInspection{}, err
	}
	if err := cfg.Cluster.Validate(cfg.Membership.ActiveCount, cfg.Membership.StandbyCount); err != nil {
		return StorageInspection{}, err
	}
	validation := superblockValidation{
		Group:                 cfg.Group,
		Membership:            cfg.Membership,
		ConfigurationChecksum: cfg.Cluster.Fingerprint(),
		Cluster:               cfg.Cluster,
	}
	minimumSize, ok := cfg.Cluster.BlockBase()
	if !ok {
		return StorageInspection{}, ErrInvalidConfiguration
	}
	size, err := storage.Size()
	if err != nil {
		return StorageInspection{}, err
	}
	if size == 0 {
		return StorageInspection{}, ErrUnformatted
	}
	if size < minimumSize || (size-minimumSize)%cfg.Cluster.BlockSize != 0 {
		return StorageInspection{}, ErrCorrupt
	}
	buffer, err := NewAlignedBuffer(SuperblockBytes, SectorSize)
	if err != nil {
		return StorageInspection{}, err
	}
	copies := int(cfg.Cluster.SuperblockCopies)
	candidates := make([]superblockCandidate, 0, copies)
	allZero := true
	successfulReads := 0
	var firstReadError error
	incompatibleCopies := 0
	var misdirectedCopies uint8

	for index := range copies {
		clear(buffer)
		if err := storage.ReadAt(buffer, uint64(index)*SuperblockBytes); err != nil {
			if firstReadError == nil {
				firstReadError = err
			}
			continue
		}
		successfulReads++
		allZero = allZero && allZeroBytes(buffer)
		candidate, decodeErr := decodeSuperblock(buffer, uint16(index), validation)
		if decodeErr != nil {
			if errors.Is(decodeErr, ErrIncompatibleConfiguration) {
				incompatibleCopies++
			}
			continue
		}
		if candidate.MisdirectedIndex {
			misdirectedCopies++
		}
		candidates = append(candidates, candidate)
	}
	if len(candidates) == 0 {
		if successfulReads == 0 && firstReadError != nil {
			return StorageInspection{}, errors.Join(ErrStorage, firstReadError)
		}
		if allZero {
			return StorageInspection{}, ErrUnformatted
		}
		openQuorum, _ := superblockOpenQuorum(cfg.Cluster.SuperblockCopies)
		if incompatibleCopies >= int(openQuorum) {
			return StorageInspection{}, ErrIncompatibleConfiguration
		}
		return StorageInspection{}, ErrCorrupt
	}
	selected, err := selectSuperblock(candidates, cfg.Cluster.SuperblockCopies)
	if err != nil {
		if errors.Is(err, ErrSuperblockInitializationIncomplete) {
			return StorageInspection{}, errors.Join(ErrUnformatted, err)
		}
		return StorageInspection{}, errors.Join(ErrCorrupt, err)
	}
	record := selected.superblockRecord
	checkpointID, err := record.State.Checkpoint.ID(cfg.Cluster.ChecksumAlgorithm)
	if err != nil {
		return StorageInspection{}, err
	}
	return StorageInspection{
		FormatVersion:         record.FormatVersion,
		ProtocolVersion:       record.ProtocolVersion,
		Release:               record.Release,
		Sequence:              record.Sequence,
		Group:                 record.Group,
		ConfigurationChecksum: record.ConfigurationChecksum,
		LocalMember:           record.State.LocalMember,
		Members:               record.State.Members,
		ActiveCount:           record.State.ActiveCount,
		StandbyCount:          record.State.StandbyCount,
		View:                  record.State.View,
		LogView:               record.State.LogView,
		CommitMax:             record.State.CommitMax,
		SyncMin:               record.State.SyncMin,
		SyncMax:               record.State.SyncMax,
		CheckpointOp:          record.State.Checkpoint.PrepareOp(),
		CheckpointRelease:     record.State.Checkpoint.Release,
		CheckpointID:          checkpointID,
		ViewHeaderCount:       record.ViewHeaderCount,
		FileSize:              size,
		ValidCopies:           uint8(len(candidates)),
		TotalCopies:           uint8(copies),
		MisdirectedCopies:     misdirectedCopies,
	}, nil
}
