package replication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var (
	ErrInvalidWAL       = errors.New("replication: invalid WAL")
	ErrWALSlotUnsafe    = errors.New("replication: WAL slot cannot be reused")
	ErrWALRemoteRepair  = errors.New("replication: WAL slot requires remote repair")
	ErrWALUncertainSolo = errors.New("replication: solo WAL uncertainty")
)

type WALLayout struct {
	HeaderBase    uint64
	HeaderSize    uint64
	PrepareBase   uint64
	PrepareStride uint64
	PrepareSize   uint64
	ReplyBase     uint64
	ReplyStride   uint64
	ReplySize     uint64
	BlockBase     uint64
}

func DeriveWALLayout(cfg ClusterConfig) (WALLayout, bool) {
	superblockSize, ok := checkedMul(cfg.SuperblockCopies, SuperblockBytes)
	if !ok {
		return WALLayout{}, false
	}
	headerBytes, ok := checkedMul(cfg.JournalSlots, protocol.HeaderSize)
	if !ok {
		return WALLayout{}, false
	}
	headerSize, ok := checkedAlignUp(headerBytes, SectorSize)
	if !ok {
		return WALLayout{}, false
	}
	prepareBase, ok := checkedAdd(superblockSize, headerSize)
	if !ok {
		return WALLayout{}, false
	}
	prepareStride, ok := checkedAlignUp(cfg.MessageSizeMax, SectorSize)
	if !ok {
		return WALLayout{}, false
	}
	prepareSize, ok := checkedMul(cfg.JournalSlots, prepareStride)
	if !ok {
		return WALLayout{}, false
	}
	replyBase, ok := checkedAdd(prepareBase, prepareSize)
	if !ok {
		return WALLayout{}, false
	}
	replySize, ok := checkedMul(cfg.ClientsMax, prepareStride)
	if !ok {
		return WALLayout{}, false
	}
	end, ok := checkedAdd(replyBase, replySize)
	if !ok {
		return WALLayout{}, false
	}
	blockBase, ok := checkedAlignUp(end, cfg.BlockSize)
	if !ok {
		return WALLayout{}, false
	}
	return WALLayout{
		HeaderBase:    superblockSize,
		HeaderSize:    headerSize,
		PrepareBase:   prepareBase,
		PrepareStride: prepareStride,
		PrepareSize:   prepareSize,
		ReplyBase:     replyBase,
		ReplyStride:   prepareStride,
		ReplySize:     replySize,
		BlockBase:     blockBase,
	}, true
}

type WALSlot struct {
	Authoritative   [protocol.HeaderSize]byte
	Redundant       [protocol.HeaderSize]byte
	PrepareChecksum protocol.Checksum
	Op              protocol.Op
	Generation      uint64
	Inhabited       bool
	Dirty           bool
	Faulty          bool
}

type WAL struct {
	mu            sync.Mutex
	storage       Storage
	config        ClusterConfig
	group         protocol.GroupID
	memberCount   uint8
	layout        WALLayout
	slots         []WALSlot
	headerRing    []byte
	prepareBuffer []byte
}

func NewWAL(storage Storage, cfg ClusterConfig, group protocol.GroupID, memberCount uint8) (*WAL, error) {
	layout, ok := DeriveWALLayout(cfg)
	if !ok || cfg.JournalSlots > uint64(int(^uint(0)>>1)) || cfg.MessageSizeMax > uint64(int(^uint(0)>>1)) || memberCount == 0 {
		return nil, ErrInvalidWAL
	}
	headerRing, err := NewAlignedBuffer(layout.HeaderSize, SectorSize)
	if err != nil {
		return nil, err
	}
	prepareBuffer, err := NewAlignedBuffer(layout.PrepareStride, SectorSize)
	if err != nil {
		return nil, err
	}
	wal := &WAL{
		storage:       storage,
		config:        cfg,
		group:         group,
		memberCount:   memberCount,
		layout:        layout,
		slots:         make([]WALSlot, int(cfg.JournalSlots)),
		headerRing:    headerRing,
		prepareBuffer: prepareBuffer,
	}
	for index := range wal.slots {
		reserved, buildErr := ReservedPrepareHeader(group, protocol.Op(index), memberCount, uint32(cfg.MessageSizeMax))
		if buildErr != nil {
			return nil, buildErr
		}
		wal.slots[index].Authoritative = reserved
		wal.slots[index].Redundant = reserved
		copy(wal.headerRing[index*protocol.HeaderSize:(index+1)*protocol.HeaderSize], reserved[:])
	}
	return wal, nil
}

func (wal *WAL) Layout() WALLayout {
	return wal.layout
}

func (wal *WAL) Append(frame []byte, reusableThrough protocol.Op) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	header, _, reason := protocol.DecodeFrame(frame, wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount)
	if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare {
		return ErrInvalidWAL
	}
	op := protocol.Op(binary.LittleEndian.Uint64(header.Fields[96:104]))
	index := uint64(op) % wal.config.JournalSlots
	slot := &wal.slots[index]
	if slot.Inhabited && slot.Op != op && slot.Op > reusableThrough {
		return ErrWALSlotUnsafe
	}

	slot.Generation++
	generation := slot.Generation
	copy(slot.Authoritative[:], frame[:protocol.HeaderSize])
	slot.Op = op
	slot.Dirty = true
	clear(wal.prepareBuffer)
	copy(wal.prepareBuffer, frame)
	prepareOffset, ok := checkedMul(index, wal.layout.PrepareStride)
	if ok {
		prepareOffset, ok = checkedAdd(wal.layout.PrepareBase, prepareOffset)
	}
	if !ok {
		return ErrInvalidWAL
	}
	if err := wal.storage.WriteAt(wal.prepareBuffer, prepareOffset); err != nil {
		return fmt.Errorf("%w: prepare op %d: %w", ErrStorage, op, err)
	}
	if err := wal.storage.Sync(); err != nil {
		return fmt.Errorf("%w: durable prepare op %d: %w", ErrStorage, op, err)
	}

	headerOffset := index * protocol.HeaderSize
	copy(wal.headerRing[headerOffset:headerOffset+protocol.HeaderSize], slot.Authoritative[:])
	sectorOffset := headerOffset &^ (SectorSize - 1)
	sector := wal.headerRing[sectorOffset : sectorOffset+SectorSize]
	if err := wal.storage.WriteAt(sector, wal.layout.HeaderBase+sectorOffset); err != nil {
		return fmt.Errorf("%w: header op %d: %w", ErrStorage, op, err)
	}
	if err := wal.storage.Sync(); err != nil {
		return fmt.Errorf("%w: durable header op %d: %w", ErrStorage, op, err)
	}
	if slot.Generation != generation || slot.Authoritative != [protocol.HeaderSize]byte(frame[:protocol.HeaderSize]) {
		return ErrInvalidWAL
	}
	slot.Redundant = slot.Authoritative
	slot.PrepareChecksum = header.HeaderChecksum
	slot.Inhabited = true
	slot.Dirty = false
	slot.Faulty = false
	return nil
}

func ReservedPrepareHeader(group protocol.GroupID, physicalSlot protocol.Op, memberCount uint8, messageSizeMax uint32) ([protocol.HeaderSize]byte, error) {
	var encoded [protocol.HeaderSize]byte
	header := protocol.Header{
		Group:    group,
		Protocol: protocol.ProtocolVersion,
		Command:  protocol.CommandPrepare,
		Author:   0,
	}
	binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(physicalSlot))
	header.Fields[124] = byte(protocol.OperationReserved)
	if err := protocol.SealFrame(encoded[:], &header); err != nil {
		return [protocol.HeaderSize]byte{}, err
	}
	if _, reason := protocol.DecodeHeader(encoded[:], group, messageSizeMax, memberCount); reason != protocol.RejectNone {
		return [protocol.HeaderSize]byte{}, ErrInvalidWAL
	}
	return encoded, nil
}

type WALCandidateKind uint8

const (
	WALCandidateInvalid WALCandidateKind = iota
	WALCandidateReserved
	WALCandidateOrdinary
)

type WALCandidate struct {
	Kind           WALCandidateKind
	Op             protocol.Op
	View           protocol.View
	HeaderChecksum protocol.Checksum
	BodyChecksum   protocol.Checksum
}

type WALRecoveryDecision uint8

const (
	WALRecoveryClean WALRecoveryDecision = iota
	WALRecoveryCleanEmpty
	WALRecoveryLocalRepair
	WALRecoveryRemoteRepair
	WALRecoveryTruncate
	WALRecoveryFail
)

type WALRecoveryContext struct {
	PhysicalSlot uint64
	JournalSlots uint64
	RetainedMin  protocol.Op
	PrepareMax   protocol.Op
	UntrustedMax protocol.Op
	TornMin      protocol.Op
}

func ClassifyWALSlot(header, prepare WALCandidate, context WALRecoveryContext) WALRecoveryDecision {
	if impossibleWALCandidate(header, context) || impossibleWALCandidate(prepare, context) {
		return WALRecoveryFail
	}
	if futureWALCandidate(header, context) || futureWALCandidate(prepare, context) {
		return WALRecoveryTruncate
	}
	switch {
	case header.Kind == WALCandidateOrdinary && prepare.Kind == WALCandidateOrdinary && candidatesEqual(header, prepare):
		return WALRecoveryClean
	case header.Kind == WALCandidateReserved && prepare.Kind == WALCandidateReserved:
		return WALRecoveryCleanEmpty
	case header.Kind == WALCandidateReserved && retainedOrdinary(prepare, context):
		return WALRecoveryLocalRepair
	case header.Kind == WALCandidateInvalid && retainedOrdinary(prepare, context) && prepare.Op == context.UntrustedMax:
		return WALRecoveryLocalRepair
	case header.Kind == WALCandidateOrdinary && retainedOrdinary(prepare, context) && prepare.Op > header.Op:
		return WALRecoveryLocalRepair
	case header.Kind == WALCandidateOrdinary && (prepare.Kind == WALCandidateInvalid || prepare.Kind == WALCandidateReserved):
		if header.Op >= context.TornMin && header.Op <= context.UntrustedMax {
			return WALRecoveryTruncate
		}
		return WALRecoveryRemoteRepair
	case header.Kind == WALCandidateInvalid && prepare.Kind != WALCandidateOrdinary:
		return WALRecoveryRemoteRepair
	case header.Kind == WALCandidateInvalid && prepare.Kind == WALCandidateOrdinary:
		return WALRecoveryRemoteRepair
	case header.Kind == WALCandidateOrdinary && prepare.Kind == WALCandidateOrdinary:
		return WALRecoveryRemoteRepair
	default:
		return WALRecoveryRemoteRepair
	}
}

func candidatesEqual(left, right WALCandidate) bool {
	return left.Op == right.Op && left.View == right.View && left.HeaderChecksum == right.HeaderChecksum && left.BodyChecksum == right.BodyChecksum
}

func retainedOrdinary(candidate WALCandidate, context WALRecoveryContext) bool {
	return candidate.Kind == WALCandidateOrdinary && candidate.Op >= context.RetainedMin && candidate.Op <= context.PrepareMax
}

func futureWALCandidate(candidate WALCandidate, context WALRecoveryContext) bool {
	return candidate.Kind == WALCandidateOrdinary && candidate.Op > context.PrepareMax
}

func impossibleWALCandidate(candidate WALCandidate, context WALRecoveryContext) bool {
	return candidate.Kind == WALCandidateOrdinary && (context.JournalSlots == 0 || uint64(candidate.Op)%context.JournalSlots != context.PhysicalSlot)
}
