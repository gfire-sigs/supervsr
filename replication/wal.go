package replication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"z.gfire.dev/supervsr/replication/internal/simhook"
	"z.gfire.dev/supervsr/replication/protocol"
)

var (
	ErrInvalidWAL       = errors.New("replication: invalid WAL")
	ErrWALSlotUnsafe    = errors.New("replication: WAL slot cannot be reused")
	ErrWALRemoteRepair  = errors.New("replication: WAL slot requires remote repair")
	ErrWALUncertainSolo = errors.New("replication: solo WAL uncertainty")
)

type walLayout = simhook.WALLayout

func deriveWALLayout(cfg ClusterConfig) (walLayout, bool) {
	return simhook.DeriveWALLayout(cfg.SuperblockCopies, cfg.JournalSlots, cfg.MessageSizeMax, cfg.ClientsMax, cfg.BlockSize)
}

type walSlot struct {
	Authoritative   [protocol.HeaderSize]byte
	Redundant       [protocol.HeaderSize]byte
	PrepareChecksum protocol.Checksum
	Op              protocol.Op
	Generation      uint64
	Inhabited       bool
	Dirty           bool
	Faulty          bool
}

type walJournal struct {
	mu            sync.Mutex
	storage       Storage
	config        ClusterConfig
	group         protocol.GroupID
	memberCount   uint8
	layout        walLayout
	slots         []walSlot
	headerRing    []byte
	prepareBuffer []byte
	ioActive      bool
}

func newWAL(storage Storage, cfg ClusterConfig, group protocol.GroupID, memberCount uint8) (*walJournal, error) {
	layout, ok := deriveWALLayout(cfg)
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
	wal := &walJournal{
		storage:       storage,
		config:        cfg,
		group:         group,
		memberCount:   memberCount,
		layout:        layout,
		slots:         make([]walSlot, int(cfg.JournalSlots)),
		headerRing:    headerRing,
		prepareBuffer: prepareBuffer,
	}
	for index := range wal.slots {
		reserved, buildErr := reservedPrepareHeader(group, protocol.Op(index), memberCount, uint32(cfg.MessageSizeMax), cfg.ChecksumAlgorithm)
		if buildErr != nil {
			return nil, buildErr
		}
		wal.slots[index].Authoritative = reserved
		wal.slots[index].Redundant = reserved
		copy(wal.headerRing[index*protocol.HeaderSize:(index+1)*protocol.HeaderSize], reserved[:])
	}
	return wal, nil
}

func (wal *walJournal) Layout() walLayout {
	return wal.layout
}

func (wal *walJournal) HasFaultySlots() bool {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	for index := range wal.slots {
		if wal.slots[index].Faulty {
			return true
		}
	}
	return false
}

func (wal *walJournal) Append(frame []byte, reusableThrough protocol.Op) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	var sequence ioSequence
	if err := wal.beginAppend(&sequence, frame, reusableThrough); err != nil {
		return err
	}
	defer func() { wal.ioActive = false }()
	for sequence.phase != ioPhaseCompletion {
		if err := wal.advanceAppend(&sequence, frame); err != nil {
			return err
		}
	}
	return nil
}

func (wal *walJournal) beginAppend(sequence *ioSequence, frame []byte, reusableThrough protocol.Op) error {
	if wal.ioActive {
		return ErrIOBackpressure
	}
	header, _, reason := protocol.DecodeFrame(frame, wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount, wal.config.ChecksumAlgorithm)
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
	copy(slot.Authoritative[:], frame[:protocol.HeaderSize])
	slot.Op = op
	slot.Dirty = true
	clear(wal.prepareBuffer)
	copy(wal.prepareBuffer, frame)
	prepareOffset, ok := wal.prepareOffset(index)
	if !ok {
		return ErrInvalidWAL
	}
	*sequence = ioSequence{phase: ioPhaseWrite, storage: wal.storage, buffer: wal.prepareBuffer, offset: prepareOffset, size: uint64(len(wal.prepareBuffer)), index: index, generation: slot.Generation, checksum: header.HeaderChecksum}
	wal.ioActive = true
	return nil
}

func (wal *walJournal) advanceAppend(sequence *ioSequence, frame []byte) error {
	slot := &wal.slots[sequence.index]
	if err := sequence.physical(); err != nil {
		labels := [...]string{"prepare", "durable prepare", "header", "durable header"}
		return fmt.Errorf("%w: %s op %d: %w", ErrStorage, labels[sequence.step], slot.Op, err)
	}
	switch sequence.step {
	case 0, 2:
		sequence.syncNext()
	case 1:
		headerOffset := sequence.index * protocol.HeaderSize
		copy(wal.headerRing[headerOffset:headerOffset+protocol.HeaderSize], frame[:protocol.HeaderSize])
		sectorOffset := headerOffset &^ (SectorSize - 1)
		sequence.phase = ioPhaseWrite
		sequence.buffer = wal.headerRing[sectorOffset : sectorOffset+SectorSize]
		sequence.offset = wal.layout.HeaderBase + sectorOffset
		sequence.size = SectorSize
	case 3:
		if slot.Generation != sequence.generation || slot.Authoritative != [protocol.HeaderSize]byte(frame[:protocol.HeaderSize]) {
			return ErrInvalidWAL
		}
		slot.Redundant = slot.Authoritative
		slot.PrepareChecksum = sequence.checksum
		slot.Inhabited = true
		slot.Dirty = false
		slot.Faulty = false
		sequence.phase = ioPhaseCompletion
	}
	sequence.step++
	return nil
}

func reservedPrepareHeader(group protocol.GroupID, physicalSlot protocol.Op, memberCount uint8, messageSizeMax uint32, algorithm ChecksumAlgorithm) ([protocol.HeaderSize]byte, error) {
	var encoded [protocol.HeaderSize]byte
	header := protocol.Header{
		Group:    group,
		Protocol: protocol.ProtocolVersion,
		Command:  protocol.CommandPrepare,
		Author:   0,
	}
	binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(physicalSlot))
	header.Fields[124] = byte(protocol.OperationReserved)
	if err := protocol.SealFrame(encoded[:], &header, algorithm); err != nil {
		return [protocol.HeaderSize]byte{}, err
	}
	if _, reason := protocol.DecodeHeader(encoded[:], group, messageSizeMax, memberCount, algorithm); reason != protocol.RejectNone {
		return [protocol.HeaderSize]byte{}, ErrInvalidWAL
	}
	return encoded, nil
}

type walCandidateKind uint8

const (
	walCandidateInvalid walCandidateKind = iota
	walCandidateReserved
	walCandidateOrdinary
)

type walCandidate struct {
	Kind           walCandidateKind
	Op             protocol.Op
	View           protocol.View
	HeaderChecksum protocol.Checksum
	BodyChecksum   protocol.Checksum
}

type walRecoveryDecision uint8

const (
	walRecoveryClean walRecoveryDecision = iota
	walRecoveryCleanEmpty
	walRecoveryLocalRepair
	walRecoveryRemoteRepair
	walRecoveryTruncate
	walRecoveryFail
)

type walRecoveryContext struct {
	PhysicalSlot uint64
	JournalSlots uint64
	RetainedMin  protocol.Op
	PrepareMax   protocol.Op
	UntrustedMax protocol.Op
	Installed    walRecoveryView
}

func classifyWALSlot(header, prepare walCandidate, context walRecoveryContext) walRecoveryDecision {
	if impossibleWALCandidate(header, context) || impossibleWALCandidate(prepare, context) {
		return walRecoveryFail
	}
	futureHeader := futureWALCandidate(header, context)
	futurePrepare := futureWALCandidate(prepare, context)
	if futureHeader || futurePrepare {
		if header.Kind == walCandidateInvalid || prepare.Kind == walCandidateInvalid {
			return walRecoveryRemoteRepair
		}
		if futureHeader && retainedOrdinary(prepare, context) {
			return walRecoveryLocalRepair
		}
		if futurePrepare && retainedOrdinary(header, context) {
			return walRecoveryRemoteRepair
		}
		return walRecoveryTruncate
	}
	obsoleteHeader := obsoleteWALCandidate(header, context.Installed)
	obsoletePrepare := obsoleteWALCandidate(prepare, context.Installed)
	if obsoleteHeader || obsoletePrepare {
		if header.Kind == walCandidateInvalid || prepare.Kind == walCandidateInvalid {
			return walRecoveryRemoteRepair
		}
		retainedHeader := header.Kind == walCandidateOrdinary && !obsoleteHeader
		retainedPrepare := prepare.Kind == walCandidateOrdinary && !obsoletePrepare
		if !retainedHeader && !retainedPrepare {
			return walRecoveryTruncate
		}
		if obsoleteHeader && retainedOrdinary(prepare, context) {
			return walRecoveryLocalRepair
		}
		return walRecoveryRemoteRepair
	}
	switch {
	case header.Kind == walCandidateOrdinary && prepare.Kind == walCandidateOrdinary && candidatesEqual(header, prepare):
		return walRecoveryClean
	case header.Kind == walCandidateReserved && prepare.Kind == walCandidateReserved:
		return walRecoveryCleanEmpty
	case header.Kind == walCandidateReserved && retainedOrdinary(prepare, context):
		return walRecoveryLocalRepair
	case header.Kind == walCandidateInvalid && retainedOrdinary(prepare, context) && prepare.Op == context.UntrustedMax:
		return walRecoveryLocalRepair
	case header.Kind == walCandidateOrdinary && retainedOrdinary(prepare, context) && prepare.Op > header.Op:
		return walRecoveryLocalRepair
	case header.Kind == walCandidateOrdinary && (prepare.Kind == walCandidateInvalid || prepare.Kind == walCandidateReserved):
		// A durable append can lose its body later; recency cannot prove it uncommitted.
		return walRecoveryRemoteRepair
	case header.Kind == walCandidateInvalid && prepare.Kind != walCandidateOrdinary:
		return walRecoveryRemoteRepair
	case header.Kind == walCandidateInvalid && prepare.Kind == walCandidateOrdinary:
		return walRecoveryRemoteRepair
	case header.Kind == walCandidateOrdinary && prepare.Kind == walCandidateOrdinary:
		return walRecoveryRemoteRepair
	default:
		return walRecoveryRemoteRepair
	}
}

func candidatesEqual(left, right walCandidate) bool {
	return left.Op == right.Op && left.View == right.View && left.HeaderChecksum == right.HeaderChecksum && left.BodyChecksum == right.BodyChecksum
}

func retainedOrdinary(candidate walCandidate, context walRecoveryContext) bool {
	return candidate.Kind == walCandidateOrdinary && candidate.Op >= context.RetainedMin && candidate.Op <= context.PrepareMax
}

func futureWALCandidate(candidate walCandidate, context walRecoveryContext) bool {
	return candidate.Kind == walCandidateOrdinary && candidate.Op > context.PrepareMax
}

func impossibleWALCandidate(candidate walCandidate, context walRecoveryContext) bool {
	return candidate.Kind == walCandidateOrdinary && (context.JournalSlots == 0 || uint64(candidate.Op)%context.JournalSlots != context.PhysicalSlot)
}

func obsoleteWALCandidate(candidate walCandidate, installed walRecoveryView) bool {
	return candidate.Kind == walCandidateOrdinary && candidate.Op > installed.HeadOp && candidate.View < installed.LogView
}
