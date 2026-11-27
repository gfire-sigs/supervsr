package replication

import (
	"encoding/binary"
	"errors"
	"fmt"

	"z.gfire.dev/supervsr/replication/protocol"
)

var ErrWALRecovery = errors.New("replication: WAL recovery failed")

// walRecoveryView is the head selected when LogView was durably installed.
// Later prepares in that log view are not bounded by HeadOp.
type walRecoveryView struct {
	LogView protocol.View
	HeadOp  protocol.Op
}

type walRecoveryCounts struct {
	Clean        uint64
	CleanEmpty   uint64
	LocalRepair  uint64
	Truncated    uint64
	RemoteRepair uint64
}

type walRecoveryReport struct {
	HeadOp       protocol.Op
	HeadHeader   protocol.Header
	FaultySlots  uint32
	UntrustedMax protocol.Op
	Counts       walRecoveryCounts
}

func (wal *walJournal) Recover(checkpoint CheckpointState, commitMax protocol.Op, installed walRecoveryView) (walRecoveryReport, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if err := wal.storage.ReadAt(wal.headerRing, wal.layout.HeaderBase); err != nil {
		return walRecoveryReport{}, errors.Join(ErrWALRecovery, err)
	}
	count := len(wal.slots)
	headerCandidates := make([]walCandidate, count)
	prepareCandidates := make([]walCandidate, count)
	prepareHeaders := make([][protocol.HeaderSize]byte, count)
	untrustedMax := checkpoint.PrepareOp()
	for slot := range count {
		headerBytes := wal.headerRing[slot*protocol.HeaderSize : (slot+1)*protocol.HeaderSize]
		headerCandidates[slot] = wal.decodeRecoveryHeader(headerBytes, uint64(slot))
		untrustedMax = maxCandidateOp(untrustedMax, headerCandidates[slot])
		clear(wal.prepareBuffer)
		offset, ok := wal.prepareOffset(uint64(slot))
		if !ok {
			return walRecoveryReport{}, ErrWALRecovery
		}
		if err := wal.storage.ReadAt(wal.prepareBuffer, offset); err != nil {
			return walRecoveryReport{}, errors.Join(ErrWALRecovery, err)
		}
		candidate, encoded := wal.decodeRecoveryPrepare(wal.prepareBuffer, uint64(slot))
		prepareCandidates[slot] = candidate
		prepareHeaders[slot] = encoded
		untrustedMax = maxCandidateOp(untrustedMax, candidate)
	}
	prepareMax, ok := wal.prepareMaximum(checkpoint.PrepareOp())
	if !ok {
		return walRecoveryReport{}, ErrWALRecovery
	}
	repairHeaders := false
	faulty := uint32(0)
	firstFaulty := -1
	var counts walRecoveryCounts
	for slot := range count {
		context := walRecoveryContext{
			PhysicalSlot: uint64(slot),
			JournalSlots: wal.config.JournalSlots,
			RetainedMin:  checkpoint.PrepareOp(),
			PrepareMax:   prepareMax,
			UntrustedMax: untrustedMax,
			Installed:    installed,
		}
		decision := classifyWALSlot(headerCandidates[slot], prepareCandidates[slot], context)
		switch decision {
		case walRecoveryClean:
			counts.Clean++
			wal.installRecoveredSlot(slot, prepareHeaders[slot], prepareCandidates[slot], false)
		case walRecoveryCleanEmpty:
			counts.CleanEmpty++
			if err := wal.installReservedSlot(slot); err != nil {
				return walRecoveryReport{}, errors.Join(ErrWALRecovery, err)
			}
		case walRecoveryLocalRepair:
			counts.LocalRepair++
			wal.installRecoveredSlot(slot, prepareHeaders[slot], prepareCandidates[slot], false)
			copy(wal.headerRing[slot*protocol.HeaderSize:(slot+1)*protocol.HeaderSize], prepareHeaders[slot][:])
			repairHeaders = true
		case walRecoveryTruncate:
			counts.Truncated++
			if err := wal.installReservedSlot(slot); err != nil {
				return walRecoveryReport{}, errors.Join(ErrWALRecovery, err)
			}
			copy(wal.headerRing[slot*protocol.HeaderSize:(slot+1)*protocol.HeaderSize], wal.slots[slot].Authoritative[:])
			repairHeaders = true
		case walRecoveryRemoteRepair:
			counts.RemoteRepair++
			if err := wal.installReservedSlot(slot); err != nil {
				return walRecoveryReport{}, errors.Join(ErrWALRecovery, err)
			}
			wal.slots[slot].Dirty = true
			wal.slots[slot].Faulty = true
			faulty++
			if firstFaulty == -1 {
				firstFaulty = slot
			}
		case walRecoveryFail:
			return walRecoveryReport{}, fmt.Errorf("%w: impossible physical slot %d", ErrWALRecovery, slot)
		default:
			return walRecoveryReport{}, ErrWALRecovery
		}
	}
	if faulty != 0 && wal.memberCount == 1 {
		return walRecoveryReport{}, fmt.Errorf("%w: %d faulty slots, first physical slot %d header=%+v prepare=%+v", ErrWALUncertainSolo, faulty, firstFaulty, headerCandidates[firstFaulty], prepareCandidates[firstFaulty])
	}
	if repairHeaders {
		if err := wal.storage.WriteAt(wal.headerRing, wal.layout.HeaderBase); err != nil {
			return walRecoveryReport{}, errors.Join(ErrWALRecovery, err)
		}
		if err := wal.storage.Sync(); err != nil {
			return walRecoveryReport{}, errors.Join(ErrWALRecovery, err)
		}
	}
	head, reason := protocol.DecodeHeader(checkpoint.Header[:], wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount, wal.config.ChecksumAlgorithm)
	if reason != protocol.RejectNone || head.Command != protocol.CommandPrepare {
		return walRecoveryReport{}, ErrWALRecovery
	}
	headOp := checkpoint.PrepareOp()
	for next := headOp + 1; next <= untrustedMax; next++ {
		slot := uint64(next) % wal.config.JournalSlots
		recovered := &wal.slots[slot]
		if !recovered.Inhabited || recovered.Op != next {
			break
		}
		candidate, reason := protocol.DecodeHeader(recovered.Authoritative[:], wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount, wal.config.ChecksumAlgorithm)
		if reason != protocol.RejectNone || prepareParent(&candidate) != head.HeaderChecksum {
			break
		}
		head = candidate
		headOp = next
	}
	if commitMax > headOp && faulty == 0 {
		return walRecoveryReport{}, ErrWALRecovery
	}
	return walRecoveryReport{HeadOp: headOp, HeadHeader: head, FaultySlots: faulty, UntrustedMax: untrustedMax, Counts: counts}, nil
}

func (wal *walJournal) ReadPrepare(op protocol.Op, destination []byte) ([]byte, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if uint64(len(destination)) < wal.layout.PrepareStride {
		return nil, ErrInvalidWAL
	}
	slot := uint64(op) % wal.config.JournalSlots
	offset, ok := wal.prepareOffset(slot)
	if !ok {
		return nil, ErrInvalidWAL
	}
	physical := destination[:wal.layout.PrepareStride]
	if err := wal.storage.ReadAt(physical, offset); err != nil {
		return nil, errors.Join(ErrStorage, err)
	}

	if len(physical) < protocol.HeaderSize {
		return nil, ErrInvalidWAL
	}
	size := binary.LittleEndian.Uint32(physical[96:100])
	if size < protocol.HeaderSize || uint64(size) > wal.layout.PrepareStride || !allZeroBytes(physical[size:]) {
		return nil, ErrInvalidWAL
	}
	frame := physical[:size:size]
	header, _, reason := protocol.DecodeFrame(frame, wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount, wal.config.ChecksumAlgorithm)
	if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare || prepareOp(&header) != op {
		return nil, ErrInvalidWAL
	}
	return frame, nil
}
func (wal *walJournal) RecoveredHeader(op protocol.Op) (protocol.Header, bool) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	slot := &wal.slots[uint64(op)%wal.config.JournalSlots]
	if !slot.Inhabited || slot.Op != op || slot.Faulty {
		return protocol.Header{}, false
	}
	header, reason := protocol.DecodeHeader(slot.Authoritative[:], wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount, wal.config.ChecksumAlgorithm)
	return header, reason == protocol.RejectNone
}

func (wal *walJournal) JoinEvidence(op protocol.Op) (protocol.Header, bool, bool) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	slot := &wal.slots[uint64(op)%wal.config.JournalSlots]
	if slot.Faulty {
		return protocol.Header{}, false, false
	}
	if !slot.Inhabited || slot.Op != op {
		return protocol.Header{}, false, true
	}
	header, reason := protocol.DecodeHeader(slot.Authoritative[:], wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount, wal.config.ChecksumAlgorithm)
	if reason != protocol.RejectNone {
		return protocol.Header{}, false, false
	}
	return header, true, slot.Dirty
}

// DiscardAfter invalidates the cache only after the caller has drained writes
// and durably installed a view whose canonical head excludes this suffix.
func (wal *walJournal) DiscardAfter(head protocol.Op) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	for index := range wal.slots {
		slot := &wal.slots[index]
		if !slot.Inhabited || slot.Op <= head {
			continue
		}
		slot.Generation++
		if err := wal.installReservedSlot(index); err != nil {
			return errors.Join(ErrWALRecovery, err)
		}
		copy(wal.headerRing[index*protocol.HeaderSize:(index+1)*protocol.HeaderSize], slot.Authoritative[:])
	}
	return nil
}

func (wal *walJournal) decodeRecoveryHeader(encoded []byte, physical uint64) walCandidate {
	header, reason := protocol.DecodeHeader(encoded, wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount, wal.config.ChecksumAlgorithm)
	if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare {
		return walCandidate{Kind: walCandidateInvalid}
	}
	return recoveryCandidate(header, physical)
}

func (wal *walJournal) decodeRecoveryPrepare(physical []byte, slot uint64) (walCandidate, [protocol.HeaderSize]byte) {
	var encoded [protocol.HeaderSize]byte
	if allZeroBytes(physical) {
		reserved, err := reservedPrepareHeader(wal.group, protocol.Op(slot), wal.memberCount, uint32(wal.config.MessageSizeMax), wal.config.ChecksumAlgorithm)
		if err != nil {
			return walCandidate{Kind: walCandidateInvalid}, encoded
		}
		return walCandidate{Kind: walCandidateReserved, Op: protocol.Op(slot)}, reserved
	}
	if len(physical) < protocol.HeaderSize {
		return walCandidate{Kind: walCandidateInvalid}, encoded
	}
	size := binary.LittleEndian.Uint32(physical[96:100])
	if size < protocol.HeaderSize || uint64(size) > wal.layout.PrepareStride || !allZeroBytes(physical[size:]) {
		return walCandidate{Kind: walCandidateInvalid}, encoded
	}
	header, _, reason := protocol.DecodeFrame(physical[:size], wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount, wal.config.ChecksumAlgorithm)
	if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare {
		return walCandidate{Kind: walCandidateInvalid}, encoded
	}
	copy(encoded[:], physical[:protocol.HeaderSize])
	return recoveryCandidate(header, slot), encoded
}

func recoveryCandidate(header protocol.Header, physical uint64) walCandidate {
	op := prepareOp(&header)
	operation := prepareOperation(&header)
	if operation == protocol.OperationReserved {
		if uint64(op) != physical {
			return walCandidate{Kind: walCandidateInvalid}
		}
		return walCandidate{Kind: walCandidateReserved, Op: op, HeaderChecksum: header.HeaderChecksum, BodyChecksum: header.BodyChecksum}
	}
	return walCandidate{Kind: walCandidateOrdinary, Op: op, View: header.View, HeaderChecksum: header.HeaderChecksum, BodyChecksum: header.BodyChecksum}
}

func (wal *walJournal) installRecoveredSlot(slot int, encoded [protocol.HeaderSize]byte, candidate walCandidate, faulty bool) {
	recovered := &wal.slots[slot]
	recovered.Authoritative = encoded
	recovered.Redundant = encoded
	recovered.PrepareChecksum = candidate.HeaderChecksum
	recovered.Op = candidate.Op
	recovered.Inhabited = true
	recovered.Dirty = faulty
	recovered.Faulty = faulty
}

func (wal *walJournal) installReservedSlot(slot int) error {
	reserved, err := reservedPrepareHeader(wal.group, protocol.Op(slot), wal.memberCount, uint32(wal.config.MessageSizeMax), wal.config.ChecksumAlgorithm)
	if err != nil {
		return err
	}
	recovered := &wal.slots[slot]
	recovered.Authoritative = reserved
	recovered.Redundant = reserved
	recovered.PrepareChecksum = protocol.Checksum{}
	recovered.Op = protocol.Op(slot)
	recovered.Inhabited = false
	recovered.Dirty = false
	recovered.Faulty = false
	return nil
}

func (wal *walJournal) prepareOffset(slot uint64) (uint64, bool) {
	offset, ok := checkedMul(slot, wal.layout.PrepareStride)
	if ok {
		offset, ok = checkedAdd(wal.layout.PrepareBase, offset)
	}
	return offset, ok
}

func (wal *walJournal) prepareMaximum(checkpoint protocol.Op) (protocol.Op, bool) {
	interval, ok := wal.config.CheckpointInterval()
	if !ok {
		return 0, false
	}
	first, ok := checkedAdd(uint64(checkpoint), interval)
	if checkpoint == 0 {
		first--
	}
	if !ok {
		return 0, false
	}
	trigger, ok := checkedAdd(first, wal.config.CompactionOps)
	if !ok {
		return 0, false
	}
	maximum, ok := checkedAdd(trigger, 2*wal.config.PipelineMax)
	return protocol.Op(maximum), ok
}

func maxCandidateOp(current protocol.Op, candidate walCandidate) protocol.Op {
	if candidate.Kind == walCandidateOrdinary && candidate.Op > current {
		return candidate.Op
	}
	return current
}
