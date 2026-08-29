package replication

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var ErrWALRecovery = errors.New("replication: WAL recovery failed")

type WALRecoveryReport struct {
	HeadOp       protocol.Op
	HeadHeader   protocol.Header
	FaultySlots  uint32
	UntrustedMax protocol.Op
}

func (wal *WAL) Recover(checkpoint CheckpointState, commitMax protocol.Op, process ProcessConfig) (WALRecoveryReport, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if err := wal.storage.ReadAt(wal.headerRing, wal.layout.HeaderBase); err != nil {
		return WALRecoveryReport{}, errors.Join(ErrWALRecovery, err)
	}
	count := len(wal.slots)
	headerCandidates := make([]WALCandidate, count)
	prepareCandidates := make([]WALCandidate, count)
	prepareHeaders := make([][protocol.HeaderSize]byte, count)
	untrustedMax := checkpoint.PrepareOp()
	for slot := range count {
		headerBytes := wal.headerRing[slot*protocol.HeaderSize : (slot+1)*protocol.HeaderSize]
		headerCandidates[slot] = wal.decodeRecoveryHeader(headerBytes, uint64(slot))
		untrustedMax = maxCandidateOp(untrustedMax, headerCandidates[slot])
		clear(wal.prepareBuffer)
		offset, ok := wal.prepareOffset(uint64(slot))
		if !ok {
			return WALRecoveryReport{}, ErrWALRecovery
		}
		if err := wal.storage.ReadAt(wal.prepareBuffer, offset); err != nil {
			return WALRecoveryReport{}, errors.Join(ErrWALRecovery, err)
		}
		candidate, encoded := wal.decodeRecoveryPrepare(wal.prepareBuffer, uint64(slot))
		prepareCandidates[slot] = candidate
		prepareHeaders[slot] = encoded
		untrustedMax = maxCandidateOp(untrustedMax, candidate)
	}
	prepareMax, ok := wal.prepareMaximum(checkpoint.PrepareOp())
	if !ok {
		return WALRecoveryReport{}, ErrWALRecovery
	}
	tornWidth := protocol.Op(min(uint64(process.JournalWriteConcurrency), wal.config.JournalSlots))
	if wal.memberCount == 1 {
		tornWidth = 1
	}
	tornMin := protocol.Op(0)
	if untrustedMax >= tornWidth {
		tornMin = untrustedMax - tornWidth + 1
	}
	repairHeaders := false
	faulty := uint32(0)
	firstFaulty := -1
	for slot := range count {
		context := WALRecoveryContext{
			PhysicalSlot: uint64(slot),
			JournalSlots: wal.config.JournalSlots,
			RetainedMin:  checkpoint.PrepareOp(),
			PrepareMax:   prepareMax,
			UntrustedMax: untrustedMax,
			TornMin:      tornMin,
		}
		decision := ClassifyWALSlot(headerCandidates[slot], prepareCandidates[slot], context)
		switch decision {
		case WALRecoveryClean:
			wal.installRecoveredSlot(slot, prepareHeaders[slot], prepareCandidates[slot], false)
		case WALRecoveryCleanEmpty:
			wal.installReservedSlot(slot)
		case WALRecoveryLocalRepair:
			wal.installRecoveredSlot(slot, prepareHeaders[slot], prepareCandidates[slot], false)
			copy(wal.headerRing[slot*protocol.HeaderSize:(slot+1)*protocol.HeaderSize], prepareHeaders[slot][:])
			repairHeaders = true
		case WALRecoveryTruncate:
			wal.installReservedSlot(slot)
			copy(wal.headerRing[slot*protocol.HeaderSize:(slot+1)*protocol.HeaderSize], wal.slots[slot].Authoritative[:])
			repairHeaders = true
		case WALRecoveryRemoteRepair:
			wal.installReservedSlot(slot)
			wal.slots[slot].Dirty = true
			wal.slots[slot].Faulty = true
			faulty++
			if firstFaulty == -1 {
				firstFaulty = slot
			}
		case WALRecoveryFail:
			return WALRecoveryReport{}, fmt.Errorf("%w: impossible physical slot %d", ErrWALRecovery, slot)
		default:
			return WALRecoveryReport{}, ErrWALRecovery
		}
	}
	if faulty != 0 && wal.memberCount == 1 {
		return WALRecoveryReport{}, fmt.Errorf("%w: %d faulty slots, first physical slot %d header=%+v prepare=%+v", ErrWALUncertainSolo, faulty, firstFaulty, headerCandidates[firstFaulty], prepareCandidates[firstFaulty])
	}
	if repairHeaders {
		if err := wal.storage.WriteAt(wal.headerRing, wal.layout.HeaderBase); err != nil {
			return WALRecoveryReport{}, errors.Join(ErrWALRecovery, err)
		}
		if err := wal.storage.Sync(); err != nil {
			return WALRecoveryReport{}, errors.Join(ErrWALRecovery, err)
		}
	}
	head, reason := protocol.DecodeHeader(checkpoint.Header[:], wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount)
	if reason != protocol.RejectNone || head.Command != protocol.CommandPrepare {
		return WALRecoveryReport{}, ErrWALRecovery
	}
	headOp := checkpoint.PrepareOp()
	for next := headOp + 1; next <= untrustedMax; next++ {
		slot := uint64(next) % wal.config.JournalSlots
		recovered := &wal.slots[slot]
		if !recovered.Inhabited || recovered.Op != next {
			break
		}
		candidate, reason := protocol.DecodeHeader(recovered.Authoritative[:], wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount)
		if reason != protocol.RejectNone || prepareParent(&candidate) != head.HeaderChecksum {
			break
		}
		head = candidate
		headOp = next
	}
	if commitMax > headOp && faulty == 0 {
		return WALRecoveryReport{}, ErrWALRecovery
	}
	return WALRecoveryReport{HeadOp: headOp, HeadHeader: head, FaultySlots: faulty, UntrustedMax: untrustedMax}, nil
}

func (wal *WAL) ReadPrepare(op protocol.Op, destination []byte) ([]byte, error) {
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
	header, _, reason := protocol.DecodeFrame(frame, wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount)
	if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare || prepareOp(&header) != op {
		return nil, ErrInvalidWAL
	}
	return frame, nil
}
func (wal *WAL) RecoveredHeader(op protocol.Op) (protocol.Header, bool) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	slot := &wal.slots[uint64(op)%wal.config.JournalSlots]
	if !slot.Inhabited || slot.Op != op || slot.Faulty {
		return protocol.Header{}, false
	}
	header, reason := protocol.DecodeHeader(slot.Authoritative[:], wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount)
	return header, reason == protocol.RejectNone
}

func (wal *WAL) JoinEvidence(op protocol.Op) (protocol.Header, bool, bool) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	slot := &wal.slots[uint64(op)%wal.config.JournalSlots]
	if slot.Faulty {
		return protocol.Header{}, false, false
	}
	if !slot.Inhabited || slot.Op != op {
		return protocol.Header{}, false, true
	}
	header, reason := protocol.DecodeHeader(slot.Authoritative[:], wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount)
	if reason != protocol.RejectNone {
		return protocol.Header{}, false, false
	}
	return header, true, slot.Dirty
}

func (wal *WAL) decodeRecoveryHeader(encoded []byte, physical uint64) WALCandidate {
	header, reason := protocol.DecodeHeader(encoded, wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount)
	if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare {
		return WALCandidate{Kind: WALCandidateInvalid}
	}
	return recoveryCandidate(header, physical)
}

func (wal *WAL) decodeRecoveryPrepare(physical []byte, slot uint64) (WALCandidate, [protocol.HeaderSize]byte) {
	var encoded [protocol.HeaderSize]byte
	if allZeroBytes(physical) {
		reserved, err := ReservedPrepareHeader(wal.group, protocol.Op(slot), wal.memberCount, uint32(wal.config.MessageSizeMax))
		if err != nil {
			return WALCandidate{Kind: WALCandidateInvalid}, encoded
		}
		return WALCandidate{Kind: WALCandidateReserved, Op: protocol.Op(slot)}, reserved
	}
	if len(physical) < protocol.HeaderSize {
		return WALCandidate{Kind: WALCandidateInvalid}, encoded
	}
	size := binary.LittleEndian.Uint32(physical[96:100])
	if size < protocol.HeaderSize || uint64(size) > wal.layout.PrepareStride || !allZeroBytes(physical[size:]) {
		return WALCandidate{Kind: WALCandidateInvalid}, encoded
	}
	header, _, reason := protocol.DecodeFrame(physical[:size], wal.group, uint32(wal.config.MessageSizeMax), wal.memberCount)
	if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare {
		return WALCandidate{Kind: WALCandidateInvalid}, encoded
	}
	copy(encoded[:], physical[:protocol.HeaderSize])
	return recoveryCandidate(header, slot), encoded
}

func recoveryCandidate(header protocol.Header, physical uint64) WALCandidate {
	op := prepareOp(&header)
	operation := prepareOperation(&header)
	if operation == protocol.OperationReserved {
		if uint64(op) != physical {
			return WALCandidate{Kind: WALCandidateInvalid}
		}
		return WALCandidate{Kind: WALCandidateReserved, Op: op, HeaderChecksum: header.HeaderChecksum, BodyChecksum: header.BodyChecksum}
	}
	return WALCandidate{Kind: WALCandidateOrdinary, Op: op, View: header.View, HeaderChecksum: header.HeaderChecksum, BodyChecksum: header.BodyChecksum}
}

func (wal *WAL) installRecoveredSlot(slot int, encoded [protocol.HeaderSize]byte, candidate WALCandidate, faulty bool) {
	recovered := &wal.slots[slot]
	recovered.Authoritative = encoded
	recovered.Redundant = encoded
	recovered.PrepareChecksum = candidate.HeaderChecksum
	recovered.Op = candidate.Op
	recovered.Inhabited = true
	recovered.Dirty = faulty
	recovered.Faulty = faulty
}

func (wal *WAL) installReservedSlot(slot int) {
	reserved, err := ReservedPrepareHeader(wal.group, protocol.Op(slot), wal.memberCount, uint32(wal.config.MessageSizeMax))
	if err != nil {
		panic(err)
	}
	recovered := &wal.slots[slot]
	recovered.Authoritative = reserved
	recovered.Redundant = reserved
	recovered.PrepareChecksum = protocol.Checksum{}
	recovered.Op = protocol.Op(slot)
	recovered.Inhabited = false
	recovered.Dirty = false
	recovered.Faulty = false
}

func (wal *WAL) prepareOffset(slot uint64) (uint64, bool) {
	offset, ok := checkedMul(slot, wal.layout.PrepareStride)
	if ok {
		offset, ok = checkedAdd(wal.layout.PrepareBase, offset)
	}
	return offset, ok
}

func (wal *WAL) prepareMaximum(checkpoint protocol.Op) (protocol.Op, bool) {
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

func maxCandidateOp(current protocol.Op, candidate WALCandidate) protocol.Op {
	if candidate.Kind == WALCandidateOrdinary && candidate.Op > current {
		return candidate.Op
	}
	return current
}
