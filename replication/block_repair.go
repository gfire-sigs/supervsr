package replication

import (
	"encoding/binary"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type BlockValidationInput struct {
	Reference          BlockReference
	NeededAtCheckpoint Op
	Snapshot           uint64
	Type               BlockType
	Metadata           [96]byte
	Body               []byte
}

type BlockRequirement struct {
	Reference     BlockReference
	Type          BlockType
	Snapshot      uint64
	SnapshotExact bool
	BodySize      uint32
}

type BlockValidator interface {
	CheckpointRoot(checkpoint CheckpointState) (BlockRequirement, error)
	ResolveBlock(checkpoint CheckpointState, address uint64) (BlockRequirement, bool, error)
	ValidateBlock(input BlockValidationInput, references []BlockRequirement) (int, error)
}

type blockRepairSource uint8

const (
	blockRepairStateSync blockRepairSource = 1 << iota
	blockRepairScrub
)

type blockRepairState uint8

const (
	blockRepairQueued blockRepairState = iota + 1
	blockRepairReading
	blockRepairMissing
	blockRepairWriting
	blockRepairDurable
)

type blockSnapshotExpectation struct {
	value uint64
	exact bool
}

type blockRepairTarget struct {
	reference          BlockReference
	neededAtCheckpoint protocol.Op
	snapshot           blockSnapshotExpectation
	expectedBodySize   uint32
	blockType          protocol.BlockType
	waiters            blockRepairSource
	state              blockRepairState
	chainBytes         uint64
	chainBlocks        uint32
}

type blockRepairIOStage uint8

const (
	blockRepairIORead blockRepairIOStage = iota + 1
	blockRepairIOWrite
	blockRepairIOSync
	blockRepairIOScrub
)

type blockRepairIO struct {
	handle  IOHandle
	buffer  []byte
	target  uint32
	address uint64
	stage   blockRepairIOStage
	busy    bool
}

func (replica *Replica) queueBlockRepair(reference BlockReference, blockType protocol.BlockType, neededAtCheckpoint protocol.Op, snapshot blockSnapshotExpectation, expectedBodySize uint32, source blockRepairSource) bool {
	if reference.Address == 0 || reference.Checksum.IsZero() || blockType < protocol.BlockFreeSet || blockType > protocol.BlockValue {
		return false
	}
	for index := range replica.blockRepairTargets {
		target := &replica.blockRepairTargets[index]
		if target.state == 0 || target.reference != reference {
			continue
		}
		if target.blockType != blockType {
			replica.fail(ErrReplicaInvariant)
			return false
		}
		if target.snapshot.exact && snapshot.exact && target.snapshot.value != snapshot.value {
			replica.fail(ErrReplicaInvariant)
			return false
		}
		if target.expectedBodySize != 0 && expectedBodySize != 0 && target.expectedBodySize != expectedBodySize {
			replica.fail(ErrReplicaInvariant)
			return false
		}
		target.neededAtCheckpoint = max(target.neededAtCheckpoint, neededAtCheckpoint)
		if snapshot.exact {
			target.snapshot = snapshot
		}
		if expectedBodySize != 0 {
			target.expectedBodySize = expectedBodySize
		}
		target.waiters |= source
		return true
	}
	for index := range replica.blockRepairTargets {
		target := &replica.blockRepairTargets[index]
		if target.state != 0 {
			continue
		}
		*target = blockRepairTarget{
			reference: reference, neededAtCheckpoint: neededAtCheckpoint, snapshot: snapshot,
			expectedBodySize: expectedBodySize, blockType: blockType, waiters: source, state: blockRepairQueued,
		}
		return true
	}
	return false
}

func (replica *Replica) continueBlockRepair(now uint64) bool {
	replica.blockRepairBudget.Expire(now)
	for index := range replica.blockRepairTargets {
		target := &replica.blockRepairTargets[index]
		if target.state != blockRepairQueued {
			continue
		}
		if replica.startBlockRepairRead(uint32(index)) {
			return true
		}
		break
	}
	var references [64]BlockReference
	limit := min(int(replica.config.Process.RepairRequestsMax), len(references))
	count := 0
	neededAtCheckpoint := protocol.Op(0)
	for index := range replica.blockRepairTargets {
		target := &replica.blockRepairTargets[index]
		if target.state != blockRepairMissing || replica.blockRepairBudget.Outstanding(target.reference) {
			continue
		}
		references[count] = target.reference
		neededAtCheckpoint = max(neededAtCheckpoint, target.neededAtCheckpoint)
		count++
		if count == limit {
			break
		}
	}
	if count == 0 {
		return false
	}
	peer, ok := replica.blockRepairDestination(neededAtCheckpoint)
	if !ok {
		return false
	}
	batch := references[:count]
	if !replica.blockRepairBudget.Reserve(peer, batch, now) {
		return false
	}
	replica.sendGetBlocks(peer, batch)
	return true
}

func (replica *Replica) blockRepairDestination(snapshot protocol.Op) (protocol.ReplicaIndex, bool) {
	memberCount := replica.membership.ActiveCount + replica.membership.StandbyCount
	prioritized := false
	for peer := range memberCount {
		if protocol.ReplicaIndex(peer) == replica.local {
			continue
		}
		if replica.peerCheckpointOps[peer] >= snapshot && replica.blockRepairBudget.CanSend(protocol.ReplicaIndex(peer)) {
			prioritized = true
			break
		}
	}
	start := uint8(replica.random.Uniform(uint64(memberCount)))
	for offset := range memberCount {
		peer := (start + offset) % memberCount
		if protocol.ReplicaIndex(peer) == replica.local || !replica.blockRepairBudget.CanSend(protocol.ReplicaIndex(peer)) {
			continue
		}
		if prioritized && replica.peerCheckpointOps[peer] < snapshot {
			continue
		}
		return protocol.ReplicaIndex(peer), true
	}
	return 0, false
}

func (replica *Replica) blockRepairActive() bool {
	for index := range replica.blockRepairTargets {
		state := replica.blockRepairTargets[index].state
		if state != 0 && state != blockRepairDurable {
			return true
		}
	}

	return false
}
func (replica *Replica) observePeerCheckpoint(peer protocol.ReplicaIndex, checkpoint protocol.Op) {
	if uint8(peer) >= uint8(len(replica.peerCheckpointOps)) {
		return
	}
	replica.peerCheckpointOps[peer] = max(replica.peerCheckpointOps[peer], checkpoint)
}

func (replica *Replica) startBlockRepairRead(targetIndex uint32) bool {
	if replica.activeRepairReads() >= int(replica.config.Process.RepairReadsMax) {
		return false
	}
	for index := range replica.blockRepairIO {
		operation := &replica.blockRepairIO[index]
		if operation.busy {
			continue
		}
		target := &replica.blockRepairTargets[targetIndex]
		offset, ok := replica.config.Cluster.BlockOffset(target.reference.Address)
		if !ok {
			return false
		}
		handle, err := replica.io.Submit(IOOperation{Kind: IORead, Offset: offset, Buffer: operation.buffer, Size: replica.config.Cluster.BlockSize})
		if err != nil {
			return false
		}
		operation.handle = handle
		operation.target = targetIndex
		operation.stage = blockRepairIORead
		operation.busy = true
		target.state = blockRepairReading
		return true
	}
	return false
}

func (replica *Replica) activeRepairReads() int {
	count := 0
	for index := range replica.repairReads {
		if replica.repairReads[index].busy {
			count++
		}
	}
	for index := range replica.blockRepairIO {
		operation := &replica.blockRepairIO[index]
		if operation.busy && (operation.stage == blockRepairIORead || operation.stage == blockRepairIOScrub) {
			count++
		}
	}
	return count
}

func (replica *Replica) sendGetBlocks(peer protocol.ReplicaIndex, references []BlockReference) {
	message, err := replica.frames.Acquire(uint32(len(references) * 32))
	if err != nil {
		return
	}
	body, err := message.Body()
	if err != nil {
		message.Release()
		return
	}
	for index, reference := range references {
		offset := index * 32
		copy(body[offset:offset+16], reference.Checksum[:])
		binary.LittleEndian.PutUint64(body[offset+16:offset+24], reference.Address)
	}
	header := protocol.Header{Group: replica.config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetBlocks, Author: replica.local}
	if message.Seal(&header) == nil {
		replica.deps.MessageBus.SendReplica(peer, message)
	}
	message.Release()
}

func (replica *Replica) handleBlock(header protocol.Header, body []byte) {
	reference := BlockReference{Checksum: header.HeaderChecksum, Address: binary.LittleEndian.Uint64(header.Fields[96:104])}
	if !replica.blockRepairBudget.Outstanding(reference) {
		return
	}
	targetIndex, found := replica.blockRepairTarget(reference)
	if !found || replica.blockRepairTargets[targetIndex].state != blockRepairMissing || !replica.validRequestedBlock(&replica.blockRepairTargets[targetIndex], header, body) {
		return
	}
	if replica.activeBlockRepairWrites() >= int(replica.config.Process.ScrubWriteConcurrency) {
		return
	}
	for index := range replica.blockRepairIO {
		operation := &replica.blockRepairIO[index]
		if operation.busy {
			continue
		}
		clear(operation.buffer)
		if err := protocol.EncodeHeader(operation.buffer[:protocol.HeaderSize], &header); err != nil {
			return
		}
		copy(operation.buffer[protocol.HeaderSize:], body)
		offset, ok := replica.config.Cluster.BlockOffset(reference.Address)
		if !ok {
			return
		}
		handle, err := replica.io.Submit(IOOperation{Kind: IOWrite, Offset: offset, Buffer: operation.buffer})
		if err != nil {
			return
		}
		operation.handle = handle
		operation.target = targetIndex
		operation.stage = blockRepairIOWrite
		operation.busy = true
		replica.blockRepairTargets[targetIndex].state = blockRepairWriting
		return
	}
}

func (replica *Replica) activeBlockRepairWrites() int {
	count := 0
	for index := range replica.blockRepairIO {
		operation := &replica.blockRepairIO[index]
		if operation.busy && (operation.stage == blockRepairIOWrite || operation.stage == blockRepairIOSync) {
			count++
		}
	}
	return count
}

func (replica *Replica) blockRepairTarget(reference BlockReference) (uint32, bool) {
	for index := range replica.blockRepairTargets {
		target := &replica.blockRepairTargets[index]
		if target.state != 0 && target.reference == reference {
			return uint32(index), true
		}
	}
	return 0, false
}

func (replica *Replica) validRequestedBlock(target *blockRepairTarget, header protocol.Header, body []byte) bool {
	if header.Command != protocol.CommandBlock || header.HeaderChecksum != target.reference.Checksum {
		return false
	}
	if binary.LittleEndian.Uint64(header.Fields[96:104]) != target.reference.Address || protocol.BlockType(header.Fields[112]) != target.blockType {
		return false
	}
	if header.Release == 0 || header.Release > replica.config.CurrentRelease || !allZeroBytes(header.Fields[113:]) {
		return false
	}
	actualSnapshot := binary.LittleEndian.Uint64(header.Fields[104:112])
	if target.snapshot.exact && actualSnapshot != target.snapshot.value {
		return false
	}
	if target.expectedBodySize != 0 && uint32(len(body)) != target.expectedBodySize {
		return false
	}
	var metadata [96]byte
	copy(metadata[:], header.Fields[:96])
	if ValidateBlockMetadata(target.blockType, metadata, body) != nil {
		return false
	}
	if target.blockType < protocol.BlockManifest {
		return true
	}
	validator := replica.deps.BlockValidator
	if validator == nil {
		return false
	}
	input := BlockValidationInput{Reference: target.reference, NeededAtCheckpoint: target.neededAtCheckpoint, Snapshot: actualSnapshot, Type: target.blockType, Metadata: metadata, Body: body}
	count, err := validator.ValidateBlock(input, replica.blockRequirements)
	return err == nil && count >= 0 && count <= len(replica.blockRequirements)
}

func (replica *Replica) handleBlockRepairIO(completion IOCompletion) bool {
	for index := range replica.blockRepairIO {
		operation := &replica.blockRepairIO[index]
		if !operation.busy || operation.handle != completion.Handle {
			continue
		}
		if operation.stage == blockRepairIOScrub {
			replica.finishScrubRead(operation, completion)
			return true
		}
		target := &replica.blockRepairTargets[operation.target]
		switch operation.stage {
		case blockRepairIORead:
			replica.finishBlockRepairRead(operation, target, completion)
		case blockRepairIOWrite:
			replica.finishBlockRepairWrite(operation, target, completion)
		case blockRepairIOSync:
			replica.finishBlockRepairSync(operation, target, completion)
		default:
			replica.fail(ErrReplicaInvariant)
		}
		return true
	}
	return false
}

func (replica *Replica) finishBlockRepairRead(operation *blockRepairIO, target *blockRepairTarget, completion IOCompletion) {
	operation.busy = false
	if completion.Err != nil {
		target.state = blockRepairMissing
		return
	}
	frame, header, ok := decodeRepairFrame(operation.buffer, replica.config.Group, uint32(replica.config.Cluster.BlockSize), replica.membership.ActiveCount+replica.membership.StandbyCount)
	if !ok || !replica.validRequestedBlock(target, header, frame[protocol.HeaderSize:]) || !allZeroBytes(operation.buffer[len(frame):]) {
		target.state = blockRepairMissing
		return
	}
	target.state = blockRepairDurable
	replica.blockRepairBudget.Fulfill(target.reference)
	replica.blockRepairCompleted(target, operation.buffer)

}

func (replica *Replica) finishBlockRepairWrite(operation *blockRepairIO, target *blockRepairTarget, completion IOCompletion) {
	if completion.Err != nil {
		operation.busy = false
		target.state = blockRepairMissing
		return
	}
	handle, err := replica.io.Submit(IOOperation{Kind: IOSync})
	if err != nil {
		operation.busy = false
		target.state = blockRepairMissing
		return
	}
	operation.handle = handle
	operation.stage = blockRepairIOSync
}

func (replica *Replica) finishBlockRepairSync(operation *blockRepairIO, target *blockRepairTarget, completion IOCompletion) {
	operation.busy = false
	if completion.Err != nil {
		target.state = blockRepairMissing
		return
	}
	target.state = blockRepairDurable
	replica.blockRepairBudget.Fulfill(target.reference)
	replica.blockRepairCompleted(target, operation.buffer)
}

func (replica *Replica) blockRepairCompleted(target *blockRepairTarget, physical []byte) {
	completed := *target
	*target = blockRepairTarget{}
	frame, header, ok := decodeRepairFrame(physical, replica.config.Group, uint32(replica.config.Cluster.BlockSize), replica.membership.ActiveCount+replica.membership.StandbyCount)
	if !ok {
		replica.fail(ErrInvalidBlock)
		return
	}
	body := frame[protocol.HeaderSize:]
	requirement := BlockRequirement{
		Reference: completed.reference, Type: completed.blockType,
		Snapshot: binary.LittleEndian.Uint64(header.Fields[104:112]), SnapshotExact: completed.snapshot.exact,
		BodySize: uint32(len(body)),
	}
	replica.rememberBlock(requirement)
	if completed.blockType == protocol.BlockFreeSet || completed.blockType == protocol.BlockClientSessions || completed.blockType == protocol.BlockManifest {
		var previous BlockRequirement
		copy(previous.Reference.Checksum[:], header.Fields[:16])
		previous.Reference.Address = binary.LittleEndian.Uint64(header.Fields[32:40])
		if previous.Reference != (BlockReference{}) {
			previous.Type = completed.blockType
			previous.Snapshot = requirement.Snapshot
			previous.SnapshotExact = completed.snapshot.exact
			replica.rememberBlock(previous)
		}
	}
	if completed.blockType >= protocol.BlockManifest && replica.deps.BlockValidator != nil {
		var metadata [96]byte
		copy(metadata[:], header.Fields[:96])
		input := BlockValidationInput{
			Reference: completed.reference, NeededAtCheckpoint: completed.neededAtCheckpoint,
			Snapshot: requirement.Snapshot, Type: completed.blockType, Metadata: metadata, Body: body,
		}
		count, err := replica.deps.BlockValidator.ValidateBlock(input, replica.blockRequirements)
		if err != nil || count < 0 || count > len(replica.blockRequirements) {
			replica.fail(ErrInvalidBlock)
			return
		}
		for index := range count {
			replica.rememberBlock(replica.blockRequirements[index])
		}
	}
	if completed.waiters&blockRepairStateSync != 0 {
		replica.continueStateSyncBlocks(&completed, physical)
	}
}
