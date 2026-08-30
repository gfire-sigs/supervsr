package replication

import (
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type scrubRuntime struct {
	next   uint64
	cursor uint64
}

func (replica *Replica) rememberBlock(requirement BlockRequirement) {
	if requirement.Reference == (BlockReference{}) || requirement.Type < protocol.BlockFreeSet || requirement.Type > protocol.BlockValue || len(replica.blockCatalog) == 0 {
		return
	}
	for index := range replica.blockCatalogCount {
		known := &replica.blockCatalog[index]
		if known.Reference.Address != requirement.Reference.Address {
			continue
		}
		if known.Reference != requirement.Reference {
			*known = requirement
			return
		}
		if known.Type != requirement.Type {
			replica.fail(ErrReplicaInvariant)
			return
		}
		if requirement.SnapshotExact {
			known.Snapshot = requirement.Snapshot
			known.SnapshotExact = true
		}
		if requirement.BodySize != 0 {
			known.BodySize = requirement.BodySize
		}
		return
	}
	if replica.blockCatalogCount < len(replica.blockCatalog) {
		replica.blockCatalog[replica.blockCatalogCount] = requirement
		replica.blockCatalogCount++
		return
	}
	if requirement.Reference.Address == 0 {
		return
	}
	index := int((requirement.Reference.Address - 1) % uint64(len(replica.blockCatalog)))
	replica.blockCatalog[index] = requirement
}

func (replica *Replica) knownBlock(address uint64) (BlockRequirement, bool) {
	for index := range replica.blockCatalogCount {
		requirement := replica.blockCatalog[index]
		if requirement.Reference.Address == address {
			return requirement, true
		}
	}
	return BlockRequirement{}, false
}

func (replica *Replica) clearBlockCatalog() {
	clear(replica.blockCatalog)
	replica.blockCatalogCount = 0
}

func (replica *Replica) pruneBlockCatalog() {
	count := 0
	for index := range replica.blockCatalogCount {
		requirement := replica.blockCatalog[index]
		blockIndex, ok := replica.blockAllocator.index(requirement.Reference.Address)
		if !ok || !replica.blockAllocator.acquired.Test(blockIndex) || replica.blockAllocator.released.Test(blockIndex) {
			continue
		}
		replica.blockCatalog[count] = requirement
		count++
	}
	clear(replica.blockCatalog[count:replica.blockCatalogCount])
	replica.blockCatalogCount = count
}

func (replica *Replica) seedScrubCatalog(checkpoint CheckpointState) {
	op := checkpoint.PrepareOp()
	replica.rememberBlock(BlockRequirement{
		Reference: BlockReference{Address: checkpoint.AcquiredTrailerLastAddress, Checksum: checkpoint.AcquiredTrailerLastChecksum},
		Type:      protocol.BlockFreeSet, Snapshot: uint64(op), SnapshotExact: true,
	})
	replica.rememberBlock(BlockRequirement{
		Reference: BlockReference{Address: checkpoint.ReleasedTrailerLastAddress, Checksum: checkpoint.ReleasedTrailerLastChecksum},
		Type:      protocol.BlockFreeSet, Snapshot: uint64(op), SnapshotExact: true,
	})
	replica.rememberBlock(BlockRequirement{
		Reference: BlockReference{Address: checkpoint.SessionTrailerLastAddress, Checksum: checkpoint.SessionTrailerLastChecksum},
		Type:      protocol.BlockClientSessions, Snapshot: uint64(op), SnapshotExact: true,
	})
	replica.rememberBlock(BlockRequirement{
		Reference: BlockReference{Address: checkpoint.NewestManifestAddress, Checksum: checkpoint.NewestManifestChecksum},
		Type:      protocol.BlockManifest,
	})
	if checkpoint.SnapshotRootAddress != 0 && replica.deps.BlockValidator != nil {
		requirement, err := replica.deps.BlockValidator.CheckpointRoot(checkpoint)
		if err == nil {
			replica.rememberBlock(requirement)
		}
	}
}

func (replica *Replica) continueScrub(now uint64) bool {
	if replica.stateSync.stage != SyncStageIdle || replica.stateSync.repairing || replica.blockAllocator == nil ||
		replica.checkpointTransitionActive() || now < replica.scrub.next {
		return false
	}
	delay := replica.scrubDelay()
	acquired := replica.blockAllocator.Acquired()
	released := replica.blockAllocator.Released()
	queued := false
	for range replica.config.Process.ScrubReadConcurrency {
		index, found := replica.nextScrubIndex(&acquired, &released)
		if !found || !replica.startScrubRead(replica.blockAllocator.address(index)) {
			break
		}
		queued = true
	}
	replica.scrub.next = now + uint64(delay)
	return queued
}

func (replica *Replica) startScrubRead(address uint64) bool {
	if replica.activeRepairReads() >= int(replica.config.Process.ScrubReadConcurrency) {
		return false
	}
	for index := range replica.blockRepairIO {
		operation := &replica.blockRepairIO[index]
		if operation.busy {
			continue
		}
		offset, ok := replica.config.Cluster.BlockOffset(address)
		if !ok {
			return false
		}
		handle, err := replica.io.Submit(IOOperation{Kind: IORead, Offset: offset, Buffer: operation.buffer, Size: replica.config.Cluster.BlockSize})
		if err != nil {
			return false
		}
		operation.handle = handle
		operation.address = address
		operation.stage = blockRepairIOScrub
		operation.busy = true
		return true
	}
	return false
}

func (replica *Replica) finishScrubRead(operation *blockRepairIO, completion IOCompletion) {
	address := operation.address
	operation.busy = false
	operation.address = 0
	if completion.Err != nil {
		return
	}
	requirement, known := replica.knownBlock(address)
	if !known && replica.deps.BlockValidator != nil {
		var err error
		requirement, known, err = replica.deps.BlockValidator.ResolveBlock(replica.checkpoint, address)
		if err != nil {
			replica.fail(err)
			return
		}
	}
	if !known || requirement.Reference.Address != address {
		replica.fail(ErrBlockMissing)
		return
	}
	frame, header, ok := decodeRepairFrame(operation.buffer, replica.config.Group, uint32(replica.config.Cluster.BlockSize), replica.membership.ActiveCount+replica.membership.StandbyCount)
	if !ok || !allZeroBytes(operation.buffer[len(frame):]) {
		replica.queueKnownCorruptBlock(address, requirement, true)
		return
	}
	target := blockRepairTarget{
		reference:        requirement.Reference,
		snapshot:         blockSnapshotExpectation{value: requirement.Snapshot, exact: requirement.SnapshotExact},
		expectedBodySize: requirement.BodySize, blockType: requirement.Type,
	}
	if !replica.validRequestedBlock(&target, header, frame[protocol.HeaderSize:]) {
		replica.queueKnownCorruptBlock(address, requirement, true)
		return
	}
	replica.rememberBlock(requirement)
}

func (replica *Replica) queueKnownCorruptBlock(address uint64, requirement BlockRequirement, known bool) {
	if !known && replica.deps.BlockValidator != nil {
		var err error
		requirement, known, err = replica.deps.BlockValidator.ResolveBlock(replica.checkpoint, address)
		if err != nil {
			replica.fail(err)
			return
		}
	}
	if !known || requirement.Reference.Address != address {
		replica.fail(ErrBlockMissing)
		return
	}
	replica.metrics.storageCorruptions.Add(1)
	snapshot := blockSnapshotExpectation{value: requirement.Snapshot, exact: requirement.SnapshotExact}
	if !replica.queueBlockRepair(requirement.Reference, requirement.Type, replica.checkpoint.PrepareOp(), snapshot, requirement.BodySize, blockRepairScrub) {
		replica.fail(ErrReplicaBackpressure)
	}
}

func (replica *Replica) nextScrubIndex(acquired, released *FixedBitSet) (uint64, bool) {
	length := acquired.Len()
	if length == 0 {
		return 0, false
	}
	for range length {
		index := replica.scrub.cursor % length
		replica.scrub.cursor = (index + 1) % length
		if acquired.Test(index) && !released.Test(index) {
			return index, true
		}
	}
	return 0, false
}

func (replica *Replica) scrubDelay() time.Duration {
	if replica.io.Available() == 0 || replica.blockRepairActive() {
		return replica.config.Process.ScrubIntervalMax
	}
	allocated := uint64(1)
	if replica.blockAllocator != nil {
		acquired := replica.blockAllocator.AcquiredCount()
		released := replica.blockAllocator.ReleasedCount()
		if acquired > released {
			allocated = acquired - released
		}
	}
	numerator, ok := checkedMul(uint64(replica.config.Process.ScrubCycle), uint64(replica.config.Process.ScrubReadConcurrency))
	if !ok {
		return replica.config.Process.ScrubIntervalMax
	}
	delay := numerator / allocated
	return time.Duration(min(max(delay, uint64(replica.config.Process.ScrubIntervalMin)), uint64(replica.config.Process.ScrubIntervalMax)))
}
