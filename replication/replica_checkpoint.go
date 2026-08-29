package replication

import (
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func checkpointAfter(cluster ClusterConfig, current protocol.Op) (protocol.Op, bool) {
	interval, ok := cluster.CheckpointInterval()
	if !ok || interval == 0 {
		return 0, false
	}
	if current == 0 {
		return protocol.Op(interval - 1), true
	}
	next, ok := checkedAdd(uint64(current), interval)
	return protocol.Op(next), ok
}

func checkpointTrigger(cluster ClusterConfig, checkpoint protocol.Op) (protocol.Op, bool) {
	trigger, ok := checkedAdd(uint64(checkpoint), cluster.CompactionOps)
	return protocol.Op(trigger), ok
}

func (replica *Replica) continueCommitMaintenance(entry *pipelineEntry) {
	op := prepareOp(&entry.header)
	entry.stage = CommitStageCheckpointDurable
	if (uint64(op)+1)%replica.config.Cluster.CompactionOps != 0 {
		replica.afterCompact(entry)
		return
	}
	entry.stage = CommitStageCompact
	entry.completion.prepare(entry.generation, replica)
	result, err := replica.deps.StateMachine.StartCompact(CompactInput{Op: op, Timestamp: prepareTimestamp(&entry.header)}, &entry.completion)
	if err != nil {
		replica.fail(errors.Join(ErrStateMachine, err))
		return
	}
	if compact, ready := result.Value(); ready {
		entry.completion.release(entry.generation)
		replica.applyCompact(entry, compact)
	}
}

func (replica *Replica) applyCompact(entry *pipelineEntry, _ CompactResult) {
	if entry.stage != CommitStageCompact {
		replica.fail(ErrReplicaInvariant)
		return
	}
	replica.afterCompact(entry)
}

func (replica *Replica) afterCompact(entry *pipelineEntry) {
	if replica.pendingView > replica.view {
		replica.yieldCheckpointToView(entry)
		return
	}
	op := prepareOp(&entry.header)
	target, ok := checkpointAfter(replica.config.Cluster, replica.checkpoint.PrepareOp())
	if !ok {
		replica.fail(ErrReplicaInvariant)
		return
	}
	if op == target {
		if _, err := replica.sessions.EncodeTrailer(replica.checkpointSession); err != nil {
			replica.fail(err)
			return
		}
		replica.checkpointSessionOp = target
	}
	trigger, ok := checkpointTrigger(replica.config.Cluster, target)
	if !ok {
		replica.fail(ErrReplicaInvariant)
		return
	}
	if op != trigger {
		replica.completeCommitEntry()
		return
	}
	if replica.checkpointSessionOp != target {
		replica.fail(ErrReplicaInvariant)
		return
	}
	replica.checkpointTarget = target
	replica.checkpointTargetRelease = replica.checkpointRelease(target)
	entry.stage = CommitStageCheckpointData
	entry.completion.prepare(entry.generation, replica)
	result, err := replica.deps.StateMachine.StartCheckpoint(CheckpointInput{Op: target, Timestamp: prepareTimestamp(&entry.header), Release: replica.checkpointTargetRelease}, &entry.completion)
	if err != nil {
		replica.fail(errors.Join(ErrStateMachine, err))
		return
	}
	if manifest, ready := result.Value(); ready {
		entry.completion.release(entry.generation)
		replica.applyCheckpoint(entry, manifest)
	}
}

func (replica *Replica) applyCheckpoint(entry *pipelineEntry, manifest CheckpointManifest) {
	if replica.pendingView > replica.view {
		replica.yieldCheckpointToView(entry)
		return
	}
	if entry.stage != CommitStageCheckpointData || replica.checkpointTarget == 0 {
		replica.fail(ErrReplicaInvariant)
		return
	}
	payload := replica.config.Cluster.BlockSize - protocol.HeaderSize
	candidate, err := replica.blockAllocator.PrepareCheckpoint(uint64(len(replica.checkpointSession)), payload)
	if err != nil {
		replica.fail(err)
		return
	}
	replica.checkpointCandidate = candidate
	snapshot := uint64(replica.checkpointTarget)
	acquired, err := replica.trailers.WriteReserved(replica.blockAllocator, candidate.AcquiredAddresses(), protocol.BlockFreeSet, snapshot, candidate.AcquiredEncoded())
	if err != nil {
		replica.abortCheckpointCandidate()
		replica.fail(err)
		return
	}
	released, err := replica.trailers.WriteReserved(replica.blockAllocator, candidate.ReleasedAddresses(), protocol.BlockFreeSet, snapshot, candidate.ReleasedEncoded())
	if err != nil {
		replica.abortCheckpointCandidate()
		replica.fail(err)
		return
	}
	sessions, err := replica.trailers.WriteReserved(replica.blockAllocator, candidate.SessionAddresses(), protocol.BlockClientSessions, snapshot, replica.checkpointSession)
	if err != nil {
		replica.abortCheckpointCandidate()
		replica.fail(err)
		return
	}
	checkpoint, err := replica.buildCheckpointState(candidate, acquired, released, sessions, manifest)
	if err != nil {
		replica.abortCheckpointCandidate()
		replica.fail(err)
		return
	}
	next := replica.superblocks.Current()
	next.ParentChecksum = next.Checksum
	next.Sequence++
	next.State.Checkpoint = checkpoint
	next.State.CommitMax = replica.commitMax
	handle, err := replica.io.Submit(IOOperation{Kind: IOSuperblockPersist, SuperblockStore: replica.superblocks, Superblock: next})
	if err != nil {
		replica.abortCheckpointCandidate()
		replica.fail(err)
		return
	}
	replica.pendingCheckpoint = checkpoint
	replica.checkpointManifest = manifest
	entry.stage = CommitStageCheckpointSuperblock
	entry.io = handle
	entry.ioKind = IOSuperblockPersist
}

func (replica *Replica) buildCheckpointState(candidate BlockCheckpointCandidate, acquired, released, sessions TrailerReference, manifest CheckpointManifest) (CheckpointState, error) {
	header, found := replica.wal.RecoveredHeader(replica.checkpointTarget)
	if !found {
		return CheckpointState{}, ErrWALRecovery
	}
	var encodedHeader [protocol.HeaderSize]byte
	if err := protocol.EncodeHeader(encodedHeader[:], &header); err != nil {
		return CheckpointState{}, err
	}
	if manifest.BlockCount == 0 {
		if manifest.Oldest != (BlockReference{}) || manifest.Newest != (BlockReference{}) {
			return CheckpointState{}, ErrInvalidCheckpoint
		}
	} else if !replica.checkpointReferenceValid(candidate, manifest.Oldest) || !replica.checkpointReferenceValid(candidate, manifest.Newest) {
		return CheckpointState{}, ErrInvalidCheckpoint
	}
	if manifest.Root != (BlockReference{}) && !replica.checkpointReferenceValid(candidate, manifest.Root) {
		return CheckpointState{}, ErrInvalidCheckpoint
	}
	logical, ok := checkedMul(candidate.blockCount, replica.config.Cluster.BlockSize)
	if ok {
		base, baseOK := replica.config.Cluster.BlockBase()
		logical, ok = checkedAdd(base, logical)
		ok = ok && baseOK
	}
	if !ok {
		return CheckpointState{}, ErrInvalidCheckpoint
	}
	state := CheckpointState{
		Header:                      encodedHeader,
		AcquiredTrailerLastChecksum: acquired.Last.Checksum, ReleasedTrailerLastChecksum: released.Last.Checksum,
		SessionTrailerLastChecksum: sessions.Last.Checksum, OldestManifestChecksum: manifest.Oldest.Checksum,
		NewestManifestChecksum: manifest.Newest.Checksum, SnapshotRootChecksum: manifest.Root.Checksum,
		AcquiredAggregateChecksum: acquired.Aggregate, ReleasedAggregateChecksum: released.Aggregate,
		SessionAggregateChecksum: sessions.Aggregate, ParentID: replica.checkpointID, GrandparentID: replica.checkpoint.ParentID,
		AcquiredTrailerLastAddress: acquired.Last.Address, ReleasedTrailerLastAddress: released.Last.Address,
		SessionTrailerLastAddress: sessions.Last.Address, OldestManifestAddress: manifest.Oldest.Address,
		NewestManifestAddress: manifest.Newest.Address, SnapshotRootAddress: manifest.Root.Address,
		LogicalStorageSize: logical, AcquiredTrailerEncodedSize: acquired.EncodedSize,
		ReleasedTrailerEncodedSize: released.EncodedSize, SessionTrailerEncodedSize: sessions.EncodedSize,
		ManifestBlockCount: manifest.BlockCount, Release: replica.checkpointTargetRelease,
	}
	validation := CheckpointValidation{
		Group: replica.config.Group, MessageSizeMax: uint32(replica.config.Cluster.MessageSizeMax),
		MemberCount: replica.membership.ActiveCount + replica.membership.StandbyCount,
		BlockSize:   replica.config.Cluster.BlockSize, ClientsMax: replica.config.Cluster.ClientsMax,
	}
	validation.BlockBase, _ = replica.config.Cluster.BlockBase()
	if err := state.Validate(validation); err != nil {
		return CheckpointState{}, err
	}
	return state, nil
}

func (replica *Replica) checkpointReferenceValid(candidate BlockCheckpointCandidate, reference BlockReference) bool {
	index, ok := replica.blockAllocator.index(reference.Address)
	return ok && !reference.Checksum.IsZero() && index < candidate.blockCount && candidate.reachable.Test(index)
}

func (replica *Replica) finishCheckpointPersistence(entry *pipelineEntry) {
	if entry.stage != CommitStageCheckpointSuperblock {
		replica.fail(ErrReplicaInvariant)
		return
	}
	reachable := replica.checkpointCandidate.Reachable()
	if err := replica.blockAllocator.CheckpointDurable(replica.checkpointCandidate, &reachable); err != nil {
		replica.fail(err)
		return
	}
	replica.checkpoint = replica.pendingCheckpoint
	targetRelease := replica.checkpoint.Release
	checkpointID, err := replica.checkpoint.ID()
	if err != nil {
		replica.fail(err)
		return
	}
	replica.checkpointID = checkpointID
	replica.checkpointSessionOp = 0
	replica.checkpointTarget = 0
	replica.checkpointTargetRelease = 0
	replica.checkpointCandidate = BlockCheckpointCandidate{}
	replica.pendingCheckpoint = CheckpointState{}
	replica.checkpointManifest = CheckpointManifest{}
	replica.upgradeWindow = upgradeWindow{}
	if targetRelease != replica.config.CurrentRelease {
		replica.beginReleaseActivation(targetRelease)
		return
	}
	if replica.pendingView > replica.view {
		target := replica.pendingView
		replica.pendingView = 0
		replica.beginViewChangeNow(target)
	}
	replica.completeCommitEntry()
}

func (replica *Replica) abortCheckpointCandidate() {
	if replica.checkpointCandidate.generation == 0 {
		return
	}
	_ = replica.blockAllocator.AbortCheckpoint(replica.checkpointCandidate)
	replica.checkpointCandidate = BlockCheckpointCandidate{}
}

func (replica *Replica) checkpointTransitionActive() bool {
	for offset := range replica.pipelineLen {
		stage := replica.pipelineEntry(offset).stage
		if stage >= CommitStageCompact && stage <= CommitStageCheckpointSuperblock {
			return true
		}
	}
	return false
}

func (replica *Replica) yieldCheckpointToView(entry *pipelineEntry) {
	target := replica.pendingView
	replica.pendingView = 0
	replica.abortCheckpointCandidate()
	replica.checkpointTarget = 0
	replica.checkpointTargetRelease = 0
	replica.pendingCheckpoint = CheckpointState{}
	replica.checkpointManifest = CheckpointManifest{}
	entry.stage = CommitStageIdle
	replica.beginViewChangeNow(target)
	replica.completeCommitEntry()
}

func (replica *Replica) completeCommitEntry() {
	replica.popPipeline()
	replica.stage = CommitStageIdle
	replica.dequeueRequest()
	replica.advanceCommit()
}
