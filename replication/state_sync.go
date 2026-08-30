package replication

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type SyncStage uint8

const (
	SyncStageIdle SyncStage = iota
	SyncStageCancelingCommit
	SyncStageCancelingGrid
	SyncStageUpdatingCheckpoint
	SyncStageResizingStorage
)

type stateSyncRuntime struct {
	checkpoint     CheckpointState
	headers        []protocol.Header
	completion     SMCompletion
	io             IOHandle
	ioKind         IOKind
	view           protocol.View
	commit         protocol.Op
	head           protocol.Op
	generation     uint64
	count          int
	persistPhase   uint8
	completionKind SMCompletionKind
	stage          SyncStage
	cancelStarted  bool
	resetStarted   bool
	resetDone      bool
	persistDone    bool
	repairing      bool
	resumeRecovery bool
	recovery       WALRecoveryReport
	replayRunning  bool
	recoveryState  Superblock
	opening        bool
}

func (replica *Replica) beginStateSync(checkpoint CheckpointState, view protocol.View, commit, head protocol.Op, headers []protocol.Header) {
	if replica.stateSync.stage != SyncStageIdle || replica.stateSync.repairing || checkpoint.PrepareOp() <= replica.checkpoint.PrepareOp() {
		return
	}
	if len(headers) == 0 || len(headers) > len(replica.stateSync.headers) || head < checkpoint.PrepareOp() || commit < checkpoint.PrepareOp() || head < commit {
		replica.fail(ErrReplicaInvariant)
		return
	}
	clear(replica.stateSync.headers)
	copy(replica.stateSync.headers, headers)
	replica.stateSync.checkpoint = checkpoint
	replica.stateSync.view = view
	replica.stateSync.commit = commit
	replica.stateSync.head = head
	replica.stateSync.count = len(headers)
	replica.stateSync.persistPhase = 0
	replica.stateSync.cancelStarted = false
	replica.stateSync.resetStarted = false
	replica.stateSync.resetDone = false
	replica.stateSync.persistDone = false
	replica.status = StatusRecovering
	replica.releaseQueuedRequests()
	if replica.stage == CommitStageIdle {
		replica.stateSync.stage = SyncStageCancelingGrid
	} else {
		replica.stateSync.stage = SyncStageCancelingCommit
	}
}

func (replica *Replica) dispatchStateSync() {
	switch replica.stateSync.stage {
	case SyncStageIdle:
		return
	case SyncStageCancelingCommit:
		if replica.stage == CommitStageIdle {
			replica.stateSync.stage = SyncStageCancelingGrid
		}
	case SyncStageCancelingGrid:
		replica.dispatchStateSyncCancelingGrid()
	case SyncStageUpdatingCheckpoint:
		replica.dispatchStateSyncCheckpoint()
	case SyncStageResizingStorage:
		replica.dispatchStateSyncResize()
	default:
		replica.fail(ErrReplicaInvariant)
	}
}

func (replica *Replica) dispatchStateSyncCancelingGrid() {
	if !replica.stateSync.cancelStarted {
		replica.cancelStateSyncReads()
		replica.stateSync.cancelStarted = true
		return
	}
	if !replica.io.Drained() {
		return
	}
	replica.stateSync.stage = SyncStageUpdatingCheckpoint
}

func (replica *Replica) cancelStateSyncReads() {
	for index := range replica.duplicateReads {
		read := &replica.duplicateReads[index]
		if read.busy {
			replica.io.Cancel(read.handle)
		}
	}
	for index := range replica.repairReads {
		read := &replica.repairReads[index]
		if read.busy {
			replica.io.Cancel(read.handle)
		}
	}
	for index := range replica.blockRepairIO {
		operation := &replica.blockRepairIO[index]
		if operation.busy && (operation.stage == blockRepairIORead || operation.stage == blockRepairIOScrub) {
			replica.io.Cancel(operation.handle)
		}
	}
}

func (replica *Replica) handleStateSyncDrainIO(completion IOCompletion) {
	if replica.viewIO == completion.Handle {
		replica.viewIO = IOHandle{}
		if completion.Err != nil {
			replica.fail(completion.Err)
		}
		return
	}
	for index := range replica.duplicateReads {
		read := &replica.duplicateReads[index]
		if !read.busy || read.handle != completion.Handle {
			continue
		}
		buffer := read.buffer
		*read = duplicateRead{buffer: buffer}
		return
	}
	for index := range replica.repairReads {
		read := &replica.repairReads[index]
		if !read.busy || read.handle != completion.Handle {
			continue
		}
		buffer := read.buffer
		*read = repairRead{buffer: buffer}
		return
	}
	if replica.repairWrite.busy && replica.repairWrite.handle == completion.Handle {
		if replica.repairWrite.message != nil {
			replica.repairWrite.message.Release()
		}
		replica.repairWrite = repairWrite{}
		if completion.Err != nil && !errors.Is(completion.Err, ErrIOCanceled) {
			replica.fail(completion.Err)
		}
		return
	}
	for index := range replica.blockRepairIO {
		operation := &replica.blockRepairIO[index]
		if !operation.busy || operation.handle != completion.Handle {
			continue
		}
		operation.busy = false
		if operation.stage != blockRepairIORead && operation.stage != blockRepairIOScrub && completion.Err != nil {
			replica.fail(completion.Err)
		}
		return
	}
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if entry.io != completion.Handle {
			continue
		}
		kind := entry.ioKind
		entry.io = IOHandle{}
		entry.ioKind = 0
		if kind == IOReplyWrite {
			replica.sessions.Abort(entry.replyPlan)
		}
		if completion.Err != nil && !errors.Is(completion.Err, ErrIOCanceled) {
			replica.fail(completion.Err)
		}
		return
	}
	replica.metrics.staleCompletions.Add(1)
}

func (replica *Replica) handleStateSyncDrainSM(event replicaEvent) {
	result, ok := event.completion.take(event.generation)
	if !ok {
		replica.metrics.staleCompletions.Add(1)
		return
	}
	if replica.handleStateSyncCompletion(event.completion, event.generation, result) {
		return
	}
	replica.metrics.staleCompletions.Add(1)
}

func (replica *Replica) dispatchStateSyncCheckpoint() {
	if replica.stateSync.io != (IOHandle{}) {
		return
	}
	switch replica.stateSync.persistPhase {
	case 0:
		next := replica.superblocks.Current()
		next.ParentChecksum = next.Checksum
		next.Sequence++
		next.State.SyncMin = replica.checkpoint.PrepareOp() + 1
		next.State.SyncMax = replica.stateSync.checkpoint.PrepareOp()
		replica.startStateSyncPersistence(next, 1)
	case 1:
		if !replica.stateSync.persistDone || !replica.startStateSyncReset() {
			return
		}
		replica.releaseOwnedFrames()
		replica.blockRepairBudget.Reset()
		clear(replica.blockRepairTargets)
		replica.stateSync.persistDone = false
		next, err := replica.stateSyncSuperblock()
		if err != nil {
			replica.fail(err)
			return
		}
		replica.startStateSyncPersistence(next, 2)
	case 2:
		if !replica.stateSync.persistDone {
			return
		}
		replica.installStateSyncCheckpoint()
	default:
		replica.fail(ErrReplicaInvariant)
	}
}
func (replica *Replica) dispatchStateSyncResize() {
	if replica.stateSync.io != (IOHandle{}) {
		return
	}
	switch replica.stateSync.persistPhase {
	case 0:
		replica.startStateSyncIO(IOResize, replica.stateSync.checkpoint.LogicalStorageSize, 1)
	case 1:
		if !replica.stateSync.persistDone {
			return
		}
		replica.stateSync.persistDone = false
		replica.startStateSyncIO(IOSync, 0, 2)
	case 2:
		if !replica.stateSync.persistDone {
			return
		}
		replica.stateSync.persistDone = false
		replica.stateSync.stage = SyncStageIdle
		if !replica.queueStateSyncRoots() {
			replica.fail(ErrReplicaBackpressure)
			return
		}
		if !replica.blockRepairActive() {
			replica.finishStateSyncBlocks()
		}
	default:
		replica.fail(ErrReplicaInvariant)
	}
}

func (replica *Replica) startStateSyncIO(kind IOKind, size uint64, phase uint8) {
	handle, err := replica.io.Submit(IOOperation{Kind: kind, Size: size})
	if err != nil {
		if !errors.Is(err, ErrIOBackpressure) {
			replica.fail(err)
		}
		return
	}
	replica.stateSync.io = handle
	replica.stateSync.ioKind = kind
	replica.stateSync.persistPhase = phase
}

func (replica *Replica) startStateSyncReset() bool {
	if replica.stateSync.resetStarted {
		return replica.stateSync.resetDone
	}
	replica.stateSync.generation++
	replica.stateSync.completion.prepare(replica.stateSync.generation, replica)
	result, err := replica.deps.StateMachine.StartReset(&replica.stateSync.completion)
	if err != nil {
		replica.stateSync.completion.release(replica.stateSync.generation)
		replica.fail(errors.Join(ErrStateMachine, err))
		return false
	}
	replica.stateSync.resetStarted = true
	replica.stateSync.completionKind = SMCompletionReset
	if result.IsReady() {
		replica.stateSync.completion.release(replica.stateSync.generation)
		replica.stateSync.resetDone = true
	}
	return false
}

func (replica *Replica) startStateSyncPersistence(next Superblock, phase uint8) {
	handle, err := replica.io.Submit(IOOperation{Kind: IOSuperblockPersist, SuperblockStore: replica.superblocks, Superblock: next})
	if err != nil {
		if !errors.Is(err, ErrIOBackpressure) {
			replica.fail(err)
		}
		return
	}
	replica.stateSync.io = handle
	replica.stateSync.ioKind = IOSuperblockPersist
	replica.stateSync.persistPhase = phase
}

func (replica *Replica) stateSyncSuperblock() (Superblock, error) {
	next := replica.superblocks.Current()
	next.ParentChecksum = next.Checksum
	next.Sequence++
	next.State.Checkpoint = replica.stateSync.checkpoint
	next.State.CommitMax = replica.stateSync.commit
	next.State.View = replica.stateSync.view
	next.State.LogView = replica.stateSync.view
	next.ViewHeaderCount = uint32(replica.stateSync.count)
	clear(next.ViewHeaders[:])
	for index := range replica.stateSync.count {
		header := replica.stateSync.headers[index]
		if err := protocol.EncodeHeader(next.ViewHeaders[index][:], &header); err != nil {
			return Superblock{}, err
		}
	}
	return next, nil
}

func (replica *Replica) handleStateSyncPersistence(completion IOCompletion) bool {
	if replica.stateSync.io == (IOHandle{}) || completion.Handle != replica.stateSync.io || completion.Kind != replica.stateSync.ioKind {
		return false
	}
	replica.stateSync.io = IOHandle{}
	replica.stateSync.ioKind = 0
	if completion.Err != nil {
		replica.fail(completion.Err)
		return true
	}
	replica.stateSync.persistDone = true
	return true
}

func (replica *Replica) installStateSyncCheckpoint() {
	target := replica.stateSync.checkpoint
	checkpointID, err := target.ID()
	if err != nil {
		replica.fail(err)
		return
	}
	replica.checkpoint = target
	replica.checkpointID = checkpointID
	replica.clearBlockCatalog()
	replica.seedScrubCatalog(target)
	replica.commitMin = target.PrepareOp()
	replica.commitMax = replica.stateSync.commit
	replica.headOp = target.PrepareOp()
	header, reason := protocol.DecodeHeader(target.Header[:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
	if reason != protocol.RejectNone {
		replica.fail(ErrInvalidCheckpoint)
		return
	}
	replica.headChecksum = header.HeaderChecksum
	replica.view = replica.stateSync.view
	replica.durableView = replica.stateSync.view
	replica.logView = replica.stateSync.view
	replica.stateSync.stage = SyncStageResizingStorage
	replica.stateSync.persistPhase = 0
	replica.stateSync.persistDone = false
	replica.stateSync.repairing = true
}

func (replica *Replica) queueStateSyncRoots() bool {
	checkpoint := replica.stateSync.checkpoint
	op := checkpoint.PrepareOp()
	if !replica.queueStateSyncTrailer(
		BlockReference{Address: checkpoint.AcquiredTrailerLastAddress, Checksum: checkpoint.AcquiredTrailerLastChecksum},
		protocol.BlockFreeSet, op, checkpoint.AcquiredTrailerEncodedSize,
	) {
		return false
	}
	if !replica.queueStateSyncTrailer(
		BlockReference{Address: checkpoint.ReleasedTrailerLastAddress, Checksum: checkpoint.ReleasedTrailerLastChecksum},
		protocol.BlockFreeSet, op, checkpoint.ReleasedTrailerEncodedSize,
	) {
		return false
	}
	if !replica.queueStateSyncTrailer(
		BlockReference{Address: checkpoint.SessionTrailerLastAddress, Checksum: checkpoint.SessionTrailerLastChecksum},
		protocol.BlockClientSessions, op, checkpoint.SessionTrailerEncodedSize,
	) {
		return false
	}
	if checkpoint.ManifestBlockCount != 0 {
		reference := BlockReference{Address: checkpoint.NewestManifestAddress, Checksum: checkpoint.NewestManifestChecksum}
		if !replica.queueBlockRepair(reference, protocol.BlockManifest, op, blockSnapshotExpectation{}, 0, blockRepairStateSync) {
			return false
		}
		target, _ := replica.blockRepairTarget(reference)
		replica.blockRepairTargets[target].chainBlocks = checkpoint.ManifestBlockCount
	}
	if checkpoint.SnapshotRootAddress != 0 {
		if replica.deps.BlockValidator == nil {
			return false
		}
		requirement, err := replica.deps.BlockValidator.CheckpointRoot(checkpoint)
		if err != nil || requirement.Reference != (BlockReference{Address: checkpoint.SnapshotRootAddress, Checksum: checkpoint.SnapshotRootChecksum}) {
			return false
		}
		snapshot := blockSnapshotExpectation{value: requirement.Snapshot, exact: requirement.SnapshotExact}
		if !replica.queueBlockRepair(requirement.Reference, requirement.Type, op, snapshot, requirement.BodySize, blockRepairStateSync) {
			return false
		}
	}
	return true
}

func (replica *Replica) queueStateSyncTrailer(reference BlockReference, blockType protocol.BlockType, checkpoint protocol.Op, encodedSize uint64) bool {
	if reference == (BlockReference{}) {
		return encodedSize == 0
	}
	payload := replica.config.Cluster.BlockSize - protocol.HeaderSize
	expected := cmp.Or(encodedSize%payload, payload)
	if !replica.queueBlockRepair(reference, blockType, checkpoint, blockSnapshotExpectation{value: uint64(checkpoint), exact: true}, uint32(expected), blockRepairStateSync) {
		return false
	}
	target, _ := replica.blockRepairTarget(reference)
	replica.blockRepairTargets[target].chainBytes = encodedSize
	return true
}

func (replica *Replica) continueStateSyncBlocks(completed *blockRepairTarget, physical []byte) {
	frame, header, ok := decodeRepairFrame(physical, replica.config.Group, uint32(replica.config.Cluster.BlockSize), replica.membership.ActiveCount+replica.membership.StandbyCount)
	if !ok {
		replica.fail(ErrInvalidBlock)
		return
	}
	body := frame[protocol.HeaderSize:]
	var previous BlockReference
	copy(previous.Checksum[:], header.Fields[:16])
	previous.Address = binary.LittleEndian.Uint64(header.Fields[32:40])
	if completed.chainBytes != 0 {
		if uint64(len(body)) > completed.chainBytes {
			replica.fail(ErrInvalidBlock)
			return
		}
		remaining := completed.chainBytes - uint64(len(body))
		if (remaining == 0) != (previous == (BlockReference{})) {
			replica.fail(ErrInvalidBlock)
			return
		}
		if remaining != 0 {
			payload := replica.config.Cluster.BlockSize - protocol.HeaderSize
			expected := min(payload, remaining)
			if !replica.queueBlockRepair(previous, completed.blockType, completed.neededAtCheckpoint, completed.snapshot, uint32(expected), blockRepairStateSync) {
				replica.fail(ErrReplicaBackpressure)
				return
			}
			target, _ := replica.blockRepairTarget(previous)
			replica.blockRepairTargets[target].chainBytes = remaining
		}
	}
	if completed.chainBlocks != 0 {
		remaining := completed.chainBlocks - 1
		if (remaining == 0) != (previous == (BlockReference{})) {
			replica.fail(ErrInvalidBlock)
			return
		}
		if remaining != 0 {
			if !replica.queueBlockRepair(previous, completed.blockType, completed.neededAtCheckpoint, completed.snapshot, 0, blockRepairStateSync) {
				replica.fail(ErrReplicaBackpressure)
				return
			}
			target, _ := replica.blockRepairTarget(previous)
			replica.blockRepairTargets[target].chainBlocks = remaining
		}
	}
	if completed.blockType >= protocol.BlockManifest {
		var metadata [96]byte
		copy(metadata[:], header.Fields[:96])
		input := BlockValidationInput{
			Reference: completed.reference, NeededAtCheckpoint: completed.neededAtCheckpoint,
			Snapshot: binary.LittleEndian.Uint64(header.Fields[104:112]), Type: completed.blockType,
			Metadata: metadata, Body: body,
		}
		count, err := replica.deps.BlockValidator.ValidateBlock(input, replica.blockRequirements)
		if err != nil || count < 0 || count > len(replica.blockRequirements) {
			replica.fail(ErrInvalidBlock)
			return
		}
		for index := range count {
			requirement := replica.blockRequirements[index]
			snapshot := blockSnapshotExpectation{value: requirement.Snapshot, exact: requirement.SnapshotExact}
			if !replica.queueBlockRepair(requirement.Reference, requirement.Type, completed.neededAtCheckpoint, snapshot, requirement.BodySize, blockRepairStateSync) {
				replica.fail(ErrReplicaBackpressure)
				return
			}
		}
	}
	if !replica.blockRepairActive() {
		replica.finishStateSyncBlocks()
	}
}

func (replica *Replica) finishStateSyncBlocks() {
	blocks, err := openBlockRuntime(replica.deps.Storage, replica.config, replica.stateSync.checkpoint)
	if err != nil {
		replica.fail(err)
		return
	}
	sessions, err := NewSessionTable(SessionTableConfig{
		ClientsMax:              uint32(replica.config.Cluster.ClientsMax),
		Group:                   replica.config.Group,
		ActiveCount:             replica.membership.ActiveCount,
		MessageSizeMax:          uint32(replica.config.Cluster.MessageSizeMax),
		ApplicationReplySizeMax: uint32(replica.config.Cluster.ApplicationReplySizeMax),
	})
	if err != nil {
		replica.fail(err)
		return
	}
	if err := loadSessionTrailer(blocks, replica.stateSync.checkpoint, sessions); err != nil {
		replica.fail(err)
		return
	}
	replica.sessions = sessions
	replica.blocks = blocks.store
	replica.trailers = blocks.trailers
	replica.blockAllocator = blocks.allocator
	replica.stateSync.generation++
	replica.stateSync.completion.prepare(replica.stateSync.generation, replica)
	result, err := replica.deps.StateMachine.StartOpen(OpenCheckpointInput{State: replica.stateSync.checkpoint}, &replica.stateSync.completion)
	if err != nil {
		replica.stateSync.completion.release(replica.stateSync.generation)
		replica.fail(errors.Join(ErrStateMachine, err))
		return
	}
	replica.stateSync.opening = true
	replica.stateSync.completionKind = SMCompletionOpen
	if result.IsReady() {
		replica.stateSync.completion.release(replica.stateSync.generation)
		replica.finishStateSyncOpen()
	}
}

func (replica *Replica) handleStateSyncCompletion(completion *SMCompletion, generation uint64, result SMResult) bool {
	if completion != &replica.stateSync.completion || generation != replica.stateSync.generation {
		return false
	}
	if result.Err != nil || result.Kind != replica.stateSync.completionKind {
		replica.fail(errors.Join(ErrStateMachine, result.Err))
		return true
	}
	switch result.Kind {
	case SMCompletionReset:
		replica.stateSync.resetDone = true
	case SMCompletionOpen:
		replica.finishStateSyncOpen()
	default:
		replica.fail(ErrReplicaInvariant)
	}
	return true
}

type recoveryReplayResult struct {
	commitMin protocol.Op
	upgrades  recoveredUpgradeState
	err       error
}

func (replica *Replica) finishStateSyncOpen() {
	replica.stateSync.opening = false
	if replica.stateSync.resumeRecovery {
		replica.finishInterruptedStateSyncOpen()
		return
	}
	replica.stateSync.repairing = false
	replica.syncRangeRepaired = true
	if replica.stateSync.head > replica.checkpoint.PrepareOp() {
		if replica.stateSync.commit > replica.commitMin && replica.stateSync.head-replica.commitMin > protocol.Op(len(replica.pipeline)) {
			if !replica.beginRepairWindow(replica.stateSync.view, replica.stateSync.commit) {
				replica.fail(ErrReplicaInvariant)
			}
			return
		}
		clear(replica.canonicalHeaders)
		copy(replica.canonicalHeaders, replica.stateSync.headers[:replica.stateSync.count])
		replica.status = StatusRecoveringHead
		replica.repairViewValid = true
		replica.repairView = replica.stateSync.view
		replica.repairViewCommit = replica.stateSync.commit
		replica.repairViewHead = replica.stateSync.head
		replica.repairViewAncestor = replica.checkpoint.PrepareOp()
		replica.repairViewCount = replica.stateSync.count
		replica.repairViewRebuilt = false
		return
	}
	replica.status = StatusNormal
	replica.refreshLocalReleaseReport()
	replica.maybeSelectUpgrade()
}

func (replica *Replica) finishInterruptedStateSyncOpen() {
	ctx, cancel := context.WithCancel(context.Background())
	replica.recoveryLifecycle.Lock()
	if replica.shutdownStarted.Load() {
		replica.recoveryLifecycle.Unlock()
		cancel()
		return
	}
	replica.recoveryCancel = cancel
	replica.recoveryWorkers.Add(1)
	replica.recoveryLifecycle.Unlock()
	replica.stateSync.replayRunning = true
	durable := replica.stateSync.recoveryState
	recovery := replica.stateSync.recovery
	checkpoint := replica.checkpoint
	go func() {
		defer replica.recoveryWorkers.Done()
		result := recoveryReplayResult{commitMin: checkpoint.PrepareOp()}
		if recovery.FaultySlots == 0 {
			startup := &startupCompletionSink{ready: make(chan *SMCompletion, 1)}
			result.commitMin, result.upgrades, result.err = replayCommitted(
				ctx, replica.config, replica.deps.StateMachine, replica.wal, replica.replies, replica.sessions,
				startup, result.commitMin, checkpoint.Release, durable.State.CommitMax,
			)
		}
		replica.recoveryReady <- result
		replica.signal()
	}()
}

func (replica *Replica) finishInterruptedReplay(result recoveryReplayResult) {
	replica.recoveryLifecycle.Lock()
	if replica.recoveryCancel != nil {
		replica.recoveryCancel()
		replica.recoveryCancel = nil
	}
	replica.recoveryLifecycle.Unlock()
	replica.stateSync.replayRunning = false
	if result.err != nil {
		replica.fail(result.err)
		return
	}
	durable := replica.stateSync.recoveryState
	recovery := replica.stateSync.recovery
	status, view, durableView, logView, headOp, err := deriveRecoveredStatus(replica.config, durable, recovery, replica.wal, replica.superblocks)
	if err != nil {
		replica.fail(err)
		return
	}
	committed, err := recoveredCommittedHeader(replica.config, durable, recovery, result.commitMin, replica.wal, replica.membership.ActiveCount+replica.membership.StandbyCount)
	if err != nil {
		replica.fail(err)
		return
	}
	replica.status = status
	replica.view = view
	replica.durableView = durableView
	replica.logView = logView
	replica.headOp = result.commitMin
	replica.commitMin = result.commitMin
	replica.commitMax = durable.State.CommitMax
	replica.headChecksum = committed.HeaderChecksum
	replica.lastPrepareTimestamp = prepareTimestamp(&committed)
	replica.lastCommitTimestamp = prepareTimestamp(&committed)
	replica.upgradeTarget = result.upgrades.target
	replica.upgradeWindow = result.upgrades.window
	if status == StatusRecoveringHead {
		replica.headOp = headOp
		replica.headChecksum = recovery.HeadHeader.HeaderChecksum
	}
	if err := loadOpenSuffix(replica, recovery, result.commitMin); err != nil {
		replica.fail(err)
		return
	}
	replica.stateSync.resumeRecovery = false
	replica.stateSync.repairing = false
	replica.stateSync.recovery = WALRecoveryReport{}
	replica.stateSync.recoveryState = Superblock{}
	replica.syncRangeRepaired = true
	replica.refreshLocalReleaseReport()
	replica.maybeSelectUpgrade()
}
