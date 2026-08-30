package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var (
	ErrAuthentication              = errors.New("replication: unauthenticated frame")
	ErrProtocolInvariant           = errors.New("replication: protocol invariant violated")
	ErrQuorumUnavailable           = errors.New("replication: quorum unavailable")
	ErrUnrecoverableConsensusState = errors.New("replication: consensus state is unrecoverable")
	ErrReplicaClosed               = errors.New("replication: replica is closed")
	ErrReplicaRunning              = errors.New("replication: replica run loop already started")
	ErrReplicaBackpressure         = errors.New("replication: replica event queue exhausted")
	ErrClockUnsynchronized         = errors.New("replication: cluster clock is not synchronized")
)

type Dependencies struct {
	Storage         Storage
	MessageBus      MessageBus
	Clock           Clock
	Entropy         io.Reader
	StateMachine    StateMachine
	ReleaseExecutor ReleaseExecutor
	BlockValidator  BlockValidator
	Metrics         *ReplicaMetrics
	Logger          *zerolog.Logger
	SynchronousIO   bool
}

type ReplicaInitialState struct {
	Status            Status
	View              protocol.View
	DurableView       protocol.View
	LogView           protocol.View
	HeadOp            protocol.Op
	CommitMin         protocol.Op
	CommitMax         protocol.Op
	Checkpoint        CheckpointState
	HeadHeader        protocol.Header
	upgradeTarget     protocol.Release
	upgradeWindow     upgradeWindow
	syncRangeRepaired bool
}

type CommitStage uint8

const (
	CommitStageIdle CommitStage = iota
	CommitStageStart
	CommitStageCheckPrepare
	CommitStagePrefetch
	CommitStageStall
	CommitStageReplySetup
	CommitStageExecute
	CommitStageCheckpointDurable
	CommitStageCompact
	CommitStageCheckpointData
	CommitStageCheckpointSuperblock
)

type replicaEventKind uint8

const (
	replicaEventMessage replicaEventKind = iota + 1
	replicaEventSMCompletion
)

type replicaEvent struct {
	kind       replicaEventKind
	message    *Message
	completion *SMCompletion
	generation uint64
}

type queuedRequest struct {
	message *Message
	time    TimeSample
}

type duplicateRead struct {
	busy   bool
	handle IOHandle
	header protocol.Header
	client protocol.ClientID
	buffer []byte
}

type pipelineEntry struct {
	generation    uint64
	prepare       *Message
	header        protocol.Header
	acks          uint16
	durable       bool
	quorum        bool
	io            IOHandle
	ioKind        IOKind
	stage         CommitStage
	token         PrefetchToken
	completion    SMCompletion
	reply         *Message
	replyHeader   protocol.Header
	replyPlan     SessionCommitPlan
	lastBroadcast uint64
}

type joinRecord struct {
	valid      bool
	present    uint16
	nack       uint16
	head       protocol.Op
	commit     protocol.Op
	checkpoint protocol.Op
	logView    protocol.View
	count      uint8
}

type repairWrite struct {
	busy    bool
	handle  IOHandle
	message *Message
	op      protocol.Op
}

type upgradeWindow struct {
	checkpoint protocol.Op
	target     protocol.Release
	valid      bool
	started    bool
}

type Replica struct {
	config     Config
	membership Membership
	local      protocol.ReplicaIndex
	quorums    Quorums
	deps       Dependencies
	logger     zerolog.Logger
	metrics    *ReplicaMetrics

	status                  Status
	view                    protocol.View
	durableView             protocol.View
	logView                 protocol.View
	headOp                  protocol.Op
	commitMin               protocol.Op
	commitMax               protocol.Op
	checkpoint              CheckpointState
	checkpointID            protocol.CheckpointID
	headChecksum            protocol.Checksum
	lastPrepareTimestamp    uint64
	lastCommitTimestamp     uint64
	lastPrimaryCommit       uint64
	failureDetector         FailureDetector
	timers                  replicaTimers
	random                  DeterministicRandom
	checkpointSession       []byte
	checkpointSessionOp     protocol.Op
	checkpointTarget        protocol.Op
	checkpointTargetRelease protocol.Release
	checkpointManifest      CheckpointManifest
	checkpointCandidate     BlockCheckpointCandidate
	pendingCheckpoint       CheckpointState
	checkpointBlocks        *CheckpointBlockTransaction
	checkpointReader        *CheckpointBlockReader
	checkpointBlockLimit    uint32
	checkpointReleases      []uint64
	checkpointSuperseded    []uint64

	wal                    *WAL
	replies                *ReplyStore
	superblocks            *SuperblockStore
	sessions               *SessionTable
	blocks                 *BlockStore
	trailers               *TrailerStore
	blockAllocator         *BlockAllocator
	io                     *IOEngine
	frames                 *protocol.FramePool
	events                 *MPSCRing[replicaEvent]
	notify                 chan struct{}
	pipeline               []pipelineEntry
	pipelineHead           uint32
	pipelineLen            uint32
	requestQueue           []queuedRequest
	requestHead            uint32
	requestLen             uint32
	duplicateReads         []duplicateRead
	repairReads            []repairRead
	stage                  CommitStage
	stageGeneration        uint64
	exitViewBits           uint16
	joins                  []joinRecord
	joinHeaders            []protocol.Header
	canonicalHeaders       []protocol.Header
	viewIO                 IOHandle
	viewInstall            bool
	pendingView            protocol.View
	viewCommit             protocol.Op
	viewHead               protocol.Op
	repairBudget           journalRepairBudget
	repairHeader           protocol.Header
	repairHeaderValid      bool
	blockRepairBudget      blockRepairBudget
	blockRepairTargets     []blockRepairTarget
	blockRepairIO          []blockRepairIO
	peerCheckpointOps      []protocol.Op
	repairWrite            repairWrite
	repairHeadersLast      uint64
	getViewNonce           protocol.Nonce
	blockRequirements      []BlockRequirement
	blockCatalog           []BlockRequirement
	blockCatalogCount      int
	scrub                  scrubRuntime
	getViewLast            uint64
	repairViewValid        bool
	repairView             protocol.View
	repairViewCommit       protocol.Op
	repairViewHead         protocol.Op
	repairViewAncestor     protocol.Op
	repairViewRebuilt      bool
	repairViewWindow       bool
	repairViewCount        int
	repairFrames           []*Message
	releaseHistory         []protocol.Release
	releaseReports         []releaseReport
	upgradeTarget          protocol.Release
	upgradeWindow          upgradeWindow
	releaseActivation      protocol.Release
	stateSync              stateSyncRuntime
	syncRangeRepaired      bool
	recoveryReady          chan recoveryReplayResult
	releaseReset           SMCompletion
	releaseResetGeneration uint64
	releaseResetDone       bool
	joinViewBits           uint16

	accepting          atomic.Bool
	submitters         atomic.Int64
	runState           atomic.Uint32
	stopOnce           sync.Once
	stop               chan struct{}
	done               chan struct{}
	shutdownStarted    atomic.Bool
	shutdownCompletion SMCompletion
	shutdownGeneration uint64
	shutdownErr        error
	recoveryLifecycle  sync.Mutex
	recoveryCancel     context.CancelFunc
	recoveryWorkers    sync.WaitGroup
	fatalErr           error
}

func newReplica(config Config, dependencies Dependencies, initial ReplicaInitialState, wal *WAL, replyStore *ReplyStore, sessions *SessionTable, superblocks *SuperblockStore) (*Replica, error) {
	return newReplicaWithBlocks(config, dependencies, initial, wal, replyStore, sessions, superblocks, nil)
}

func newReplicaWithBlocks(config Config, dependencies Dependencies, initial ReplicaInitialState, wal *WAL, replyStore *ReplyStore, sessions *SessionTable, superblocks *SuperblockStore, blocks *blockRuntime) (*Replica, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if dependencies.Storage == nil || dependencies.MessageBus == nil || dependencies.Clock == nil || dependencies.Entropy == nil {
		return nil, ErrInvalidConfiguration
	}
	if dependencies.StateMachine == nil || wal == nil || replyStore == nil || sessions == nil || superblocks == nil {
		return nil, ErrInvalidConfiguration
	}
	capacities := dependencies.StateMachine.Capacities()
	if uint64(capacities.RequestBytes) != config.Cluster.ApplicationBatchSizeMax || uint64(capacities.ReplyBytes) != config.Cluster.ApplicationReplySizeMax || uint64(capacities.PrefetchMax) < config.Cluster.PipelineMax || capacities.CheckpointMax == 0 {
		return nil, ErrInvalidConfiguration
	}
	blockBase, _ := config.Cluster.BlockBase()
	maximumCheckpointBlocks := (config.Process.StorageSizeLimit - blockBase) / config.Cluster.BlockSize
	if uint64(capacities.CheckpointMax) > maximumCheckpointBlocks {
		return nil, ErrInvalidConfiguration
	}
	local, ok := config.Membership.LocalIndex()
	if !ok {
		return nil, ErrInvalidMembership
	}
	quorums, ok := QuorumsFor(config.Membership.ActiveCount, uint8(min(config.Cluster.ReplicationQuorumMax, uint64(^uint8(0)))))
	if !ok {
		return nil, ErrInvalidConfiguration
	}
	repairBudget, err := newJournalRepairBudget(config.Membership.ActiveCount, local)
	if err != nil {
		return nil, err
	}
	memberCount := config.Membership.ActiveCount + config.Membership.StandbyCount
	blockBudget, err := newBlockRepairBudget(memberCount, local, config.Process.RepairRequestsMax)
	if err != nil {
		return nil, err
	}
	releases, err := executableReleases(config, dependencies.ReleaseExecutor)
	if err != nil || !containsRelease(releases, initial.Checkpoint.Release) {
		return nil, ErrReleaseUnavailable
	}
	if initial.upgradeTarget != 0 {
		if initial.upgradeTarget <= config.CurrentRelease || !containsRelease(releases, initial.upgradeTarget) {
			return nil, ErrWALRecovery
		}
		if initial.upgradeWindow.target != 0 && initial.upgradeWindow.target != initial.upgradeTarget {
			return nil, ErrWALRecovery
		}
	}
	releaseReports := make([]releaseReport, int(config.Membership.ActiveCount))
	releaseStorage := make([]protocol.Release, int(config.Cluster.ReleaseHistoryMax)*len(releaseReports))
	for index := range releaseReports {
		start := index * int(config.Cluster.ReleaseHistoryMax)
		releaseReports[index].releases = releaseStorage[start : start+int(config.Cluster.ReleaseHistoryMax)]
	}
	if blocks == nil {
		var err error
		blocks, err = openBlockRuntime(dependencies.Storage, config, initial.Checkpoint)
		if err != nil {
			return nil, err
		}
	}
	eventCapacity := uint64(2)
	minimumEvents := config.Cluster.PipelineMax*4 + uint64(config.Process.PrimaryRequestQueueMax) + 8
	for eventCapacity < minimumEvents {
		eventCapacity <<= 1
	}
	events, err := NewMPSCRing[replicaEvent](eventCapacity)
	if err != nil {
		return nil, err
	}
	frameCount := uint64(config.Cluster.PipelineMax)*3 + uint64(config.Process.PrimaryRequestQueueMax) + 16
	if frameCount > uint64(^uint32(0)) {
		return nil, ErrInvalidConfiguration
	}
	frames, err := protocol.NewFramePool(uint32(frameCount), uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		return nil, err
	}
	blockIOCount := max(config.Process.RepairReadsMax, config.Process.ScrubWriteConcurrency)
	requestCount := config.Process.JournalWriteConcurrency + config.Process.ReplyReadConcurrency + config.Process.ReplyWriteConcurrency + config.Process.RepairReadsMax + blockIOCount + 4
	ioEngine, err := newIOEngine(dependencies.Storage, requestCount, min(requestCount, uint32(4)), dependencies.SynchronousIO)
	if err != nil {
		return nil, err
	}
	logger := zerolog.Nop()
	if dependencies.Logger != nil {
		logger = *dependencies.Logger
	}
	metrics := dependencies.Metrics
	if metrics == nil {
		metrics = &ReplicaMetrics{}
	}
	checkpointID, err := initial.Checkpoint.ID()
	if err != nil {
		_ = ioEngine.Close(context.Background())
		return nil, err
	}
	duplicateBytes, ok := checkedMul(wal.Layout().ReplyStride, uint64(config.Process.ReplyReadConcurrency))
	if !ok {
		_ = ioEngine.Close(context.Background())
		return nil, ErrInvalidConfiguration
	}
	duplicateStorage, err := NewAlignedBuffer(duplicateBytes, SectorSize)
	if err != nil {
		_ = ioEngine.Close(context.Background())
		return nil, err
	}
	duplicateReads := make([]duplicateRead, int(config.Process.ReplyReadConcurrency))
	for index := range duplicateReads {
		start := uint64(index) * wal.Layout().ReplyStride
		duplicateReads[index].buffer = duplicateStorage[start : start+wal.Layout().ReplyStride]
	}
	repairStride := max(wal.Layout().PrepareStride, config.Cluster.BlockSize)
	repairBytes, ok := checkedMul(repairStride, uint64(config.Process.RepairReadsMax))
	if !ok {
		_ = ioEngine.Close(context.Background())
		return nil, ErrInvalidConfiguration
	}
	repairStorage, err := NewAlignedBuffer(repairBytes, SectorSize)
	if err != nil {
		_ = ioEngine.Close(context.Background())
		return nil, err
	}
	repairReads := make([]repairRead, int(config.Process.RepairReadsMax))
	for index := range repairReads {
		start := uint64(index) * repairStride
		repairReads[index].buffer = repairStorage[start : start+repairStride]
	}
	blockRepairBytes, ok := checkedMul(config.Cluster.BlockSize, uint64(blockIOCount))
	if !ok {
		_ = ioEngine.Close(context.Background())
		return nil, ErrInvalidConfiguration
	}
	blockRepairStorage, err := NewAlignedBuffer(blockRepairBytes, SectorSize)
	if err != nil {
		_ = ioEngine.Close(context.Background())
		return nil, err
	}
	blockRepairIO := make([]blockRepairIO, int(blockIOCount))
	for index := range blockRepairIO {
		start := uint64(index) * config.Cluster.BlockSize
		blockRepairIO[index].buffer = blockRepairStorage[start : start+config.Cluster.BlockSize]
	}
	targetCapacity := int(memberCount)*int(config.Process.RepairRequestsMax+1) + int(config.Process.ScrubReadConcurrency) + 5
	joins := make([]joinRecord, int(config.Membership.ActiveCount))
	joinHeaders := make([]protocol.Header, int(config.Membership.ActiveCount)*int(config.Cluster.PipelineMax+1))
	canonicalHeaders := make([]protocol.Header, int(config.Cluster.PipelineMax+1))
	initialTime := dependencies.Clock.Now()
	timers, err := newReplicaTimers(config.Process)
	if err != nil {
		_ = ioEngine.Close(context.Background())
		return nil, err
	}
	replica := &Replica{
		config:               config,
		membership:           config.Membership,
		local:                local,
		quorums:              quorums,
		deps:                 dependencies,
		logger:               logger,
		metrics:              metrics,
		status:               initial.Status,
		view:                 initial.View,
		durableView:          initial.DurableView,
		logView:              initial.LogView,
		headOp:               initial.HeadOp,
		commitMin:            initial.CommitMin,
		commitMax:            initial.CommitMax,
		checkpoint:           initial.Checkpoint,
		syncRangeRepaired:    initial.syncRangeRepaired,
		checkpointID:         checkpointID,
		headChecksum:         initial.HeadHeader.HeaderChecksum,
		lastPrepareTimestamp: prepareTimestamp(&initial.HeadHeader),
		lastCommitTimestamp:  prepareTimestamp(&initial.HeadHeader),
		failureDetector:      NewFailureDetector(initialTime.Monotonic),
		timers:               timers,
		random:               NewDeterministicRandom(uint64(local) + 1),
		checkpointSession:    make([]byte, sessions.TrailerSize()),
		wal:                  wal,
		replies:              replyStore,
		superblocks:          superblocks,
		sessions:             sessions,
		io:                   ioEngine,
		frames:               frames,
		blocks:               blocks.store,
		trailers:             blocks.trailers,
		blockAllocator:       blocks.allocator,
		checkpointBlocks:     newCheckpointBlockTransaction(blocks.allocator, blocks.store, capacities.CheckpointMax),
		checkpointReader:     newCheckpointBlockReader(blocks.allocator, blocks.store),
		checkpointBlockLimit: capacities.CheckpointMax,
		events:               events,
		notify:               make(chan struct{}, 1),
		pipeline:             make([]pipelineEntry, int(config.Cluster.PipelineMax)),
		requestQueue:         make([]queuedRequest, int(config.Process.PrimaryRequestQueueMax)),
		duplicateReads:       duplicateReads,
		repairReads:          repairReads,
		joins:                joins,
		joinHeaders:          joinHeaders,
		repairFrames:         make([]*Message, int(config.Cluster.PipelineMax+1)),
		canonicalHeaders:     canonicalHeaders,
		repairBudget:         repairBudget,
		blockRepairBudget:    blockBudget,
		blockRepairTargets:   make([]blockRepairTarget, targetCapacity),
		blockRepairIO:        blockRepairIO,
		blockRequirements:    make([]BlockRequirement, targetCapacity),
		peerCheckpointOps:    make([]protocol.Op, int(memberCount)),
		releaseHistory:       releases,
		releaseReports:       releaseReports,
		upgradeTarget:        initial.upgradeTarget,
		blockCatalog:         make([]BlockRequirement, targetCapacity),
		upgradeWindow:        initial.upgradeWindow,
		recoveryReady:        make(chan recoveryReplayResult, 1),
		stateSync:            stateSyncRuntime{headers: make([]protocol.Header, int(config.Cluster.PipelineMax+1))},
		stop:                 make(chan struct{}),
		done:                 make(chan struct{}),
	}
	replica.seedScrubCatalog(initial.Checkpoint)
	replica.refreshLocalReleaseReport()
	replica.maybeSelectUpgrade()
	if err := replica.checkInvariants(); err != nil {
		_ = ioEngine.Close(context.Background())
		return nil, err
	}
	replica.accepting.Store(true)
	return replica, nil
}

func (replica *Replica) Snapshot() ReplicaSnapshot {
	committing := false
	if replica.pipelineLen > 0 {
		entry := replica.pipelineEntry(0)
		committing = entry.stage >= CommitStageCheckpointDurable && prepareOp(&entry.header) == replica.commitMin
	}
	return ReplicaSnapshot{
		Status:      replica.status,
		View:        replica.view,
		DurableView: replica.durableView,
		LogView:     replica.logView,
		HeadOp:      replica.headOp,
		CommitMin:   replica.commitMin,
		CommitMax:   replica.commitMax,
		Checkpoint:  replica.checkpoint,
		PipelineLen: replica.pipelineLen,
		Committing:  committing,
		Primary:     replica.membership.Primary(replica.view),
	}
}

func (replica *Replica) DurableChecksum(op protocol.Op) (protocol.Checksum, bool) {
	if op < replica.checkpoint.PrepareOp() || op > replica.headOp {
		return protocol.Checksum{}, false
	}
	header, present, dirty, found := replica.localHeaderEvidence(op)
	if !found || !present || dirty {
		return protocol.Checksum{}, false
	}
	return header.HeaderChecksum, true
}

func (replica *Replica) Submit(frame *protocol.Frame) error {
	replica.submitters.Add(1)
	defer replica.submitters.Add(-1)
	if !replica.accepting.Load() {
		return ErrReplicaClosed
	}
	source, member, sender := frame.Source()
	memberCount := replica.membership.ActiveCount + replica.membership.StandbyCount
	invalidReplicaSource := source == protocol.FrameSourceReplica &&
		(uint8(sender) >= memberCount || member != replica.membership.Members[sender])
	if source == protocol.FrameSourceUnbound || invalidReplicaSource {
		replica.metrics.framesRejected.Add(1)
		return ErrAuthentication
	}
	if _, err := frame.Bytes(); err != nil {
		return err
	}
	if !replica.events.TryPush(replicaEvent{kind: replicaEventMessage, message: frame}) {
		replica.metrics.eventBackpressure.Add(1)
		return ErrReplicaBackpressure
	}
	replica.signal()
	return nil
}

func (replica *Replica) enqueueSMCompletion(completion *SMCompletion) error {
	event := replicaEvent{kind: replicaEventSMCompletion, completion: completion, generation: completion.generation}
	if !replica.events.TryPush(event) {
		replica.metrics.eventBackpressure.Add(1)
		return ErrReplicaBackpressure
	}
	replica.signal()
	return nil
}

func (replica *Replica) Run(ctx context.Context) (runErr error) {
	if !replica.runState.CompareAndSwap(0, 1) {
		return ErrReplicaRunning
	}
	defer func() {
		replica.accepting.Store(false)
		replica.startShutdown()
		<-replica.done
		replica.runState.Store(2)
		if runErr == nil {
			runErr = replica.shutdownErr
		}
	}()
	ticker := time.NewTicker(replica.config.Process.Tick)
	defer ticker.Stop()
	for {
		if _, err := replica.Process(64); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-replica.stop:
			return nil
		case <-replica.notify:
		case <-replica.io.Ready():
		case <-ticker.C:
			if !replica.stateSync.replayRunning {
				replica.handleTick(replica.deps.Clock.Now())
			}
		}
	}
}

func (replica *Replica) Tick() error {
	if !replica.accepting.Load() {
		return ErrReplicaClosed
	}
	if !replica.stateSync.replayRunning {
		replica.handleTick(replica.deps.Clock.Now())
	}
	return replica.fatalErr
}

func (replica *Replica) Process(limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	if replica.releaseActivation != 0 {
		return replica.processReleaseActivation(limit)
	}
	processed := 0
	for processed < limit {
		select {
		case result := <-replica.recoveryReady:
			replica.finishInterruptedReplay(result)
			processed++
			continue
		default:
		}
		var completion IOCompletion
		if replica.io.Poll(&completion) {
			if replica.stateSync.stage == SyncStageCancelingGrid {
				replica.handleStateSyncDrainIO(completion)
			} else {
				replica.handleIOCompletion(completion)
			}
			processed++
			if replica.releaseActivation != 0 {
				drained, err := replica.processReleaseActivation(limit - processed)
				return processed + drained, err
			}
			continue
		}
		var event replicaEvent
		if !replica.events.TryPop(&event) {
			break
		}
		switch event.kind {
		case replicaEventMessage:
			if replica.stateSync.stage != SyncStageIdle || replica.stateSync.replayRunning {
				event.message.Release()
			} else if !replica.handleMessage(event.message) {
				event.message.Release()
			}
		case replicaEventSMCompletion:
			if replica.stateSync.stage == SyncStageCancelingGrid || replica.stateSync.stage == SyncStageUpdatingCheckpoint || replica.stateSync.replayRunning {
				replica.handleStateSyncDrainSM(event)
			} else {
				replica.handleSMCompletion(event)
			}
		default:
			replica.fail(ErrReplicaInvariant)
		}
		processed++
		if replica.releaseActivation != 0 {
			drained, err := replica.processReleaseActivation(limit - processed)
			return processed + drained, err
		}
	}
	if replica.releaseActivation != 0 {
		drained, err := replica.processReleaseActivation(limit - processed)
		return processed + drained, err
	}
	if !replica.stateSync.replayRunning && (replica.stateSync.stage == SyncStageIdle || replica.stateSync.stage == SyncStageCancelingCommit) {
		replica.advanceCommit()
	}
	replica.dispatchStateSync()
	if replica.fatalErr != nil {
		return processed, replica.fatalErr
	}
	if err := replica.checkInvariants(); err != nil {
		replica.fail(err)
		return processed, err
	}
	return processed, nil
}

func (replica *Replica) Close(ctx context.Context) error {
	replica.accepting.Store(false)
	if replica.runState.CompareAndSwap(0, 2) {
		replica.startShutdown()
	} else {
		replica.stopOnce.Do(func() { close(replica.stop) })
	}
	select {
	case <-replica.done:
		return replica.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (replica *Replica) startShutdown() {
	if replica.shutdownStarted.Swap(true) {
		return
	}
	replica.stopOnce.Do(func() { close(replica.stop) })
	go replica.finishShutdown()
}

func (replica *Replica) finishShutdown() {
	defer close(replica.done)
	replica.accepting.Store(false)
	for replica.submitters.Load() != 0 {
		time.Sleep(time.Millisecond)
	}
	replica.recoveryLifecycle.Lock()
	if replica.recoveryCancel != nil {
		replica.recoveryCancel()
	}
	replica.recoveryLifecycle.Unlock()
	replica.recoveryWorkers.Wait()
	replica.shutdownGeneration++
	generation := replica.shutdownGeneration
	replica.shutdownCompletion.prepare(generation, replica)
	reset, err := replica.deps.StateMachine.StartReset(&replica.shutdownCompletion)
	if err != nil {
		replica.shutdownErr = errors.Join(ErrStateMachine, err)
	} else if reset.IsReady() {
		replica.shutdownCompletion.release(generation)
	} else {
		replica.waitForReset(generation)
	}
	if err := replica.io.Close(context.Background()); err != nil && replica.shutdownErr == nil {
		replica.shutdownErr = err
	}
	replica.drainShutdownEvents(generation)
	replica.releaseOwnedFrames()
	if err := replica.deps.StateMachine.Close(); err != nil && replica.shutdownErr == nil {
		replica.shutdownErr = err
	}
	if err := replica.deps.Storage.Close(); err != nil && replica.shutdownErr == nil {
		replica.shutdownErr = err
	}
}

func (replica *Replica) waitForReset(generation uint64) {
	for {
		if replica.drainShutdownEvents(generation) {
			return
		}
		select {
		case <-replica.notify:
		case <-replica.io.Ready():
		}
	}
}

func (replica *Replica) drainShutdownEvents(generation uint64) bool {
	resetComplete := false
	var completion IOCompletion
	for replica.io.Poll(&completion) {
	}
	var event replicaEvent
	for replica.events.TryPop(&event) {
		switch event.kind {
		case replicaEventMessage:
			event.message.Release()
		case replicaEventSMCompletion:
			result, ok := event.completion.take(event.generation)
			if event.completion == &replica.shutdownCompletion && event.generation == generation {
				resetComplete = ok && result.Kind == SMCompletionReset
				if !resetComplete && replica.shutdownErr == nil {
					replica.shutdownErr = errors.Join(ErrStateMachine, result.Err)
				}
			}
		}
	}
	return resetComplete
}

func (replica *Replica) releaseOwnedFrames() {
	for replica.requestLen > 0 {
		queued := &replica.requestQueue[replica.requestHead]
		if queued.message != nil {
			queued.message.Release()
		}
		*queued = queuedRequest{}
		replica.requestHead = (replica.requestHead + 1) % uint32(len(replica.requestQueue))
		replica.requestLen--
	}
	for replica.pipelineLen > 0 {
		entry := replica.pipelineEntry(0)
		replica.sessions.Abort(entry.replyPlan)
		replica.popPipeline()
	}
	if replica.repairWrite.busy && replica.repairWrite.message != nil {
		replica.repairWrite.message.Release()
		replica.repairWrite = repairWrite{}
	}
	for index := range replica.repairFrames {
		if replica.repairFrames[index] != nil {
			replica.repairFrames[index].Release()
			replica.repairFrames[index] = nil
		}
	}
}

func (replica *Replica) Transition(to Status, cause TransitionCause) error {
	if !validStatusTransition(replica.status, to, cause) {
		return ErrStatusTransition
	}
	replica.status = to
	return replica.checkInvariants()
}

func (replica *Replica) signal() {
	select {
	case replica.notify <- struct{}{}:
	default:
	}
}

func (replica *Replica) pipelineEntry(offset uint32) *pipelineEntry {
	return &replica.pipeline[(replica.pipelineHead+offset)%uint32(len(replica.pipeline))]
}

func (replica *Replica) pushPipeline(frame *Message, header protocol.Header) (*pipelineEntry, bool) {
	if replica.pipelineLen == uint32(len(replica.pipeline)) {
		return nil, false
	}
	entry := replica.pipelineEntry(replica.pipelineLen)
	generation := entry.generation + 1
	*entry = pipelineEntry{generation: generation, prepare: frame, header: header, stage: CommitStageIdle}
	replica.pipelineLen++
	replica.headOp = prepareOp(&header)
	replica.headChecksum = header.HeaderChecksum
	return entry, true
}

func (replica *Replica) popPipeline() {
	entry := replica.pipelineEntry(0)
	if entry.prepare != nil {
		entry.prepare.Release()
	}
	if entry.reply != nil {
		entry.reply.Release()
	}
	generation := entry.generation
	*entry = pipelineEntry{generation: generation}
	replica.pipelineHead = (replica.pipelineHead + 1) % uint32(len(replica.pipeline))
	replica.pipelineLen--
}

func (replica *Replica) checkInvariants() error {
	return validateReplicaSnapshot(replica.Snapshot(), replica.config.Cluster, replica.local)
}

func (replica *Replica) isPrimary() bool {
	return replica.membership.Primary(replica.view) == replica.local
}

func (replica *Replica) fail(err error) {
	if replica.fatalErr == nil {
		replica.fatalErr = err
		replica.accepting.Store(false)
		event := replica.logger.Error()
		event.Err(err)
		event.Uint32("view", uint32(replica.view))
		event.Uint64("head", uint64(replica.headOp))
		event.Msg("replica fail-stop")
	}
}

func (replica *Replica) handleTick(sample TimeSample) {
	replica.tickTimers(sample)
}

func (replica *Replica) handleSMCompletion(event replicaEvent) {
	result, ok := event.completion.take(event.generation)
	if !ok {
		replica.metrics.staleCompletions.Add(1)
		return
	}
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if &entry.completion != event.completion || entry.generation != event.generation {
			continue
		}
		if result.Err != nil {
			if entry.stage == CommitStageCheckpointData {
				if err := replica.checkpointBlocks.abort(); err != nil {
					replica.fail(errors.Join(ErrStateMachine, result.Err, err))
					return
				}
			}
			replica.fail(errors.Join(ErrStateMachine, result.Err))
			return
		}
		switch result.Kind {
		case SMCompletionPrefetch:
			if entry.stage != CommitStagePrefetch {
				replica.fail(ErrReplicaInvariant)
				return
			}
			entry.token = result.Prefetch
			entry.stage = CommitStageStall
		case SMCompletionCompact:
			replica.applyCompact(entry, result.Compact)
		case SMCompletionCheckpoint:
			replica.applyCheckpoint(entry, result.Manifest)
		default:
			replica.fail(ErrReplicaInvariant)
		}
		return
	}
	if replica.handleStateSyncCompletion(event.completion, event.generation, result) {
		return
	}
	replica.metrics.staleCompletions.Add(1)
}

func prepareOp(header *protocol.Header) protocol.Op {
	return protocol.Op(binary.LittleEndian.Uint64(header.Fields[96:104]))
}

func prepareCommit(header *protocol.Header) protocol.Op {
	return protocol.Op(binary.LittleEndian.Uint64(header.Fields[104:112]))
}

func prepareTimestamp(header *protocol.Header) uint64 {
	return binary.LittleEndian.Uint64(header.Fields[112:120])
}

func prepareRequest(header *protocol.Header) protocol.RequestNo {
	return protocol.RequestNo(binary.LittleEndian.Uint32(header.Fields[120:124]))
}

func prepareOperation(header *protocol.Header) protocol.Operation {
	return protocol.Operation(header.Fields[124])
}

func prepareClient(header *protocol.Header) protocol.ClientID {
	var client protocol.ClientID
	copy(client[:], header.Fields[80:96])
	return client
}

func prepareRequestChecksum(header *protocol.Header) protocol.Checksum {
	var checksum protocol.Checksum
	copy(checksum[:], header.Fields[32:48])
	return checksum
}

func prepareParent(header *protocol.Header) protocol.Checksum {
	var checksum protocol.Checksum
	copy(checksum[:], header.Fields[0:16])
	return checksum
}

func countAckBits(value uint16) uint8 {
	return uint8(bits.OnesCount16(value))
}
