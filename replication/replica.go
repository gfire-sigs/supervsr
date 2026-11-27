package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"z.gfire.dev/supervsr/replication/internal/simhook"
	"z.gfire.dev/supervsr/replication/protocol"
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

// Dependencies injects storage, transport, time, entropy, and state machine
// collaborators required to open and drive a Replica.
type Dependencies struct {
	Storage         Storage
	MessageBus      MessageBus
	Clock           Clock
	Entropy         io.Reader
	StateMachine    StateMachine
	ReleaseExecutor ReleaseExecutor
	BlockValidator  BlockValidator
	Metrics         *ReplicaMetrics
	Logger          *slog.Logger
}

type replicaInitialState struct {
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
	checkpointSession checkpointSessionSnapshot
}

// CommitStage identifies the active commit or checkpoint pipeline phase reported in ReplicaSnapshot.
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
	handle ioHandle
	header protocol.Header
	client protocol.ClientID
	buffer []byte
}

type pipelineEntry struct {
	generation       uint64
	prepare          *Message
	header           protocol.Header
	acks             uint16
	durable          bool
	quorum           bool
	io               ioHandle
	ioKind           ioKind
	stage            CommitStage
	token            PrefetchToken
	completion       SMCompletion
	reply            *Message
	replyHeader      protocol.Header
	replyPlan        sessionCommitPlan
	createdMonotonic uint64
	lastBroadcast    uint64
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
	handle  ioHandle
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
	config        Config
	membership    Membership
	local         protocol.ReplicaIndex
	quorums       Quorums
	deps          Dependencies
	clockObserver ClockObserver
	logger        *slog.Logger
	metrics       *ReplicaMetrics

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
	failureDetector         livenessDetector
	timers                  replicaTimers
	random                  deterministicRandom
	checkpointSession       []byte
	checkpointSessionOp     protocol.Op
	checkpointTarget        protocol.Op
	checkpointTargetRelease protocol.Release
	checkpointManifest      CheckpointManifest
	checkpointCandidate     blockCheckpointCandidate
	pendingCheckpoint       CheckpointState
	checkpointBlocks        *CheckpointBlockTransaction
	checkpointReader        *CheckpointBlockReader
	checkpointBlockLimit    uint32
	checkpointReleases      []uint64
	checkpointSuperseded    []uint64
	checkpointScratch       checkpointGraphScratch
	checkpointRepairDrain   bool

	wal                    *walJournal
	replies                *durableReplyStore
	superblocks            *superblockStore
	sessions               *sessionTable
	blocks                 *blockStore
	trailers               *trailerStore
	blockAllocator         *blockSpaceAllocator
	io                     *storageIOEngine
	frames                 *protocol.FramePool
	events                 *mpscRing[replicaEvent]
	notify                 chan struct{}
	pipeline               []pipelineEntry
	pipelineHead           uint32
	pipelineLen            uint32
	requestQueue           []queuedRequest
	requestHead            uint32
	requestLen             uint32
	duplicateReads         []duplicateRead
	repairReads            []repairRead
	replyRepair            replyRepairRuntime
	stage                  CommitStage
	stageGeneration        uint64
	exitViewBits           uint16
	exitViewAt             [ActiveMax]uint64
	abdicated              bool
	viewChangeStalled      bool
	viewChangeStarted      uint64
	lastTickMonotonic      uint64
	joins                  []joinRecord
	joinHeaders            []protocol.Header
	canonicalHeaders       []protocol.Header
	viewIO                 ioHandle
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
	getViewTarget          protocol.View
	repairViewValid        bool
	repairView             protocol.View
	repairViewCommit       protocol.Op
	repairViewHead         protocol.Op
	repairViewAncestor     protocol.Op
	repairViewPriorHead    protocol.Op
	repairViewRebuilt      bool
	repairViewWindow       bool
	repairViewPartial      bool
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
	submitMu           sync.RWMutex
	runState           atomic.Uint32
	loopMu             sync.RWMutex
	publishedSnapshot  atomic.Pointer[ReplicaSnapshot]
	stopOnce           sync.Once
	stop               chan struct{}
	done               chan struct{}
	shutdownStarted    atomic.Bool
	shutdownCtx        context.Context
	shutdownCancel     context.CancelCauseFunc
	shutdownCompletion SMCompletion
	shutdownGeneration uint64
	shutdownErr        error
	recoveryLifecycle  sync.Mutex
	recoveryCancel     context.CancelFunc
	recoveryWorkers    sync.WaitGroup
	fatalErr           error
}

func newReplica(config Config, dependencies Dependencies, initial replicaInitialState, wal *walJournal, replyStore *durableReplyStore, sessions *sessionTable, superblocks *superblockStore) (*Replica, error) {
	return newReplicaWithBlocks(config, dependencies, simhook.IOOptions{}, initial, wal, replyStore, sessions, superblocks, nil)
}

func newReplicaWithBlocks(config Config, dependencies Dependencies, ioOptions simhook.IOOptions, initial replicaInitialState, wal *walJournal, replyStore *durableReplyStore, sessions *sessionTable, superblocks *superblockStore, blocks *blockRuntime) (*Replica, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if dependencies.Storage == nil || dependencies.MessageBus == nil || dependencies.Clock == nil || dependencies.Entropy == nil {
		return nil, ErrInvalidConfiguration
	}
	if dependencies.StateMachine == nil || wal == nil || replyStore == nil || sessions == nil || superblocks == nil {
		return nil, ErrInvalidConfiguration
	}
	if err := validateStateMachineCapacities(config, dependencies.StateMachine); err != nil {
		return nil, err
	}
	capacities := dependencies.StateMachine.Capacities()
	random, err := newReplicaRandom(dependencies.Entropy)
	if err != nil {
		return nil, err
	}
	local, ok := config.Membership.LocalIndex()
	if !ok {
		return nil, ErrInvalidMembership
	}
	quorums, ok := QuorumsFor(config.Membership.ActiveCount, uint8(min(config.Cluster.ReplicationQuorumMax, uint64(^uint8(0)))))
	if !ok {
		return nil, ErrInvalidConfiguration
	}
	resolvedTimers := config.Process.resolvedTimers()
	repairBudget, err := newJournalRepairBudgetWithTimeout(config.Membership.ActiveCount, local, resolvedTimers.journalRepairTimeout)
	if err != nil {
		return nil, err
	}
	memberCount := config.Membership.ActiveCount + config.Membership.StandbyCount
	blockBudget, err := newBlockRepairBudgetWithTimeouts(memberCount, local, config.Process.RepairRequestsMax, resolvedTimers.blockRepairRetryAfter, resolvedTimers.blockRepairTimeout)
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
	events, err := newMPSCRing[replicaEvent](eventCapacity)
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
	ioEngine, err := newIOEngine(dependencies.Storage, requestCount, min(requestCount, uint32(4)), ioOptions.SynchronousIO, ioOptions.Controller)
	if err != nil {
		return nil, err
	}
	clockObserver, _ := dependencies.Clock.(ClockObserver)
	metrics := dependencies.Metrics
	if metrics == nil {
		metrics = &ReplicaMetrics{}
	}
	checkpointID, err := initial.Checkpoint.ID(config.Cluster.ChecksumAlgorithm)
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
	checkpointBitCapacity := uint64(0)
	if blocks.allocator != nil {
		checkpointBitCapacity = blocks.allocator.acquired.Len()
	}
	checkpointScratch, err := newCheckpointGraphScratch(
		int(capacities.CheckpointMax), checkpointBitCapacity, config.Cluster.BlockSize,
	)
	if err != nil {
		_ = ioEngine.Close(context.Background())
		return nil, err
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
	checkpointSession := initial.checkpointSession.data
	if checkpointSession == nil {
		checkpointSession = make([]byte, sessions.TrailerSize())
	}
	shutdownCtx, shutdownCancel := context.WithCancelCause(context.Background())
	replica := &Replica{
		config:               config,
		membership:           config.Membership,
		local:                local,
		quorums:              quorums,
		deps:                 dependencies,
		clockObserver:        clockObserver,
		logger:               dependencies.Logger,
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
		failureDetector:      newLivenessDetectorFromProcess(initialTime.Monotonic, config.Process),
		timers:               timers,
		random:               random,
		checkpointSession:    checkpointSession,
		checkpointSessionOp:  initial.checkpointSession.op,
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
		checkpointScratch:    checkpointScratch,
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
		shutdownCtx:          shutdownCtx,
		shutdownCancel:       shutdownCancel,
	}
	replica.seedScrubCatalog(initial.Checkpoint)
	if replica.blockAllocator != nil {
		if err := replica.catalogCurrentCheckpointTrailers(); err != nil {
			_ = ioEngine.Close(context.Background())
			return nil, err
		}
	}
	replica.refreshLocalReleaseReport()
	replica.maybeSelectUpgrade()
	if err := replica.checkInvariants(); err != nil {
		_ = ioEngine.Close(context.Background())
		return nil, err
	}
	if err := replica.initReplyRepair(); err != nil {
		_ = ioEngine.Close(context.Background())
		return nil, err
	}
	metrics.framePoolCapacity.Store(uint64(frames.Capacity()))
	metrics.ioCapacity.Store(uint64(len(ioEngine.slots)))
	replica.updateMetricGauges(initialTime.Monotonic)
	replica.accepting.Store(true)
	return replica, nil
}

// Snapshot returns a consistent point-in-time view of consensus status, view,
// commit progress, and durable checkpoint state. Safe for concurrent calls.
func (replica *Replica) Snapshot() ReplicaSnapshot {
	if replica.runState.Load() == 1 {
		if current := replica.publishedSnapshot.Load(); current != nil {
			return *current
		}
	}
	replica.loopMu.RLock()
	defer replica.loopMu.RUnlock()
	if replica.runState.Load() == 1 {
		return *replica.publishedSnapshot.Load()
	}
	return replica.computeSnapshot()
}

func (replica *Replica) publishSnapshot() {
	snapshot := replica.computeSnapshot()
	replica.publishedSnapshot.Store(&snapshot)
}

func (replica *Replica) computeSnapshot() ReplicaSnapshot {
	commitStage := CommitStageIdle
	prepareWritePending := false
	committing := false
	if replica.pipelineLen > 0 {
		entry := replica.pipelineEntry(0)
		commitStage = entry.stage
		prepareWritePending = entry.ioKind == ioWALAppend
		committing = entry.stage >= CommitStageCheckpointDurable && prepareOp(&entry.header) == replica.commitMin
	}
	return ReplicaSnapshot{
		Status: replica.status, View: replica.view, DurableView: replica.durableView, LogView: replica.logView,
		HeadOp: replica.headOp, CommitMin: replica.commitMin, CommitMax: replica.commitMax, Checkpoint: replica.checkpoint,
		PipelineLen: replica.pipelineLen, CommitStage: commitStage, PrepareWritePending: prepareWritePending,
		Committing: committing, Primary: replica.membership.Primary(replica.view),
	}
}

// DurableChecksum reports the durable prepare header checksum for op when op
// lies within the retained WAL window and is free of local storage faults.
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

// Submit validates frame source authentication and enqueues an inbound protocol
// frame onto the Replica's MPSC event queue. On a nil return, Replica assumes
// frame ownership and releases it once processed; on error, the caller retains
// ownership and must call Release. Safe for concurrent calls.
func (replica *Replica) Submit(frame *protocol.Frame) error {
	if !replica.accepting.Load() {
		return ErrReplicaClosed
	}
	replica.submitMu.RLock()
	defer replica.submitMu.RUnlock()
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

// Run executes the Replica event loop, driving Process and Tick until Close is
// called, ctx is canceled, or a fatal storage/invariant error occurs. Run must be
// invoked at most once per Replica.
func (replica *Replica) Run(ctx context.Context) (runErr error) {
	replica.loopMu.Lock()
	if replica.runState.Load() != 0 {
		replica.loopMu.Unlock()
		return ErrReplicaRunning
	}
	replica.publishSnapshot()
	if !replica.runState.CompareAndSwap(0, 1) {
		replica.loopMu.Unlock()
		return ErrReplicaRunning
	}
	replica.loopMu.Unlock()
	defer func() {
		replica.accepting.Store(false)
		replica.startShutdown()
		<-replica.done
		replica.loopMu.Lock()
		replica.publishSnapshot()
		replica.runState.Store(2)
		replica.loopMu.Unlock()
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
				replica.publishSnapshot()
			}
		}
	}
}

// Tick advances logical consensus, failure detection, and repair timers by one
// configured Process.Tick step. Intended for deterministic single-goroutine callers
// that do not invoke Run.
func (replica *Replica) Tick() error {
	if !replica.accepting.Load() {
		return ErrReplicaClosed
	}
	if !replica.stateSync.replayRunning {
		replica.handleTick(replica.deps.Clock.Now())
	}
	return replica.fatalErr
}

// Process drains up to limit queued inbound frames, I/O completions, and
// asynchronous StateMachine completions in deterministic order. Intended for manual
// stepping when Run is not active.
func (replica *Replica) Process(limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	defer func() {
		if replica.runState.Load() == 1 {
			replica.publishSnapshot()
		}
	}()
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
		var completion ioCompletion
		polled, pollErr := replica.io.Poll(&completion)
		if pollErr != nil {
			replica.fail(pollErr)
			return processed, pollErr
		}
		if polled {
			if replica.stateSync.stage == syncStageCancelingStorage {
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
			if replica.stateSync.stage != syncStageIdle || replica.stateSync.replayRunning {
				event.message.Release()
			} else if !replica.handleMessage(event.message) {
				event.message.Release()
			}
		case replicaEventSMCompletion:
			if replica.stateSync.stage == syncStageCancelingStorage || replica.stateSync.stage == syncStageUpdatingCheckpoint || replica.stateSync.replayRunning {
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
	if !replica.stateSyncAwaitingOpen() && replica.pendingView > replica.view && replica.viewIO == (ioHandle{}) && !replica.commitInFlight() {
		replica.resumePendingViewChange()
	}
	if replica.stateSync.stage == syncStageIdle && !replica.stateSync.replayRunning && !replica.stateSyncAwaitingOpen() {
		if replica.repairViewValid {
			replica.continueRecoveringView(replica.deps.Clock.Now().Monotonic)
		}
		replica.acknowledgeDurablePrepares()
	}
	if !replica.stateSync.replayRunning && (replica.stateSync.stage == syncStageIdle || replica.stateSync.stage == syncStageCancelingCommit) {
		replica.advanceCommit()
	}
	replica.dispatchStateSync()
	if processed > 0 && replica.deps.Clock != nil {
		replica.updateMetricGauges(replica.deps.Clock.Now().Monotonic)
	}
	if replica.fatalErr != nil {
		return processed, replica.fatalErr
	}
	if err := replica.checkInvariants(); err != nil {
		replica.fail(err)
		return processed, err
	}
	return processed, nil
}

// Close halts acceptance of new frames, resets the state machine, drains
// I/O and worker goroutines, and closes both StateMachine and Storage.
// Canceling ctx aborts waiting on pending reset or I/O without leaking
// shutdown goroutines while still releasing owned frames and closing resources.
func (replica *Replica) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	replica.accepting.Store(false)
	if replica.runState.CompareAndSwap(0, 2) {
		replica.startShutdown()
	} else {
		replica.stopOnce.Do(func() { close(replica.stop) })
	}
	stopCancel := context.AfterFunc(ctx, func() {
		if replica.shutdownCancel != nil {
			replica.shutdownCancel(ctx.Err())
		}
	})
	defer stopCancel()
	select {
	case <-replica.done:
		return replica.shutdownErr
	case <-ctx.Done():
		if replica.shutdownCancel != nil {
			replica.shutdownCancel(ctx.Err())
		}
		<-replica.done
		if replica.shutdownErr != nil {
			return replica.shutdownErr
		}
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
	defer func() {
		if replica.shutdownCancel != nil {
			replica.shutdownCancel(nil)
		}
		close(replica.done)
	}()
	replica.accepting.Store(false)
	replica.submitMu.Lock()
	replica.submitMu.Unlock()
	replica.recoveryLifecycle.Lock()
	if replica.recoveryCancel != nil {
		replica.recoveryCancel()
	}
	replica.recoveryLifecycle.Unlock()
	replica.recoveryWorkers.Wait()
	shutdownCtx := replica.shutdownCtx
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	replica.shutdownGeneration++
	generation := replica.shutdownGeneration
	replica.shutdownCompletion.prepare(generation, replica)
	reset, err := replica.deps.StateMachine.StartReset(&replica.shutdownCompletion)
	if err != nil {
		replica.shutdownCompletion.release(generation)
		replica.shutdownErr = errors.Join(ErrStateMachine, err)
	} else if reset.IsReady() {
		replica.shutdownCompletion.release(generation)
	} else {
		replica.waitForReset(shutdownCtx, generation)
	}
	if err := replica.io.Close(shutdownCtx); err != nil && replica.shutdownErr == nil {
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

func (replica *Replica) waitForReset(ctx context.Context, generation uint64) {
	for {
		if replica.drainShutdownEvents(generation) {
			return
		}
		select {
		case <-replica.notify:
		case <-replica.io.Ready():
		case <-ctx.Done():
			replica.shutdownCompletion.release(generation)
			if replica.shutdownErr == nil {
				replica.shutdownErr = context.Cause(ctx)
			}
			return
		}
	}
}

func (replica *Replica) drainShutdownEvents(generation uint64) bool {
	resetComplete := false
	var completion ioCompletion
	for {
		polled, pollErr := replica.io.Poll(&completion)
		if pollErr != nil {
			if replica.shutdownErr == nil {
				replica.shutdownErr = pollErr
			}
			break
		}
		if !polled {
			break
		}
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
	replica.releaseReplyRepair()
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

func (replica *Replica) transition(to Status, cause transitionCause) error {
	if !validStatusTransition(replica.status, to, cause) {
		return errStatusTransition
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

func (replica *Replica) durableHeadOp() protocol.Op {
	head := replica.commitMin
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if !entry.durable {
			break
		}
		head = max(head, prepareOp(&entry.header))
	}
	return head
}

func (replica *Replica) checkInvariants() error {
	return validateReplicaSnapshot(replica.computeSnapshot(), replica.config.Cluster, replica.local)
}

func (replica *Replica) isPrimary() bool {
	return replica.membership.Primary(replica.view) == replica.local
}

func (replica *Replica) fail(err error) {
	if replica.fatalErr == nil {
		replica.fatalErr = err
		replica.accepting.Store(false)
		if replica.logger != nil && replica.logger.Enabled(context.Background(), slog.LevelError) {
			replica.logger.LogAttrs(
				context.Background(),
				slog.LevelError,
				"replica fail-stop",
				slog.Any("error", err),
				slog.Uint64("view", uint64(replica.view)),
				slog.Uint64("head", uint64(replica.headOp)),
			)
		}
	}
}

func (replica *Replica) handleTick(sample TimeSample) {
	if replica.lastTickMonotonic != 0 && sample.Monotonic > replica.lastTickMonotonic {
		elapsed := sample.Monotonic - replica.lastTickMonotonic
		expected := uint64(replica.config.Process.Tick)
		if expected != 0 && elapsed > expected && replica.metrics != nil {
			replica.metrics.eventLoopLagNanos.Store(elapsed - expected)
		} else if replica.metrics != nil {
			replica.metrics.eventLoopLagNanos.Store(0)
		}
	}
	replica.lastTickMonotonic = sample.Monotonic
	replica.tickTimers(sample)
	replica.updateMetricGauges(sample.Monotonic)
}

func (replica *Replica) updateMetricGauges(now uint64) {
	metrics := replica.metrics
	if metrics == nil {
		return
	}
	metrics.status.Store(uint64(replica.status))
	metrics.view.Store(uint64(replica.view))
	metrics.logView.Store(uint64(replica.logView))
	metrics.headOp.Store(uint64(replica.headOp))
	metrics.commitMin.Store(uint64(replica.commitMin))
	metrics.commitMax.Store(uint64(replica.commitMax))
	metrics.checkpointOp.Store(uint64(replica.checkpoint.PrepareOp()))
	metrics.pipelineDepth.Store(uint64(replica.pipelineLen))
	metrics.stateSyncStage.Store(uint64(replica.stateSync.stage))
	metrics.faultDetectorEWMANanos.Store(replica.failureDetector.ewma)
	metrics.faultDetectorLevel.Store(uint64(replica.failureDetector.Level(now)))
	if replica.frames != nil {
		metrics.framePoolAvailable.Store(uint64(replica.frames.Available()))
	}
	if replica.io != nil {
		metrics.ioInFlight.Store(uint64(len(replica.io.slots) - replica.io.Available()))
	}
	blockDebt := uint64(0)
	for index := range replica.blockRepairTargets {
		state := replica.blockRepairTargets[index].state
		if state != 0 && state != blockRepairDurable {
			blockDebt++
		}
	}
	metrics.stateSyncBlockDebt.Store(blockDebt)
	replyDebt := uint64(0)
	for index := range replica.replyRepair.faults {
		if !replica.replyRepair.faults[index].IsZero() {
			replyDebt++
		}
	}
	metrics.stateSyncReplyDebt.Store(replyDebt)
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
