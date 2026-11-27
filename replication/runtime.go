package replication

import (
	"sync/atomic"

	"z.gfire.dev/supervsr/replication/protocol"
)

type Message = protocol.Frame

// MessageBus transports encoded protocol frames between replicas and clients.
// Frame buffers are pool-owned and released when Send methods return; asynchronous
// implementations must either copy frame.Bytes() synchronously or call frame.Retain
// before returning and frame.Release after transmission completes.
type MessageBus interface {
	SendReplica(to protocol.ReplicaIndex, message *Message)
	SendClient(to protocol.ClientID, message *Message)
	BroadcastReplicas(message *Message)
}

// TimeSample supplies synchronized wall-clock and monotonic timestamps in nanoseconds.
type TimeSample struct {
	Wall         uint64
	Monotonic    uint64
	Synchronized bool
}

// Clock provides current wall and monotonic time samples to replicas and clients.
type Clock interface {
	Now() TimeSample
}

// ClockObserver is an optional Clock extension implemented by synchronizers
// (such as ClockSynchronizer) that consume peer Pong timing samples.
// Open binds this observer once when Dependencies.Clock implements it.
type ClockObserver interface {
	Observe(source protocol.ReplicaIndex, pingMonotonic, peerWall, receiveMonotonic uint64) error
}

type ReplicaMetricSnapshot struct {
	// Consensus state gauges (bounded scalar values, no high-cardinality labels).
	Status       uint64
	View         uint64
	LogView      uint64
	HeadOp       uint64
	CommitMin    uint64
	CommitMax    uint64
	CheckpointOp uint64

	// Pipeline and quorum latency.
	PipelineDepth           uint64
	QuorumLatencyNanos      uint64
	QuorumLatencySamples    uint64
	QuorumLatencyTotalNanos uint64

	// Frame, admission, and client session counters.
	FramesRejected   uint64
	RequestsDropped  uint64
	ClientDuplicates uint64
	ClientForks      uint64
	ClientEvictions  uint64
	ReplyRepairs     uint64

	// Normal replication and WAL classification counters.
	PreparesCreated         uint64
	PreparesDurable         uint64
	PrepareAcks             uint64
	OperationsCommitted     uint64
	WALWrites               uint64
	WALRecoveryClean        uint64
	WALRecoveryCleanEmpty   uint64
	WALRecoveryLocalRepair  uint64
	WALRecoveryTruncated    uint64
	WALRecoveryRemoteRepair uint64

	// View change and primary abdication metrics.
	ViewChanges             uint64
	ViewChangeDurationNanos uint64
	ViewChangeTruncationOp  uint64
	PrimaryAbdications      uint64

	// Repair metrics.
	RepairRequests      uint64
	RepairResponses     uint64
	RepairBytes         uint64
	RepairStalls        uint64
	RepairPeerRotations uint64

	// State synchronization metrics.
	StateSyncStage         uint64
	StateSyncBlockDebt     uint64
	StateSyncReplyDebt     uint64
	StateSyncDurationNanos uint64

	// Failure detector, RTT, and clock metrics.
	FaultDetectorEWMANanos uint64
	FaultDetectorLevel     uint64
	PeerRTTNanos           uint64
	ClockDisagreements     uint64

	// Pool availability, backpressure, event-loop lag, I/O concurrency, and faults.
	FramePoolAvailable         uint64
	FramePoolCapacity          uint64
	EventBackpressure          uint64
	IOBackpressure             uint64
	FramePoolBackpressure      uint64
	EventLoopLagNanos          uint64
	IOInFlight                 uint64
	IOCapacity                 uint64
	StorageFailures            uint64
	StaleCompletions           uint64
	QuorumUnavailable          uint64
	StorageCorruptions         uint64
	IncompatibleConfigurations uint64
}

// ReplicaMetrics holds bounded-cardinality atomic telemetry exported via Snapshot.
type ReplicaMetrics struct {
	status       atomic.Uint64
	view         atomic.Uint64
	logView      atomic.Uint64
	headOp       atomic.Uint64
	commitMin    atomic.Uint64
	commitMax    atomic.Uint64
	checkpointOp atomic.Uint64

	pipelineDepth           atomic.Uint64
	quorumLatencyNanos      atomic.Uint64
	quorumLatencySamples    atomic.Uint64
	quorumLatencyTotalNanos atomic.Uint64

	framesRejected   atomic.Uint64
	requestsDropped  atomic.Uint64
	clientDuplicates atomic.Uint64
	clientForks      atomic.Uint64
	clientEvictions  atomic.Uint64
	replyRepairs     atomic.Uint64

	preparesCreated         atomic.Uint64
	preparesDurable         atomic.Uint64
	prepareAcks             atomic.Uint64
	operationsCommitted     atomic.Uint64
	walWrites               atomic.Uint64
	walRecoveryClean        atomic.Uint64
	walRecoveryCleanEmpty   atomic.Uint64
	walRecoveryLocalRepair  atomic.Uint64
	walRecoveryTruncated    atomic.Uint64
	walRecoveryRemoteRepair atomic.Uint64

	viewChanges             atomic.Uint64
	viewChangeDurationNanos atomic.Uint64
	viewChangeTruncationOp  atomic.Uint64
	primaryAbdications      atomic.Uint64

	repairRequests      atomic.Uint64
	repairResponses     atomic.Uint64
	repairBytes         atomic.Uint64
	repairStalls        atomic.Uint64
	repairPeerRotations atomic.Uint64

	stateSyncStage         atomic.Uint64
	stateSyncBlockDebt     atomic.Uint64
	stateSyncReplyDebt     atomic.Uint64
	stateSyncDurationNanos atomic.Uint64

	faultDetectorEWMANanos atomic.Uint64
	faultDetectorLevel     atomic.Uint64
	peerRTTNanos           atomic.Uint64
	clockDisagreements     atomic.Uint64

	framePoolAvailable         atomic.Uint64
	framePoolCapacity          atomic.Uint64
	eventBackpressure          atomic.Uint64
	ioBackpressure             atomic.Uint64
	framePoolBackpressure      atomic.Uint64
	eventLoopLagNanos          atomic.Uint64
	ioInFlight                 atomic.Uint64
	ioCapacity                 atomic.Uint64
	storageFailures            atomic.Uint64
	staleCompletions           atomic.Uint64
	quorumUnavailable          atomic.Uint64
	storageCorruptions         atomic.Uint64
	incompatibleConfigurations atomic.Uint64
}

func (metrics *ReplicaMetrics) Snapshot() ReplicaMetricSnapshot {
	return ReplicaMetricSnapshot{
		Status:                     metrics.status.Load(),
		View:                       metrics.view.Load(),
		LogView:                    metrics.logView.Load(),
		HeadOp:                     metrics.headOp.Load(),
		CommitMin:                  metrics.commitMin.Load(),
		CommitMax:                  metrics.commitMax.Load(),
		CheckpointOp:               metrics.checkpointOp.Load(),
		PipelineDepth:              metrics.pipelineDepth.Load(),
		QuorumLatencyNanos:         metrics.quorumLatencyNanos.Load(),
		QuorumLatencySamples:       metrics.quorumLatencySamples.Load(),
		QuorumLatencyTotalNanos:    metrics.quorumLatencyTotalNanos.Load(),
		FramesRejected:             metrics.framesRejected.Load(),
		RequestsDropped:            metrics.requestsDropped.Load(),
		ClientDuplicates:           metrics.clientDuplicates.Load(),
		ClientForks:                metrics.clientForks.Load(),
		ClientEvictions:            metrics.clientEvictions.Load(),
		ReplyRepairs:               metrics.replyRepairs.Load(),
		PreparesCreated:            metrics.preparesCreated.Load(),
		PreparesDurable:            metrics.preparesDurable.Load(),
		PrepareAcks:                metrics.prepareAcks.Load(),
		OperationsCommitted:        metrics.operationsCommitted.Load(),
		WALWrites:                  metrics.walWrites.Load(),
		WALRecoveryClean:           metrics.walRecoveryClean.Load(),
		WALRecoveryCleanEmpty:      metrics.walRecoveryCleanEmpty.Load(),
		WALRecoveryLocalRepair:     metrics.walRecoveryLocalRepair.Load(),
		WALRecoveryTruncated:       metrics.walRecoveryTruncated.Load(),
		WALRecoveryRemoteRepair:    metrics.walRecoveryRemoteRepair.Load(),
		ViewChanges:                metrics.viewChanges.Load(),
		ViewChangeDurationNanos:    metrics.viewChangeDurationNanos.Load(),
		ViewChangeTruncationOp:     metrics.viewChangeTruncationOp.Load(),
		PrimaryAbdications:         metrics.primaryAbdications.Load(),
		RepairRequests:             metrics.repairRequests.Load(),
		RepairResponses:            metrics.repairResponses.Load(),
		RepairBytes:                metrics.repairBytes.Load(),
		RepairStalls:               metrics.repairStalls.Load(),
		RepairPeerRotations:        metrics.repairPeerRotations.Load(),
		StateSyncStage:             metrics.stateSyncStage.Load(),
		StateSyncBlockDebt:         metrics.stateSyncBlockDebt.Load(),
		StateSyncReplyDebt:         metrics.stateSyncReplyDebt.Load(),
		StateSyncDurationNanos:     metrics.stateSyncDurationNanos.Load(),
		FaultDetectorEWMANanos:     metrics.faultDetectorEWMANanos.Load(),
		FaultDetectorLevel:         metrics.faultDetectorLevel.Load(),
		PeerRTTNanos:               metrics.peerRTTNanos.Load(),
		ClockDisagreements:         metrics.clockDisagreements.Load(),
		FramePoolAvailable:         metrics.framePoolAvailable.Load(),
		FramePoolCapacity:          metrics.framePoolCapacity.Load(),
		EventBackpressure:          metrics.eventBackpressure.Load(),
		IOBackpressure:             metrics.ioBackpressure.Load(),
		FramePoolBackpressure:      metrics.framePoolBackpressure.Load(),
		EventLoopLagNanos:          metrics.eventLoopLagNanos.Load(),
		IOInFlight:                 metrics.ioInFlight.Load(),
		IOCapacity:                 metrics.ioCapacity.Load(),
		StorageFailures:            metrics.storageFailures.Load(),
		StaleCompletions:           metrics.staleCompletions.Load(),
		QuorumUnavailable:          metrics.quorumUnavailable.Load(),
		StorageCorruptions:         metrics.storageCorruptions.Load(),
		IncompatibleConfigurations: metrics.incompatibleConfigurations.Load(),
	}
}
