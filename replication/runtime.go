package replication

import (
	"sync/atomic"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type Message = protocol.Frame

type MessageBus interface {
	SendReplica(to protocol.ReplicaIndex, message *Message)
	SendClient(to protocol.ClientID, message *Message)
	BroadcastReplicas(message *Message)
}

type TimeSample struct {
	Wall         uint64
	Monotonic    uint64
	Synchronized bool
}

type Clock interface {
	Now() TimeSample
}

type ReplicaMetricSnapshot struct {
	FramesRejected             uint64
	RequestsDropped            uint64
	ClientForks                uint64
	PreparesCreated            uint64
	PreparesDurable            uint64
	PrepareAcks                uint64
	OperationsCommitted        uint64
	ViewChanges                uint64
	StorageFailures            uint64
	StaleCompletions           uint64
	EventBackpressure          uint64
	QuorumUnavailable          uint64
	ClockDisagreements         uint64
	StorageCorruptions         uint64
	RepairStalls               uint64
	IncompatibleConfigurations uint64
}

type ReplicaMetrics struct {
	framesRejected             atomic.Uint64
	requestsDropped            atomic.Uint64
	clientForks                atomic.Uint64
	preparesCreated            atomic.Uint64
	preparesDurable            atomic.Uint64
	prepareAcks                atomic.Uint64
	operationsCommitted        atomic.Uint64
	viewChanges                atomic.Uint64
	storageFailures            atomic.Uint64
	staleCompletions           atomic.Uint64
	eventBackpressure          atomic.Uint64
	quorumUnavailable          atomic.Uint64
	clockDisagreements         atomic.Uint64
	storageCorruptions         atomic.Uint64
	repairStalls               atomic.Uint64
	incompatibleConfigurations atomic.Uint64
}

func (metrics *ReplicaMetrics) Snapshot() ReplicaMetricSnapshot {
	return ReplicaMetricSnapshot{
		FramesRejected:             metrics.framesRejected.Load(),
		RequestsDropped:            metrics.requestsDropped.Load(),
		ClientForks:                metrics.clientForks.Load(),
		PreparesCreated:            metrics.preparesCreated.Load(),
		PreparesDurable:            metrics.preparesDurable.Load(),
		PrepareAcks:                metrics.prepareAcks.Load(),
		OperationsCommitted:        metrics.operationsCommitted.Load(),
		ViewChanges:                metrics.viewChanges.Load(),
		StorageFailures:            metrics.storageFailures.Load(),
		StaleCompletions:           metrics.staleCompletions.Load(),
		EventBackpressure:          metrics.eventBackpressure.Load(),
		QuorumUnavailable:          metrics.quorumUnavailable.Load(),
		ClockDisagreements:         metrics.clockDisagreements.Load(),
		StorageCorruptions:         metrics.storageCorruptions.Load(),
		RepairStalls:               metrics.repairStalls.Load(),
		IncompatibleConfigurations: metrics.incompatibleConfigurations.Load(),
	}
}
