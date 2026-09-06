package sim

import (
	"errors"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var ErrStaleIO = errors.New("simulation: IO event belongs to an old replica incarnation")

type ScheduledIO struct {
	Replica     protocol.ReplicaIndex
	Incarnation uint64
	Operation   replication.IOEvent
}

type ResourceUsage struct {
	ClientProcesses      int
	ClientCapacity       int
	Packets              int
	PacketCapacity       int
	IOPending            int
	IOCapacity           int
	IOInUse              int
	DelayedWrites        int
	DelayedWriteCapacity int
}

// PendingIO observes controller work without advancing virtual time or consuming faults.
func (cluster *Cluster) PendingIO(destination []ScheduledIO) int {
	count := 0
	for index := range cluster.nodes {
		node := &cluster.nodes[index]
		if node.replica == nil || node.controller == nil {
			continue
		}
		available := node.controller.Pending(node.ioEvents)
		for _, event := range node.ioEvents[:available] {
			if count == len(destination) {
				return count
			}
			destination[count] = ScheduledIO{Replica: protocol.ReplicaIndex(index), Incarnation: node.generation, Operation: event}
			count++
		}
	}
	return count
}

func (cluster *Cluster) AdvanceIO(event ScheduledIO) error {
	if cluster.closed {
		return replication.ErrReplicaClosed
	}
	if int(event.Replica) >= len(cluster.nodes) {
		return replication.ErrInvalidConfiguration
	}
	node := &cluster.nodes[event.Replica]
	if node.replica == nil || node.controller == nil || node.generation != event.Incarnation {
		return ErrStaleIO
	}
	if err := node.controller.Advance(event.Operation.Handle); err != nil {
		return err
	}
	return cluster.CheckInvariants()
}

// Resources reports owned simulator resources; IO counts cover controlled engines only.
func (cluster *Cluster) Resources() ResourceUsage {
	usage := ResourceUsage{
		ClientProcesses: len(cluster.clients), ClientCapacity: int(cluster.config.ClientProcessesMax),
		Packets: cluster.network.Pending(), PacketCapacity: cluster.network.maximum,
		DelayedWriteCapacity: len(cluster.stores) * DelayedWritesMax,
	}
	for index, storage := range cluster.stores {
		usage.DelayedWrites += storage.PendingWrites()
		if controller := cluster.nodes[index].controller; controller != nil {
			usage.IOPending += controller.PendingCount()
			usage.IOCapacity += controller.Capacity()
			usage.IOInUse += controller.Used()
		}
	}
	return usage
}
