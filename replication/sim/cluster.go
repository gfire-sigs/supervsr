package sim

import (
	"bytes"
	"context"
	"fmt"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type MachineFactory func(protocol.ReplicaIndex) replication.StateMachine

type Config struct {
	Group          protocol.GroupID
	ActiveCount    uint8
	StandbyCount   uint8
	CurrentRelease protocol.Release
	Cluster        replication.ClusterConfig
	Process        replication.ProcessConfig
	MaximumPackets uint32
	WorkLimit      int
}

func DefaultConfig(activeCount uint8) Config {
	cluster := replication.DefaultClusterConfig()
	cluster.ClientsMax = 4
	cluster.PipelineMax = 4
	cluster.ViewChangeHeadersSuffixMax = 5
	cluster.JournalSlots = 128
	cluster.MessageSizeMax = 4 << 10
	cluster.ApplicationBatchSizeMax = cluster.MessageSizeMax - protocol.HeaderSize
	cluster.ApplicationReplySizeMax = cluster.MessageSizeMax - protocol.HeaderSize
	cluster.BlockSize = 4 << 10
	cluster.CompactionOps = 16
	return Config{
		Group: protocol.GroupID{1}, ActiveCount: activeCount, CurrentRelease: 1,
		Cluster: cluster, Process: replication.DefaultProcessConfig(), MaximumPackets: 4096, WorkLimit: 100_000,
	}
}

type NodeError struct {
	Index protocol.ReplicaIndex
	Err   error
}

func (err *NodeError) Error() string {
	return fmt.Sprintf("simulation: replica %d: %v", err.Index, err.Err)
}

func (err *NodeError) Unwrap() error {
	return err.Err
}

type node struct {
	config     replication.Config
	replica    *replication.Replica
	machine    replication.StateMachine
	metrics    *replication.ReplicaMetrics
	generation uint64
}

type Cluster struct {
	config       Config
	quorums      replication.Quorums
	clock        *Clock
	memberClocks []*Clock
	network      *Network
	factory      MachineFactory
	members      [replication.MembersMax]protocol.MemberID
	stores       []*Storage
	nodes        []node
	clients      []*replication.Client
	closed       bool
	invariants   invariantState
}

func NewCluster(ctx context.Context, config Config, factory MachineFactory) (*Cluster, error) {
	memberCount := config.ActiveCount + config.StandbyCount
	if memberCount == 0 || config.MaximumPackets == 0 || config.WorkLimit <= 0 {
		return nil, replication.ErrInvalidConfiguration
	}
	quorumLimit := uint8(min(config.Cluster.ReplicationQuorumMax, uint64(^uint8(0))))
	quorums, ok := replication.QuorumsFor(config.ActiveCount, quorumLimit)
	if !ok {
		return nil, replication.ErrInvalidConfiguration
	}
	network, err := NewNetwork(memberCount, uint32(config.Cluster.MessageSizeMax), config.MaximumPackets)
	if err != nil {
		return nil, err
	}
	cluster := &Cluster{
		config: config, clock: NewClock(), network: network, factory: factory, quorums: quorums,
		memberClocks: make([]*Clock, memberCount), stores: make([]*Storage, memberCount), nodes: make([]node, memberCount),
	}
	for index := range memberCount {
		cluster.memberClocks[index] = NewClock()
		cluster.members[index][15] = index + 1
	}
	if cluster.factory == nil {
		capacities := replication.StateMachineCapacities{
			RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
			PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
		}
		cluster.factory = func(protocol.ReplicaIndex) replication.StateMachine { return NewMachine(capacities) }
	}
	for index := range memberCount {
		membership := replication.Membership{
			Members: cluster.members, ActiveCount: config.ActiveCount, StandbyCount: config.StandbyCount, LocalMember: cluster.members[index],
		}
		replicaConfig := replication.Config{
			Cluster: config.Cluster, Process: config.Process, Membership: membership,
			Group: config.Group, CurrentRelease: config.CurrentRelease, ClientReleaseMin: config.CurrentRelease,
		}
		if err := replicaConfig.Validate(); err != nil {
			return nil, err
		}
		storage := NewStorage()
		if err := replication.Format(ctx, replication.FormatConfig{
			Group: config.Group, Membership: membership, Cluster: config.Cluster, CurrentRelease: config.CurrentRelease,
		}, replication.FormatDependencies{Storage: storage}); err != nil {
			return nil, err
		}
		cluster.stores[index] = storage
		cluster.nodes[index].config = replicaConfig
		if err := cluster.openNode(ctx, protocol.ReplicaIndex(index)); err != nil {
			_ = cluster.Close(context.Background())
			return nil, err
		}
	}
	if err := cluster.CheckInvariants(); err != nil {
		_ = cluster.Close(context.Background())
		return nil, err
	}
	return cluster, nil
}

func (cluster *Cluster) Clock() *Clock {
	return cluster.clock
}

func (cluster *Cluster) MemberClock(index protocol.ReplicaIndex) *Clock {
	if int(index) >= len(cluster.memberClocks) {
		return nil
	}
	return cluster.memberClocks[index]
}

func (cluster *Cluster) SetAllClocksSynchronized(synchronized bool) {
	cluster.clock.SetSynchronized(synchronized)
	for _, clock := range cluster.memberClocks {
		clock.SetSynchronized(synchronized)
	}
}

func (cluster *Cluster) Network() *Network {
	return cluster.network
}

func (cluster *Cluster) Storage(index protocol.ReplicaIndex) *Storage {
	if int(index) >= len(cluster.stores) {
		return nil
	}
	return cluster.stores[index]
}

func (cluster *Cluster) Snapshot(index protocol.ReplicaIndex) (replication.ReplicaSnapshot, bool) {
	if int(index) >= len(cluster.nodes) || cluster.nodes[index].replica == nil {
		return replication.ReplicaSnapshot{}, false
	}
	return cluster.nodes[index].replica.Snapshot(), true
}

func (cluster *Cluster) Machine(index protocol.ReplicaIndex) (replication.StateMachine, bool) {
	if int(index) >= len(cluster.nodes) || cluster.nodes[index].machine == nil {
		return nil, false
	}
	return cluster.nodes[index].machine, true
}

func (cluster *Cluster) Metrics(index protocol.ReplicaIndex) (replication.ReplicaMetricSnapshot, bool) {
	if int(index) >= len(cluster.nodes) || cluster.nodes[index].metrics == nil {
		return replication.ReplicaMetricSnapshot{}, false
	}
	return cluster.nodes[index].metrics.Snapshot(), true
}

func (cluster *Cluster) AddClient(id protocol.ClientID, events replication.ClientEvents) (*replication.Client, error) {
	if uint64(len(cluster.clients)) >= cluster.config.Cluster.ClientsMax {
		return nil, replication.ErrInvalidConfiguration
	}
	if cluster.closed {
		return nil, replication.ErrReplicaClosed
	}
	observer := &observedClientEvents{cluster: cluster, client: id, target: events}
	client, err := replication.NewClient(replication.ClientConfig{
		Group: cluster.config.Group, ID: id, Release: cluster.config.CurrentRelease, ActiveCount: cluster.config.ActiveCount,
		MessageSizeMax: uint32(cluster.config.Cluster.MessageSizeMax), Process: cluster.config.Process,
	}, cluster.network.ClientBus(), cluster.clock, bytes.NewReader(id[:8]), observer)
	if err != nil {
		return nil, err
	}
	if err := cluster.network.RegisterClient(id, client.HandleFrame); err != nil {
		_ = client.Close()
		return nil, err
	}
	cluster.clients = append(cluster.clients, client)
	return client, nil
}

func (cluster *Cluster) Step() error {
	if cluster.closed {
		return replication.ErrReplicaClosed
	}
	cluster.clock.Advance(cluster.config.Process.Tick)
	for _, clock := range cluster.memberClocks {
		clock.Advance(cluster.config.Process.Tick)
	}
	cluster.network.Advance()
	if err := cluster.CheckInvariants(); err != nil {
		return err
	}
	for _, client := range cluster.clients {
		if err := client.Tick(); err != nil {
			return err
		}
		if err := cluster.CheckInvariants(); err != nil {
			return err
		}
		if err := cluster.settle(); err != nil {
			return err
		}
	}
	for index := range cluster.nodes {
		replica := cluster.nodes[index].replica
		if replica == nil {
			continue
		}
		if err := replica.Tick(); err != nil {
			return &NodeError{Index: protocol.ReplicaIndex(index), Err: err}
		}
		if err := cluster.CheckInvariants(); err != nil {
			return err
		}
		if err := cluster.settle(); err != nil {
			return err
		}
	}
	if err := cluster.settle(); err != nil {
		return err
	}
	return cluster.CheckInvariants()
}

func (cluster *Cluster) Run(steps int) error {
	for range steps {
		if err := cluster.Step(); err != nil {
			return err
		}
	}
	return nil
}

func (cluster *Cluster) Crash(ctx context.Context, index protocol.ReplicaIndex) error {
	if int(index) >= len(cluster.nodes) || cluster.nodes[index].replica == nil {
		return replication.ErrInvalidConfiguration
	}
	node := &cluster.nodes[index]
	cluster.network.UnregisterReplica(index)
	_ = node.replica.Close(ctx)
	node.replica = nil
	node.machine = nil
	cluster.stores[index].Crash()
	return cluster.CheckInvariants()
}

func (cluster *Cluster) Restart(ctx context.Context, index protocol.ReplicaIndex) error {
	if int(index) >= len(cluster.nodes) || cluster.nodes[index].replica != nil {
		return replication.ErrInvalidConfiguration
	}
	if err := cluster.openNode(ctx, index); err != nil {
		return err
	}
	return cluster.CheckInvariants()
}

func (cluster *Cluster) Close(ctx context.Context) error {
	if cluster.closed {
		return nil
	}
	cluster.closed = true
	var first error
	for _, client := range cluster.clients {
		if err := client.Close(); err != nil && first == nil {
			first = err
		}
	}
	for index := range cluster.nodes {
		node := &cluster.nodes[index]
		if node.replica == nil {
			continue
		}
		cluster.network.UnregisterReplica(protocol.ReplicaIndex(index))
		if err := node.replica.Close(ctx); err != nil && first == nil {
			first = err
		}
		node.replica = nil
		node.machine = nil
	}
	return first
}

func (cluster *Cluster) openNode(ctx context.Context, index protocol.ReplicaIndex) error {
	node := &cluster.nodes[index]
	node.generation++
	machine := cluster.factory(index)
	if machine == nil {
		return replication.ErrInvalidConfiguration
	}
	var seed [8]byte
	seed[0] = byte(index) + 1
	seed[7] = byte(node.generation)
	metrics := &replication.ReplicaMetrics{}
	replica, err := replication.Open(ctx, node.config, replication.Dependencies{
		Storage: cluster.stores[index], MessageBus: cluster.network.ReplicaBus(index), Clock: cluster.memberClocks[index],
		Entropy: bytes.NewReader(seed[:]), StateMachine: machine, Metrics: metrics, SynchronousIO: true,
	})
	if err != nil {
		return err
	}
	node.replica = replica
	node.machine = machine
	node.metrics = metrics
	if err := cluster.network.RegisterReplica(index, replica.Submit); err != nil {
		_ = replica.Close(context.Background())
		node.replica = nil
		node.machine = nil
		return err
	}
	return nil
}

func (cluster *Cluster) settle() error {
	idleRounds := 0
	work := 0
	for work < cluster.config.WorkLimit {
		delivered, err := cluster.network.DeliverReady()
		if err != nil {
			return err
		}
		if delivered != 0 {
			if err := cluster.CheckInvariants(); err != nil {
				return err
			}
		}
		processed := 0
		for index := range cluster.nodes {
			replica := cluster.nodes[index].replica
			if replica == nil {
				continue
			}
			count, err := replica.Process(64)
			if err != nil {
				return &NodeError{Index: protocol.ReplicaIndex(index), Err: err}
			}
			processed += count
			if count != 0 {
				if err := cluster.CheckInvariants(); err != nil {
					return err
				}
			}
		}
		work += delivered + processed + 1
		if delivered == 0 && processed == 0 {
			idleRounds++
			if idleRounds == 2 {
				return cluster.CheckInvariants()
			}
		} else {
			idleRounds = 0
		}
	}
	return ErrNetworkBackpressure
}
