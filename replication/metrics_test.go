package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"z.gfire.dev/supervsr/replication/internal/simhook"
	"z.gfire.dev/supervsr/replication/protocol"
)

func TestMetricsDistinguishOperationalFailureCategories(t *testing.T) {
	t.Run("primary_abdication_vs_quorum_unavailable_and_clock_disagreement", func(t *testing.T) {
		membership := Membership{
			Members:     [MembersMax]protocol.MemberID{{1}, {2}, {3}},
			ActiveCount: 3,
			LocalMember: protocol.MemberID{1},
		}
		frames, err := protocol.NewFramePool(4, 4096)
		if err != nil {
			t.Fatal(err)
		}
		metrics := &ReplicaMetrics{}
		bus := &captureBus{}
		replica := Replica{
			config:     Config{Group: protocol.GroupID{1}, Membership: membership},
			membership: membership,
			local:      0,
			quorums:    Quorums{ViewChange: 2},
			status:     StatusNormal,
			pipeline:   make([]pipelineEntry, 1),
			frames:     frames,
			metrics:    metrics,
			deps:       Dependencies{MessageBus: bus},
		}

		// Stalled pipeline head with synced clock -> PrimaryAbdications + QuorumUnavailable, no ClockDisagreements.
		replica.pipelineLen = 1
		replica.handleAbdicationTimeout(TimeSample{Wall: 1, Monotonic: uint64(10 * time.Second), Synchronized: true})
		snap := metrics.Snapshot()
		if snap.PrimaryAbdications != 1 || snap.QuorumUnavailable != 1 || snap.ClockDisagreements != 0 {
			t.Fatalf("quorum stall metrics: abdications=%d quorum=%d clock=%d", snap.PrimaryAbdications, snap.QuorumUnavailable, snap.ClockDisagreements)
		}

		// Empty pipeline with unsynchronized clock -> PrimaryAbdications + ClockDisagreements, no QuorumUnavailable increment.
		replica.abdicated = false
		replica.pipelineLen = 0
		replica.handleAbdicationTimeout(TimeSample{Wall: 2, Monotonic: uint64(20 * time.Second), Synchronized: false})
		snap = metrics.Snapshot()
		if snap.PrimaryAbdications != 2 || snap.QuorumUnavailable != 1 || snap.ClockDisagreements != 1 {
			t.Fatalf("clock desync metrics: abdications=%d quorum=%d clock=%d", snap.PrimaryAbdications, snap.QuorumUnavailable, snap.ClockDisagreements)
		}
	})

	t.Run("storage_corruption_and_wal_recovery_classifications", func(t *testing.T) {
		config, storage := threeReplicaFormat(t)
		layout, ok := deriveWALLayout(config.Cluster)
		if !ok {
			t.Fatal("invalid WAL layout")
		}
		// Corrupt slot 1 header in the header ring to trigger remote-repair faulty classification.
		storage.working[layout.HeaderBase+protocol.HeaderSize] ^= 0xff
		storage.durable = append(storage.durable[:0], storage.working...)
		metrics := &ReplicaMetrics{}
		machine := &testStateMachine{capacities: StateMachineCapacities{
			RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
			PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
		}}
		replica, err := Open(simhook.WithIOOptions(t.Context(), simhook.IOOptions{SynchronousIO: true}), config, Dependencies{
			Storage: storage, MessageBus: &captureBus{}, Clock: fixedClock{sample: TimeSample{Wall: 100, Monotonic: 10, Synchronized: true}},
			Entropy: bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), StateMachine: machine, Metrics: metrics,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer closeReplica(t, replica)
		snap := metrics.Snapshot()
		if snap.StorageCorruptions == 0 || snap.WALRecoveryRemoteRepair == 0 {
			t.Fatalf("expected corruption/remote repair metrics: corruptions=%d remoteRepair=%d", snap.StorageCorruptions, snap.WALRecoveryRemoteRepair)
		}
		if snap.WALRecoveryClean == 0 || snap.WALRecoveryCleanEmpty == 0 {
			t.Fatalf("expected WAL recovery clean classifications: clean=%d empty=%d", snap.WALRecoveryClean, snap.WALRecoveryCleanEmpty)
		}
		if snap.IncompatibleConfigurations != 0 || snap.PrimaryAbdications != 0 || snap.RepairStalls != 0 {
			t.Fatalf("unrelated counters incremented: %+v", snap)
		}
	})

	t.Run("repair_stall_and_peer_rotation", func(t *testing.T) {
		config, storage := threeReplicaFormat(t)
		metrics := &ReplicaMetrics{}
		machine := &testStateMachine{capacities: StateMachineCapacities{
			RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
			PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
		}}
		replica, err := Open(simhook.WithIOOptions(t.Context(), simhook.IOOptions{SynchronousIO: true}), config, Dependencies{
			Storage: storage, MessageBus: &captureBus{}, Clock: fixedClock{sample: TimeSample{Wall: 100, Monotonic: 10, Synchronized: true}},
			Entropy: bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), StateMachine: machine, Metrics: metrics,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer closeReplica(t, replica)

		if _, ok, err := replica.repairBudget.Reserve(1, 1, &replica.random); err != nil || !ok {
			t.Fatalf("reserve journal repair: ok=%t err=%v", ok, err)
		}
		replica.handleRepairTimeout(TimeSample{Wall: 100, Monotonic: uint64(time.Second), Synchronized: true})
		snap := metrics.Snapshot()
		if snap.RepairStalls == 0 || snap.RepairPeerRotations == 0 {
			t.Fatalf("expected repair stall and peer rotation: stalls=%d rotations=%d", snap.RepairStalls, snap.RepairPeerRotations)
		}
	})

	t.Run("incompatible_configuration", func(t *testing.T) {
		config, storage := threeReplicaFormat(t)
		config.Cluster.ChecksumAlgorithm = ChecksumWyhash128
		metrics := &ReplicaMetrics{}
		machine := &testStateMachine{capacities: StateMachineCapacities{
			RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
			PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
		}}
		if _, err := Open(t.Context(), config, Dependencies{
			Storage: storage, MessageBus: &captureBus{}, Clock: fixedClock{},
			Entropy: bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), StateMachine: machine, Metrics: metrics,
		}); !errors.Is(err, ErrIncompatibleConfiguration) {
			t.Fatalf("open error = %v, want ErrIncompatibleConfiguration", err)
		}
		snap := metrics.Snapshot()
		if snap.IncompatibleConfigurations != 1 {
			t.Fatalf("incompatible configurations = %d, want 1", snap.IncompatibleConfigurations)
		}
		if snap.StorageCorruptions != 0 || snap.QuorumUnavailable != 0 || snap.PrimaryAbdications != 0 {
			t.Fatalf("unrelated counters incremented: %+v", snap)
		}
	})

	t.Run("normal_gauges_rtt_duplicates_and_evictions", func(t *testing.T) {
		config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
		metrics := &ReplicaMetrics{}
		bus := &captureBus{}
		clock := &steppingClock{wall: 1_000, monotonic: uint64(time.Second), synchronized: true}
		machine := &testStateMachine{capacities: StateMachineCapacities{
			RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
			PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
		}}
		replica, err := newReplicaWithBlocks(config, Dependencies{
			Storage: storage, MessageBus: bus, Clock: clock,
			Entropy: bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), StateMachine: machine, Metrics: metrics,
		}, simhook.IOOptions{SynchronousIO: true}, initial, wal, replies, sessions, superblocks, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer closeReplica(t, replica)

		client := protocol.ClientID{7}
		reg := makeClientRequest(t, replica.frames, config.Group, client, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
		if err := replica.Submit(reg); err != nil {
			t.Fatal(err)
		}
		clock.monotonic += uint64(5 * time.Millisecond)
		if _, err := replica.Process(32); err != nil {
			t.Fatal(err)
		}
		// Duplicate register request triggers ClientDuplicates.
		dup := makeClientRequest(t, replica.frames, config.Group, client, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
		if err := replica.Submit(dup); err != nil {
			t.Fatal(err)
		}
		if _, err := replica.Process(32); err != nil {
			t.Fatal(err)
		}
		// Invalid request from unknown session triggers ClientEvictions.
		evict := makeClientRequest(t, replica.frames, config.Group, protocol.ClientID{99}, 1, 1, protocol.Checksum{1}, protocol.OperationNoop, nil)
		if err := replica.Submit(evict); err != nil {
			t.Fatal(err)
		}
		if _, err := replica.Process(32); err != nil {
			t.Fatal(err)
		}
		// Pong records PeerRTTNanos.
		var pong protocol.Header
		pong.Command = protocol.CommandPong
		binary.LittleEndian.PutUint64(pong.Fields[:8], clock.monotonic-uint64(2*time.Millisecond))
		binary.LittleEndian.PutUint64(pong.Fields[8:16], clock.wall)
		replica.handlePong(pong, clock.Now())

		// Tick with lag records EventLoopLagNanos and refreshes gauges.
		if err := replica.Tick(); err != nil {
			t.Fatal(err)
		}
		clock.monotonic += uint64(config.Process.Tick + 3*time.Millisecond)
		if err := replica.Tick(); err != nil {
			t.Fatal(err)
		}

		snap := metrics.Snapshot()
		if snap.WALWrites == 0 || snap.QuorumLatencySamples == 0 || snap.ClientDuplicates != 1 || snap.ClientEvictions != 1 {
			t.Fatalf("normal metrics mismatch: walWrites=%d quorumSamples=%d duplicates=%d evictions=%d",
				snap.WALWrites, snap.QuorumLatencySamples, snap.ClientDuplicates, snap.ClientEvictions)
		}
		if snap.PeerRTTNanos != uint64(2*time.Millisecond) {
			t.Fatalf("peer RTT = %d, want %d", snap.PeerRTTNanos, uint64(2*time.Millisecond))
		}
		if snap.EventLoopLagNanos != uint64(3*time.Millisecond) {
			t.Fatalf("event loop lag = %d, want %d", snap.EventLoopLagNanos, uint64(3*time.Millisecond))
		}
		if snap.FramePoolCapacity == 0 || snap.FramePoolAvailable == 0 || snap.IOCapacity == 0 || snap.HeadOp == 0 || snap.CommitMin == 0 {
			t.Fatalf("gauges not populated: %+v", snap)
		}
	})
}

type steppingClock struct {
	wall         uint64
	monotonic    uint64
	synchronized bool
}

func (clock *steppingClock) Now() TimeSample {
	return TimeSample{Wall: clock.wall, Monotonic: clock.monotonic, Synchronized: clock.synchronized}
}

func TestReplicaCloseDoesNotRequireLogger(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
	}}
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: &captureBus{}, Clock: fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), StateMachine: machine,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	replica.fail(ErrReplicaInvariant)
	if err := replica.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
