package sim

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type ledgerClient struct {
	world         *ledgerWorld
	id            protocol.ClientID
	client        *replication.Client
	pending       uint64
	registered    bool
	evicted       bool
	allowEviction bool
}

func (client *ledgerClient) Reply(reply replication.ClientReply) {
	if reply.Operation == protocol.OperationRegister {
		client.registered = true
		return
	}
	if client.pending == 0 {
		client.world.fail("unexpected successful reply for client %x", client.id)
	}
	client.world.check(client.world.history.Complete(client.pending, reply.Body))
	client.pending = 0
}

func (client *ledgerClient) Evicted(reason protocol.EvictionReason) {
	client.evicted = true
	if !client.allowEviction {
		client.world.fail("client %x evicted: %v", client.id, reason)
	}
}

type ledgerWorld struct {
	t             *testing.T
	cluster       *Cluster
	history       *History
	random        *rand.Rand
	seed          uint64
	steps         int
	phase         string
	events        []ScheduledIO
	lastIO        ScheduledIO
	trace         protocol.Checksum
	maxCommit     protocol.Op
	maxCheckpoint protocol.Op
	expectedFault *protocol.ReplicaIndex
	faults        int
}

func newLedgerWorld(t *testing.T, seed uint64, config Config) *ledgerWorld {
	t.Helper()
	config.ControlledIO = true
	config.BlockValidator = LedgerValidator{}
	config.Cluster.JournalSlots = 64
	base, ok := config.Cluster.BlockBase()
	if !ok {
		t.Fatal("invalid block base")
	}
	config.Process.StorageSizeLimit = base + 64*config.Cluster.BlockSize
	cluster, err := NewCluster(t.Context(), config, func(protocol.ReplicaIndex) replication.StateMachine {
		machine, err := NewLedgerMachine(config.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		return machine
	})
	if err != nil {
		t.Fatal(err)
	}
	history, err := NewHistory(16)
	if err != nil {
		t.Fatal(err)
	}
	world := &ledgerWorld{t: t, cluster: cluster, history: history, seed: seed, phase: "open",
		random: rand.New(rand.NewPCG(seed, seed^0x6a09e667f3bcc909)),
		events: make([]ScheduledIO, cluster.Resources().IOCapacity),
	}
	t.Cleanup(func() {
		if err := cluster.Close(context.Background()); err != nil && !t.Failed() {
			t.Error(err)
		}
	})
	world.until(20_000, func() bool { return world.agree() })
	return world
}

func (world *ledgerWorld) fail(format string, args ...any) {
	world.t.Helper()
	world.t.Fatalf("seed=%d phase=%s step=%d resources=%+v states=%v lastIO=%+v: %s", world.seed, world.phase, world.steps,
		world.cluster.Resources(), compactMemberSnapshots(world.cluster, uint8(len(world.cluster.nodes))), world.lastIO, fmt.Sprintf(format, args...))
}

func (world *ledgerWorld) check(err error) {
	world.t.Helper()
	if err != nil {
		world.fail("%v", err)
	}
}

func (world *ledgerWorld) advance(event ScheduledIO) {
	world.lastIO = event
	world.check(world.cluster.AdvanceIO(event))
	var record [72]byte
	copy(record[:16], world.trace[:])
	binary.LittleEndian.PutUint64(record[16:24], uint64(world.steps))
	record[24], record[25], record[26] = byte(event.Replica), byte(event.Operation.Kind), byte(event.Operation.Phase)
	binary.LittleEndian.PutUint64(record[32:40], event.Incarnation)
	binary.LittleEndian.PutUint32(record[40:44], event.Operation.Handle.Index)
	binary.LittleEndian.PutUint64(record[48:56], event.Operation.Handle.Generation)
	binary.LittleEndian.PutUint64(record[56:64], event.Operation.Offset)
	binary.LittleEndian.PutUint64(record[64:72], event.Operation.Size)
	world.trace = protocol.ChecksumBytes(record[:])
}

func (world *ledgerWorld) step() {
	world.steps++
	if err := world.cluster.Step(); err != nil {
		var nodeError *NodeError
		if world.expectedFault == nil || !errors.As(err, &nodeError) || nodeError.Index != *world.expectedFault || !errors.Is(err, ErrInjectedFault) {
			world.fail("unexpected event error: %v", err)
		}
		world.check(world.cluster.Crash(world.t.Context(), nodeError.Index))
		world.expectedFault = nil
		world.faults++
	}
	for range 32 {
		count := world.cluster.PendingIO(world.events)
		if count == 0 {
			break
		}
		event := world.events[world.random.IntN(count)]
		world.advance(event)
	}
	usage := world.cluster.Resources()
	if usage.Packets > usage.PacketCapacity || usage.ClientProcesses > usage.ClientCapacity || usage.IOInUse > usage.IOCapacity || usage.DelayedWrites > usage.DelayedWriteCapacity {
		world.fail("resource capacity exceeded")
	}
	for index := range world.cluster.nodes {
		state, live := world.cluster.Snapshot(protocol.ReplicaIndex(index))
		if live {
			world.maxCommit = max(world.maxCommit, state.CommitMin)
			world.maxCheckpoint = max(world.maxCheckpoint, state.Checkpoint.PrepareOp())
		}
	}
}

func (world *ledgerWorld) until(limit int, reached func() bool) {
	for range limit {
		if reached() {
			return
		}
		world.step()
	}
	world.fail("condition not reached after %d steps", limit)
}

func (world *ledgerWorld) agree() bool {
	return configuredMembersAgree(world.cluster, uint8(len(world.cluster.nodes)))
}

func (world *ledgerWorld) addClient(id uint64) *ledgerClient {
	client := &ledgerClient{world: world}
	binary.LittleEndian.PutUint64(client.id[:8], id)
	var err error
	client.client, err = world.cluster.AddClient(client.id, client)
	world.check(err)
	world.check(client.client.Register())
	world.until(20_000, func() bool { return client.registered })
	return client
}

func (world *ledgerWorld) submit(client *ledgerClient) {
	if client.pending != 0 {
		world.fail("client already has an outstanding logical request")
	}
	token := world.history.Submitted() + 1
	world.check(world.history.Submit(token))
	client.pending = token
	body := EncodeLedgerRequest(token)
	world.check(client.client.Submit(LedgerIncrement, body[:]))
}

func (world *ledgerWorld) increments(count int, clients ...*ledgerClient) {
	for sent := 0; sent < count; {
		for _, client := range clients {
			if sent == count {
				break
			}
			world.submit(client)
			sent++
		}
		world.until(20_000, func() bool { return world.history.PendingCount() == 0 })
	}
}

func (world *ledgerWorld) primary() protocol.ReplicaIndex {
	for index := range world.cluster.config.ActiveCount {
		state, live := world.cluster.Snapshot(protocol.ReplicaIndex(index))
		if live && state.Status == replication.StatusNormal && state.Primary == protocol.ReplicaIndex(index) {
			return protocol.ReplicaIndex(index)
		}
	}
	world.fail("normal primary unavailable")
	return 0
}

func (world *ledgerWorld) final() {
	world.until(20_000, func() bool {
		if !world.agree() || world.history.PendingCount() != 0 {
			return false
		}
		for index := range world.cluster.nodes {
			state, _ := world.cluster.Snapshot(protocol.ReplicaIndex(index))
			if state.CommitMin != state.HeadOp {
				return false
			}
		}
		return true
	})
	for _, node := range world.cluster.nodes {
		world.check(world.history.CheckFinal(node.machine.(*LedgerMachine).Snapshot()))
	}
	world.check(world.cluster.CheckInvariants())
}

func (world *ledgerWorld) waitIO(member protocol.ReplicaIndex, kind replication.IOKind, phase replication.IOPhase) ScheduledIO {
	for range 20_000 {
		for range 32 {
			count := world.cluster.PendingIO(world.events)
			for _, event := range world.events[:count] {
				if event.Replica == member && event.Operation.Kind == kind && event.Operation.Phase == phase {
					return event
				}
			}
			if count == 0 {
				break
			}
			event := world.events[world.random.IntN(count)]
			world.advance(event)
		}
		world.steps++
		if err := world.cluster.Step(); err != nil {
			world.fail("waiting for IO boundary: %v", err)
		}
	}
	world.fail("IO boundary not reached member=%d kind=%d phase=%d", member, kind, phase)
	return ScheduledIO{}
}

func TestLongDeterministicFaultCampaign(t *testing.T) {
	start := campaignEnvironment(t, "VSR_LONG_SEED_START", 1)
	count := campaignEnvironment(t, "VSR_LONG_SEED_COUNT", 2)
	if count == 0 || count-1 > ^uint64(0)-start {
		t.Fatal("invalid long-campaign seed range")
	}
	for offset := range count {
		seed := start + offset
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			runLongFaultCampaign(t, seed)
		})
	}
}

func runLongFaultCampaign(t *testing.T, seed uint64) {
	t.Helper()
	config := DefaultConfig(3)
	config.StandbyCount = uint8(seed % 2)
	world := newLedgerWorld(t, seed, config)
	clients := []*ledgerClient{world.addClient(1), world.addClient(2)}
	world.phase = "checkpoint_warmup"
	world.increments(96, clients...)
	world.final()
	lagging := protocol.ReplicaIndex((uint8(world.primary()) + 1) % 3)
	if config.StandbyCount != 0 {
		lagging = protocol.ReplicaIndex(config.ActiveCount)
	}
	before, _ := world.cluster.Snapshot(lagging)
	world.phase = "partition_across_wraps"
	for peer := range len(world.cluster.nodes) {
		if protocol.ReplicaIndex(peer) != lagging {
			world.check(world.cluster.Network().Partition(protocol.ReplicaIndex(peer), lagging))
		}
	}
	world.increments(160, clients...)
	current, _ := world.cluster.Snapshot(world.primary())
	if current.Checkpoint.PrepareOp()-before.Checkpoint.PrepareOp() <= protocol.Op(world.cluster.config.Cluster.JournalSlots) {
		world.fail("lagging replica did not fall beyond retained WAL")
	}
	world.phase = "primary_restart"
	oldPrimary := world.primary()
	world.check(world.cluster.Crash(t.Context(), oldPrimary))
	world.check(world.cluster.Restart(t.Context(), oldPrimary))
	world.until(20_000, func() bool {
		for peer := range len(world.cluster.nodes) {
			if protocol.ReplicaIndex(peer) == lagging {
				continue
			}
			state, _ := world.cluster.Snapshot(protocol.ReplicaIndex(peer))
			if state.Status != replication.StatusNormal || state.View <= current.View {
				return false
			}
		}
		return true
	})
	world.phase = "sync_crash_at_barrier"
	world.cluster.Network().HealAll()
	barrier := world.waitIO(lagging, replication.IOSuperblockPersist, replication.IOPhaseSync)
	world.advance(barrier)
	world.check(world.cluster.Crash(t.Context(), lagging))
	world.check(world.cluster.Restart(t.Context(), lagging))
	if err := world.cluster.AdvanceIO(barrier); !errors.Is(err, ErrStaleIO) {
		world.fail("old incarnation IO accepted: %v", err)
	}
	world.phase = "sync_suffix_write_error"
	write := world.waitIO(lagging, replication.IOWALAppend, replication.IOPhaseWrite)
	storage := world.cluster.Storage(lagging)
	world.check(storage.Arm(StorageFault{At: storage.NextOperation(), Kind: replication.IOWrite, Effect: FaultFail}))
	world.expectedFault = &lagging
	world.advance(write)
	world.until(20_000, func() bool { return world.faults == 1 })
	world.check(world.cluster.Restart(t.Context(), lagging))
	world.final()
	world.phase = "more_checkpoints"
	world.increments(96, clients...)
	world.final()
	world.phase = "whole_cluster_restart"
	for member := range len(world.cluster.nodes) {
		world.check(world.cluster.Crash(t.Context(), protocol.ReplicaIndex(member)))
	}
	for _, member := range world.random.Perm(len(world.cluster.nodes)) {
		world.check(world.cluster.Restart(t.Context(), protocol.ReplicaIndex(member)))
	}
	world.final()
	world.phase = "post_recovery_progress"
	world.increments(32, clients...)
	world.final()
	interval, _ := world.cluster.config.Cluster.CheckpointInterval()
	if world.maxCheckpoint < protocol.Op(8*interval-1) || uint64(world.maxCommit)/world.cluster.config.Cluster.JournalSlots < 4 {
		world.fail("checkpoint/wrap coverage not reached checkpoint=%d commit=%d", world.maxCheckpoint, world.maxCommit)
	}
	t.Logf("seed=%d standby=%d increments=%d checkpoint=%d wraps=%d injected_errors=%d steps=%d io_trace=%s", seed, config.StandbyCount, world.history.Completed(), world.maxCheckpoint, uint64(world.maxCommit)/world.cluster.config.Cluster.JournalSlots, world.faults, world.steps, world.trace)
}

func TestControlledSmallTopologiesRecoverWithoutDuplicateEffects(t *testing.T) {
	for _, active := range []uint8{1, 2} {
		t.Run(fmt.Sprintf("active_%d", active), func(t *testing.T) {
			world := newLedgerWorld(t, uint64(active)+10, DefaultConfig(active))
			client := world.addClient(1)
			world.increments(96, client)
			world.final()
			world.phase = "pending_append_crash"
			victim := world.primary()
			world.submit(client)
			barrier := world.waitIO(victim, replication.IOWALAppend, replication.IOPhaseSync)
			world.advance(barrier)
			world.check(world.cluster.Crash(t.Context(), victim))
			completed := world.history.Completed()
			for range 20 {
				world.step()
			}
			if world.history.Completed() != completed {
				world.fail("request completed without an available quorum")
			}
			world.check(world.cluster.Restart(t.Context(), victim))
			world.until(20_000, func() bool { return client.pending == 0 })
			world.increments(32, client)
			world.final()
		})
	}
}

func TestClientSessionChurnKeepsBoundedOwnership(t *testing.T) {
	config := DefaultConfig(2)
	config.Cluster.ClientsMax = 1
	config.ClientProcessesMax = 2
	world := newLedgerWorld(t, 23, config)
	client := world.addClient(1)
	world.increments(1, client)
	for generation := uint64(2); generation <= 33; generation++ {
		world.phase = "session_churn"
		next := world.addClient(generation)
		world.until(20_000, world.agree)
		client.allowEviction = true
		body := EncodeLedgerRequest(world.history.Submitted() + 1)
		world.check(client.client.Submit(LedgerIncrement, body[:]))
		world.until(20_000, func() bool { return client.evicted })
		world.check(world.cluster.CloseClient(client.id))
		world.increments(1, next)
		world.final()
		if len(world.cluster.network.clients) != 1 || world.cluster.Resources().ClientProcesses != 1 {
			world.fail("retired client routing or process retained")
		}
		client = next
	}
}

func TestInterruptedViewPersistenceRecoverySeed6(t *testing.T) {
	runLongFaultCampaign(t, 6)
}
