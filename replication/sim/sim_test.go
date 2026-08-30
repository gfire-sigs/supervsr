package sim

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestStorageCrashDiscardsVolatileAndPreservesTornSyncPrefix(t *testing.T) {
	storage := NewStorage()
	if err := storage.Resize(8); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("abcdefgh"), 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("ABCDEFGH"), 0); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	actual := make([]byte, 8)
	if err := storage.ReadAt(actual, 0); err != nil {
		t.Fatal(err)
	}
	if string(actual) != "abcdefgh" {
		t.Fatalf("after crash=%q", actual)
	}
	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultTornWrite, Prefix: 2}); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("12345678"), 0); !errors.Is(err, ErrInjectedFault) {
		t.Fatalf("torn write error=%v", err)
	}
	if err := storage.ReadAt(actual, 0); err != nil {
		t.Fatal(err)
	}
	if string(actual) != "12cdefgh" {
		t.Fatalf("after torn write=%q", actual)
	}
	storage.Crash()
	if err := storage.ReadAt(actual, 1); !errors.Is(err, replication.ErrShortIO) {
		t.Fatalf("short read error=%v", err)
	}
	if err := storage.WriteAt([]byte("ABCDEFGH"), 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultTornSync, Prefix: 3}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Sync(); !errors.Is(err, ErrInjectedFault) {
		t.Fatalf("torn sync error=%v", err)
	}
	storage.Crash()
	if err := storage.ReadAt(actual, 0); err != nil {
		t.Fatal(err)
	}
	if string(actual) != "ABCdefgh" {
		t.Fatalf("after torn sync=%q", actual)
	}
}

func TestStorageModelsLostStaleCorruptMisdirectedAndReorderedIO(t *testing.T) {
	storage := NewStorage()
	if err := storage.Resize(8); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("abcdefgh"), 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	actual := make([]byte, 8)

	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultLostWrite}); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("ABCDEFGH"), 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.ReadAt(actual, 0); err != nil || string(actual) != "abcdefgh" {
		t.Fatalf("lost write read=%q error=%v", actual, err)
	}

	if err := storage.WriteAt([]byte("ABCDEFGH"), 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultStaleRead}); err != nil {
		t.Fatal(err)
	}
	if err := storage.ReadAt(actual, 0); err != nil || string(actual) != "abcdefgh" {
		t.Fatalf("stale read=%q error=%v", actual, err)
	}
	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultCorruptRead, Prefix: 1, Mask: 0x20}); err != nil {
		t.Fatal(err)
	}
	if err := storage.ReadAt(actual, 0); err != nil || string(actual) != "AbCDEFGH" {
		t.Fatalf("corrupt read=%q error=%v", actual, err)
	}
	if err := storage.ReadAt(actual, 0); err != nil || string(actual) != "ABCDEFGH" {
		t.Fatalf("corrupt read mutated storage=%q error=%v", actual, err)
	}

	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultMisdirectedWrite, Target: 4}); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("12"), 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultMisdirectedRead, Target: 4}); err != nil {
		t.Fatal(err)
	}
	if err := storage.ReadAt(actual[:2], 0); err != nil || string(actual[:2]) != "12" {
		t.Fatalf("misdirected read=%q error=%v", actual[:2], err)
	}
	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultCorruptWrite, Prefix: 1, Mask: 0x20}); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("zz"), 6); err != nil {
		t.Fatal(err)
	}
	if err := storage.ReadAt(actual[6:8], 6); err != nil || string(actual[6:8]) != "zZ" {
		t.Fatalf("corrupt write=%q error=%v", actual[6:8], err)
	}
	clear(actual[:4])
	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultTornRead, Prefix: 2}); err != nil {
		t.Fatal(err)
	}
	if err := storage.ReadAt(actual[:4], 0); !errors.Is(err, ErrInjectedFault) || string(actual[:2]) != "AB" {
		t.Fatalf("torn read=%q error=%v", actual[:4], err)
	}

	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultDelayedWrite}); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("old!"), 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultDelayedWrite}); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("new!"), 0); err != nil {
		t.Fatal(err)
	}
	if storage.PendingWrites() != 2 {
		t.Fatalf("pending writes = %d, want 2", storage.PendingWrites())
	}
	if err := storage.ReleaseDelayedWrite(1); err != nil {
		t.Fatal(err)
	}
	if err := storage.ReleaseDelayedWrite(0); err != nil {
		t.Fatal(err)
	}
	if err := storage.ReadAt(actual[:4], 0); err != nil || string(actual[:4]) != "old!" {
		t.Fatalf("reordered writes=%q error=%v", actual[:4], err)
	}

	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultLostSync}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	if err := storage.ReadAt(actual, 0); err != nil || string(actual) != "abcdefgh" {
		t.Fatalf("lost sync crash=%q error=%v", actual, err)
	}
	if err := storage.CorruptDurable(0, 0x20); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	if err := storage.ReadAt(actual, 0); err != nil || string(actual) != "Abcdefgh" {
		t.Fatalf("durable corruption=%q error=%v", actual, err)
	}
}

func TestStorageBoundsDelayedWrites(t *testing.T) {
	storage := NewStorage()
	if err := storage.Resize(1); err != nil {
		t.Fatal(err)
	}
	for range DelayedWritesMax {
		if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultDelayedWrite}); err != nil {
			t.Fatal(err)
		}
		if err := storage.WriteAt([]byte{1}, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Effect: FaultDelayedWrite}); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte{1}, 0); !errors.Is(err, ErrInjectedFault) {
		t.Fatalf("overflow error = %v", err)
	}
	storage.Crash()
	if storage.PendingWrites() != 0 {
		t.Fatalf("pending writes after crash = %d", storage.PendingWrites())
	}
}

func TestNetworkAppliesPartitionDuplicationAndCorruptionDeterministically(t *testing.T) {
	network, err := NewNetwork(2, 4096, 8)
	if err != nil {
		t.Fatal(err)
	}
	var received [][]byte
	if err := network.RegisterReplica(1, func(frame *protocol.Frame) error {
		encoded, err := frame.Bytes()
		if err == nil {
			received = append(received, append([]byte(nil), encoded...))
		}
		frame.Release()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	pool, err := protocol.NewFramePool(1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	message, err := pool.Acquire(1)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := message.Body()
	body[0] = 7
	header := protocol.Header{Group: protocol.GroupID{1}, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPing}
	if err := message.Seal(&header); err != nil {
		t.Fatal(err)
	}
	if err := network.Partition(0, 1); err != nil {
		t.Fatal(err)
	}
	network.ReplicaBus(0).SendReplica(1, message)
	if count, err := network.DeliverReady(); err != nil || count != 0 {
		t.Fatalf("partition delivery count=%d err=%v", count, err)
	}
	if err := network.Heal(0, 1); err != nil {
		t.Fatal(err)
	}
	network.DropNext(1)
	network.ReplicaBus(0).SendReplica(1, message)
	if count, err := network.DeliverReady(); err != nil || count != 0 {
		t.Fatalf("drop delivery count=%d err=%v", count, err)
	}
	network.SetDelay(2)
	network.DuplicateNext(1)
	network.CorruptNext(protocol.HeaderSize, 0xff)
	network.ReplicaBus(0).SendReplica(1, message)
	if count, err := network.DeliverReady(); err != nil || count != 0 {
		t.Fatalf("early delivery count=%d err=%v", count, err)
	}
	network.Advance()
	if count, err := network.DeliverReady(); err != nil || count != 0 {
		t.Fatalf("early delivery count=%d err=%v", count, err)
	}
	network.Advance()
	if count, err := network.DeliverReady(); err != nil || count != 2 {
		t.Fatalf("duplicate delivery count=%d err=%v", count, err)
	}
	message.Release()
	if len(received) != 2 || received[0][protocol.HeaderSize] != 0xf8 || received[1][protocol.HeaderSize] != 7 {
		t.Fatalf("received bodies=%v", received)
	}
}
func TestNetworkControlsAsymmetricDelayReorderingMisdirectionAndReconnect(t *testing.T) {
	network, err := NewNetwork(3, 4096, 16)
	if err != nil {
		t.Fatal(err)
	}
	type delivery struct {
		to     protocol.ReplicaIndex
		marker byte
	}
	var deliveries []delivery
	for index := protocol.ReplicaIndex(1); index <= 2; index++ {
		target := index
		if err := network.RegisterReplica(target, func(frame *protocol.Frame) error {
			encoded, err := frame.Bytes()
			if err == nil {
				deliveries = append(deliveries, delivery{to: target, marker: encoded[protocol.HeaderSize]})
			}
			frame.Release()
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	pool, err := protocol.NewFramePool(1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	send := func(to protocol.ReplicaIndex, marker byte) {
		frame, acquireErr := pool.Acquire(1)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		body, _ := frame.Body()
		body[0] = marker
		header := protocol.Header{Group: protocol.GroupID{1}, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPing}
		if sealErr := frame.Seal(&header); sealErr != nil {
			t.Fatal(sealErr)
		}
		network.ReplicaBus(0).SendReplica(to, frame)
		frame.Release()
	}

	if err := network.SetLinkDelay(0, 1, 2); err != nil {
		t.Fatal(err)
	}
	send(1, 1)
	send(2, 2)
	if count, err := network.DeliverReady(); err != nil || count != 1 || deliveries[0] != (delivery{to: 2, marker: 2}) {
		t.Fatalf("asymmetric delivery count=%d deliveries=%v error=%v", count, deliveries, err)
	}
	network.Advance()
	network.Advance()
	if count, err := network.DeliverReady(); err != nil || count != 1 || deliveries[1] != (delivery{to: 1, marker: 1}) {
		t.Fatalf("delayed delivery count=%d deliveries=%v error=%v", count, deliveries, err)
	}

	if err := network.SetLinkDelay(0, 1, 0); err != nil {
		t.Fatal(err)
	}
	network.DelayNext(2)
	send(1, 3)
	send(1, 4)
	if count, err := network.DeliverReady(); err != nil || count != 1 || deliveries[2].marker != 4 {
		t.Fatalf("reordered immediate count=%d deliveries=%v error=%v", count, deliveries, err)
	}
	network.Advance()
	network.Advance()
	if count, err := network.DeliverReady(); err != nil || count != 1 || deliveries[3].marker != 3 {
		t.Fatalf("reordered delayed count=%d deliveries=%v error=%v", count, deliveries, err)
	}

	network.DelayNext(1)
	send(1, 5)
	if err := network.PartitionDirected(0, 1); err != nil {
		t.Fatal(err)
	}
	network.Advance()
	if count, err := network.DeliverReady(); err != nil || count != 0 {
		t.Fatalf("partitioned queued delivery count=%d error=%v", count, err)
	}
	if err := network.HealDirected(0, 1); err != nil {
		t.Fatal(err)
	}
	if count, err := network.DeliverReady(); err != nil || count != 1 || deliveries[4].marker != 5 {
		t.Fatalf("reconnected delivery count=%d deliveries=%v error=%v", count, deliveries, err)
	}

	if err := network.MisdirectNext(2); err != nil {
		t.Fatal(err)
	}
	send(1, 6)
	if count, err := network.DeliverReady(); err != nil || count != 1 || deliveries[5] != (delivery{to: 2, marker: 6}) {
		t.Fatalf("misdirected delivery count=%d deliveries=%v error=%v", count, deliveries, err)
	}
	if err := network.Disconnect(1); err != nil {
		t.Fatal(err)
	}
	send(1, 7)
	if err := network.Reconnect(1); err != nil {
		t.Fatal(err)
	}
	if count, err := network.DeliverReady(); err != nil || count != 0 {
		t.Fatalf("disconnected send survived count=%d error=%v", count, err)
	}
}

func TestClockControlsDriftJumpsFreezeAndSynchronizationLoss(t *testing.T) {
	clock := NewClock()
	clock.SetTime(1_000, 2_000)
	if err := clock.SetDrift(500_000); err != nil {
		t.Fatal(err)
	}
	clock.Advance(100 * time.Nanosecond)
	sample := clock.Now()
	if sample.Wall != 1_150 || sample.Monotonic != 2_150 || !sample.Synchronized {
		t.Fatalf("drifted sample = %+v", sample)
	}
	if err := clock.JumpWall(-50 * time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	if err := clock.JumpMonotonic(25 * time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	clock.SetSynchronized(false)
	clock.Freeze(true)
	clock.Advance(time.Second)
	sample = clock.Now()
	if sample.Wall != 1_100 || sample.Monotonic != 2_175 || sample.Synchronized {
		t.Fatalf("jumped frozen sample = %+v", sample)
	}
	if err := clock.JumpWall(-2_000 * time.Nanosecond); !errors.Is(err, replication.ErrInvalidConfiguration) {
		t.Fatalf("underflow jump error = %v", err)
	}
	if err := clock.SetDrift(-1_000_000); !errors.Is(err, replication.ErrInvalidConfiguration) {
		t.Fatalf("invalid drift error = %v", err)
	}
}

func TestNetworkReportsDeterministicBackpressure(t *testing.T) {
	network, err := NewNetwork(2, 4096, 1)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := protocol.NewFramePool(1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	message, err := pool.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	header := protocol.Header{Group: protocol.GroupID{1}, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPing}
	if err := message.Seal(&header); err != nil {
		t.Fatal(err)
	}
	network.ReplicaBus(0).SendReplica(1, message)
	network.ReplicaBus(0).SendReplica(1, message)
	message.Release()
	if _, err := network.DeliverReady(); !errors.Is(err, ErrNetworkBackpressure) {
		t.Fatalf("delivery error=%v", err)
	}
}

func TestClusterSupportsEveryActiveAndStandbyTopology(t *testing.T) {
	for active := uint8(1); active <= replication.ActiveMax; active++ {
		for standby := uint8(0); standby <= replication.StandbyMax; standby++ {
			name := fmt.Sprintf("active_%d_standby_%d", active, standby)
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				config := DefaultConfig(active)
				config.StandbyCount = standby
				cluster, err := NewCluster(ctx, config, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := cluster.Close(ctx); err != nil {
						t.Error(err)
					}
				}()
				members := active + standby
				if err := runUntil(cluster, 2_000, func(cluster *Cluster) bool {
					return configuredMembersAgree(cluster, members)
				}); err != nil {
					t.Fatalf("%v states=%v", err, compactMemberSnapshots(cluster, members))
				}
				if err := cluster.CheckInvariants(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestClusterChecksGlobalInvariantsAfterEveryStep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cluster, err := NewCluster(ctx, DefaultConfig(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	machine, ok := cluster.nodes[0].machine.(*Machine)
	if !ok {
		t.Fatal("default simulation machine missing")
	}
	snapshot, ok := cluster.Snapshot(0)
	if !ok {
		t.Fatal("replica snapshot missing")
	}
	machine.mu.Lock()
	machine.commits = append(machine.commits,
		Commit{Operation: protocol.OperationApplicationMin, Op: snapshot.CommitMin},
		Commit{Operation: protocol.OperationApplicationMin, Op: snapshot.CommitMin},
	)
	machine.mu.Unlock()
	if err := cluster.Step(); !errors.Is(err, ErrInvariant) {
		t.Fatalf("step invariant error = %v", err)
	}
}

func TestClusterRestartsAcrossApplicationIOCrashPoints(t *testing.T) {
	crashPoints := 0
	for point := uint64(0); point < 32; point++ {
		if !exerciseApplicationIOCrashPoint(t, point) {
			break
		}
		crashPoints++
	}
	if crashPoints < 4 {
		t.Fatalf("exercised %d application I/O crash points", crashPoints)
	}
}

func exerciseApplicationIOCrashPoint(t testing.TB, point uint64) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cluster, err := NewCluster(ctx, DefaultConfig(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	events := &clientEvents{}
	client, err := cluster.AddClient(protocol.ClientID{byte(point + 1)}, events)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Register(); err != nil {
		t.Fatal(err)
	}
	if err := runUntil(cluster, 2_000, func(*Cluster) bool { return events.replyCount() == 1 }); err != nil {
		t.Fatal(err)
	}
	before, _ := cluster.Snapshot(0)
	storage := cluster.Storage(0)
	if err := storage.Arm(StorageFault{At: storage.NextOperation() + point, Effect: FaultFail}); err != nil {
		t.Fatal(err)
	}
	if err := client.Submit(protocol.OperationApplicationMin, []byte{byte(point)}); err != nil {
		t.Fatal(err)
	}
	faulted := false
	for range 4_000 {
		if events.replyCount() == 2 {
			if faulted {
				after, _ := cluster.Snapshot(0)
				if after.View <= before.View {
					t.Fatalf("point %d solo restart retained view %d", point, after.View)
				}
			}
			return faulted
		}
		stepErr := cluster.Step()
		if stepErr == nil {
			continue
		}
		if !errors.Is(stepErr, ErrInjectedFault) {
			t.Fatalf("point %d step error = %v", point, stepErr)
		}
		faulted = true
		if err := cluster.Crash(ctx, 0); err != nil {
			t.Fatalf("point %d crash: %v", point, err)
		}
		if err := cluster.Restart(ctx, 0); err != nil {
			t.Fatalf("point %d restart: %v", point, err)
		}
	}
	t.Fatalf("point %d did not complete", point)
	return false
}

func TestClusterCommitsAcrossPrimaryCrashAndRepairsRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cluster, err := NewCluster(ctx, DefaultConfig(3), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if err := runUntil(cluster, 2_000, replicasAgree); err != nil {
		t.Fatalf("%v snapshots=%v", err, clusterSnapshots(cluster))
	}
	events := &clientEvents{}
	clientID := protocol.ClientID{1}
	client, err := cluster.AddClient(clientID, events)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Register(); err != nil {
		t.Fatal(err)
	}
	if err := runUntil(cluster, 2_000, func(*Cluster) bool { return events.replyCount() == 1 }); err != nil {
		t.Fatal(err)
	}
	if err := client.Submit(protocol.OperationApplicationMin, []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := runUntil(cluster, 2_000, func(*Cluster) bool { return events.replyCount() == 2 }); err != nil {
		t.Fatal(err)
	}
	primary := currentPrimary(t, cluster)
	for index := range uint8(3) {
		peer := protocol.ReplicaIndex(index)
		if peer != primary {
			if err := cluster.Network().Partition(primary, peer); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := cluster.Crash(ctx, primary); err != nil {
		t.Fatal(err)
	}
	if err := runUntil(cluster, 4_000, liveReplicasAgree); err != nil {
		t.Fatalf("%v snapshots=%v", err, clusterSnapshots(cluster))
	}
	if err := client.Submit(protocol.OperationApplicationMin, []byte("after")); err != nil {
		t.Fatal(err)
	}
	if err := runUntil(cluster, 4_000, func(*Cluster) bool { return events.replyCount() == 3 }); err != nil {
		t.Fatalf("%v snapshots=%v replies=%d", err, clusterSnapshots(cluster), events.replyCount())
	}
	if err := cluster.Restart(ctx, primary); err != nil {
		t.Fatal(err)
	}
	for index := range uint8(3) {
		peer := protocol.ReplicaIndex(index)
		if peer != primary {
			if err := cluster.Network().Heal(primary, peer); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := runUntil(cluster, 4_000, replicasAgree); err != nil {
		t.Fatalf("%v snapshots=%v", err, clusterSnapshots(cluster))
	}
	if events.replyCount() != 3 {
		t.Fatalf("reply count=%d, want 3", events.replyCount())
	}
}

func TestClusterRecoversAfterAllReplicasRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cluster, err := NewCluster(ctx, DefaultConfig(3), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if err := runUntil(cluster, 2_000, replicasAgree); err != nil {
		t.Fatal(err)
	}
	events := &clientEvents{}
	client, err := cluster.AddClient(protocol.ClientID{1}, events)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Register(); err != nil {
		t.Fatal(err)
	}
	if err := runUntil(cluster, 2_000, func(*Cluster) bool { return events.replyCount() == 1 }); err != nil {
		t.Fatal(err)
	}
	if err := client.Submit(protocol.OperationApplicationMin, []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := runUntil(cluster, 2_000, func(*Cluster) bool { return events.replyCount() == 2 }); err != nil {
		t.Fatal(err)
	}
	for index := range uint8(3) {
		if err := cluster.Crash(ctx, protocol.ReplicaIndex(index)); err != nil {
			t.Fatal(err)
		}
	}
	for index := range uint8(3) {
		if err := cluster.Restart(ctx, protocol.ReplicaIndex(index)); err != nil {
			t.Fatal(err)
		}
	}
	if err := runUntil(cluster, 4_000, replicasAgree); err != nil {
		t.Fatalf("%v snapshots=%v", err, clusterSnapshots(cluster))
	}
	if err := client.Submit(protocol.OperationApplicationMin, []byte("after")); err != nil {
		t.Fatal(err)
	}
	if err := runUntil(cluster, 2_000, func(*Cluster) bool { return events.replyCount() == 3 }); err != nil {
		t.Fatalf("%v snapshots=%v", err, clusterSnapshots(cluster))
	}
}

func TestClusterStateSyncsReplicaBeyondRetainedCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	config := DefaultConfig(3)
	cluster, err := NewCluster(ctx, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if err := runUntil(cluster, 2_000, replicasAgree); err != nil {
		t.Fatal(err)
	}
	lagging := protocol.ReplicaIndex(2)
	if err := cluster.Network().Partition(0, lagging); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Network().Partition(1, lagging); err != nil {
		t.Fatal(err)
	}
	laggingClock := cluster.MemberClock(lagging)
	if err := laggingClock.SetDrift(500_000); err != nil {
		t.Fatal(err)
	}
	laggingClock.SetSynchronized(false)
	laggingClock.Freeze(true)
	events := &clientEvents{}
	client, err := cluster.AddClient(protocol.ClientID{2}, events)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Register(); err != nil {
		t.Fatal(err)
	}
	if err := runUntil(cluster, 2_000, func(*Cluster) bool { return events.replyCount() == 1 }); err != nil {
		t.Fatal(err)
	}
	interval, ok := config.Cluster.CheckpointInterval()
	if !ok {
		t.Fatal("checkpoint interval invalid")
	}
	requests := int(2*interval + 1)
	for request := range requests {
		switch {
		case request%31 == 0:
			if err := cluster.Network().MisdirectNext(lagging); err != nil {
				t.Fatal(err)
			}
		case request%23 == 0:
			cluster.Network().CorruptNext(protocol.HeaderSize, 0x01)
		case request%17 == 0:
			cluster.Network().DropNext(1)
		case request%13 == 0:
			cluster.Network().DelayNext(3)
		case request%11 == 0:
			cluster.Network().DuplicateNext(1)
		}
		if err := client.Submit(protocol.OperationApplicationMin, []byte{byte(request)}); err != nil {
			t.Fatal(err)
		}
		want := request + 2
		if err := runUntil(cluster, 2_000, func(*Cluster) bool { return events.replyCount() == want }); err != nil {
			t.Fatalf("request %d: %v", request, err)
		}
	}
	before, _ := cluster.Snapshot(lagging)
	primary := currentPrimary(t, cluster)
	current, _ := cluster.Snapshot(primary)
	if current.Checkpoint.PrepareOp() <= before.Checkpoint.PrepareOp() {
		t.Fatalf("checkpoint did not advance: current=%d lagging=%d", current.Checkpoint.PrepareOp(), before.Checkpoint.PrepareOp())
	}
	laggingClock.Freeze(false)
	if err := laggingClock.JumpWall(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	laggingClock.SetSynchronized(true)
	if err := cluster.Network().SetLinkDelay(0, lagging, 3); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Network().SetLinkDelay(lagging, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Network().Heal(0, lagging); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Network().Heal(1, lagging); err != nil {
		t.Fatal(err)
	}
	if err := runUntil(cluster, 20_000, replicasAgree); err != nil {
		metrics, _ := cluster.Metrics(lagging)
		t.Fatalf("simulation: %v current_checkpoint=%d lagging_checkpoint=%d states=%v lagging_metrics=%+v", err, current.Checkpoint.PrepareOp(), before.Checkpoint.PrepareOp(), compactSnapshots(cluster), metrics)
	}
	after, _ := cluster.Snapshot(lagging)
	if after.Checkpoint.PrepareOp() != current.Checkpoint.PrepareOp() || after.CommitMin != current.CommitMin {
		t.Fatalf("lagging checkpoint=%d commit=%d, want checkpoint=%d commit=%d", after.Checkpoint.PrepareOp(), after.CommitMin, current.Checkpoint.PrepareOp(), current.CommitMin)
	}
}

type clientEvents struct {
	replies []replication.ClientReply
	evicted bool
}

func (events *clientEvents) Reply(reply replication.ClientReply) {
	reply.Body = append([]byte(nil), reply.Body...)
	events.replies = append(events.replies, reply)
}

func (events *clientEvents) Evicted(protocol.EvictionReason) {
	events.evicted = true
}

func (events *clientEvents) replyCount() int {
	return len(events.replies)
}

func runUntil(cluster *Cluster, limit int, condition func(*Cluster) bool) error {
	for range limit {
		if condition(cluster) {
			return nil
		}
		if err := cluster.Step(); err != nil {
			return err
		}
	}
	return errors.New("simulation: condition did not converge")
}

func currentPrimary(t testing.TB, cluster *Cluster) protocol.ReplicaIndex {
	t.Helper()
	for index := range uint8(3) {
		if snapshot, ok := cluster.Snapshot(protocol.ReplicaIndex(index)); ok && snapshot.Status == replication.StatusNormal {
			return snapshot.Primary
		}
	}
	t.Fatal("normal replica missing")
	return 0
}

func replicasAgree(cluster *Cluster) bool {
	var expected replication.ReplicaSnapshot
	found := false
	for index := range uint8(3) {
		snapshot, ok := cluster.Snapshot(protocol.ReplicaIndex(index))
		if !ok || snapshot.Status != replication.StatusNormal {
			return false
		}
		if !found {
			expected = snapshot
			found = true
			continue
		}
		if snapshot.View != expected.View || snapshot.CommitMin != expected.CommitMin || snapshot.HeadOp != expected.HeadOp {
			return false
		}
	}
	return found
}

func compactSnapshots(cluster *Cluster) []string {
	states := make([]string, 0, 3)
	for index := range uint8(3) {
		snapshot, ok := cluster.Snapshot(protocol.ReplicaIndex(index))
		if !ok {
			states = append(states, "down")
			continue
		}
		states = append(states, fmt.Sprintf("s=%d v=%d dv=%d lv=%d cp=%d h=%d c=%d cm=%d p=%d", snapshot.Status, snapshot.View, snapshot.DurableView, snapshot.LogView, snapshot.Checkpoint.PrepareOp(), snapshot.HeadOp, snapshot.CommitMin, snapshot.CommitMax, snapshot.PipelineLen))
	}
	return states
}

func clusterSnapshots(cluster *Cluster) []replication.ReplicaSnapshot {
	snapshots := make([]replication.ReplicaSnapshot, 0, 3)
	for index := range uint8(3) {
		if snapshot, ok := cluster.Snapshot(protocol.ReplicaIndex(index)); ok {
			snapshots = append(snapshots, snapshot)
		}
	}
	return snapshots
}

func liveReplicasAgree(cluster *Cluster) bool {
	var expected replication.ReplicaSnapshot
	found := false
	for index := range uint8(3) {
		snapshot, ok := cluster.Snapshot(protocol.ReplicaIndex(index))
		if !ok {
			continue
		}
		if snapshot.Status != replication.StatusNormal {
			return false
		}
		if !found {
			expected = snapshot
			found = true
			continue
		}
		if snapshot.View != expected.View || snapshot.CommitMin != expected.CommitMin {
			return false
		}
	}
	return found
}

func configuredMembersAgree(cluster *Cluster, members uint8) bool {
	var expected replication.ReplicaSnapshot
	for index := range members {
		snapshot, ok := cluster.Snapshot(protocol.ReplicaIndex(index))
		if !ok || snapshot.Status != replication.StatusNormal {
			return false
		}
		if index == 0 {
			expected = snapshot
			continue
		}
		if snapshot.View != expected.View || snapshot.CommitMin != expected.CommitMin || snapshot.HeadOp != expected.HeadOp {
			return false
		}
	}
	return true
}

func compactMemberSnapshots(cluster *Cluster, members uint8) []string {
	states := make([]string, 0, members)
	for index := range members {
		snapshot, ok := cluster.Snapshot(protocol.ReplicaIndex(index))
		if !ok {
			states = append(states, "down")
			continue
		}
		states = append(states, fmt.Sprintf("s=%d v=%d cp=%d h=%d c=%d", snapshot.Status, snapshot.View, snapshot.Checkpoint.PrepareOp(), snapshot.HeadOp, snapshot.CommitMin))
	}
	return states
}
