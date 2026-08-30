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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
