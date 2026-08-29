package replication

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestCanonicalSuffixSelectionAndNegativeTruncation(t *testing.T) {
	replica, root, first, second := canonicalFixture(t)
	installJoinRecord(replica, 0, 2, 0, []protocol.Header{second, first, root}, 0b111, 0)
	installJoinRecord(replica, 1, 2, 0, []protocol.Header{second, first, root}, 0b111, 0)
	commit, head, count, resolved := replica.selectCanonicalSuffix()
	if !resolved || commit != 0 || head != 2 || count != 3 {
		t.Fatalf("selection = commit %d head %d count %d resolved %t", commit, head, count, resolved)
	}
	if prepareOp(&replica.canonicalHeaders[0]) != 2 || prepareOp(&replica.canonicalHeaders[2]) != 0 {
		t.Fatalf("canonical order = %d,%d", prepareOp(&replica.canonicalHeaders[0]), prepareOp(&replica.canonicalHeaders[2]))
	}

	replica, root, first, second = canonicalFixture(t)
	installJoinRecord(replica, 0, 2, 0, []protocol.Header{second, first, root}, 0b111, 0)
	installJoinRecord(replica, 1, 1, 0, []protocol.Header{first, root}, 0b11, 0)
	installJoinRecord(replica, 2, 1, 0, []protocol.Header{first, root}, 0b11, 0)
	commit, head, count, resolved = replica.selectCanonicalSuffix()
	if !resolved || commit != 0 || head != 1 || count != 2 {
		t.Fatalf("truncation = commit %d head %d count %d resolved %t", commit, head, count, resolved)
	}
}

func TestExitViewQuorumPersistsBeforeJoin(t *testing.T) {
	config, storage := threeReplicaFormat(t)
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(config.Cluster.PipelineMax),
		CheckpointMax: 1,
	}}
	replica, err := Open(context.Background(), config, Dependencies{
		Storage:      storage,
		MessageBus:   &captureBus{},
		Clock:        fixedClock{sample: TimeSample{Wall: 1, Synchronized: true}},
		Entropy:      bytes.NewReader([]byte{1}),
		StateMachine: machine,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, replica)
	if replica.Snapshot().Status != StatusNormal {
		t.Fatalf("initial status = %v", replica.Snapshot().Status)
	}
	pool, err := protocol.NewFramePool(2, uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	for author := range uint8(2) {
		message, err := pool.Acquire(0)
		if err != nil {
			t.Fatal(err)
		}
		header := protocol.Header{Group: config.Group, View: 0, Protocol: protocol.ProtocolVersion, Command: protocol.CommandExitView, Author: protocol.ReplicaIndex(author)}
		if err := message.Seal(&header); err != nil {
			t.Fatal(err)
		}
		if err := replica.Submit(message); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for replica.Snapshot().DurableView != 1 {
		if _, err := replica.Process(64); err != nil {
			t.Fatal(err)
		}
		if replica.Snapshot().DurableView == 1 {
			break
		}
		select {
		case <-replica.io.Ready():
		case <-replica.notify:
		case <-deadline.C:
			t.Fatalf("view snapshot = %+v", replica.Snapshot())
		}
	}
	snapshot := replica.Snapshot()
	if snapshot.Status != StatusViewChange || snapshot.View != 1 || snapshot.DurableView != 1 || snapshot.LogView != 0 {
		t.Fatalf("view snapshot = %+v", snapshot)
	}
	durable := replica.superblocks.Current()
	if durable.State.View != 1 || durable.State.LogView != 0 {
		t.Fatalf("durable view = %d log view = %d", durable.State.View, durable.State.LogView)
	}
	if replica.joinViewBits != 1<<uint8(replica.local) {
		t.Fatalf("local join bits = %b", replica.joinViewBits)
	}
}

func canonicalFixture(t testing.TB) (*Replica, protocol.Header, protocol.Header, protocol.Header) {
	t.Helper()
	cluster := compactTestClusterConfig()
	membership := Membership{Members: [MembersMax]protocol.MemberID{{1}, {2}, {3}}, ActiveCount: 3, LocalMember: protocol.MemberID{1}}
	rootBytes, err := rootPrepareHeader(protocol.GroupID{1}, 3, uint32(cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	root, reason := protocol.DecodeHeader(rootBytes[:], protocol.GroupID{1}, uint32(cluster.MessageSizeMax), 3)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	firstFrame := connectedPrepareFrame(t, cluster, root, 1)
	first, _, reason := protocol.DecodeFrame(firstFrame, protocol.GroupID{1}, uint32(cluster.MessageSizeMax), 3)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	secondFrame := connectedPrepareFrame(t, cluster, first, 2)
	second, _, reason := protocol.DecodeFrame(secondFrame, protocol.GroupID{1}, uint32(cluster.MessageSizeMax), 3)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	quorums, _ := QuorumsFor(3, 3)
	replica := &Replica{
		config:           Config{Group: protocol.GroupID{1}, Membership: membership, Cluster: cluster},
		membership:       membership,
		quorums:          quorums,
		checkpoint:       CheckpointState{Header: rootBytes},
		joins:            make([]joinRecord, 3),
		joinHeaders:      make([]protocol.Header, 3*int(cluster.PipelineMax+1)),
		canonicalHeaders: make([]protocol.Header, int(cluster.PipelineMax+1)),
		logger:           zerolog.Nop(),
	}
	return replica, root, first, second
}

func installJoinRecord(replica *Replica, sender uint8, head, commit protocol.Op, headers []protocol.Header, present, nack uint16) {
	replica.joins[sender] = joinRecord{valid: true, present: present, nack: nack, head: head, commit: commit, checkpoint: 0, logView: 0, count: uint8(len(headers))}
	copy(replica.joinHeaderSlice(sender), headers)
}

func threeReplicaFormat(t testing.TB) (Config, *crashStorage) {
	t.Helper()
	cluster := compactTestClusterConfig()
	membership := Membership{Members: [MembersMax]protocol.MemberID{{1}, {2}, {3}}, ActiveCount: 3, LocalMember: protocol.MemberID{2}}
	config := Config{Group: protocol.GroupID{8}, Membership: membership, Cluster: cluster, Process: DefaultProcessConfig(), CurrentRelease: 1, ClientReleaseMin: 1}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	storage := &crashStorage{}
	if err := Format(context.Background(), FormatConfig{Group: config.Group, Membership: membership, Cluster: cluster, CurrentRelease: 1}, FormatDependencies{Storage: storage}); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	return config, storage
}
