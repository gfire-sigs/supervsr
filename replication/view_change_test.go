package replication

import (
	"bytes"
	"context"
	"encoding/binary"
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

func TestCanonicalRecoveryRequiresDurableLocalEntry(t *testing.T) {
	replica, root, first, _ := canonicalFixture(t)
	replica.pipeline = make([]pipelineEntry, int(replica.config.Cluster.PipelineMax))
	replica.pipeline[0] = pipelineEntry{header: first}
	replica.pipelineLen = 1
	replica.headOp = 1
	replica.canonicalHeaders[0] = first
	replica.canonicalHeaders[1] = root

	if replica.canonicalAvailable(1, 2) {
		t.Fatal("non-durable canonical entry reported available")
	}
	if ancestor, found := replica.recoveringCommonAncestor(2); !found || ancestor != 0 {
		t.Fatalf("non-durable recovery ancestor = %d, found %t", ancestor, found)
	}

	replica.pipeline[0].durable = true
	if !replica.canonicalAvailable(1, 2) {
		t.Fatal("durable canonical entry reported unavailable")
	}
	if ancestor, found := replica.recoveringCommonAncestor(2); !found || ancestor != 1 {
		t.Fatalf("durable recovery ancestor = %d, found %t", ancestor, found)
	}
}

func TestSelectedPrimaryRepairsMissingCommittedPrepare(t *testing.T) {
	replica, root, first, _ := canonicalFixture(t)
	replica.pipeline = make([]pipelineEntry, int(replica.config.Cluster.PipelineMax))
	replica.repairFrames = make([]*Message, int(replica.config.Cluster.PipelineMax+1))
	replica.status = StatusViewChange
	replica.view = 3
	replica.headOp = 0
	replica.headChecksum = root.HeaderChecksum
	installJoinRecord(replica, 0, 1, 1, []protocol.Header{first}, 0b1, 0)
	installJoinRecord(replica, 1, 1, 1, []protocol.Header{first}, 0b1, 0)

	replica.tryInstallCanonicalView()

	if replica.status != StatusRecoveringHead || !replica.repairViewValid {
		t.Fatalf("recovery status = %d, proof valid %t", replica.status, replica.repairViewValid)
	}
	if replica.repairViewAncestor != 0 || replica.repairViewCommit != 1 || replica.repairViewHead != 1 {
		t.Fatalf("recovery proof = ancestor %d commit %d head %d", replica.repairViewAncestor, replica.repairViewCommit, replica.repairViewHead)
	}
	bus := &captureBus{}
	frames, err := protocol.NewFramePool(1, uint32(replica.config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	budget, err := newJournalRepairBudget(3, replica.local)
	if err != nil {
		t.Fatal(err)
	}
	replica.frames = frames
	replica.repairBudget = budget
	replica.random = NewDeterministicRandom(1)
	replica.deps.MessageBus = bus
	replica.continueRecoveringView(1)
	request, _, reason := protocol.DecodeFrame(bus.replicaMessage(t, 0), replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3)
	if reason != protocol.RejectNone || request.Command != protocol.CommandGetPrepare || binary.LittleEndian.Uint64(request.Fields[32:40]) != 1 {
		t.Fatalf("repair request command=%d op=%d reason=%d", request.Command, binary.LittleEndian.Uint64(request.Fields[32:40]), reason)
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

func TestViewChangeWaitsForCheckpointTransition(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
	}}
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: machine,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, replica)
	replica.pipelineLen = 1
	replica.pipeline[0].header = initial.HeadHeader
	replica.pipeline[0].stage = CommitStageCheckpointData
	replica.beginViewChange(1)
	if replica.view != 0 || replica.pendingView != 1 || replica.viewIO != (IOHandle{}) {
		t.Fatalf("checkpoint did not defer view: view=%d pending=%d io=%+v", replica.view, replica.pendingView, replica.viewIO)
	}
	replica.pipelineLen = 0
	replica.pipeline[0] = pipelineEntry{}
	if !replica.resumePendingViewChange() {
		t.Fatal("pending view did not resume")
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for replica.durableView != 1 {
		if _, err := replica.Process(8); err != nil {
			t.Fatal(err)
		}
		select {
		case <-replica.io.Ready():
		case <-deadline.C:
			t.Fatalf("view persistence stalled: %+v", replica.Snapshot())
		default:
		}
	}
	if replica.fatalErr != nil {
		t.Fatal(replica.fatalErr)
	}
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
