package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestCanonicalDirtyCopyRemainsRepairSource(t *testing.T) {
	replica, root, first, _ := canonicalFixture(t)
	installJoinRecord(replica, 0, 1, 0, []protocol.Header{first, root}, 3, 1)
	negatives, copies := replica.canonicalEvidence(1, first, true)
	if negatives != 1 || copies != 1 {
		t.Fatalf("dirty copy: negatives=%d copies=%d, want 1 each", negatives, copies)
	}
}

func TestCanonicalExplicitTailCommitsLowerBound(t *testing.T) {
	replica, _, _, second := canonicalFixture(t)
	installJoinRecord(replica, 0, 2, 0, []protocol.Header{second}, 1, 0)
	installJoinRecord(replica, 1, 2, 0, []protocol.Header{second}, 1, 0)
	commit, head, count, resolved := replica.selectCanonicalSuffix()
	if !resolved || commit != 2 || head != 2 || count != 1 {
		t.Fatalf("selection commit=%d head=%d count=%d resolved=%t", commit, head, count, resolved)
	}
}

func TestCanonicalLatestLogViewExcludesTruncatedSuffix(t *testing.T) {
	replica, _, first, second := canonicalFixture(t)
	second.View, second.Author = 1, 1
	frame := make([]byte, protocol.HeaderSize+1)
	frame[protocol.HeaderSize] = 1
	if err := protocol.SealFrame(frame, &second); err != nil {
		t.Fatal(err)
	}
	installJoinRecord(replica, 0, 1, 1, []protocol.Header{first}, 1, 0)
	installJoinRecord(replica, 1, 2, 1, []protocol.Header{second, first}, 3, 0)
	replica.joins[0].logView, replica.joins[1].logView = 2, 1
	commit, head, count, resolved := replica.selectCanonicalSuffix()
	if !resolved || commit != 1 || head != 1 || count != 1 {
		t.Fatalf("selection commit=%d head=%d count=%d resolved=%t", commit, head, count, resolved)
	}
}

func TestCanonicalOmittedTailCannotEraseCommittedPrefix(t *testing.T) {
	replica, root, first, second := canonicalFixture(t)
	installJoinRecord(replica, 0, 2, 0, []protocol.Header{second}, 1, 0)
	installJoinRecord(replica, 1, 2, 0, []protocol.Header{second, first, root}, 7, 0)
	installJoinRecord(replica, 2, 1, 0, []protocol.Header{first, root}, 3, 0)
	commit, head, _, resolved := replica.selectCanonicalSuffix()
	if !resolved || commit != 2 || head != 2 {
		t.Fatalf("omitted prefix selected commit=%d head=%d resolved=%t", commit, head, resolved)
	}
}

func consensusBackup(t testing.TB) (*Replica, *captureBus) {
	t.Helper()
	config, storage := threeReplicaFormat(t)
	bus := &captureBus{}
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
	}}
	replica, err := Open(context.Background(), config, Dependencies{
		Storage: storage, MessageBus: bus, Clock: fixedClock{sample: TimeSample{Wall: 100, Monotonic: 10, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), StateMachine: machine, SynchronousIO: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeReplica(t, replica) })
	return replica, bus
}

func consensusPulse(t testing.TB, replica *Replica, parent protocol.Header, op protocol.Op, view protocol.View) (*Message, protocol.Header) {
	t.Helper()
	message, err := replica.frames.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	header := protocol.Header{Group: replica.config.Group, View: view, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPrepare, Author: replica.membership.Primary(view)}
	copy(header.Fields[:16], parent.HeaderChecksum[:])
	copy(header.Fields[64:80], replica.checkpointID[:])
	binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(op))
	binary.LittleEndian.PutUint64(header.Fields[112:120], uint64(op)+100)
	header.Fields[124] = byte(protocol.OperationPulse)
	if err := message.Seal(&header); err != nil {
		t.Fatal(err)
	}
	return message, header
}

func consensusRoot(t testing.TB, replica *Replica) protocol.Header {
	t.Helper()
	header, reason := protocol.DecodeHeader(replica.checkpoint.Header[:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	return header
}

func consensusProcess(t testing.TB, replica *Replica) {
	t.Helper()
	if _, err := replica.Process(64); err != nil {
		t.Fatal(err)
	}
}

func countConsensusCommand(t testing.TB, replica *Replica, bus *captureBus, command protocol.Command) int {
	t.Helper()
	count := 0
	for _, frame := range bus.replicaMessages() {
		header, _, reason := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount)
		if reason != protocol.RejectNone {
			t.Fatal(reason)
		}
		if header.Command == command {
			count++
		}
	}
	return count
}

func TestDurableDuplicatePrepareRestoresLostAcknowledgement(t *testing.T) {
	replica, bus := consensusBackup(t)
	message, header := consensusPulse(t, replica, consensusRoot(t, replica), 1, 0)
	if !replica.handlePrepare(message, header, replica.deps.Clock.Now()) {
		message.Release()
		t.Fatal("prepare rejected")
	}
	consensusProcess(t, replica)
	before := countConsensusCommand(t, replica, bus, protocol.CommandPrepareOK)
	duplicate, _ := consensusPulse(t, replica, consensusRoot(t, replica), 1, 0)
	if replica.handlePrepare(duplicate, header, replica.deps.Clock.Now()) {
		t.Fatal("duplicate retained")
	}
	duplicate.Release()
	if got := countConsensusCommand(t, replica, bus, protocol.CommandPrepareOK); got != before+1 {
		t.Fatalf("ack count=%d want=%d", got, before+1)
	}
	if replica.Snapshot().HeadOp != 1 {
		t.Fatalf("duplicate advanced head: %+v", replica.Snapshot())
	}
}

func TestOldWALCompletionCannotAcknowledgeDuringViewChange(t *testing.T) {
	replica, bus := consensusBackup(t)
	message, header := consensusPulse(t, replica, consensusRoot(t, replica), 1, 0)
	if !replica.handlePrepare(message, header, replica.deps.Clock.Now()) {
		message.Release()
		t.Fatal("prepare rejected")
	}
	replica.beginViewChange(1)
	consensusProcess(t, replica)
	if got := countConsensusCommand(t, replica, bus, protocol.CommandPrepareOK); got != 0 {
		t.Fatalf("premature acknowledgements=%d", got)
	}
}

func TestCanonicalInstallationPreservesCommittedPrefixAndTruncatesTraffic(t *testing.T) {
	replica, _ := consensusBackup(t)
	root := consensusRoot(t, replica)
	message, first := consensusPulse(t, replica, root, 1, 0)
	if !replica.handlePrepare(message, first, replica.deps.Clock.Now()) {
		message.Release()
		t.Fatal("first rejected")
	}
	consensusProcess(t, replica)
	replica.commitMax = 1
	consensusProcess(t, replica)
	message, second := consensusPulse(t, replica, first, 2, 0)
	if !replica.handlePrepare(message, second, replica.deps.Clock.Now()) {
		message.Release()
		t.Fatal("second rejected")
	}
	consensusProcess(t, replica)
	message, _ = consensusPulse(t, replica, second, 3, 0)
	frame, _ := message.Bytes()
	third, _, _ := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3)
	if !replica.handlePrepare(message, third, replica.deps.Clock.Now()) {
		message.Release()
		t.Fatal("third rejected")
	}
	consensusProcess(t, replica)
	body := make([]byte, CheckpointStateSize+2*protocol.HeaderSize)
	if err := replica.checkpoint.Encode(body[:CheckpointStateSize]); err != nil {
		t.Fatal(err)
	}
	if err := protocol.EncodeHeader(body[CheckpointStateSize:CheckpointStateSize+protocol.HeaderSize], &second); err != nil {
		t.Fatal(err)
	}
	if err := protocol.EncodeHeader(body[CheckpointStateSize+protocol.HeaderSize:], &root); err != nil {
		t.Fatal(err)
	}
	view := protocol.Header{Group: replica.config.Group, View: 2, Author: 2, Command: protocol.CommandView}
	binary.LittleEndian.PutUint64(view.Fields[16:24], 2)
	binary.LittleEndian.PutUint64(view.Fields[24:32], 1)
	replica.handleView(view, body)
	replica.continueRecoveringView(10)
	consensusProcess(t, replica)
	if got := replica.Snapshot(); got.Status != StatusNormal || got.HeadOp != 2 || got.CommitMin != 1 {
		t.Fatalf("installed state=%+v", got)
	}
	message, replacement := consensusPulse(t, replica, second, 3, 2)
	if !replica.handlePrepare(message, replacement, replica.deps.Clock.Now()) {
		message.Release()
		t.Fatal("post-truncation prepare rejected")
	}
	consensusProcess(t, replica)
	if got, found := replica.DurableChecksum(3); !found || got != replacement.HeaderChecksum {
		t.Fatalf("replacement durable checksum=%x found=%t", got, found)
	}
	if got, _ := replica.DurableChecksum(1); got != first.HeaderChecksum {
		t.Fatal("committed prefix changed")
	}
}

func TestViewNonceRejectedBeforeRepairWindowMutation(t *testing.T) {
	replica, _ := consensusBackup(t)
	replica.status = StatusRecoveringHead
	replica.getViewNonce = protocol.Nonce{1}
	root := consensusRoot(t, replica)
	body := make([]byte, CheckpointStateSize+protocol.HeaderSize)
	if err := replica.checkpoint.Encode(body[:CheckpointStateSize]); err != nil {
		t.Fatal(err)
	}
	if err := protocol.EncodeHeader(body[CheckpointStateSize:], &root); err != nil {
		t.Fatal(err)
	}
	header := protocol.Header{Group: replica.config.Group, View: 2, Author: 2, Command: protocol.CommandView}
	header.Fields[0] = 2
	binary.LittleEndian.PutUint64(header.Fields[16:24], 100)
	binary.LittleEndian.PutUint64(header.Fields[24:32], 100)
	replica.handleView(header, body)
	if replica.repairViewValid || replica.commitMax != 0 || replica.stateSync.stage != SyncStageIdle {
		t.Fatalf("mismatched nonce mutated state: %+v", replica.Snapshot())
	}
}

func TestCachedReplyEnvelopeTracksNewPrimary(t *testing.T) {
	for _, body := range [][]byte{nil, []byte("saved result")} {
		t.Run(string(body), func(t *testing.T) {
			replica, bus := consensusBackup(t)
			client := protocol.ClientID{7}
			frame, original := makeReplyFrame(t, client, 1, body)
			original.Group = replica.config.Group
			if err := protocol.SealFrame(frame, &original); err != nil {
				t.Fatal(err)
			}
			replica.view, replica.logView, replica.durableView = 2, 2, 2
			if len(body) == 0 {
				replica.sendHeaderOnlyCachedReply(client, original)
			} else {
				read := duplicateRead{busy: true, header: original, client: client}
				replica.finishDuplicateRead(&read, IOCompletion{Size: uint64(len(frame)), Buffer: frame})
			}
			got, gotBody, reason := protocol.DecodeFrame(bus.clientMessage(t, 0), replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3)
			if reason != protocol.RejectNone || got.View != 2 || got.Author != 2 || !bytes.Equal(gotBody, body) || replyContext(&got) != replyContext(&original) {
				t.Fatalf("cached envelope view=%d author=%d body=%q reason=%v", got.View, got.Author, gotBody, reason)
			}
			if reason := protocol.ValidateSemantics(&got, gotBody, replica.validationContext(protocol.FrameSourceReplica, replica.local)); reason != protocol.RejectNone {
				t.Fatalf("cached reply semantic rejection=%v", reason)
			}
			if original.View != 0 || original.Author != 0 {
				t.Fatal("persisted identity mutated")
			}
		})
	}
}

type consensusRoutingBus struct {
	captureBus
	destination protocol.ReplicaIndex
}

func (bus *consensusRoutingBus) SendReplica(destination protocol.ReplicaIndex, message *Message) {
	bus.destination = destination
	bus.capture(message, false)
}

func TestHigherViewEvidenceRequestsEvidencePrimary(t *testing.T) {
	for _, command := range []protocol.Command{protocol.CommandPrepare, protocol.CommandCommit, protocol.CommandPing, protocol.CommandJoinView} {
		t.Run(strconv.Itoa(int(command)), func(t *testing.T) {
			replica, _ := consensusBackup(t)
			bus := &consensusRoutingBus{}
			replica.deps.MessageBus = bus
			if command == protocol.CommandPing || command == protocol.CommandJoinView {
				replica.status = StatusRecoveringHead
			}
			replica.handleHigherViewEvidence(protocol.Header{Command: command, View: 2, Author: 2})
			header, _, reason := protocol.DecodeFrame(bus.replicaMessage(t, 0), replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3)
			if reason != protocol.RejectNone || header.Command != protocol.CommandGetView || header.View != 2 || bus.destination != 2 || protocol.Nonce(header.Fields[:16]) == (protocol.Nonce{}) {
				t.Fatalf("request destination=%d header=%+v reason=%v", bus.destination, header, reason)
			}
			if replica.view != 0 {
				t.Fatalf("evidence prematurely installed view %d", replica.view)
			}
		})
	}
}

func TestSameViewLaggingBackupRequestsAndInstallsView(t *testing.T) {
	replica, bus := consensusBackup(t)
	root := consensusRoot(t, replica)
	firstMessage, first := consensusPulse(t, replica, root, 1, 0)
	firstMessage.Release()
	secondMessage, second := consensusPulse(t, replica, first, 2, 0)
	if replica.handlePrepare(secondMessage, second, replica.deps.Clock.Now()) {
		t.Fatal("gap accepted")
	}
	secondMessage.Release()
	request, _, reason := protocol.DecodeFrame(bus.replicaMessage(t, 0), replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3)
	if reason != protocol.RejectNone || request.Command != protocol.CommandGetView {
		t.Fatalf("gap response=%+v reason=%v", request, reason)
	}
	body := make([]byte, CheckpointStateSize+3*protocol.HeaderSize)
	if err := replica.checkpoint.Encode(body[:CheckpointStateSize]); err != nil {
		t.Fatal(err)
	}
	for index, header := range []protocol.Header{second, first, root} {
		if err := protocol.EncodeHeader(body[CheckpointStateSize+index*protocol.HeaderSize:CheckpointStateSize+(index+1)*protocol.HeaderSize], &header); err != nil {
			t.Fatal(err)
		}
	}
	view := protocol.Header{Group: replica.config.Group, Author: 0, View: 0, Command: protocol.CommandView}
	copy(view.Fields[:16], request.Fields[:16])
	binary.LittleEndian.PutUint64(view.Fields[16:24], 2)
	replica.handleView(view, body)
	replica.continueRecoveringView(10)
	if got := countConsensusCommand(t, replica, bus, protocol.CommandGetPrepare); got != 1 {
		t.Fatalf("same-view catchup repair requests=%d", got)
	}
}

func TestRegistrationAdmissionCannotEvictPendingApplicationSession(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	bus := &captureBus{}
	machine := &testStateMachine{capacities: StateMachineCapacities{RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax), PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1}}
	replica, err := newReplica(config, Dependencies{Storage: storage, MessageBus: bus, Clock: fixedClock{sample: TimeSample{Wall: 100, Monotonic: 10, Synchronized: true}}, Entropy: bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), StateMachine: machine, SynchronousIO: true}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, replica)
	for index := uint64(1); index <= config.Cluster.ClientsMax; index++ {
		request := makeClientRequest(t, replica.frames, config.Group, protocol.ClientID{byte(index)}, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
		if err := replica.Submit(request); err != nil {
			t.Fatal(err)
		}
		processReplicaUntil(t, replica, protocol.Op(index))
	}
	old, _, found := sessions.Reply(protocol.ClientID{1}, 1, 0)
	if !found {
		t.Fatal("oldest session missing before eviction")
	}
	registration := makeClientRequest(t, replica.frames, config.Group, protocol.ClientID{99}, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
	frame, _ := registration.Bytes()
	header, body, _ := protocol.DecodeFrame(frame, config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if !replica.handleRequest(registration, header, body, replica.deps.Clock.Now()) {
		registration.Release()
	}
	application := makeClientRequest(t, replica.frames, config.Group, protocol.ClientID{1}, 1, 1, replyContext(&old), protocol.OperationApplicationMin, []byte("must not execute"))
	frame, _ = application.Bytes()
	header, body, _ = protocol.DecodeFrame(frame, config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if !replica.handleRequest(application, header, body, replica.deps.Clock.Now()) {
		application.Release()
	}
	processReplicaUntil(t, replica, protocol.Op(config.Cluster.ClientsMax+1))
	consensusProcess(t, replica)
	if machine.commits != 0 {
		t.Fatalf("evicted session executed %d application requests", machine.commits)
	}
	if got := replica.Snapshot().HeadOp; got != protocol.Op(config.Cluster.ClientsMax+1) {
		t.Fatalf("evicted request entered log at head=%d", got)
	}
}

func TestCheckpointProgressReleasesWithheldPrepareAcknowledgement(t *testing.T) {
	replica, bus := consensusBackup(t)
	root := consensusRoot(t, replica)
	next, ok := checkpointAfter(replica.config.Cluster, 0)
	if !ok {
		t.Fatal("checkpoint unavailable")
	}
	trigger, ok := checkpointTrigger(replica.config.Cluster, next)
	if !ok {
		t.Fatal("checkpoint trigger unavailable")
	}
	op := trigger + protocol.Op(replica.config.Cluster.PipelineMax) + 1
	message, header := consensusPulse(t, replica, root, op, 0)
	entry, ok := replica.pushPipeline(message, header)
	if !ok {
		message.Release()
		t.Fatal("pipeline full")
	}
	entry.durable = true
	replica.commitMin, replica.commitMax = op-1, op-1
	consensusProcess(t, replica)
	if got := countConsensusCommand(t, replica, bus, protocol.CommandPrepareOK); got != 0 {
		t.Fatalf("unsafe checkpoint acknowledgements=%d", got)
	}
	checkpointMessage, checkpointHeader := consensusPulse(t, replica, root, next, 0)
	checkpointMessage.Release()
	if err := protocol.EncodeHeader(replica.checkpoint.Header[:], &checkpointHeader); err != nil {
		t.Fatal(err)
	}
	consensusProcess(t, replica)
	if got := countConsensusCommand(t, replica, bus, protocol.CommandPrepareOK); got != 1 {
		t.Fatalf("checkpoint advancement released %d acknowledgements, want 1", got)
	}
	consensusProcess(t, replica)
	if got := countConsensusCommand(t, replica, bus, protocol.CommandPrepareOK); got != 1 {
		t.Fatalf("idle processing repeated acknowledgement: %d", got)
	}
}
