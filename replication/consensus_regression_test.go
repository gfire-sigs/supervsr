package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	"z.gfire.dev/supervsr/replication/protocol"
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
	if err := protocol.SealFrame(frame, &second, protocol.ChecksumAEGIS128L); err != nil {
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
	if err := message.Seal(&header, protocol.ChecksumAEGIS128L); err != nil {
		t.Fatal(err)
	}
	return message, header
}

func consensusRoot(t testing.TB, replica *Replica) protocol.Header {
	t.Helper()
	header, reason := protocol.DecodeHeader(replica.checkpoint.Header[:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount, protocol.ChecksumAEGIS128L)
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
		header, _, reason := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount, protocol.ChecksumAEGIS128L)
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
	third, _, _ := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3, protocol.ChecksumAEGIS128L)
	if !replica.handlePrepare(message, third, replica.deps.Clock.Now()) {
		message.Release()
		t.Fatal("third rejected")
	}
	consensusProcess(t, replica)
	body := make([]byte, CheckpointStateSize+2*protocol.HeaderSize)
	if err := replica.checkpoint.encode(body[:CheckpointStateSize]); err != nil {
		t.Fatal(err)
	}
	if err := protocol.EncodeHeader(body[CheckpointStateSize:CheckpointStateSize+protocol.HeaderSize], &second, protocol.ChecksumAEGIS128L); err != nil {
		t.Fatal(err)
	}
	if err := protocol.EncodeHeader(body[CheckpointStateSize+protocol.HeaderSize:], &root, protocol.ChecksumAEGIS128L); err != nil {
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
	if err := replica.checkpoint.encode(body[:CheckpointStateSize]); err != nil {
		t.Fatal(err)
	}
	if err := protocol.EncodeHeader(body[CheckpointStateSize:], &root, protocol.ChecksumAEGIS128L); err != nil {
		t.Fatal(err)
	}
	header := protocol.Header{Group: replica.config.Group, View: 2, Author: 2, Command: protocol.CommandView}
	header.Fields[0] = 2
	binary.LittleEndian.PutUint64(header.Fields[16:24], 100)
	binary.LittleEndian.PutUint64(header.Fields[24:32], 100)
	replica.handleView(header, body)
	if replica.repairViewValid || replica.commitMax != 0 || replica.stateSync.stage != syncStageIdle {
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
			if err := protocol.SealFrame(frame, &original, protocol.ChecksumAEGIS128L); err != nil {
				t.Fatal(err)
			}
			replica.view, replica.logView, replica.durableView = 2, 2, 2
			if len(body) == 0 {
				replica.sendHeaderOnlyCachedReply(client, original)
			} else {
				read := duplicateRead{busy: true, header: original, client: client}
				replica.finishDuplicateRead(&read, ioCompletion{Size: uint64(len(frame)), Buffer: frame})
			}
			got, gotBody, reason := protocol.DecodeFrame(bus.clientMessage(t, 0), replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3, protocol.ChecksumAEGIS128L)
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
			header, _, reason := protocol.DecodeFrame(bus.replicaMessage(t, 0), replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3, protocol.ChecksumAEGIS128L)
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
	request, _, reason := protocol.DecodeFrame(bus.replicaMessage(t, 0), replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3, protocol.ChecksumAEGIS128L)
	if reason != protocol.RejectNone || request.Command != protocol.CommandGetView {
		t.Fatalf("gap response=%+v reason=%v", request, reason)
	}
	body := make([]byte, CheckpointStateSize+3*protocol.HeaderSize)
	if err := replica.checkpoint.encode(body[:CheckpointStateSize]); err != nil {
		t.Fatal(err)
	}
	for index, header := range []protocol.Header{second, first, root} {
		if err := protocol.EncodeHeader(body[CheckpointStateSize+index*protocol.HeaderSize:CheckpointStateSize+(index+1)*protocol.HeaderSize], &header, protocol.ChecksumAEGIS128L); err != nil {
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
	header, body, _ := protocol.DecodeFrame(frame, config.Group, uint32(config.Cluster.MessageSizeMax), 1, protocol.ChecksumAEGIS128L)
	if !replica.handleRequest(registration, header, body, replica.deps.Clock.Now()) {
		registration.Release()
	}
	application := makeClientRequest(t, replica.frames, config.Group, protocol.ClientID{1}, 1, 1, replyContext(&old), protocol.OperationApplicationMin, []byte("must not execute"))
	frame, _ = application.Bytes()
	header, body, _ = protocol.DecodeFrame(frame, config.Group, uint32(config.Cluster.MessageSizeMax), 1, protocol.ChecksumAEGIS128L)
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
	if err := protocol.EncodeHeader(replica.checkpoint.Header[:], &checkpointHeader, protocol.ChecksumAEGIS128L); err != nil {
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

func TestHandleViewSupersedesStaleRepairView(t *testing.T) {
	replica, _ := consensusBackup(t)
	root := consensusRoot(t, replica)
	encodeViewBody := func(headers ...protocol.Header) []byte {
		body := make([]byte, CheckpointStateSize+len(headers)*protocol.HeaderSize)
		if err := replica.checkpoint.encode(body[:CheckpointStateSize]); err != nil {
			t.Fatal(err)
		}
		for index := range headers {
			start := CheckpointStateSize + index*protocol.HeaderSize
			if err := protocol.EncodeHeader(body[start:start+protocol.HeaderSize], &headers[index], protocol.ChecksumAEGIS128L); err != nil {
				t.Fatal(err)
			}
		}
		return body
	}
	makeViewHeader := func(view protocol.View, head, commit protocol.Op) protocol.Header {
		header := protocol.Header{
			Group:   replica.config.Group,
			View:    view,
			Author:  replica.membership.Primary(view),
			Command: protocol.CommandView,
		}
		binary.LittleEndian.PutUint64(header.Fields[16:24], uint64(head))
		binary.LittleEndian.PutUint64(header.Fields[24:32], uint64(commit))
		return header
	}

	staleMsg, staleFirst := consensusPulse(t, replica, root, 1, 2)
	staleMsg.Release()
	replica.handleView(makeViewHeader(2, 1, 0), encodeViewBody(staleFirst, root))
	if replica.status != StatusRecoveringHead || !replica.repairViewValid || replica.repairView != 2 {
		t.Fatalf("initial repair state status=%d valid=%t repairView=%d", replica.status, replica.repairViewValid, replica.repairView)
	}
	if replica.canonicalHeaders[0].HeaderChecksum != staleFirst.HeaderChecksum {
		t.Fatal("initial repair view did not install canonical header")
	}

	badSecondMsg, badSecond := consensusPulse(t, replica, staleFirst, 2, 3)
	badSecondMsg.Release()
	replica.handleView(makeViewHeader(3, 2, 1), encodeViewBody(badSecond, root))
	if replica.repairView != 2 || replica.canonicalHeaders[0].HeaderChecksum != staleFirst.HeaderChecksum {
		t.Fatalf("rejected higher view mutated repair state: repairView=%d", replica.repairView)
	}

	sameViewMsg, sameViewAlt := consensusPulse(t, replica, root, 1, 2)
	sameViewMsg.Release()
	replica.handleView(makeViewHeader(2, 1, 1), encodeViewBody(sameViewAlt, root))
	if replica.repairView != 2 || replica.canonicalHeaders[0].HeaderChecksum != staleFirst.HeaderChecksum {
		t.Fatalf("same-view response overwrote active repair view: repairView=%d", replica.repairView)
	}

	replacementMsg, replacement := consensusPulse(t, replica, root, 1, 3)
	replica.handleView(makeViewHeader(3, 1, 1), encodeViewBody(replacement, root))
	if !replica.repairViewValid || replica.repairView != 3 {
		t.Fatalf("higher view did not supersede stale repair view: valid=%t repairView=%d", replica.repairViewValid, replica.repairView)
	}
	if replica.canonicalHeaders[0].HeaderChecksum != replacement.HeaderChecksum {
		t.Fatal("superseding view did not install replacement header")
	}

	replica.continueRecoveringView(20)
	if !replica.handleRepairPrepare(replacementMsg, replacement, replica.deps.Clock.Now()) {
		replacementMsg.Release()
		t.Fatal("replacement prepare rejected")
	}
	for range 8 {
		if replica.Snapshot().Status == StatusNormal {
			break
		}
		consensusProcess(t, replica)
	}
	got := replica.Snapshot()
	if got.Status != StatusNormal || got.View != 3 || got.DurableView != 3 || got.LogView != 3 {
		t.Fatalf("superseded recovery view state = %+v", got)
	}
	if got.HeadOp != 1 || got.CommitMin != 1 || got.CommitMax != 1 {
		t.Fatalf("superseded recovery progress = %+v", got)
	}
}

func encodeTestViewBody(t testing.TB, replica *Replica, headers ...protocol.Header) []byte {
	t.Helper()
	body := make([]byte, CheckpointStateSize+len(headers)*protocol.HeaderSize)
	if err := replica.checkpoint.encode(body[:CheckpointStateSize]); err != nil {
		t.Fatal(err)
	}
	for index := range headers {
		start := CheckpointStateSize + index*protocol.HeaderSize
		if err := protocol.EncodeHeader(body[start:start+protocol.HeaderSize], &headers[index], protocol.ChecksumAEGIS128L); err != nil {
			t.Fatal(err)
		}
	}
	return body
}

func makeTestViewHeader(replica *Replica, view protocol.View, head, commit protocol.Op, nonce protocol.Nonce) protocol.Header {
	header := protocol.Header{
		Group:   replica.config.Group,
		View:    view,
		Author:  replica.membership.Primary(view),
		Command: protocol.CommandView,
	}
	copy(header.Fields[:16], nonce[:])
	binary.LittleEndian.PutUint64(header.Fields[16:24], uint64(head))
	binary.LittleEndian.PutUint64(header.Fields[24:32], uint64(commit))
	binary.LittleEndian.PutUint64(header.Fields[32:40], uint64(replica.checkpoint.PrepareOp()))
	return header
}

func consensusProcessUntilNormal(t testing.TB, replica *Replica) {
	t.Helper()
	for range 16 {
		if replica.Snapshot().Status == StatusNormal {
			return
		}
		consensusProcess(t, replica)
	}
}

func consensusPulseWithCommit(t testing.TB, replica *Replica, parent protocol.Header, op, commit protocol.Op, view protocol.View) (*Message, protocol.Header) {
	t.Helper()
	message, err := replica.frames.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	header := protocol.Header{Group: replica.config.Group, View: view, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPrepare, Author: replica.membership.Primary(view)}
	copy(header.Fields[:16], parent.HeaderChecksum[:])
	copy(header.Fields[64:80], replica.checkpointID[:])
	binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(op))
	binary.LittleEndian.PutUint64(header.Fields[104:112], uint64(commit))
	binary.LittleEndian.PutUint64(header.Fields[112:120], uint64(op)+100)
	header.Fields[124] = byte(protocol.OperationPulse)
	if err := message.Seal(&header, protocol.ChecksumAEGIS128L); err != nil {
		t.Fatal(err)
	}
	return message, header
}

func TestRepairWindowPreservesAckedSameViewSuffix(t *testing.T) {
	replica, bus := consensusBackup(t)
	root := consensusRoot(t, replica)

	headers := make([]protocol.Header, 6)
	headers[0] = root
	for op := protocol.Op(1); op <= 4; op++ {
		message, header := consensusPulse(t, replica, headers[op-1], op, 0)
		if !replica.handlePrepare(message, header, replica.deps.Clock.Now()) {
			message.Release()
			t.Fatalf("prepare %d rejected", op)
		}
		headers[op] = header
		consensusProcess(t, replica)
	}
	if got := replica.Snapshot(); got.HeadOp != 4 || got.CommitMin != 0 || got.PipelineLen != 4 {
		t.Fatalf("unexpected pre-catchup state: %+v", got)
	}
	if acks := countConsensusCommand(t, replica, bus, protocol.CommandPrepareOK); acks != 4 {
		t.Fatalf("prepareOK count = %d, want 4", acks)
	}

	msg5, hdr5 := consensusPulseWithCommit(t, replica, headers[4], 5, 3, 0)
	headers[5] = hdr5
	if replica.handlePrepare(msg5, hdr5, replica.deps.Clock.Now()) {
		t.Fatal("advanced prepare unexpectedly accepted before commit catchup")
	}
	msg5.Release()

	var nonce protocol.Nonce
	for _, frame := range bus.replicaMessages() {
		decoded, _, reason := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3, protocol.ChecksumAEGIS128L)
		if reason == protocol.RejectNone && decoded.Command == protocol.CommandGetView {
			copy(nonce[:], decoded.Fields[:16])
		}
	}
	if nonce == (protocol.Nonce{}) {
		t.Fatal("expected CommandGetView with non-zero nonce")
	}

	viewBody := encodeTestViewBody(t, replica, headers[5], headers[4], headers[3], headers[2], root)
	replica.handleView(makeTestViewHeader(replica, 0, 5, 3, nonce), viewBody)
	consensusProcess(t, replica)

	got := replica.Snapshot()
	if got.Status != StatusNormal || got.CommitMin != 3 || got.CommitMax != 3 {
		t.Fatalf("same-view catchup status/commit mismatch: %+v", got)
	}
	if got.HeadOp != 4 || got.PipelineLen != 1 {
		t.Fatalf("same-view catchup discarded acked suffix: %+v", got)
	}
	if checksum, found := replica.DurableChecksum(4); !found || checksum != headers[4].HeaderChecksum {
		t.Fatalf("acked op 4 discarded or corrupted: found=%t checksum=%x want=%x", found, checksum, headers[4].HeaderChecksum)
	}

	retryMsg5, retryHdr5 := consensusPulseWithCommit(t, replica, headers[4], 5, 3, 0)
	if !replica.handlePrepare(retryMsg5, retryHdr5, replica.deps.Clock.Now()) {
		retryMsg5.Release()
		t.Fatal("post-catchup prepare 5 rejected")
	}
	consensusProcess(t, replica)
	if got := replica.Snapshot(); got.HeadOp != 5 || got.CommitMin != 3 {
		t.Fatalf("post-catchup state = %+v", got)
	}
}

func TestRepairWindowDoesNotStampLogViewBeforeCanonicalHead(t *testing.T) {
	replica, bus := consensusBackup(t)
	root := consensusRoot(t, replica)

	frames := make([]*Message, 6)
	headers := make([]protocol.Header, 6)
	headers[0] = root
	for op := protocol.Op(1); op <= 5; op++ {
		msg, hdr := consensusPulse(t, replica, headers[op-1], op, 2)
		frames[op] = msg
		headers[op] = hdr
	}
	t.Cleanup(func() {
		for op := 1; op <= 5; op++ {
			if frames[op] != nil {
				frames[op].Release()
			}
		}
	})

	viewBody := encodeTestViewBody(t, replica, headers[5], headers[4], headers[3], headers[2], root)
	replica.handleView(makeTestViewHeader(replica, 3, 5, 4, protocol.Nonce{}), viewBody)
	if !replica.repairViewValid || !replica.repairViewPartial || replica.repairViewHead != 4 {
		t.Fatalf("expected partial repair window up to 4: valid=%t partial=%t head=%d", replica.repairViewValid, replica.repairViewPartial, replica.repairViewHead)
	}

	hdrBody := make([]byte, 4*protocol.HeaderSize)
	for index := range 4 {
		if err := protocol.EncodeHeader(hdrBody[index*protocol.HeaderSize:(index+1)*protocol.HeaderSize], &headers[index+1], protocol.ChecksumAEGIS128L); err != nil {
			t.Fatal(err)
		}
	}
	replica.handleHeaders(protocol.Header{Group: replica.config.Group, Author: 0, Command: protocol.CommandHeaders}, hdrBody)

	for op := protocol.Op(1); op <= 4; op++ {
		replica.continueRecoveringView(uint64(op) * 100)
		msg := frames[op]
		frames[op] = nil
		if !replica.handleRepairPrepare(msg, headers[op], replica.deps.Clock.Now()) {
			msg.Release()
			t.Fatalf("repair prepare %d rejected", op)
		}
		consensusProcess(t, replica)
	}
	consensusProcess(t, replica)

	partial := replica.Snapshot()
	if partial.CommitMin != 4 || partial.CommitMax != 4 || partial.HeadOp != 4 {
		t.Fatalf("partial window execution mismatch: %+v", partial)
	}
	if partial.View != 3 || partial.DurableView != 3 || partial.LogView != 0 || partial.Status != StatusViewChange {
		t.Fatalf("partial window stamped premature LogView or Normal status: %+v", partial)
	}

	sentJoin := false
	for _, frame := range bus.replicaMessages() {
		decoded, _, reason := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 3, protocol.ChecksumAEGIS128L)
		if reason == protocol.RejectNone && decoded.Command == protocol.CommandJoinView && decoded.View == 3 {
			logView := protocol.View(binary.LittleEndian.Uint32(decoded.Fields[56:60]))
			if logView != 0 {
				t.Fatalf("partial window JoinView advertised premature logView=%d", logView)
			}
			sentJoin = true
		}
	}
	if !sentJoin {
		t.Fatal("expected CommandJoinView in view 3 after partial repair window")
	}

	// Because LogView remained 0, a subsequent view change preserves sender 2's higher-logView op 5.
	joinProbe, _, _, _ := canonicalFixture(t)
	joinProbe.view = 4
	installJoinRecord(joinProbe, 1, 4, 4, []protocol.Header{headers[4], headers[3], headers[2], headers[1], root}, 0b11111, 0)
	installJoinRecord(joinProbe, 2, 5, 4, []protocol.Header{headers[5], headers[4], headers[3], headers[2], headers[1]}, 0b11111, 0)
	joinProbe.joins[2].logView = 2
	selCommit, selHead, _, selResolved := joinProbe.selectCanonicalSuffix()
	if !selResolved || selCommit != 4 || selHead != 5 {
		t.Fatalf("view-4 selection lost higher-logView op 5: commit=%d head=%d resolved=%t", selCommit, selHead, selResolved)
	}

	replica.handleView(makeTestViewHeader(replica, 3, 5, 4, protocol.Nonce{}), viewBody)
	replica.continueRecoveringView(600)
	msg5 := frames[5]
	frames[5] = nil
	if !replica.handleRepairPrepare(msg5, headers[5], replica.deps.Clock.Now()) {
		msg5.Release()
		t.Fatal("repair prepare 5 rejected")
	}
	consensusProcessUntilNormal(t, replica)

	installed := replica.Snapshot()
	if installed.Status != StatusNormal || installed.View != 3 || installed.DurableView != 3 || installed.LogView != 3 {
		t.Fatalf("full canonical head did not install view 3: %+v", installed)
	}
	if installed.HeadOp != 5 || installed.CommitMin != 4 {
		t.Fatalf("full canonical head progress mismatch: %+v", installed)
	}
}

func TestLaggingPrimarySelectCanonicalSuffixBridgesCommittedGap(t *testing.T) {
	config, _ := threeReplicaFormat(t)
	config.Membership.LocalMember = config.Membership.Members[2]
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	storage := &crashStorage{}
	if err := Format(context.Background(), FormatConfig{Group: config.Group, Membership: config.Membership, Cluster: config.Cluster, CurrentRelease: 1}, FormatDependencies{Storage: storage}); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
	}}
	replica, err := Open(context.Background(), config, Dependencies{
		Storage: storage, MessageBus: &captureBus{}, Clock: fixedClock{sample: TimeSample{Wall: 100, Monotonic: 10, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), StateMachine: machine, SynchronousIO: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeReplica(t, replica) })

	root := consensusRoot(t, replica)
	m1, h1 := consensusPulse(t, replica, root, 1, 0)
	if !replica.handlePrepare(m1, h1, replica.deps.Clock.Now()) {
		m1.Release()
		t.Fatal("prepare 1 rejected")
	}
	consensusProcess(t, replica)
	replica.commitMax = 1
	consensusProcess(t, replica)

	// Install view 2 where local replica 2 is primary, then prepare divergent uncommitted 2..3.
	view2Body := encodeTestViewBody(t, replica, h1, root)
	replica.handleView(makeTestViewHeader(replica, 2, 1, 1, protocol.Nonce{}), view2Body)
	consensusProcessUntilNormal(t, replica)
	if got := replica.Snapshot(); got.Status != StatusNormal || got.View != 2 || got.LogView != 2 || !replica.isPrimary() {
		t.Fatalf("failed to install view 2 on primary 2: %+v", got)
	}

	m2Old, h2Old := consensusPulse(t, replica, h1, 2, 2)
	if !replica.acceptPrepare(m2Old, h2Old) {
		m2Old.Release()
		t.Fatal("h2_old rejected")
	}
	consensusProcess(t, replica)
	m3Old, h3Old := consensusPulse(t, replica, h2Old, 3, 2)
	if !replica.acceptPrepare(m3Old, h3Old) {
		m3Old.Release()
		t.Fatal("h3_old rejected")
	}
	consensusProcess(t, replica)

	// Enter view 5 (primary is also replica 2) with CommitMin=1, HeadOp=3.
	replica.beginViewChange(5)
	consensusProcess(t, replica)
	preJoin := replica.Snapshot()
	if preJoin.Status != StatusViewChange || preJoin.View != 5 || preJoin.DurableView != 5 {
		t.Fatalf("unexpected view 5 view state: %+v", preJoin)
	}
	if preJoin.CommitMin != 1 || preJoin.HeadOp != 3 {
		t.Fatalf("unexpected view 5 log progress: %+v", preJoin)
	}

	m2Can, h2Can := consensusPulse(t, replica, h1, 2, 4)
	m3Can, h3Can := consensusPulse(t, replica, h2Can, 3, 4)
	m4Can, h4Can := consensusPulse(t, replica, h3Can, 4, 4)
	canonicalMsgs := []*Message{m2Can, m3Can, m4Can}
	canonicalHdrs := []protocol.Header{h2Can, h3Can, h4Can}
	t.Cleanup(func() {
		for _, m := range canonicalMsgs {
			if m != nil {
				m.Release()
			}
		}
	})

	joinHdrs := []protocol.Header{h4Can, h3Can, h2Can, h1, root}
	joinBody := make([]byte, len(joinHdrs)*protocol.HeaderSize)
	for index := range joinHdrs {
		start := index * protocol.HeaderSize
		if err := protocol.EncodeHeader(joinBody[start:start+protocol.HeaderSize], &joinHdrs[index], protocol.ChecksumAEGIS128L); err != nil {
			t.Fatal(err)
		}
	}
	joinHeader := protocol.Header{Group: replica.config.Group, View: 5, Protocol: protocol.ProtocolVersion, Command: protocol.CommandJoinView, Author: 0}
	binary.LittleEndian.PutUint16(joinHeader.Fields[0:2], 0b11111)
	binary.LittleEndian.PutUint64(joinHeader.Fields[32:40], 4)
	binary.LittleEndian.PutUint64(joinHeader.Fields[40:48], 4)
	binary.LittleEndian.PutUint32(joinHeader.Fields[56:60], 4)
	replica.handleJoinView(joinHeader, joinBody)

	if replica.status != StatusRecoveringHead || !replica.repairViewValid {
		t.Fatalf("lagging primary did not enter RecoveringHead: status=%d valid=%t", replica.status, replica.repairViewValid)
	}
	if replica.repairViewAncestor != 1 || replica.repairViewCommit != 4 || replica.repairViewHead != 4 {
		t.Fatalf("lagging primary did not bridge canonical gap: anc=%d commit=%d head=%d",
			replica.repairViewAncestor, replica.repairViewCommit, replica.repairViewHead)
	}

	for index := range canonicalMsgs {
		replica.continueRecoveringView(uint64(index+1) * 100)
		msg := canonicalMsgs[index]
		canonicalMsgs[index] = nil
		if !replica.handleRepairPrepare(msg, canonicalHdrs[index], replica.deps.Clock.Now()) {
			msg.Release()
			t.Fatalf("canonical repair prepare %x rejected", canonicalHdrs[index].HeaderChecksum[:4])
		}
		consensusProcess(t, replica)
	}
	consensusProcessUntilNormal(t, replica)

	got := replica.Snapshot()
	if got.Status != StatusNormal || got.View != 5 || got.DurableView != 5 || got.LogView != 5 {
		t.Fatalf("bridged canonical view state mismatch: %+v", got)
	}
	if got.CommitMin != 4 || got.HeadOp != 4 {
		t.Fatalf("bridged canonical view progress mismatch: %+v", got)
	}
	for index, expected := range canonicalHdrs {
		op := protocol.Op(index + 2)
		if checksum, found := replica.DurableChecksum(op); !found || checksum != expected.HeaderChecksum {
			t.Fatalf("op %d checksum mismatch: found=%t got=%x want=%x", op, found, checksum, expected.HeaderChecksum)
		}
	}
}

func TestCanonicalUncertainLatestLogViewDoesNotTruncateCommittedOp(t *testing.T) {
	cluster := compactTestClusterConfig()
	membership := Membership{
		Members:     [MembersMax]protocol.MemberID{{1}, {2}, {3}, {4}, {5}},
		ActiveCount: 5,
		LocalMember: protocol.MemberID{1},
	}
	rootBytes, err := rootPrepareHeader(protocol.GroupID{1}, 5, uint32(cluster.MessageSizeMax), ChecksumAEGIS128L)
	if err != nil {
		t.Fatal(err)
	}
	root, reason := protocol.DecodeHeader(rootBytes[:], protocol.GroupID{1}, uint32(cluster.MessageSizeMax), 5, ChecksumAEGIS128L)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	firstFrame := connectedPrepareFrame(t, cluster, root, 1)
	first, _, reason := protocol.DecodeFrame(firstFrame, protocol.GroupID{1}, uint32(cluster.MessageSizeMax), 5, ChecksumAEGIS128L)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	quorums, ok := QuorumsFor(5, 3)
	if !ok {
		t.Fatal("quorums unavailable")
	}
	makeReplica := func() *Replica {
		return &Replica{
			config:           Config{Group: protocol.GroupID{1}, Membership: membership, Cluster: cluster},
			membership:       membership,
			quorums:          quorums,
			checkpoint:       CheckpointState{Header: rootBytes},
			joins:            make([]joinRecord, 5),
			joinHeaders:      make([]protocol.Header, 5*int(cluster.PipelineMax+1)),
			canonicalHeaders: make([]protocol.Header, int(cluster.PipelineMax+1)),
		}
	}

	replica := makeReplica()
	// Senders 0 and 1 hold durable op 1 from LogView 0; sender 2 is at HeadOp 0;
	// sender 3 is at LogView 1 with an uncertain faulty blank slot at op 1 (Present=0, Nack=0).
	installJoinRecord(replica, 0, 1, 0, []protocol.Header{first, root}, 0b11, 0)
	installJoinRecord(replica, 1, 1, 0, []protocol.Header{first, root}, 0b11, 0)
	installJoinRecord(replica, 2, 0, 0, []protocol.Header{root}, 0b1, 0)
	installJoinRecord(replica, 3, 1, 0, []protocol.Header{{}, root}, 0b10, 0)
	replica.joins[3].logView = 1

	negatives, copies := replica.canonicalEvidence(1, protocol.Header{}, false)
	if negatives >= quorums.Negative || copies != 0 {
		t.Fatalf("uncertain latest LogView produced negatives=%d (Qn=%d) copies=%d", negatives, quorums.Negative, copies)
	}
	if _, _, _, resolved := replica.selectCanonicalSuffix(); resolved {
		t.Fatal("uncertain latest LogView truncated potentially committed op 1")
	}

	// Contrast: when sender 3 explicitly NACKs op 1 (Nack bit set), truncation proceeds cleanly.
	explicit := makeReplica()
	installJoinRecord(explicit, 0, 1, 0, []protocol.Header{first, root}, 0b11, 0)
	installJoinRecord(explicit, 1, 1, 0, []protocol.Header{first, root}, 0b11, 0)
	installJoinRecord(explicit, 2, 0, 0, []protocol.Header{root}, 0b1, 0)
	installJoinRecord(explicit, 3, 1, 0, []protocol.Header{{}, root}, 0b10, 0b01)
	explicit.joins[3].logView = 1

	commit, head, count, resolved := explicit.selectCanonicalSuffix()
	if !resolved || commit != 0 || head != 0 || count != 1 {
		t.Fatalf("explicit latest NACK selection = commit=%d head=%d count=%d resolved=%t", commit, head, count, resolved)
	}
}

func consensusPrimary(t testing.TB, synchronized bool) (*Replica, *captureBus) {
	t.Helper()
	config, _ := threeReplicaFormat(t)
	config.Membership.LocalMember = config.Membership.Members[0]
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	storage := &crashStorage{}
	if err := Format(context.Background(), FormatConfig{Group: config.Group, Membership: config.Membership, Cluster: config.Cluster, CurrentRelease: 1}, FormatDependencies{Storage: storage}); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	bus := &captureBus{}
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
	}}
	replica, err := Open(context.Background(), config, Dependencies{
		Storage: storage, MessageBus: bus, Clock: fixedClock{sample: TimeSample{Wall: 100, Monotonic: 10, Synchronized: synchronized}},
		Entropy: bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), StateMachine: machine, SynchronousIO: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeReplica(t, replica) })
	// Open fences a restarted pre-crash primary into StatusViewChange(1); install View 3 where member 0 leads.
	root := consensusRoot(t, replica)
	viewBody := encodeTestViewBody(t, replica, root)
	replica.handleView(makeTestViewHeader(replica, 3, 0, 0, protocol.Nonce{}), viewBody)
	consensusProcessUntilNormal(t, replica)
	if snap := replica.Snapshot(); snap.Status != StatusNormal || snap.View != 3 || !replica.isPrimary() {
		t.Fatalf("failed to establish normal primary: %+v", snap)
	}
	return replica, bus
}

func TestPrimaryAbdicationSuppressesCommitHeartbeatsAndResumesOnQuorum(t *testing.T) {
	replica, bus := consensusPrimary(t, true)
	msg, hdr := consensusPulse(t, replica, consensusRoot(t, replica), 1, replica.view)
	if !replica.acceptPrepare(msg, hdr) {
		msg.Release()
		t.Fatal("prepare rejected")
	}
	consensusProcess(t, replica)
	if !replica.pipelineEntry(0).durable || replica.pipelineEntry(0).quorum {
		t.Fatalf("unexpected pipeline entry state: durable=%t quorum=%t", replica.pipelineEntry(0).durable, replica.pipelineEntry(0).quorum)
	}

	step := uint64(replica.config.Process.Tick)
	ticksForTenSeconds := int(10 * time.Second / replica.config.Process.Tick)
	for tick := 1; tick <= ticksForTenSeconds; tick++ {
		now := uint64(tick) * step
		replica.handleTick(TimeSample{Wall: 100 + now, Monotonic: now, Synchronized: true})
	}
	if !replica.abdicated {
		t.Fatal("primary did not abdicate after 10 seconds without quorum")
	}
	if replica.metrics.Snapshot().QuorumUnavailable != 1 {
		t.Fatalf("quorum unavailable count = %d, want 1", replica.metrics.Snapshot().QuorumUnavailable)
	}
	commitsBefore := countConsensusCommand(t, replica, bus, protocol.CommandCommit)
	preparesBefore := countConsensusCommand(t, replica, bus, protocol.CommandPrepare)
	exitsBefore := countConsensusCommand(t, replica, bus, protocol.CommandExitView)
	if exitsBefore == 0 {
		t.Fatal("abdicating primary did not emit ExitView")
	}

	for tick := ticksForTenSeconds + 1; tick <= ticksForTenSeconds+200; tick++ {
		now := uint64(tick) * step
		replica.handleTick(TimeSample{Wall: 100 + now, Monotonic: now, Synchronized: true})
	}
	if got := countConsensusCommand(t, replica, bus, protocol.CommandCommit); got != commitsBefore {
		t.Fatalf("abdicated primary kept sending Commit heartbeats: before=%d after=%d", commitsBefore, got)
	}
	if got := countConsensusCommand(t, replica, bus, protocol.CommandPrepare); got != preparesBefore {
		t.Fatalf("abdicated primary kept resending Prepares: before=%d after=%d", preparesBefore, got)
	}
	if got := countConsensusCommand(t, replica, bus, protocol.CommandExitView); got <= exitsBefore {
		t.Fatalf("abdicated primary did not rebroadcast ExitView: before=%d after=%d", exitsBefore, got)
	}

	// Restoring quorum clears abdication and resumes Commit heartbeats.
	replica.countPrepareAck(replica.pipelineEntry(0), 1)
	if !replica.pipelineEntry(0).quorum {
		t.Fatal("pipeline head did not reach quorum")
	}
	for tick := ticksForTenSeconds + 201; tick <= ticksForTenSeconds+260; tick++ {
		now := uint64(tick) * step
		replica.handleTick(TimeSample{Wall: 100 + now, Monotonic: now, Synchronized: true})
	}
	if replica.abdicated {
		t.Fatal("primary remained abdicated after regaining quorum")
	}
	if got := countConsensusCommand(t, replica, bus, protocol.CommandCommit); got <= commitsBefore {
		t.Fatalf("primary did not resume Commit heartbeats after regaining quorum: before=%d after=%d", commitsBefore, got)
	}
}

func TestPrimaryClockDesyncTriggersAbdication(t *testing.T) {
	replica, bus := consensusPrimary(t, false)
	step := uint64(replica.config.Process.Tick)
	ticksForTenSeconds := int(10 * time.Second / replica.config.Process.Tick)
	for tick := 1; tick <= ticksForTenSeconds; tick++ {
		now := uint64(tick) * step
		replica.handleTick(TimeSample{Wall: 100 + now, Monotonic: now, Synchronized: false})
	}
	if !replica.abdicated {
		t.Fatal("unsynchronized primary did not abdicate with empty pipeline")
	}
	commitsAtAbdication := countConsensusCommand(t, replica, bus, protocol.CommandCommit)
	for tick := ticksForTenSeconds + 1; tick <= ticksForTenSeconds+120; tick++ {
		now := uint64(tick) * step
		replica.handleTick(TimeSample{Wall: 100 + now, Monotonic: now, Synchronized: false})
	}
	if got := countConsensusCommand(t, replica, bus, protocol.CommandCommit); got != commitsAtAbdication {
		t.Fatalf("unsynchronized abdicated primary kept sending Commit: before=%d after=%d", commitsAtAbdication, got)
	}
}

func TestDuplicatePrepareDoesNotRefreshFailureDetector(t *testing.T) {
	replica, _ := consensusBackup(t)
	root := consensusRoot(t, replica)
	firstMsg, firstHdr := consensusPulse(t, replica, root, 1, 0)
	t1 := uint64(time.Second)
	if !replica.handlePrepare(firstMsg, firstHdr, TimeSample{Wall: t1, Monotonic: t1, Synchronized: true}) {
		firstMsg.Release()
		t.Fatal("initial prepare rejected")
	}
	consensusProcess(t, replica)
	for _, seconds := range []time.Duration{3, 5, 7, 9} {
		dup, _ := consensusPulse(t, replica, root, 1, 0)
		now := uint64(seconds * time.Second)
		if replica.handlePrepare(dup, firstHdr, TimeSample{Wall: now, Monotonic: now, Synchronized: true}) {
			t.Fatal("duplicate prepare retained")
		}
		dup.Release()
	}
	if level := replica.failureDetector.Level(uint64(9 * time.Second)); level != failureRed {
		t.Fatalf("failure detector level after duplicate-only prepares = %d, want failureRed (%d)", level, failureRed)
	}
}

func TestIsolatedViewChangeReplicaCannotInflateViewAndExitWindowExpires(t *testing.T) {
	replica, bus := consensusBackup(t)
	replica.beginViewChange(1)
	consensusProcess(t, replica)
	if snap := replica.Snapshot(); snap.Status != StatusViewChange || snap.View != 1 || snap.DurableView != 1 {
		t.Fatalf("unexpected initial view-change snapshot: %+v", snap)
	}

	step := uint64(replica.config.Process.Tick)
	ticks := int(16 * time.Second / replica.config.Process.Tick)
	for tick := 1; tick <= ticks; tick++ {
		now := uint64(tick) * step
		replica.handleTick(TimeSample{Wall: 100 + now, Monotonic: now, Synchronized: true})
		consensusProcess(t, replica)
	}
	if snap := replica.Snapshot(); snap.View != 1 || snap.Status != StatusViewChange {
		t.Fatalf("isolated view-change replica inflated view: %+v", snap)
	}
	if !replica.viewChangeStalled {
		t.Fatal("view-change stall flag not set after 5s timeout")
	}
	if countConsensusCommand(t, replica, bus, protocol.CommandExitView) == 0 {
		t.Fatal("stalled view-change replica did not emit ExitView")
	}

	// Verify exit-view collection window expires stale votes after 5 seconds.
	replica.exitViewBits = 0
	clear(replica.exitViewAt[:])
	exitFrom2 := protocol.Header{Group: replica.config.Group, View: 1, Command: protocol.CommandExitView, Author: 2}
	exitFrom0 := protocol.Header{Group: replica.config.Group, View: 1, Command: protocol.CommandExitView, Author: 0}
	tStart := uint64(20 * time.Second)
	replica.handleExitView(exitFrom2, tStart)
	replica.handleExitView(exitFrom0, tStart+uint64(6*time.Second))
	if replica.view != 1 || replica.exitViewBits != 1<<0 {
		t.Fatalf("stale ExitView vote did not expire: view=%d bits=%03b", replica.view, replica.exitViewBits)
	}
	replica.handleExitView(exitFrom2, tStart+uint64(7*time.Second))
	consensusProcess(t, replica)
	if snap := replica.Snapshot(); snap.View != 2 || snap.Status != StatusViewChange {
		t.Fatalf("fresh ExitView quorum did not advance view: %+v", snap)
	}
}

func TestHandleViewAcceptsLowerViewCommitMaxWhenHeadCoversLocalCommitMax(t *testing.T) {
	replica, _ := consensusBackup(t)
	root := consensusRoot(t, replica)
	headers := make([]protocol.Header, 4)
	headers[0] = root
	for op := protocol.Op(1); op <= 3; op++ {
		msg, hdr := consensusPulse(t, replica, headers[op-1], op, 0)
		if !replica.handlePrepare(msg, hdr, replica.deps.Clock.Now()) {
			msg.Release()
			t.Fatalf("prepare %d rejected", op)
		}
		headers[op] = hdr
		consensusProcess(t, replica)
	}
	replica.commitMax = 1
	consensusProcess(t, replica)
	// Backup learned CommitMax=3 before view change, while new primary's View reports CommitMax=1 and HeadOp=3.
	replica.commitMax = 3
	viewBody := encodeTestViewBody(t, replica, headers[3], headers[2], headers[1], root)
	replica.handleView(makeTestViewHeader(replica, 2, 3, 1, protocol.Nonce{}), viewBody)
	if !replica.repairViewValid || replica.repairView != 2 {
		t.Fatalf("View with lower CommitMax rejected: valid=%t repairView=%d", replica.repairViewValid, replica.repairView)
	}
	replica.continueRecoveringView(20)
	consensusProcessUntilNormal(t, replica)
	got := replica.Snapshot()
	if got.Status != StatusNormal || got.View != 2 || got.CommitMin != 3 || got.CommitMax != 3 || got.HeadOp != 3 {
		t.Fatalf("installed view state = %+v", got)
	}
}

func TestStalledRecoveringHeadAbortsUninstalledRepairOnExitQuorum(t *testing.T) {
	replica, _ := consensusBackup(t)
	root := consensusRoot(t, replica)
	msg1, hdr1 := consensusPulse(t, replica, root, 1, 1)
	msg1.Release()
	viewBody := encodeTestViewBody(t, replica, hdr1, root)
	replica.handleView(makeTestViewHeader(replica, 1, 1, 0, protocol.Nonce{}), viewBody)
	if replica.status != StatusRecoveringHead || !replica.repairViewValid {
		t.Fatalf("expected RecoveringHead with active repairView: status=%d valid=%t", replica.status, replica.repairViewValid)
	}

	step := uint64(replica.config.Process.Tick)
	ticks := int(5 * time.Second / replica.config.Process.Tick)
	var now uint64
	for tick := 1; tick <= ticks; tick++ {
		now = uint64(tick) * step
		replica.handleTick(TimeSample{Wall: 100 + now, Monotonic: now, Synchronized: true})
	}
	if !replica.viewChangeStalled {
		t.Fatal("stalled RecoveringHead did not trip view-change progress timeout")
	}
	replica.handleExitView(protocol.Header{Group: replica.config.Group, View: 0, Command: protocol.CommandExitView, Author: 2}, now+step)
	consensusProcess(t, replica)
	if replica.repairViewValid || replica.status != StatusViewChange || replica.view != 1 {
		t.Fatalf("stalled RecoveringHead did not abort uninstalled repair on ExitView quorum: status=%d view=%d repairValid=%t",
			replica.status, replica.view, replica.repairViewValid)
	}
}

func TestStateSyncDoesNotStampLogViewBeforeSuffixInstall(t *testing.T) {
	replica, _ := consensusBackup(t)
	root := consensusRoot(t, replica)
	msg1, hdr1 := consensusPulse(t, replica, root, 1, 4)
	msg1.Release()

	replica.stateSync.checkpoint = replica.checkpoint
	replica.stateSync.view = 4
	replica.stateSync.commit = 0
	replica.stateSync.head = 1
	replica.stateSync.count = 2
	replica.stateSync.headers[0] = hdr1
	replica.stateSync.headers[1] = root

	withSuffix, err := replica.stateSyncSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	if withSuffix.State.View != 4 || withSuffix.State.LogView != 0 {
		t.Fatalf("state sync with suffix stamped LogView prematurely: view=%d logView=%d", withSuffix.State.View, withSuffix.State.LogView)
	}

	replica.stateSync.head = replica.checkpoint.PrepareOp()
	replica.stateSync.count = 1
	replica.stateSync.headers[0] = root
	atCheckpoint, err := replica.stateSyncSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	if atCheckpoint.State.View != 4 || atCheckpoint.State.LogView != 4 {
		t.Fatalf("state sync at checkpoint head did not stamp LogView: view=%d logView=%d", atCheckpoint.State.View, atCheckpoint.State.LogView)
	}
}
