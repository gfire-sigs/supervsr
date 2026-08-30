package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestReplicaSoloRegistrationApplicationAndDuplicateReply(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	bus := &captureBus{}
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(config.Cluster.PipelineMax),
		CheckpointMax: 1,
	}}
	replica, err := newReplica(config, Dependencies{
		Storage:      storage,
		MessageBus:   bus,
		Clock:        fixedClock{sample: TimeSample{Wall: 100, Monotonic: 10, Synchronized: true}},
		Entropy:      bytes.NewReader([]byte{1}),
		StateMachine: machine,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, replica)

	pool, err := protocol.NewFramePool(4, uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	client := protocol.ClientID{7}
	registration := makeClientRequest(t, pool, config.Group, client, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
	if err := replica.Submit(registration); err != nil {
		t.Fatal(err)
	}
	processReplicaUntil(t, replica, 1)
	registrationReply := bus.clientMessage(t, 0)
	registrationHeader, registrationBody, reason := protocol.DecodeFrame(registrationReply, config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || len(registrationBody) != 64 {
		t.Fatalf("registration reply reason=%v size=%d", reason, len(registrationBody))
	}
	if got := binary.LittleEndian.Uint32(registrationBody[:4]); uint64(got) != config.Cluster.ApplicationBatchSizeMax {
		t.Fatalf("batch limit = %d", got)
	}

	session := protocol.Session(replyCommit(&registrationHeader))
	parent := replyContext(&registrationHeader)
	machine.validation = ValidationInvalidBody
	rejected := makeClientRequest(t, pool, config.Group, client, session, 1, parent, protocol.OperationApplicationMin, []byte("bad"))
	if err := replica.Submit(rejected); err != nil {
		t.Fatal(err)
	}
	processReplicaMessages(t, replica, 2)
	rejection, _, reason := protocol.DecodeFrame(bus.clientMessage(t, 1), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || protocol.EvictionReason(rejection.Fields[127]) != protocol.EvictionInvalidBody {
		t.Fatalf("application rejection=%+v reason=%v", rejection, reason)
	}
	machine.validation = ValidationOK
	request := makeClientRequest(t, pool, config.Group, client, session, 1, parent, protocol.OperationApplicationMin, []byte("put"))
	if err := replica.Submit(request); err != nil {
		t.Fatal(err)
	}
	processReplicaUntil(t, replica, 2)
	applicationReply := bus.clientMessage(t, 2)
	applicationHeader, applicationBody, reason := protocol.DecodeFrame(applicationReply, config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || string(applicationBody) != "reply:put" {
		t.Fatalf("application reply reason=%v body=%q", reason, applicationBody)
	}
	if machine.commits != 1 {
		t.Fatalf("application commits = %d, want 1", machine.commits)
	}

	duplicate := makeClientRequest(t, pool, config.Group, client, session, 1, parent, protocol.OperationApplicationMin, []byte("put"))
	if err := replica.Submit(duplicate); err != nil {
		t.Fatal(err)
	}
	processReplicaMessages(t, replica, 4)
	duplicateReply := bus.clientMessage(t, 3)
	_, duplicateBody, reason := protocol.DecodeFrame(duplicateReply, config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || string(duplicateBody) != "reply:put" {
		t.Fatalf("duplicate reply reason=%v body=%q", reason, duplicateBody)
	}
	if machine.commits != 1 {
		t.Fatalf("duplicate re-executed operation: commits=%d", machine.commits)
	}
	prepareHeader, found := wal.RecoveredHeader(2)
	if !found {
		t.Fatal("durable prepare header missing")
	}
	headersRequest := protocol.Header{Group: config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetHeaders, Author: 0}
	binary.LittleEndian.PutUint64(headersRequest.Fields[:8], 1)
	binary.LittleEndian.PutUint64(headersRequest.Fields[8:16], 2)
	headersStart := bus.replicaCount()
	if err := replica.Submit(makeReplicaCommand(t, pool, headersRequest, nil)); err != nil {
		t.Fatal(err)
	}
	processReplicaNetworkMessages(t, replica, bus, headersStart+1)
	headers, headersBody, reason := protocol.DecodeFrame(bus.replicaMessage(t, headersStart), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || headers.Command != protocol.CommandHeaders || len(headersBody) != 2*protocol.HeaderSize {
		t.Fatalf("headers command=%d body=%d reason=%v", headers.Command, len(headersBody), reason)
	}
	prepareRequest := protocol.Header{Group: config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetPrepare, Author: 0}
	copy(prepareRequest.Fields[:16], prepareHeader.HeaderChecksum[:])
	binary.LittleEndian.PutUint64(prepareRequest.Fields[32:40], 2)
	prepareStart := bus.replicaCount()
	if err := replica.Submit(makeReplicaCommand(t, pool, prepareRequest, nil)); err != nil {
		t.Fatal(err)
	}
	processReplicaNetworkMessages(t, replica, bus, prepareStart+1)
	repairedPrepare, _, reason := protocol.DecodeFrame(bus.replicaMessage(t, prepareStart), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || repairedPrepare.Command != protocol.CommandPrepare || repairedPrepare.HeaderChecksum != prepareHeader.HeaderChecksum {
		t.Fatalf("prepare repair=%+v reason=%v", repairedPrepare, reason)
	}
	replyRequest := protocol.Header{Group: config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetReply, Author: 0}
	copy(replyRequest.Fields[:16], applicationHeader.HeaderChecksum[:])
	copy(replyRequest.Fields[32:48], client[:])
	binary.LittleEndian.PutUint64(replyRequest.Fields[48:56], 2)
	if protocol.Checksum(replyRequest.Fields[:16]) != applicationHeader.HeaderChecksum || protocol.ClientID(replyRequest.Fields[32:48]) != client {
		t.Fatal("GetReply wire identity fields reversed")
	}
	replyStart := bus.replicaCount()
	if err := replica.Submit(makeReplicaCommand(t, pool, replyRequest, nil)); err != nil {
		t.Fatal(err)
	}
	processReplicaNetworkMessages(t, replica, bus, replyStart+1)
	repairedReply, replyBody, reason := protocol.DecodeFrame(bus.replicaMessage(t, replyStart), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || repairedReply.HeaderChecksum != applicationHeader.HeaderChecksum || string(replyBody) != "reply:put" {
		t.Fatalf("reply repair=%+v body=%q reason=%v", repairedReply, replyBody, reason)
	}
	var metadata [96]byte
	binary.LittleEndian.PutUint32(metadata[:4], 1)
	binary.LittleEndian.PutUint32(metadata[4:8], 1)
	binary.LittleEndian.PutUint32(metadata[8:12], 1)
	blockBase, ok := config.Cluster.BlockBase()
	if !ok {
		t.Fatal("block base overflow")
	}
	if err := storage.Resize(blockBase + config.Cluster.BlockSize); err != nil {
		t.Fatal(err)
	}
	blockReference, err := replica.blocks.Write(1, 1, protocol.BlockValue, metadata, []byte{7})
	if err != nil {
		t.Fatal(err)
	}
	var blockRequestBody [32]byte
	copy(blockRequestBody[:16], blockReference.Checksum[:])
	binary.LittleEndian.PutUint64(blockRequestBody[16:24], blockReference.Address)
	blockRequest := protocol.Header{Group: config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetBlocks, Author: 0}
	blockStart := bus.replicaCount()
	if err := replica.Submit(makeReplicaCommand(t, pool, blockRequest, blockRequestBody[:])); err != nil {
		t.Fatal(err)
	}
	processReplicaNetworkMessages(t, replica, bus, blockStart+1)
	repairedBlock, blockBody, reason := protocol.DecodeFrame(bus.replicaMessage(t, blockStart), config.Group, uint32(config.Cluster.BlockSize), 1)
	if reason != protocol.RejectNone || repairedBlock.HeaderChecksum != blockReference.Checksum || len(blockBody) != 1 || blockBody[0] != 7 {
		t.Fatalf("block repair=%+v body=%x reason=%v", repairedBlock, blockBody, reason)
	}
	getView := protocol.Header{Group: config.Group, View: replica.view, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetView, Author: 0}
	getView.Fields[0] = 5
	viewStart := bus.replicaCount()
	if err := replica.Submit(makeReplicaCommand(t, pool, getView, nil)); err != nil {
		t.Fatal(err)
	}
	processReplicaNetworkMessages(t, replica, bus, viewStart+1)
	view, _, reason := protocol.DecodeFrame(bus.replicaMessage(t, viewStart), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || view.Command != protocol.CommandView || view.Fields[0] != 5 {
		t.Fatalf("view response=%+v reason=%v", view, reason)
	}
	reconfigure := makeClientRequest(t, pool, config.Group, client, session, 2, replyContext(&applicationHeader), protocol.OperationReconfigure, reconfigurationBody(config.Membership, 0))
	if err := replica.Submit(reconfigure); err != nil {
		t.Fatal(err)
	}
	processReplicaUntil(t, replica, 3)
	_, reconfigurationReply, reason := protocol.DecodeFrame(bus.clientMessage(t, 4), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || len(reconfigurationReply) != 4 || ReconfigurationResult(binary.LittleEndian.Uint32(reconfigurationReply)) != ReconfigurationApplied {
		t.Fatalf("reconfiguration reply=%x reason=%v", reconfigurationReply, reason)
	}
	if machine.commits != 1 {
		t.Fatalf("reconfiguration reached application state machine: commits=%d", machine.commits)
	}
}
func TestReplicaRejectsUnboundAndUnknownMemberFrames(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	metrics := &ReplicaMetrics{}
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: &testStateMachine{capacities: StateMachineCapacities{
			RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
			PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
		}},
		Metrics: metrics,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, replica)
	pool, err := protocol.NewFramePool(3, uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	bound := makeClientRequest(t, pool, config.Group, protocol.ClientID{7}, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
	encoded, err := bound.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	unbound, err := pool.AcquireEncoded(encoded)
	if err != nil {
		t.Fatal(err)
	}
	bound.Release()
	if err := replica.Submit(unbound); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("unbound submit error = %v, want %v", err, ErrAuthentication)
	}
	unbound.Release()
	unknownMember, err := pool.AcquireEncoded(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := unknownMember.BindReplica(protocol.MemberID{2}, 1); err != nil {
		t.Fatal(err)
	}
	if err := replica.Submit(unknownMember); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("unknown member submit error = %v, want %v", err, ErrAuthentication)
	}
	unknownMember.Release()
	wrongIdentity, err := pool.AcquireEncoded(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := wrongIdentity.BindReplica(protocol.MemberID{2}, 0); err != nil {
		t.Fatal(err)
	}
	if err := replica.Submit(wrongIdentity); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("wrong identity submit error = %v, want %v", err, ErrAuthentication)
	}
	wrongIdentity.Release()
	if rejected := metrics.Snapshot().FramesRejected; rejected != 3 {
		t.Fatalf("rejected frames = %d, want 3", rejected)
	}
}
func TestReplicaFailStopsUnknownSupportedCommand(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: &testStateMachine{capacities: StateMachineCapacities{
			RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
			PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
		}},
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, replica)
	pool, err := protocol.NewFramePool(2, uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	request := makeClientRequest(t, pool, config.Group, protocol.ClientID{7}, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
	requestBytes, err := request.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	encoded := append([]byte(nil), requestBytes...)
	request.Release()
	encoded[114] = 0xff
	checksum := protocol.ChecksumBytes(encoded[protocol.HeaderChecksumFrom:])
	copy(encoded[:16], checksum[:])
	unknown, err := pool.AcquireEncoded(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := unknown.BindClient(); err != nil {
		t.Fatal(err)
	}
	if err := replica.Submit(unknown); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Process(1); !errors.Is(err, ErrProtocolInvariant) {
		t.Fatalf("process error = %v, want %v", err, ErrProtocolInvariant)
	}
}

func TestReplicaClientPingAndNoSessionEviction(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	bus := &captureBus{}
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: bus,
		Clock:   fixedClock{sample: TimeSample{Wall: 100, Monotonic: 10, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: &testStateMachine{capacities: StateMachineCapacities{
			RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
			PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1,
		}},
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, replica)
	pool, err := protocol.NewFramePool(2, uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	client := protocol.ClientID{8}
	ping, err := pool.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	pingHeader := protocol.Header{Group: config.Group, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandClientPing}
	copy(pingHeader.Fields[:16], client[:])
	binary.LittleEndian.PutUint64(pingHeader.Fields[16:24], 99)
	if err := ping.Seal(&pingHeader); err != nil {
		t.Fatal(err)
	}
	if err := ping.BindClient(); err != nil {
		t.Fatal(err)
	}
	if err := replica.Submit(ping); err != nil {
		t.Fatal(err)
	}
	processReplicaMessages(t, replica, 1)
	pong, _, reason := protocol.DecodeFrame(bus.clientMessage(t, 0), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || pong.Command != protocol.CommandClientPong || binary.LittleEndian.Uint64(pong.Fields[:8]) != 99 || bus.clientDestination(0) != client {
		t.Fatalf("pong=%+v reason=%v destination=%x", pong, reason, bus.clientDestination(0))
	}
	lagging := makeClientRequest(t, pool, config.Group, client, 1, 1, protocol.Checksum{1}, protocol.OperationApplicationMin, nil)
	if err := replica.Submit(lagging); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Process(64); err != nil {
		t.Fatal(err)
	}
	if bus.clientCount() != 1 {
		t.Fatalf("lagging replica evicted registered client: messages=%d", bus.clientCount())
	}
	replica.headOp = 1
	replica.commitMin = 1
	replica.commitMax = 1
	request := makeClientRequest(t, pool, config.Group, client, 1, 1, protocol.Checksum{1}, protocol.OperationApplicationMin, nil)
	if err := replica.Submit(request); err != nil {
		t.Fatal(err)
	}
	processReplicaMessages(t, replica, 2)
	eviction, _, reason := protocol.DecodeFrame(bus.clientMessage(t, 1), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || eviction.Command != protocol.CommandEviction || protocol.EvictionReason(eviction.Fields[127]) != protocol.EvictionNoSession {
		t.Fatalf("eviction=%+v reason=%v", eviction, reason)
	}
	invalid := makeClientRequest(t, pool, config.Group, client, 1, 2, protocol.Checksum{1}, protocol.OperationUpgrade, nil)
	if err := replica.Submit(invalid); err != nil {
		t.Fatal(err)
	}
	processReplicaMessages(t, replica, 3)
	invalidEviction, _, reason := protocol.DecodeFrame(bus.clientMessage(t, 2), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || protocol.EvictionReason(invalidEviction.Fields[127]) != protocol.EvictionInvalidOperation {
		t.Fatalf("invalid operation eviction=%+v reason=%v", invalidEviction, reason)
	}
	malformed, err := pool.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	malformedHeader := protocol.Header{Group: config.Group, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandRequest}
	malformedHeader.Fields[16] = 1
	copy(malformedHeader.Fields[32:48], client[:])
	binary.LittleEndian.PutUint64(malformedHeader.Fields[48:56], 1)
	binary.LittleEndian.PutUint32(malformedHeader.Fields[64:68], 3)
	malformedHeader.Fields[68] = byte(protocol.OperationApplicationMin)
	if err := malformed.Seal(&malformedHeader); err != nil {
		t.Fatal(err)
	}
	if err := malformed.BindClient(); err != nil {
		t.Fatal(err)
	}
	if err := replica.Submit(malformed); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Process(64); err != nil {
		t.Fatal(err)
	}
	future, err := pool.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	futureHeader := protocol.Header{Group: config.Group, View: 1, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandRequest}
	futureHeader.Fields[0] = 1
	copy(futureHeader.Fields[32:48], client[:])
	binary.LittleEndian.PutUint64(futureHeader.Fields[48:56], 1)
	binary.LittleEndian.PutUint32(futureHeader.Fields[64:68], 4)
	futureHeader.Fields[68] = byte(protocol.OperationApplicationMin)
	if err := future.Seal(&futureHeader); err != nil {
		t.Fatal(err)
	}
	if err := future.BindClient(); err != nil {
		t.Fatal(err)
	}
	if err := replica.Submit(future); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Process(64); err != nil {
		t.Fatal(err)
	}
	if bus.clientCount() != 3 {
		t.Fatalf("drop-only requests produced %d client messages", bus.clientCount())
	}
}
func TestValidationEvictionMapping(t *testing.T) {
	tests := []struct {
		result ValidationResult
		reason protocol.EvictionReason
		reject bool
	}{
		{ValidationOK, protocol.EvictionReserved, false},
		{ValidationInvalidOperation, protocol.EvictionInvalidOperation, true},
		{ValidationInvalidBody, protocol.EvictionInvalidBody, true},
		{ValidationInvalidBodySize, protocol.EvictionInvalidBodySize, true},
	}
	for _, test := range tests {
		reason, reject := validationEviction(test.result)
		if reason != test.reason || reject != test.reject {
			t.Fatalf("result=%d reason=%d reject=%v", test.result, reason, reject)
		}
	}
}

func TestOpenAdvancesSoloViewAndReplaysDurablePrepare(t *testing.T) {
	config, storage, _, _, _, _, _ := replicaFixture(t)
	capacities := StateMachineCapacities{
		RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(config.Cluster.PipelineMax),
		CheckpointMax: 1,
	}
	replica, err := Open(context.Background(), config, Dependencies{
		Storage:      storage,
		MessageBus:   &captureBus{},
		Clock:        fixedClock{sample: TimeSample{Wall: 1, Synchronized: true}},
		Entropy:      bytes.NewReader([]byte{1}),
		StateMachine: &testStateMachine{capacities: capacities},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := replica.Snapshot()
	if snapshot.Status != StatusNormal || snapshot.View != 1 || snapshot.DurableView != 1 || snapshot.LogView != 1 {
		t.Fatalf("open snapshot = %+v", snapshot)
	}
	pool, err := protocol.NewFramePool(1, uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	client := protocol.ClientID{4}
	registration := makeClientRequest(t, pool, config.Group, client, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
	if err := replica.Submit(registration); err != nil {
		t.Fatal(err)
	}
	processReplicaUntil(t, replica, 1)
	closeReplica(t, replica)
	storage.Crash()

	reopened, err := Open(context.Background(), config, Dependencies{
		Storage:      storage,
		MessageBus:   &captureBus{},
		Clock:        fixedClock{sample: TimeSample{Wall: 2, Synchronized: true}},
		Entropy:      bytes.NewReader([]byte{2}),
		StateMachine: &testStateMachine{capacities: capacities},
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := reopened.Snapshot(); snapshot.Status != StatusNormal || snapshot.View != 2 || snapshot.CommitMin != 1 {
		t.Fatalf("reopened snapshot = %+v", snapshot)
	}
	if session, found := reopened.sessions.Session(client); !found || session != 1 {
		t.Fatalf("replayed session = %d, found %t", session, found)
	}
	closeReplica(t, reopened)
}

func TestReplicaConcurrentCloseOwnsShutdownOnce(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	machine := &testStateMachine{
		capacities: StateMachineCapacities{
			RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
			ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
			PrefetchMax:   uint32(config.Cluster.PipelineMax),
			CheckpointMax: 1,
		},
		resetPending: true,
		resetStarted: make(chan struct{}),
	}
	replica, err := newReplica(config, Dependencies{
		Storage:      storage,
		MessageBus:   &captureBus{},
		Clock:        fixedClock{sample: TimeSample{Wall: 1, Synchronized: true}},
		Entropy:      bytes.NewReader([]byte{1}),
		StateMachine: machine,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := protocol.NewFramePool(1, uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	message := makeClientRequest(t, pool, config.Group, protocol.ClientID{3}, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
	if err := replica.Submit(message); err != nil {
		t.Fatal(err)
	}

	const callers = 8
	results := make(chan error, callers)
	for range callers {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			results <- replica.Close(ctx)
		}()
	}
	select {
	case <-machine.resetStarted:
	case <-time.After(time.Second):
		t.Fatal("state-machine reset did not start")
	}
	if err := machine.resetCompletion.Complete(SMResult{Kind: SMCompletionReset}); err != nil {
		t.Fatal(err)
	}
	for range callers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if machine.closes.Load() != 1 {
		t.Fatalf("state-machine closes = %d, want 1", machine.closes.Load())
	}
	if pool.Available() != 1 {
		t.Fatalf("submitted frame was not released: available=%d", pool.Available())
	}
}

func TestReplicaCheckpointPersistsSessionTrailersAndReopens(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	bus := &captureBus{}
	capacities := StateMachineCapacities{
		RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(config.Cluster.PipelineMax),
		CheckpointMax: 1,
	}
	machine := &testStateMachine{capacities: capacities}
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: bus,
		Clock:   fixedClock{sample: TimeSample{Wall: 100, Monotonic: 10, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: machine,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := protocol.NewFramePool(2, uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	client := protocol.ClientID{12}
	registration := makeClientRequest(t, pool, config.Group, client, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
	if err := replica.Submit(registration); err != nil {
		t.Fatal(err)
	}
	processReplicaUntil(t, replica, 1)
	registrationHeader, _, reason := protocol.DecodeFrame(bus.clientMessage(t, 0), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone {
		t.Fatalf("registration reply: %v", reason)
	}
	session := protocol.Session(replyCommit(&registrationHeader))
	parent := replyContext(&registrationHeader)
	for requestNo := protocol.RequestNo(1); requestNo <= 110; requestNo++ {

		request := makeClientRequest(t, pool, config.Group, client, session, requestNo, parent, protocol.OperationNoop, nil)
		if err := replica.Submit(request); err != nil {
			t.Fatal(err)
		}
		processReplicaUntil(t, replica, protocol.Op(requestNo)+1)
		reply, _, replyReason := protocol.DecodeFrame(bus.clientMessage(t, int(requestNo)), config.Group, uint32(config.Cluster.MessageSizeMax), 1)
		if replyReason != protocol.RejectNone {
			t.Fatalf("request %d reply: %v", requestNo, replyReason)
		}
		parent = replyContext(&reply)
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for snapshot := replica.Snapshot(); snapshot.Checkpoint.PrepareOp() != 95 || snapshot.PipelineLen != 0; snapshot = replica.Snapshot() {
		if _, err := replica.Process(64); err != nil {
			t.Fatal(err)
		}
		select {
		case <-replica.io.Ready():
		case <-replica.notify:
		case <-deadline.C:
			t.Fatalf("checkpoint did not finish: %+v", replica.Snapshot())
		default:
		}
	}
	if machine.compacts != 7 || machine.checkpoints != 1 {
		t.Fatalf("maintenance compacts=%d checkpoints=%d", machine.compacts, machine.checkpoints)
	}
	closeReplica(t, replica)
	storage.Crash()
	reopened, err := Open(context.Background(), config, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 200, Monotonic: 20, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{2}), StateMachine: &testStateMachine{capacities: capacities},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, reopened)
	reopenedSnapshot := reopened.Snapshot()
	if reopenedSnapshot.Checkpoint.PrepareOp() != 95 || reopenedSnapshot.CommitMin != 111 {
		t.Fatalf("reopened checkpoint: %+v", reopenedSnapshot)
	}

	if _, _, found := reopened.sessions.Reply(client, session, 110); !found {
		t.Fatal("reopened session reply missing")
	}
}
func makeReplicaCommand(t testing.TB, pool *protocol.FramePool, header protocol.Header, body []byte) *protocol.Frame {
	t.Helper()
	frame, err := pool.Acquire(uint32(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	frameBody, err := frame.Body()
	if err != nil {
		t.Fatal(err)
	}
	copy(frameBody, body)
	if err := frame.Seal(&header); err != nil {
		t.Fatal(err)
	}
	if err := frame.BindReplica(protocol.MemberID{byte(header.Author) + 1}, header.Author); err != nil {
		t.Fatal(err)
	}
	return frame
}
func processReplicaNetworkMessages(t testing.TB, replica *Replica, bus *captureBus, messages int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for bus.replicaCount() < messages {
		if _, err := replica.Process(64); err != nil {
			t.Fatal(err)
		}
		if bus.replicaCount() >= messages {
			return
		}
		select {
		case <-replica.io.Ready():
		case <-replica.notify:
		case <-deadline.C:
			t.Fatalf("replica messages=%d, want %d", bus.replicaCount(), messages)
		}
	}
}

func replicaFixture(t testing.TB) (Config, *crashStorage, ReplicaInitialState, *WAL, *ReplyStore, *SessionTable, *SuperblockStore) {
	t.Helper()
	cluster := compactTestClusterConfig()

	membership := Membership{Members: [MembersMax]protocol.MemberID{{1}}, ActiveCount: 1, LocalMember: protocol.MemberID{1}}
	config := Config{
		Group:            protocol.GroupID{9},
		Membership:       membership,
		Cluster:          cluster,
		Process:          DefaultProcessConfig(),
		CurrentRelease:   1,
		ClientReleaseMin: 1,
	}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	storage := &crashStorage{}
	if err := Format(context.Background(), FormatConfig{Group: config.Group, Membership: membership, Cluster: cluster, CurrentRelease: 1}, FormatDependencies{Storage: storage}); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	validation := SuperblockValidation{Group: config.Group, Membership: membership, ConfigurationChecksum: cluster.Fingerprint(), Cluster: cluster}
	store, err := OpenSuperblockStore(storage, validation)
	if err != nil {
		t.Fatal(err)
	}
	superblock := store.Current()
	head, reason := protocol.DecodeHeader(superblock.State.Checkpoint.Header[:], config.Group, uint32(cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone {
		t.Fatalf("root header: %v", reason)
	}
	wal, err := NewWAL(storage, cluster, config.Group, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Recover(superblock.State.Checkpoint, superblock.State.CommitMax, config.Process); err != nil {
		t.Fatal(err)
	}
	replies, err := NewReplyStore(storage, cluster, config.Group, 1)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := NewSessionTable(SessionTableConfig{
		ClientsMax:              uint32(cluster.ClientsMax),
		Group:                   config.Group,
		ActiveCount:             1,
		MessageSizeMax:          uint32(cluster.MessageSizeMax),
		ApplicationReplySizeMax: uint32(cluster.ApplicationReplySizeMax),
	})
	if err != nil {
		t.Fatal(err)
	}
	initial := ReplicaInitialState{
		Status:      StatusNormal,
		View:        superblock.State.View,
		DurableView: superblock.State.View,
		LogView:     superblock.State.LogView,
		HeadOp:      0,
		CommitMin:   0,
		CommitMax:   superblock.State.CommitMax,
		Checkpoint:  superblock.State.Checkpoint,
		HeadHeader:  head,
	}
	return config, storage, initial, wal, replies, sessions, store
}

func makeClientRequest(t testing.TB, pool *protocol.FramePool, group protocol.GroupID, client protocol.ClientID, session protocol.Session, request protocol.RequestNo, parent protocol.Checksum, operation protocol.Operation, body []byte) *protocol.Frame {
	t.Helper()
	frame, err := pool.Acquire(uint32(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	frameBody, err := frame.Body()
	if err != nil {
		t.Fatal(err)
	}
	copy(frameBody, body)
	header := protocol.Header{Group: group, View: 0, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandRequest, Author: 0}
	copy(header.Fields[0:16], parent[:])
	copy(header.Fields[32:48], client[:])
	binary.LittleEndian.PutUint64(header.Fields[48:56], uint64(session))
	binary.LittleEndian.PutUint32(header.Fields[64:68], uint32(request))
	header.Fields[68] = byte(operation)
	if err := frame.Seal(&header); err != nil {
		t.Fatal(err)
	}
	if err := frame.BindClient(); err != nil {
		t.Fatal(err)
	}
	return frame
}

func processReplicaUntil(t testing.TB, replica *Replica, commit protocol.Op) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for replica.Snapshot().CommitMin < commit {
		if _, err := replica.Process(64); err != nil {
			t.Fatal(err)
		}
		if replica.Snapshot().CommitMin >= commit {
			return
		}
		select {
		case <-replica.io.Ready():
		case <-replica.notify:
		case <-deadline.C:
			t.Fatalf("commit=%d, want %d", replica.Snapshot().CommitMin, commit)
		}
	}
}

func processReplicaMessages(t testing.TB, replica *Replica, clientMessages int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	bus := replica.deps.MessageBus.(*captureBus)
	for bus.clientCount() < clientMessages {
		if _, err := replica.Process(64); err != nil {
			t.Fatal(err)
		}
		if bus.clientCount() >= clientMessages {
			return
		}
		select {
		case <-replica.io.Ready():
		case <-replica.notify:
		case <-deadline.C:
			t.Fatalf("client messages=%d, want %d", bus.clientCount(), clientMessages)
		}
	}
}

func closeReplica(t testing.TB, replica *Replica) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := replica.Close(ctx); err != nil {
		t.Errorf("close replica: %v", err)
	}
}

type fixedClock struct{ sample TimeSample }

func (clock fixedClock) Now() TimeSample { return clock.sample }

type observingClock struct {
	sample       TimeSample
	observations int
}

func (clock *observingClock) Now() TimeSample { return clock.sample }

func (clock *observingClock) Observe(_ protocol.ReplicaIndex, _, _, _ uint64) error {
	clock.observations++
	return nil
}

type captureBus struct {
	mu           sync.Mutex
	clients      [][]byte
	destinations []protocol.ClientID
	replicas     [][]byte
}

func (bus *captureBus) SendReplica(_ protocol.ReplicaIndex, message *Message) {
	bus.capture(message, false)
}
func (bus *captureBus) SendClient(client protocol.ClientID, message *Message) {
	bus.mu.Lock()
	bus.destinations = append(bus.destinations, client)
	bus.mu.Unlock()
	bus.capture(message, true)
}
func (bus *captureBus) BroadcastReplicas(message *Message) { bus.capture(message, false) }

func (bus *captureBus) capture(message *Message, client bool) {
	frame, _ := message.Bytes()
	copyOfFrame := append([]byte(nil), frame...)
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if client {
		bus.clients = append(bus.clients, copyOfFrame)
	} else {
		bus.replicas = append(bus.replicas, copyOfFrame)
	}
}

func (bus *captureBus) clientCount() int {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return len(bus.clients)
}

func (bus *captureBus) clientMessage(t testing.TB, index int) []byte {
	t.Helper()
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if index >= len(bus.clients) {
		t.Fatalf("client message %d missing", index)
	}
	return append([]byte(nil), bus.clients[index]...)
}

func (bus *captureBus) clientDestination(index int) protocol.ClientID {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return bus.destinations[index]
}

func (bus *captureBus) replicaMessages() [][]byte {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	messages := make([][]byte, len(bus.replicas))
	copy(messages, bus.replicas)
	return messages
}

func (bus *captureBus) replicaCount() int {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return len(bus.replicas)
}

func (bus *captureBus) replicaMessage(t testing.TB, index int) []byte {
	t.Helper()
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if index >= len(bus.replicas) {
		t.Fatalf("replica message %d missing", index)
	}
	return append([]byte(nil), bus.replicas[index]...)
}

func TestHigherViewPingAndPongDoNotAdvanceConsensusState(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	bus := &captureBus{}
	clock := &observingClock{sample: TimeSample{Wall: 100, Monotonic: 100, Synchronized: true}}
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(config.Cluster.PipelineMax),
		CheckpointMax: 1,
	}}
	replica, err := newReplica(config, Dependencies{
		Storage:      storage,
		MessageBus:   bus,
		Clock:        clock,
		Entropy:      bytes.NewReader([]byte{1}),
		StateMachine: machine,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, replica)
	pool, err := protocol.NewFramePool(2, uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	ping, err := pool.Acquire(uint32(config.Cluster.ReleaseHistoryMax * 4))
	if err != nil {
		t.Fatal(err)
	}
	pingBody, _ := ping.Body()
	binary.LittleEndian.PutUint32(pingBody[:4], 1)
	pingHeader := protocol.Header{Group: config.Group, View: 7, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPing, Author: 0}
	binary.LittleEndian.PutUint64(pingHeader.Fields[24:32], 11)
	binary.LittleEndian.PutUint16(pingHeader.Fields[32:34], 1)
	if err := ping.Seal(&pingHeader); err != nil {
		t.Fatal(err)
	}
	if err := ping.BindReplica(protocol.MemberID{1}, 0); err != nil {
		t.Fatal(err)
	}
	pong, err := pool.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	pongHeader := protocol.Header{Group: config.Group, View: 7, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPong, Author: 0}
	binary.LittleEndian.PutUint64(pongHeader.Fields[:8], 11)
	binary.LittleEndian.PutUint64(pongHeader.Fields[8:16], 22)
	if err := pong.Seal(&pongHeader); err != nil {
		t.Fatal(err)
	}
	if err := pong.BindReplica(protocol.MemberID{1}, 0); err != nil {
		t.Fatal(err)
	}
	if err := replica.Submit(ping); err != nil {
		t.Fatal(err)
	}
	if err := replica.Submit(pong); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Process(2); err != nil {
		t.Fatal(err)
	}
	if snapshot := replica.Snapshot(); snapshot.View != 0 || snapshot.Status != StatusNormal || snapshot.HeadOp != 0 {
		t.Fatalf("ping changed consensus state: %+v", snapshot)
	}
	if clock.observations != 1 {
		t.Fatalf("pong observations = %d", clock.observations)
	}
	messages := bus.replicaMessages()
	if len(messages) != 1 {
		t.Fatalf("ping replies = %d", len(messages))
	}
	reply, _, reason := protocol.DecodeFrame(messages[0], config.Group, uint32(config.Cluster.MessageSizeMax), 1)
	if reason != protocol.RejectNone || reply.Command != protocol.CommandPong {
		t.Fatalf("ping response command=%d reason=%v", reply.Command, reason)
	}
}

func TestReplicaTimersBroadcastCommitAndPing(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	bus := &captureBus{}
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(config.Cluster.PipelineMax),
		CheckpointMax: 1,
	}}
	replica, err := newReplica(config, Dependencies{
		Storage:      storage,
		MessageBus:   bus,
		Clock:        fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
		Entropy:      bytes.NewReader([]byte{1}),
		StateMachine: machine,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, replica)
	for tick := 1; tick <= 100; tick++ {
		monotonic := uint64(tick) * uint64(config.Process.Tick)
		replica.handleTick(TimeSample{Wall: monotonic, Monotonic: monotonic, Synchronized: true})
	}
	var commits, pings int
	for _, frame := range bus.replicaMessages() {
		header, _, reason := protocol.DecodeFrame(frame, config.Group, uint32(config.Cluster.MessageSizeMax), 1)
		if reason != protocol.RejectNone {
			t.Fatalf("timer frame rejected: %v", reason)
		}
		switch header.Command {
		case protocol.CommandCommit:
			commits++
		case protocol.CommandPing:
			pings++
		}
	}
	if commits != 2 || pings != 1 {
		t.Fatalf("timer messages: commits=%d pings=%d", commits, pings)
	}
}

func TestReplicaSingleExitSuspicionDoesNotChangeView(t *testing.T) {
	membership := Membership{
		Members:     [MembersMax]protocol.MemberID{{1}, {2}, {3}},
		ActiveCount: 3,
		LocalMember: protocol.MemberID{2},
	}
	frames, err := protocol.NewFramePool(2, 4096)
	if err != nil {
		t.Fatal(err)
	}
	bus := &captureBus{}
	replica := Replica{
		config:          Config{Group: protocol.GroupID{1}},
		membership:      membership,
		local:           1,
		quorums:         Quorums{ViewChange: 2},
		deps:            Dependencies{MessageBus: bus},
		status:          StatusNormal,
		failureDetector: NewFailureDetector(1),
		frames:          frames,
	}
	replica.handleExitTimeout(TimeSample{Monotonic: uint64(7*time.Second) + 2})
	if replica.status != StatusNormal || replica.view != 0 {
		t.Fatalf("single suspicion changed view: status=%d view=%d", replica.status, replica.view)
	}
	if replica.exitViewBits != 1<<1 || len(bus.replicaMessages()) != 1 {
		t.Fatalf("exit evidence bits=%03b messages=%d", replica.exitViewBits, len(bus.replicaMessages()))
	}
}

func TestUnsynchronizedMultiReplicaClockSuppressesPulse(t *testing.T) {
	membership := Membership{
		Members:     [MembersMax]protocol.MemberID{{1}, {2}},
		ActiveCount: 2,
		LocalMember: protocol.MemberID{1},
	}
	machine := &testStateMachine{pulseNeeded: true}
	replica := Replica{
		config:     Config{Membership: membership},
		membership: membership,
		local:      0,
		status:     StatusNormal,
		deps:       Dependencies{StateMachine: machine},
		pipeline:   make([]pipelineEntry, 1),
	}
	replica.handlePulseTimeout(TimeSample{Wall: 10, Monotonic: 10})
	if replica.pipelineLen != 0 || replica.headOp != 0 {
		t.Fatalf("unsynchronized pulse admitted: head=%d pipeline=%d", replica.headOp, replica.pipelineLen)
	}
}

type testStateMachine struct {
	capacities      StateMachineCapacities
	commits         int
	pulseNeeded     bool
	closes          atomic.Uint32
	resetPending    bool
	resetStarted    chan struct{}
	resetCompletion *SMCompletion
	compacts        int
	checkpoints     int
	validation      ValidationResult
}

func (machine *testStateMachine) Capacities() StateMachineCapacities { return machine.capacities }
func (machine *testStateMachine) Validate(ValidateInput) ValidationResult {
	return machine.validation
}
func (machine *testStateMachine) PulseNeeded(uint64) bool { return machine.pulseNeeded }
func (machine *testStateMachine) StartPrefetch(PrefetchInput, *SMCompletion) (StartResult[PrefetchToken], error) {
	return Ready(PrefetchToken(1)), nil
}
func (machine *testStateMachine) Commit(input CommitInput, _ PrefetchToken, reply []byte) (int, error) {
	machine.commits++
	length := copy(reply, "reply:")
	length += copy(reply[length:], input.Body)
	return length, nil
}
func (machine *testStateMachine) StartCompact(CompactInput, *SMCompletion) (StartResult[CompactResult], error) {
	machine.compacts++
	return Ready(CompactResult{}), nil
}
func (machine *testStateMachine) StartCheckpoint(CheckpointInput, *SMCompletion) (StartResult[CheckpointManifest], error) {
	machine.checkpoints++
	return Ready(CheckpointManifest{}), nil
}
func (machine *testStateMachine) StartOpen(OpenCheckpointInput, *SMCompletion) (StartResult[OpenResult], error) {
	return Ready(OpenResult{}), nil
}
func (machine *testStateMachine) StartReset(completion *SMCompletion) (StartResult[ResetResult], error) {
	if machine.resetPending {
		machine.resetCompletion = completion
		close(machine.resetStarted)
		return Pending[ResetResult](), nil
	}
	return Ready(ResetResult{}), nil
}
func (machine *testStateMachine) Close() error {
	machine.closes.Add(1)
	return nil
}
