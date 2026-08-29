package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type testReleaseExecutor struct {
	releases []protocol.Release
	err      error
	calls    int
	target   protocol.Release
}

func (executor *testReleaseExecutor) Releases() []protocol.Release {
	return executor.releases
}

func (executor *testReleaseExecutor) Execute(target protocol.Release) error {
	executor.calls++
	executor.target = target
	return executor.err
}

func TestReleaseSelectionRequiresEveryActiveReplica(t *testing.T) {
	replica := Replica{
		config: Config{CurrentRelease: 1},
		membership: Membership{
			Members:     [MembersMax]protocol.MemberID{{1}, {2}, {3}},
			ActiveCount: 3,
		},
		local:          0,
		status:         StatusNormal,
		view:           3,
		releaseHistory: []protocol.Release{1, 2, 3},
		releaseReports: []releaseReport{
			{releases: make([]protocol.Release, 3)},
			{valid: true, view: 3, count: 2, releases: []protocol.Release{1, 2, 0}},
			{releases: make([]protocol.Release, 3)},
		},
	}
	replica.refreshLocalReleaseReport()
	replica.maybeSelectUpgrade()
	if replica.upgradeTarget != 0 {
		t.Fatalf("target without quorum reports = %d", replica.upgradeTarget)
	}
	replica.releaseReports[2] = releaseReport{valid: true, view: 3, count: 3, releases: []protocol.Release{1, 2, 3}}
	replica.maybeSelectUpgrade()
	if replica.upgradeTarget != 2 {
		t.Fatalf("target = %d, want 2", replica.upgradeTarget)
	}
}

func TestRecoveredUpgradeStatePreservesPartialAndCompleteBars(t *testing.T) {
	cluster := compactTestClusterConfig()
	checkpoint := protocol.Op(0)
	next, ok := checkpointAfter(cluster, checkpoint)
	if !ok {
		t.Fatal("next checkpoint unavailable")
	}
	trigger, ok := checkpointTrigger(cluster, next)
	if !ok {
		t.Fatal("checkpoint trigger unavailable")
	}
	body := make([]byte, 16)
	body[0] = 2
	makeHeader := func(op protocol.Op, operation protocol.Operation) protocol.Header {
		header := protocol.Header{Release: 1}
		binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(op))
		header.Fields[124] = byte(operation)
		return header
	}

	var partial recoveredUpgradeState
	for op := next + 1; op < trigger; op++ {
		if err := partial.observe(cluster, checkpoint, 1, makeHeader(op, protocol.OperationUpgrade), body); err != nil {
			t.Fatal(err)
		}
	}
	if partial.target != 2 || !partial.window.started || !partial.window.valid || partial.window.checkpoint != next || partial.window.target != 2 {
		t.Fatalf("partial recovery = %+v", partial)
	}
	if err := partial.observe(cluster, checkpoint, 1, makeHeader(trigger, protocol.OperationUpgrade), body); err != nil {
		t.Fatal(err)
	}
	if !partial.window.valid || partial.window.target != 2 {
		t.Fatalf("complete recovery = %+v", partial)
	}

	var interrupted recoveredUpgradeState
	if err := interrupted.observe(cluster, checkpoint, 1, makeHeader(next+1, protocol.OperationNoop), nil); err != nil {
		t.Fatal(err)
	}
	if !interrupted.window.started || interrupted.window.valid {
		t.Fatalf("interrupted recovery = %+v", interrupted)
	}
}

func TestRecoveredUpgradeAtCheckpointReleaseIsReplayNoop(t *testing.T) {
	cluster := compactTestClusterConfig()
	checkpoint, ok := checkpointAfter(cluster, 0)
	if !ok {
		t.Fatal("checkpoint unavailable")
	}
	header := protocol.Header{Release: 1}
	binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(checkpoint+1))
	header.Fields[124] = byte(protocol.OperationUpgrade)
	body := make([]byte, 16)
	body[0] = 2
	var recovered recoveredUpgradeState
	if err := recovered.observe(cluster, checkpoint, 2, header, body); err != nil {
		t.Fatal(err)
	}
	if recovered.target != 0 || recovered.window.started {
		t.Fatalf("replayed target checkpoint upgrade = %+v", recovered)
	}
}

func TestOpenRecoversPartialUpgradeBar(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	capacities := StateMachineCapacities{
		RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(config.Cluster.PipelineMax),
		CheckpointMax: 1,
	}
	executor := &testReleaseExecutor{releases: []protocol.Release{1, 2}}
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: &testStateMachine{capacities: capacities},
		ReleaseExecutor: executor,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, ok := checkpointAfter(config.Cluster, 0)
	if !ok {
		t.Fatal("checkpoint unavailable")
	}
	target := checkpoint + 2
	for op := protocol.Op(1); op <= target; op++ {
		replica.handlePulseTimeout(TimeSample{Wall: uint64(op), Monotonic: uint64(op), Synchronized: true})
		processReplicaUntil(t, replica, op)
	}
	closeReplica(t, replica)
	storage.Crash()

	reopened, err := Open(context.Background(), config, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 100, Monotonic: 100, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{2}), StateMachine: &testStateMachine{capacities: capacities},
		ReleaseExecutor: executor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.upgradeTarget != 2 || !reopened.upgradeWindow.started || !reopened.upgradeWindow.valid {
		t.Fatalf("recovered upgrade = target %d window %+v", reopened.upgradeTarget, reopened.upgradeWindow)
	}
	if reopened.upgradeWindow.checkpoint != checkpoint || reopened.upgradeWindow.target != 2 {
		t.Fatalf("recovered window = %+v", reopened.upgradeWindow)
	}
	closeReplica(t, reopened)
}

func TestUpgradeHandoffReopensAtTargetRelease(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	capacities := StateMachineCapacities{
		RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(config.Cluster.PipelineMax),
		CheckpointMax: 1,
	}
	handoffErr := errors.New("release process replaced")
	executor := &testReleaseExecutor{releases: []protocol.Release{1, 2}, err: handoffErr}
	machine := &testStateMachine{capacities: capacities}
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: machine, ReleaseExecutor: executor,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, ok := checkpointAfter(config.Cluster, 0)
	if !ok {
		t.Fatal("checkpoint unavailable")
	}
	trigger, ok := checkpointTrigger(config.Cluster, checkpoint)
	if !ok {
		t.Fatal("checkpoint trigger unavailable")
	}
	commitUpgrade := func(op protocol.Op) {
		t.Helper()
		replica.handlePulseTimeout(TimeSample{Wall: uint64(op), Monotonic: uint64(op), Synchronized: true})
		processReplicaUntil(t, replica, op)
	}
	for op := protocol.Op(1); op <= checkpoint+2; op++ {
		commitUpgrade(op)
	}
	window := replica.upgradeWindow
	replica.beginViewChangeNow(replica.view + 1)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for replica.status != StatusNormal {
		if _, err := replica.Process(64); err != nil {
			t.Fatal(err)
		}
		select {
		case <-replica.io.Ready():
		case <-replica.notify:
		case <-deadline.C:
			t.Fatal("view change did not install")
		default:
		}
	}
	if replica.upgradeTarget != 2 || replica.upgradeWindow != window {
		t.Fatalf("upgrade changed across view change: target=%d window=%+v want=%+v", replica.upgradeTarget, replica.upgradeWindow, window)
	}
	for op := checkpoint + 3; op < trigger; op++ {
		commitUpgrade(op)
	}
	replica.handlePulseTimeout(TimeSample{Wall: uint64(trigger), Monotonic: uint64(trigger), Synchronized: true})
	for executor.calls == 0 {
		_, processErr := replica.Process(64)
		if processErr != nil && !errors.Is(processErr, handoffErr) {
			t.Fatal(processErr)
		}
		select {
		case <-replica.io.Ready():
		case <-replica.notify:
		case <-deadline.C:
			t.Fatal("release handoff did not execute")
		default:
		}
	}
	if durable := replica.superblocks.Current().State.Checkpoint; durable.Release != 2 || durable.PrepareOp() != checkpoint {
		t.Fatalf("durable checkpoint release=%d op=%d", durable.Release, durable.PrepareOp())
	}
	closeReplica(t, replica)
	storage.Crash()

	recoveryHandoff := &testReleaseExecutor{releases: []protocol.Release{1, 2}, err: handoffErr}
	if _, err := Open(context.Background(), config, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 200, Monotonic: 200, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{2}), StateMachine: &testStateMachine{capacities: capacities},
		ReleaseExecutor: recoveryHandoff,
	}); !errors.Is(err, handoffErr) {
		t.Fatalf("old release open error = %v", err)
	}
	if recoveryHandoff.calls != 1 || recoveryHandoff.target != 2 || storage.operation != 0 {
		t.Fatalf("recovery handoff calls=%d target=%d storage operations=%d", recoveryHandoff.calls, recoveryHandoff.target, storage.operation)
	}
	storage.Crash()

	targetConfig := config
	targetConfig.CurrentRelease = 2
	targetExecutor := &testReleaseExecutor{releases: []protocol.Release{2}}
	reopened, err := Open(context.Background(), targetConfig, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 300, Monotonic: 300, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{3}), StateMachine: &testStateMachine{capacities: capacities},
		ReleaseExecutor: targetExecutor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.upgradeTarget != 0 || reopened.upgradeWindow.started || !reopened.accepting.Load() || targetExecutor.calls != 0 {
		t.Fatalf("target reopen target=%d window=%+v accepting=%t calls=%d", reopened.upgradeTarget, reopened.upgradeWindow, reopened.accepting.Load(), targetExecutor.calls)
	}
	pool, err := protocol.NewFramePool(1, uint32(targetConfig.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	request := makeClientRequest(t, pool, targetConfig.Group, protocol.ClientID{9}, 0, 0, protocol.Checksum{}, protocol.OperationRegister, nil)
	if err := reopened.Submit(request); err != nil {
		t.Fatal(err)
	}
	processReplicaUntil(t, reopened, trigger+1)
	closeReplica(t, reopened)
}

func TestReleaseActivationDrainsResetIOAndOwnedFrames(t *testing.T) {
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
	executeErr := errors.New("release handoff stopped")
	executor := &testReleaseExecutor{releases: []protocol.Release{1, 2}, err: executeErr}
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: machine, ReleaseExecutor: executor,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		machine.resetPending = false
		closeReplica(t, replica)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := replica.io.Close(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	blocked := newBlockingStorage()
	ioEngine, err := NewIOEngine(blocked, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	replica.io = ioEngine
	if _, err := ioEngine.Submit(IOOperation{Kind: IOWrite, Buffer: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked.started:
	case <-time.After(time.Second):
		t.Fatal("blocking write did not start")
	}
	for op := protocol.Op(1); op <= 2; op++ {
		message, err := replica.frames.Acquire(0)
		if err != nil {
			t.Fatal(err)
		}
		header := protocol.Header{}
		binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(op))
		if _, ok := replica.pushPipeline(message, header); !ok {
			t.Fatal("pipeline push failed")
		}
	}

	replica.beginReleaseActivation(2)
	if replica.pipelineLen != 1 {
		t.Fatalf("pipeline length after checkpoint release = %d, want 1", replica.pipelineLen)
	}
	if _, err := replica.Process(64); err != nil {
		t.Fatal(err)
	}
	if executor.calls != 0 {
		t.Fatal("release executed before reset completion")
	}
	if err := machine.resetCompletion.Complete(SMResult{Kind: SMCompletionReset}); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Process(64); err != nil {
		t.Fatal(err)
	}
	if executor.calls != 0 {
		t.Fatal("release executed before IO completion")
	}
	replica.submitters.Add(1)
	close(blocked.release)

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for !ioEngine.Drained() {
		select {
		case <-ioEngine.Ready():
			if _, err := replica.Process(64); err != nil {
				t.Fatal(err)
			}
		case <-deadline.C:
			t.Fatal("IO did not drain")
		}
	}
	if executor.calls != 0 {
		t.Fatal("release executed while a submitter owned admission")
	}
	late, err := replica.frames.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	if !replica.events.TryPush(replicaEvent{kind: replicaEventMessage, message: late}) {
		t.Fatal("late admitted message was not queued")
	}
	replica.submitters.Add(-1)
	for executor.calls == 0 {
		_, processErr := replica.Process(64)
		if processErr != nil && !errors.Is(processErr, executeErr) {
			t.Fatal(processErr)
		}
		select {
		case <-ioEngine.Ready():
		case <-deadline.C:
			t.Fatal("release executor was not called")
		default:
		}
	}
	if executor.target != 2 || replica.pipelineLen != 0 || !ioEngine.Drained() {
		t.Fatalf("activation target=%d pipeline=%d drained=%t", executor.target, replica.pipelineLen, ioEngine.Drained())
	}
}
