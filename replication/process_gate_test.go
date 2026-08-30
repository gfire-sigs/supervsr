package replication

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
	"github.com/rs/zerolog"
)

const (
	processReplicaHelperEnvironment = "SUPERVSR_PROCESS_REPLICA_HELPER"
	processReplicaDirectory         = "SUPERVSR_PROCESS_REPLICA_DIRECTORY"
	processReplicaIndex             = "SUPERVSR_PROCESS_REPLICA_INDEX"
)

type processCrashStage uint32

const (
	processCrashWrite processCrashStage = iota + 1
	processCrashPrepare
	processCrashCommit
	processCrashView
)

func (stage processCrashStage) String() string {
	switch stage {
	case processCrashWrite:
		return "write"
	case processCrashPrepare:
		return "prepare"
	case processCrashCommit:
		return "commit"
	case processCrashView:
		return "view"
	default:
		return "unknown"
	}
}

type processWireMessage struct {
	Kind   string            `json:"kind"`
	Stage  processCrashStage `json:"stage,omitempty"`
	From   uint8             `json:"from,omitempty"`
	To     uint8             `json:"to,omitempty"`
	Source string            `json:"source,omitempty"`
	Frame  []byte            `json:"frame,omitempty"`
	Op     uint64            `json:"op,omitempty"`
	View   uint32            `json:"view,omitempty"`
	Error  string            `json:"error,omitempty"`
}

type processEmitter struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

func (emitter *processEmitter) emit(message processWireMessage) error {
	emitter.mu.Lock()
	defer emitter.mu.Unlock()
	return emitter.encoder.Encode(message)
}

type processStageGate struct {
	armed   atomic.Uint32
	index   protocol.ReplicaIndex
	emitter *processEmitter
}

func (gate *processStageGate) arm(stage processCrashStage) {
	gate.armed.Store(uint32(stage))
}

func (gate *processStageGate) hit(stage processCrashStage, op protocol.Op, view protocol.View) {
	if !gate.armed.CompareAndSwap(uint32(stage), 0) {
		return
	}
	if err := gate.emitter.emit(processWireMessage{
		Kind: "stage", Stage: stage, From: uint8(gate.index), Op: uint64(op), View: uint32(view),
	}); err != nil {
		panic(err)
	}
	select {}
}

type processGateStorage struct {
	Storage
	gate *processStageGate
}

func (storage *processGateStorage) WriteAt(buffer []byte, offset uint64) error {
	if err := storage.Storage.WriteAt(buffer, offset); err != nil {
		return err
	}
	storage.gate.hit(processCrashWrite, 0, 0)
	return nil
}

type processReplicaBus struct {
	index       protocol.ReplicaIndex
	activeCount uint8
	gate        *processStageGate
	emitter     *processEmitter
}

func (bus *processReplicaBus) SendReplica(to protocol.ReplicaIndex, message *Message) {
	frame, err := message.Bytes()
	if err != nil {
		return
	}
	header, reason := protocol.DecodeHeader(frame[:protocol.HeaderSize], processGateGroup(), uint32(processGateClusterConfig().MessageSizeMax), bus.activeCount)
	if err := bus.emitter.emit(processWireMessage{Kind: "replica_frame", From: uint8(bus.index), To: uint8(to), Frame: frame}); err != nil {
		panic(err)
	}
	if reason == protocol.RejectNone && header.Command == protocol.CommandJoinView && to != bus.index {
		bus.gate.hit(processCrashView, 0, header.View)
	}
}

func (bus *processReplicaBus) SendClient(to protocol.ClientID, message *Message) {
	frame, err := message.Bytes()
	if err != nil {
		return
	}
	if err := bus.emitter.emit(processWireMessage{Kind: "client_frame", From: uint8(bus.index), Frame: frame}); err != nil {
		panic(err)
	}
}

func (bus *processReplicaBus) BroadcastReplicas(message *Message) {
	frame, err := message.Bytes()
	if err != nil {
		return
	}
	header, reason := protocol.DecodeHeader(frame[:protocol.HeaderSize], processGateGroup(), uint32(processGateClusterConfig().MessageSizeMax), bus.activeCount)
	trigger := reason == protocol.RejectNone && header.Command == protocol.CommandPrepare
	for index := range bus.activeCount {
		to := protocol.ReplicaIndex(index)
		if to == bus.index {
			continue
		}
		if err := bus.emitter.emit(processWireMessage{Kind: "replica_frame", From: uint8(bus.index), To: uint8(to), Frame: frame}); err != nil {
			panic(err)
		}
	}
	if trigger {
		bus.gate.hit(processCrashPrepare, prepareOp(&header), header.View)
	}
}

type processCounterState struct {
	Counter uint64            `json:"counter"`
	Applied map[string]uint64 `json:"applied"`
}

type processCounterMachine struct {
	path   string
	gate   *processStageGate
	state  processCounterState
	lastOp protocol.Op
}

func openProcessCounterMachine(path string, gate *processStageGate) (*processCounterMachine, error) {
	machine := &processCounterMachine{path: path, gate: gate, state: processCounterState{Applied: make(map[string]uint64)}}
	encoded, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return machine, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(encoded, &machine.state); err != nil || machine.state.Applied == nil {
		return nil, ErrStateMachine
	}
	return machine, nil
}

func (*processCounterMachine) Capacities() StateMachineCapacities {
	config := processGateClusterConfig()
	return StateMachineCapacities{
		RequestBytes: uint32(config.ApplicationBatchSizeMax), ReplyBytes: uint32(config.ApplicationReplySizeMax),
		PrefetchMax: uint32(config.PipelineMax), CheckpointMax: 1,
	}
}

func (*processCounterMachine) Validate(input ValidateInput) ValidationResult {
	switch input.Operation {
	case protocol.OperationApplicationMin:
		if len(input.Body) == 0 {
			return ValidationInvalidBody
		}
		return ValidationOK
	case protocol.OperationApplicationMin + 1:
		if len(input.Body) != 0 {
			return ValidationInvalidBody
		}
		return ValidationOK
	default:
		return ValidationInvalidOperation
	}
}

func (*processCounterMachine) PulseNeeded(uint64) bool { return false }

func (*processCounterMachine) StartPrefetch(PrefetchInput, *SMCompletion) (StartResult[PrefetchToken], error) {
	return Ready(PrefetchToken(0)), nil
}

func (machine *processCounterMachine) Commit(input CommitInput, _ PrefetchToken, reply []byte) (int, error) {
	if input.Op <= machine.lastOp {
		return 0, ErrStateMachine
	}
	machine.lastOp = input.Op
	switch input.Operation {
	case protocol.OperationPulse:
		return 0, nil
	case protocol.OperationApplicationMin:
		key := string(input.Body)
		value, found := machine.state.Applied[key]
		if !found {
			machine.state.Counter++
			value = machine.state.Counter
			machine.state.Applied[key] = value
			if err := machine.persist(); err != nil {
				return 0, err
			}
			machine.gate.hit(processCrashCommit, input.Op, 0)
		}
		return encodeProcessCounter(reply, value)
	case protocol.OperationApplicationMin + 1:
		return encodeProcessCounter(reply, machine.state.Counter)
	default:
		return 0, ErrStateMachine
	}
}

func (*processCounterMachine) StartCompact(CompactInput, *SMCompletion) (StartResult[CompactResult], error) {
	return Ready(CompactResult{}), nil
}

func (*processCounterMachine) StartCheckpoint(CheckpointInput, *SMCompletion) (StartResult[CheckpointManifest], error) {
	return Ready(CheckpointManifest{}), nil
}

func (machine *processCounterMachine) StartOpen(input OpenCheckpointInput, _ *SMCompletion) (StartResult[OpenResult], error) {
	if checkpointOp := input.State.PrepareOp(); checkpointOp > machine.lastOp {
		machine.lastOp = checkpointOp
	}
	return Ready(OpenResult{}), nil
}

func (machine *processCounterMachine) StartReset(*SMCompletion) (StartResult[ResetResult], error) {
	return Ready(ResetResult{}), nil
}

func (*processCounterMachine) Close() error { return nil }

func (machine *processCounterMachine) persist() error {
	encoded, err := json.Marshal(machine.state)
	if err != nil {
		return err
	}
	temporary := machine.path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(encoded); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, machine.path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(machine.path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func encodeProcessCounter(reply []byte, value uint64) (int, error) {
	if len(reply) < 8 {
		return 0, ErrStateMachine
	}
	binary.LittleEndian.PutUint64(reply, value)
	return 8, nil
}

func TestReplicaProcessHelper(t *testing.T) {
	if os.Getenv(processReplicaHelperEnvironment) != "1" {
		return
	}
	if err := runReplicaProcessHelper(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		t.FailNow()
	}
}

func runReplicaProcessHelper() error {
	indexValue, err := strconv.ParseUint(os.Getenv(processReplicaIndex), 10, 8)
	if err != nil || indexValue >= 3 {
		return ErrInvalidConfiguration
	}
	index := protocol.ReplicaIndex(indexValue)
	directory := os.Getenv(processReplicaDirectory)
	emitter := &processEmitter{encoder: json.NewEncoder(os.Stdout)}
	gate := &processStageGate{index: index, emitter: emitter}
	storage, err := OpenFileStorage(processGateReplicaPath(directory, index), false, false)
	if err != nil {
		return err
	}
	gatedStorage := &processGateStorage{Storage: storage, gate: gate}
	machine, err := openProcessCounterMachine(processGateCounterPath(directory, index), gate)
	if err != nil {
		_ = storage.Close()
		return err
	}
	config := processGateReplicaConfig(index)
	bus := &processReplicaBus{index: index, activeCount: 3, gate: gate, emitter: emitter}
	logger := zerolog.Nop()
	replica, err := Open(context.Background(), config, Dependencies{
		Storage: gatedStorage, MessageBus: bus, Clock: newProcessGateClock(), Entropy: bytes.NewReader(bytes.Repeat([]byte{byte(index) + 1}, 32)),
		StateMachine: machine, Logger: &logger,
	})
	if err != nil {
		_ = storage.Close()
		return err
	}
	pool, err := protocol.NewFramePool(128, uint32(config.Cluster.MessageSizeMax))
	if err != nil {
		_ = replica.Close(context.Background())
		return err
	}
	if err := emitter.emit(processWireMessage{Kind: "ready", From: uint8(index)}); err != nil {
		_ = replica.Close(context.Background())
		return err
	}
	runDone := make(chan error, 1)
	go func() { runDone <- replica.Run(context.Background()) }()
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 256<<10)
	for scanner.Scan() {
		var command processWireMessage
		if err := json.Unmarshal(scanner.Bytes(), &command); err != nil {
			return err
		}
		switch command.Kind {
		case "arm":
			gate.arm(command.Stage)
			if err := emitter.emit(processWireMessage{Kind: "armed", Stage: command.Stage, From: uint8(index)}); err != nil {
				return err
			}
		case "frame":
			frame, err := pool.AcquireEncoded(command.Frame)
			if err == nil {
				if command.Source == "client" {
					err = frame.BindClient()
				} else {
					members := processGateMembers()
					err = frame.BindReplica(members[command.From], protocol.ReplicaIndex(command.From))
				}
			}
			if err == nil {
				err = replica.Submit(frame)
			}
			if err != nil {
				if frame != nil {
					frame.Release()
				}
				return err
			}
		case "stop":
			closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			err := replica.Close(closeCtx)
			cancel()
			runErr := <-runDone
			if errors.Is(runErr, context.Canceled) {
				runErr = nil
			}
			return errors.Join(err, runErr)
		default:
			return ErrInvalidConfiguration
		}
		select {
		case runErr := <-runDone:
			return runErr
		default:
		}
	}
	return scanner.Err()
}

type processGateClock struct {
	start time.Time
}

func newProcessGateClock() *processGateClock {
	return &processGateClock{start: time.Now()}
}

func (clock *processGateClock) Now() TimeSample {
	now := time.Now()
	return TimeSample{Wall: uint64(now.UnixNano()), Monotonic: uint64(now.Sub(clock.start)) + 1, Synchronized: true}
}

func processGateGroup() protocol.GroupID {
	return protocol.GroupID{0x51, 0x27}
}

func processGateMembers() [MembersMax]protocol.MemberID {
	var members [MembersMax]protocol.MemberID
	for index := range uint8(3) {
		members[index][15] = index + 1
	}
	return members
}

func processGateClusterConfig() ClusterConfig {
	config := DefaultClusterConfig()
	config.ClientsMax = 4
	config.PipelineMax = 4
	config.ViewChangeHeadersSuffixMax = 5
	config.JournalSlots = 128
	config.MessageSizeMax = 64 << 10
	config.ApplicationBatchSizeMax = 512
	config.ApplicationReplySizeMax = 512
	config.BlockSize = 64 << 10
	config.CompactionOps = 32
	return config
}

func processGateProcessConfig() ProcessConfig {
	config := DefaultProcessConfig()
	cluster := processGateClusterConfig()
	base, _ := cluster.BlockBase()
	config.StorageSizeLimit = base + 64*cluster.BlockSize
	return config
}

func processGateReplicaConfig(index protocol.ReplicaIndex) Config {
	members := processGateMembers()
	return Config{
		Group: processGateGroup(), Membership: Membership{Members: members, ActiveCount: 3, LocalMember: members[index]},
		Cluster: processGateClusterConfig(), Process: processGateProcessConfig(), CurrentRelease: 1, ClientReleaseMin: 1,
	}
}

func processGateReplicaPath(directory string, index protocol.ReplicaIndex) string {
	return filepath.Join(directory, fmt.Sprintf("replica-%d.vsr", index))
}

func processGateCounterPath(directory string, index protocol.ReplicaIndex) string {
	return filepath.Join(directory, fmt.Sprintf("counter-%d.json", index))
}

type processReplicaEvent struct {
	process *replicaChildProcess
	message processWireMessage
}

type processLockedBuffer struct {
	lock   sync.Mutex
	buffer bytes.Buffer
}

func (buffer *processLockedBuffer) Write(data []byte) (int, error) {
	buffer.lock.Lock()
	defer buffer.lock.Unlock()
	return buffer.buffer.Write(data)
}

func (buffer *processLockedBuffer) String() string {
	buffer.lock.Lock()
	defer buffer.lock.Unlock()
	return buffer.buffer.String()
}

type replicaChildProcess struct {
	index   protocol.ReplicaIndex
	command *exec.Cmd
	input   io.WriteCloser
	encoder *json.Encoder
	events  chan<- processReplicaEvent
	stderr  processLockedBuffer
	writeMu sync.Mutex
}

func startReplicaChild(t testing.TB, directory string, index protocol.ReplicaIndex, events chan<- processReplicaEvent) *replicaChildProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestReplicaProcessHelper$", "-test.count=1")
	command.Env = append(os.Environ(),
		processReplicaHelperEnvironment+"=1", processReplicaDirectory+"="+directory,
		processReplicaIndex+"="+strconv.FormatUint(uint64(index), 10),
	)
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	process := &replicaChildProcess{index: index, command: command, input: input, encoder: json.NewEncoder(input), events: events}
	command.Stderr = &process.stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	go process.readEvents(output)
	return process
}

func (process *replicaChildProcess) readEvents(output io.Reader) {
	decoder := json.NewDecoder(output)
	for {
		var message processWireMessage
		if err := decoder.Decode(&message); err != nil {
			return
		}
		process.events <- processReplicaEvent{process: process, message: message}
	}
}

func (process *replicaChildProcess) send(message processWireMessage) error {
	process.writeMu.Lock()
	defer process.writeMu.Unlock()
	return process.encoder.Encode(message)
}

func (process *replicaChildProcess) kill() error {
	if err := process.command.Process.Kill(); err != nil {
		return err
	}
	if err := process.command.Wait(); err == nil {
		return errors.New("killed replica process exited successfully")
	}
	return nil
}

func (process *replicaChildProcess) stop() error {
	if err := process.send(processWireMessage{Kind: "stop"}); err != nil {
		return err
	}
	waited := make(chan error, 1)
	go func() { waited <- process.command.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			return fmt.Errorf("replica %d stop: %w: %s", process.index, err, process.stderr.String())
		}
		return nil
	case <-time.After(30 * time.Second):
		_ = process.command.Process.Kill()
		<-waited
		return fmt.Errorf("replica %d stop timed out: %s", process.index, process.stderr.String())
	}
}

type processClientEvents struct {
	replies []ClientReply
	evicted protocol.EvictionReason
}

func (events *processClientEvents) Reply(reply ClientReply) {
	reply.Body = append([]byte(nil), reply.Body...)
	events.replies = append(events.replies, reply)
}

func (events *processClientEvents) Evicted(reason protocol.EvictionReason) {
	events.evicted = reason
}

type processClusterHarness struct {
	t              testing.TB
	directory      string
	processes      [3]*replicaChildProcess
	events         chan processReplicaEvent
	ready          [3]bool
	armed          [3]processCrashStage
	stage          processWireMessage
	dropFrom       protocol.ReplicaIndex
	dropFromActive bool
	client         *Client
	clientEvents   *processClientEvents
	clock          *processGateClock
	lastReplyFrom  protocol.ReplicaIndex
	lastReplyView  protocol.View
}

func newProcessClusterHarness(t testing.TB) *processClusterHarness {
	t.Helper()
	directory := t.TempDir()
	formatProcessGateCluster(t, directory)
	cluster := &processClusterHarness{t: t, directory: directory, events: make(chan processReplicaEvent, 4096), clock: newProcessGateClock()}
	t.Cleanup(cluster.close)
	for index := range uint8(3) {
		cluster.start(protocol.ReplicaIndex(index))
	}
	cluster.wait(30*time.Second, func() bool { return cluster.ready[0] && cluster.ready[1] && cluster.ready[2] })
	clientEvents := &processClientEvents{}
	client, err := NewClient(ClientConfig{
		Group: processGateGroup(), ID: protocol.ClientID{0x61}, Release: 1, ActiveCount: 3,
		MessageSizeMax: uint32(processGateClusterConfig().MessageSizeMax), Process: processGateProcessConfig(),
	}, processHarnessClientBus{cluster: cluster}, cluster.clock, bytes.NewReader(bytes.Repeat([]byte{1}, 32)), clientEvents)
	if err != nil {
		t.Fatal(err)
	}
	cluster.client = client
	cluster.clientEvents = clientEvents
	if err := client.Register(); err != nil {
		t.Fatal(err)
	}
	cluster.waitReplies(1)
	return cluster
}

func formatProcessGateCluster(t testing.TB, directory string) {
	t.Helper()
	for index := range uint8(3) {
		replicaIndex := protocol.ReplicaIndex(index)
		storage, err := OpenFileStorage(processGateReplicaPath(directory, replicaIndex), true, false)
		if err != nil {
			t.Fatal(err)
		}
		config := processGateReplicaConfig(replicaIndex)
		err = Format(context.Background(), FormatConfig{
			Group: config.Group, Membership: config.Membership, Cluster: config.Cluster, CurrentRelease: config.CurrentRelease,
		}, FormatDependencies{Storage: storage})
		closeErr := storage.Close()
		if err != nil || closeErr != nil {
			t.Fatal(errors.Join(err, closeErr))
		}
	}
}

func (cluster *processClusterHarness) start(index protocol.ReplicaIndex) {
	process := startReplicaChild(cluster.t, cluster.directory, index, cluster.events)
	cluster.processes[index] = process
	cluster.ready[index] = false
}

func (cluster *processClusterHarness) route(event processReplicaEvent) {
	message := event.message
	index := event.process.index
	if cluster.processes[index] != event.process {
		return
	}
	switch message.Kind {
	case "ready":
		cluster.ready[index] = true
	case "armed":
		cluster.armed[index] = message.Stage
	case "stage":
		cluster.stage = message
	case "replica_frame":
		if cluster.dropFromActive && index == cluster.dropFrom && protocol.ReplicaIndex(message.To) != index {
			return
		}
		target := cluster.processes[message.To]
		if target != nil {
			if err := target.send(processWireMessage{Kind: "frame", Source: "replica", From: uint8(index), Frame: message.Frame}); err != nil {
				cluster.t.Fatalf("route replica %d to %d: %v", index, message.To, err)
			}
		}
	case "client_frame":
		if cluster.client == nil {
			return
		}
		header, _, reason := protocol.DecodeFrame(message.Frame, processGateGroup(), uint32(processGateClusterConfig().MessageSizeMax), 3)
		if reason == protocol.RejectNone && header.Command == protocol.CommandReply {
			cluster.lastReplyFrom = header.Author
			cluster.lastReplyView = header.View
		}
		cluster.client.HandleFrame(index, message.Frame)
	case "error":
		cluster.t.Fatalf("replica %d: %s", index, message.Error)
	default:
		cluster.t.Fatalf("replica %d emitted unknown event %q", index, message.Kind)
	}
}

func (cluster *processClusterHarness) wait(timeout time.Duration, ready func() bool) {
	cluster.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(processGateProcessConfig().Tick)
	defer tick.Stop()
	for !ready() {
		select {
		case event := <-cluster.events:
			cluster.route(event)
		case <-tick.C:
			if cluster.client != nil {
				if err := cluster.client.Tick(); err != nil {
					cluster.t.Fatal(err)
				}
			}
		case <-deadline.C:
			replies := 0
			if cluster.clientEvents != nil {
				replies = len(cluster.clientEvents.replies)
			}
			cluster.t.Fatalf("process gate timed out: stage=%s replies=%d stderr=%s", cluster.stage.Stage, replies, cluster.stderr())
		}
	}
}

func (cluster *processClusterHarness) waitReplies(count int) {
	cluster.wait(45*time.Second, func() bool { return len(cluster.clientEvents.replies) >= count })
	if cluster.clientEvents.evicted != protocol.EvictionReserved {
		cluster.t.Fatalf("client evicted: %d", cluster.clientEvents.evicted)
	}
}

func (cluster *processClusterHarness) arm(index protocol.ReplicaIndex, stage processCrashStage) {
	process := cluster.processes[index]
	if process == nil {
		cluster.t.Fatalf("replica %d is not running", index)
	}
	cluster.armed[index] = 0
	if err := process.send(processWireMessage{Kind: "arm", Stage: stage}); err != nil {
		cluster.t.Fatal(err)
	}
	cluster.wait(10*time.Second, func() bool { return cluster.armed[index] == stage })
}

func (cluster *processClusterHarness) waitStage(stage processCrashStage) processWireMessage {
	cluster.stage = processWireMessage{}
	cluster.wait(45*time.Second, func() bool { return cluster.stage.Stage == stage })
	return cluster.stage
}

func (cluster *processClusterHarness) kill(index protocol.ReplicaIndex) {
	process := cluster.processes[index]
	cluster.processes[index] = nil
	cluster.ready[index] = false
	if err := process.kill(); err != nil {
		cluster.t.Fatal(err)
	}
}

func (cluster *processClusterHarness) restart(index protocol.ReplicaIndex) {
	cluster.start(index)
	cluster.wait(30*time.Second, func() bool { return cluster.ready[index] })
}

func (cluster *processClusterHarness) counterState(index protocol.ReplicaIndex) (processCounterState, bool) {
	encoded, err := os.ReadFile(processGateCounterPath(cluster.directory, index))
	if err != nil {
		return processCounterState{}, false
	}
	var state processCounterState
	if json.Unmarshal(encoded, &state) != nil {
		return processCounterState{}, false
	}
	return state, true
}

func (cluster *processClusterHarness) waitApplied(key string) {
	cluster.wait(45*time.Second, func() bool {
		for index := range uint8(3) {
			state, ok := cluster.counterState(protocol.ReplicaIndex(index))
			if !ok || state.Counter != 1 || state.Applied[key] != 1 {
				return false
			}
		}
		return true
	})
}

func (cluster *processClusterHarness) close() {
	if cluster.client != nil {
		_ = cluster.client.Close()
	}
	for index := range cluster.processes {
		process := cluster.processes[index]
		if process == nil {
			continue
		}
		cluster.processes[index] = nil
		if err := process.stop(); err != nil {
			cluster.t.Error(err)
		}
	}
}

func (cluster *processClusterHarness) stderr() string {
	var output bytes.Buffer
	for _, process := range cluster.processes {
		if process != nil {
			if stderr := process.stderr.String(); stderr != "" {
				fmt.Fprintf(&output, " replica%d=%q", process.index, stderr)
			}
		}
	}
	return output.String()
}

type processHarnessClientBus struct {
	cluster *processClusterHarness
}

func (bus processHarnessClientBus) SendReplica(to protocol.ReplicaIndex, message *Message) {
	process := bus.cluster.processes[to]
	if process == nil {
		return
	}
	frame, err := message.Bytes()
	if err != nil {
		return
	}
	if err := process.send(processWireMessage{Kind: "frame", Source: "client", Frame: frame}); err != nil {
		bus.cluster.t.Fatal(err)
	}
}

func (processHarnessClientBus) SendClient(protocol.ClientID, *Message) {}

func (bus processHarnessClientBus) BroadcastReplicas(message *Message) {
	for index := range uint8(3) {
		bus.SendReplica(protocol.ReplicaIndex(index), message)
	}
}

func TestProcessPrimaryCrashStagesApplyLogicalIncrementOnce(t *testing.T) {
	if os.Getenv(processReplicaHelperEnvironment) == "1" {
		return
	}
	for _, stage := range []processCrashStage{processCrashWrite, processCrashPrepare, processCrashCommit, processCrashView} {
		t.Run(stage.String(), func(t *testing.T) {
			cluster := newProcessClusterHarness(t)
			primary := cluster.lastReplyFrom
			initialView := cluster.lastReplyView
			key := "increment-" + stage.String()
			if stage == processCrashView {
				cluster.dropFrom = primary
				cluster.dropFromActive = true
				cluster.arm(primary, stage)
			} else {
				cluster.arm(primary, stage)
				if err := cluster.client.Submit(protocol.OperationApplicationMin, []byte(key)); err != nil {
					t.Fatal(err)
				}
			}
			trigger := cluster.waitStage(stage)
			killed := protocol.ReplicaIndex(trigger.From)
			if killed != primary {
				t.Fatalf("%s stage fired on replica %d, want primary %d", stage, killed, primary)
			}
			switch stage {
			case processCrashPrepare, processCrashCommit:
				if trigger.Op == 0 {
					t.Fatalf("%s stage did not identify a prepared operation", stage)
				}
			case processCrashView:
				if protocol.View(trigger.View) <= initialView {
					t.Fatalf("view stage=%d, want greater than initial view %d", trigger.View, initialView)
				}
			}
			cluster.kill(killed)
			cluster.dropFromActive = false
			if stage == processCrashView {
				if err := cluster.client.Submit(protocol.OperationApplicationMin, []byte(key)); err != nil {
					t.Fatal(err)
				}
			}
			cluster.waitReplies(2)
			if reply := cluster.clientEvents.replies[1]; len(reply.Body) != 8 || binary.LittleEndian.Uint64(reply.Body) != 1 {
				t.Fatalf("increment reply=%x, want 1", reply.Body)
			}
			cluster.restart(killed)
			cluster.waitApplied(key)
			if err := cluster.client.Submit(protocol.OperationApplicationMin+1, nil); err != nil {
				t.Fatal(err)
			}
			cluster.waitReplies(3)
			if reply := cluster.clientEvents.replies[2]; len(reply.Body) != 8 || binary.LittleEndian.Uint64(reply.Body) != 1 {
				t.Fatalf("recovered counter reply=%x, want 1", reply.Body)
			}
		})
	}
}
