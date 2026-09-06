package replication

import (
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
	"strings"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

const processReleaseExitCode = 23

var errProcessHandoffInjected = errors.New("injected release executor handoff failure")

// Both advertised releases use this executable; this exercises handoff, not binary compatibility.
type processReleaseExecutor struct {
	current protocol.Release
	mode    string
	emitter *processEmitter
	exit    chan struct{}
}

func (executor *processReleaseExecutor) Releases() []protocol.Release {
	if executor.current == 1 && executor.mode != "" {
		return []protocol.Release{1, 2}
	}
	return []protocol.Release{executor.current}
}

func (executor *processReleaseExecutor) Execute(target protocol.Release) error {
	message := processWireMessage{Kind: "handoff", Release: target}
	if executor.mode == "fail" {
		message.Error = errProcessHandoffInjected.Error()
	}
	if err := executor.emitter.emit(message); err != nil {
		return err
	}
	<-executor.exit
	if executor.mode == "fail" {
		return errProcessHandoffInjected
	}
	os.Exit(processReleaseExitCode)
	return ErrReleaseExecutorReturned
}

func (cluster *processClusterHarness) freshClient(id protocol.ClientID, release protocol.Release) {
	cluster.t.Helper()
	if cluster.client != nil {
		if err := cluster.client.Close(); err != nil {
			cluster.t.Fatal(err)
		}
	}
	events := &processClientEvents{}
	client, err := NewClient(ClientConfig{
		Group: processGateGroup(), ID: id, Release: release, ActiveCount: 3,
		MessageSizeMax: uint32(processGateClusterConfig().MessageSizeMax), Process: processGateProcessConfig(),
	}, processHarnessClientBus{cluster: cluster}, cluster.clock, bytes.NewReader(bytes.Repeat([]byte{id[0]}, 32)), events)
	if err != nil {
		cluster.t.Fatal(err)
	}
	cluster.client, cluster.clientEvents = client, events
}

func (cluster *processClusterHarness) registerFreshClient(id protocol.ClientID, release protocol.Release) {
	cluster.freshClient(id, release)
	if err := cluster.client.Register(); err != nil {
		cluster.t.Fatal(err)
	}
	cluster.waitReplies(1)
}

func (cluster *processClusterHarness) requireCounter(operation protocol.Operation, body string, want uint64) {
	cluster.t.Helper()
	count := len(cluster.clientEvents.replies)
	if err := cluster.client.Submit(operation, []byte(body)); err != nil {
		cluster.t.Fatal(err)
	}
	cluster.waitReplies(count + 1)
	reply := cluster.clientEvents.replies[count]
	if len(reply.Body) != 8 || binary.LittleEndian.Uint64(reply.Body) != want {
		cluster.t.Fatalf("operation %d body %q returned %x, want counter %d", operation, body, reply.Body, want)
	}
}

func (cluster *processClusterHarness) waitCounter(want uint64) {
	cluster.t.Helper()
	cluster.wait(45*time.Second, func() bool {
		for index := range uint8(3) {
			path := processGateCounterPath(cluster.directory, protocol.ReplicaIndex(index))
			encoded, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) {
				return false
			}
			if err != nil {
				cluster.t.Fatalf("read counter %d: %v", index, err)
			}
			var state processCounterState
			if err := json.Unmarshal(encoded, &state); err != nil {
				cluster.t.Fatalf("decode counter %d: %v", index, err)
			}
			if state.Counter != want {
				return false
			}
		}
		return true
	})
}

func (cluster *processClusterHarness) stopAll() {
	cluster.t.Helper()
	if cluster.client != nil {
		if err := cluster.client.Close(); err != nil {
			cluster.t.Fatal(err)
		}
		cluster.client = nil
	}
	for index, process := range cluster.processes {
		if process == nil {
			continue
		}
		cluster.processes[index] = nil
		cluster.ready[index] = false
		if err := process.stop(); err != nil {
			cluster.t.Fatal(err)
		}
	}
}

func (cluster *processClusterHarness) startAll(release protocol.Release, upgrade string) {
	for index := range cluster.processes {
		cluster.processes[index] = startReplicaChildRelease(cluster.t, cluster.directory, protocol.ReplicaIndex(index), cluster.events, release, upgrade)
		cluster.ready[index] = false
	}
	cluster.wait(30*time.Second, func() bool { return cluster.ready[0] && cluster.ready[1] && cluster.ready[2] })
}

type processReplacementClient struct {
	cluster *processClusterHarness
}

func (client processReplacementClient) Register(ctx context.Context) (ReplacementReply, error) {
	return client.request(ctx, true)
}

func (client processReplacementClient) Noop(ctx context.Context) (ReplacementReply, error) {
	return client.request(ctx, false)
}

func (client processReplacementClient) request(ctx context.Context, register bool) (ReplacementReply, error) {
	cluster := client.cluster
	before := len(cluster.clientEvents.replies)
	var err error
	if register {
		err = cluster.client.Register()
	} else {
		err = cluster.client.Submit(protocol.OperationNoop, nil)
	}
	if err != nil {
		return ReplacementReply{}, err
	}
	tick := time.NewTicker(processGateProcessConfig().Tick)
	defer tick.Stop()
	for len(cluster.clientEvents.replies) == before {
		select {
		case <-ctx.Done():
			return ReplacementReply{}, ctx.Err()
		case event := <-cluster.events:
			cluster.route(event)
		case <-tick.C:
			if err := cluster.client.Tick(); err != nil {
				return ReplacementReply{}, err
			}
		}
		if cluster.clientEvents.evicted != protocol.EvictionReserved {
			return ReplacementReply{}, fmt.Errorf("replacement client evicted: %d", cluster.clientEvents.evicted)
		}
	}
	reply := cluster.clientEvents.replies[before]
	return ReplacementReply{View: cluster.client.View(), Operation: reply.Operation, Request: reply.Request}, nil
}

type processReplacementFence struct {
	cluster    *processClusterHarness
	index      protocol.ReplicaIndex
	quarantine string
	verified   bool
}

func (fence *processReplacementFence) VerifyReplacementFence(ctx context.Context, input ReplacementFenceInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	config := processGateReplicaConfig(fence.index)
	if input.Group != config.Group || input.Member != config.Membership.LocalMember || input.ConfigurationChecksum != config.Cluster.Fingerprint() {
		return ErrInvalidConfiguration
	}
	old := fence.cluster.processes[fence.index]
	if old == nil {
		return errors.New("lost process was not owned by replacement fence")
	}
	fence.cluster.processes[fence.index] = nil
	fence.cluster.ready[fence.index] = false
	if err := old.kill(); err != nil {
		return fmt.Errorf("terminate old member: %w", err)
	}
	if old.command.ProcessState == nil || old.command.ProcessState.Success() {
		return errors.New("old member termination was not observed")
	}
	for _, path := range []string{processGateReplicaPath(fence.cluster.directory, fence.index), processGateCounterPath(fence.cluster.directory, fence.index)} {
		if err := os.Rename(path, filepath.Join(fence.quarantine, filepath.Base(path))); err != nil {
			return fmt.Errorf("isolate old storage %s: %w", path, err)
		}
	}
	fence.verified = true
	return nil
}

type processFencedStorage struct {
	Storage
	fence *processReplacementFence
}

func (storage processFencedStorage) Resize(size uint64) error {
	if !storage.fence.verified {
		return ErrReplacementNotFenced
	}
	return storage.Storage.Resize(size)
}

func (storage processFencedStorage) WriteAt(body []byte, offset uint64) error {
	if !storage.fence.verified {
		return ErrReplacementNotFenced
	}
	return storage.Storage.WriteAt(body, offset)
}

func TestProcessFencedLostReplicaReplacementPreservesCounter(t *testing.T) {
	cluster := newProcessClusterHarness(t)
	cluster.requireCounter(protocol.OperationApplicationMin, "before-replacement-1", 1)
	cluster.requireCounter(protocol.OperationApplicationMin, "before-replacement-2", 2)
	cluster.waitCounter(2)
	lost := cluster.lastReplyFrom
	fence := &processReplacementFence{cluster: cluster, index: lost, quarantine: t.TempDir()}
	newPath := filepath.Join(t.TempDir(), "replacement.vsr")
	storage, err := OpenFileStorage(newPath, true, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := storage.Close(); err != nil {
			t.Error(err)
		}
	})
	cluster.freshClient(protocol.ClientID{0x62}, 1)
	config := processGateReplicaConfig(lost)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	err = ReplaceLostReplica(ctx, ReplacementConfig{
		Group: config.Group, Membership: config.Membership, Cluster: config.Cluster,
		CurrentRelease: 1, ConfigurationChecksum: config.Cluster.Fingerprint(),
	}, ReplacementDependencies{Storage: processFencedStorage{Storage: storage, fence: fence}, Client: processReplacementClient{cluster}, Fence: fence})
	if err := errors.Join(err, storage.Close()); err != nil {
		t.Fatal(err)
	}
	if !fence.verified {
		t.Fatal("replacement formatted without fencing")
	}
	if err := os.Rename(newPath, processGateReplicaPath(cluster.directory, lost)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(processGateCounterPath(cluster.directory, lost)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement inherited old counter file: %v", err)
	}
	cluster.restart(lost)
	cluster.waitCounter(2)
	cluster.requireCounter(protocol.OperationApplicationMin+1, "", 2)
	cluster.requireCounter(protocol.OperationApplicationMin, "after-replacement-3", 3)
	cluster.waitCounter(3)
}

func copyProcessData(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported owned data entry %s", path)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return errors.Join(err, input.Close())
		}
		_, copyErr := io.Copy(output, input)
		return errors.Join(copyErr, output.Sync(), output.Close(), input.Close())
	})
}

// The complete cluster is stopped before copying; no online snapshot API is assumed.
func TestProcessQuiescentBackupRestoresExactCounterPoint(t *testing.T) {
	cluster := newProcessClusterHarness(t)
	cluster.requireCounter(protocol.OperationApplicationMin, "backup-point-1", 1)
	cluster.requireCounter(protocol.OperationApplicationMin, "backup-point-2", 2)
	cluster.waitCounter(2)
	cluster.stopAll()
	backup := t.TempDir()
	if err := copyProcessData(cluster.directory, backup); err != nil {
		t.Fatal(err)
	}
	cluster.startAll(1, "")
	cluster.registerFreshClient(protocol.ClientID{0x62}, 1)
	cluster.requireCounter(protocol.OperationApplicationMin, "later-3", 3)
	cluster.requireCounter(protocol.OperationApplicationMin, "later-4", 4)
	cluster.waitCounter(4)
	cluster.stopAll()
	restored := t.TempDir()
	if err := copyProcessData(backup, restored); err != nil {
		t.Fatal(err)
	}
	cluster.directory = restored
	cluster.startAll(1, "")
	cluster.registerFreshClient(protocol.ClientID{0x63}, 1)
	cluster.requireCounter(protocol.OperationApplicationMin+1, "", 2)
	cluster.waitCounter(2)
	cluster.requireCounter(protocol.OperationApplicationMin, "restored-3", 3)
	cluster.waitCounter(3)
}

func (cluster *processClusterHarness) finishHandoff(index protocol.ReplicaIndex, failed bool) {
	cluster.t.Helper()
	process := cluster.processes[index]
	cluster.processes[index] = nil
	cluster.ready[index] = false
	if err := process.send(processWireMessage{Kind: "handoff_exit"}); err != nil {
		cluster.t.Fatal(errors.Join(err, process.kill()))
	}
	close(process.readerCancel)
	waited := make(chan error, 1)
	go func() { waited <- process.command.Wait() }()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	var err error
	waiting := true
	for waiting {
		select {
		case event := <-cluster.events:
			cluster.route(event)
		case err = <-waited:
			waiting = false
		case <-deadline.C:
			killErr := process.command.Process.Kill()
			waitErr := <-waited
			<-process.readerDone
			cluster.t.Fatalf("handoff exit timed out: %v", errors.Join(killErr, waitErr))
		}
	}
	<-process.readerDone
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		cluster.t.Fatalf("handoff process exit: %v", err)
	}
	wantCode := processReleaseExitCode
	if failed {
		wantCode = 1
		if !strings.Contains(process.stderr.String(), errProcessHandoffInjected.Error()) {
			cluster.t.Fatalf("handoff lost injected cause: %s", process.stderr.String())
		}
	}
	if exitErr.ExitCode() != wantCode {
		cluster.t.Fatalf("handoff exit code %d, want %d: %s", exitErr.ExitCode(), wantCode, process.stderr.String())
	}
}

func TestProcessReleaseHandoffRestartMechanics(t *testing.T) {
	for _, mode := range []string{"exit", "fail"} {
		t.Run(mode, func(t *testing.T) {
			cluster := newProcessClusterHarness(t)
			cluster.requireCounter(protocol.OperationApplicationMin, "before-release-1", 1)
			cluster.waitCounter(1)
			cluster.stopAll()
			cluster.startAll(1, mode)
			var completed [3]bool
			cluster.wait(90*time.Second, func() bool {
				for index := range cluster.processes {
					if completed[index] || cluster.handoff[index].Kind == "" {
						continue
					}
					message := cluster.handoff[index]
					if message.Release != 2 || (message.Error != "") != (mode == "fail") {
						t.Fatalf("replica %d handoff: %+v", index, message)
					}
					cluster.finishHandoff(protocol.ReplicaIndex(index), mode == "fail")
					cluster.processes[index] = startReplicaChildRelease(t, cluster.directory, protocol.ReplicaIndex(index), cluster.events, 2, "")
					completed[index] = true
				}
				return completed[0] && completed[1] && completed[2] && cluster.ready[0] && cluster.ready[1] && cluster.ready[2]
			})
			cluster.registerFreshClient(protocol.ClientID{0x64}, 2)
			cluster.requireCounter(protocol.OperationApplicationMin+1, "", 1)
			cluster.requireCounter(protocol.OperationApplicationMin, "after-release-2", 2)
			cluster.waitCounter(2)
		})
	}
}
