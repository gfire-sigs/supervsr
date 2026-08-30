package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
	"github.com/rs/zerolog"
)

func TestSnapshotJSONRoundTripDeterministic(t *testing.T) {
	directory := t.TempDir()
	machine := newSnapshotTestMachine(directory)
	machine.values["zeta"] = []byte("last")
	machine.values["alpha"] = []byte{0, 1, 2, 0xff}
	const op protocol.Op = 42

	persistCurrentSnapshot(t, machine, op)
	path := machine.snapshotPath(op)
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(first) {
		t.Fatalf("snapshot is not JSON: %q", first)
	}
	persistCurrentSnapshot(t, machine, op)
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("equivalent state produced different JSON")
	}

	restored := newSnapshotTestMachine(directory)
	if err := restored.readSnapshot(op); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.values, machine.values) {
		t.Fatalf("restored values = %#v, want %#v", restored.values, machine.values)
	}
}

func TestSnapshotRejectsCorruptionWithoutChangingState(t *testing.T) {
	directory := t.TempDir()
	writer := newSnapshotTestMachine(directory)
	writer.values["alpha"] = []byte("one")
	writer.values["beta"] = []byte("two")
	const op protocol.Op = 7
	persistCurrentSnapshot(t, writer, op)
	path := writer.snapshotPath(op)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(t *testing.T, source []byte) []byte
	}{
		{
			name: "checksum mismatch",
			mutate: func(t *testing.T, source []byte) []byte {
				t.Helper()
				var document snapshotDocument
				if err := json.Unmarshal(source, &document); err != nil {
					t.Fatal(err)
				}
				document.Payload.Entries[0].Value[0] ^= 1
				encoded, err := json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				return encoded
			},
		},
		{
			name: "noncanonical JSON",
			mutate: func(t *testing.T, source []byte) []byte {
				t.Helper()
				return append(append([]byte(nil), source...), '\n')
			},
		},
		{
			name: "unsorted entries",
			mutate: func(t *testing.T, source []byte) []byte {
				t.Helper()
				var document snapshotDocument
				if err := json.Unmarshal(source, &document); err != nil {
					t.Fatal(err)
				}
				document.Payload.Entries[0], document.Payload.Entries[1] = document.Payload.Entries[1], document.Payload.Entries[0]
				checksumInput, err := json.Marshal(document.Payload)
				if err != nil {
					t.Fatal(err)
				}
				checksum := protocol.ChecksumBytes(checksumInput)
				document.Checksum = hex.EncodeToString(checksum[:])
				encoded, err := json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				return encoded
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(path, test.mutate(t, original), 0o600); err != nil {
				t.Fatal(err)
			}
			reader := newSnapshotTestMachine(directory)
			reader.values["stable"] = []byte("unchanged")
			if err := reader.readSnapshot(op); !errors.Is(err, errInvalidSnapshot) {
				t.Fatalf("read error = %v, want %v", err, errInvalidSnapshot)
			}
			if !reflect.DeepEqual(reader.values, map[string][]byte{"stable": []byte("unchanged")}) {
				t.Fatalf("state changed after rejected snapshot: %#v", reader.values)
			}
		})
	}
}

func TestCheckpointFromRecoveryReplayPreservesTargetState(t *testing.T) {
	directory := t.TempDir()
	crashed := newSnapshotTestMachine(directory)
	applyTargetState(t, crashed)
	if result, err := crashed.StartCompact(replication.CompactInput{Op: 63}, nil); err != nil {
		t.Fatal(err)
	} else if !result.IsReady() {
		t.Fatal("target compaction did not complete synchronously")
	}
	if _, err := os.Stat(crashed.snapshotPath(63)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot before trigger: %v", err)
	}

	recovered := newSnapshotTestMachine(directory)
	applyTargetState(t, recovered)
	target := cloneValues(recovered.values)
	applyPostTarget(t, recovered)
	beforeRestart := cloneValues(recovered.values)
	result, err := recovered.StartCheckpoint(replication.CheckpointInput{Op: 63}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsReady() {
		t.Fatal("checkpoint did not complete synchronously")
	}

	restarted := newSnapshotTestMachine(directory)
	openResult, err := restarted.StartOpen(checkpointOpenInput(t, restarted.group, 63), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !openResult.IsReady() {
		t.Fatal("checkpoint open did not complete synchronously")
	}
	if !reflect.DeepEqual(restarted.values, target) {
		t.Fatalf("checkpoint state = %#v, want target %#v", restarted.values, target)
	}
	applyPostTarget(t, restarted)
	if !reflect.DeepEqual(restarted.values, beforeRestart) {
		t.Fatalf("replayed state = %#v, want pre-restart %#v", restarted.values, beforeRestart)
	}
}

func TestStartCompactCapturesNonApplicationBoundary(t *testing.T) {
	machine := newSnapshotTestMachine(t.TempDir())
	commitPut(t, machine, 1, "alpha", "old")
	result, err := machine.StartCompact(replication.CompactInput{Op: 31}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsReady() {
		t.Fatal("compaction did not complete synchronously")
	}
	if _, found := machine.frozenSnapshot(31); !found {
		t.Fatal("non-application compaction boundary was not captured")
	}
}

func TestStateSyncOpenClearsObsoleteSnapshotCleanup(t *testing.T) {
	directory := t.TempDir()
	machine := newSnapshotTestMachine(directory)
	machine.values["alpha"] = []byte("checkpoint")
	persistCurrentSnapshot(t, machine, 127)
	machine.pendingSnapshot = 63

	if result, err := machine.StartOpen(checkpointOpenInput(t, machine.group, 127), nil); err != nil {
		t.Fatal(err)
	} else if !result.IsReady() {
		t.Fatal("checkpoint open did not complete synchronously")
	}
	if result, err := machine.StartCompact(replication.CompactInput{Op: 159}, nil); err != nil {
		t.Fatal(err)
	} else if !result.IsReady() {
		t.Fatal("compaction did not complete synchronously")
	}
	if _, err := os.Stat(machine.snapshotPath(127)); err != nil {
		t.Fatalf("opened checkpoint removed by stale cleanup: %v", err)
	}

	machine.pendingSnapshot = 127
	if result, err := machine.StartReset(nil); err != nil {
		t.Fatal(err)
	} else if !result.IsReady() {
		t.Fatal("reset did not complete synchronously")
	}
	if machine.pendingSnapshot != 0 {
		t.Fatalf("pending snapshot after reset = %d", machine.pendingSnapshot)
	}
}

func applyTargetState(t *testing.T, machine *kvMachine) {
	t.Helper()
	commitPut(t, machine, 1, "alpha", "old")
	commitPut(t, machine, 2, "beta", "keep")
}

func applyPostTarget(t *testing.T, machine *kvMachine) {
	t.Helper()
	commitPut(t, machine, 64, "alpha", "new")
	commitDelete(t, machine, 65, "beta")
	commitPut(t, machine, 66, "gamma", "final")
	commitPulse(t, machine, 95)
}

func commitPut(t *testing.T, machine *kvMachine, op protocol.Op, key, value string) {
	t.Helper()
	body, err := encodePut(key, value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Commit(replication.CommitInput{Operation: operationPut, Body: body, Op: op}, 0, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
}

func commitDelete(t *testing.T, machine *kvMachine, op protocol.Op, key string) {
	t.Helper()
	body, err := encodeKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Commit(replication.CommitInput{Operation: operationDelete, Body: body, Op: op}, 0, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
}

func commitPulse(t *testing.T, machine *kvMachine, op protocol.Op) {
	t.Helper()
	if _, err := machine.Commit(replication.CommitInput{Operation: protocol.OperationPulse, Op: op}, 0, nil); err != nil {
		t.Fatal(err)
	}
}

func checkpointOpenInput(t *testing.T, group protocol.GroupID, op protocol.Op) replication.OpenCheckpointInput {
	t.Helper()
	header := protocol.Header{
		Group: group, Size: protocol.HeaderSize, Protocol: protocol.ProtocolVersion,
		Command: protocol.CommandPrepare, Author: 0,
	}
	binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(op))
	var input replication.OpenCheckpointInput
	if err := protocol.EncodeHeader(input.State.Header[:], &header); err != nil {
		t.Fatal(err)
	}
	return input
}

func cloneValues(values map[string][]byte) map[string][]byte {
	cloned := make(map[string][]byte, len(values))
	for key, value := range values {
		cloned[key] = append([]byte(nil), value...)
	}
	return cloned
}

func persistCurrentSnapshot(t *testing.T, machine *kvMachine, op protocol.Op) {
	t.Helper()
	encoded, err := machine.encodeSnapshot(op)
	if err != nil {
		t.Fatal(err)
	}
	if err := machine.writeSnapshot(op, encoded); err != nil {
		t.Fatal(err)
	}
}

func newSnapshotTestMachine(directory string) *kvMachine {
	return newKVMachine(directory, protocol.GroupID{1}, 3, 128<<10, 32, zerolog.Nop())
}
