package sim

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestLedgerDuplicateCommitHasAnotherEffect(t *testing.T) {
	machine, err := NewLedgerMachine(DefaultConfig(1).Cluster)
	if err != nil {
		t.Fatal(err)
	}
	body := EncodeLedgerRequest(7)
	input := replication.CommitInput{Operation: LedgerIncrement, Body: body[:], Op: 2}
	var previous protocol.Checksum
	for want := uint64(1); want <= 2; want++ {
		var reply [16]byte
		if _, err := machine.Commit(input, 0, reply[:]); err != nil {
			t.Fatal(err)
		}
		token, value, err := DecodeLedgerReply(reply[:])
		if err != nil || token != 7 || value != want {
			t.Fatalf("duplicate commit token=%d value=%d error=%v, want token=7 value=%d", token, value, err, want)
		}
		state := machine.Snapshot()
		if state.Value != want || state.Digest == previous {
			t.Fatalf("commit %d snapshot=%+v previous digest=%s", want, state, previous)
		}
		previous = state.Digest
	}
}

type ledgerOpenObserver struct {
	*LedgerMachine
	opened LedgerSnapshot
}

func (machine *ledgerOpenObserver) StartOpen(input replication.OpenCheckpointInput, completion *replication.SMCompletion) (replication.StartResult[replication.OpenResult], error) {
	result, err := machine.LedgerMachine.StartOpen(input, completion)
	if err == nil {
		machine.opened = machine.Snapshot()
	}
	return result, err
}

func TestLedgerRestoresFrozenStateAcrossMultipleCheckpoints(t *testing.T) {
	config := DefaultConfig(1)
	config.Cluster.JournalSlots = 48
	config.Cluster.ClientsMax = 32
	config.BlockValidator = LedgerValidator{}
	cluster, err := NewCluster(t.Context(), config, func(protocol.ReplicaIndex) replication.StateMachine {
		machine, err := NewLedgerMachine(config.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		return &ledgerOpenObserver{LedgerMachine: machine}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cluster.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
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
	interval, ok := config.Cluster.CheckpointInterval()
	if !ok {
		t.Fatal("invalid checkpoint interval")
	}
	history := newTestHistory(t, 1)
	expected := make(map[protocol.Op]LedgerSnapshot)
	registrations := config.Cluster.CompactionOps + 1
	for registration := uint64(2); registration <= registrations; registration++ {
		registered := &clientEvents{}
		controlClient, err := cluster.AddClient(protocol.ClientID{byte(registration)}, registered)
		if err != nil {
			t.Fatal(err)
		}
		if err := controlClient.Register(); err != nil {
			t.Fatal(err)
		}
		if err := runUntil(cluster, 2_000, func(*Cluster) bool { return registered.replyCount() == 1 }); err != nil {
			t.Fatal(err)
		}
		if registration == interval-1 {
			machine, _ := cluster.Machine(0)
			expected[protocol.Op(registration)] = machine.(*ledgerOpenObserver).Snapshot()
		}
	}
	for checkpoint := uint64(1); checkpoint <= 3; checkpoint++ {
		target := protocol.Op(checkpoint*interval - 1)
		trigger := uint64(target) + config.Cluster.CompactionOps
		for history.Submitted() < trigger-registrations {
			token := history.Submitted() + 1
			if err := history.Submit(token); err != nil {
				t.Fatal(err)
			}
			body := EncodeLedgerRequest(token)
			if err := client.Submit(LedgerIncrement, body[:]); err != nil {
				t.Fatal(err)
			}
			if err := runUntil(cluster, 2_000, func(*Cluster) bool { return events.replyCount() == int(token)+1 }); err != nil {
				t.Fatalf("token %d: %v", token, err)
			}
			if err := history.Complete(token, events.replies[len(events.replies)-1].Body); err != nil {
				t.Fatal(err)
			}
			op := protocol.Op(token + registrations)
			if (uint64(op)+1)%interval == 0 {
				machine, _ := cluster.Machine(0)
				expected[op] = machine.(*ledgerOpenObserver).Snapshot()
			}
		}
		if err := runUntil(cluster, 2_000, func(cluster *Cluster) bool {
			state, _ := cluster.Snapshot(0)
			return state.Checkpoint.PrepareOp() == target
		}); err != nil {
			t.Fatal(err)
		}
		if err := cluster.Crash(t.Context(), 0); err != nil {
			t.Fatal(err)
		}
		if err := cluster.Restart(t.Context(), 0); err != nil {
			t.Fatal(err)
		}
		machine, _ := cluster.Machine(0)
		restored := machine.(*ledgerOpenObserver)
		want := expected[target]
		if restored.opened != want || restored.opened.Value >= history.Completed() {
			t.Fatalf("checkpoint %d restored=%+v want frozen=%+v current value=%d", target, restored.opened, want, history.Completed())
		}
		if err := runUntil(cluster, 2_000, func(*Cluster) bool { return restored.Snapshot().Value == history.Completed() }); err != nil {
			t.Fatal(err)
		}
		if err := history.CheckFinal(restored.Snapshot()); err != nil {
			t.Fatal(err)
		}
	}
	state, _ := cluster.Snapshot(0)
	if err := cluster.Crash(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	offset, ok := config.Cluster.BlockOffset(state.Checkpoint.SnapshotRootAddress)
	if !ok {
		t.Fatal("invalid root address")
	}
	if err := cluster.Storage(0).CorruptDurable(offset+protocol.HeaderSize+24, 1); err != nil {
		t.Fatal(err)
	}
	cluster.Storage(0).Crash()
	if err := cluster.Restart(t.Context(), 0); !errors.Is(err, replication.ErrInvalidBlock) {
		t.Fatalf("corrupted checkpoint digest restart error=%v, want invalid block", err)
	}
}

func TestLedgerValidatorRejectsMalformedSnapshot(t *testing.T) {
	body := encodeLedgerSnapshot(LedgerSnapshot{LastOp: 15, Value: 1, Digest: protocol.Checksum{1}})
	valid := replication.BlockValidationInput{
		Reference:          replication.BlockReference{Address: 1, Checksum: protocol.Checksum{1}},
		NeededAtCheckpoint: 31, Snapshot: 15, Type: replication.BlockValue, Metadata: ledgerMetadata(), Body: body[:],
	}
	if _, err := (LedgerValidator{}).ValidateBlock(valid, nil); err != nil {
		t.Fatalf("original snapshot referenced by later checkpoint: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*replication.BlockValidationInput)
	}{
		{name: "format", mutate: func(input *replication.BlockValidationInput) { input.Body[0] = 2 }},
		{name: "truncated", mutate: func(input *replication.BlockValidationInput) { input.Body = input.Body[:39] }},
		{name: "trailing bytes", mutate: func(input *replication.BlockValidationInput) { input.Body = append(input.Body, 0) }},
		{name: "snapshot identity", mutate: func(input *replication.BlockValidationInput) { input.Snapshot = 31 }},
		{name: "future snapshot", mutate: func(input *replication.BlockValidationInput) { input.NeededAtCheckpoint = 14 }},
		{name: "metadata count", mutate: func(input *replication.BlockValidationInput) { input.Metadata[4] = 2 }},
		{name: "metadata reserved", mutate: func(input *replication.BlockValidationInput) { input.Metadata[95] = 1 }},
		{name: "zero reference", mutate: func(input *replication.BlockValidationInput) { input.Reference.Address = 0 }},
		{name: "wrong type", mutate: func(input *replication.BlockValidationInput) { input.Type = replication.BlockManifest }},
		{name: "zero digest", mutate: func(input *replication.BlockValidationInput) { clear(input.Body[24:]) }},
		{name: "empty state with digest", mutate: func(input *replication.BlockValidationInput) { binary.LittleEndian.PutUint64(input.Body[16:24], 0) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := valid
			input.Body = append([]byte(nil), valid.Body...)
			test.mutate(&input)
			if _, err := (LedgerValidator{}).ValidateBlock(input, nil); !errors.Is(err, ErrLedgerSnapshot) {
				t.Fatalf("malformed snapshot error=%v", err)
			}
		})
	}
}

func TestLedgerValidatorPinsRootSnapshotIdentity(t *testing.T) {
	checkpoint := replication.CheckpointState{
		SnapshotRootAddress: 7, SnapshotRootChecksum: protocol.Checksum{9},
	}
	binary.LittleEndian.PutUint64(checkpoint.Header[224:232], 31)
	requirement, err := (LedgerValidator{}).CheckpointRoot(checkpoint)
	if err != nil || requirement.Reference.Address != 7 || requirement.Reference.Checksum != checkpoint.SnapshotRootChecksum || requirement.Snapshot != 31 || !requirement.SnapshotExact {
		t.Fatalf("root requirement=%+v error=%v", requirement, err)
	}
	if _, found, err := (LedgerValidator{}).ResolveBlock(checkpoint, 8); found || err != nil {
		t.Fatalf("unreferenced block resolved: found=%v error=%v", found, err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*replication.CheckpointState)
	}{
		{name: "missing root", mutate: func(state *replication.CheckpointState) { state.SnapshotRootAddress = 0 }},
		{name: "missing checksum", mutate: func(state *replication.CheckpointState) { state.SnapshotRootChecksum = protocol.Checksum{} }},
		{name: "unexpected manifest", mutate: func(state *replication.CheckpointState) { state.ManifestBlockCount = 1 }},
		{name: "dangling manifest checksum", mutate: func(state *replication.CheckpointState) { state.OldestManifestChecksum = protocol.Checksum{1} }},
		{name: "genesis with data", mutate: func(state *replication.CheckpointState) { clear(state.Header[:]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := checkpoint
			test.mutate(&state)
			if _, err := (LedgerValidator{}).CheckpointRoot(state); !errors.Is(err, ErrLedgerSnapshot) {
				t.Fatalf("invalid checkpoint root error=%v", err)
			}
		})
	}
}
