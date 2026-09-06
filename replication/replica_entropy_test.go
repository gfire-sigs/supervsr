package replication

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestReplicaRecoveryNonceUsesRestartEntropy(t *testing.T) {
	config, storage := threeReplicaFormat(t)
	var nonces [3]protocol.Nonce
	for attempt, seed := range []byte{1, 2, 1} {
		bus := &captureBus{}
		machine := &testStateMachine{capacities: StateMachineCapacities{
			RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
			ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
			PrefetchMax:   uint32(config.Cluster.PipelineMax),
			CheckpointMax: 1,
		}}
		entropy := [8]byte{seed}
		replica, err := Open(context.Background(), config, Dependencies{
			Storage: storage, MessageBus: bus,
			Clock:   fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
			Entropy: bytes.NewReader(entropy[:]), StateMachine: machine,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeReplica(t, replica) })
		replica.sendGetView(1)
		for _, frame := range bus.replicaMessages() {
			header, _, reason := protocol.DecodeFrame(frame, config.Group, uint32(config.Cluster.MessageSizeMax), config.Membership.ActiveCount)
			if reason != protocol.RejectNone {
				t.Fatal(reason)
			}
			if header.Command == protocol.CommandGetView {
				copy(nonces[attempt][:], header.Fields[:16])
			}
		}
		closeReplica(t, replica)
		storage.Crash()
		if nonces[attempt] == (protocol.Nonce{}) {
			t.Fatalf("restart %d emitted no nonzero recovery nonce", attempt)
		}
	}
	if nonces[0] == nonces[1] {
		t.Fatal("different restart entropy reused the same recovery nonce")
	}
	if nonces[0] != nonces[2] {
		t.Fatal("identical entropy and event schedule did not replay the recovery nonce")
	}
}

func TestReplicaRejectsIncompleteRestartEntropy(t *testing.T) {
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes:  uint32(config.Cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(config.Cluster.PipelineMax),
		CheckpointMax: 1,
	}}
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 1, Synchronized: true}},
		Entropy: bytes.NewReader(make([]byte, 7)), StateMachine: machine,
	}, initial, wal, replies, sessions, superblocks)
	if replica != nil {
		closeReplica(t, replica)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("incomplete entropy error=%v, want io.ErrUnexpectedEOF", err)
	}
}
