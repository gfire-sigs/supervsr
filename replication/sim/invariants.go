package sim

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var ErrInvariant = errors.New("simulation: global invariant violated")

type invariantState struct {
	mu  sync.Mutex
	err error
}

type observedClientEvents struct {
	cluster   *Cluster
	client    protocol.ClientID
	target    replication.ClientEvents
	lastReply protocol.RequestNo
	replied   bool
	frame     []byte
}

func (events *observedClientEvents) Reply(reply replication.ClientReply) {
	if events.replied && reply.Request <= events.lastReply {
		events.cluster.failInvariant("client %x reply %d followed reply %d", events.client, reply.Request, events.lastReply)
	} else if err := events.cluster.checkDurableReply(events.frame); err != nil {
		events.cluster.failInvariant("client %x reply %d: %v", events.client, reply.Request, err)
	}
	events.lastReply = reply.Request
	events.replied = true
	events.target.Reply(reply)
}

func (events *observedClientEvents) Evicted(reason protocol.EvictionReason) {
	events.target.Evicted(reason)
}

func (cluster *Cluster) CheckInvariants() error {
	if err := cluster.invariantFailure(); err != nil {
		return err
	}
	active := cluster.config.ActiveCount
	if active == 0 ||
		uint16(cluster.quorums.Replication)+uint16(cluster.quorums.ViewChange) <= uint16(active) ||
		uint16(cluster.quorums.Replication)+uint16(cluster.quorums.Negative) <= uint16(active) {
		return fmt.Errorf("%w: configured quorums do not intersect", ErrInvariant)
	}
	if uint32(len(cluster.clients)) > cluster.config.ClientProcessesMax {
		return fmt.Errorf("%w: client count %d exceeds %d", ErrInvariant, len(cluster.clients), cluster.config.ClientProcessesMax)
	}
	if cluster.network.Pending() > cluster.network.maximum {
		return fmt.Errorf("%w: packet count exceeds capacity", ErrInvariant)
	}
	for index, storage := range cluster.stores {
		if storage.PendingWrites() > DelayedWritesMax {
			return fmt.Errorf("%w: replica %d delayed writes exceed capacity", ErrInvariant, index)
		}
	}
	for index := range cluster.nodes {
		node := &cluster.nodes[index]
		if node.replica == nil {
			continue
		}
		snapshot := node.replica.Snapshot()
		if err := cluster.checkReplicaSnapshot(protocol.ReplicaIndex(index), snapshot); err != nil {
			return err
		}
		if node.controller != nil {
			used := node.controller.Used()
			if used < 0 || used > node.controller.Capacity() {
				return fmt.Errorf("%w: replica %d IO capacity exceeded", ErrInvariant, index)
			}
		}
		switch machine := node.machine.(type) {
		case *LedgerMachine:
			state := machine.Snapshot()
			if state.LastOp > snapshot.HeadOp || state.Value > uint64(snapshot.HeadOp) {
				return fmt.Errorf("%w: replica %d ledger exceeds log head", ErrInvariant, index)
			}
		case *Machine:
			if err := checkMachineOrder(protocol.ReplicaIndex(index), machine, snapshot.HeadOp); err != nil {
				return err
			}
		case interface{ Commits() []Commit }:
			if err := checkCommitOrder(protocol.ReplicaIndex(index), machine.Commits(), snapshot.HeadOp); err != nil {
				return err
			}
		}
	}
	for left := 0; left < len(cluster.nodes); left++ {
		if cluster.nodes[left].replica == nil {
			continue
		}
		for right := left + 1; right < len(cluster.nodes); right++ {
			if cluster.nodes[right].replica == nil {
				continue
			}
			if err := cluster.checkCommittedPrefix(left, right); err != nil {
				return err
			}
			if err := checkMachineAgreement(left, right, cluster.nodes[left].machine, cluster.nodes[right].machine); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cluster *Cluster) checkReplicaSnapshot(index protocol.ReplicaIndex, snapshot replication.ReplicaSnapshot) error {
	invalid := func(reason string) error {
		return fmt.Errorf("%w: replica %d: %s", ErrInvariant, index, reason)
	}
	if snapshot.Status > replication.StatusRecoveringHead {
		return invalid("invalid status")
	}
	if snapshot.DurableView > snapshot.View || snapshot.LogView > snapshot.View {
		return invalid("durable state exceeds view")
	}
	if snapshot.Status == replication.StatusNormal && (snapshot.View != snapshot.DurableView || snapshot.View != snapshot.LogView) {
		return invalid("normal view is not durable")
	}
	checkpoint := snapshot.Checkpoint.PrepareOp()
	if checkpoint > snapshot.CommitMin || snapshot.CommitMin > snapshot.HeadOp || snapshot.CommitMin > snapshot.CommitMax {
		return invalid("checkpoint, commit, and head order")
	}
	minimumCommit := protocol.Op(0)
	if snapshot.HeadOp > protocol.Op(cluster.config.Cluster.PipelineMax) {
		minimumCommit = snapshot.HeadOp - protocol.Op(cluster.config.Cluster.PipelineMax)
	}
	if snapshot.CommitMax < minimumCommit {
		return invalid("learned commit falls outside retained pipeline")
	}
	if uint64(snapshot.PipelineLen) > cluster.config.Cluster.PipelineMax {
		return invalid("pipeline exceeds capacity")
	}
	expectedPrimary := protocol.ReplicaIndex(uint64(snapshot.View) % uint64(cluster.config.ActiveCount))
	if snapshot.Primary != expectedPrimary {
		return invalid("primary does not match view")
	}
	return nil
}

func (cluster *Cluster) checkCommittedPrefix(left, right int) error {
	leftReplica := cluster.nodes[left].replica
	rightReplica := cluster.nodes[right].replica
	leftSnapshot := leftReplica.Snapshot()
	rightSnapshot := rightReplica.Snapshot()
	common := min(leftSnapshot.CommitMin, rightSnapshot.CommitMin)
	floor := max(leftSnapshot.Checkpoint.PrepareOp(), rightSnapshot.Checkpoint.PrepareOp())
	if common < floor {
		return nil
	}
	leftChecksum, leftOK := leftReplica.DurableChecksum(common)
	rightChecksum, rightOK := rightReplica.DurableChecksum(common)
	if !leftOK || !rightOK {
		return fmt.Errorf("%w: replicas %d and %d lack durable evidence at op %d", ErrInvariant, left, right, common)
	}
	if leftChecksum != rightChecksum {
		return fmt.Errorf("%w: replicas %d and %d conflict at committed op %d", ErrInvariant, left, right, common)
	}
	return nil
}

func checkMachineOrder(index protocol.ReplicaIndex, machine *Machine, commitMax protocol.Op) error {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	return checkCommitOrder(index, machine.commits, commitMax)
}

func checkCommitOrder(index protocol.ReplicaIndex, commits []Commit, commitMax protocol.Op) error {
	for position, commit := range commits {
		if commit.Op > commitMax {
			return fmt.Errorf("%w: replica %d state machine applies uncommitted op %d", ErrInvariant, index, commit.Op)
		}
		if position != 0 && commits[position-1].Op >= commit.Op {
			return fmt.Errorf("%w: replica %d state machine repeats or reorders op %d", ErrInvariant, index, commit.Op)
		}
	}
	return nil
}

func checkMachineAgreement(leftIndex, rightIndex int, leftMachine, rightMachine replication.StateMachine) error {
	leftLedger, leftLedgerOK := leftMachine.(*LedgerMachine)
	rightLedger, rightLedgerOK := rightMachine.(*LedgerMachine)
	if leftLedgerOK && rightLedgerOK {
		left, right := leftLedger.Snapshot(), rightLedger.Snapshot()
		if left.LastOp == right.LastOp && left.Value != right.Value {
			return fmt.Errorf("%w: replicas %d and %d disagree at ledger op %d: values %d and %d", ErrInvariant, leftIndex, rightIndex, left.LastOp, left.Value, right.Value)
		}
		if left.Value == right.Value && left.Digest != right.Digest {
			return fmt.Errorf("%w: replicas %d and %d disagree on ledger prefix %d", ErrInvariant, leftIndex, rightIndex, left.Value)
		}
		return nil
	}
	leftDefault, leftDefaultOK := leftMachine.(*Machine)
	rightDefault, rightDefaultOK := rightMachine.(*Machine)
	if leftDefaultOK && rightDefaultOK {
		leftDefault.mu.Lock()
		defer leftDefault.mu.Unlock()
		rightDefault.mu.Lock()
		defer rightDefault.mu.Unlock()
		return compareCommits(leftIndex, rightIndex, leftDefault.commits, rightDefault.commits)
	}
	leftObserver, leftOK := leftMachine.(interface{ Commits() []Commit })
	rightObserver, rightOK := rightMachine.(interface{ Commits() []Commit })
	if !leftOK || !rightOK {
		return nil
	}
	return compareCommits(leftIndex, rightIndex, leftObserver.Commits(), rightObserver.Commits())
}

func compareCommits(left, right int, leftCommits, rightCommits []Commit) error {
	leftPosition, rightPosition := 0, 0
	for leftPosition < len(leftCommits) && rightPosition < len(rightCommits) {
		leftCommit := leftCommits[leftPosition]
		rightCommit := rightCommits[rightPosition]
		switch {
		case leftCommit.Op < rightCommit.Op:
			leftPosition++
		case rightCommit.Op < leftCommit.Op:
			rightPosition++
		default:
			if leftCommit.Operation != rightCommit.Operation || leftCommit.Timestamp != rightCommit.Timestamp || leftCommit.Release != rightCommit.Release || !bytes.Equal(leftCommit.Body, rightCommit.Body) {
				return fmt.Errorf("%w: replicas %d and %d apply different values at op %d", ErrInvariant, left, right, leftCommit.Op)
			}
			leftPosition++
			rightPosition++
		}
	}
	return nil
}

func (cluster *Cluster) checkDurableReply(frame []byte) error {
	reply, _, reason := protocol.DecodeFrame(frame, cluster.config.Group, uint32(cluster.config.Cluster.MessageSizeMax), uint8(len(cluster.nodes)))
	if reason != protocol.RejectNone || reply.Command != protocol.CommandReply {
		return fmt.Errorf("%w: accepted reply has no valid wire identity", ErrInvariant)
	}
	op := protocol.Op(binary.LittleEndian.Uint64(reply.Fields[80:88]))
	var expected protocol.Checksum
	for index := range cluster.nodes {
		node := &cluster.nodes[index]
		if node.replica == nil || node.replica.Snapshot().CommitMin < op {
			continue
		}
		if checksum, durable := node.replica.DurableChecksum(op); durable {
			if !expected.IsZero() && expected != checksum {
				return fmt.Errorf("%w: committed reply conflicts at op %d", ErrInvariant, op)
			}
			expected = checksum
		}
	}
	if expected.IsZero() {
		for index := range cluster.config.ActiveCount {
			if checksum, found := cluster.durablePrepareForReply(int(index), reply); found {
				expected = checksum
				break
			}
		}
	}
	durable := uint8(0)
	for index := range cluster.config.ActiveCount {
		node := &cluster.nodes[index]
		checkpoint := node.lastCheckpoint
		if node.replica != nil {
			checkpoint = node.replica.Snapshot().Checkpoint
		}
		if checkpoint.PrepareOp() >= op {
			durable++
			continue
		}
		var checksum protocol.Checksum
		var found bool
		if node.replica != nil {
			checksum, found = node.replica.DurableChecksum(op)
		} else {
			checksum, found = cluster.durablePrepareForReply(int(index), reply)
		}
		if found && !expected.IsZero() && checksum == expected {
			durable++
		}
	}
	if durable < cluster.quorums.Replication {
		return fmt.Errorf("%w: reply op %d has %d durable replicas, need %d", ErrInvariant, op, durable, cluster.quorums.Replication)
	}
	return nil
}

func (cluster *Cluster) durablePrepareForReply(index int, reply protocol.Header) (protocol.Checksum, bool) {
	op := binary.LittleEndian.Uint64(reply.Fields[80:88])
	offset := cluster.layout.PrepareBase + (op%cluster.config.Cluster.JournalSlots)*cluster.layout.PrepareStride
	storage := cluster.stores[index]
	storage.mu.Lock()
	if offset > uint64(len(storage.durable)) || uint64(len(cluster.proofBuffer)) > uint64(len(storage.durable))-offset {
		storage.mu.Unlock()
		return protocol.Checksum{}, false
	}
	copy(cluster.proofBuffer, storage.durable[offset:offset+uint64(len(cluster.proofBuffer))])
	storage.mu.Unlock()
	size := binary.LittleEndian.Uint32(cluster.proofBuffer[96:100])
	if size < protocol.HeaderSize || uint64(size) > uint64(len(cluster.proofBuffer)) {
		return protocol.Checksum{}, false
	}
	header, _, reason := protocol.DecodeFrame(cluster.proofBuffer[:size], cluster.config.Group, uint32(cluster.config.Cluster.MessageSizeMax), uint8(len(cluster.nodes)))
	if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare {
		return protocol.Checksum{}, false
	}
	identity := bytes.Equal(header.Fields[32:48], reply.Fields[:16]) && bytes.Equal(header.Fields[80:96], reply.Fields[64:80])
	position := bytes.Equal(header.Fields[96:104], reply.Fields[80:88]) && bytes.Equal(header.Fields[112:120], reply.Fields[96:104])
	request := bytes.Equal(header.Fields[120:125], reply.Fields[104:109]) && header.Release == reply.Release
	return header.HeaderChecksum, identity && position && request
}

func (cluster *Cluster) failInvariant(format string, arguments ...any) {
	cluster.invariants.mu.Lock()
	defer cluster.invariants.mu.Unlock()
	if cluster.invariants.err == nil {
		cluster.invariants.err = fmt.Errorf("%w: %s", ErrInvariant, fmt.Sprintf(format, arguments...))
	}
}

func (cluster *Cluster) invariantFailure() error {
	cluster.invariants.mu.Lock()
	defer cluster.invariants.mu.Unlock()
	return cluster.invariants.err
}
