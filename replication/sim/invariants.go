package sim

import (
	"bytes"
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
}

func (events *observedClientEvents) Reply(reply replication.ClientReply) {
	if events.replied && reply.Request <= events.lastReply {
		events.cluster.failInvariant("client %x reply %d followed reply %d", events.client, reply.Request, events.lastReply)
	} else if err := events.cluster.checkDurableReply(reply); err != nil {
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
	quorum := active/2 + 1
	if active == 0 || 2*quorum <= active {
		return fmt.Errorf("%w: active quorum does not intersect", ErrInvariant)
	}
	if uint64(len(cluster.clients)) > cluster.config.Cluster.ClientsMax {
		return fmt.Errorf("%w: client count %d exceeds %d", ErrInvariant, len(cluster.clients), cluster.config.Cluster.ClientsMax)
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
		switch machine := node.machine.(type) {
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

func (cluster *Cluster) checkDurableReply(reply replication.ClientReply) error {
	if reply.Operation < protocol.OperationApplicationMin {
		return nil
	}
	var candidateOp protocol.Op
	found := false
	for index := range cluster.nodes {
		op, ok := findReplyCommit(cluster.nodes[index].machine, reply)
		if ok && (!found || op > candidateOp) {
			candidateOp = op
			found = true
		}
	}
	if !found {
		return fmt.Errorf("%w: reply has no committed operation", ErrInvariant)
	}
	durable := uint8(0)
	var expected protocol.Checksum
	for index := range cluster.config.ActiveCount {
		node := &cluster.nodes[index]
		if node.replica == nil {
			continue
		}
		snapshot := node.replica.Snapshot()
		if snapshot.Checkpoint.PrepareOp() >= candidateOp {
			durable++
			continue
		}
		checksum, ok := node.replica.DurableChecksum(candidateOp)
		if !ok {
			continue
		}
		if expected.IsZero() {
			expected = checksum
		} else if checksum != expected {
			return fmt.Errorf("%w: durable reply quorum conflicts at op %d", ErrInvariant, candidateOp)
		}
		durable++
	}
	if durable < cluster.config.ActiveCount/2+1 {
		return fmt.Errorf("%w: reply at op %d has %d durable replicas", ErrInvariant, candidateOp, durable)
	}
	return nil
}

func findReplyCommit(stateMachine replication.StateMachine, reply replication.ClientReply) (protocol.Op, bool) {
	if machine, ok := stateMachine.(*Machine); ok {
		machine.mu.Lock()
		defer machine.mu.Unlock()
		return findCommit(machine.commits, reply)
	}
	observer, ok := stateMachine.(interface{ Commits() []Commit })
	if !ok {
		return 0, false
	}
	return findCommit(observer.Commits(), reply)
}

func findCommit(commits []Commit, reply replication.ClientReply) (protocol.Op, bool) {
	var op protocol.Op
	found := false
	for _, commit := range commits {
		sameReply := commit.Operation == reply.Operation && bytes.Equal(commit.Body, reply.Body)
		newer := !found || commit.Op > op
		if sameReply && newer {
			op = commit.Op
			found = true
		}
	}
	return op, found
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
