package replication

import (
	"errors"
	"fmt"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var (
	ErrReplicaInvariant = errors.New("replication: replica invariant violated")
	ErrStatusTransition = errors.New("replication: invalid status transition")
)

type Status uint8

const (
	StatusNormal Status = iota
	StatusViewChange
	StatusRecovering
	StatusRecoveringHead
)

type TransitionCause uint8

const (
	CauseRecoveredBackup TransitionCause = iota + 1
	CauseRecoveredUncertainHead
	CauseRecoveredViewChange
	CauseCanonicalViewInstalled
	CauseLocalTimeout
	CauseHigherView
	CauseNewerViewChange
)

type ReplicaSnapshot struct {
	Status      Status
	View        protocol.View
	DurableView protocol.View
	LogView     protocol.View
	HeadOp      protocol.Op
	CommitMin   protocol.Op
	CommitMax   protocol.Op
	Checkpoint  CheckpointState
	PipelineLen uint32
	Committing  bool
	Primary     protocol.ReplicaIndex
}

func validStatusTransition(from, to Status, cause TransitionCause) bool {
	switch from {
	case StatusRecovering:
		return to == StatusNormal && cause == CauseRecoveredBackup ||
			to == StatusRecoveringHead && cause == CauseRecoveredUncertainHead ||
			to == StatusViewChange && cause == CauseRecoveredViewChange
	case StatusRecoveringHead:
		return to == StatusNormal && cause == CauseCanonicalViewInstalled
	case StatusNormal:
		return to == StatusViewChange && (cause == CauseLocalTimeout || cause == CauseHigherView)
	case StatusViewChange:
		return to == StatusViewChange && cause == CauseNewerViewChange ||
			to == StatusNormal && cause == CauseCanonicalViewInstalled
	default:
		return false
	}
}

func validateReplicaSnapshot(snapshot ReplicaSnapshot, config ClusterConfig, local protocol.ReplicaIndex) error {
	invalid := func(reason string) error {
		return fmt.Errorf("%w: %s", ErrReplicaInvariant, reason)
	}
	if snapshot.DurableView > snapshot.View {
		return invalid("durable view exceeds in-memory view")
	}
	if snapshot.LogView > snapshot.View {
		return invalid("log view exceeds in-memory view")
	}
	if snapshot.Status == StatusNormal && (snapshot.View != snapshot.LogView || snapshot.View != snapshot.DurableView) {
		return invalid("normal view is not durably installed")
	}
	checkpointOp := snapshot.Checkpoint.PrepareOp()
	if checkpointOp > snapshot.CommitMin || snapshot.CommitMin > snapshot.HeadOp {
		return invalid("checkpoint, commit, and head order")
	}
	if snapshot.CommitMin > snapshot.CommitMax {
		return invalid("executed commit exceeds learned commit")
	}
	minimumCommit := protocol.Op(0)
	if snapshot.HeadOp > protocol.Op(config.PipelineMax) {
		minimumCommit = snapshot.HeadOp - protocol.Op(config.PipelineMax)
	}
	if snapshot.CommitMax < minimumCommit {
		return invalid("learned commit falls outside retained pipeline")
	}
	if snapshot.Status == StatusNormal && snapshot.Primary == local {
		if snapshot.CommitMin != snapshot.CommitMax {
			return invalid("normal primary commit bounds differ")
		}
		pending := snapshot.PipelineLen
		if snapshot.Committing {
			if pending == 0 {
				return invalid("commit maintenance has no pipeline entry")
			}
			pending--
		}
		if snapshot.HeadOp-snapshot.CommitMin != protocol.Op(pending) {
			return invalid("normal primary pipeline length differs from head")
		}
	}
	return nil
}
