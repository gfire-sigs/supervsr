package replication

import (
	"context"
	"errors"
)

var (
	ErrReplacementNotFenced   = errors.New("replication: lost replica is not fenced")
	ErrReplacementObservation = errors.New("replication: invalid replacement client observation")
	ErrViewOverflow           = errors.New("replication: view cannot advance safely")
)

// ReplacementConfig fixes the immutable identity and layout of the lost replica.
type ReplacementConfig struct {
	Group                 GroupID
	Membership            Membership
	Cluster               ClusterConfig
	CurrentRelease        Release
	ConfigurationChecksum Checksum
}

// ReplacementFenceInput binds a fencing decision to one immutable member configuration.
type ReplacementFenceInput struct {
	Group                 GroupID
	Member                MemberID
	ConfigurationChecksum Checksum
}

// ReplacementFence verifies that the old process, storage, and credentials cannot reappear.
type ReplacementFence interface {
	VerifyReplacementFence(ctx context.Context, input ReplacementFenceInput) error
}

// ReplacementReply identifies one fully validated ordinary-client reply.
type ReplacementReply struct {
	View      View
	Operation Operation
	Request   RequestNo
}

// ReplacementClient performs exactly-once requests against the surviving cluster.
type ReplacementClient interface {
	Register(ctx context.Context) (ReplacementReply, error)
	Noop(ctx context.Context) (ReplacementReply, error)
}

// ReplacementDependencies supplies the surviving-cluster client and exclusively fenced empty storage.
type ReplacementDependencies struct {
	Storage Storage
	Client  ReplacementClient
	Fence   ReplacementFence
}

// ReplaceLostReplica fences the lost identity, advances the surviving commit boundary, and formats a replacement file.
func ReplaceLostReplica(ctx context.Context, cfg ReplacementConfig, dependencies ReplacementDependencies) error {
	format := FormatConfig{
		Group: cfg.Group, Membership: cfg.Membership, Cluster: cfg.Cluster, CurrentRelease: cfg.CurrentRelease,
	}
	if dependencies.Client == nil || dependencies.Fence == nil {
		return ErrInvalidConfiguration
	}
	if err := validateFormatConfig(format, FormatDependencies{Storage: dependencies.Storage}); err != nil {
		return err
	}
	if cfg.Membership.ActiveCount < 3 || cfg.ConfigurationChecksum.IsZero() || cfg.ConfigurationChecksum != cfg.Cluster.Fingerprint() {
		return ErrInvalidConfiguration
	}
	size, err := dependencies.Storage.Size()
	if err != nil {
		return err
	}
	if size != 0 {
		return ErrStorageNotEmpty
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	fence := ReplacementFenceInput{
		Group: cfg.Group, Member: cfg.Membership.LocalMember, ConfigurationChecksum: cfg.ConfigurationChecksum,
	}
	if err := dependencies.Fence.VerifyReplacementFence(ctx, fence); err != nil {
		return errors.Join(ErrReplacementNotFenced, err)
	}

	reply, err := dependencies.Client.Register(ctx)
	if err != nil {
		return err
	}
	if reply.Operation != OperationRegister || reply.Request != 0 {
		return ErrReplacementObservation
	}
	observedView := reply.View
	for request := RequestNo(1); request < RequestNo(cfg.Cluster.PipelineMax); request++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		reply, err = dependencies.Client.Noop(ctx)
		if err != nil {
			return err
		}
		if reply.Operation != OperationNoop || reply.Request != request {
			return ErrReplacementObservation
		}
		observedView = max(observedView, reply.View)
	}
	if observedView > MaxView-2 {
		return ErrViewOverflow
	}
	return formatAtView(ctx, format, dependencies.Storage, observedView+2, 0)
}
