package replication

import (
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var ErrInvalidMembership = errors.New("replication: invalid immutable membership")

type Membership struct {
	Members      [MembersMax]protocol.MemberID
	ActiveCount  uint8
	StandbyCount uint8
	LocalMember  protocol.MemberID
}

func (membership Membership) Validate() error {
	total := int(membership.ActiveCount) + int(membership.StandbyCount)
	if membership.ActiveCount == 0 || membership.ActiveCount > ActiveMax || membership.StandbyCount > StandbyMax || total > MembersMax {
		return ErrInvalidMembership
	}

	localMatches := 0
	for index := range membership.Members {
		member := membership.Members[index]
		if index < total {
			if member.IsZero() {
				return ErrInvalidMembership
			}
			if member == membership.LocalMember {
				localMatches++
			}
			for previous := range index {
				if member == membership.Members[previous] {
					return ErrInvalidMembership
				}
			}
			continue
		}
		if !member.IsZero() {
			return ErrInvalidMembership
		}
	}
	if membership.LocalMember.IsZero() || localMatches != 1 {
		return ErrInvalidMembership
	}
	return nil
}

func (membership Membership) LocalIndex() (protocol.ReplicaIndex, bool) {
	for index, member := range membership.Members {
		if member == membership.LocalMember {
			return protocol.ReplicaIndex(index), true
		}
	}
	return 0, false
}

func (membership Membership) Primary(view protocol.View) protocol.ReplicaIndex {
	if membership.ActiveCount == 0 {
		panic(ErrInvalidMembership)
	}
	return protocol.ReplicaIndex(uint32(view) % uint32(membership.ActiveCount))
}

type Config struct {
	Group            protocol.GroupID
	Membership       Membership
	Cluster          ClusterConfig
	Process          ProcessConfig
	CurrentRelease   protocol.Release
	ClientReleaseMin protocol.Release
}

func (cfg Config) Validate() error {
	if cfg.Group.IsZero() {
		return &ConfigError{Field: "Group", Reason: "must be nonzero"}
	}
	if err := cfg.Membership.Validate(); err != nil {
		return errors.Join(ErrInvalidConfiguration, err)
	}
	if err := cfg.Cluster.Validate(cfg.Membership.ActiveCount, cfg.Membership.StandbyCount); err != nil {
		return err
	}
	if err := cfg.Process.Validate(cfg.Cluster); err != nil {
		return err
	}
	if cfg.ClientReleaseMin == 0 || cfg.CurrentRelease < cfg.ClientReleaseMin {
		return &ConfigError{Field: "release", Reason: "require 0 < ClientReleaseMin <= CurrentRelease"}
	}
	return nil
}
