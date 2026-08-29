package replication

import (
	"errors"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestDefaultConfigurationFingerprint(t *testing.T) {
	cfg := DefaultClusterConfig()
	if actual := cfg.Fingerprint().String(); actual != "fdaf799fc9fedca12ad9450103550779" {
		t.Fatalf("fingerprint = %s, want fdaf799fc9fedca12ad9450103550779", actual)
	}
	if err := cfg.Validate(6, 6); err != nil {
		t.Fatalf("default cluster configuration: %v", err)
	}
	if err := DefaultProcessConfig().Validate(cfg); err != nil {
		t.Fatalf("default process configuration: %v", err)
	}
}

func TestQuorumTable(t *testing.T) {
	want := [...]Quorums{
		{},
		{Replication: 1, ViewChange: 1, Negative: 1, Majority: 1, Upgrade: 1},
		{Replication: 2, ViewChange: 2, Negative: 1, Majority: 2, Upgrade: 2},
		{Replication: 2, ViewChange: 2, Negative: 2, Majority: 2, Upgrade: 3},
		{Replication: 2, ViewChange: 3, Negative: 3, Majority: 3, Upgrade: 4},
		{Replication: 3, ViewChange: 3, Negative: 3, Majority: 3, Upgrade: 5},
		{Replication: 3, ViewChange: 4, Negative: 4, Majority: 4, Upgrade: 6},
	}
	for active := uint8(1); active <= ActiveMax; active++ {
		actual, ok := QuorumsFor(active, 3)
		if !ok {
			t.Fatalf("active %d: quorum derivation failed", active)
		}
		if actual != want[active] {
			t.Fatalf("active %d: quorums = %+v, want %+v", active, actual, want[active])
		}
		if int(actual.Replication)+int(actual.ViewChange) <= int(active) {
			t.Fatalf("active %d: replication/view quorums do not intersect", active)
		}
		if int(actual.Replication)+int(actual.Negative) <= int(active) {
			t.Fatalf("active %d: replication/negative quorums do not intersect", active)
		}
	}
	for _, input := range [][2]uint8{{0, 3}, {ActiveMax + 1, 3}, {1, 0}} {
		if _, ok := QuorumsFor(input[0], input[1]); ok {
			t.Fatalf("QuorumsFor(%d,%d) succeeded", input[0], input[1])
		}
	}
}

func TestClusterConfigurationRejectsInvalidBounds(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ClusterConfig)
		active  uint8
		standby uint8
	}{
		{name: "zero active", active: 0},
		{name: "active maximum", active: ActiveMax + 1},
		{name: "standby maximum", active: 1, standby: StandbyMax + 1},
		{name: "cache line", mutate: func(cfg *ClusterConfig) { cfg.CacheLineSize = 128 }},
		{name: "zero clients", mutate: func(cfg *ClusterConfig) { cfg.ClientsMax = 0 }},
		{name: "zero pipeline", mutate: func(cfg *ClusterConfig) { cfg.PipelineMax = 0 }},
		{name: "pipeline maximum", mutate: func(cfg *ClusterConfig) { cfg.PipelineMax = 16; cfg.ViewChangeHeadersSuffixMax = 17 }},
		{name: "view suffix", mutate: func(cfg *ClusterConfig) { cfg.ViewChangeHeadersSuffixMax++ }},
		{name: "zero quorum", mutate: func(cfg *ClusterConfig) { cfg.ReplicationQuorumMax = 0 }},
		{name: "small journal", mutate: func(cfg *ClusterConfig) { cfg.JournalSlots = 16 }},
		{name: "unaligned message", mutate: func(cfg *ClusterConfig) { cfg.MessageSizeMax-- }},
		{name: "small message", mutate: func(cfg *ClusterConfig) {
			cfg.MessageSizeMax = SectorSize
			cfg.ApplicationBatchSizeMax = 1
			cfg.ApplicationReplySizeMax = 0
		}},
		{name: "zero batch", mutate: func(cfg *ClusterConfig) { cfg.ApplicationBatchSizeMax = 0 }},
		{name: "large reply", mutate: func(cfg *ClusterConfig) { cfg.ApplicationReplySizeMax = cfg.MessageSizeMax }},
		{name: "superblock copies", mutate: func(cfg *ClusterConfig) { cfg.SuperblockCopies = 5 }},
		{name: "unaligned block", mutate: func(cfg *ClusterConfig) { cfg.BlockSize++ }},
		{name: "large block", mutate: func(cfg *ClusterConfig) { cfg.BlockSize = cfg.MessageSizeMax * 2 }},
		{name: "zero levels", mutate: func(cfg *ClusterConfig) { cfg.StorageLevels = 0 }},
		{name: "levels maximum", mutate: func(cfg *ClusterConfig) { cfg.StorageLevels = 65 }},
		{name: "growth factor", mutate: func(cfg *ClusterConfig) { cfg.StorageGrowthFactor = 1 }},
		{name: "compaction power", mutate: func(cfg *ClusterConfig) { cfg.CompactionOps = 3 }},
		{name: "zero snapshots", mutate: func(cfg *ClusterConfig) { cfg.StorageSnapshotsMax = 0 }},
		{name: "zero compact extra", mutate: func(cfg *ClusterConfig) { cfg.ManifestCompactExtraBlocks = 0 }},
		{name: "zero coalescing", mutate: func(cfg *ClusterConfig) { cfg.TableCoalescingThresholdPercent = 0 }},
		{name: "coalescing maximum", mutate: func(cfg *ClusterConfig) { cfg.TableCoalescingThresholdPercent = 101 }},
		{name: "zero release history", mutate: func(cfg *ClusterConfig) { cfg.ReleaseHistoryMax = 0 }},
		{name: "zero scans", mutate: func(cfg *ClusterConfig) { cfg.StorageScansMax = 0 }},
		{name: "checkpoint interval", mutate: func(cfg *ClusterConfig) { cfg.JournalSlots = 64 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultClusterConfig()
			active := test.active
			if active == 0 && test.name != "zero active" {
				active = 3
			}
			if test.mutate != nil {
				test.mutate(&cfg)
			}
			if err := cfg.Validate(active, test.standby); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidConfiguration)
			}
		})
	}
}

func TestProcessConfigurationRejectsInvalidBounds(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ProcessConfig)
	}{
		{name: "zero tick", mutate: func(cfg *ProcessConfig) { cfg.Tick = 0 }},
		{name: "RTT below tick", mutate: func(cfg *ProcessConfig) { cfg.InitialRTT = cfg.Tick / 2 }},
		{name: "maximum RTT below initial", mutate: func(cfg *ProcessConfig) { cfg.MaximumRTT = cfg.InitialRTT / 2 }},
		{name: "zero RTT multiplier", mutate: func(cfg *ProcessConfig) { cfg.RTTMultiplier = 0 }},
		{name: "zero backoff", mutate: func(cfg *ProcessConfig) { cfg.BackoffMin = 0 }},
		{name: "backoff order", mutate: func(cfg *ProcessConfig) { cfg.BackoffMax = cfg.BackoffMin / 2 }},
		{name: "zero queue", mutate: func(cfg *ProcessConfig) { cfg.PrimaryRequestQueueMax = 0 }},
		{name: "zero journal reads", mutate: func(cfg *ProcessConfig) { cfg.JournalReadConcurrency = 0 }},
		{name: "zero journal writes", mutate: func(cfg *ProcessConfig) { cfg.JournalWriteConcurrency = 0 }},
		{name: "zero reply reads", mutate: func(cfg *ProcessConfig) { cfg.ReplyReadConcurrency = 0 }},
		{name: "zero reply writes", mutate: func(cfg *ProcessConfig) { cfg.ReplyWriteConcurrency = 0 }},
		{name: "zero block reads", mutate: func(cfg *ProcessConfig) { cfg.BlockReadConcurrency = 0 }},
		{name: "zero block writes", mutate: func(cfg *ProcessConfig) { cfg.BlockWriteConcurrency = 0 }},
		{name: "zero repair requests", mutate: func(cfg *ProcessConfig) { cfg.RepairRequestsMax = 0 }},
		{name: "zero repair reads", mutate: func(cfg *ProcessConfig) { cfg.RepairReadsMax = 0 }},
		{name: "zero scrub reads", mutate: func(cfg *ProcessConfig) { cfg.ScrubReadConcurrency = 0 }},
		{name: "zero scrub writes", mutate: func(cfg *ProcessConfig) { cfg.ScrubWriteConcurrency = 0 }},
		{name: "zero scrub minimum", mutate: func(cfg *ProcessConfig) { cfg.ScrubIntervalMin = 0 }},
		{name: "scrub interval order", mutate: func(cfg *ProcessConfig) { cfg.ScrubIntervalMax = cfg.ScrubIntervalMin / 2 }},
		{name: "scrub cycle", mutate: func(cfg *ProcessConfig) { cfg.ScrubCycle = time.Second }},
		{name: "zero clock window", mutate: func(cfg *ProcessConfig) { cfg.ClockWindowMin = 0 }},
		{name: "clock window order", mutate: func(cfg *ProcessConfig) { cfg.ClockWindowMax = cfg.ClockWindowMin / 2 }},
		{name: "clock epoch", mutate: func(cfg *ProcessConfig) { cfg.ClockEpochMax = cfg.ClockWindowMin }},
		{name: "small storage", mutate: func(cfg *ProcessConfig) { cfg.StorageSizeLimit = 1 }},
		{name: "unaligned storage", mutate: func(cfg *ProcessConfig) { cfg.StorageSizeLimit++ }},
	}
	cluster := DefaultClusterConfig()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultProcessConfig()
			test.mutate(&cfg)
			if err := cfg.Validate(cluster); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidConfiguration)
			}
		})
	}
}

func TestMembershipRequiresOrderedUniqueMembersAndLocalIdentity(t *testing.T) {
	member1 := protocol.MemberID{1}
	member2 := protocol.MemberID{2}
	member3 := protocol.MemberID{3}
	valid := Membership{
		Members:      [MembersMax]protocol.MemberID{member1, member2, member3},
		ActiveCount:  2,
		StandbyCount: 1,
		LocalMember:  member2,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid membership: %v", err)
	}
	if index, ok := valid.LocalIndex(); !ok || index != 1 {
		t.Fatalf("local index = (%d,%t), want (1,true)", index, ok)
	}
	if primary := valid.Primary(3); primary != 1 {
		t.Fatalf("primary(view 3) = %d, want 1", primary)
	}

	tests := []struct {
		name   string
		mutate func(*Membership)
	}{
		{name: "zero active", mutate: func(value *Membership) { value.ActiveCount = 0 }},
		{name: "duplicate", mutate: func(value *Membership) { value.Members[2] = member1 }},
		{name: "gap", mutate: func(value *Membership) { value.Members[1] = protocol.MemberID{} }},
		{name: "nonzero trailing", mutate: func(value *Membership) { value.Members[3] = protocol.MemberID{4} }},
		{name: "missing local", mutate: func(value *Membership) { value.LocalMember = protocol.MemberID{9} }},
		{name: "zero local", mutate: func(value *Membership) { value.LocalMember = protocol.MemberID{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			membership := valid
			test.mutate(&membership)
			if !errors.Is(membership.Validate(), ErrInvalidMembership) {
				t.Fatalf("error = %v, want %v", membership.Validate(), ErrInvalidMembership)
			}
		})
	}
}
