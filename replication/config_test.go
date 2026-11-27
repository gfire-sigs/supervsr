package replication

import (
	"errors"
	"testing"
	"time"

	"z.gfire.dev/supervsr/replication/protocol"
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
func TestBlockOffsetUsesOneBasedLogicalAddresses(t *testing.T) {
	config := compactTestClusterConfig()
	base, ok := config.BlockBase()
	if !ok {
		t.Fatal("block base overflow")
	}
	first, ok := config.BlockOffset(1)
	if !ok || first != base {
		t.Fatalf("first block offset = %d valid=%t, want %d", first, ok, base)
	}
	second, ok := config.BlockOffset(2)
	if !ok || second != base+config.BlockSize {
		t.Fatalf("second block offset = %d valid=%t, want %d", second, ok, base+config.BlockSize)
	}
	if _, ok := config.BlockOffset(0); ok {
		t.Fatal("zero block address accepted")
	}
	if _, ok := config.BlockOffset(^uint64(0)); ok {
		t.Fatal("overflowing block address accepted")
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
		{name: "small reply", mutate: func(cfg *ClusterConfig) { cfg.ApplicationReplySizeMax = 63 }},
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
		{name: "slow tick", mutate: func(cfg *ProcessConfig) { cfg.Tick = 50 * time.Millisecond }},
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
		{name: "negative ping interval", mutate: func(cfg *ProcessConfig) { cfg.PingInterval = -time.Millisecond }},
		{name: "negative client ping interval", mutate: func(cfg *ProcessConfig) { cfg.ClientPingInterval = -time.Millisecond }},
		{name: "timer below tick", mutate: func(cfg *ProcessConfig) { cfg.RepairInterval = cfg.Tick / 2 }},
		{name: "failure interval order", mutate: func(cfg *ProcessConfig) {
			cfg.FailureIntervalMin = 3 * time.Second
			cfg.FailureIntervalMax = 2 * time.Second
		}},
		{name: "yellow permille below 1000", mutate: func(cfg *ProcessConfig) { cfg.FailureYellowPermille = 999 }},
		{name: "red permille not above yellow", mutate: func(cfg *ProcessConfig) { cfg.FailureYellowPermille = 2000; cfg.FailureRedPermille = 2000 }},
		{name: "block repair timeout below retry", mutate: func(cfg *ProcessConfig) {
			cfg.BlockRepairRetryAfter = 500 * time.Millisecond
			cfg.BlockRepairTimeout = 250 * time.Millisecond
		}},
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

func TestProcessConfigZeroOptionalTimersUseV1DefaultsAndSupportWANTuning(t *testing.T) {
	cluster := DefaultClusterConfig()

	// Legacy v1.0 literal ProcessConfig with zero optional timer fields must validate and resolve to v1.0 defaults.
	legacy := ProcessConfig{
		Tick:                    10 * time.Millisecond,
		InitialRTT:              300 * time.Millisecond,
		MaximumRTT:              60 * time.Second,
		RTTMultiplier:           2,
		BackoffMin:              10 * time.Millisecond,
		BackoffMax:              10 * time.Second,
		PrimaryRequestQueueMax:  2,
		JournalReadConcurrency:  8,
		JournalWriteConcurrency: 32,
		ReplyReadConcurrency:    1,
		ReplyWriteConcurrency:   2,
		BlockReadConcurrency:    32,
		BlockWriteConcurrency:   32,
		StorageSizeLimit:        16 << 40,
		RepairRequestsMax:       4,
		RepairReadsMax:          4,
		ScrubReadConcurrency:    1,
		ScrubWriteConcurrency:   1,
		ScrubCycle:              180 * 24 * time.Hour,
		ScrubIntervalMin:        50 * time.Millisecond,
		ScrubIntervalMax:        10 * time.Second,
		ClockOffsetToleranceMax: 10 * time.Second,
		ClockEpochMax:           60 * time.Second,
		ClockWindowMin:          2 * time.Second,
		ClockWindowMax:          20 * time.Second,
	}
	if err := legacy.Validate(cluster); err != nil {
		t.Fatalf("legacy v1.0 ProcessConfig rejected: %v", err)
	}
	if legacy.resolvedTimers() != DefaultProcessConfig().resolvedTimers() {
		t.Fatalf("zero-field resolution mismatch: got %+v want %+v", legacy.resolvedTimers(), DefaultProcessConfig().resolvedTimers())
	}

	// WAN profile with larger FailureIntervalMin permits larger Tick and custom timers across replica, budgets, and client.
	wan := DefaultProcessConfig()
	wan.Tick = 50 * time.Millisecond
	wan.InitialRTT = 250 * time.Millisecond
	wan.BackoffMin = 50 * time.Millisecond
	wan.PingInterval = 2 * time.Second
	wan.PrepareTimeout = 600 * time.Millisecond
	wan.AbdicationTimeout = 20 * time.Second
	wan.CommitInterval = 1 * time.Second
	wan.ExitViewTimeout = 1 * time.Second
	wan.JoinViewTimeout = 1 * time.Second
	wan.ViewChangeTimeout = 12 * time.Second
	wan.ExitViewCollectionWindow = 15 * time.Second
	wan.PulseInterval = 250 * time.Millisecond
	wan.RepairInterval = 500 * time.Millisecond
	wan.JournalRepairTimeout = 2 * time.Second
	wan.BlockRepairRetryAfter = 500 * time.Millisecond
	wan.BlockRepairTimeout = 2 * time.Second
	wan.ClientPingInterval = 5 * time.Second
	wan.FailureIntervalMin = 500 * time.Millisecond
	wan.FailureIntervalMax = 6 * time.Second
	wan.FailureYellowPermille = 2000
	wan.FailureRedPermille = 5000

	if err := wan.Validate(cluster); err != nil {
		t.Fatalf("WAN ProcessConfig rejected: %v", err)
	}
	if err := validateClientProcess(wan); err != nil {
		t.Fatalf("WAN Client ProcessConfig rejected: %v", err)
	}
	timers, err := newReplicaTimers(wan)
	if err != nil {
		t.Fatalf("newReplicaTimers(wan): %v", err)
	}
	gotThresholds := [6]uint64{
		timers.ping.Threshold(),
		timers.prepare.Threshold(),
		timers.commit.Threshold(),
		timers.repair.Threshold(),
		timers.viewChange.Threshold(),
		timers.abdication.Threshold(),
	}
	wantThresholds := [6]uint64{40, 12, 20, 10, 240, 400}
	if gotThresholds != wantThresholds {
		t.Fatalf("WAN timer thresholds = %v, want %v", gotThresholds, wantThresholds)
	}

	// Journal repair budget respects configured 2s deadline instead of hardcoded 500ms.
	jBudget, err := newJournalRepairBudgetWithTimeout(3, 0, wan.JournalRepairTimeout)
	if err != nil {
		t.Fatal(err)
	}
	jBudget.ewma[1] = uint64(time.Second)
	jBudget.ewma[2] = uint64(time.Second)
	rng := newDeterministicRandom(7)
	if _, ok, reserveErr := jBudget.Reserve(1, 100, &rng); reserveErr != nil || !ok {
		t.Fatalf("journal reserve ok=%v err=%v", ok, reserveErr)
	}
	if expired, expireErr := jBudget.Expire(100 + uint64(time.Second)); expireErr != nil || expired != 0 {
		t.Fatalf("expired at 1s = (%d, %v), want (0, nil)", expired, expireErr)
	}
	if expired, expireErr := jBudget.Expire(100 + uint64(2*time.Second)); expireErr != nil || expired != 1 {
		t.Fatalf("expired at 2s = (%d, %v), want (1, nil)", expired, expireErr)
	}

	// Block repair budget respects configured 500ms retry and 2s expiry.
	bBudget, err := newBlockRepairBudgetWithTimeouts(3, 0, 2, wan.BlockRepairRetryAfter, wan.BlockRepairTimeout)
	if err != nil {
		t.Fatal(err)
	}
	ref := BlockReference{Address: 1, Checksum: protocol.Checksum{1}}
	if !bBudget.Reserve(1, []BlockReference{ref}, 100) {
		t.Fatal("initial block reserve failed")
	}
	if bBudget.Reserve(2, []BlockReference{ref}, 100+uint64(300*time.Millisecond)) {
		t.Fatal("block retry succeeded before configured BlockRepairRetryAfter")
	}
	if !bBudget.Reserve(2, []BlockReference{ref}, 100+uint64(500*time.Millisecond)) {
		t.Fatal("block retry failed at configured BlockRepairRetryAfter")
	}
	if expired, expireErr := bBudget.Expire(100 + uint64(time.Second)); expireErr != nil || expired != 0 {
		t.Fatalf("block expired at 1s = (%d, %v), want (0, nil)", expired, expireErr)
	}
	if expired, expireErr := bBudget.Expire(100 + uint64(2*time.Second)); expireErr != nil || expired != 1 {
		t.Fatalf("block expired at 2s = (%d, %v), want (1, nil)", expired, expireErr)
	}

	// Client honors custom ClientPingInterval (5s / 50ms = 100 ticks).
	clientBus := &captureBus{}
	client, err := NewClient(ClientConfig{
		Group:             protocol.GroupID{1},
		ID:                protocol.ClientID{2},
		Release:           1,
		ActiveCount:       3,
		MessageSizeMax:    4096,
		ChecksumAlgorithm: protocol.ChecksumAEGIS128L,
		Process:           wan,
	}, clientBus, fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}}, zeroReader{}, &clientEventCollector{})
	if err != nil {
		t.Fatalf("NewClient(wan): %v", err)
	}
	if client.ping.Threshold() != 100 {
		t.Fatalf("client ping threshold = %d, want 100", client.ping.Threshold())
	}
	_ = client.Close()

	// ExitView collection honors configured ExitViewCollectionWindow (15s instead of 5s).
	replica := &Replica{
		config: Config{Process: wan},
		membership: Membership{
			Members:     [MembersMax]protocol.MemberID{{1}, {2}, {3}, {4}},
			ActiveCount: 4,
			LocalMember: protocol.MemberID{1},
		},
		quorums: Quorums{Replication: 2, ViewChange: 3, Negative: 3, Majority: 3, Upgrade: 4},
		status:  StatusNormal,
		view:    1,
	}
	replica.handleExitView(protocol.Header{View: 1, Author: 1, Command: protocol.CommandExitView}, uint64(time.Second))
	// At +10s (past default 5s, inside 15s window), author 1 vote must still be retained.
	replica.handleExitView(protocol.Header{View: 1, Author: 2, Command: protocol.CommandExitView}, uint64(11*time.Second))
	if replica.exitViewBits&(1<<1) == 0 {
		t.Fatalf("configured 15s ExitViewCollectionWindow expired early at +10s (bits=%b)", replica.exitViewBits)
	}
	// At +16s (past 15s window from t=1s), author 1 vote must expire while author 2 (t=11s) remains.
	replica.handleExitView(protocol.Header{View: 1, Author: 3, Command: protocol.CommandExitView}, uint64(17*time.Second))
	if replica.exitViewBits&(1<<1) != 0 || replica.exitViewBits&(1<<2) == 0 || replica.exitViewBits&(1<<3) == 0 {
		t.Fatalf("unexpected exitViewBits after 15s window expiry: %b", replica.exitViewBits)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

type clientEventCollector struct{}

func (*clientEventCollector) Reply(ClientReply)               {}
func (*clientEventCollector) Evicted(protocol.EvictionReason) {}

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
