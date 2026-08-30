package replication

import (
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestScrubCursorVisitsAllocatedBlocksInAddressOrder(t *testing.T) {
	acquired := mustFixedBitSet(t, 5, 0, 1, 3, 4)
	released := mustFixedBitSet(t, 5, 1, 4)
	replica := Replica{}
	for index, want := range []uint64{0, 3, 0, 3} {
		got, ok := replica.nextScrubIndex(&acquired, &released)
		if !ok || got != want {
			t.Fatalf("step %d: index=%d found=%t, want %d", index, got, ok, want)
		}
	}
}

func TestScrubDelayTracksAllocatedBlocksAndClamps(t *testing.T) {
	acquired := mustFixedBitSet(t, 4, 0, 1, 2)
	released := mustFixedBitSet(t, 4)
	replica := Replica{
		config: Config{Process: ProcessConfig{
			ScrubCycle: 10 * time.Second, ScrubReadConcurrency: 2,
			ScrubIntervalMin: 250 * time.Millisecond, ScrubIntervalMax: 20 * time.Second,
		}},
		io:             &IOEngine{freeLen: 1},
		blockAllocator: &BlockAllocator{blockCount: 4, acquired: acquired, released: released},
	}
	if got, want := replica.scrubDelay(), 20*time.Second/3; got != want {
		t.Fatalf("scrub delay=%s, want %s", got, want)
	}
	replica.config.Process.ScrubCycle = time.Millisecond
	if got, want := replica.scrubDelay(), 250*time.Millisecond; got != want {
		t.Fatalf("minimum scrub delay=%s, want %s", got, want)
	}
	replica.io.freeLen = 0
	if got, want := replica.scrubDelay(), 20*time.Second; got != want {
		t.Fatalf("busy scrub delay=%s, want %s", got, want)
	}
}

func TestScrubResolvesUncachedExpectationBeforeValidation(t *testing.T) {
	config := Config{
		Cluster: compactTestClusterConfig(), Process: DefaultProcessConfig(),
		Group: protocol.GroupID{1}, CurrentRelease: 1,
		Membership: Membership{Members: [MembersMax]protocol.MemberID{{1}}, ActiveCount: 1, LocalMember: protocol.MemberID{1}},
	}
	wrongHeader, _ := makeRepairBlock(t, config, 1, 7)
	wrongFrame := make([]byte, config.Cluster.BlockSize)
	body := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	copy(wrongFrame[protocol.HeaderSize:], body)
	if err := protocol.EncodeHeader(wrongFrame[:protocol.HeaderSize], &wrongHeader); err != nil {
		t.Fatal(err)
	}
	expected := BlockRequirement{
		Reference: BlockReference{Address: 1, Checksum: protocol.Checksum{9}},
		Type:      protocol.BlockFreeSet, Snapshot: 7, SnapshotExact: true, BodySize: uint32(len(body)),
	}
	validator := &resolvingBlockValidator{requirement: expected}
	metrics := &ReplicaMetrics{}
	replica := Replica{
		config: config, membership: config.Membership,
		deps:         Dependencies{BlockValidator: validator},
		blockCatalog: make([]BlockRequirement, 1), blockRepairTargets: make([]blockRepairTarget, 1),
		metrics: metrics,
	}
	operation := blockRepairIO{buffer: wrongFrame, address: 1, busy: true, stage: blockRepairIOScrub}
	replica.finishScrubRead(&operation, IOCompletion{})
	if validator.resolveCalls != 1 {
		t.Fatalf("resolve calls=%d, want 1", validator.resolveCalls)
	}
	if replica.fatalErr != nil {
		t.Fatal(replica.fatalErr)
	}
	target := replica.blockRepairTargets[0]
	if target.state != blockRepairQueued || target.reference != expected.Reference {
		t.Fatalf("repair target=%+v, want expected reference", target)
	}
	if replica.blockCatalogCount != 0 {
		t.Fatalf("untrusted block cached: count=%d", replica.blockCatalogCount)
	}
	if snapshot := metrics.Snapshot(); snapshot.StorageCorruptions != 1 {
		t.Fatalf("storage corruption metric = %d", snapshot.StorageCorruptions)
	}
}

func mustFixedBitSet(t testing.TB, length uint64, indexes ...uint64) FixedBitSet {
	t.Helper()
	set, err := NewFixedBitSet(length)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range indexes {
		if !set.Set(index) {
			t.Fatalf("set index %d", index)
		}
	}
	return set
}

type resolvingBlockValidator struct {
	requirement  BlockRequirement
	resolveCalls int
}

func (validator *resolvingBlockValidator) CheckpointRoot(CheckpointState) (BlockRequirement, error) {
	return validator.requirement, nil
}

func (validator *resolvingBlockValidator) ResolveBlock(_ CheckpointState, address uint64) (BlockRequirement, bool, error) {
	validator.resolveCalls++
	return validator.requirement, validator.requirement.Reference.Address == address, nil
}

func (validator *resolvingBlockValidator) ValidateBlock(BlockValidationInput, []BlockRequirement) (int, error) {
	return 0, nil
}
