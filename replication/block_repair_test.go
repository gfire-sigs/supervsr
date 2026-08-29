package replication

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestBlockRepairCoalescesWaitersInBothOrders(t *testing.T) {
	reference := BlockReference{Address: 4096, Checksum: protocol.Checksum{1}}
	exact := blockSnapshotExpectation{value: 7, exact: true}
	orders := [][2]blockRepairSource{{blockRepairScrub, blockRepairStateSync}, {blockRepairStateSync, blockRepairScrub}}
	for _, order := range orders {
		replica := Replica{blockRepairTargets: make([]blockRepairTarget, 1)}
		firstCheckpoint := protocol.Op(0)
		firstSnapshot := blockSnapshotExpectation{}
		if order[0] == blockRepairStateSync {
			firstCheckpoint = 9
			firstSnapshot = exact
		}
		if !replica.queueBlockRepair(reference, protocol.BlockFreeSet, firstCheckpoint, firstSnapshot, 8, order[0]) {
			t.Fatal("first waiter rejected")
		}
		secondCheckpoint := protocol.Op(0)
		secondSnapshot := blockSnapshotExpectation{}
		if order[1] == blockRepairStateSync {
			secondCheckpoint = 9
			secondSnapshot = exact
		}
		if !replica.queueBlockRepair(reference, protocol.BlockFreeSet, secondCheckpoint, secondSnapshot, 8, order[1]) {
			t.Fatal("second waiter rejected")
		}
		target := replica.blockRepairTargets[0]
		if target.waiters != blockRepairScrub|blockRepairStateSync || target.neededAtCheckpoint != 9 || target.snapshot != exact {
			t.Fatalf("coalesced target = %+v", target)
		}
	}
}

func TestBlockRepairRejectsDuplicateResponsesAndRecyclesTargets(t *testing.T) {
	cluster := compactTestClusterConfig()
	config := Config{
		Cluster: cluster, Process: DefaultProcessConfig(), Group: protocol.GroupID{1}, CurrentRelease: 1,
		Membership: Membership{Members: [MembersMax]protocol.MemberID{{1}, {2}}, ActiveCount: 2, LocalMember: protocol.MemberID{1}},
	}
	storage := newBlockingStorage()
	engine, err := NewIOEngine(storage, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIOEngine(t, engine)
	budget, err := newBlockRepairBudget(2, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := NewAlignedBuffer(cluster.BlockSize, SectorSize)
	if err != nil {
		t.Fatal(err)
	}
	replica := Replica{
		config: config, membership: config.Membership, local: 0, deps: Dependencies{}, io: engine,
		blockRepairBudget: budget, blockRepairTargets: make([]blockRepairTarget, 1),
		blockRepairIO: []blockRepairIO{{buffer: buffer}},
	}
	base, _ := cluster.BlockBase()
	for index := 0; index < 4; index++ {
		address := base + uint64(index)*cluster.BlockSize
		header, body := makeRepairBlock(t, config, address, 7)
		reference := BlockReference{Address: address, Checksum: header.HeaderChecksum}
		if !replica.queueBlockRepair(reference, protocol.BlockFreeSet, 9, blockSnapshotExpectation{value: 7, exact: true}, uint32(len(body)), blockRepairScrub) {
			t.Fatal("repair target rejected")
		}
		replica.blockRepairTargets[0].state = blockRepairMissing
		if !replica.blockRepairBudget.Reserve(1, []BlockReference{reference}, uint64(index+1)*blockRepairExpires) {
			t.Fatal("repair budget rejected")
		}
		replica.handleBlock(header, body)
		if index == 0 {
			replica.handleBlock(header, body)
			select {
			case <-storage.started:
			case <-time.After(time.Second):
				t.Fatal("block write did not start")
			}
			if storage.writes.Load() != 1 || replica.activeBlockRepairWrites() != 1 {
				t.Fatalf("duplicate write count=%d active=%d", storage.writes.Load(), replica.activeBlockRepairWrites())
			}
			close(storage.release)
		}
		drainBlockRepairIO(t, &replica)
		if replica.blockRepairTargets[0].state != 0 || !replica.blockRepairBudget.valid() {
			t.Fatalf("target was not recycled: %+v", replica.blockRepairTargets[0])
		}
	}
}

func TestStalledBlockRepairDoesNotStarveJournalRepair(t *testing.T) {
	membership := Membership{Members: [MembersMax]protocol.MemberID{{1}, {2}}, ActiveCount: 2, LocalMember: protocol.MemberID{1}}
	blockBudget, err := newBlockRepairBudget(2, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	reference := BlockReference{Address: 4096, Checksum: protocol.Checksum{1}}
	if !blockBudget.Reserve(1, []BlockReference{reference}, 1) {
		t.Fatal("block reservation rejected")
	}
	journalBudget, err := newJournalRepairBudget(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := protocol.NewFramePool(2, 4096)
	if err != nil {
		t.Fatal(err)
	}
	bus := &captureBus{}
	repairHeader := protocol.Header{Command: protocol.CommandPrepare, HeaderChecksum: protocol.Checksum{2}}
	binary.LittleEndian.PutUint64(repairHeader.Fields[96:104], 1)
	replica := Replica{
		config:     Config{Cluster: compactTestClusterConfig(), Process: DefaultProcessConfig(), Group: protocol.GroupID{1}},
		membership: membership, local: 0, deps: Dependencies{MessageBus: bus}, frames: frames,
		random: NewDeterministicRandom(1), repairBudget: journalBudget, repairHeader: repairHeader, repairHeaderValid: true,
		blockRepairBudget: blockBudget, blockRepairTargets: []blockRepairTarget{{reference: reference, state: blockRepairMissing}},
		peerCheckpointOps: make([]protocol.Op, 2),
	}
	replica.handleRepairTimeout(TimeSample{Monotonic: 2})
	if bus.replicaCount() != 1 {
		t.Fatalf("journal repair messages = %d, want 1", bus.replicaCount())
	}
	header, _, reason := protocol.DecodeFrame(bus.replicaMessage(t, 0), replica.config.Group, 4096, 2)
	if reason != protocol.RejectNone || header.Command != protocol.CommandGetPrepare {
		t.Fatalf("journal repair frame command=%d reason=%d", header.Command, reason)
	}
}

func TestBlockRepairRevalidatesConcurrentChildrenAtDurability(t *testing.T) {
	config := Config{
		Cluster: compactTestClusterConfig(), Process: DefaultProcessConfig(),
		Group: protocol.GroupID{1}, CurrentRelease: 1,
		Membership: Membership{Members: [MembersMax]protocol.MemberID{{1}}, ActiveCount: 1, LocalMember: protocol.MemberID{1}},
	}
	base, ok := config.Cluster.BlockBase()
	if !ok {
		t.Fatal("block base overflow")
	}
	parentA, bodyA, physicalA := makeValueRepairBlock(t, config, base, 1)
	parentB, bodyB, physicalB := makeValueRepairBlock(t, config, base+config.Cluster.BlockSize, 2)
	childA := BlockRequirement{Reference: BlockReference{Address: base + 2*config.Cluster.BlockSize, Checksum: protocol.Checksum{3}}, Type: protocol.BlockValue}
	childB := BlockRequirement{Reference: BlockReference{Address: base + 3*config.Cluster.BlockSize, Checksum: protocol.Checksum{4}}, Type: protocol.BlockValue}
	validator := &branchingBlockValidator{children: map[uint64]BlockRequirement{
		base: childA, base + config.Cluster.BlockSize: childB,
	}}
	replica := Replica{
		config: config, membership: config.Membership, deps: Dependencies{BlockValidator: validator},
		blockCatalog: make([]BlockRequirement, 4), blockRequirements: make([]BlockRequirement, 1),
	}
	targetA := blockRepairTarget{reference: BlockReference{Address: base, Checksum: parentA.HeaderChecksum}, blockType: protocol.BlockValue, state: blockRepairWriting}
	targetB := blockRepairTarget{reference: BlockReference{Address: base + config.Cluster.BlockSize, Checksum: parentB.HeaderChecksum}, blockType: protocol.BlockValue, state: blockRepairWriting}
	if !replica.validRequestedBlock(&targetA, parentA, bodyA) || !replica.validRequestedBlock(&targetB, parentB, bodyB) {
		t.Fatal("valid concurrent parent rejected")
	}
	replica.blockRepairCompleted(&targetA, physicalA)
	replica.blockRepairCompleted(&targetB, physicalB)
	if replica.fatalErr != nil {
		t.Fatal(replica.fatalErr)
	}
	if _, ok := replica.knownBlock(childA.Reference.Address); !ok {
		t.Fatal("first durable parent's child was lost")
	}
	if _, ok := replica.knownBlock(childB.Reference.Address); !ok {
		t.Fatal("second durable parent's child was lost")
	}
}

type branchingBlockValidator struct {
	children map[uint64]BlockRequirement
}

func (validator *branchingBlockValidator) CheckpointRoot(CheckpointState) (BlockRequirement, error) {
	return BlockRequirement{}, nil
}

func (validator *branchingBlockValidator) ResolveBlock(CheckpointState, uint64) (BlockRequirement, bool, error) {
	return BlockRequirement{}, false, nil
}

func (validator *branchingBlockValidator) ValidateBlock(input BlockValidationInput, references []BlockRequirement) (int, error) {
	child, ok := validator.children[input.Reference.Address]
	if !ok {
		return 0, ErrInvalidBlock
	}
	references[0] = child
	return 1, nil
}

func makeValueRepairBlock(t testing.TB, config Config, address, marker uint64) (protocol.Header, []byte, []byte) {
	t.Helper()
	body := []byte{byte(marker)}
	physical := make([]byte, config.Cluster.BlockSize)
	copy(physical[protocol.HeaderSize:], body)
	header := protocol.Header{Group: config.Group, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandBlock}
	binary.LittleEndian.PutUint32(header.Fields[:4], 1)
	binary.LittleEndian.PutUint32(header.Fields[4:8], 1)
	binary.LittleEndian.PutUint32(header.Fields[8:12], 1)
	binary.LittleEndian.PutUint64(header.Fields[96:104], address)
	header.Fields[112] = byte(protocol.BlockValue)
	frame := physical[:protocol.HeaderSize+len(body)]
	if err := protocol.SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}
	return header, body, physical
}

func makeRepairBlock(t testing.TB, config Config, address, snapshot uint64) (protocol.Header, []byte) {
	body := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	frame := make([]byte, protocol.HeaderSize+len(body))
	copy(frame[protocol.HeaderSize:], body)
	header := protocol.Header{Group: config.Group, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandBlock}
	binary.LittleEndian.PutUint64(header.Fields[96:104], address)
	binary.LittleEndian.PutUint64(header.Fields[104:112], snapshot)
	header.Fields[112] = byte(protocol.BlockFreeSet)
	if err := protocol.SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}
	return header, body
}

func drainBlockRepairIO(t testing.TB, replica *Replica) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for !replica.io.Drained() {
		select {
		case <-replica.io.Ready():
			for {
				var completion IOCompletion
				if !replica.io.Poll(&completion) {
					break
				}
				replica.handleBlockRepairIO(completion)
			}
		case <-deadline.C:
			t.Fatal("block repair IO did not drain")
		}
	}
}
