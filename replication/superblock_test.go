package replication

import (
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestCheckpointStateRoundTrip(t *testing.T) {
	validation, superblock := validSuperblockFixture(t)
	checkpointValidation := checkpointValidationFor(t, validation)
	var encoded [CheckpointStateSize]byte
	if err := superblock.State.Checkpoint.Encode(encoded[:]); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCheckpointState(encoded[:], checkpointValidation)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != superblock.State.Checkpoint {
		t.Fatal("checkpoint round trip changed state")
	}
	id, err := decoded.ID()
	if err != nil {
		t.Fatal(err)
	}
	if id != protocol.CheckpointID(protocol.ChecksumBytes(encoded[:])) {
		t.Fatalf("checkpoint ID = %x", id)
	}
}

func TestCheckpointRejectsInvalidReferenceAndReservation(t *testing.T) {
	validation, superblock := validSuperblockFixture(t)
	checkpointValidation := checkpointValidationFor(t, validation)
	var encoded [CheckpointStateSize]byte
	if err := superblock.State.Checkpoint.Encode(encoded[:]); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "zero trailer aggregate", mutate: func(value []byte) { clear(value[448:464]) }},
		{name: "logical size", mutate: func(value []byte) { clear(value[576:584]) }},
		{name: "reserved extension", mutate: func(value []byte) { value[272] = 1 }},
		{name: "manifest endpoint", mutate: func(value []byte) { value[608] = 1 }},
		{name: "reservation", mutate: func(value []byte) { value[616] = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			corrupt := encoded
			test.mutate(corrupt[:])
			if _, err := DecodeCheckpointState(corrupt[:], checkpointValidation); !errors.Is(err, ErrInvalidCheckpoint) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidCheckpoint)
			}
		})
	}
}

func TestCheckpointRejectsOversizedEWAHTrailer(t *testing.T) {
	validation, superblock := validSuperblockFixture(t)
	checkpointValidation := checkpointValidationFor(t, validation)
	state := superblock.State.Checkpoint
	state.LogicalStorageSize += validation.Cluster.BlockSize
	state.AcquiredTrailerLastAddress = 1
	state.AcquiredTrailerLastChecksum = protocol.Checksum{1}
	state.AcquiredAggregateChecksum = protocol.Checksum{2}
	state.AcquiredTrailerEncodedSize = 24
	if err := state.Validate(checkpointValidation); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("oversized EWAH error = %v", err)
	}
}

func TestSuperblockCopiesShareLogicalChecksum(t *testing.T) {
	validation, superblock := validSuperblockFixture(t)
	var first, second [SuperblockBytes]byte
	if err := superblock.Encode(first[:], 0, validation); err != nil {
		t.Fatal(err)
	}
	firstChecksum := superblock.Checksum
	if err := superblock.Encode(second[:], 1, validation); err != nil {
		t.Fatal(err)
	}
	if superblock.Checksum != firstChecksum {
		t.Fatalf("physical copy changed checksum: %x != %x", superblock.Checksum, firstChecksum)
	}
	if first[32] == second[32] {
		t.Fatal("physical copy indexes are equal")
	}

	firstCandidate, err := DecodeSuperblock(first[:], 0, validation)
	if err != nil {
		t.Fatal(err)
	}
	if firstCandidate.MisdirectedIndex {
		t.Fatal("correct physical index marked misdirected")
	}
	misdirected, err := DecodeSuperblock(first[:], 1, validation)
	if err != nil {
		t.Fatal(err)
	}
	if !misdirected.MisdirectedIndex {
		t.Fatal("wrong physical index was not retained as ambiguous")
	}
}

func TestSuperblockSelectionPrefersNewestOpenQuorum(t *testing.T) {
	validation, old := validSuperblockFixture(t)
	oldCandidates := encodeSuperblockCopies(t, old, validation, 0, 1)

	newer := old
	newer.Sequence = 2
	newer.ParentChecksum = oldCandidates[0].Superblock.Checksum
	newer.State.View = 1
	newCandidates := encodeSuperblockCopies(t, newer, validation, 2, 3)
	candidates := append(oldCandidates, newCandidates...)
	selected, err := SelectSuperblock(candidates, 4)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Superblock.Sequence != 2 {
		t.Fatalf("selected sequence = %d, want 2", selected.Superblock.Sequence)
	}
}

func TestSuperblockSelectionRejectsForkAndRegression(t *testing.T) {
	validation, base := validSuperblockFixture(t)
	baseCandidates := encodeSuperblockCopies(t, base, validation, 0, 1)

	t.Run("fork", func(t *testing.T) {
		fork := base
		fork.State.View = 1
		forkCandidates := encodeSuperblockCopies(t, fork, validation, 2, 3)
		_, err := SelectSuperblock(append(baseCandidates, forkCandidates...), 4)
		if !errors.Is(err, ErrSuperblockFork) {
			t.Fatalf("error = %v, want %v", err, ErrSuperblockFork)
		}
	})

	t.Run("regression", func(t *testing.T) {
		older := base
		older.State.View = 1
		olderCandidates := encodeSuperblockCopies(t, older, validation, 0, 1)
		newer := base
		newer.Sequence = 2
		newer.ParentChecksum = olderCandidates[0].Superblock.Checksum
		newCandidates := encodeSuperblockCopies(t, newer, validation, 2, 3)
		_, err := SelectSuperblock(append(olderCandidates, newCandidates...), 4)
		if !errors.Is(err, ErrInvalidSuperblock) {
			t.Fatalf("error = %v, want %v", err, ErrInvalidSuperblock)
		}
	})
}

func TestSuperblockRejectsCorruptionAndReservedBytes(t *testing.T) {
	validation, superblock := validSuperblockFixture(t)
	var encoded [SuperblockBytes]byte
	if err := superblock.Encode(encoded[:], 0, validation); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "checksum", mutate: func(value []byte) { value[40] ^= 1 }},
		{name: "extension", mutate: func(value []byte) { value[16] = 1 }},
		{name: "feature", mutate: func(value []byte) { value[2144] = 1; resealSuperblock(value) }},
		{name: "reservation", mutate: func(value []byte) { value[2174] = 1; resealSuperblock(value) }},
		{name: "header tail", mutate: func(value []byte) { value[len(value)-1] = 1; resealSuperblock(value) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			corrupt := encoded
			test.mutate(corrupt[:])
			if _, err := DecodeSuperblock(corrupt[:], 0, validation); !errors.Is(err, ErrInvalidSuperblock) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidSuperblock)
			}
		})
	}
}

func validSuperblockFixture(t testing.TB) (SuperblockValidation, Superblock) {
	t.Helper()
	cluster := DefaultClusterConfig()
	blockBase, ok := cluster.BlockBase()
	if !ok {
		t.Fatal("BlockBase overflow")
	}
	group := protocol.GroupID{1}
	membership := Membership{
		Members:     [MembersMax]protocol.MemberID{{1}, {2}, {3}},
		ActiveCount: 3,
		LocalMember: protocol.MemberID{1},
	}
	rootHeader := validRootHeader(t, group, membership.ActiveCount)
	emptyChecksum := protocol.ChecksumBytes(nil)
	checkpoint := CheckpointState{
		Header:                    rootHeader,
		AcquiredAggregateChecksum: emptyChecksum,
		ReleasedAggregateChecksum: emptyChecksum,
		SessionAggregateChecksum:  emptyChecksum,
		LogicalStorageSize:        blockBase,
		Release:                   1,
	}
	validation := SuperblockValidation{
		Group:                 group,
		Membership:            membership,
		ConfigurationChecksum: cluster.Fingerprint(),
		Cluster:               cluster,
	}
	superblock := Superblock{
		FormatVersion: protocol.FormatVersion,
		Release:       1,
		Sequence:      1,
		Group:         group,
		State: DurableReplicaState{
			Checkpoint:  checkpoint,
			LocalMember: membership.LocalMember,
			Members:     membership.Members,
			ActiveCount: membership.ActiveCount,
		},
		ViewHeaderCount:       1,
		ConfigurationChecksum: validation.ConfigurationChecksum,
		ProtocolVersion:       protocol.ProtocolVersion,
	}
	superblock.ViewHeaders[0] = rootHeader
	return validation, superblock
}

func validRootHeader(t testing.TB, group protocol.GroupID, memberCount uint8) [protocol.HeaderSize]byte {
	t.Helper()
	var encoded [protocol.HeaderSize]byte
	header := protocol.Header{
		Group:    group,
		Protocol: protocol.ProtocolVersion,
		Command:  protocol.CommandPrepare,
		Author:   0,
	}
	header.Fields[124] = byte(protocol.OperationRoot)
	if err := protocol.SealFrame(encoded[:], &header); err != nil {
		t.Fatal(err)
	}
	if _, reason := protocol.DecodeHeader(encoded[:], group, 1<<20, memberCount); reason != protocol.RejectNone {
		t.Fatalf("root header reason = %d", reason)
	}
	return encoded
}

func checkpointValidationFor(t testing.TB, validation SuperblockValidation) CheckpointValidation {
	t.Helper()
	blockBase, ok := validation.Cluster.BlockBase()
	if !ok {
		t.Fatal("BlockBase overflow")
	}
	return CheckpointValidation{
		Group:          validation.Group,
		MessageSizeMax: uint32(validation.Cluster.MessageSizeMax),
		MemberCount:    validation.Membership.ActiveCount + validation.Membership.StandbyCount,
		BlockBase:      blockBase,
		BlockSize:      validation.Cluster.BlockSize,
		ClientsMax:     validation.Cluster.ClientsMax,
	}
}

func encodeSuperblockCopies(t testing.TB, superblock Superblock, validation SuperblockValidation, indexes ...uint16) []SuperblockCandidate {
	t.Helper()
	candidates := make([]SuperblockCandidate, 0, len(indexes))
	for _, index := range indexes {
		var encoded [SuperblockBytes]byte
		if err := superblock.Encode(encoded[:], index, validation); err != nil {
			t.Fatal(err)
		}
		candidate, err := DecodeSuperblock(encoded[:], index, validation)
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}

func resealSuperblock(encoded []byte) {
	checksum := protocol.ChecksumBytes(encoded[34:])
	copy(encoded[:16], checksum[:])
}
