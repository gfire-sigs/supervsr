package replication

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"z.gfire.dev/supervsr/replication/protocol"
)

func TestCheckpointStateRoundTrip(t *testing.T) {
	validation, superblock := validSuperblockFixture(t)
	checkpointValidation := checkpointValidationFor(t, validation)
	var encoded [checkpointStateSize]byte
	if err := superblock.State.Checkpoint.encode(encoded[:]); err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCheckpointState(encoded[:], checkpointValidation)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != superblock.State.Checkpoint {
		t.Fatal("checkpoint round trip changed state")
	}
	id, err := decoded.ID(ChecksumAEGIS128L)
	if err != nil {
		t.Fatal(err)
	}
	expectedID, err := protocol.ChecksumBytes(protocol.ChecksumAEGIS128L, encoded[:])
	if err != nil {
		t.Fatal(err)
	}
	if id != protocol.CheckpointID(expectedID) {
		t.Fatalf("checkpoint ID = %x", id)
	}
}

func TestCheckpointRejectsInvalidReferenceAndReservation(t *testing.T) {
	validation, superblock := validSuperblockFixture(t)
	checkpointValidation := checkpointValidationFor(t, validation)
	var encoded [checkpointStateSize]byte
	if err := superblock.State.Checkpoint.encode(encoded[:]); err != nil {
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
			if _, err := decodeCheckpointState(corrupt[:], checkpointValidation); !errors.Is(err, ErrInvalidCheckpoint) {
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
	if err := state.validate(checkpointValidation); !errors.Is(err, ErrInvalidCheckpoint) {
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

	firstCandidate, err := decodeSuperblock(first[:], 0, validation)
	if err != nil {
		t.Fatal(err)
	}
	if firstCandidate.MisdirectedIndex {
		t.Fatal("correct physical index marked misdirected")
	}
	misdirected, err := decodeSuperblock(first[:], 1, validation)
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
	newer.ParentChecksum = oldCandidates[0].superblockRecord.Checksum
	newer.State.View = 1
	newCandidates := encodeSuperblockCopies(t, newer, validation, 2, 3)
	candidates := append(oldCandidates, newCandidates...)
	selected, err := selectSuperblock(candidates, 4)
	if err != nil {
		t.Fatal(err)
	}
	if selected.superblockRecord.Sequence != 2 {
		t.Fatalf("selected sequence = %d, want 2", selected.superblockRecord.Sequence)
	}
}

func TestSuperblockSelectionRejectsForkAndRegression(t *testing.T) {
	validation, base := validSuperblockFixture(t)
	baseCandidates := encodeSuperblockCopies(t, base, validation, 0, 1)

	t.Run("fork", func(t *testing.T) {
		fork := base
		fork.State.View = 1
		forkCandidates := encodeSuperblockCopies(t, fork, validation, 2, 3)
		_, err := selectSuperblock(append(baseCandidates, forkCandidates...), 4)
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
		newer.ParentChecksum = olderCandidates[0].superblockRecord.Checksum
		newCandidates := encodeSuperblockCopies(t, newer, validation, 2, 3)
		_, err := selectSuperblock(append(olderCandidates, newCandidates...), 4)
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
			if _, err := decodeSuperblock(corrupt[:], 0, validation); !errors.Is(err, ErrInvalidSuperblock) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidSuperblock)
			}
		})
	}
}

func validSuperblockFixture(t testing.TB) (superblockValidation, superblockRecord) {
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
	emptyChecksum, err := protocol.ChecksumBytes(protocol.ChecksumAEGIS128L, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := CheckpointState{
		Header:                    rootHeader,
		AcquiredAggregateChecksum: emptyChecksum,
		ReleasedAggregateChecksum: emptyChecksum,
		SessionAggregateChecksum:  emptyChecksum,
		LogicalStorageSize:        blockBase,
		Release:                   1,
	}
	validation := superblockValidation{
		Group:                 group,
		Membership:            membership,
		ConfigurationChecksum: cluster.Fingerprint(),
		Cluster:               cluster,
	}
	superblock := superblockRecord{
		FormatVersion: protocol.FormatVersion,
		Release:       1,
		Sequence:      1,
		Group:         group,
		State: durableReplicaState{
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
	if err := protocol.SealFrame(encoded[:], &header, protocol.ChecksumAEGIS128L); err != nil {
		t.Fatal(err)
	}
	if _, reason := protocol.DecodeHeader(encoded[:], group, 1<<20, memberCount, protocol.ChecksumAEGIS128L); reason != protocol.RejectNone {
		t.Fatalf("root header reason = %d", reason)
	}
	return encoded
}

func checkpointValidationFor(t testing.TB, validation superblockValidation) checkpointRules {
	t.Helper()
	blockBase, ok := validation.Cluster.BlockBase()
	if !ok {
		t.Fatal("BlockBase overflow")
	}
	return checkpointRules{
		Group:          validation.Group,
		MessageSizeMax: uint32(validation.Cluster.MessageSizeMax),
		MemberCount:    validation.Membership.ActiveCount + validation.Membership.StandbyCount,
		BlockBase:      blockBase,
		BlockSize:      validation.Cluster.BlockSize,
		ClientsMax:     validation.Cluster.ClientsMax,
	}
}

func encodeSuperblockCopies(t testing.TB, superblock superblockRecord, validation superblockValidation, indexes ...uint16) []superblockCandidate {
	t.Helper()
	candidates := make([]superblockCandidate, 0, len(indexes))
	for _, index := range indexes {
		var encoded [SuperblockBytes]byte
		if err := superblock.Encode(encoded[:], index, validation); err != nil {
			t.Fatal(err)
		}
		candidate, err := decodeSuperblock(encoded[:], index, validation)
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}

func resealSuperblock(encoded []byte) {
	checksum, err := protocol.ChecksumBytes(protocol.ChecksumAEGIS128L, encoded[34:])
	if err != nil {
		panic(err)
	}
	copy(encoded[:16], checksum[:])
}

func TestFormatAndOpenSuperblockStoreWithWyhash128(t *testing.T) {
	cluster := DefaultClusterConfig()
	cluster.ChecksumAlgorithm = ChecksumWyhash128
	cluster.ClientsMax = 4
	cluster.PipelineMax = 4
	cluster.ViewChangeHeadersSuffixMax = 5
	cluster.JournalSlots = 128
	cluster.MessageSizeMax = 128 << 10
	cluster.ApplicationBatchSizeMax = cluster.MessageSizeMax - protocol.HeaderSize
	cluster.ApplicationReplySizeMax = cluster.MessageSizeMax - protocol.HeaderSize
	cluster.BlockSize = 128 << 10
	cluster.CompactionOps = 16

	membership := Membership{
		Members:     [MembersMax]protocol.MemberID{{1}, {2}, {3}},
		ActiveCount: 3,
		LocalMember: protocol.MemberID{1},
	}
	group := protocol.GroupID{7}
	path := filepath.Join(t.TempDir(), "wyhash.vsr")

	formatStorage, err := OpenFileStorage(path, FileStorageOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := Format(context.Background(), FormatConfig{
		Group:          group,
		Membership:     membership,
		Cluster:        cluster,
		CurrentRelease: 1,
	}, FormatDependencies{Storage: formatStorage}); err != nil {
		_ = formatStorage.Close()
		t.Fatal(err)
	}
	if err := formatStorage.Close(); err != nil {
		t.Fatal(err)
	}

	storage, err := OpenFileStorage(path, FileStorageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := storage.Close(); closeErr != nil {
			t.Errorf("close: %v", closeErr)
		}
	}()

	validation := superblockValidation{
		Group:                 group,
		Membership:            membership,
		ConfigurationChecksum: cluster.Fingerprint(),
		Cluster:               cluster,
	}
	store, err := openSuperblockStore(storage, validation)
	if err != nil {
		t.Fatal(err)
	}
	if store.Current().ConfigurationChecksum != cluster.Fingerprint() {
		t.Fatalf("fingerprint = %x, want %x", store.Current().ConfigurationChecksum, cluster.Fingerprint())
	}

	// Opening the same file with AEGIS128L must report ErrIncompatibleConfiguration.
	aegisCluster := cluster
	aegisCluster.ChecksumAlgorithm = ChecksumAEGIS128L
	if aegisCluster.Fingerprint() == cluster.Fingerprint() {
		t.Fatal("AEGIS128L and Wyhash128 fingerprints collided")
	}
	aegisValidation := superblockValidation{
		Group:                 group,
		Membership:            membership,
		ConfigurationChecksum: aegisCluster.Fingerprint(),
		Cluster:               aegisCluster,
	}
	if _, err := openSuperblockStore(storage, aegisValidation); !errors.Is(err, ErrIncompatibleConfiguration) {
		t.Fatalf("mismatched checksum algorithm error = %v, want %v", err, ErrIncompatibleConfiguration)
	}
}

func TestSuperblockAEGIS128LRejectedUnderWyhash128Config(t *testing.T) {
	validation, superblock := validSuperblockFixture(t)
	var encoded [SuperblockBytes]byte
	if err := superblock.Encode(encoded[:], 0, validation); err != nil {
		t.Fatal(err)
	}
	wyhashCluster := validation.Cluster
	wyhashCluster.ChecksumAlgorithm = ChecksumWyhash128
	wyhashValidation := validation
	wyhashValidation.Cluster = wyhashCluster
	wyhashValidation.ConfigurationChecksum = wyhashCluster.Fingerprint()
	if _, err := decodeSuperblock(encoded[:], 0, wyhashValidation); !errors.Is(err, ErrIncompatibleConfiguration) {
		t.Fatalf("AEGIS superblock under Wyhash config error = %v, want %v", err, ErrIncompatibleConfiguration)
	}
}

func TestRequireHardwareChecksumOptIn(t *testing.T) {
	cluster := DefaultClusterConfig()
	process := DefaultProcessConfig()
	blockBase, ok := cluster.BlockBase()
	if !ok {
		t.Fatal("BlockBase overflow")
	}
	process.StorageSizeLimit = blockBase + cluster.BlockSize
	if err := process.Validate(cluster); err != nil {
		t.Fatalf("default process validate failed: %v", err)
	}

	process.RequireHardwareChecksum = true
	err := process.Validate(cluster)
	if protocol.HardwareChecksumAvailable() {
		if err != nil {
			t.Fatalf("hardware process validate unexpected error: %v", err)
		}
	} else if !errors.Is(err, ErrHardwareChecksumUnavailable) {
		t.Fatalf("software process validate error = %v, want %v", err, ErrHardwareChecksumUnavailable)
	}

	optInStorage, err := OpenFileStorage(filepath.Join(t.TempDir(), "optin.vsr"), FileStorageOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := optInStorage.Close(); closeErr != nil {
			t.Errorf("close: %v", closeErr)
		}
	}()
	formatErr := validateFormatConfig(FormatConfig{
		Group:                   protocol.GroupID{1},
		Membership:              Membership{Members: [MembersMax]protocol.MemberID{{1}}, ActiveCount: 1, LocalMember: protocol.MemberID{1}},
		Cluster:                 cluster,
		CurrentRelease:          1,
		RequireHardwareChecksum: true,
	}, FormatDependencies{Storage: optInStorage})
	if protocol.HardwareChecksumAvailable() {
		if formatErr != nil {
			t.Fatalf("hardware format validate unexpected error: %v", formatErr)
		}
	} else if !errors.Is(formatErr, ErrHardwareChecksumUnavailable) {
		t.Fatalf("software format validate error = %v, want %v", formatErr, ErrHardwareChecksumUnavailable)
	}
}
