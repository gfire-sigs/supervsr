package replication

import (
	"errors"
	"testing"

	"z.gfire.dev/supervsr/replication/protocol"
)

type readOnlyRejectStorage struct {
	inner Storage
	wrote bool
}

func (s *readOnlyRejectStorage) ReadAt(buf []byte, offset uint64) error {
	return s.inner.ReadAt(buf, offset)
}
func (s *readOnlyRejectStorage) WriteAt([]byte, uint64) error {
	s.wrote = true
	return ErrStorage
}
func (s *readOnlyRejectStorage) Sync() error {
	s.wrote = true
	return ErrStorage
}
func (s *readOnlyRejectStorage) Resize(uint64) error {
	s.wrote = true
	return ErrStorage
}
func (s *readOnlyRejectStorage) Size() (uint64, error) { return s.inner.Size() }
func (s *readOnlyRejectStorage) SyncParent() error     { return nil }
func (s *readOnlyRejectStorage) Close() error          { return nil }

func TestInspectStorageReadOnlyAndReportsMetadata(t *testing.T) {
	format, _ := formatFixture(t)
	storage := &crashStorage{}
	if err := Format(t.Context(), format, FormatDependencies{Storage: storage}); err != nil {
		t.Fatal(err)
	}
	// Format writes superblockWriteCopies (3 of 4); openSuperblockStore would repair
	// the remaining copy, so inspecting directly verifies InspectStorage stays read-only.
	ro := &readOnlyRejectStorage{inner: storage}
	info, err := InspectStorage(ro, InspectConfig{
		Group:      format.Group,
		Membership: format.Membership,
		Cluster:    format.Cluster,
	})
	if err != nil {
		t.Fatalf("InspectStorage: %v", err)
	}
	if ro.wrote {
		t.Fatal("InspectStorage attempted to mutate storage")
	}
	if info.FormatVersion != protocol.FormatVersion || info.ProtocolVersion != protocol.ProtocolVersion {
		t.Fatalf("versions = (%d, %d), want (%d, %d)", info.FormatVersion, info.ProtocolVersion, protocol.FormatVersion, protocol.ProtocolVersion)
	}
	if info.ConfigurationChecksum != format.Cluster.Fingerprint() {
		t.Fatalf("fingerprint = %x, want %x", info.ConfigurationChecksum, format.Cluster.Fingerprint())
	}
	if info.Sequence != 1 || info.Release != format.CurrentRelease || info.CheckpointRelease != format.CurrentRelease {
		t.Fatalf("unexpected sequence/release: %+v", info)
	}
	writeQuorum, _ := superblockWriteCopies(format.Cluster.SuperblockCopies)
	if info.ValidCopies != writeQuorum || info.TotalCopies != uint8(format.Cluster.SuperblockCopies) {
		t.Fatalf("copies = %d/%d, want %d/%d", info.ValidCopies, info.TotalCopies, writeQuorum, format.Cluster.SuperblockCopies)
	}

	// Wrong group must report ErrIncompatibleConfiguration.
	wrongGroup := format.Group
	wrongGroup[0] ^= 0x02
	if _, err := InspectStorage(ro, InspectConfig{
		Group:      wrongGroup,
		Membership: format.Membership,
		Cluster:    format.Cluster,
	}); !errors.Is(err, ErrIncompatibleConfiguration) {
		t.Fatalf("wrong group error = %v, want ErrIncompatibleConfiguration", err)
	}

	// Empty file must report ErrUnformatted.
	if _, err := InspectStorage(&crashStorage{}, InspectConfig{
		Group:      format.Group,
		Membership: format.Membership,
		Cluster:    format.Cluster,
	}); !errors.Is(err, ErrUnformatted) {
		t.Fatalf("empty storage error = %v, want ErrUnformatted", err)
	}
}
