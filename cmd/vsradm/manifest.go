package main

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
)

var (
	errUsage              = errors.New("usage: vsradm <format|inspect|replace> [flags]")
	errFormatConfirmation = errors.New("format refused: pass -confirm-new-cluster for initial cluster creation; use 'replace' for a lost replica")
	errInvalidManifest    = errors.New("invalid cluster manifest")
	errReadOnlyStorage    = errors.New("read-only storage refuses mutations")
	errFenceInvalid       = errors.New("fence attestation missing or mismatched")
	errMissingPeerAddrs   = errors.New("manifest peer_addrs must include active cluster endpoints for replacement")
)

type clusterManifest struct {
	Group        string              `json:"group"`
	ActiveCount  uint8               `json:"active_count"`
	StandbyCount uint8               `json:"standby_count"`
	Members      []string            `json:"members"`
	PeerAddrs    []string            `json:"peer_addrs,omitempty"`
	Release      uint32              `json:"release,omitempty"`
	Cluster      *clusterConfigPatch `json:"cluster,omitempty"`
}

type clusterConfigPatch struct {
	ClientsMax                      uint64 `json:"clients_max,omitempty"`
	PipelineMax                     uint64 `json:"pipeline_max,omitempty"`
	ViewChangeHeadersSuffixMax      uint64 `json:"view_change_headers_suffix_max,omitempty"`
	ReplicationQuorumMax            uint64 `json:"replication_quorum_max,omitempty"`
	JournalSlots                    uint64 `json:"journal_slots,omitempty"`
	MessageSizeMax                  uint64 `json:"message_size_max,omitempty"`
	ApplicationBatchSizeMax         uint64 `json:"application_batch_size_max,omitempty"`
	ApplicationReplySizeMax         uint64 `json:"application_reply_size_max,omitempty"`
	SuperblockCopies                uint64 `json:"superblock_copies,omitempty"`
	BlockSize                       uint64 `json:"block_size,omitempty"`
	StorageLevels                   uint64 `json:"storage_levels,omitempty"`
	StorageGrowthFactor             uint64 `json:"storage_growth_factor,omitempty"`
	CompactionOps                   uint64 `json:"compaction_ops,omitempty"`
	StorageSnapshotsMax             uint64 `json:"storage_snapshots_max,omitempty"`
	ManifestCompactExtraBlocks      uint64 `json:"manifest_compact_extra_blocks,omitempty"`
	TableCoalescingThresholdPercent uint64 `json:"table_coalescing_threshold_percent,omitempty"`
	ReleaseHistoryMax               uint64 `json:"release_history_max,omitempty"`
	StorageScansMax                 uint64 `json:"storage_scans_max,omitempty"`
	ChecksumAlgorithm               string `json:"checksum_algorithm,omitempty"`
}

type resolvedManifest struct {
	Group      protocol.GroupID
	Membership replication.Membership
	Cluster    replication.ClusterConfig
	Release    protocol.Release
	PeerAddrs  [replication.MembersMax]string
	LocalIndex protocol.ReplicaIndex
}

func loadManifest(path, memberSelector string) (resolvedManifest, error) {
	if path == "" || memberSelector == "" {
		return resolvedManifest{}, errInvalidManifest
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return resolvedManifest{}, fmt.Errorf("%w: %w", errInvalidManifest, err)
	}
	var manifest clusterManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return resolvedManifest{}, fmt.Errorf("%w: %w", errInvalidManifest, err)
	}
	group, err := decodeHex16(manifest.Group)
	if err != nil {
		return resolvedManifest{}, fmt.Errorf("%w: group: %w", errInvalidManifest, err)
	}
	totalMembers := int(manifest.ActiveCount) + int(manifest.StandbyCount)
	if totalMembers == 0 || totalMembers > replication.MembersMax || len(manifest.Members) != totalMembers {
		return resolvedManifest{}, fmt.Errorf("%w: member count mismatch", errInvalidManifest)
	}
	var membership replication.Membership
	membership.ActiveCount = manifest.ActiveCount
	membership.StandbyCount = manifest.StandbyCount
	for index, encoded := range manifest.Members {
		id, decErr := decodeHex16(encoded)
		if decErr != nil {
			return resolvedManifest{}, fmt.Errorf("%w: member[%d]: %w", errInvalidManifest, index, decErr)
		}
		membership.Members[index] = protocol.MemberID(id)
	}
	localIndex, err := selectMember(membership, totalMembers, memberSelector)
	if err != nil {
		return resolvedManifest{}, err
	}
	membership.LocalMember = membership.Members[localIndex]
	if err := membership.Validate(); err != nil {
		return resolvedManifest{}, fmt.Errorf("%w: %w", errInvalidManifest, err)
	}
	cluster, err := buildClusterConfig(manifest.Cluster)
	if err != nil {
		return resolvedManifest{}, err
	}
	if err := cluster.Validate(membership.ActiveCount, membership.StandbyCount); err != nil {
		return resolvedManifest{}, fmt.Errorf("%w: %w", errInvalidManifest, err)
	}
	release := cmp.Or(protocol.Release(manifest.Release), protocol.Release(1))
	var peerAddrs [replication.MembersMax]string
	for index := 0; index < len(manifest.PeerAddrs) && index < replication.MembersMax; index++ {
		peerAddrs[index] = strings.TrimSpace(manifest.PeerAddrs[index])
	}
	return resolvedManifest{
		Group:      protocol.GroupID(group),
		Membership: membership,
		Cluster:    cluster,
		Release:    release,
		PeerAddrs:  peerAddrs,
		LocalIndex: localIndex,
	}, nil
}

func selectMember(membership replication.Membership, total int, selector string) (protocol.ReplicaIndex, error) {
	if idx, err := strconv.Atoi(selector); err == nil {
		if idx < 0 || idx >= total {
			return 0, fmt.Errorf("%w: member index %d out of range", errInvalidManifest, idx)
		}
		return protocol.ReplicaIndex(idx), nil
	}
	idBytes, err := decodeHex16(selector)
	if err != nil {
		return 0, fmt.Errorf("%w: member %q: %w", errInvalidManifest, selector, err)
	}
	target := protocol.MemberID(idBytes)
	for index := range total {
		if membership.Members[index] == target {
			return protocol.ReplicaIndex(index), nil
		}
	}
	return 0, fmt.Errorf("%w: member %s not in manifest", errInvalidManifest, selector)
}

func buildClusterConfig(patch *clusterConfigPatch) (replication.ClusterConfig, error) {
	cfg := replication.DefaultClusterConfig()
	if patch == nil {
		return cfg, nil
	}
	overridePositive(&cfg.ClientsMax, patch.ClientsMax)
	overridePositive(&cfg.PipelineMax, patch.PipelineMax)
	if patch.ViewChangeHeadersSuffixMax != 0 {
		cfg.ViewChangeHeadersSuffixMax = patch.ViewChangeHeadersSuffixMax
	} else if patch.PipelineMax != 0 {
		cfg.ViewChangeHeadersSuffixMax = cfg.PipelineMax + 1
	}
	overridePositive(&cfg.ReplicationQuorumMax, patch.ReplicationQuorumMax)
	overridePositive(&cfg.JournalSlots, patch.JournalSlots)
	overridePositive(&cfg.MessageSizeMax, patch.MessageSizeMax)
	overridePositive(&cfg.ApplicationBatchSizeMax, patch.ApplicationBatchSizeMax)
	overridePositive(&cfg.ApplicationReplySizeMax, patch.ApplicationReplySizeMax)
	overridePositive(&cfg.SuperblockCopies, patch.SuperblockCopies)
	overridePositive(&cfg.BlockSize, patch.BlockSize)
	overridePositive(&cfg.StorageLevels, patch.StorageLevels)
	overridePositive(&cfg.StorageGrowthFactor, patch.StorageGrowthFactor)
	overridePositive(&cfg.CompactionOps, patch.CompactionOps)
	overridePositive(&cfg.StorageSnapshotsMax, patch.StorageSnapshotsMax)
	overridePositive(&cfg.ManifestCompactExtraBlocks, patch.ManifestCompactExtraBlocks)
	overridePositive(&cfg.TableCoalescingThresholdPercent, patch.TableCoalescingThresholdPercent)
	overridePositive(&cfg.ReleaseHistoryMax, patch.ReleaseHistoryMax)
	overridePositive(&cfg.StorageScansMax, patch.StorageScansMax)

	if patch.ChecksumAlgorithm != "" {
		alg, err := parseChecksumAlgorithm(patch.ChecksumAlgorithm)
		if err != nil {
			return replication.ClusterConfig{}, err
		}
		cfg.ChecksumAlgorithm = alg
	}
	return cfg, nil
}

func overridePositive(dst *uint64, value uint64) {
	if value != 0 {
		*dst = value
	}
}

func parseChecksumAlgorithm(name string) (protocol.ChecksumAlgorithm, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "aegis", "aegis128l":
		return protocol.ChecksumAEGIS128L, nil
	case "wyhash", "wyhash128":
		return protocol.ChecksumWyhash128, nil
	default:
		return 0, fmt.Errorf("%w: unknown checksum_algorithm %q", errInvalidManifest, name)
	}
}

func decodeHex16(encoded string) ([16]byte, error) {
	var out [16]byte
	raw, err := hex.DecodeString(strings.TrimSpace(encoded))
	if err != nil || len(raw) != len(out) {
		return out, fmt.Errorf("%w: expected 32 hex chars", errInvalidManifest)
	}
	copy(out[:], raw)
	return out, nil
}

// fenceAttestation records operator-verified fencing bound to one immutable
// (group, member, configuration_checksum) tuple.
type fenceAttestation struct {
	Group                 string `json:"group"`
	Member                string `json:"member"`
	ConfigurationChecksum string `json:"configuration_checksum"`
	Fenced                bool   `json:"fenced"`
	Reason                string `json:"reason"`
}

type fileReplacementFence struct {
	path string
}

func (f fileReplacementFence) VerifyReplacementFence(_ context.Context, input replication.ReplacementFenceInput) error {
	if f.path == "" {
		return fmt.Errorf("%w: -fence attestation file is required", errFenceInvalid)
	}
	raw, err := os.ReadFile(f.path)
	if err != nil {
		return fmt.Errorf("%w: read fence attestation: %w", errFenceInvalid, err)
	}
	var att fenceAttestation
	if err := json.Unmarshal(raw, &att); err != nil {
		return fmt.Errorf("%w: parse fence attestation: %w", errFenceInvalid, err)
	}
	if !att.Fenced || strings.TrimSpace(att.Reason) == "" {
		return fmt.Errorf("%w: attestation must set fenced=true with a non-empty reason", errFenceInvalid)
	}
	group, err := decodeHex16(att.Group)
	if err != nil || protocol.GroupID(group) != input.Group {
		return fmt.Errorf("%w: group mismatch", errFenceInvalid)
	}
	member, err := decodeHex16(att.Member)
	if err != nil || protocol.MemberID(member) != input.Member {
		return fmt.Errorf("%w: member mismatch", errFenceInvalid)
	}
	checksum, err := decodeHex16(att.ConfigurationChecksum)
	if err != nil || protocol.Checksum(checksum) != input.ConfigurationChecksum {
		return fmt.Errorf("%w: configuration_checksum mismatch", errFenceInvalid)
	}
	return nil
}

type readOnlyFileStorage struct {
	file *os.File
}

func openReadOnlyFileStorage(path string) (*readOnlyFileStorage, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return &readOnlyFileStorage{file: file}, nil
}

func (s *readOnlyFileStorage) ReadAt(buffer []byte, offset uint64) error {
	n, err := s.file.ReadAt(buffer, int64(offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n != len(buffer) {
		return replication.ErrShortIO
	}
	return nil
}

func (*readOnlyFileStorage) WriteAt([]byte, uint64) error { return errReadOnlyStorage }
func (*readOnlyFileStorage) Sync() error                  { return errReadOnlyStorage }
func (*readOnlyFileStorage) Resize(uint64) error          { return errReadOnlyStorage }
func (*readOnlyFileStorage) SyncParent() error            { return nil }

func (s *readOnlyFileStorage) Size() (uint64, error) {
	info, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()), nil
}

func (s *readOnlyFileStorage) Close() error {
	return s.file.Close()
}

type managedReplacementClient struct {
	client *replication.ManagedClient
}

func (c managedReplacementClient) Register(ctx context.Context) (replication.ReplacementReply, error) {
	if err := c.client.Register(ctx); err != nil {
		return replication.ReplacementReply{}, err
	}
	return replication.ReplacementReply{
		View:      c.client.View(),
		Operation: protocol.OperationRegister,
		Request:   0,
	}, nil
}

func (c managedReplacementClient) Noop(ctx context.Context) (replication.ReplacementReply, error) {
	reply, err := c.client.Submit(ctx, protocol.OperationNoop, nil)
	if err != nil {
		return replication.ReplacementReply{}, err
	}
	return replication.ReplacementReply{
		View:      c.client.View(),
		Operation: reply.Operation,
		Request:   reply.Request,
	}, nil
}

type systemClock struct {
	start time.Time
}

func (c systemClock) Now() replication.TimeSample {
	now := time.Now()
	mono := now.Sub(c.start)
	if mono <= 0 {
		mono = 1
	}
	return replication.TimeSample{
		Wall:         uint64(now.UnixNano()),
		Monotonic:    uint64(mono),
		Synchronized: true,
	}
}
