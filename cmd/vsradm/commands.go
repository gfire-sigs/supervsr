package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
	"z.gfire.dev/supervsr/transport"
)

type inspectionOutput struct {
	FormatVersion         uint16   `json:"format_version"`
	ProtocolVersion       uint16   `json:"protocol_version"`
	Release               uint32   `json:"release"`
	Sequence              uint64   `json:"sequence"`
	Group                 string   `json:"group"`
	ConfigurationChecksum string   `json:"configuration_checksum"`
	LocalMember           string   `json:"local_member"`
	ActiveCount           uint8    `json:"active_count"`
	StandbyCount          uint8    `json:"standby_count"`
	Members               []string `json:"members"`
	View                  uint32   `json:"view"`
	LogView               uint32   `json:"log_view"`
	CommitMax             uint64   `json:"commit_max"`
	SyncMin               uint64   `json:"sync_min"`
	SyncMax               uint64   `json:"sync_max"`
	CheckpointOp          uint64   `json:"checkpoint_op"`
	CheckpointRelease     uint32   `json:"checkpoint_release"`
	CheckpointID          string   `json:"checkpoint_id"`
	ViewHeaderCount       uint32   `json:"view_header_count"`
	FileSize              uint64   `json:"file_size"`
	ValidCopies           uint8    `json:"valid_copies"`
	TotalCopies           uint8    `json:"total_copies"`
	MisdirectedCopies     uint8    `json:"misdirected_copies"`
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errUsage
	}
	switch args[0] {
	case "format":
		return runFormat(ctx, args[1:], stdout, stderr)
	case "inspect":
		return runInspect(args[1:], stdout, stderr)
	case "replace":
		return runReplace(ctx, args[1:], stdout, stderr)
	default:
		return errUsage
	}
}

func runFormat(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("format", flag.ContinueOnError)
	fs.SetOutput(stderr)
	manifestPath := fs.String("manifest", "", "path to cluster manifest JSON")
	filePath := fs.String("file", "", "path to empty replica storage file")
	member := fs.String("member", "", "replica index or 32-char hex member ID")
	confirm := fs.Bool("confirm-new-cluster", false, "confirm formatting a brand-new cluster file (never use on a lost replica)")
	directIO := fs.Bool("direct-io", false, "enable direct I/O on the replica file")
	asJSON := fs.Bool("json", false, "emit JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*confirm {
		return errFormatConfirmation
	}
	if *filePath == "" {
		return errInvalidManifest
	}
	resolved, err := loadManifest(*manifestPath, *member)
	if err != nil {
		return err
	}
	if err := ensureFileEmptyOrMissing(*filePath); err != nil {
		return err
	}
	storage, err := replication.OpenFileStorage(*filePath, replication.FileStorageOptions{
		Create:   true,
		DirectIO: *directIO,
	})
	if err != nil {
		return err
	}
	defer func() { _ = storage.Close() }()

	if err := replication.Format(ctx, replication.FormatConfig{
		Group:          resolved.Group,
		Membership:     resolved.Membership,
		Cluster:        resolved.Cluster,
		CurrentRelease: resolved.Release,
	}, replication.FormatDependencies{Storage: storage}); err != nil {
		return err
	}
	info, err := replication.InspectStorage(storage, replication.InspectConfig{
		Group:      resolved.Group,
		Membership: resolved.Membership,
		Cluster:    resolved.Cluster,
	})
	if err != nil {
		return err
	}
	return writeInspection(stdout, info, *asJSON)
}

func runInspect(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	manifestPath := fs.String("manifest", "", "path to cluster manifest JSON")
	filePath := fs.String("file", "", "path to replica storage file")
	member := fs.String("member", "", "replica index or 32-char hex member ID")
	asJSON := fs.Bool("json", false, "emit JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *filePath == "" {
		return errInvalidManifest
	}
	resolved, err := loadManifest(*manifestPath, *member)
	if err != nil {
		return err
	}
	storage, err := openReadOnlyFileStorage(*filePath)
	if err != nil {
		return err
	}
	defer func() { _ = storage.Close() }()

	info, err := replication.InspectStorage(storage, replication.InspectConfig{
		Group:      resolved.Group,
		Membership: resolved.Membership,
		Cluster:    resolved.Cluster,
	})
	if err != nil {
		return err
	}
	return writeInspection(stdout, info, *asJSON)
}

func runReplace(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("replace", flag.ContinueOnError)
	fs.SetOutput(stderr)
	manifestPath := fs.String("manifest", "", "path to cluster manifest JSON")
	filePath := fs.String("file", "", "path to empty replacement replica storage file")
	member := fs.String("member", "", "lost replica index or 32-char hex member ID")
	fencePath := fs.String("fence", "", "path to signed/operator fence attestation JSON")
	timeout := fs.Duration("timeout", 30*time.Second, "maximum duration to contact surviving cluster and format replacement")
	directIO := fs.Bool("direct-io", false, "enable direct I/O on the replacement storage file")
	asJSON := fs.Bool("json", false, "emit JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *filePath == "" {
		return errInvalidManifest
	}
	resolved, err := loadManifest(*manifestPath, *member)
	if err != nil {
		return err
	}
	if !hasSurvivingEndpoints(resolved) {
		return errMissingPeerAddrs
	}
	if err := ensureFileEmptyOrMissing(*filePath); err != nil {
		return err
	}

	opCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	// Verify fencing before allocating storage or contacting surviving peers.
	fence := fileReplacementFence{path: *fencePath}
	fingerprint := resolved.Cluster.Fingerprint()
	if err := fence.VerifyReplacementFence(opCtx, replication.ReplacementFenceInput{
		Group:                 resolved.Group,
		Member:                resolved.Membership.LocalMember,
		ConfigurationChecksum: fingerprint,
	}); err != nil {
		return errors.Join(replication.ErrReplacementNotFenced, err)
	}

	storage, err := replication.OpenFileStorage(*filePath, replication.FileStorageOptions{
		Create:   true,
		DirectIO: *directIO,
	})
	if err != nil {
		return err
	}
	defer func() { _ = storage.Close() }()

	var clientID protocol.ClientID
	if _, err := io.ReadFull(rand.Reader, clientID[:]); err != nil {
		return err
	}
	if clientID.IsZero() {
		clientID[0] = 1
	}

	var managedPtr atomic.Pointer[replication.ManagedClient]
	clientTr, err := transport.NewClientTransport(transport.ClientConfig{
		Group:             resolved.Group,
		ClientID:          clientID,
		ActiveCount:       resolved.Membership.ActiveCount,
		Members:           resolved.Membership.Members,
		PeerAddrs:         resolved.PeerAddrs,
		MessageSizeMax:    uint32(resolved.Cluster.MessageSizeMax),
		ChecksumAlgorithm: resolved.Cluster.ChecksumAlgorithm,
		BackoffMin:        20 * time.Millisecond,
		BackoffMax:        250 * time.Millisecond,
		OnFrame: func(from protocol.ReplicaIndex, raw []byte) {
			if client := managedPtr.Load(); client != nil {
				client.HandleFrame(from, raw)
			}
		},
	})
	if err != nil {
		return err
	}

	process := replication.DefaultProcessConfig()
	process.Tick = 10 * time.Millisecond
	process.BackoffMax = time.Second
	managed, err := replication.NewManagedClient(replication.ClientConfig{
		Group:             resolved.Group,
		ID:                clientID,
		Release:           resolved.Release,
		ActiveCount:       resolved.Membership.ActiveCount,
		MessageSizeMax:    uint32(resolved.Cluster.MessageSizeMax),
		ChecksumAlgorithm: resolved.Cluster.ChecksumAlgorithm,
		Process:           process,
	}, clientTr, systemClock{start: time.Now().Add(-time.Second)}, rand.Reader)
	if err != nil {
		return err
	}
	managedPtr.Store(managed)
	if err := clientTr.Start(opCtx); err != nil {
		_ = managed.Close()
		return err
	}
	defer func() {
		_ = managed.Close()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()
		_ = clientTr.Close(closeCtx)
	}()

	if err := replication.ReplaceLostReplica(opCtx, replication.ReplacementConfig{
		Group:                 resolved.Group,
		Membership:            resolved.Membership,
		Cluster:               resolved.Cluster,
		CurrentRelease:        resolved.Release,
		ConfigurationChecksum: fingerprint,
	}, replication.ReplacementDependencies{
		Storage: storage,
		Client:  managedReplacementClient{client: managed},
		Fence:   fence,
	}); err != nil {
		return err
	}

	info, err := replication.InspectStorage(storage, replication.InspectConfig{
		Group:      resolved.Group,
		Membership: resolved.Membership,
		Cluster:    resolved.Cluster,
	})
	if err != nil {
		return err
	}
	return writeInspection(stdout, info, *asJSON)
}

func hasSurvivingEndpoints(resolved resolvedManifest) bool {
	count := 0
	for index := range resolved.Membership.ActiveCount {
		if protocol.ReplicaIndex(index) == resolved.LocalIndex {
			continue
		}
		if resolved.PeerAddrs[index] != "" {
			count++
		}
	}
	return count > 0
}

func ensureFileEmptyOrMissing(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Size() != 0 {
		return replication.ErrStorageNotEmpty
	}
	return nil
}

func writeInspection(out io.Writer, info replication.StorageInspection, asJSON bool) error {
	total := int(info.ActiveCount) + int(info.StandbyCount)
	members := make([]string, total)
	for i := range total {
		members[i] = hex.EncodeToString(info.Members[i][:])
	}
	payload := inspectionOutput{
		FormatVersion:         info.FormatVersion,
		ProtocolVersion:       info.ProtocolVersion,
		Release:               uint32(info.Release),
		Sequence:              info.Sequence,
		Group:                 hex.EncodeToString(info.Group[:]),
		ConfigurationChecksum: info.ConfigurationChecksum.String(),
		LocalMember:           hex.EncodeToString(info.LocalMember[:]),
		ActiveCount:           info.ActiveCount,
		StandbyCount:          info.StandbyCount,
		Members:               members,
		View:                  uint32(info.View),
		LogView:               uint32(info.LogView),
		CommitMax:             uint64(info.CommitMax),
		SyncMin:               uint64(info.SyncMin),
		SyncMax:               uint64(info.SyncMax),
		CheckpointOp:          uint64(info.CheckpointOp),
		CheckpointRelease:     uint32(info.CheckpointRelease),
		CheckpointID:          hex.EncodeToString(info.CheckpointID[:]),
		ViewHeaderCount:       info.ViewHeaderCount,
		FileSize:              info.FileSize,
		ValidCopies:           info.ValidCopies,
		TotalCopies:           info.TotalCopies,
		MisdirectedCopies:     info.MisdirectedCopies,
	}
	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(payload)
	}
	_, err := fmt.Fprintf(out,
		"format_version=%d protocol_version=%d release=%d sequence=%d\n"+
			"group=%s configuration_checksum=%s\n"+
			"local_member=%s active=%d standby=%d\n"+
			"view=%d log_view=%d commit_max=%d sync=%d..%d\n"+
			"checkpoint_op=%d checkpoint_release=%d checkpoint_id=%s\n"+
			"copies=%d/%d misdirected=%d file_size=%d\n",
		payload.FormatVersion, payload.ProtocolVersion, payload.Release, payload.Sequence,
		payload.Group, payload.ConfigurationChecksum,
		payload.LocalMember, payload.ActiveCount, payload.StandbyCount,
		payload.View, payload.LogView, payload.CommitMax, payload.SyncMin, payload.SyncMax,
		payload.CheckpointOp, payload.CheckpointRelease, payload.CheckpointID,
		payload.ValidCopies, payload.TotalCopies, payload.MisdirectedCopies, payload.FileSize,
	)
	return err
}
