package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
	"z.gfire.dev/supervsr/transport"
)

func writeTestManifest(t *testing.T, dir string, group protocol.GroupID, members [3]protocol.MemberID, addrs [3]string) (string, replication.ClusterConfig) {
	t.Helper()
	patch := &clusterConfigPatch{
		ClientsMax:                 4,
		PipelineMax:                4,
		ViewChangeHeadersSuffixMax: 5,
		JournalSlots:               256,
		MessageSizeMax:             8192,
		ApplicationBatchSizeMax:    1024,
		ApplicationReplySizeMax:    1024,
		BlockSize:                  4096,
		CompactionOps:              16,
		ChecksumAlgorithm:          "aegis128l",
	}
	cluster, err := buildClusterConfig(patch)
	if err != nil {
		t.Fatal(err)
	}
	manifest := clusterManifest{
		Group:       hex.EncodeToString(group[:]),
		ActiveCount: 3,
		Members: []string{
			hex.EncodeToString(members[0][:]),
			hex.EncodeToString(members[1][:]),
			hex.EncodeToString(members[2][:]),
		},
		PeerAddrs: []string{addrs[0], addrs[1], addrs[2]},
		Release:   1,
		Cluster:   patch,
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return manifestPath, cluster
}

func TestFormatRefusesWithoutConfirmationAndRefusesNonEmptyFile(t *testing.T) {
	dir := t.TempDir()
	group := protocol.GroupID{1, 2, 3}
	members := [3]protocol.MemberID{{1}, {2}, {3}}
	manifestPath, cluster := writeTestManifest(t, dir, group, members, [3]string{})
	dataFile := filepath.Join(dir, "node0.vsr")

	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{"format", "-manifest", manifestPath, "-file", dataFile, "-member", "0"}, &stdout, &stderr)
	if !errors.Is(err, errFormatConfirmation) {
		t.Fatalf("format without confirmation error = %v, want errFormatConfirmation", err)
	}
	if _, statErr := os.Stat(dataFile); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("format created file without confirmation: %v", statErr)
	}

	// Valid format with confirmation succeeds and inspect matches fingerprint.
	stdout.Reset()
	if err := run(t.Context(), []string{"format", "-manifest", manifestPath, "-file", dataFile, "-member", "0", "-confirm-new-cluster", "-json"}, &stdout, &stderr); err != nil {
		t.Fatalf("format failed: %v", err)
	}
	var formatted inspectionOutput
	if err := json.Unmarshal(stdout.Bytes(), &formatted); err != nil {
		t.Fatalf("unmarshal format json: %v", err)
	}
	if formatted.ConfigurationChecksum != cluster.Fingerprint().String() {
		t.Fatalf("fingerprint = %s, want %s", formatted.ConfigurationChecksum, cluster.Fingerprint().String())
	}
	if formatted.Sequence != 1 || formatted.ValidCopies != 3 || formatted.TotalCopies != 4 {
		t.Fatalf("unexpected format inspection: %+v", formatted)
	}

	// Re-formatting an existing non-empty file must fail with ErrStorageNotEmpty.
	err = run(t.Context(), []string{"format", "-manifest", manifestPath, "-file", dataFile, "-member", "0", "-confirm-new-cluster"}, &stdout, &stderr)
	if !errors.Is(err, replication.ErrStorageNotEmpty) {
		t.Fatalf("re-format non-empty error = %v, want ErrStorageNotEmpty", err)
	}

	// Read-only inspect succeeds on formatted file and reports identical metadata.
	stdout.Reset()
	if err := run(t.Context(), []string{"inspect", "-manifest", manifestPath, "-file", dataFile, "-member", "0", "-json"}, &stdout, &stderr); err != nil {
		t.Fatalf("inspect failed: %v", err)
	}
	var inspected inspectionOutput
	if err := json.Unmarshal(stdout.Bytes(), &inspected); err != nil {
		t.Fatalf("unmarshal inspect json: %v", err)
	}
	if inspected.ConfigurationChecksum != formatted.ConfigurationChecksum || inspected.LocalMember != formatted.LocalMember {
		t.Fatalf("inspect mismatch: got %+v want %+v", inspected, formatted)
	}
}

type noopStateMachine struct {
	capacities replication.StateMachineCapacities
}

func (m *noopStateMachine) Capacities() replication.StateMachineCapacities { return m.capacities }
func (m *noopStateMachine) Validate(replication.ValidateInput) replication.ValidationResult {
	return replication.ValidationOK
}
func (m *noopStateMachine) PulseNeeded(uint64) bool { return false }
func (m *noopStateMachine) StartPrefetch(replication.PrefetchInput, *replication.SMCompletion) (replication.StartResult[replication.PrefetchToken], error) {
	return replication.Ready(replication.PrefetchToken(1)), nil
}
func (m *noopStateMachine) Commit(replication.CommitInput, replication.PrefetchToken, []byte) (int, error) {
	return 0, nil
}
func (m *noopStateMachine) StartCompact(replication.CompactInput, *replication.SMCompletion) (replication.StartResult[replication.CompactResult], error) {
	return replication.Ready(replication.CompactResult{}), nil
}
func (m *noopStateMachine) StartCheckpoint(replication.CheckpointInput, *replication.SMCompletion) (replication.StartResult[replication.CheckpointManifest], error) {
	return replication.Ready(replication.CheckpointManifest{}), nil
}
func (m *noopStateMachine) StartOpen(replication.OpenCheckpointInput, *replication.SMCompletion) (replication.StartResult[replication.OpenResult], error) {
	return replication.Ready(replication.OpenResult{}), nil
}
func (m *noopStateMachine) StartReset(*replication.SMCompletion) (replication.StartResult[replication.ResetResult], error) {
	return replication.Ready(replication.ResetResult{}), nil
}
func (m *noopStateMachine) Close() error { return nil }

func TestReplaceEnforcesFencingAndFormatsReplacementAgainstLiveCluster(t *testing.T) {
	dir := t.TempDir()
	group := protocol.GroupID{9, 8, 7}
	members := [3]protocol.MemberID{{1}, {2}, {3}}

	// Start only surviving replicas 0 and 1 (replica 2 is permanently lost).
	var listeners [2]net.Listener
	var addrs [3]string
	for i := range 2 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}
	addrs[2] = "127.0.0.1:1" // unreachable lost node address

	manifestPath, cluster := writeTestManifest(t, dir, group, members, addrs)
	var peerAddrs [replication.MembersMax]string
	copy(peerAddrs[:3], addrs[:])

	process := replication.DefaultProcessConfig()
	process.Tick = 5 * time.Millisecond
	clock := systemClock{start: time.Now().Add(-time.Second)}

	runCtx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	var replicas [2]*replication.Replica
	var transports [2]*transport.ReplicaTransport
	var wg sync.WaitGroup

	for i := range 2 {
		nodeFile := filepath.Join(dir, "node-"+string(rune('0'+i))+".vsr")
		var out, errOut bytes.Buffer
		if err := run(t.Context(), []string{"format", "-manifest", manifestPath, "-file", nodeFile, "-member", string(rune('0' + i)), "-confirm-new-cluster"}, &out, &errOut); err != nil {
			t.Fatalf("format node %d: %v", i, err)
		}
		storage, err := replication.OpenFileStorage(nodeFile, replication.FileStorageOptions{})
		if err != nil {
			t.Fatal(err)
		}
		membership := replication.Membership{
			ActiveCount: 3,
			Members:     [replication.MembersMax]protocol.MemberID{members[0], members[1], members[2]},
			LocalMember: members[i],
		}
		idx := i
		tr, err := transport.NewReplicaTransport(transport.ReplicaConfig{
			Group:             group,
			Membership:        membership,
			PeerAddrs:         peerAddrs,
			MessageSizeMax:    uint32(cluster.MessageSizeMax),
			ChecksumAlgorithm: cluster.ChecksumAlgorithm,
			BackoffMin:        10 * time.Millisecond,
			BackoffMax:        50 * time.Millisecond,
			OnFrame: func(frame *protocol.Frame) error {
				if replicas[idx] == nil {
					return transport.ErrTransportClosed
				}
				return replicas[idx].Submit(frame)
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		sm := &noopStateMachine{capacities: replication.StateMachineCapacities{
			RequestBytes:  uint32(cluster.ApplicationBatchSizeMax),
			ReplyBytes:    uint32(cluster.ApplicationReplySizeMax),
			PrefetchMax:   4,
			CheckpointMax: 4,
		}}
		replica, err := replication.Open(t.Context(), replication.Config{
			Group:            group,
			Membership:       membership,
			Cluster:          cluster,
			Process:          process,
			CurrentRelease:   1,
			ClientReleaseMin: 1,
		}, replication.Dependencies{
			Storage:      storage,
			MessageBus:   tr,
			Clock:        clock,
			Entropy:      rand.Reader,
			StateMachine: sm,
		})
		if err != nil {
			t.Fatal(err)
		}
		replicas[i] = replica
		transports[i] = tr
		if err := tr.StartWithListener(runCtx, listeners[i]); err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(r *replication.Replica) {
			defer wg.Done()
			_ = r.Run(runCtx)
		}(replica)
	}
	defer func() {
		cancelRun()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer closeCancel()
		for i := range 2 {
			_ = replicas[i].Close(closeCtx)
			_ = transports[i].Close(closeCtx)
		}
		wg.Wait()
	}()

	replacementFile := filepath.Join(dir, "node-2-replacement.vsr")
	var stdout, stderr bytes.Buffer

	// 1. Replace without fence attestation must fail with ErrReplacementNotFenced.
	err := run(t.Context(), []string{"replace", "-manifest", manifestPath, "-file", replacementFile, "-member", "2"}, &stdout, &stderr)
	if !errors.Is(err, replication.ErrReplacementNotFenced) {
		t.Fatalf("replace without fence error = %v, want ErrReplacementNotFenced", err)
	}
	if _, statErr := os.Stat(replacementFile); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("unfenced replace created storage file: %v", statErr)
	}

	// 2. Replace with fence bound to wrong member must fail with ErrReplacementNotFenced.
	badFence := filepath.Join(dir, "bad-fence.json")
	badRaw, _ := json.Marshal(fenceAttestation{
		Group:                 hex.EncodeToString(group[:]),
		Member:                hex.EncodeToString(members[1][:]),
		ConfigurationChecksum: cluster.Fingerprint().String(),
		Fenced:                true,
		Reason:                "wrong member",
	})
	if err := os.WriteFile(badFence, badRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	err = run(t.Context(), []string{"replace", "-manifest", manifestPath, "-file", replacementFile, "-member", "2", "-fence", badFence}, &stdout, &stderr)
	if !errors.Is(err, replication.ErrReplacementNotFenced) {
		t.Fatalf("replace with mismatched fence error = %v, want ErrReplacementNotFenced", err)
	}

	// 3. Replace with valid fence attestation succeeds against live surviving quorum and formats View >= 2.
	goodFence := filepath.Join(dir, "good-fence.json")
	goodRaw, _ := json.Marshal(fenceAttestation{
		Group:                 hex.EncodeToString(group[:]),
		Member:                hex.EncodeToString(members[2][:]),
		ConfigurationChecksum: cluster.Fingerprint().String(),
		Fenced:                true,
		Reason:                "host decommissioned and credentials revoked",
	})
	if err := os.WriteFile(goodFence, goodRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := run(t.Context(), []string{"replace", "-manifest", manifestPath, "-file", replacementFile, "-member", "2", "-fence", goodFence, "-timeout", "10s", "-json"}, &stdout, &stderr); err != nil {
		t.Fatalf("fenced replace failed: %v", err)
	}
	var replaced inspectionOutput
	if err := json.Unmarshal(stdout.Bytes(), &replaced); err != nil {
		t.Fatalf("unmarshal replace output: %v", err)
	}
	if replaced.View < 2 || replaced.LogView != 0 {
		t.Fatalf("replacement view/log_view = (%d, %d), want view >= 2 and log_view = 0", replaced.View, replaced.LogView)
	}
	if replaced.ConfigurationChecksum != cluster.Fingerprint().String() {
		t.Fatalf("replacement fingerprint = %s, want %s", replaced.ConfigurationChecksum, cluster.Fingerprint().String())
	}
	if replaced.LocalMember != hex.EncodeToString(members[2][:]) {
		t.Fatalf("replacement local_member = %s, want %s", replaced.LocalMember, hex.EncodeToString(members[2][:]))
	}
}
