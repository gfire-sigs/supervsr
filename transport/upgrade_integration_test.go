package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
)

type releaseNodeConfig struct {
	Group        string   `json:"group"`
	ActiveCount  uint8    `json:"active_count"`
	Members      []string `json:"members"`
	LocalIndex   uint8    `json:"local_index"`
	PeerAddrs    []string `json:"peer_addrs"`
	File         string   `json:"file"`
	JournalSlots uint64   `json:"journal_slots"`
	PipelineMax  uint64   `json:"pipeline_max"`
	Compaction   uint64   `json:"compaction_ops"`
	MessageMax   uint64   `json:"message_size_max"`
	BlockSize    uint64   `json:"block_size"`
	ClientsMax   uint64   `json:"clients_max"`
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestUpgradeHandoffAcrossReleaseBinaries(t *testing.T) {
	dir := t.TempDir()
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	binR1 := filepath.Join(dir, "releasenode-r1"+ext)
	binR2 := filepath.Join(dir, "releasenode-r2"+ext)

	buildBinary := func(output, release string) {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "go", "build", "-ldflags", "-X main.compiledRelease="+release, "-o", output, "./internal/releasenode")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build release %s binary: %v\n%s", release, err, string(out))
		}
	}
	buildBinary(binR1, "1")
	buildBinary(binR2, "2")

	// Reserve three ephemeral loopback ports and immediately release them for the child binaries.
	var addrs [3]string
	for i := range 3 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = ln.Addr().String()
		_ = ln.Close()
	}

	group := protocol.GroupID{0x55, 0x19, 0x02}
	membership0 := testClusterMembership(0)
	cluster := replication.DefaultClusterConfig()
	cluster.ClientsMax = 4
	cluster.PipelineMax = 2
	cluster.ViewChangeHeadersSuffixMax = 3
	cluster.JournalSlots = 32
	cluster.CompactionOps = 4
	cluster.MessageSizeMax = 8192
	cluster.ApplicationBatchSizeMax = 1024
	cluster.ApplicationReplySizeMax = 1024
	cluster.BlockSize = 4096

	var files [3]string
	var cfgPaths [3]string
	var handoffMarkers [3]string
	var readyPrefixes [3]string
	memberHex := []string{
		hex.EncodeToString(membership0.Members[0][:]),
		hex.EncodeToString(membership0.Members[1][:]),
		hex.EncodeToString(membership0.Members[2][:]),
	}

	for i := range 3 {
		files[i] = filepath.Join(dir, fmt.Sprintf("node-%d.vsr", i))
		membership := testClusterMembership(i)
		storage, err := replication.OpenFileStorage(files[i], replication.FileStorageOptions{Create: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := replication.Format(t.Context(), replication.FormatConfig{
			Group:          group,
			Membership:     membership,
			Cluster:        cluster,
			CurrentRelease: 1,
		}, replication.FormatDependencies{Storage: storage}); err != nil {
			_ = storage.Close()
			t.Fatal(err)
		}
		if err := storage.Close(); err != nil {
			t.Fatal(err)
		}

		cfgPaths[i] = filepath.Join(dir, fmt.Sprintf("node-%d.json", i))
		raw, err := json.Marshal(releaseNodeConfig{
			Group:        hex.EncodeToString(group[:]),
			ActiveCount:  3,
			Members:      memberHex,
			LocalIndex:   uint8(i),
			PeerAddrs:    addrs[:],
			File:         files[i],
			JournalSlots: cluster.JournalSlots,
			PipelineMax:  cluster.PipelineMax,
			Compaction:   cluster.CompactionOps,
			MessageMax:   cluster.MessageSizeMax,
			BlockSize:    cluster.BlockSize,
			ClientsMax:   cluster.ClientsMax,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cfgPaths[i], raw, 0o600); err != nil {
			t.Fatal(err)
		}
		handoffMarkers[i] = filepath.Join(dir, fmt.Sprintf("handoff-%d.json", i))
		readyPrefixes[i] = filepath.Join(dir, fmt.Sprintf("ready-%d", i))
	}

	// Launch all three Release 1 binaries advertising [1, 2] with Release 2 handoff target.
	var cmds [3]*exec.Cmd
	var stdins [3]io.WriteCloser
	var stderrs [3]syncBuffer
	for i := range 3 {
		cmd := exec.Command(
			binR1,
			"-config", cfgPaths[i],
			"-advertise", "1,2",
			"-handoff-bin", binR2,
			"-handoff-file", handoffMarkers[i],
			"-ready-prefix", readyPrefixes[i],
		)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = &stderrs[i]
		if err := cmd.Start(); err != nil {
			t.Fatalf("start node %d (r1): %v", i, err)
		}
		cmds[i] = cmd
		stdins[i] = stdin
	}

	stopCluster := func() {
		for i := range 3 {
			if stdins[i] != nil {
				_ = stdins[i].Close()
				stdins[i] = nil
			}
		}
		for i := range 3 {
			if cmds[i] == nil {
				continue
			}
			done := make(chan struct{})
			go func(c *exec.Cmd) {
				_ = c.Wait()
				close(done)
			}(cmds[i])
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = cmds[i].Process.Kill()
				<-done
			}
			cmds[i] = nil
		}
	}
	defer stopCluster()

	// Wait for all three processes to complete OperationUpgrade -> ReleaseExecutor.Execute(2) -> Release 2 startup.
	deadline := time.NewTimer(12 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready := true
		for i := range 3 {
			if _, err := os.Stat(handoffMarkers[i]); err != nil {
				ready = false
				break
			}
			if _, err := os.Stat(readyPrefixes[i] + "-r2.json"); err != nil {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for 3-node release 1->2 handoff; stderrs: [0]=%q [1]=%q [2]=%q", stderrs[0].String(), stderrs[1].String(), stderrs[2].String())
		case <-ticker.C:
		}
	}

	// Verify the Release 2 cluster accepts a Release 2 client and commits with the Release 2 binary prefix.
	clientID := protocol.ClientID{0x42}
	var peerAddrs [replication.MembersMax]string
	copy(peerAddrs[:3], addrs[:])
	var managedPtr atomic.Pointer[replication.ManagedClient]
	clientTr, err := NewClientTransport(ClientConfig{
		Group:             group,
		ClientID:          clientID,
		ActiveCount:       3,
		Members:           membership0.Members,
		PeerAddrs:         peerAddrs,
		MessageSizeMax:    uint32(cluster.MessageSizeMax),
		ChecksumAlgorithm: cluster.ChecksumAlgorithm,
		BackoffMin:        10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
		OnFrame: func(from protocol.ReplicaIndex, raw []byte) {
			if client := managedPtr.Load(); client != nil {
				client.HandleFrame(from, raw)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	process := replication.DefaultProcessConfig()
	process.Tick = 5 * time.Millisecond
	process.BackoffMax = 250 * time.Millisecond
	managed, err := replication.NewManagedClient(replication.ClientConfig{
		Group:             group,
		ID:                clientID,
		Release:           2,
		ActiveCount:       3,
		MessageSizeMax:    uint32(cluster.MessageSizeMax),
		ChecksumAlgorithm: cluster.ChecksumAlgorithm,
		Process:           process,
	}, clientTr, testClock{start: time.Now().Add(-time.Second)}, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	managedPtr.Store(managed)
	if err := clientTr.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		managed.Close()
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = clientTr.Close(closeCtx)
	}()

	opCtx, cancelOp := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancelOp()
	if err := managed.Register(opCtx); err != nil {
		t.Fatalf("post-upgrade Register failed: %v", err)
	}
	reply, err := managed.Submit(opCtx, protocol.OperationApplicationMin, []byte("upgraded"))
	if err != nil {
		t.Fatalf("post-upgrade Submit failed: %v", err)
	}
	if string(reply.Body) != "r2:upgraded" {
		t.Fatalf("post-upgrade reply = %q, want %q", string(reply.Body), "r2:upgraded")
	}

	// Shut down all Release 2 nodes before testing rollback rejection on disk.
	stopCluster()

	storageBefore, err := replication.OpenFileStorage(files[0], replication.FileStorageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before, err := replication.InspectStorage(storageBefore, replication.InspectConfig{
		Group:      group,
		Membership: testClusterMembership(0),
		Cluster:    cluster,
	})
	_ = storageBefore.Close()
	if err != nil {
		t.Fatalf("inspect upgraded storage: %v", err)
	}
	if before.CheckpointRelease != 2 {
		t.Fatalf("upgraded checkpoint release = %d, want 2", before.CheckpointRelease)
	}

	// Rollback rejection: launching Release 1 binary without a Release 2 executor must fail with ErrReleaseUnavailable and leave storage unchanged.
	rollbackCmd := exec.CommandContext(t.Context(), binR1, "-config", cfgPaths[0], "-advertise", "")
	rollbackOut, rollbackErr := rollbackCmd.CombinedOutput()
	if rollbackErr == nil {
		t.Fatal("expected Release 1 binary to reject Release 2 storage file")
	}
	if !strings.Contains(string(rollbackOut), replication.ErrReleaseUnavailable.Error()) {
		t.Fatalf("rollback output = %q, want %q", string(rollbackOut), replication.ErrReleaseUnavailable.Error())
	}

	storageAfter, err := replication.OpenFileStorage(files[0], replication.FileStorageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	after, err := replication.InspectStorage(storageAfter, replication.InspectConfig{
		Group:      group,
		Membership: testClusterMembership(0),
		Cluster:    cluster,
	})
	_ = storageAfter.Close()
	if err != nil {
		t.Fatalf("inspect storage after rejected rollback: %v", err)
	}
	if after.CheckpointRelease != 2 || after.Sequence != before.Sequence {
		t.Fatalf("rollback attempt mutated storage: before=%+v after=%+v", before, after)
	}
}
