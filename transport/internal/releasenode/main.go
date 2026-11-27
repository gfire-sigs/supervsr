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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
	"z.gfire.dev/supervsr/transport"
)

// compiledRelease is overridden via -ldflags "-X main.compiledRelease=2" to
// build distinct release binaries for heterogeneous upgrade handoff tests.
var compiledRelease = "1"

var (
	errInvalidRelease = errors.New("releasenode: invalid compiled release")
	errNoHandoffBin   = errors.New("releasenode: handoff binary not configured")
	errInvalidConfig  = errors.New("releasenode: invalid node config")
)

type nodeConfig struct {
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

type markerPayload struct {
	Release uint32 `json:"release"`
	From    uint32 `json:"from,omitempty"`
	To      uint32 `json:"to,omitempty"`
	PID     int    `json:"pid"`
}

type taggedStateMachine struct {
	capacities replication.StateMachineCapacities
	prefix     string
}

func (m *taggedStateMachine) Capacities() replication.StateMachineCapacities { return m.capacities }
func (m *taggedStateMachine) Validate(replication.ValidateInput) replication.ValidationResult {
	return replication.ValidationOK
}
func (m *taggedStateMachine) PulseNeeded(uint64) bool { return false }
func (m *taggedStateMachine) StartPrefetch(replication.PrefetchInput, *replication.SMCompletion) (replication.StartResult[replication.PrefetchToken], error) {
	return replication.Ready(replication.PrefetchToken(1)), nil
}
func (m *taggedStateMachine) Commit(input replication.CommitInput, _ replication.PrefetchToken, reply []byte) (int, error) {
	n := copy(reply, m.prefix)
	n += copy(reply[n:], input.Body)
	return n, nil
}
func (m *taggedStateMachine) StartCompact(replication.CompactInput, *replication.SMCompletion) (replication.StartResult[replication.CompactResult], error) {
	return replication.Ready(replication.CompactResult{}), nil
}
func (m *taggedStateMachine) StartCheckpoint(replication.CheckpointInput, *replication.SMCompletion) (replication.StartResult[replication.CheckpointManifest], error) {
	return replication.Ready(replication.CheckpointManifest{}), nil
}
func (m *taggedStateMachine) StartOpen(replication.OpenCheckpointInput, *replication.SMCompletion) (replication.StartResult[replication.OpenResult], error) {
	return replication.Ready(replication.OpenResult{}), nil
}
func (m *taggedStateMachine) StartReset(*replication.SMCompletion) (replication.StartResult[replication.ResetResult], error) {
	return replication.Ready(replication.ResetResult{}), nil
}
func (m *taggedStateMachine) Close() error { return nil }

type systemClock struct{ start time.Time }

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

type binaryReleaseExecutor struct {
	current      protocol.Release
	releases     []protocol.Release
	handoffBin   string
	configPath   string
	readyPrefix  string
	handoffFile  string
	closeHandles func()
}

func (e *binaryReleaseExecutor) Releases() []protocol.Release {
	out := make([]protocol.Release, len(e.releases))
	copy(out, e.releases)
	return out
}

func (e *binaryReleaseExecutor) Execute(target protocol.Release) error {
	if e.handoffBin == "" {
		return errNoHandoffBin
	}
	if e.handoffFile != "" {
		_ = writeMarker(e.handoffFile, markerPayload{
			Release: uint32(target),
			From:    uint32(e.current),
			To:      uint32(target),
			PID:     os.Getpid(),
		})
	}
	if e.closeHandles != nil {
		e.closeHandles()
	}
	nextArgs := []string{
		"-config", e.configPath,
		"-advertise", strconv.FormatUint(uint64(target), 10),
		"-ready-prefix", e.readyPrefix,
	}
	return execHandoff(e.handoffBin, nextArgs)
}

func main() {
	if err := runMain(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func runMain() error {
	configPath := flag.String("config", "", "path to nodeConfig JSON")
	advertise := flag.String("advertise", "", "comma-separated executable releases; empty disables ReleaseExecutor")
	handoffBin := flag.String("handoff-bin", "", "next release binary path for ReleaseExecutor.Execute")
	readyPrefix := flag.String("ready-prefix", "", "path prefix for ready-r<N>.json marker")
	handoffFile := flag.String("handoff-file", "", "path to write handoff marker before exec")
	flag.Parse()

	parsedRel, err := strconv.ParseUint(strings.TrimSpace(compiledRelease), 10, 32)
	if err != nil || parsedRel == 0 {
		return errInvalidRelease
	}
	currentRelease := protocol.Release(parsedRel)

	cfg, group, membership, cluster, peerAddrs, err := loadNodeConfig(*configPath)
	if err != nil {
		return err
	}
	releases, err := parseReleases(*advertise)
	if err != nil {
		return err
	}

	storage, err := replication.OpenFileStorage(cfg.File, replication.FileStorageOptions{})
	if err != nil {
		return err
	}

	var replicaMu sync.Mutex
	var replica *replication.Replica

	tr, err := transport.NewReplicaTransport(transport.ReplicaConfig{
		Group:             group,
		Membership:        membership,
		ListenAddr:        peerAddrs[cfg.LocalIndex],
		PeerAddrs:         peerAddrs,
		MessageSizeMax:    uint32(cluster.MessageSizeMax),
		ChecksumAlgorithm: cluster.ChecksumAlgorithm,
		BackoffMin:        5 * time.Millisecond,
		BackoffMax:        40 * time.Millisecond,
		DialTimeout:       500 * time.Millisecond,
		WriteTimeout:      500 * time.Millisecond,
		OnFrame: func(frame *protocol.Frame) error {
			replicaMu.Lock()
			r := replica
			replicaMu.Unlock()
			if r == nil {
				return transport.ErrTransportClosed
			}
			return r.Submit(frame)
		},
	})
	if err != nil {
		_ = storage.Close()
		return err
	}

	var closeOnce sync.Once
	closeHandles := func() {
		closeOnce.Do(func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = tr.Close(closeCtx)
			_ = storage.Close()
		})
	}

	var executor replication.ReleaseExecutor
	if len(releases) > 0 {
		executor = &binaryReleaseExecutor{
			current:      currentRelease,
			releases:     releases,
			handoffBin:   *handoffBin,
			configPath:   *configPath,
			readyPrefix:  *readyPrefix,
			handoffFile:  *handoffFile,
			closeHandles: closeHandles,
		}
	}

	process := replication.DefaultProcessConfig()
	process.Tick = 5 * time.Millisecond
	process.BackoffMin = 10 * time.Millisecond
	process.BackoffMax = 500 * time.Millisecond

	sm := &taggedStateMachine{
		prefix: fmt.Sprintf("r%d:", currentRelease),
		capacities: replication.StateMachineCapacities{
			RequestBytes:  uint32(cluster.ApplicationBatchSizeMax),
			ReplyBytes:    uint32(cluster.ApplicationReplySizeMax),
			PrefetchMax:   uint32(cluster.PipelineMax),
			CheckpointMax: 4,
		},
	}

	opened, err := replication.Open(context.Background(), replication.Config{
		Group:            group,
		Membership:       membership,
		Cluster:          cluster,
		Process:          process,
		CurrentRelease:   currentRelease,
		ClientReleaseMin: 1,
	}, replication.Dependencies{
		Storage:         storage,
		MessageBus:      tr,
		Clock:           systemClock{start: time.Now().Add(-time.Second)},
		Entropy:         rand.Reader,
		StateMachine:    sm,
		ReleaseExecutor: executor,
	})
	if err != nil {
		closeHandles()
		return err
	}
	replicaMu.Lock()
	replica = opened
	replicaMu.Unlock()

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	if err := tr.Start(runCtx); err != nil {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
		_ = opened.Close(closeCtx)
		cancelClose()
		closeHandles()
		return err
	}

	if *readyPrefix != "" {
		readyPath := fmt.Sprintf("%s-r%d.json", *readyPrefix, currentRelease)
		if err := writeMarker(readyPath, markerPayload{Release: uint32(currentRelease), PID: os.Getpid()}); err != nil {
			cancelRun()
			closeHandles()
			return err
		}
	}

	// Shut down cleanly when the test harness closes stdin.
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancelRun()
	}()

	runErr := opened.Run(runCtx)
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
	_ = opened.Close(closeCtx)
	cancelClose()
	closeHandles()
	if errors.Is(runErr, context.Canceled) {
		return nil
	}
	return runErr
}

func loadNodeConfig(path string) (nodeConfig, protocol.GroupID, replication.Membership, replication.ClusterConfig, [replication.MembersMax]string, error) {
	var zeroCfg nodeConfig
	var zeroGroup protocol.GroupID
	var zeroMem replication.Membership
	var zeroCluster replication.ClusterConfig
	var zeroAddrs [replication.MembersMax]string
	raw, err := os.ReadFile(path)
	if err != nil {
		return zeroCfg, zeroGroup, zeroMem, zeroCluster, zeroAddrs, err
	}
	var cfg nodeConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return zeroCfg, zeroGroup, zeroMem, zeroCluster, zeroAddrs, err
	}
	groupBytes, err := hex.DecodeString(cfg.Group)
	if err != nil || len(groupBytes) != 16 {
		return zeroCfg, zeroGroup, zeroMem, zeroCluster, zeroAddrs, errInvalidConfig
	}
	var group protocol.GroupID
	copy(group[:], groupBytes)
	if len(cfg.Members) != int(cfg.ActiveCount) || int(cfg.LocalIndex) >= len(cfg.Members) {
		return zeroCfg, zeroGroup, zeroMem, zeroCluster, zeroAddrs, errInvalidConfig
	}
	var membership replication.Membership
	membership.ActiveCount = cfg.ActiveCount
	for i, m := range cfg.Members {
		b, decErr := hex.DecodeString(m)
		if decErr != nil || len(b) != 16 {
			return zeroCfg, zeroGroup, zeroMem, zeroCluster, zeroAddrs, errInvalidConfig
		}
		copy(membership.Members[i][:], b)
	}
	membership.LocalMember = membership.Members[cfg.LocalIndex]
	var addrs [replication.MembersMax]string
	copy(addrs[:], cfg.PeerAddrs)

	cluster := replication.DefaultClusterConfig()
	cluster.ClientsMax = cfg.ClientsMax
	cluster.PipelineMax = cfg.PipelineMax
	cluster.ViewChangeHeadersSuffixMax = cfg.PipelineMax + 1
	cluster.JournalSlots = cfg.JournalSlots
	cluster.CompactionOps = cfg.Compaction
	cluster.MessageSizeMax = cfg.MessageMax
	cluster.ApplicationBatchSizeMax = 1024
	cluster.ApplicationReplySizeMax = 1024
	cluster.BlockSize = cfg.BlockSize
	return cfg, group, membership, cluster, addrs, nil
}

func parseReleases(csv string) ([]protocol.Release, error) {
	trimmed := strings.TrimSpace(csv)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	out := make([]protocol.Release, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.ParseUint(strings.TrimSpace(p), 10, 32)
		if err != nil || v == 0 {
			return nil, errInvalidRelease
		}
		out = append(out, protocol.Release(v))
	}
	return out, nil
}

func writeMarker(path string, payload markerPayload) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Clean(path))
}
