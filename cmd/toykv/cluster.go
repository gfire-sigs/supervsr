package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
	"github.com/rs/zerolog"
)

const metadataVersion = 1

var errClusterMetadata = errors.New("toykv: invalid cluster metadata")

type diskMetadata struct {
	Version int      `json:"version"`
	Group   string   `json:"group"`
	Members []string `json:"members"`
	Ready   bool     `json:"ready"`
}

type clusterIdentity struct {
	group   protocol.GroupID
	members []protocol.MemberID
	ready   bool
}

type realClock struct {
	start time.Time
}

func (clock realClock) Now() replication.TimeSample {
	now := time.Now()
	return replication.TimeSample{
		Wall: uint64(now.UnixNano()), Monotonic: uint64(now.Sub(clock.start)), Synchronized: true,
	}
}

type clientEvents struct {
	replies chan replication.ClientReply
	evicted chan protocol.EvictionReason
}

func (events *clientEvents) Reply(reply replication.ClientReply) {
	reply.Body = append([]byte(nil), reply.Body...)
	events.replies <- reply
}

func (events *clientEvents) Evicted(reason protocol.EvictionReason) {
	events.evicted <- reason
}

type localTransport struct {
	mu          sync.RWMutex
	clientMu    sync.Mutex
	pool        *protocol.FramePool
	replicas    [replication.MembersMax]*replication.Replica
	memberCount uint8
	client      *replication.Client
	clientID    protocol.ClientID
	logger      zerolog.Logger
}

type localBus struct {
	transport  *localTransport
	from       protocol.ReplicaIndex
	fromMember bool
}

func (bus localBus) SendReplica(to protocol.ReplicaIndex, message *replication.Message) {
	bus.transport.sendReplica(to, message)
}

func (bus localBus) SendClient(to protocol.ClientID, message *replication.Message) {
	if !bus.fromMember {
		return
	}
	bus.transport.sendClient(bus.from, to, message)
}

func (bus localBus) BroadcastReplicas(message *replication.Message) {
	bus.transport.mu.RLock()
	count := int(bus.transport.memberCount)
	bus.transport.mu.RUnlock()
	for index := range count {
		to := protocol.ReplicaIndex(index)
		if bus.fromMember && to == bus.from {
			continue
		}
		bus.transport.sendReplica(to, message)
	}
}

func (transport *localTransport) sendReplica(to protocol.ReplicaIndex, message *replication.Message) {
	encoded, err := message.Bytes()
	if err != nil {
		return
	}
	frame, err := transport.pool.AcquireEncoded(encoded)
	if err != nil {
		event := transport.logger.Warn()
		event.Err(err)
		event.Uint8("replica", uint8(to))
		event.Msg("local transport dropped frame")
		return
	}
	transport.mu.RLock()
	var target *replication.Replica
	if int(to) < len(transport.replicas) {
		target = transport.replicas[to]
	}
	transport.mu.RUnlock()
	if target == nil {
		frame.Release()
		return
	}
	if err := target.Submit(frame); err != nil {
		frame.Release()
		event := transport.logger.Warn()
		event.Err(err)
		event.Uint8("replica", uint8(to))
		event.Msg("local transport dropped frame")
	}
}

func (transport *localTransport) sendClient(from protocol.ReplicaIndex, to protocol.ClientID, message *replication.Message) {
	encoded, err := message.Bytes()
	if err != nil {
		return
	}
	transport.clientMu.Lock()
	defer transport.clientMu.Unlock()
	transport.mu.RLock()
	client := transport.client
	clientID := transport.clientID
	transport.mu.RUnlock()
	if client != nil && clientID == to {
		client.HandleFrame(from, encoded)
	}
}

type localCluster struct {
	group        protocol.GroupID
	config       replication.ClusterConfig
	process      replication.ProcessConfig
	transport    *localTransport
	replicas     []*replication.Replica
	runResults   chan error
	runs         int
	clientEvents *clientEvents
	client       *replication.Client
	logger       zerolog.Logger
}

func openCluster(ctx context.Context, opts options, logger zerolog.Logger) (*localCluster, error) {
	if opts.replicas == 0 || opts.replicas > replication.ActiveMax {
		return nil, fmt.Errorf("replicas must be between 1 and %d", replication.ActiveMax)
	}
	if opts.commandTimeout <= 0 {
		return nil, errors.New("timeout must be positive")
	}
	if err := os.MkdirAll(opts.dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	identity, err := loadOrCreateIdentity(opts.dataDir, uint8(opts.replicas))
	if err != nil {
		return nil, err
	}

	clusterConfig := toykvClusterConfig()
	processConfig := replication.DefaultProcessConfig()
	processConfig.StorageSizeLimit = 1 << 30
	pool, err := protocol.NewFramePool(1024, uint32(clusterConfig.MessageSizeMax))
	if err != nil {
		return nil, err
	}
	transport := &localTransport{
		pool: pool, memberCount: uint8(len(identity.members)),
		logger: logger.With().Str("component", "transport").Logger(),
	}
	cluster := &localCluster{
		group: identity.group, config: clusterConfig, process: processConfig, transport: transport,
		replicas: make([]*replication.Replica, len(identity.members)), runResults: make(chan error, len(identity.members)), logger: logger,
	}
	if err := cluster.openReplicas(ctx, opts, identity); err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = cluster.Close(closeCtx)
		return nil, err
	}
	return cluster, nil
}

func toykvClusterConfig() replication.ClusterConfig {
	config := replication.DefaultClusterConfig()
	config.JournalSlots = 128
	config.MessageSizeMax = 128 << 10
	config.ApplicationBatchSizeMax = maxRequestBytes
	config.ApplicationReplySizeMax = maxReplyBytes
	config.BlockSize = 128 << 10
	return config
}

func (cluster *localCluster) openReplicas(ctx context.Context, opts options, identity clusterIdentity) error {
	if !identity.ready {
		if err := initializeReplicaFiles(ctx, opts, identity, cluster.config); err != nil {
			return err
		}
		identity.ready = true
		if err := storeIdentity(opts.dataDir, identity); err != nil {
			return err
		}
	}

	clock := realClock{start: time.Now()}
	for index := range identity.members {
		membership := membershipFor(identity.members, index)
		config := replication.Config{
			Group: identity.group, Membership: membership, Cluster: cluster.config, Process: cluster.process,
			CurrentRelease: 1, ClientReleaseMin: 1,
		}
		storage, err := replication.OpenFileStorage(replicaPath(opts.dataDir, index), false, opts.directIO)
		if err != nil {
			return fmt.Errorf("open replica %d storage: %w", index, err)
		}
		machineDir := filepath.Join(opts.dataDir, fmt.Sprintf("machine-%d", index))
		if err := os.MkdirAll(machineDir, 0o700); err != nil {
			_ = storage.Close()
			return err
		}
		loggerContext := cluster.logger.With()
		loggerContext = loggerContext.Uint8("replica", uint8(index))
		replicaLogger := loggerContext.Logger()
		machine := newKVMachine(
			machineDir,
			identity.group,
			uint8(len(identity.members)),
			uint32(cluster.config.MessageSizeMax),
			cluster.config.CompactionOps,
			replicaLogger,
		)
		replica, err := replication.Open(ctx, config, replication.Dependencies{
			Storage: storage, MessageBus: localBus{transport: cluster.transport, from: protocol.ReplicaIndex(index), fromMember: true},
			Clock: clock, Entropy: rand.Reader, StateMachine: machine, Logger: &replicaLogger,
		})
		if err != nil {
			_ = storage.Close()
			return fmt.Errorf("open replica %d: %w", index, err)
		}
		cluster.replicas[index] = replica
		snapshot := replica.Snapshot()
		event := replicaLogger.Info()
		event.Uint8("status", uint8(snapshot.Status))
		event.Uint32("view", uint32(snapshot.View))
		event.Uint64("head", uint64(snapshot.HeadOp))
		event.Uint64("commit", uint64(snapshot.CommitMin))
		event.Msg("replica recovered")
	}
	cluster.transport.mu.Lock()
	for index, replica := range cluster.replicas {
		cluster.transport.replicas[index] = replica
	}
	cluster.transport.mu.Unlock()
	cluster.runs = len(cluster.replicas)
	for index, replica := range cluster.replicas {
		go func(index int, replica *replication.Replica) {
			err := replica.Run(context.Background())
			if err != nil {
				event := cluster.logger.Error()
				event.Err(err)
				event.Int("replica", index)
				event.Msg("replica stopped")
			}
			cluster.runResults <- err
		}(index, replica)
	}
	return nil
}

func initializeReplicaFiles(ctx context.Context, opts options, identity clusterIdentity, config replication.ClusterConfig) error {
	for index := range identity.members {
		if err := os.Remove(replicaPath(opts.dataDir, index)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.RemoveAll(filepath.Join(opts.dataDir, fmt.Sprintf("machine-%d", index))); err != nil {
			return err
		}
	}
	for index := range identity.members {
		storage, err := replication.OpenFileStorage(replicaPath(opts.dataDir, index), true, opts.directIO)
		if err != nil {
			return err
		}
		membership := membershipFor(identity.members, index)
		err = replication.Format(ctx, replication.FormatConfig{
			Group: identity.group, Membership: membership, Cluster: config, CurrentRelease: 1,
		}, replication.FormatDependencies{Storage: storage})
		closeErr := storage.Close()
		if err != nil {
			return fmt.Errorf("format replica %d: %w", index, err)
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func membershipFor(members []protocol.MemberID, local int) replication.Membership {
	membership := replication.Membership{ActiveCount: uint8(len(members)), LocalMember: members[local]}
	copy(membership.Members[:], members)
	return membership
}

func (cluster *localCluster) RegisterClient(ctx context.Context, timeout time.Duration) error {
	var id protocol.ClientID
	for id.IsZero() {
		if _, err := io.ReadFull(rand.Reader, id[:]); err != nil {
			return err
		}
	}
	events := &clientEvents{replies: make(chan replication.ClientReply, 1), evicted: make(chan protocol.EvictionReason, 1)}
	client, err := replication.NewClient(replication.ClientConfig{
		Group: cluster.group, ID: id, Release: 1, ActiveCount: uint8(len(cluster.replicas)),
		MessageSizeMax: uint32(cluster.config.MessageSizeMax), Process: cluster.process,
	}, localBus{transport: cluster.transport}, realClock{start: time.Now()}, rand.Reader, events)
	if err != nil {
		return err
	}
	cluster.client = client
	cluster.clientEvents = events
	cluster.transport.mu.Lock()
	cluster.transport.client = client
	cluster.transport.clientID = id
	cluster.transport.mu.Unlock()
	cluster.transport.clientMu.Lock()
	err = client.Register()
	cluster.transport.clientMu.Unlock()
	if err != nil {
		return err
	}
	_, err = cluster.awaitReply(ctx, timeout)
	return err
}

func (cluster *localCluster) Put(ctx context.Context, timeout time.Duration, key, value string) (string, error) {
	body, err := encodePut(key, value)
	if err != nil {
		return "", err
	}
	reply, err := cluster.request(ctx, timeout, operationPut, body)
	if err != nil {
		return "", err
	}
	return statusResult(reply.Body, "OK")
}

func (cluster *localCluster) Get(ctx context.Context, timeout time.Duration, key string) (string, error) {
	body, err := encodeKey(key)
	if err != nil {
		return "", err
	}
	reply, err := cluster.request(ctx, timeout, operationGet, body)
	if err != nil {
		return "", err
	}
	if len(reply.Body) == 0 {
		return "", errInvalidKVRequest
	}
	switch reply.Body[0] {
	case resultOK:
		return string(reply.Body[1:]), nil
	case resultNotFound:
		return "NOT_FOUND", nil
	default:
		return "", errInvalidKVRequest
	}
}

func (cluster *localCluster) Delete(ctx context.Context, timeout time.Duration, key string) (string, error) {
	body, err := encodeKey(key)
	if err != nil {
		return "", err
	}
	reply, err := cluster.request(ctx, timeout, operationDelete, body)
	if err != nil {
		return "", err
	}
	return statusResult(reply.Body, "DELETED")
}

func statusResult(body []byte, success string) (string, error) {
	if len(body) != 1 {
		return "", errInvalidKVRequest
	}
	switch body[0] {
	case resultOK:
		return success, nil
	case resultNotFound:
		return "NOT_FOUND", nil
	case resultCapacity:
		return "CAPACITY", nil
	default:
		return "", errInvalidKVRequest
	}
}

func (cluster *localCluster) request(ctx context.Context, timeout time.Duration, operation protocol.Operation, body []byte) (replication.ClientReply, error) {
	cluster.transport.clientMu.Lock()
	err := cluster.client.Submit(operation, body)
	cluster.transport.clientMu.Unlock()
	if err != nil {
		return replication.ClientReply{}, err
	}
	return cluster.awaitReply(ctx, timeout)
}

func (cluster *localCluster) awaitReply(ctx context.Context, timeout time.Duration) (replication.ClientReply, error) {
	deadline := time.NewTimer(timeout)
	ticker := time.NewTicker(cluster.process.Tick)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		select {
		case reply := <-cluster.clientEvents.replies:
			return reply, nil
		case reason := <-cluster.clientEvents.evicted:
			return replication.ClientReply{}, fmt.Errorf("client evicted: %d", reason)
		case <-ticker.C:
			cluster.transport.clientMu.Lock()
			err := cluster.client.Tick()
			cluster.transport.clientMu.Unlock()
			if err != nil {
				return replication.ClientReply{}, err
			}
		case <-deadline.C:
			return replication.ClientReply{}, context.DeadlineExceeded
		case <-ctx.Done():
			return replication.ClientReply{}, ctx.Err()
		}
	}
}

func (cluster *localCluster) Close(ctx context.Context) error {
	cluster.transport.mu.Lock()
	cluster.transport.client = nil
	cluster.transport.clientID = protocol.ClientID{}
	cluster.transport.mu.Unlock()
	var first error
	if cluster.client != nil {
		cluster.transport.clientMu.Lock()
		err := cluster.client.Close()
		cluster.transport.clientMu.Unlock()
		if err != nil && first == nil {
			first = err
		}
	}
	for _, replica := range cluster.replicas {
		if replica == nil {
			continue
		}
		if err := replica.Close(ctx); err != nil && first == nil {
			first = err
		}
	}
	for range cluster.runs {
		select {
		case err := <-cluster.runResults:
			if err != nil && first == nil {
				first = err
			}
		case <-ctx.Done():
			if first == nil {
				first = ctx.Err()
			}
			return first
		}
	}
	return first
}

func loadOrCreateIdentity(directory string, replicas uint8) (clusterIdentity, error) {
	path := filepath.Join(directory, "cluster.json")
	encoded, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		identity, generateErr := generateIdentity(replicas)
		if generateErr != nil {
			return clusterIdentity{}, generateErr
		}
		if storeErr := storeIdentity(directory, identity); storeErr != nil {
			return clusterIdentity{}, storeErr
		}
		return identity, nil
	}
	if err != nil {
		return clusterIdentity{}, err
	}
	var metadata diskMetadata
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		return clusterIdentity{}, errors.Join(errClusterMetadata, err)
	}
	if metadata.Version != metadataVersion || len(metadata.Members) != int(replicas) {
		return clusterIdentity{}, errClusterMetadata
	}
	groupBytes, err := hex.DecodeString(metadata.Group)
	if err != nil || len(groupBytes) != len(protocol.GroupID{}) {
		return clusterIdentity{}, errClusterMetadata
	}
	identity := clusterIdentity{members: make([]protocol.MemberID, len(metadata.Members)), ready: metadata.Ready}
	copy(identity.group[:], groupBytes)
	if identity.group.IsZero() {
		return clusterIdentity{}, errClusterMetadata
	}
	for index, encodedMember := range metadata.Members {
		memberBytes, err := hex.DecodeString(encodedMember)
		if err != nil || len(memberBytes) != len(protocol.MemberID{}) {
			return clusterIdentity{}, errClusterMetadata
		}
		copy(identity.members[index][:], memberBytes)
		if identity.members[index].IsZero() {
			return clusterIdentity{}, errClusterMetadata
		}
		for previous := range index {
			if identity.members[index] == identity.members[previous] {
				return clusterIdentity{}, errClusterMetadata
			}
		}
	}
	return identity, nil
}

func generateIdentity(replicas uint8) (clusterIdentity, error) {
	identity := clusterIdentity{members: make([]protocol.MemberID, replicas)}
	if _, err := io.ReadFull(rand.Reader, identity.group[:]); err != nil {
		return clusterIdentity{}, err
	}
	for index := range identity.members {
		for identity.members[index].IsZero() {
			if _, err := io.ReadFull(rand.Reader, identity.members[index][:]); err != nil {
				return clusterIdentity{}, err
			}
		}
	}
	return identity, nil
}

func storeIdentity(directory string, identity clusterIdentity) error {
	metadata := diskMetadata{
		Version: metadataVersion, Group: hex.EncodeToString(identity.group[:]), Members: make([]string, len(identity.members)), Ready: identity.ready,
	}
	for index := range identity.members {
		metadata.Members[index] = hex.EncodeToString(identity.members[index][:])
	}
	encoded, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return writeAtomic(filepath.Join(directory, "cluster.json"), encoded, 0o600)
}

func writeAtomic(path string, contents []byte, mode os.FileMode) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func replicaPath(directory string, index int) string {
	return filepath.Join(directory, fmt.Sprintf("replica-%d.vsr", index))
}
