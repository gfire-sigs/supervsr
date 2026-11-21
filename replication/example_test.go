package replication_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
)

type noopBus struct{}

func (noopBus) SendReplica(protocol.ReplicaIndex, *replication.Message) {}
func (noopBus) SendClient(protocol.ClientID, *replication.Message)      {}
func (noopBus) BroadcastReplicas(*replication.Message)                  {}

type systemClock struct{ start time.Time }

func (c systemClock) Now() replication.TimeSample {
	now := time.Now()
	return replication.TimeSample{
		Wall:         uint64(now.UnixNano()),
		Monotonic:    uint64(now.Sub(c.start)),
		Synchronized: true,
	}
}

type echoMachine struct {
	cluster replication.ClusterConfig
	values  map[protocol.Op][]byte
}

func (m *echoMachine) Capacities() replication.StateMachineCapacities {
	return replication.StateMachineCapacities{
		RequestBytes:  uint32(m.cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(m.cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(m.cluster.PipelineMax),
		CheckpointMax: 1,
	}
}

func (m *echoMachine) Validate(in replication.ValidateInput) replication.ValidationResult {
	if in.Operation < protocol.OperationApplicationMin {
		return replication.ValidationInvalidOperation
	}
	return replication.ValidationOK
}

func (m *echoMachine) PulseNeeded(uint64) bool { return false }

func (m *echoMachine) StartPrefetch(replication.PrefetchInput, *replication.SMCompletion) (replication.StartResult[replication.PrefetchToken], error) {
	return replication.Ready(replication.PrefetchToken(0)), nil
}

func (m *echoMachine) Commit(in replication.CommitInput, _ replication.PrefetchToken, reply []byte) (int, error) {
	m.values[in.Op] = append([]byte(nil), in.Body...)
	return copy(reply, in.Body), nil
}

func (m *echoMachine) StartCompact(replication.CompactInput, *replication.SMCompletion) (replication.StartResult[replication.CompactResult], error) {
	return replication.Ready(replication.CompactResult{}), nil
}

func (m *echoMachine) StartCheckpoint(replication.CheckpointInput, *replication.SMCompletion) (replication.StartResult[replication.CheckpointManifest], error) {
	return replication.Ready(replication.CheckpointManifest{}), nil
}

func (m *echoMachine) StartOpen(replication.OpenCheckpointInput, *replication.SMCompletion) (replication.StartResult[replication.OpenResult], error) {
	clear(m.values)
	return replication.Ready(replication.OpenResult{}), nil
}

func (m *echoMachine) StartReset(*replication.SMCompletion) (replication.StartResult[replication.ResetResult], error) {
	clear(m.values)
	return replication.Ready(replication.ResetResult{}), nil
}

func (m *echoMachine) Close() error { return nil }

func ExampleOpen() {
	dir, err := os.MkdirTemp("", "supervsr-example-*")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	ctx := context.Background()
	cluster := replication.DefaultClusterConfig()
	cluster.JournalSlots = 128
	cluster.ClientsMax = 8
	cluster.MessageSizeMax = 64 << 10
	cluster.ApplicationBatchSizeMax = cluster.MessageSizeMax - replication.HeaderBytes
	cluster.ApplicationReplySizeMax = cluster.MessageSizeMax - replication.HeaderBytes
	cluster.BlockSize = 64 << 10

	group := protocol.GroupID{1}
	membership := replication.Membership{
		Members:     [replication.MembersMax]protocol.MemberID{{1}},
		ActiveCount: 1,
		LocalMember: protocol.MemberID{1},
	}
	path := filepath.Join(dir, "replica.vsr")

	formatStorage, err := replication.OpenFileStorage(path, true, false)
	if err != nil {
		panic(err)
	}
	if err := replication.Format(ctx, replication.FormatConfig{
		Group:          group,
		Membership:     membership,
		Cluster:        cluster,
		CurrentRelease: 1,
	}, replication.FormatDependencies{Storage: formatStorage}); err != nil {
		_ = formatStorage.Close()
		panic(err)
	}
	if err := formatStorage.Close(); err != nil {
		panic(err)
	}

	storage, err := replication.OpenFileStorage(path, false, false)
	if err != nil {
		panic(err)
	}
	replica, err := replication.Open(ctx, replication.Config{
		Group:            group,
		Membership:       membership,
		Cluster:          cluster,
		Process:          replication.DefaultProcessConfig(),
		CurrentRelease:   1,
		ClientReleaseMin: 1,
	}, replication.Dependencies{
		Storage:      storage,
		MessageBus:   noopBus{},
		Clock:        systemClock{start: time.Now()},
		Entropy:      rand.Reader,
		StateMachine: &echoMachine{cluster: cluster, values: make(map[protocol.Op][]byte)},
	})
	if err != nil {
		_ = storage.Close()
		panic(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = replica.Close(closeCtx)
	}()

	snap := replica.Snapshot()
	fmt.Printf("status=%d view=%d head=%d commit=%d\n", snap.Status, snap.View, snap.HeadOp, snap.CommitMin)
	// Output:
	// status=0 view=1 head=0 commit=0
}
