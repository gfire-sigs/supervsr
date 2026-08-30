package replication

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestReplaceLostReplicaFencesAdvancesPipelineAndFormatsFutureView(t *testing.T) {
	format, validation := formatFixture(t)
	cfg := replacementConfig(format)
	storage := &crashStorage{}
	fence := &replacementFenceRecorder{}
	replies := make([]ReplacementReply, format.Cluster.PipelineMax)
	replies[0] = ReplacementReply{View: 4, Operation: OperationRegister}
	for request := RequestNo(1); request < RequestNo(format.Cluster.PipelineMax); request++ {
		replies[request] = ReplacementReply{View: View(4 + request%3), Operation: OperationNoop, Request: request}
	}
	client := &replacementClientStub{replies: replies}
	if err := ReplaceLostReplica(t.Context(), cfg, ReplacementDependencies{Storage: storage, Client: client, Fence: fence}); err != nil {
		t.Fatal(err)
	}
	if fence.calls != 1 || fence.input.Group != cfg.Group || fence.input.Member != cfg.Membership.LocalMember || fence.input.ConfigurationChecksum != cfg.ConfigurationChecksum {
		t.Fatalf("fence calls=%d input=%+v", fence.calls, fence.input)
	}
	if client.noops != int(format.Cluster.PipelineMax-1) {
		t.Fatalf("noops = %d, want %d", client.noops, format.Cluster.PipelineMax-1)
	}
	storage.Crash()
	store, err := OpenSuperblockStore(storage, validation)
	if err != nil {
		t.Fatal(err)
	}
	if store.Current().State.View != 8 || store.Current().State.LogView != 0 {
		t.Fatalf("replacement durable state = %+v", store.Current().State)
	}
	config := Config{
		Group: format.Group, Membership: format.Membership, Cluster: format.Cluster,
		Process: DefaultProcessConfig(), CurrentRelease: format.CurrentRelease, ClientReleaseMin: format.CurrentRelease,
	}
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes: uint32(format.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(format.Cluster.ApplicationReplySizeMax),
		PrefetchMax: uint32(format.Cluster.PipelineMax), CheckpointMax: 1,
	}}
	replica, err := Open(t.Context(), config, Dependencies{
		Storage: storage, MessageBus: &captureBus{}, Clock: fixedClock{sample: TimeSample{Wall: 100, Monotonic: 10, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: machine,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := replica.Snapshot()
	if snapshot.Status != StatusViewChange || snapshot.View != 8 || snapshot.LogView != 0 {
		t.Fatalf("replacement recovery snapshot = %+v", snapshot)
	}
	closeReplica(t, replica)
}
func TestReplaceLostReplicaCrashSelectsOnlyDurableReplacementQuorum(t *testing.T) {
	format, validation := formatFixture(t)
	cfg := replacementConfig(format)
	probe := &crashStorage{}
	if err := ReplaceLostReplica(t.Context(), cfg, ReplacementDependencies{
		Storage: probe, Client: &replacementClientStub{replies: replacementReplies(format.Cluster.PipelineMax, 3)},
		Fence: &replacementFenceRecorder{},
	}); err != nil {
		t.Fatal(err)
	}
	operationCount := probe.operation
	writeQuorum, _ := superblockWriteCopies(format.Cluster.SuperblockCopies)
	for failAt := 1; failAt <= operationCount; failAt++ {
		t.Run(operationName(failAt), func(t *testing.T) {
			storage := &crashStorage{failAt: failAt}
			client := &replacementClientStub{replies: replacementReplies(format.Cluster.PipelineMax, 3)}
			err := ReplaceLostReplica(context.Background(), cfg, ReplacementDependencies{
				Storage: storage, Client: client, Fence: &replacementFenceRecorder{},
			})
			if err == nil {
				t.Fatal("replacement succeeded across injected crash")
			}
			if client.noops != int(format.Cluster.PipelineMax-1) {
				t.Fatalf("noops = %d, want %d", client.noops, format.Cluster.PipelineMax-1)
			}
			storage.Crash()
			durableCopies := validSequenceCopyCount(storage, validation, 1)
			store, openErr := OpenSuperblockStore(storage, validation)
			if durableCopies < int(writeQuorum) {
				if !errors.Is(openErr, ErrUnformatted) {
					t.Fatalf("durable copies %d: open error = %v, want %v", durableCopies, openErr, ErrUnformatted)
				}
				return
			}
			if openErr != nil {
				t.Fatalf("durable copies %d: open: %v", durableCopies, openErr)
			}
			if store.Current().State.View != 5 || store.Current().State.LogView != 0 {
				t.Fatalf("durable copies %d: state=%+v", durableCopies, store.Current().State)
			}
		})
	}
}

func TestReplaceLostReplicaRejectsUnsafePrerequisitesBeforeClientRequests(t *testing.T) {
	format, _ := formatFixture(t)
	tests := []struct {
		name   string
		mutate func(*ReplacementConfig, *crashStorage, *replacementFenceRecorder)
		want   error
	}{
		{
			name: "too few active replicas",
			mutate: func(cfg *ReplacementConfig, _ *crashStorage, _ *replacementFenceRecorder) {
				cfg.Membership.ActiveCount = 1
				cfg.Membership.Members[1] = MemberID{}
				cfg.Membership.Members[2] = MemberID{}
				cfg.ConfigurationChecksum = cfg.Cluster.Fingerprint()
			},
			want: ErrInvalidConfiguration,
		},
		{
			name: "configuration checksum mismatch",
			mutate: func(cfg *ReplacementConfig, _ *crashStorage, _ *replacementFenceRecorder) {
				cfg.ConfigurationChecksum[0] ^= 0xff
			},
			want: ErrInvalidConfiguration,
		},
		{
			name: "nonempty storage",
			mutate: func(_ *ReplacementConfig, storage *crashStorage, _ *replacementFenceRecorder) {
				storage.working = []byte{1}
				storage.durable = []byte{1}
			},
			want: ErrStorageNotEmpty,
		},
		{
			name: "fence denied",
			mutate: func(_ *ReplacementConfig, _ *crashStorage, fence *replacementFenceRecorder) {
				fence.err = errors.New("credentials remain active")
			},
			want: ErrReplacementNotFenced,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := replacementConfig(format)
			storage := &crashStorage{}
			fence := &replacementFenceRecorder{}
			client := &replacementClientStub{}
			test.mutate(&cfg, storage, fence)
			err := ReplaceLostReplica(t.Context(), cfg, ReplacementDependencies{Storage: storage, Client: client, Fence: fence})
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if client.registers != 0 || client.noops != 0 {
				t.Fatalf("client requests registers=%d noops=%d", client.registers, client.noops)
			}
		})
	}
}

func TestReplaceLostReplicaRejectsAmbiguousOrUnsafeObservationsWithoutFormatting(t *testing.T) {
	format, _ := formatFixture(t)
	clientFailure := errors.New("ambiguous request")
	overflowReplies := replacementReplies(format.Cluster.PipelineMax, MaxView-1)
	tests := []struct {
		name    string
		replies []ReplacementReply
		errAt   int
		err     error
		want    error
	}{
		{
			name:    "wrong registration reply",
			replies: []ReplacementReply{{Operation: OperationNoop}},
			want:    ErrReplacementObservation,
		},
		{
			name: "skipped noop reply",
			replies: []ReplacementReply{
				{Operation: OperationRegister},
				{Operation: OperationNoop, Request: 2},
			},
			want: ErrReplacementObservation,
		},
		{
			name:    "view overflow",
			replies: overflowReplies,
			want:    ErrViewOverflow,
		},
		{
			name:    "ambiguous noop",
			replies: []ReplacementReply{{Operation: OperationRegister}},
			errAt:   1,
			err:     clientFailure,
			want:    clientFailure,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storage := &crashStorage{}
			client := &replacementClientStub{replies: test.replies, errAt: test.errAt, err: test.err}
			err := ReplaceLostReplica(t.Context(), replacementConfig(format), ReplacementDependencies{
				Storage: storage, Client: client, Fence: &replacementFenceRecorder{},
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if size, sizeErr := storage.Size(); sizeErr != nil || size != 0 {
				t.Fatalf("storage size=%d error=%v", size, sizeErr)
			}
		})
	}
}

func replacementReplies(pipeline uint64, view View) []ReplacementReply {
	replies := make([]ReplacementReply, pipeline)
	replies[0] = ReplacementReply{View: view, Operation: OperationRegister}
	for request := RequestNo(1); request < RequestNo(pipeline); request++ {
		replies[request] = ReplacementReply{View: view, Operation: OperationNoop, Request: request}
	}
	return replies
}

func replacementConfig(format FormatConfig) ReplacementConfig {
	return ReplacementConfig{
		Group: format.Group, Membership: format.Membership, Cluster: format.Cluster,
		CurrentRelease: format.CurrentRelease, ConfigurationChecksum: format.Cluster.Fingerprint(),
	}
}

type replacementFenceRecorder struct {
	calls int
	input ReplacementFenceInput
	err   error
}

func (fence *replacementFenceRecorder) VerifyReplacementFence(_ context.Context, input ReplacementFenceInput) error {
	fence.calls++
	fence.input = input
	return fence.err
}

type replacementClientStub struct {
	replies   []ReplacementReply
	position  int
	registers int
	noops     int
	errAt     int
	err       error
}

func (client *replacementClientStub) Register(context.Context) (ReplacementReply, error) {
	client.registers++
	return client.next()
}

func (client *replacementClientStub) Noop(context.Context) (ReplacementReply, error) {
	client.noops++
	return client.next()
}

func (client *replacementClientStub) next() (ReplacementReply, error) {
	if client.err != nil && client.position == client.errAt {
		return ReplacementReply{}, client.err
	}
	if client.position >= len(client.replies) {
		return ReplacementReply{}, ErrReplacementObservation
	}
	reply := client.replies[client.position]
	client.position++
	return reply, nil
}
