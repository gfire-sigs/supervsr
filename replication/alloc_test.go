package replication

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"z.gfire.dev/supervsr/replication/internal/simhook"
	"z.gfire.dev/supervsr/replication/protocol"
)

type zeroAllocStorage struct {
	working []byte
	durable []byte
}

func newZeroAllocStorage(size uint64) *zeroAllocStorage {
	return &zeroAllocStorage{working: make([]byte, int(size)), durable: make([]byte, int(size))}
}

func (storage *zeroAllocStorage) ReadAt(destination []byte, offset uint64) error {
	copy(destination, storage.working[offset:offset+uint64(len(destination))])
	return nil
}

func (storage *zeroAllocStorage) WriteAt(source []byte, offset uint64) error {
	copy(storage.working[offset:offset+uint64(len(source))], source)
	return nil
}

func (storage *zeroAllocStorage) Sync() error {
	copy(storage.durable, storage.working)
	return nil
}

func (storage *zeroAllocStorage) Resize(size uint64) error {
	if uint64(len(storage.working)) == size {
		return nil
	}
	next := make([]byte, int(size))
	copy(next, storage.working)
	storage.working = next
	if uint64(len(storage.durable)) != size {
		durable := make([]byte, int(size))
		copy(durable, storage.durable)
		storage.durable = durable
	}
	return nil
}

func (storage *zeroAllocStorage) Size() (uint64, error) { return uint64(len(storage.working)), nil }
func (storage *zeroAllocStorage) SyncParent() error     { return nil }
func (storage *zeroAllocStorage) Close() error          { return nil }

type zeroAllocStateMachine struct {
	capacities StateMachineCapacities
}

func (machine *zeroAllocStateMachine) Capacities() StateMachineCapacities      { return machine.capacities }
func (machine *zeroAllocStateMachine) Validate(ValidateInput) ValidationResult { return ValidationOK }
func (machine *zeroAllocStateMachine) PulseNeeded(uint64) bool                 { return false }
func (machine *zeroAllocStateMachine) StartPrefetch(PrefetchInput, *SMCompletion) (StartResult[PrefetchToken], error) {
	return Ready(PrefetchToken(1)), nil
}
func (machine *zeroAllocStateMachine) Commit(input CommitInput, _ PrefetchToken, reply []byte) (int, error) {
	return copy(reply, input.Body), nil
}
func (machine *zeroAllocStateMachine) StartCompact(CompactInput, *SMCompletion) (StartResult[CompactResult], error) {
	return Ready(CompactResult{}), nil
}
func (machine *zeroAllocStateMachine) StartCheckpoint(CheckpointInput, *SMCompletion) (StartResult[CheckpointManifest], error) {
	return Ready(CheckpointManifest{}), nil
}
func (machine *zeroAllocStateMachine) StartOpen(OpenCheckpointInput, *SMCompletion) (StartResult[OpenResult], error) {
	return Ready(OpenResult{}), nil
}
func (machine *zeroAllocStateMachine) StartReset(*SMCompletion) (StartResult[ResetResult], error) {
	return Ready(ResetResult{}), nil
}
func (machine *zeroAllocStateMachine) Close() error { return nil }

type zeroAllocBus struct {
	replicaPool *protocol.FramePool
	lastReplica [4096]byte
	lastLen     int
	lastClient  [4096]byte
	lastClientL int
	clients     uint64
	replicas    uint64
}

func (bus *zeroAllocBus) SendReplica(_ protocol.ReplicaIndex, message *Message) {
	bus.recordReplica(message)
}

func (bus *zeroAllocBus) BroadcastReplicas(message *Message) {
	bus.recordReplica(message)
}

func (bus *zeroAllocBus) recordReplica(message *Message) {
	if encoded, err := message.Bytes(); err == nil && len(encoded) <= len(bus.lastReplica) {
		bus.lastLen = copy(bus.lastReplica[:], encoded)
	}
	bus.replicas++
}

func (bus *zeroAllocBus) SendClient(_ protocol.ClientID, message *Message) {
	if encoded, err := message.Bytes(); err == nil && len(encoded) <= len(bus.lastClient) {
		bus.lastClientL = copy(bus.lastClient[:], encoded)
	}
	bus.clients++
}

func openZeroAllocPair(t *testing.T) (*Replica, *Replica, *zeroAllocBus, *zeroAllocBus, *steppingClock) {
	t.Helper()
	cluster := compactTestClusterConfig()
	cluster.JournalSlots = 256
	members := [MembersMax]protocol.MemberID{{1}, {2}, {3}}
	layout, ok := deriveWALLayout(cluster)
	if !ok {
		t.Fatal("invalid WAL layout")
	}
	clock := &steppingClock{wall: uint64(time.Hour), monotonic: uint64(time.Second), synchronized: true}
	openMember := func(index int) (*Replica, *zeroAllocBus) {
		storage := newZeroAllocStorage(0)
		membership := Membership{Members: members, ActiveCount: 3, LocalMember: members[index]}
		cfg := Config{
			Group: protocol.GroupID{9}, Membership: membership, Cluster: cluster,
			Process: DefaultProcessConfig(), CurrentRelease: 1, ClientReleaseMin: 1,
		}
		if err := Format(t.Context(), FormatConfig{
			Group: cfg.Group, Membership: membership, Cluster: cluster, CurrentRelease: 1,
		}, FormatDependencies{Storage: storage}); err != nil {
			t.Fatal(err)
		}
		if uint64(len(storage.working)) != layout.BlockBase {
			t.Fatalf("storage size=%d want=%d", len(storage.working), layout.BlockBase)
		}
		bus := &zeroAllocBus{}
		machine := &zeroAllocStateMachine{capacities: StateMachineCapacities{
			RequestBytes: uint32(cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(cluster.ApplicationReplySizeMax),
			PrefetchMax: uint32(cluster.PipelineMax), CheckpointMax: 1,
		}}
		replica, err := Open(simhook.WithIOOptions(t.Context(), simhook.IOOptions{SynchronousIO: true}), cfg, Dependencies{
			Storage: storage, MessageBus: bus, Clock: clock,
			Entropy:      bytes.NewReader([]byte{byte(index + 1), 2, 3, 4, 5, 6, 7, 8}),
			StateMachine: machine, Metrics: &ReplicaMetrics{},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeReplica(t, replica) })
		return replica, bus
	}
	primary, primaryBus := openMember(0)
	backup, backupBus := openMember(1)

	// Install view 3 so member 0 is normal primary and member 1 is normal backup.
	root := consensusRoot(t, primary)
	viewBody := encodeTestViewBody(t, primary, root)
	viewHeader := makeTestViewHeader(primary, 3, 0, 0, protocol.Nonce{})
	for _, node := range []*Replica{primary, backup} {
		node.handleView(viewHeader, viewBody)
		for range 8 {
			if _, err := node.Process(32); err != nil {
				t.Fatal(err)
			}
		}
		if snap := node.Snapshot(); snap.Status != StatusNormal || snap.View != 3 {
			t.Fatalf("member %d did not enter StatusNormal(3): %+v", node.local, snap)
		}
	}
	return primary, backup, primaryBus, backupBus, clock
}

func TestReplicaNormalPathsZeroHeapAllocations(t *testing.T) {
	primary, backup, primaryBus, backupBus, clock := openZeroAllocPair(t)
	inboundPool, err := protocol.NewFramePool(8, uint32(primary.config.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	client := protocol.ClientID{0x55}
	payload := [4]byte{1, 2, 3, 4}

	// Register client before measuring steady-state paths.
	regFrame, err := inboundPool.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	regHeader := protocol.Header{
		Group: primary.config.Group, View: 3, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandRequest,
	}
	copy(regHeader.Fields[32:48], client[:])
	regHeader.Fields[68] = byte(protocol.OperationRegister)
	if err := regFrame.Seal(&regHeader, primary.config.Cluster.ChecksumAlgorithm); err != nil {
		t.Fatal(err)
	}
	if err := regFrame.BindClient(); err != nil {
		t.Fatal(err)
	}
	if err := primary.Submit(regFrame); err != nil {
		t.Fatal(err)
	}
	drain := func(target *Replica) {
		for range 8 {
			count, procErr := target.Process(16)
			if procErr != nil {
				t.Fatal(procErr)
			}
			if count == 0 {
				break
			}
		}
	}
	drain(primary)
	// Deliver Prepare(1) to backup and PrepareOK(1) back to primary so Register commits.
	deliver := func(target *Replica, from protocol.ReplicaIndex, raw []byte) {
		frame, acquireErr := inboundPool.AcquireEncoded(raw)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		if bindErr := frame.BindReplica(target.membership.Members[from], from); bindErr != nil {
			t.Fatal(bindErr)
		}
		if submitErr := target.Submit(frame); submitErr != nil {
			t.Fatal(submitErr)
		}
		drain(target)
	}
	deliver(backup, 0, primaryBus.lastReplica[:primaryBus.lastLen])
	deliver(primary, 1, backupBus.lastReplica[:backupBus.lastLen])
	// Advance backup's commitMin via Commit heartbeat.
	clock.monotonic += uint64(time.Millisecond)
	primary.sendCommit(clock.Now())
	deliver(backup, 0, primaryBus.lastReplica[:primaryBus.lastLen])
	if primary.Snapshot().CommitMin != 1 || backup.Snapshot().CommitMin != 1 {
		t.Fatalf("registration did not commit: primary=%+v backup=%+v", primary.Snapshot(), backup.Snapshot())
	}
	replyHeader, _, reason := protocol.DecodeFrame(primaryBus.lastClient[:primaryBus.lastClientL], primary.config.Group, uint32(primary.config.Cluster.MessageSizeMax), 3, primary.config.Cluster.ChecksumAlgorithm)
	if reason != protocol.RejectNone || replyHeader.Command != protocol.CommandReply {
		t.Fatalf("registration reply invalid: reason=%v cmd=%d", reason, replyHeader.Command)
	}
	session := protocol.Session(replyCommit(&replyHeader))
	parent := replyContext(&replyHeader)
	nextReq := protocol.RequestNo(1)

	// Warm up one application op so backup also commits op 1.
	sendAppOp := func() {
		clock.wall += uint64(time.Millisecond)
		clock.monotonic += uint64(time.Millisecond)
		reqFrame, acquireErr := inboundPool.Acquire(uint32(len(payload)))
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		body, bodyErr := reqFrame.Body()
		if bodyErr != nil {
			t.Fatal(bodyErr)
		}
		copy(body, payload[:])
		reqHeader := protocol.Header{
			Group: primary.config.Group, View: 3, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandRequest,
		}
		copy(reqHeader.Fields[:16], parent[:])
		copy(reqHeader.Fields[32:48], client[:])
		binary.LittleEndian.PutUint64(reqHeader.Fields[48:56], uint64(session))
		binary.LittleEndian.PutUint32(reqHeader.Fields[64:68], uint32(nextReq))
		reqHeader.Fields[68] = byte(protocol.OperationApplicationMin)
		if sealErr := reqFrame.Seal(&reqHeader, primary.config.Cluster.ChecksumAlgorithm); sealErr != nil {
			t.Fatal(sealErr)
		}
		if bindErr := reqFrame.BindClient(); bindErr != nil {
			t.Fatal(bindErr)
		}
		if submitErr := primary.Submit(reqFrame); submitErr != nil {
			t.Fatal(submitErr)
		}
		drain(primary)
		deliver(backup, 0, primaryBus.lastReplica[:primaryBus.lastLen])
		deliver(primary, 1, backupBus.lastReplica[:backupBus.lastLen])
		primary.sendCommit(clock.Now())
		deliver(backup, 0, primaryBus.lastReplica[:primaryBus.lastLen])
		lastReply, _, decodeReason := protocol.DecodeFrame(primaryBus.lastClient[:primaryBus.lastClientL], primary.config.Group, uint32(primary.config.Cluster.MessageSizeMax), 3, primary.config.Cluster.ChecksumAlgorithm)
		if decodeReason != protocol.RejectNone {
			t.Fatalf("reply decode reason=%v", decodeReason)
		}
		parent = replyContext(&lastReply)
		nextReq++
	}
	sendAppOp()
	var dupReqBytes [4096]byte
	var dupReqLen int
	{
		clock.wall += uint64(time.Millisecond)
		clock.monotonic += uint64(time.Millisecond)
		reqFrame, acquireErr := inboundPool.Acquire(uint32(len(payload)))
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		body, _ := reqFrame.Body()
		copy(body, payload[:])
		reqHeader := protocol.Header{
			Group: primary.config.Group, View: 3, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandRequest,
		}
		copy(reqHeader.Fields[:16], parent[:])
		copy(reqHeader.Fields[32:48], client[:])
		binary.LittleEndian.PutUint64(reqHeader.Fields[48:56], uint64(session))
		binary.LittleEndian.PutUint32(reqHeader.Fields[64:68], uint32(nextReq))
		reqHeader.Fields[68] = byte(protocol.OperationApplicationMin)
		if err := reqFrame.Seal(&reqHeader, primary.config.Cluster.ChecksumAlgorithm); err != nil {
			t.Fatal(err)
		}
		raw, _ := reqFrame.Bytes()
		dupReqLen = copy(dupReqBytes[:], raw)
		if err := reqFrame.BindClient(); err != nil {
			t.Fatal(err)
		}
		if err := primary.Submit(reqFrame); err != nil {
			t.Fatal(err)
		}
		drain(primary)
		deliver(backup, 0, primaryBus.lastReplica[:primaryBus.lastLen])
		deliver(primary, 1, backupBus.lastReplica[:backupBus.lastLen])
		primary.sendCommit(clock.Now())
		deliver(backup, 0, primaryBus.lastReplica[:primaryBus.lastLen])
		lastReply, _, decodeReason := protocol.DecodeFrame(primaryBus.lastClient[:primaryBus.lastClientL], primary.config.Group, uint32(primary.config.Cluster.MessageSizeMax), 3, primary.config.Cluster.ChecksumAlgorithm)
		if decodeReason != protocol.RejectNone {
			t.Fatalf("reply decode reason=%v", decodeReason)
		}
		parent = replyContext(&lastReply)
		nextReq++
	}

	t.Run("duplicate_request", func(t *testing.T) {
		allocs := testing.AllocsPerRun(50, func() {
			frame, acquireErr := inboundPool.AcquireEncoded(dupReqBytes[:dupReqLen])
			if acquireErr != nil {
				t.Fatal(acquireErr)
			}
			if bindErr := frame.BindClient(); bindErr != nil {
				t.Fatal(bindErr)
			}
			if submitErr := primary.Submit(frame); submitErr != nil {
				t.Fatal(submitErr)
			}
			drain(primary)
		})
		if allocs != 0 {
			t.Fatalf("duplicate request path allocated %.2f times/op, want 0", allocs)
		}
	})

	t.Run("commit_heartbeat", func(t *testing.T) {
		allocs := testing.AllocsPerRun(50, func() {
			clock.monotonic += uint64(time.Millisecond)
			primary.sendCommit(clock.Now())
			frame, acquireErr := inboundPool.AcquireEncoded(primaryBus.lastReplica[:primaryBus.lastLen])
			if acquireErr != nil {
				t.Fatal(acquireErr)
			}
			if bindErr := frame.BindReplica(backup.membership.Members[0], 0); bindErr != nil {
				t.Fatal(bindErr)
			}
			if submitErr := backup.Submit(frame); submitErr != nil {
				t.Fatal(submitErr)
			}
			drain(backup)
		})
		if allocs != 0 {
			t.Fatalf("commit heartbeat path allocated %.2f times/op, want 0", allocs)
		}
	})

	t.Run("timer_tick", func(t *testing.T) {
		allocs := testing.AllocsPerRun(50, func() {
			clock.monotonic += uint64(primary.config.Process.Tick)
			if err := primary.Tick(); err != nil {
				t.Fatal(err)
			}
			if err := backup.Tick(); err != nil {
				t.Fatal(err)
			}
		})
		if allocs != 0 {
			t.Fatalf("timer tick path allocated %.2f times/op, want 0", allocs)
		}
	})

	t.Run("normal_prepare_and_prepare_ok", func(t *testing.T) {
		// Stay well below compaction boundary to measure steady-state replication only.
		allocs := testing.AllocsPerRun(10, func() {
			sendAppOp()
		})
		if allocs != 0 {
			t.Fatalf("normal prepare/prepare_ok path allocated %.2f times/op, want 0", allocs)
		}
	})
}
