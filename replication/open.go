package replication

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type startupCompletionSink struct {
	ready chan *SMCompletion
}

func (sink *startupCompletionSink) enqueueSMCompletion(completion *SMCompletion) error {
	select {
	case sink.ready <- completion:
		return nil
	default:
		return ErrReplicaBackpressure
	}
}

func Open(ctx context.Context, config Config, dependencies Dependencies) (*Replica, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if dependencies.Storage == nil || dependencies.MessageBus == nil || dependencies.Clock == nil || dependencies.Entropy == nil || dependencies.StateMachine == nil {
		return nil, ErrInvalidConfiguration
	}
	validation := SuperblockValidation{
		Group:                 config.Group,
		Membership:            config.Membership,
		ConfigurationChecksum: config.Cluster.Fingerprint(),
		Cluster:               config.Cluster,
	}
	superblocks, err := OpenSuperblockStore(dependencies.Storage, validation)
	if err != nil {
		return nil, err
	}
	memberCount := config.Membership.ActiveCount + config.Membership.StandbyCount
	wal, err := NewWAL(dependencies.Storage, config.Cluster, config.Group, memberCount)
	if err != nil {
		return nil, err
	}
	durable := superblocks.Current()
	recovery, err := wal.Recover(durable.State.Checkpoint, durable.State.CommitMax, config.Process)
	if err != nil {
		return nil, err
	}
	if config.Membership.ActiveCount == 1 && recovery.FaultySlots == 0 {
		durable.State.CommitMax = recovery.HeadOp
	}
	replyStore, err := NewReplyStore(dependencies.Storage, config.Cluster, config.Group, memberCount)
	if err != nil {
		return nil, err
	}
	sessions, err := NewSessionTable(SessionTableConfig{
		ClientsMax:              uint32(config.Cluster.ClientsMax),
		Group:                   config.Group,
		ActiveCount:             config.Membership.ActiveCount,
		MessageSizeMax:          uint32(config.Cluster.MessageSizeMax),
		ApplicationReplySizeMax: uint32(config.Cluster.ApplicationReplySizeMax),
	})
	if err != nil {
		return nil, err
	}
	blocks, err := openBlockRuntime(dependencies.Storage, config, durable.State.Checkpoint)
	if err != nil {
		return nil, err
	}
	if err := loadSessionTrailer(blocks, durable.State.Checkpoint, sessions); err != nil {
		return nil, err
	}
	startup := &startupCompletionSink{ready: make(chan *SMCompletion, 1)}
	if err := startOpenStateMachine(ctx, dependencies.StateMachine, durable.State.Checkpoint, startup); err != nil {
		return nil, err
	}
	commitMin := durable.State.Checkpoint.PrepareOp()
	if recovery.FaultySlots == 0 {
		commitMin, err = replayCommitted(ctx, config, dependencies.StateMachine, wal, replyStore, sessions, startup, commitMin, durable.State.CommitMax)
		if err != nil {
			return nil, err
		}
	}
	initial, err := deriveInitialState(config, durable, recovery, commitMin, wal, superblocks, memberCount)
	if err != nil {
		return nil, err
	}
	replica, err := newReplicaWithBlocks(config, dependencies, initial, wal, replyStore, sessions, superblocks, blocks)
	if err != nil {
		return nil, err
	}
	if err := loadOpenSuffix(replica, recovery, commitMin); err != nil {
		_ = replica.Close(context.Background())
		return nil, err
	}
	return replica, nil
}

func deriveInitialState(config Config, durable Superblock, recovery WALRecoveryReport, commitMin protocol.Op, wal *WAL, superblocks *SuperblockStore, memberCount uint8) (ReplicaInitialState, error) {
	status, view, durableView, logView, headOp, err := deriveRecoveredStatus(config, durable, recovery, wal, superblocks)
	if err != nil {
		return ReplicaInitialState{}, err
	}
	committedHeader, err := recoveredCommittedHeader(config, durable, recovery, commitMin, wal, memberCount)
	if err != nil {
		return ReplicaInitialState{}, err
	}
	initial := ReplicaInitialState{
		Status:      status,
		View:        view,
		DurableView: durableView,
		LogView:     logView,
		HeadOp:      commitMin,
		CommitMin:   commitMin,
		CommitMax:   durable.State.CommitMax,
		Checkpoint:  durable.State.Checkpoint,
		HeadHeader:  committedHeader,
	}
	if status == StatusRecoveringHead {
		initial.HeadOp = headOp
		initial.HeadHeader = recovery.HeadHeader
	}
	return initial, nil
}

func deriveRecoveredStatus(config Config, durable Superblock, recovery WALRecoveryReport, wal *WAL, superblocks *SuperblockStore) (Status, protocol.View, protocol.View, protocol.View, protocol.Op, error) {
	view := durable.State.View
	logView := durable.State.LogView
	headOp := recovery.HeadOp
	if recovery.FaultySlots != 0 {
		return StatusRecoveringHead, view, view, logView, max(headOp, durable.State.CommitMax), nil
	}
	if view > logView {
		return StatusViewChange, view, view, logView, headOp, nil
	}
	local, _ := config.Membership.LocalIndex()
	if config.Membership.ActiveCount > 1 && config.Membership.Primary(view) != local {
		return StatusNormal, view, view, logView, headOp, nil
	}
	if view == protocol.MaxView {
		return 0, 0, 0, 0, 0, ErrReplicaInvariant
	}
	view++
	status := StatusViewChange
	if config.Membership.ActiveCount == 1 {
		logView = view
		status = StatusNormal
	}
	if err := persistRecoveredView(superblocks, wal, recovery.HeadOp, view, logView, durable.State.CommitMax, config.Cluster); err != nil {
		return 0, 0, 0, 0, 0, err
	}
	return status, view, view, logView, headOp, nil
}

func recoveredCommittedHeader(config Config, durable Superblock, recovery WALRecoveryReport, commitMin protocol.Op, wal *WAL, memberCount uint8) (protocol.Header, error) {
	if commitMin == recovery.HeadOp {
		return recovery.HeadHeader, nil
	}
	if commitMin != durable.State.Checkpoint.PrepareOp() {
		header, found := wal.RecoveredHeader(commitMin)
		if !found {
			return protocol.Header{}, ErrWALRecovery
		}
		return header, nil
	}
	header, reason := protocol.DecodeHeader(durable.State.Checkpoint.Header[:], config.Group, uint32(config.Cluster.MessageSizeMax), memberCount)
	if reason != protocol.RejectNone {
		return protocol.Header{}, ErrWALRecovery
	}
	return header, nil
}

func loadOpenSuffix(replica *Replica, recovery WALRecoveryReport, commitMin protocol.Op) error {
	if recovery.FaultySlots != 0 {
		return nil
	}
	return replica.loadRecoveredSuffix(commitMin+1, recovery.HeadOp)
}

func startOpenStateMachine(ctx context.Context, machine StateMachine, checkpoint CheckpointState, sink *startupCompletionSink) error {
	var completion SMCompletion
	const generation = 1
	completion.prepare(generation, sink)
	result, err := machine.StartOpen(OpenCheckpointInput{State: checkpoint}, &completion)
	if err != nil {
		return errors.Join(ErrStateMachine, err)
	}
	if result.IsReady() {
		completion.release(generation)
		return nil
	}
	completed, err := waitStartupCompletion(ctx, sink, &completion, generation, SMCompletionOpen)
	if err != nil {
		return err
	}
	return completed.Err
}

func replayCommitted(ctx context.Context, config Config, machine StateMachine, wal *WAL, replies *ReplyStore, sessions *SessionTable, sink *startupCompletionSink, from, target protocol.Op) (protocol.Op, error) {
	if target < from {
		return from, ErrWALRecovery
	}
	prepareBuffer, err := NewAlignedBuffer(wal.Layout().PrepareStride, SectorSize)
	if err != nil {
		return from, err
	}
	replyFrame := make([]byte, int(config.Cluster.MessageSizeMax))
	for op := from + 1; op <= target; op++ {
		if err := ctx.Err(); err != nil {
			return op - 1, err
		}
		frame, err := wal.ReadPrepare(op, prepareBuffer)
		if err != nil {
			return op - 1, err
		}
		header, body, reason := protocol.DecodeFrame(frame, config.Group, uint32(config.Cluster.MessageSizeMax), config.Membership.ActiveCount+config.Membership.StandbyCount)
		if reason != protocol.RejectNone {
			return op - 1, ErrWALRecovery
		}
		token, err := replayPrefetch(ctx, machine, sink, header, body)
		if err != nil {
			return op - 1, err
		}
		if err := replayExecute(config, machine, replies, sessions, header, body, token, replyFrame); err != nil {
			return op - 1, err
		}
	}
	return target, nil
}

func replayPrefetch(ctx context.Context, machine StateMachine, sink *startupCompletionSink, header protocol.Header, body []byte) (PrefetchToken, error) {
	operation := prepareOperation(&header)
	if operation < protocol.OperationApplicationMin && operation != protocol.OperationPulse {
		return 0, nil
	}
	var completion SMCompletion
	generation := uint64(prepareOp(&header)) + 1
	completion.prepare(generation, sink)
	result, err := machine.StartPrefetch(PrefetchInput{Operation: operation, Body: body, Timestamp: prepareTimestamp(&header), Op: prepareOp(&header), Release: header.Release}, &completion)
	if err != nil {
		return 0, errors.Join(ErrStateMachine, err)
	}
	if token, ready := result.Value(); ready {
		completion.release(generation)
		return token, nil
	}
	completed, err := waitStartupCompletion(ctx, sink, &completion, generation, SMCompletionPrefetch)
	if err != nil {
		return 0, err
	}
	return completed.Prefetch, completed.Err
}

func waitStartupCompletion(ctx context.Context, sink *startupCompletionSink, expected *SMCompletion, generation uint64, kind SMCompletionKind) (SMResult, error) {
	select {
	case completion := <-sink.ready:
		if completion != expected {
			return SMResult{}, ErrCompletionGeneration
		}
		result, ok := completion.take(generation)
		if !ok || result.Kind != kind {
			return SMResult{}, ErrCompletionGeneration
		}
		return result, nil
	case <-ctx.Done():
		return SMResult{}, ctx.Err()
	}
}

func replayExecute(config Config, machine StateMachine, replies *ReplyStore, sessions *SessionTable, prepare protocol.Header, body []byte, token PrefetchToken, replyFrame []byte) error {
	operation := prepareOperation(&prepare)
	client := prepareClient(&prepare)
	if client.IsZero() {
		if operation == protocol.OperationPulse {
			_, err := machine.Commit(CommitInput{Operation: operation, Body: body, Timestamp: prepareTimestamp(&prepare), Op: prepareOp(&prepare), Release: prepare.Release}, token, nil)
			return err
		}
		return nil
	}
	clear(replyFrame)
	replyBody := replyFrame[protocol.HeaderSize:]
	replyLength := 0
	var err error
	switch {
	case operation == protocol.OperationRegister:
		binary.LittleEndian.PutUint32(replyBody[:4], uint32(config.Cluster.ApplicationBatchSizeMax))
		replyLength = 64
	case operation == protocol.OperationReconfigure:
		copy(replyBody[:4], body[len(body)-4:])
		replyLength = 4
	case operation == protocol.OperationNoop:
	case operation >= protocol.OperationApplicationMin:
		replyLength, err = machine.Commit(CommitInput{Operation: operation, Body: body, Timestamp: prepareTimestamp(&prepare), Op: prepareOp(&prepare), Release: prepare.Release}, token, replyBody)
	default:
		return ErrReplicaInvariant
	}
	if err != nil || replyLength < 0 || uint64(replyLength) > config.Cluster.ApplicationReplySizeMax {
		return errors.Join(ErrStateMachine, err)
	}
	reply := protocol.Header{Group: config.Group, View: prepare.View, Release: prepare.Release, Protocol: protocol.ProtocolVersion, Command: protocol.CommandReply, Author: config.Membership.Primary(prepare.View)}
	copy(reply.Fields[0:16], prepare.Fields[32:48])
	copy(reply.Fields[64:80], prepare.Fields[80:96])
	copy(reply.Fields[80:88], prepare.Fields[96:104])
	copy(reply.Fields[88:96], prepare.Fields[96:104])
	copy(reply.Fields[96:104], prepare.Fields[112:120])
	copy(reply.Fields[104:109], prepare.Fields[120:125])
	frame := replyFrame[:protocol.HeaderSize+replyLength]
	if err := protocol.SealFrame(frame, &reply); err != nil {
		return err
	}
	context := protocol.ChecksumBytes(frame[protocol.HeaderChecksumFrom:protocol.HeaderSize])
	copy(reply.Fields[32:48], context[:])
	if err := protocol.SealFrame(frame, &reply); err != nil {
		return err
	}
	session := protocol.Session(prepareOp(&prepare))
	if operation != protocol.OperationRegister {
		var found bool
		session, found = sessions.Session(client)
		if !found {
			return ErrSessionEncoding
		}
	}
	plan, err := sessions.PlanCommit(reply, session)
	if err != nil {
		return err
	}
	if replyLength != 0 {
		if err := replies.Write(plan.Slot, frame); err != nil {
			sessions.Abort(plan)
			return err
		}
	}
	if err := sessions.CommitAt(plan, reply, session); err != nil {
		sessions.Abort(plan)
		return err
	}
	return nil
}

func persistRecoveredView(store *SuperblockStore, wal *WAL, head protocol.Op, view, logView protocol.View, commitMax protocol.Op, config ClusterConfig) error {
	next := store.Current()
	next.ParentChecksum = next.Checksum
	next.Sequence++
	next.State.View = view
	next.State.LogView = logView
	next.State.CommitMax = commitMax
	count := uint32(0)
	for op := head; ; op-- {
		header, found := wal.RecoveredHeader(op)
		if found {
			var encoded [protocol.HeaderSize]byte
			copyOfHeader := header
			if err := protocol.EncodeHeader(encoded[:], &copyOfHeader); err != nil {
				return err
			}
			next.ViewHeaders[count] = encoded
			count++
		}
		if op == 0 || count == uint32(config.PipelineMax+1) {
			break
		}
	}
	if count == 0 {
		next.ViewHeaders[0] = next.State.Checkpoint.Header
		count = 1
	}
	next.ViewHeaderCount = count
	return store.Persist(next)
}

func (replica *Replica) loadRecoveredSuffix(first, last protocol.Op) error {
	if first > last {
		return nil
	}
	buffer, err := NewAlignedBuffer(replica.wal.Layout().PrepareStride, SectorSize)
	if err != nil {
		return err
	}
	for op := first; op <= last; op++ {
		frame, err := replica.wal.ReadPrepare(op, buffer)
		if err != nil {
			return err
		}
		header, body, reason := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
		if reason != protocol.RejectNone {
			return ErrWALRecovery
		}
		message, err := replica.frames.Acquire(uint32(len(body)))
		if err != nil {
			return err
		}
		messageBody, err := message.Body()
		if err != nil {
			message.Release()
			return err
		}
		copy(messageBody, body)
		if err := message.Seal(&header); err != nil {
			message.Release()
			return err
		}
		entry, ok := replica.pushPipeline(message, header)
		if !ok {
			message.Release()
			return ErrReplicaInvariant
		}
		entry.durable = true
		if replica.membership.ActiveCount == 1 {
			replica.countPrepareAck(entry, replica.local)
		}
	}
	return nil
}
