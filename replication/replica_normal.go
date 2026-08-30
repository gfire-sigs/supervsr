package replication

import (
	"encoding/binary"
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func (replica *Replica) handleMessage(message *Message) bool {
	frame, err := message.Bytes()
	if err != nil {
		replica.metrics.framesRejected.Add(1)
		return false
	}
	header, body, reason := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
	if reason != protocol.RejectNone {
		replica.metrics.framesRejected.Add(1)
		if reason == protocol.RejectCommand {
			replica.fail(ErrProtocolInvariant)
		}
		return false
	}
	source, _, sender := message.Source()
	context := replica.validationContext(source, sender)
	if semanticReason := protocol.ValidateSemantics(&header, body, context); semanticReason != protocol.RejectNone {
		if eviction, reflect := replica.invalidRequestEviction(header, body); reflect {
			replica.sendEviction(requestClient(&header), eviction)
		}
		replica.metrics.framesRejected.Add(1)
		return false
	}
	if replica.handleHigherViewEvidence(header) {
		return false
	}
	sample := replica.deps.Clock.Now()
	switch header.Command {
	case protocol.CommandRequest:
		return replica.handleRequest(message, header, body, sample)
	case protocol.CommandPrepare:
		if replica.handleRepairPrepare(message, header, sample) {
			return true
		}
		return replica.handlePrepare(message, header, sample)
	case protocol.CommandPrepareOK:
		replica.handlePrepareOK(header)
	case protocol.CommandCommit:
		replica.handleCommit(header, sample)
	case protocol.CommandExitView:
		replica.handleExitView(header)
	case protocol.CommandJoinView:
		replica.handleJoinView(header, body)
	case protocol.CommandView:
		replica.handleView(header, body)
	case protocol.CommandHeaders:
		replica.handleHeaders(header, body)
	case protocol.CommandGetView:
		replica.handleGetView(header)
	case protocol.CommandGetHeaders:
		replica.handleGetHeaders(header)
	case protocol.CommandGetPrepare:
		replica.handleGetPrepare(header)
	case protocol.CommandGetReply:
		replica.handleGetReply(header)
	case protocol.CommandGetBlocks:
		replica.handleGetBlocks(header, body)
	case protocol.CommandBlock:
		replica.handleBlock(header, body)
	case protocol.CommandClientPing:
		replica.handleClientPing(header)
	case protocol.CommandPing:
		replica.handlePing(header, body)
	case protocol.CommandPong:
		replica.handlePong(header, sample)
	}
	return false
}

func (replica *Replica) validationContext(source protocol.FrameSource, sender protocol.ReplicaIndex) protocol.ValidationContext {
	return protocol.ValidationContext{
		Authenticated: source != protocol.FrameSourceUnbound, ReplicaSource: source == protocol.FrameSourceReplica, Sender: sender,
		ActiveCount: replica.membership.ActiveCount, MemberCount: replica.membership.ActiveCount + replica.membership.StandbyCount,
		PipelineMax: uint8(replica.config.Cluster.PipelineMax), ReleaseHistoryMax: uint16(replica.config.Cluster.ReleaseHistoryMax),
		ApplicationBatchSizeMax: uint32(replica.config.Cluster.ApplicationBatchSizeMax),
		ApplicationReplySizeMax: uint32(replica.config.Cluster.ApplicationReplySizeMax),
		RepairRequestsMax:       replica.config.Process.RepairRequestsMax,
		CurrentRelease:          replica.config.CurrentRelease, ClientReleaseMin: replica.config.ClientReleaseMin,
		Group: replica.config.Group, MessageSizeMax: uint32(replica.config.Cluster.MessageSizeMax),
	}
}

func (replica *Replica) handleRequest(message *Message, header protocol.Header, body []byte, sample TimeSample) bool {
	if replica.status != StatusNormal || replica.durableView != replica.view {
		replica.metrics.requestsDropped.Add(1)
		return false
	}
	if header.View > replica.view {
		replica.metrics.requestsDropped.Add(1)
		return false
	}
	if replica.membership.ActiveCount > 1 && !sample.Synchronized {
		replica.metrics.requestsDropped.Add(1)
		return false
	}
	if header.Release < replica.config.ClientReleaseMin {
		replica.sendEviction(requestClient(&header), protocol.EvictionClientReleaseTooLow)
		return false
	}
	operation := protocol.Operation(header.Fields[68])
	client := requestClient(&header)
	if operation == protocol.OperationRegister {
		if session, found := replica.sessions.Session(client); found {
			cached, _, ready := replica.sessions.Reply(client, session, 0)
			if ready && replyRequestChecksum(&cached) == header.HeaderChecksum {
				replica.sendCachedReply(client, session, 0)
			} else {
				replica.metrics.clientForks.Add(1)
			}
			return false
		}
	} else {
		decision := replica.sessions.Decide(SessionRequest{
			Client:          client,
			Session:         protocol.Session(binary.LittleEndian.Uint64(header.Fields[48:56])),
			Request:         protocol.RequestNo(binary.LittleEndian.Uint32(header.Fields[64:68])),
			Release:         header.Release,
			RequestChecksum: header.HeaderChecksum,
			Parent:          requestParent(&header),
		})
		switch decision {
		case SessionDuplicate:
			replica.sendCachedReply(client, protocol.Session(binary.LittleEndian.Uint64(header.Fields[48:56])), protocol.RequestNo(binary.LittleEndian.Uint32(header.Fields[64:68])))
			return false
		case SessionClientFork:
			replica.metrics.clientForks.Add(1)
			return false
		case SessionNoSession:
			session := protocol.Session(binary.LittleEndian.Uint64(header.Fields[48:56]))
			if protocol.Op(session) > replica.commitMin {
				replica.metrics.requestsDropped.Add(1)
				return false
			}
			replica.sendEviction(client, protocol.EvictionNoSession)
			return false
		case SessionTooLow:
			replica.sendEviction(client, protocol.EvictionSessionTooLow)
			return false
		case SessionReleaseMismatch:
			replica.sendEviction(client, protocol.EvictionSessionReleaseMismatch)
			return false
		case SessionAdmit:
		default:
			replica.metrics.requestsDropped.Add(1)
			return false
		}
	}
	if replica.upgradeTarget != 0 {
		replica.metrics.requestsDropped.Add(1)
		return false
	}
	if operation >= protocol.OperationApplicationMin {
		result := replica.deps.StateMachine.Validate(ValidateInput{Operation: operation, Body: body})
		if eviction, reject := validationEviction(result); reject {
			replica.sendEviction(client, eviction)
			replica.metrics.framesRejected.Add(1)
			return false
		}
	}
	if !replica.isPrimary() {
		replica.metrics.requestsDropped.Add(1)
		return false
	}
	requestNo := protocol.RequestNo(binary.LittleEndian.Uint32(header.Fields[64:68]))
	if replica.pipelineConflict(client, requestNo, header.HeaderChecksum) {
		return false
	}
	if replica.pipelineLen == uint32(len(replica.pipeline)) {
		return replica.queueRequest(message, sample)
	}
	if err := replica.createPrepare(header, body, sample); err != nil {
		if errors.Is(err, ErrIOBackpressure) || errors.Is(err, protocol.ErrFramePoolEmpty) {
			return replica.queueRequest(message, sample)
		}
		replica.fail(err)
	}
	return false
}

func (replica *Replica) invalidRequestEviction(header protocol.Header, body []byte) (protocol.EvictionReason, bool) {
	if header.Command != protocol.CommandRequest || header.Author != 0 || header.View > replica.view {
		return protocol.EvictionReserved, false
	}
	if replica.status != StatusNormal || replica.durableView != replica.view {
		return protocol.EvictionReserved, false
	}
	if replica.membership.ActiveCount > 1 && !replica.deps.Clock.Now().Synchronized {
		return protocol.EvictionReserved, false
	}
	fields := header.Fields[:]
	if !zeroBytes(fields[16:32]) || !zeroBytes(fields[69:72]) || !zeroBytes(fields[76:]) || requestClient(&header).IsZero() {
		return protocol.EvictionReserved, false
	}
	operation := protocol.Operation(fields[68])
	session := binary.LittleEndian.Uint64(fields[48:56])
	request := binary.LittleEndian.Uint32(fields[64:68])
	if operation == protocol.OperationRegister {
		if !zeroBytes(fields[:16]) || session != 0 || request != 0 {
			return protocol.EvictionReserved, false
		}
	} else if session == 0 || request == 0 {
		return protocol.EvictionReserved, false
	}
	switch {
	case header.Release < replica.config.ClientReleaseMin:
		return protocol.EvictionClientReleaseTooLow, true
	case header.Release > replica.config.CurrentRelease:
		return protocol.EvictionClientReleaseTooHigh, true
	case !validClientOperation(operation):
		return protocol.EvictionInvalidOperation, true
	case uint32(len(body)) > uint32(replica.config.Cluster.ApplicationBatchSizeMax):
		return protocol.EvictionInvalidBodySize, true
	case !validClientBody(operation, body):
		return protocol.EvictionInvalidBody, true
	default:
		return protocol.EvictionReserved, false
	}
}

func validClientBody(operation protocol.Operation, body []byte) bool {
	switch operation {
	case protocol.OperationRegister:
		return (len(body) == 0 || len(body) == 256) && zeroBytes(body)
	case protocol.OperationNoop:
		return len(body) == 0
	case protocol.OperationReconfigure:
		return len(body) == 256 && binary.LittleEndian.Uint32(body[252:]) == 0
	default:
		return operation >= protocol.OperationApplicationMin
	}
}
func validClientOperation(operation protocol.Operation) bool {
	return operation == protocol.OperationRegister ||
		operation == protocol.OperationNoop ||
		operation == protocol.OperationReconfigure ||
		operation >= protocol.OperationApplicationMin
}

func validationEviction(result ValidationResult) (protocol.EvictionReason, bool) {
	switch result {
	case ValidationOK:
		return protocol.EvictionReserved, false
	case ValidationInvalidOperation:
		return protocol.EvictionInvalidOperation, true
	case ValidationInvalidBody:
		return protocol.EvictionInvalidBody, true
	case ValidationInvalidBodySize:
		return protocol.EvictionInvalidBodySize, true
	default:
		return protocol.EvictionReserved, false
	}
}

func (replica *Replica) sendEviction(client protocol.ClientID, reason protocol.EvictionReason) {
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	header := protocol.Header{
		Group: replica.config.Group, View: replica.logView, Release: replica.config.CurrentRelease,
		Protocol: protocol.ProtocolVersion, Command: protocol.CommandEviction, Author: replica.local,
	}
	copy(header.Fields[:16], client[:])
	header.Fields[127] = byte(reason)
	if message.Seal(&header) == nil {
		replica.deps.MessageBus.SendClient(client, message)
	}
	message.Release()
}

func (replica *Replica) handleClientPing(ping protocol.Header) {
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	header := protocol.Header{
		Group: replica.config.Group, View: replica.logView, Release: replica.config.CurrentRelease,
		Protocol: protocol.ProtocolVersion, Command: protocol.CommandClientPong, Author: replica.local,
	}
	copy(header.Fields[:8], ping.Fields[16:24])
	var client protocol.ClientID
	copy(client[:], ping.Fields[:16])
	if message.Seal(&header) == nil {
		replica.deps.MessageBus.SendClient(client, message)
	}
	message.Release()
}

func (replica *Replica) createPrepare(request protocol.Header, requestBody []byte, sample TimeSample) error {
	operation := protocol.Operation(request.Fields[68])
	bodySize := uint32(len(requestBody))
	if operation == protocol.OperationRegister {
		bodySize = 256
	}
	prepare, err := replica.frames.Acquire(bodySize)
	if err != nil {
		return err
	}
	body, err := prepare.Body()
	if err != nil {
		prepare.Release()
		return err
	}
	switch operation {
	case protocol.OperationRegister:
		binary.LittleEndian.PutUint32(body[:4], uint32(replica.config.Cluster.ApplicationBatchSizeMax))
	case protocol.OperationReconfigure:
		copy(body, requestBody)
		result := ValidateReconfiguration(body, replica.membership, 0)
		binary.LittleEndian.PutUint32(body[252:], uint32(result))
	default:
		copy(body, requestBody)
	}
	timestamp := max(uint64(1), sample.Wall, replica.lastPrepareTimestamp+1, replica.lastCommitTimestamp+1)
	op := replica.headOp + 1
	header := protocol.Header{
		Group:    replica.config.Group,
		View:     replica.view,
		Release:  request.Release,
		Protocol: protocol.ProtocolVersion,
		Command:  protocol.CommandPrepare,
		Author:   replica.membership.Primary(replica.view),
	}
	copy(header.Fields[0:16], replica.headChecksum[:])
	copy(header.Fields[32:48], request.HeaderChecksum[:])
	copy(header.Fields[64:80], replica.checkpointID[:])
	copy(header.Fields[80:96], request.Fields[32:48])
	binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(op))
	binary.LittleEndian.PutUint64(header.Fields[104:112], uint64(replica.commitMin))
	binary.LittleEndian.PutUint64(header.Fields[112:120], timestamp)
	copy(header.Fields[120:124], request.Fields[64:68])
	header.Fields[124] = byte(operation)
	if err := prepare.Seal(&header); err != nil {
		prepare.Release()
		return err
	}
	if !replica.acceptPrepare(prepare, header) {
		prepare.Release()
		return ErrReplicaBackpressure
	}
	replica.metrics.preparesCreated.Add(1)
	replica.deps.MessageBus.BroadcastReplicas(prepare)
	return nil
}

func (replica *Replica) handlePrepare(message *Message, header protocol.Header, sample TimeSample) bool {
	if replica.status != StatusNormal || header.View != replica.view || header.Author != replica.membership.Primary(header.View) {
		return false
	}
	replica.failureDetector.Signal(sample.Monotonic)
	op := prepareOp(&header)
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if prepareOp(&entry.header) == op {
			return false
		}
	}
	return replica.acceptPrepare(message, header)
}

func (replica *Replica) acceptPrepare(message *Message, header protocol.Header) bool {
	invalidCapacity := replica.pipelineLen == uint32(len(replica.pipeline)) || replica.io.Available() == 0
	invalidChain := prepareOp(&header) != replica.headOp+1 || prepareParent(&header) != replica.headChecksum
	invalidProgress := prepareCommit(&header) > replica.commitMin || prepareTimestamp(&header) <= replica.lastPrepareTimestamp
	if invalidCapacity || invalidChain || invalidProgress {
		return false
	}
	frame, err := message.Bytes()
	if err != nil {
		return false
	}
	handle, err := replica.io.Submit(IOOperation{
		Kind:            IOWALAppend,
		Buffer:          frame,
		WAL:             replica.wal,
		ReusableThrough: replica.checkpoint.PrepareOp(),
	})
	if err != nil {
		return false
	}
	entry, ok := replica.pushPipeline(message, header)
	if !ok {
		replica.io.Cancel(handle)
		return false
	}
	entry.io = handle
	entry.ioKind = IOWALAppend
	replica.lastPrepareTimestamp = prepareTimestamp(&header)
	return true
}

func (replica *Replica) handleIOCompletion(completion IOCompletion) {
	if replica.handleStateSyncPersistence(completion) {
		return
	}
	if replica.handleViewPersistence(completion) {
		return
	}
	if replica.handleBlockRepairIO(completion) {
		return
	}
	for index := range replica.repairReads {
		read := &replica.repairReads[index]
		if !read.busy || read.handle != completion.Handle || completion.Kind != IORead {
			continue
		}
		replica.finishRepairRead(read, completion)
		return
	}
	if replica.repairWrite.busy && replica.repairWrite.handle == completion.Handle && completion.Kind == IOWALAppend {
		replica.finishRepairWrite(completion)
		return
	}
	for index := range replica.duplicateReads {
		read := &replica.duplicateReads[index]
		if !read.busy || read.handle != completion.Handle || completion.Kind != IOReplyRead {
			continue
		}
		replica.finishDuplicateRead(read, completion)
		return
	}
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if entry.io != completion.Handle || entry.ioKind != completion.Kind {
			continue
		}
		entry.io = IOHandle{}
		entry.ioKind = 0
		if completion.Err != nil {
			if completion.Kind == IOReplyWrite {
				replica.sessions.Abort(entry.replyPlan)
			}
			replica.metrics.storageFailures.Add(1)
			replica.fail(completion.Err)
			return
		}
		switch completion.Kind {
		case IOWALAppend:
			entry.durable = true
			replica.metrics.preparesDurable.Add(1)
			if replica.isPrimary() {
				replica.countPrepareAck(entry, replica.local)
			} else {
				replica.sendPrepareOK(entry)
			}
		case IOReplyWrite:
			replica.finishCommit(entry)
		case IOSuperblockPersist:
			replica.finishCheckpointPersistence(entry)
		default:
			replica.fail(ErrReplicaInvariant)
		}
		return
	}
	replica.metrics.staleCompletions.Add(1)
}

func (replica *Replica) sendPrepareOK(entry *pipelineEntry) {
	message, err := replica.frames.Acquire(0)
	if err != nil {
		replica.fail(err)
		return
	}
	header := protocol.Header{
		Group:    replica.config.Group,
		View:     replica.view,
		Protocol: protocol.ProtocolVersion,
		Command:  protocol.CommandPrepareOK,
		Author:   replica.local,
	}
	copy(header.Fields[0:16], entry.header.Fields[0:16])
	copy(header.Fields[32:48], entry.header.HeaderChecksum[:])
	copy(header.Fields[64:96], entry.header.Fields[64:96])
	copy(header.Fields[96:104], entry.header.Fields[96:104])
	binary.LittleEndian.PutUint64(header.Fields[104:112], uint64(replica.commitMin))
	copy(header.Fields[112:125], entry.header.Fields[112:125])
	if err := message.Seal(&header); err != nil {
		message.Release()
		replica.fail(err)
		return
	}
	replica.deps.MessageBus.SendReplica(replica.membership.Primary(replica.view), message)
	message.Release()
}

func (replica *Replica) handlePrepareOK(ack protocol.Header) {
	if replica.status != StatusNormal || !replica.isPrimary() || ack.View != replica.view || uint8(ack.Author) >= replica.membership.ActiveCount {
		return
	}
	op := protocol.Op(binary.LittleEndian.Uint64(ack.Fields[96:104]))
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if prepareOp(&entry.header) != op {
			continue
		}
		if !prepareOKMatches(&ack, &entry.header) {
			return
		}
		replica.countPrepareAck(entry, ack.Author)
		return
	}
}

func (replica *Replica) countPrepareAck(entry *pipelineEntry, author protocol.ReplicaIndex) {
	bit := uint16(1) << uint8(author)
	if entry.acks&bit != 0 {
		return
	}
	entry.acks |= bit
	replica.metrics.prepareAcks.Add(1)
	if countAckBits(entry.acks) >= replica.quorums.Replication {
		entry.quorum = true
	}
}

func (replica *Replica) handleCommit(header protocol.Header, sample TimeSample) {
	if replica.status != StatusNormal || replica.isPrimary() || header.View != replica.view || header.Author != replica.membership.Primary(replica.view) {
		return
	}
	monotonic := binary.LittleEndian.Uint64(header.Fields[64:72])
	if monotonic <= replica.lastPrimaryCommit {
		return
	}
	commit := protocol.Op(binary.LittleEndian.Uint64(header.Fields[56:64]))
	if local, _, _, found := replica.localHeaderEvidence(commit); found && local.HeaderChecksum != protocol.Checksum(header.Fields[:16]) {
		return
	}
	replica.lastPrimaryCommit = monotonic
	replica.failureDetector.Signal(sample.Monotonic)
	if commit > replica.commitMax {
		replica.commitMax = commit
	}
}

func (replica *Replica) advanceCommit() {
	recoveringCommit := replica.status == StatusRecoveringHead && replica.repairViewValid && replica.repairViewRebuilt
	invalidStatus := replica.status != StatusNormal && !recoveringCommit
	unavailable := replica.pipelineLen == 0 || replica.fatalErr != nil
	if invalidStatus || unavailable {
		return
	}
	entry := replica.pipelineEntry(0)
	op := prepareOp(&entry.header)
	ready := entry.durable && (replica.isPrimary() && entry.quorum || !replica.isPrimary() && replica.commitMax >= op)
	if !ready {
		return
	}
	switch entry.stage {
	case CommitStageIdle:
		entry.stage = CommitStageStart
		fallthrough
	case CommitStageStart:
		entry.stage = CommitStageCheckPrepare
		fallthrough
	case CommitStageCheckPrepare:
		replica.startPrefetch(entry)
	case CommitStageStall:
		entry.stage = CommitStageReplySetup
		fallthrough
	case CommitStageReplySetup:
		entry.stage = CommitStageExecute
		replica.executeEntry(entry)
	}
}

func (replica *Replica) startPrefetch(entry *pipelineEntry) {
	operation := prepareOperation(&entry.header)
	if operation < protocol.OperationApplicationMin && operation != protocol.OperationPulse {
		entry.stage = CommitStageStall
		replica.advanceCommit()
		return
	}
	frame, err := entry.prepare.Bytes()
	if err != nil {
		replica.fail(err)
		return
	}
	entry.stage = CommitStagePrefetch
	entry.completion.prepare(entry.generation, replica)
	result, err := replica.deps.StateMachine.StartPrefetch(PrefetchInput{
		Operation: operation,
		Body:      frame[protocol.HeaderSize:],
		Timestamp: prepareTimestamp(&entry.header),
		Op:        prepareOp(&entry.header),
		Release:   entry.header.Release,
	}, &entry.completion)
	if err != nil {
		replica.fail(errors.Join(ErrStateMachine, err))
		return
	}
	if token, ready := result.Value(); ready {
		entry.completion.release(entry.generation)
		entry.token = token
		entry.stage = CommitStageStall
		replica.advanceCommit()
	}
}

func (replica *Replica) executeEntry(entry *pipelineEntry) {
	operation := prepareOperation(&entry.header)
	client := prepareClient(&entry.header)
	if client.IsZero() {
		switch operation {
		case protocol.OperationPulse:
			if _, err := replica.commitApplication(entry, nil); err != nil {
				replica.fail(err)
				return
			}
		case protocol.OperationUpgrade:
			if err := replica.executeUpgrade(entry); err != nil {
				replica.fail(err)
				return
			}
		}
		replica.finishCommit(entry)
		return
	}
	bodyCapacity := uint32(replica.config.Cluster.ApplicationReplySizeMax)
	if operation == protocol.OperationRegister {
		bodyCapacity = 64
	}
	reply, err := replica.frames.Acquire(bodyCapacity)
	if err != nil {
		replica.fail(err)
		return
	}
	replyBody, err := reply.Body()
	if err != nil {
		reply.Release()
		replica.fail(err)
		return
	}
	replyLength := 0
	switch {
	case operation == protocol.OperationRegister:
		binary.LittleEndian.PutUint32(replyBody[:4], uint32(replica.config.Cluster.ApplicationBatchSizeMax))
		replyLength = 64
	case operation == protocol.OperationNoop:
		replyLength = 0
	case operation == protocol.OperationReconfigure:
		frame, frameErr := entry.prepare.Bytes()
		if frameErr != nil {
			err = frameErr
			break
		}
		copy(replyBody[:4], frame[protocol.HeaderSize+252:protocol.HeaderSize+256])
		replyLength = 4
	case operation >= protocol.OperationApplicationMin:
		replyLength, err = replica.commitApplication(entry, replyBody)
	default:
		err = ErrReplicaInvariant
	}
	if err != nil || replyLength < 0 || uint64(replyLength) > replica.config.Cluster.ApplicationReplySizeMax {
		reply.Release()
		replica.fail(errors.Join(ErrStateMachine, err))
		return
	}
	if err := reply.ResizeBody(uint32(replyLength)); err != nil {
		reply.Release()
		replica.fail(err)
		return
	}
	header, err := replica.buildReplyHeader(entry, reply, uint32(replyLength))
	if err != nil {
		reply.Release()
		replica.fail(err)
		return
	}
	session := protocol.Session(prepareOp(&entry.header))
	if operation != protocol.OperationRegister {
		var found bool
		session, found = replica.sessions.Session(client)
		if !found {
			reply.Release()
			replica.fail(errors.Join(ErrReplicaInvariant, ErrSessionEncoding))
			return
		}
	}
	plan, err := replica.sessions.PlanCommit(header, session)
	if err != nil {
		reply.Release()
		replica.fail(err)
		return
	}
	entry.reply = reply
	entry.replyHeader = header
	entry.replyPlan = plan
	if replyLength == 0 {
		replica.finishCommit(entry)
		return
	}
	frame, err := reply.Bytes()
	if err != nil {
		replica.sessions.Abort(plan)
		replica.fail(err)
		return
	}
	handle, err := replica.io.Submit(IOOperation{Kind: IOReplyWrite, Offset: uint64(plan.Slot), Buffer: frame, ReplyStore: replica.replies})
	if err != nil {
		replica.sessions.Abort(plan)
		replica.fail(err)
		return
	}
	entry.io = handle
	entry.ioKind = IOReplyWrite
}

func (replica *Replica) commitApplication(entry *pipelineEntry, reply []byte) (int, error) {
	frame, err := entry.prepare.Bytes()
	if err != nil {
		return 0, err
	}
	return replica.deps.StateMachine.Commit(CommitInput{
		Operation: prepareOperation(&entry.header),
		Body:      frame[protocol.HeaderSize:],
		Timestamp: prepareTimestamp(&entry.header),
		Op:        prepareOp(&entry.header),
		Release:   entry.header.Release,
	}, entry.token, reply)
}

func (replica *Replica) buildReplyHeader(entry *pipelineEntry, reply *Message, replyLength uint32) (protocol.Header, error) {
	body, err := reply.Body()
	if err != nil {
		return protocol.Header{}, err
	}
	header := protocol.Header{
		BodyChecksum: protocol.ChecksumBytes(body[:replyLength]),
		Group:        replica.config.Group,
		Size:         protocol.HeaderSize + replyLength,
		View:         entry.header.View,
		Release:      entry.header.Release,
		Protocol:     protocol.ProtocolVersion,
		Command:      protocol.CommandReply,
		Author:       replica.membership.Primary(entry.header.View),
	}
	copy(header.Fields[0:16], entry.header.Fields[32:48])
	copy(header.Fields[64:80], entry.header.Fields[80:96])
	copy(header.Fields[80:88], entry.header.Fields[96:104])
	copy(header.Fields[88:96], entry.header.Fields[96:104])
	copy(header.Fields[96:104], entry.header.Fields[112:120])
	copy(header.Fields[104:109], entry.header.Fields[120:125])
	var encoded [protocol.HeaderSize]byte
	if err := protocol.EncodeHeader(encoded[:], &header); err != nil {
		return protocol.Header{}, err
	}
	context := protocol.ChecksumBytes(encoded[protocol.HeaderChecksumFrom:])
	copy(header.Fields[32:48], context[:])
	if err := reply.Seal(&header); err != nil {
		return protocol.Header{}, err
	}
	return header, nil
}

func (replica *Replica) finishCommit(entry *pipelineEntry) {
	op := prepareOp(&entry.header)
	if entry.reply != nil {
		if err := replica.sessions.CommitAt(entry.replyPlan, entry.replyHeader, entry.replyPlan.Session); err != nil {
			replica.sessions.Abort(entry.replyPlan)
			replica.fail(err)
			return
		}
		replica.sendReply(entry)
	}
	replica.commitMin = op
	if replica.commitMax < op {
		replica.commitMax = op
	}
	replica.lastCommitTimestamp = prepareTimestamp(&entry.header)
	replica.metrics.operationsCommitted.Add(1)
	replica.trackUpgradeWindow(entry)
	replica.continueCommitMaintenance(entry)
}

func (replica *Replica) sendReply(entry *pipelineEntry) {
	primary := replica.membership.Primary(entry.header.View)
	sender := replica.local == primary
	if replica.membership.ActiveCount > 1 && replica.local == replyBackup(prepareOp(&entry.header), primary, replica.membership.ActiveCount) {
		sender = true
	}
	if sender {
		replica.deps.MessageBus.SendClient(prepareClient(&entry.header), entry.reply)
	}
}

func (replica *Replica) sendCachedReply(client protocol.ClientID, session protocol.Session, request protocol.RequestNo) {
	header, slot, found := replica.sessions.Reply(client, session, request)
	if !found {
		return
	}
	if header.Size == protocol.HeaderSize {
		replica.sendHeaderOnlyCachedReply(client, header)
		return
	}
	for index := range replica.duplicateReads {
		read := &replica.duplicateReads[index]
		if read.busy {
			continue
		}
		handle, err := replica.io.Submit(IOOperation{
			Kind:           IOReplyRead,
			Offset:         uint64(slot),
			Buffer:         read.buffer,
			ReplyStore:     replica.replies,
			ExpectedHeader: header,
		})
		if err != nil {
			return
		}
		read.busy = true
		read.handle = handle
		read.header = header
		read.client = client
		return
	}
}

func (replica *Replica) sendHeaderOnlyCachedReply(client protocol.ClientID, header protocol.Header) {
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	header.View = replica.logView
	if err := message.Seal(&header); err != nil {
		message.Release()
		return
	}
	replica.deps.MessageBus.SendClient(client, message)
	message.Release()
}

func (replica *Replica) finishDuplicateRead(read *duplicateRead, completion IOCompletion) {
	header := read.header
	client := read.client
	read.busy = false
	read.handle = IOHandle{}
	read.header = protocol.Header{}
	read.client = protocol.ClientID{}
	if completion.Err != nil || completion.Size < protocol.HeaderSize || completion.Size != uint64(header.Size) {
		replica.metrics.storageFailures.Add(1)
		return
	}
	message, err := replica.frames.Acquire(uint32(completion.Size - protocol.HeaderSize))
	if err != nil {
		return
	}
	body, err := message.Body()
	if err != nil {
		message.Release()
		return
	}
	copy(body, completion.Buffer[protocol.HeaderSize:completion.Size])
	header.View = replica.logView
	if err := message.Seal(&header); err != nil {
		message.Release()
		return
	}
	replica.deps.MessageBus.SendClient(client, message)
	message.Release()
}

func (replica *Replica) queueRequest(message *Message, sample TimeSample) bool {
	if replica.requestLen == uint32(len(replica.requestQueue)) {
		replica.metrics.requestsDropped.Add(1)
		return false
	}
	index := (replica.requestHead + replica.requestLen) % uint32(len(replica.requestQueue))
	replica.requestQueue[index] = queuedRequest{message: message, time: sample}
	replica.requestLen++
	return true
}

func (replica *Replica) dequeueRequest() {
	if replica.requestLen == 0 || replica.pipelineLen == uint32(len(replica.pipeline)) {
		return
	}
	queued := replica.requestQueue[replica.requestHead]
	replica.requestQueue[replica.requestHead] = queuedRequest{}
	replica.requestHead = (replica.requestHead + 1) % uint32(len(replica.requestQueue))
	replica.requestLen--
	frame, err := queued.message.Bytes()
	if err != nil {
		queued.message.Release()
		return
	}
	header, body, reason := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
	if reason != protocol.RejectNone || !replica.handleRequest(queued.message, header, body, queued.time) {
		queued.message.Release()
	}
}

func (replica *Replica) pipelineConflict(client protocol.ClientID, request protocol.RequestNo, checksum protocol.Checksum) bool {
	for offset := range replica.pipelineLen {
		header := &replica.pipelineEntry(offset).header
		if prepareClient(header) == client && prepareRequest(header) == request {
			if prepareRequestChecksum(header) != checksum {
				replica.metrics.clientForks.Add(1)
			}
			return true
		}
	}
	for offset := range replica.requestLen {
		queued := &replica.requestQueue[(replica.requestHead+offset)%uint32(len(replica.requestQueue))]
		frame, err := queued.message.Bytes()
		if err != nil {
			continue
		}
		header, _, reason := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
		queuedRequestNo := protocol.RequestNo(binary.LittleEndian.Uint32(header.Fields[64:68]))
		if reason == protocol.RejectNone && requestClient(&header) == client && queuedRequestNo == request {
			if header.HeaderChecksum != checksum {
				replica.metrics.clientForks.Add(1)
			}
			return true
		}
	}
	return false
}

func prepareOKMatches(ack, prepare *protocol.Header) bool {
	if binary.LittleEndian.Uint64(ack.Fields[96:104]) != uint64(prepareOp(prepare)) || binary.LittleEndian.Uint64(ack.Fields[112:120]) != prepareTimestamp(prepare) || binary.LittleEndian.Uint32(ack.Fields[120:124]) != uint32(prepareRequest(prepare)) || ack.Fields[124] != byte(prepareOperation(prepare)) {
		return false
	}
	return equal16(ack.Fields[0:16], prepare.Fields[0:16]) && equal16(ack.Fields[32:48], prepare.HeaderChecksum[:]) && equal16(ack.Fields[64:80], prepare.Fields[64:80]) && equal16(ack.Fields[80:96], prepare.Fields[80:96])
}

func requestClient(header *protocol.Header) protocol.ClientID {
	var client protocol.ClientID
	copy(client[:], header.Fields[32:48])
	return client
}

func requestParent(header *protocol.Header) protocol.Checksum {
	var parent protocol.Checksum
	copy(parent[:], header.Fields[0:16])
	return parent
}

func equal16(left, right []byte) bool {
	if len(left) != 16 || len(right) != 16 {
		return false
	}
	var difference byte
	for index := range 16 {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}

func replyBackup(op protocol.Op, primary protocol.ReplicaIndex, activeCount uint8) protocol.ReplicaIndex {
	if activeCount <= 1 {
		return primary
	}
	x := uint64(op) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	return protocol.ReplicaIndex((uint64(primary) + 1 + x%uint64(activeCount-1)) % uint64(activeCount))
}
