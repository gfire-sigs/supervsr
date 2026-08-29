package replication

import (
	"encoding/binary"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type repairReadKind uint8

const (
	repairReadPrepare repairReadKind = iota + 1
	repairReadReply
	repairReadBlock
)

type repairRead struct {
	busy     bool
	kind     repairReadKind
	handle   IOHandle
	peer     protocol.ReplicaIndex
	op       protocol.Op
	client   protocol.ClientID
	checksum protocol.Checksum
	address  uint64
	buffer   []byte
}

func (replica *Replica) handleGetHeaders(request protocol.Header) {
	start := protocol.Op(binary.LittleEndian.Uint64(request.Fields[:8]))
	end := protocol.Op(binary.LittleEndian.Uint64(request.Fields[8:16]))
	bodyCapacity := min(64, (int(replica.config.Cluster.MessageSizeMax)-protocol.HeaderSize)/protocol.HeaderSize)
	if bodyCapacity == 0 {
		return
	}
	message, err := replica.frames.Acquire(uint32(bodyCapacity * protocol.HeaderSize))
	if err != nil {
		return
	}
	body, err := message.Body()
	if err != nil {
		message.Release()
		return
	}
	count := 0
	for op := start; op <= end && count < bodyCapacity; op++ {
		header, present, dirty, found := replica.localHeaderEvidence(op)
		if found && present && !dirty && prepareOperation(&header) != protocol.OperationReserved {
			if protocol.EncodeHeader(body[count*protocol.HeaderSize:(count+1)*protocol.HeaderSize], &header) != nil {
				message.Release()
				return
			}
			count++
		}
		if op == ^protocol.Op(0) {
			break
		}
	}
	if count == 0 {
		message.Release()
		return
	}
	if err := message.ResizeBody(uint32(count * protocol.HeaderSize)); err != nil {
		message.Release()
		return
	}
	header := protocol.Header{Group: replica.config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandHeaders, Author: replica.local}
	if message.Seal(&header) == nil {
		replica.deps.MessageBus.SendReplica(request.Author, message)
	}
	message.Release()
}

func (replica *Replica) handleGetPrepare(request protocol.Header) {
	op := protocol.Op(binary.LittleEndian.Uint64(request.Fields[32:40]))
	var checksum protocol.Checksum
	copy(checksum[:], request.Fields[:16])
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if prepareOp(&entry.header) != op {
			continue
		}
		if !entry.durable || !prepareRequestMatches(request, entry.header, checksum) {
			return
		}
		replica.deps.MessageBus.SendReplica(request.Author, entry.prepare)
		return
	}
	header, present, dirty, found := replica.localHeaderEvidence(op)
	if !found || !present || dirty || !prepareRequestMatches(request, header, checksum) {
		return
	}
	slot := uint64(op) % replica.config.Cluster.JournalSlots
	offset, ok := replica.wal.prepareOffset(slot)
	if !ok {
		return
	}
	replica.startRepairRead(repairReadPrepare, request.Author, op, protocol.ClientID{}, header.HeaderChecksum, offset, replica.wal.Layout().PrepareStride)
}

func prepareRequestMatches(request protocol.Header, candidate protocol.Header, checksum protocol.Checksum) bool {
	if request.View == 0 {
		return !checksum.IsZero() && candidate.HeaderChecksum == checksum
	}
	return checksum.IsZero() && candidate.View == request.View
}

func (replica *Replica) handleGetReply(request protocol.Header) {
	var checksum protocol.Checksum
	copy(checksum[:], request.Fields[:16])
	var client protocol.ClientID
	copy(client[:], request.Fields[32:48])
	op := protocol.Op(binary.LittleEndian.Uint64(request.Fields[48:56]))
	_, slot, found := replica.sessions.RepairReply(client, op, checksum)
	if !found {
		return
	}
	offset, ok := replica.replies.slotOffset(slot)
	if !ok {
		return
	}
	replica.startRepairRead(repairReadReply, request.Author, op, client, checksum, offset, replica.wal.Layout().ReplyStride)
}

func (replica *Replica) handleGetBlocks(request protocol.Header, body []byte) {
	for offset := 0; offset < len(body); offset += 32 {
		var checksum protocol.Checksum
		copy(checksum[:], body[offset:offset+16])
		address := binary.LittleEndian.Uint64(body[offset+16 : offset+24])
		if !replica.startRepairRead(repairReadBlock, request.Author, 0, protocol.ClientID{}, checksum, address, replica.config.Cluster.BlockSize) {
			return
		}
	}
}

func (replica *Replica) startRepairRead(kind repairReadKind, peer protocol.ReplicaIndex, op protocol.Op, client protocol.ClientID, checksum protocol.Checksum, offset, size uint64) bool {
	if replica.stateSync.stage != SyncStageIdle {
		return false
	}
	if replica.activeRepairReads() >= int(replica.config.Process.RepairReadsMax) {
		return false
	}
	for index := range replica.repairReads {
		read := &replica.repairReads[index]
		if read.busy {
			continue
		}
		buffer := read.buffer[:size]
		handle, err := replica.io.Submit(IOOperation{Kind: IORead, Offset: offset, Buffer: buffer, Size: size})
		if err != nil {
			return false
		}
		*read = repairRead{busy: true, kind: kind, handle: handle, peer: peer, op: op, client: client, checksum: checksum, address: offset, buffer: read.buffer}
		return true
	}
	return false
}

func (replica *Replica) finishRepairRead(read *repairRead, completion IOCompletion) {
	kind := read.kind
	peer := read.peer
	op := read.op
	client := read.client
	checksum := read.checksum
	address := read.address
	buffer := read.buffer
	*read = repairRead{buffer: buffer}
	if completion.Err != nil {
		return
	}
	maximum := uint32(replica.config.Cluster.MessageSizeMax)
	if kind == repairReadBlock {
		maximum = uint32(replica.config.Cluster.BlockSize)
	}
	frame, header, ok := decodeRepairFrame(completion.Buffer, replica.config.Group, maximum, replica.membership.ActiveCount+replica.membership.StandbyCount)
	if !ok || header.HeaderChecksum != checksum {
		return
	}
	context := replica.validationContext(header.Author)
	context.MessageSizeMax = maximum
	if protocol.ValidateSemantics(&header, frame[protocol.HeaderSize:], context) != protocol.RejectNone {
		return
	}
	switch kind {
	case repairReadPrepare:
		if header.Command != protocol.CommandPrepare || prepareOp(&header) != op {
			return
		}
	case repairReadReply:
		if header.Command != protocol.CommandReply || replyClient(&header) != client || replyOp(&header) != op {
			return
		}
	case repairReadBlock:
		if !replica.validRepairBlock(header, frame[protocol.HeaderSize:], address) {
			return
		}
	default:
		return
	}
	replica.sendRepairFrame(peer, frame, header)
}

func (replica *Replica) validRepairBlock(header protocol.Header, body []byte, address uint64) bool {
	if header.Command != protocol.CommandBlock || binary.LittleEndian.Uint64(header.Fields[96:104]) != address {
		return false
	}
	if header.Release == 0 || header.Release > replica.config.CurrentRelease || !replica.blocks.validAddress(address) {
		return false
	}
	var metadata [96]byte
	copy(metadata[:], header.Fields[:96])
	return ValidateBlockMetadata(protocol.BlockType(header.Fields[112]), metadata, body) == nil
}

func decodeRepairFrame(physical []byte, group protocol.GroupID, maximum uint32, memberCount uint8) ([]byte, protocol.Header, bool) {
	if len(physical) < protocol.HeaderSize {
		return nil, protocol.Header{}, false
	}
	size := binary.LittleEndian.Uint32(physical[96:100])
	if size < protocol.HeaderSize || size > maximum || int(size) > len(physical) || !zeroBytes(physical[size:]) {
		return nil, protocol.Header{}, false
	}
	frame := physical[:size:size]
	header, _, reason := protocol.DecodeFrame(frame, group, maximum, memberCount)
	return frame, header, reason == protocol.RejectNone
}

func (replica *Replica) sendRepairFrame(peer protocol.ReplicaIndex, frame []byte, header protocol.Header) {
	message, err := replica.frames.Acquire(uint32(len(frame) - protocol.HeaderSize))
	if err != nil {
		return
	}
	body, err := message.Body()
	if err != nil {
		message.Release()
		return
	}
	copy(body, frame[protocol.HeaderSize:])
	if message.Seal(&header) == nil {
		replica.deps.MessageBus.SendReplica(peer, message)
	}
	message.Release()
}

func (replica *Replica) handleRepairTimeout(sample TimeSample) {
	if replica.continueBlockRepair(sample.Monotonic) {
		return
	}
	replica.repairBudget.Expire(sample.Monotonic)
	if replica.repairViewValid {
		replica.continueRecoveringView(sample.Monotonic)
		return
	}
	if replica.headOp < replica.commitMax {
		replica.sendGetView(sample.Monotonic)
		return
	}
	if replica.repairWrite.busy {
		return
	}
	if replica.repairHeaderValid {
		replica.sendGetPrepare(sample.Monotonic)
		return
	}
	missing, found := replica.newestMissingHeader()
	if !found {
		replica.continueScrub(sample.Monotonic)
		return
	}
	interval := uint64(replica.config.Process.InitialRTT)
	if replica.repairHeadersLast != 0 && elapsedSince(replica.repairHeadersLast, sample.Monotonic) < interval {
		return
	}
	replica.sendGetHeaders(missing)
	replica.repairHeadersLast = sample.Monotonic
}

func (replica *Replica) newestMissingHeader() (protocol.Op, bool) {
	if replica.headOp == 0 {
		return 0, false
	}
	minimum := replica.checkpoint.PrepareOp() + 1
	retained := protocol.Op(0)
	if uint64(replica.headOp)+1 > replica.config.Cluster.JournalSlots {
		retained = replica.headOp - protocol.Op(replica.config.Cluster.JournalSlots) + 1
	}
	minimum = max(minimum, retained)
	for op := replica.headOp; op >= minimum; op-- {
		_, present, _, found := replica.localHeaderEvidence(op)
		if !found || !present {
			return op, true
		}
		if op == 0 {
			break
		}
	}
	return 0, false
}

func (replica *Replica) sendGetHeaders(missing protocol.Op) {
	peer, ok := replica.randomRepairPeer()
	if !ok {
		return
	}
	start := protocol.Op(0)
	if missing > 63 {
		start = missing - 63
	}
	start = max(start, replica.checkpoint.PrepareOp()+1)
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	header := protocol.Header{Group: replica.config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetHeaders, Author: replica.local}
	binary.LittleEndian.PutUint64(header.Fields[:8], uint64(start))
	binary.LittleEndian.PutUint64(header.Fields[8:16], uint64(missing))
	if message.Seal(&header) == nil {
		replica.deps.MessageBus.SendReplica(peer, message)
	}
	message.Release()
}

func (replica *Replica) randomRepairPeer() (protocol.ReplicaIndex, bool) {
	count := replica.membership.ActiveCount
	if uint8(replica.local) < count {
		if count <= 1 {
			return 0, false
		}
		value := uint8(replica.random.Uniform(uint64(count - 1)))
		if value >= uint8(replica.local) {
			value++
		}
		return protocol.ReplicaIndex(value), true
	}
	if count == 0 {
		return 0, false
	}
	return protocol.ReplicaIndex(replica.random.Uniform(uint64(count))), true
}

func (replica *Replica) handleHeaders(body []byte) {
	if replica.repairWrite.busy {
		return
	}
	for offset := len(body) - protocol.HeaderSize; offset >= 0; offset -= protocol.HeaderSize {
		candidate, reason := protocol.DecodeHeader(body[offset:offset+protocol.HeaderSize], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
		if reason != protocol.RejectNone || !replica.safeRepairHeader(candidate) {
			continue
		}
		replica.repairHeader = candidate
		replica.repairHeaderValid = true
		replica.sendGetPrepare(replica.deps.Clock.Now().Monotonic)
		return
	}
}

func (replica *Replica) safeRepairHeader(candidate protocol.Header) bool {
	op := prepareOp(&candidate)
	if replica.canonicalRepairHeader(candidate) {
		return true
	}
	if candidate.Command != protocol.CommandPrepare || op == 0 || op > replica.headOp || candidate.View > replica.view {
		return false
	}
	retained := protocol.Op(0)
	if uint64(replica.headOp)+1 > replica.config.Cluster.JournalSlots {
		retained = replica.headOp - protocol.Op(replica.config.Cluster.JournalSlots) + 1
	}
	if op < retained {
		return false
	}
	if op == replica.headOp && candidate.HeaderChecksum != replica.headChecksum {
		return false
	}
	existing, present, _, found := replica.localHeaderEvidence(op)
	existingMatches := found && present && existing.HeaderChecksum == candidate.HeaderChecksum
	if op <= replica.commitMin && !existingMatches {
		return false
	}
	parent := candidate.HeaderChecksum
	for next := op + 1; next <= replica.headOp; next++ {
		header, present, _, found := replica.localHeaderEvidence(next)
		if !found || !present || prepareParent(&header) != parent {
			return false
		}
		parent = header.HeaderChecksum
	}
	return true
}

func (replica *Replica) canonicalRepairHeader(candidate protocol.Header) bool {
	if !replica.repairViewValid || replica.status != StatusRecoveringHead {
		return false
	}
	op := prepareOp(&candidate)
	if candidate.Command != protocol.CommandPrepare || op <= replica.repairViewAncestor || op > replica.repairViewHead || candidate.View > replica.repairView {
		return false
	}
	retained := protocol.Op(0)
	if uint64(replica.repairViewHead)+1 > replica.config.Cluster.JournalSlots {
		retained = replica.repairViewHead - protocol.Op(replica.config.Cluster.JournalSlots) + 1
	}
	if op < retained || op <= replica.commitMin {
		return false
	}
	for index := range replica.repairViewCount {
		header := replica.canonicalHeaders[index]
		if prepareOp(&header) == op {
			return header.HeaderChecksum == candidate.HeaderChecksum
		}
	}
	return false
}

func (replica *Replica) continueRecoveringView(now uint64) {
	if replica.repairWrite.busy || replica.viewIO != (IOHandle{}) {
		return
	}
	if replica.repairViewRebuilt {
		replica.finishRecoveringView()
		return
	}
	for index := replica.repairViewCount - 1; index >= 0; index-- {
		header := replica.canonicalHeaders[index]
		if prepareOp(&header) <= replica.repairViewAncestor || replica.repairFrames[index] != nil {
			continue
		}
		replica.repairHeader = header
		replica.repairHeaderValid = true
		replica.sendGetPrepare(now)
		return
	}
	replica.finishRecoveringView()
}

func (replica *Replica) finishRecoveringView() {
	if !replica.repairViewRebuilt {
		for index := replica.repairViewCount - 1; index >= 0; index-- {
			header := replica.canonicalHeaders[index]
			if prepareOp(&header) <= replica.repairViewAncestor {
				continue
			}
			if replica.repairFrames[index] == nil {
				return
			}
		}
		if !replica.truncatePipelineAfter(replica.repairViewAncestor) {
			replica.fail(ErrReplicaInvariant)
			return
		}
		for index := replica.repairViewCount - 1; index >= 0; index-- {
			header := replica.canonicalHeaders[index]
			if prepareOp(&header) <= replica.repairViewAncestor {
				continue
			}
			message := replica.repairFrames[index]
			entry, ok := replica.pushPipeline(message, header)
			if !ok {
				replica.fail(ErrReplicaInvariant)
				return
			}
			entry.durable = true
			replica.repairFrames[index] = nil
		}
		replica.view = replica.repairView
		replica.commitMax = max(replica.commitMax, replica.repairViewCommit)
		replica.repairViewRebuilt = true
	}
	if replica.isPrimary() && replica.commitMin < replica.repairViewCommit {
		for offset := range replica.pipelineLen {
			entry := replica.pipelineEntry(offset)
			if prepareOp(&entry.header) <= replica.repairViewCommit {
				entry.quorum = true
			}
		}
		replica.advanceCommit()
		return
	}
	if err := replica.persistView(true, replica.repairViewCommit, replica.repairViewHead, replica.canonicalHeaders[:replica.repairViewCount]); err != nil {
		replica.fail(err)
		return
	}
	replica.repairViewValid = false
	replica.repairViewRebuilt = false
	replica.repairHeader = protocol.Header{}
	replica.repairHeaderValid = false
}

func (replica *Replica) truncatePipelineAfter(ancestor protocol.Op) bool {
	for replica.pipelineLen > 0 {
		entry := replica.pipelineEntry(replica.pipelineLen - 1)
		if prepareOp(&entry.header) <= ancestor {
			break
		}
		if entry.io != (IOHandle{}) || entry.stage != CommitStageIdle {
			return false
		}
		if entry.prepare != nil {
			entry.prepare.Release()
		}
		if entry.reply != nil {
			entry.reply.Release()
		}
		generation := entry.generation
		*entry = pipelineEntry{generation: generation}
		replica.pipelineLen--
	}
	header, present, _, found := replica.localHeaderEvidence(ancestor)
	if !found || !present {
		return false
	}
	replica.headOp = ancestor
	replica.headChecksum = header.HeaderChecksum
	replica.lastPrepareTimestamp = prepareTimestamp(&header)
	return true
}

func (replica *Replica) sendGetPrepare(now uint64) {
	op := prepareOp(&replica.repairHeader)
	peer, ok := replica.repairBudget.Reserve(op, now, &replica.random)
	if !ok {
		return
	}
	message, err := replica.frames.Acquire(0)
	if err != nil {
		replica.repairBudget.Complete(op, now)
		return
	}
	header := protocol.Header{Group: replica.config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetPrepare, Author: replica.local}
	copy(header.Fields[:16], replica.repairHeader.HeaderChecksum[:])
	binary.LittleEndian.PutUint64(header.Fields[32:40], uint64(op))
	if message.Seal(&header) == nil {
		replica.deps.MessageBus.SendReplica(peer, message)
	} else {
		replica.repairBudget.Complete(op, now)
	}
	message.Release()
}

func (replica *Replica) handleRepairPrepare(message *Message, header protocol.Header, sample TimeSample) bool {
	if !replica.repairHeaderValid || replica.repairWrite.busy {
		return false
	}
	op := prepareOp(&header)
	if op != prepareOp(&replica.repairHeader) ||
		header.HeaderChecksum != replica.repairHeader.HeaderChecksum ||
		!replica.safeRepairHeader(header) {
		return false
	}
	replica.repairBudget.Complete(op, sample.Monotonic)
	frame, err := message.Bytes()
	if err != nil {
		return false
	}
	handle, err := replica.io.Submit(IOOperation{
		Kind: IOWALAppend, Buffer: frame, WAL: replica.wal,
		ReusableThrough: replica.checkpoint.PrepareOp(),
	})
	if err != nil {
		return false
	}
	replica.repairWrite = repairWrite{busy: true, handle: handle, message: message, op: op}
	return true
}

func (replica *Replica) finishRepairWrite(completion IOCompletion) {
	message := replica.repairWrite.message
	op := replica.repairWrite.op
	replica.repairWrite = repairWrite{}
	if completion.Err != nil {
		message.Release()
		replica.metrics.storageFailures.Add(1)
		replica.fail(completion.Err)
		return
	}
	stored := false
	if replica.repairViewValid && op > replica.repairViewAncestor {
		for index := range replica.repairViewCount {
			if prepareOp(&replica.canonicalHeaders[index]) != op {
				continue
			}
			if replica.repairFrames[index] != nil {
				replica.repairFrames[index].Release()
			}
			replica.repairFrames[index] = message
			stored = true
			break
		}
	}
	if !stored {
		message.Release()
	}
	replica.repairHeader = protocol.Header{}
	replica.repairHeaderValid = false
}

func (replica *Replica) sendGetView(now uint64) {
	primary := replica.membership.Primary(replica.view)
	if primary == replica.local {
		return
	}
	interval := uint64(replica.config.Process.InitialRTT)
	if replica.getViewLast != 0 && elapsedSince(replica.getViewLast, now) < interval {
		return
	}
	if replica.getViewNonce == (protocol.Nonce{}) {
		binary.LittleEndian.PutUint64(replica.getViewNonce[:8], replica.random.Next())
		binary.LittleEndian.PutUint64(replica.getViewNonce[8:], replica.random.Next())
		if replica.getViewNonce == (protocol.Nonce{}) {
			replica.getViewNonce[0] = 1
		}
	}
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	header := protocol.Header{Group: replica.config.Group, View: replica.view, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetView, Author: replica.local}
	copy(header.Fields[:16], replica.getViewNonce[:])
	if message.Seal(&header) == nil {
		replica.deps.MessageBus.SendReplica(primary, message)
		replica.getViewLast = now
	}
	message.Release()
}
