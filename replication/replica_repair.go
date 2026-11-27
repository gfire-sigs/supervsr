package replication

import (
	"encoding/binary"
	"errors"

	"z.gfire.dev/supervsr/replication/protocol"
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
	handle   ioHandle
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
			if protocol.EncodeHeader(body[count*protocol.HeaderSize:(count+1)*protocol.HeaderSize], &header, replica.config.Cluster.ChecksumAlgorithm) != nil {
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
	if message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm) == nil {
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
	if replica.stateSync.stage != syncStageIdle {
		return false
	}
	address := offset
	if kind == repairReadBlock {
		var ok bool
		offset, ok = replica.config.Cluster.BlockOffset(address)
		if !ok {
			return false
		}
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
		handle, err := replica.io.Submit(ioOperation{Kind: ioRead, Offset: offset, Buffer: buffer, Size: size})
		if err != nil {
			return false
		}
		*read = repairRead{busy: true, kind: kind, handle: handle, peer: peer, op: op, client: client, checksum: checksum, address: address, buffer: read.buffer}
		return true
	}
	return false
}

func (replica *Replica) finishRepairRead(read *repairRead, completion ioCompletion) {
	kind := read.kind
	peer := read.peer
	op := read.op
	client := read.client
	checksum := read.checksum
	address := read.address
	buffer := read.buffer
	*read = repairRead{buffer: buffer}
	if completion.Err != nil {
		if kind == repairReadReply {
			replica.markReplyIdentityFault(client, op, checksum)
		}
		return
	}
	maximum := uint32(replica.config.Cluster.MessageSizeMax)
	if kind == repairReadBlock {
		maximum = uint32(replica.config.Cluster.BlockSize)
	}
	frame, header, ok := decodeRepairFrame(completion.Buffer, replica.config.Group, maximum, replica.membership.ActiveCount+replica.membership.StandbyCount, replica.config.Cluster.ChecksumAlgorithm)
	if !ok || header.HeaderChecksum != checksum {
		if kind == repairReadReply {
			replica.markReplyIdentityFault(client, op, checksum)
		}
		return
	}
	context := replica.validationContext(protocol.FrameSourceReplica, header.Author)
	context.MessageSizeMax = maximum
	if protocol.ValidateSemantics(&header, frame[protocol.HeaderSize:], context) != protocol.RejectNone {
		if kind == repairReadReply {
			replica.markReplyIdentityFault(client, op, checksum)
		}
		return
	}
	switch kind {
	case repairReadPrepare:
		if header.Command != protocol.CommandPrepare || prepareOp(&header) != op {
			return
		}
	case repairReadReply:
		if header.Command != protocol.CommandReply || replyClient(&header) != client || replyOp(&header) != op {
			replica.markReplyIdentityFault(client, op, checksum)
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
	return validateBlockMetadata(protocol.BlockType(header.Fields[112]), metadata, body) == nil
}

func decodeRepairFrame(physical []byte, group protocol.GroupID, maximum uint32, memberCount uint8, algorithm ChecksumAlgorithm) ([]byte, protocol.Header, bool) {
	if len(physical) < protocol.HeaderSize {
		return nil, protocol.Header{}, false
	}
	size := binary.LittleEndian.Uint32(physical[96:100])
	if size < protocol.HeaderSize || size > maximum || int(size) > len(physical) || !zeroBytes(physical[size:]) {
		return nil, protocol.Header{}, false
	}
	frame := physical[:size:size]
	header, _, reason := protocol.DecodeFrame(frame, group, maximum, memberCount, algorithm)
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
	if message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm) == nil {
		replica.deps.MessageBus.SendReplica(peer, message)
	}
	message.Release()
}

func (replica *Replica) handleRepairTimeout(sample TimeSample) {
	if replica.stateSync.requestedCheckpoint > replica.checkpoint.PrepareOp() {
		replica.sendGetView(sample.Monotonic)
	}
	if replica.continueBlockRepair(sample.Monotonic) {
		return
	}
	if replica.stateSyncAwaitingOpen() {
		return
	}
	if replica.stateSync.repliesPending {
		replica.continueReplyRepair(sample.Monotonic)
		if replica.replyRepair.handle != (ioHandle{}) {
			return
		}
	}
	expired, err := replica.repairBudget.Expire(sample.Monotonic)
	if err != nil {
		replica.fail(err)
		return
	}
	if expired > 0 && replica.metrics != nil {
		replica.metrics.repairStalls.Add(uint64(expired))
		replica.metrics.repairPeerRotations.Add(uint64(expired))
	}
	if replica.repairViewValid {
		replica.continueRecoveringViewWithRetry(sample.Monotonic, true)
		return
	}
	if replica.headOp < replica.commitMax || replica.status == StatusRecoveringHead {
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
		if replica.continueReplyRepair(sample.Monotonic) {
			return
		}
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
	start := protocol.Op(0)
	if missing > 63 {
		start = missing - 63
	}
	start = max(start, replica.checkpoint.PrepareOp()+1)
	peer, ok := replica.randomRepairPeer()
	if !ok {
		return
	}
	replica.sendGetHeadersRange(peer, start, missing)
}

func (replica *Replica) sendGetHeadersRange(peer protocol.ReplicaIndex, start, end protocol.Op) {
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	header := protocol.Header{Group: replica.config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetHeaders, Author: replica.local}
	binary.LittleEndian.PutUint64(header.Fields[:8], uint64(start))
	binary.LittleEndian.PutUint64(header.Fields[8:16], uint64(end))
	if message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm) == nil {
		replica.deps.MessageBus.SendReplica(peer, message)
		if replica.metrics != nil {
			replica.metrics.repairRequests.Add(1)
		}
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

func (replica *Replica) handleHeaders(header protocol.Header, body []byte) {
	if replica.repairWrite.busy {
		return
	}
	if replica.metrics != nil {
		replica.metrics.repairResponses.Add(1)
		replica.metrics.repairBytes.Add(uint64(header.Size))
	}
	if replica.repairViewWindow {
		replica.handleRepairWindowHeaders(header, body)
		return
	}
	for offset := len(body) - protocol.HeaderSize; offset >= 0; offset -= protocol.HeaderSize {
		candidate, reason := protocol.DecodeHeader(body[offset:offset+protocol.HeaderSize], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount, replica.config.Cluster.ChecksumAlgorithm)
		if reason != protocol.RejectNone || !replica.safeRepairHeader(candidate) {
			continue
		}
		replica.repairHeader = candidate
		replica.repairHeaderValid = true
		replica.sendGetPrepare(replica.deps.Clock.Now().Monotonic)
		return
	}
}

func (replica *Replica) handleRepairWindowHeaders(header protocol.Header, body []byte) {
	count := len(body) / protocol.HeaderSize
	if header.Author != replica.membership.Primary(replica.repairView) || count == 0 || count > len(replica.canonicalHeaders) || len(body) != count*protocol.HeaderSize {
		return
	}
	expected := replica.commitMin + 1
	committed, present, dirty, found := replica.localHeaderEvidence(replica.commitMin)
	if !found || !present || dirty {
		return
	}
	parent := committed.HeaderChecksum
	for index := range count {
		candidate, reason := protocol.DecodeHeader(body[index*protocol.HeaderSize:(index+1)*protocol.HeaderSize], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount, replica.config.Cluster.ChecksumAlgorithm)
		if reason != protocol.RejectNone || prepareOp(&candidate) != expected || candidate.View > replica.repairView || prepareParent(&candidate) != parent {
			return
		}
		replica.canonicalHeaders[count-index-1] = candidate
		parent = candidate.HeaderChecksum
		expected++
	}
	if expected-1 != replica.repairViewHead {
		return
	}
	replica.repairViewCount = count
	replica.repairViewWindow = false
	replica.repairHeadersLast = 0
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

func (replica *Replica) beginRepairWindow(view protocol.View, commit protocol.Op) bool {
	if replica.commitInFlight() {
		return false
	}
	limit, ok := checkedAdd(uint64(replica.commitMin), uint64(len(replica.pipeline)))
	if !ok {
		replica.fail(ErrReplicaInvariant)
		return false
	}
	windowHead := min(commit, protocol.Op(limit))
	if windowHead <= replica.commitMin {
		return false
	}
	priorHead := max(replica.headOp, replica.repairViewPriorHead)
	replica.clearRepairView()
	clear(replica.canonicalHeaders)
	replica.status = StatusRecoveringHead
	replica.releaseQueuedRequests()
	replica.repairViewValid = true
	replica.repairView = view
	replica.repairViewCommit = windowHead
	replica.repairViewHead = windowHead
	replica.repairViewAncestor = replica.commitMin
	replica.repairViewPriorHead = priorHead
	replica.repairViewWindow = true
	replica.repairViewPartial = true
	replica.commitMax = max(replica.commitMax, commit)
	replica.repairHeadersLast = 0
	replica.viewChangeStalled = false
	replica.timers.viewChange.Reset()
	replica.requestRepairWindowHeaders(replica.deps.Clock.Now().Monotonic)
	return true
}

func (replica *Replica) requestRepairWindowHeaders(now uint64) {
	interval := uint64(replica.config.Process.InitialRTT)
	if replica.repairHeadersLast != 0 && elapsedSince(replica.repairHeadersLast, now) < interval {
		return
	}
	peer := replica.membership.Primary(replica.repairView)
	replica.sendGetHeadersRange(peer, replica.commitMin+1, replica.repairViewHead)
	replica.repairHeadersLast = now
}

func (replica *Replica) continueRecoveringView(now uint64) {
	replica.continueRecoveringViewWithRetry(now, false)
}

func (replica *Replica) continueRecoveringViewWithRetry(now uint64, retryPending bool) {
	if replica.repairWrite.busy || replica.viewIO != (ioHandle{}) {
		return
	}
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if entry.io != (ioHandle{}) || entry.stage != CommitStageIdle {
			return
		}
	}
	if replica.repairViewWindow {
		replica.requestRepairWindowHeaders(now)
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
		op := prepareOp(&header)
		if !retryPending && replica.repairBudget.HasPending(op) {
			return
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
		for offset := range replica.pipelineLen {
			entry := replica.pipelineEntry(offset)
			entry.acks = 0
			entry.quorum = false
		}
		replica.headOp = replica.repairViewHead
		replica.headChecksum = replica.canonicalHeaders[0].HeaderChecksum
		replica.lastPrepareTimestamp = prepareTimestamp(&replica.canonicalHeaders[0])
		replica.view = replica.repairView
		replica.commitMax = max(replica.commitMax, replica.repairViewCommit)
		replica.repairViewRebuilt = true
	}
	if replica.commitMin < replica.repairViewCommit {
		for offset := range replica.pipelineLen {
			entry := replica.pipelineEntry(offset)
			if prepareOp(&entry.header) <= replica.repairViewCommit {
				entry.quorum = true
			}
		}
		replica.advanceCommit()
		if replica.commitMin < replica.repairViewCommit || replica.commitInFlight() || replica.releaseActivation != 0 {
			return
		}
	}
	if replica.commitInFlight() || replica.releaseActivation != 0 {
		return
	}
	if replica.repairViewPartial {
		replica.finishPartialRepairWindow()
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
	replica.viewChangeStalled = false
	replica.timers.viewChange.Reset()
}

func (replica *Replica) finishPartialRepairWindow() {
	targetView := replica.repairView
	priorHead := replica.repairViewPriorHead
	now := replica.deps.Clock.Now().Monotonic
	if replica.commitMin < replica.commitMax {
		if !replica.beginRepairWindow(targetView, replica.commitMax) {
			replica.fail(ErrReplicaInvariant)
		}
		return
	}
	replica.clearRepairView()
	if replica.stateSync.head > replica.commitMin && replica.stateSync.view == targetView && replica.stateSync.count > 0 {
		headers := replica.stateSync.headers[:replica.stateSync.count]
		if replica.validRecoveringView(targetView, replica.stateSync.commit, replica.stateSync.head, headers) {
			replica.status = StatusRecoveringHead
			replica.recordRecoveringView(targetView, replica.stateSync.commit, replica.stateSync.head, headers)
			return
		}
	}
	if !replica.stateSync.repairing {
		replica.restoreDurableSuffix(priorHead)
	}
	if uint8(replica.local) >= replica.membership.ActiveCount || replica.stateSync.repairing || replica.wal.HasFaultySlots() {
		replica.status = StatusRecoveringHead
		replica.requestView(targetView, now)
		return
	}
	replica.status = StatusViewChange
	replica.abdicated = false
	replica.viewChangeStalled = false
	replica.exitViewBits = 0
	clear(replica.exitViewAt[:])
	replica.invalidateReleaseReports()
	replica.joinViewBits = 0
	clear(replica.joins)
	replica.timers.abdication.Reset()
	replica.timers.join.Reset()
	replica.timers.viewChange.Reset()
	count := replica.collectLocalSuffix(replica.canonicalHeaders)
	durableCommit := max(replica.superblocks.Current().State.CommitMax, min(replica.commitMax, replica.durableHeadOp()))
	if err := replica.persistView(false, durableCommit, replica.headOp, replica.canonicalHeaders[:count]); err != nil {
		replica.fail(err)
		return
	}
}

func (replica *Replica) restoreDurableSuffix(priorHead protocol.Op) {
	limit, ok := checkedAdd(uint64(replica.commitMin), uint64(len(replica.pipeline)))
	if !ok || priorHead <= replica.headOp {
		return
	}
	target := min(priorHead, protocol.Op(limit))
	if target <= replica.headOp {
		return
	}
	buffer, err := NewAlignedBuffer(replica.wal.Layout().PrepareStride, SectorSize)
	if err != nil {
		return
	}
	memberCount := replica.membership.ActiveCount + replica.membership.StandbyCount
	for op := replica.headOp + 1; op <= target; op++ {
		candidate, found := replica.wal.RecoveredHeader(op)
		if !found || prepareParent(&candidate) != replica.headChecksum {
			return
		}
		frame, readErr := replica.wal.ReadPrepare(op, buffer)
		if readErr != nil {
			return
		}
		header, body, reason := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), memberCount, replica.config.Cluster.ChecksumAlgorithm)
		if reason != protocol.RejectNone || header.HeaderChecksum != candidate.HeaderChecksum {
			return
		}
		message, acquireErr := replica.frames.Acquire(uint32(len(body)))
		if acquireErr != nil {
			return
		}
		messageBody, bodyErr := message.Body()
		if bodyErr != nil {
			message.Release()
			return
		}
		copy(messageBody, body)
		if message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm) != nil {
			message.Release()
			return
		}
		entry, pushed := replica.pushPipeline(message, header)
		if !pushed {
			message.Release()
			return
		}
		entry.durable = true
		replica.lastPrepareTimestamp = max(replica.lastPrepareTimestamp, prepareTimestamp(&header))
	}
}

func (replica *Replica) truncatePipelineAfter(ancestor protocol.Op) bool {
	if ancestor < replica.commitMin {
		return false
	}
	header, present, _, found := replica.localHeaderEvidence(ancestor)
	if !found || !present {
		return false
	}
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if prepareOp(&entry.header) > ancestor && (entry.io != (ioHandle{}) || entry.stage != CommitStageIdle) {
			return false
		}
	}
	for replica.pipelineLen > 0 {
		entry := replica.pipelineEntry(replica.pipelineLen - 1)
		if prepareOp(&entry.header) <= ancestor {
			break
		}
		if entry.io != (ioHandle{}) || entry.stage != CommitStageIdle {
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
	replica.headOp = ancestor
	replica.headChecksum = header.HeaderChecksum
	replica.lastPrepareTimestamp = prepareTimestamp(&header)
	return true
}

func (replica *Replica) sendGetPrepare(now uint64) {
	op := prepareOp(&replica.repairHeader)
	peer, ok, err := replica.repairBudget.Reserve(op, now, &replica.random)
	if err != nil {
		replica.fail(err)
		return
	}
	if !ok {
		if replica.metrics != nil {
			replica.metrics.repairStalls.Add(1)
		}
		return
	}
	message, err := replica.frames.Acquire(0)
	if err != nil {
		if replica.metrics != nil && errors.Is(err, protocol.ErrFramePoolEmpty) {
			replica.metrics.framePoolBackpressure.Add(1)
		}
		if _, budgetErr := replica.repairBudget.Complete(op, now); budgetErr != nil {
			replica.fail(budgetErr)
		}
		return
	}
	header := protocol.Header{Group: replica.config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetPrepare, Author: replica.local}
	copy(header.Fields[:16], replica.repairHeader.HeaderChecksum[:])
	binary.LittleEndian.PutUint64(header.Fields[32:40], uint64(op))
	if message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm) == nil {
		replica.deps.MessageBus.SendReplica(peer, message)
		if replica.metrics != nil {
			replica.metrics.repairRequests.Add(1)
		}
	} else if _, budgetErr := replica.repairBudget.Complete(op, now); budgetErr != nil {
		replica.fail(budgetErr)
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
	if _, err := replica.repairBudget.Complete(op, sample.Monotonic); err != nil {
		replica.fail(err)
		return false
	}
	if replica.metrics != nil {
		replica.metrics.repairResponses.Add(1)
		replica.metrics.repairBytes.Add(uint64(header.Size))
	}
	frame, err := message.Bytes()
	if err != nil {
		return false
	}
	handle, err := replica.io.Submit(ioOperation{
		Kind: ioWALAppend, Buffer: frame, walJournal: replica.wal,
		ReusableThrough: replica.checkpoint.PrepareOp(),
	})
	if err != nil {
		return false
	}
	replica.repairWrite = repairWrite{busy: true, handle: handle, message: message, op: op}
	return true
}

func (replica *Replica) finishRepairWrite(completion ioCompletion) {
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
	replica.viewChangeStalled = false
	replica.timers.viewChange.Reset()
}

func (replica *Replica) requestView(view protocol.View, now uint64) {
	if view > replica.getViewTarget {
		replica.getViewTarget = view
		replica.getViewNonce = protocol.Nonce{}
		replica.getViewLast = 0
	}
	replica.sendGetView(now)
}

func (replica *Replica) sendGetView(now uint64) {
	view := max(replica.view, replica.getViewTarget)
	primary := replica.membership.Primary(view)
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
	header := protocol.Header{Group: replica.config.Group, View: view, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetView, Author: replica.local}
	copy(header.Fields[:16], replica.getViewNonce[:])
	if message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm) == nil {
		replica.deps.MessageBus.SendReplica(primary, message)
		replica.getViewLast = now
	}
	message.Release()
}
