package replication

import (
	"encoding/binary"
	"errors"

	"z.gfire.dev/supervsr/replication/protocol"
)

func (replica *Replica) handleHigherViewEvidence(header protocol.Header) bool {
	if header.View < replica.view || uint8(header.Author) >= replica.membership.ActiveCount {
		return false
	}
	if replica.stateSyncAwaitingOpen() {
		switch header.Command {
		case protocol.CommandPrepare, protocol.CommandCommit, protocol.CommandExitView, protocol.CommandJoinView:
			replica.requestView(header.View, replica.deps.Clock.Now().Monotonic)
			return true
		case protocol.CommandPing, protocol.CommandPong:
			if header.View > replica.view {
				replica.requestView(header.View, replica.deps.Clock.Now().Monotonic)
			}
			return false
		}
	}
	switch header.Command {
	case protocol.CommandPrepare, protocol.CommandCommit:
		if header.Command == protocol.CommandPrepare && replica.repairHeaderValid && header.HeaderChecksum == replica.repairHeader.HeaderChecksum {
			return false
		}
		if header.View > replica.view || replica.status == StatusViewChange || replica.status == StatusRecoveringHead {
			replica.requestView(header.View, replica.deps.Clock.Now().Monotonic)
			return true
		}
	case protocol.CommandExitView, protocol.CommandJoinView, protocol.CommandPing, protocol.CommandPong:
		if !replica.canEnterViewChange() {
			if header.View > replica.view {
				replica.requestView(header.View, replica.deps.Clock.Now().Monotonic)
			}
			return header.Command != protocol.CommandPing && header.Command != protocol.CommandPong
		}
		if header.View > replica.view {
			replica.beginViewChange(header.View)
			return true
		}
	}
	return false
}

func (replica *Replica) handleExitView(header protocol.Header) {
	if uint8(header.Author) >= replica.membership.ActiveCount {
		return
	}
	if header.View > replica.view {
		replica.beginViewChange(header.View)
		return
	}
	if replica.status != StatusNormal || header.View != replica.view {
		return
	}
	bit := uint16(1) << uint8(header.Author)
	if replica.exitViewBits&bit != 0 {
		return
	}
	replica.exitViewBits |= bit
	if countAckBits(replica.exitViewBits) < replica.quorums.ViewChange {
		return
	}
	if replica.view == protocol.MaxView {
		replica.fail(ErrReplicaInvariant)
		return
	}
	replica.beginViewChange(replica.view + 1)
}

func (replica *Replica) canEnterViewChange() bool {
	if uint8(replica.local) >= replica.membership.ActiveCount {
		return false
	}
	if replica.status != StatusNormal && replica.status != StatusViewChange {
		return false
	}
	if replica.repairViewValid || replica.stateSync.stage != syncStageIdle {
		return false
	}
	if replica.stateSync.repairing || replica.stateSync.opening {
		return false
	}
	return !replica.stateSync.replayRunning
}

func (replica *Replica) beginViewChange(target protocol.View) {
	if target <= replica.view || !replica.canEnterViewChange() {
		return
	}
	if replica.viewIO != (IOHandle{}) || replica.commitInFlight() {
		replica.pendingView = max(replica.pendingView, target)
		return
	}
	replica.beginViewChangeNow(target)
}

func (replica *Replica) beginViewChangeNow(target protocol.View) {
	replica.status = StatusViewChange
	replica.view = target
	replica.exitViewBits = 0
	replica.invalidateReleaseReports()
	replica.joinViewBits = 0
	clear(replica.joins)
	replica.timers.join.Reset()
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		entry.acks = 0
		entry.quorum = false
	}
	replica.releaseQueuedRequests()
	count := replica.collectLocalSuffix(replica.canonicalHeaders)
	durableCommit := max(replica.superblocks.Current().State.CommitMax, min(replica.commitMax, replica.durableHeadOp()))
	if err := replica.persistView(false, durableCommit, replica.headOp, replica.canonicalHeaders[:count]); err != nil {
		replica.fail(err)
		return
	}
	replica.metrics.viewChanges.Add(1)
}

func (replica *Replica) releaseQueuedRequests() {
	for replica.requestLen > 0 {
		queued := &replica.requestQueue[replica.requestHead]
		if queued.message != nil {
			queued.message.Release()
		}
		*queued = queuedRequest{}
		replica.requestHead = (replica.requestHead + 1) % uint32(len(replica.requestQueue))
		replica.requestLen--
	}
}

func (replica *Replica) collectLocalSuffix(destination []protocol.Header) int {
	maximum := min(len(destination), int(replica.config.Cluster.PipelineMax+1))
	count := 0
	for op := replica.headOp; count < maximum; op-- {
		header, _, _, found := replica.localHeaderEvidence(op)
		if found {
			destination[count] = header
		} else {
			destination[count] = protocol.Header{}
		}
		count++
		if op == replica.checkpoint.PrepareOp() || op == 0 {
			break
		}
	}
	return count
}

func (replica *Replica) persistView(install bool, commit, head protocol.Op, headers []protocol.Header) error {
	if replica.viewIO != (IOHandle{}) || len(headers) == 0 || len(headers) > int(replica.config.Cluster.PipelineMax+1) {
		return ErrReplicaBackpressure
	}
	next := replica.superblocks.Current()
	next.ParentChecksum = next.Checksum
	next.Sequence++
	next.State.View = replica.view
	if install {
		next.State.LogView = replica.view
	}
	next.State.CommitMax = max(next.State.CommitMax, commit)
	keepExistingHeaders := false
	if !install && next.ViewHeaderCount != 0 {
		memberCount := replica.membership.ActiveCount + replica.membership.StandbyCount
		existing, reason := protocol.DecodeHeader(next.ViewHeaders[0][:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), memberCount, replica.config.Cluster.ChecksumAlgorithm)
		keepExistingHeaders = reason == protocol.RejectNone && prepareOp(&existing) > head && prepareOp(&existing) >= next.State.Checkpoint.PrepareOp()
	}
	if !keepExistingHeaders {
		next.ViewHeaderCount = uint32(len(headers))
		clear(next.ViewHeaders[:])
		for index := range headers {
			var encoded [protocol.HeaderSize]byte
			header := headers[index]
			if header.Command == 0 {
				continue
			}
			if err := protocol.EncodeHeader(encoded[:], &header, replica.config.Cluster.ChecksumAlgorithm); err != nil {
				return err
			}
			next.ViewHeaders[index] = encoded
		}
	}
	handle, err := replica.io.Submit(ioOperation{Kind: IOSuperblockPersist, superblockStore: replica.superblocks, superblockRecord: next})
	if err != nil {
		return err
	}
	replica.viewIO = handle
	replica.viewInstall = install
	replica.viewCommit = commit
	replica.viewHead = head
	return nil
}

func (replica *Replica) handleViewPersistence(completion ioCompletion) bool {
	if completion.Kind != IOSuperblockPersist || completion.Handle != replica.viewIO {
		return false
	}
	replica.viewIO = IOHandle{}
	if completion.Err != nil {
		replica.metrics.storageFailures.Add(1)
		replica.fail(completion.Err)
		return true
	}
	replica.durableView = replica.view
	if !replica.viewInstall {
		if replica.resumePendingViewChange() {
			return true
		}
		replica.sendJoinView()
		return true
	}
	replica.wal.DiscardAfter(replica.viewHead)
	replica.lastPrimaryCommit = 0
	replica.failureDetector.Signal(replica.deps.Clock.Now().Monotonic)
	replica.logView = replica.view
	replica.commitMax = max(replica.commitMax, replica.viewCommit)
	replica.status = StatusNormal
	replica.refreshLocalReleaseReport()
	replica.maybeSelectUpgrade()
	replica.viewInstall = false
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if !replica.canAcknowledge(entry) {
			continue
		}
		if replica.isPrimary() {
			replica.countPrepareAck(entry, replica.local)
		} else {
			replica.sendPrepareOK(entry)
		}
	}
	if replica.resumePendingViewChange() {
		return true
	}

	if replica.isPrimary() {
		replica.broadcastView()
	}
	replica.advanceCommit()
	return true
}
func (replica *Replica) resumePendingViewChange() bool {
	if replica.pendingView <= replica.view {
		replica.pendingView = 0
		return false
	}
	if !replica.canEnterViewChange() {
		return false
	}
	target := replica.pendingView
	replica.pendingView = 0
	replica.beginViewChangeNow(target)
	return true
}

func (replica *Replica) sendJoinView() {
	if replica.status != StatusViewChange || replica.durableView != replica.view {
		return
	}
	count := min(int(replica.headOp-replica.checkpoint.PrepareOp()+1), int(replica.config.Cluster.PipelineMax+1))
	if count <= 0 {
		count = 1
	}
	message, err := replica.frames.Acquire(uint32(count * protocol.HeaderSize))
	if err != nil {
		replica.fail(err)
		return
	}
	body, err := message.Body()
	if err != nil {
		message.Release()
		replica.fail(err)
		return
	}
	var present, nack uint16
	for index := range count {
		op := replica.headOp - protocol.Op(index)
		header, available, negative, found := replica.localHeaderEvidence(op)
		if found {
			if err := protocol.EncodeHeader(body[index*protocol.HeaderSize:(index+1)*protocol.HeaderSize], &header, replica.config.Cluster.ChecksumAlgorithm); err != nil {
				message.Release()
				replica.fail(err)
				return
			}
		}
		if available {
			present |= uint16(1) << index
		}
		if negative {
			nack |= uint16(1) << index
		}
	}
	header := protocol.Header{Group: replica.config.Group, View: replica.view, Protocol: protocol.ProtocolVersion, Command: protocol.CommandJoinView, Author: replica.local}
	binary.LittleEndian.PutUint16(header.Fields[0:2], present)
	binary.LittleEndian.PutUint16(header.Fields[16:18], nack)
	binary.LittleEndian.PutUint64(header.Fields[32:40], uint64(replica.headOp))
	binary.LittleEndian.PutUint64(header.Fields[40:48], uint64(replica.commitMin))
	binary.LittleEndian.PutUint64(header.Fields[48:56], uint64(replica.checkpoint.PrepareOp()))
	binary.LittleEndian.PutUint32(header.Fields[56:60], uint32(replica.logView))
	if err := message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm); err != nil {
		message.Release()
		replica.fail(err)
		return
	}
	if replica.isPrimary() {
		replica.recordJoinView(header, body)
	} else {
		replica.deps.MessageBus.SendReplica(replica.membership.Primary(replica.view), message)
	}
	message.Release()
}

func (replica *Replica) localHeaderEvidence(op protocol.Op) (protocol.Header, bool, bool, bool) {
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if prepareOp(&entry.header) == op {
			return entry.header, true, !entry.durable, true
		}
	}
	if op == replica.checkpoint.PrepareOp() {
		header, reason := protocol.DecodeHeader(replica.checkpoint.Header[:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount, replica.config.Cluster.ChecksumAlgorithm)
		return header, reason == protocol.RejectNone, false, reason == protocol.RejectNone
	}
	header, present, nack := replica.wal.JoinEvidence(op)
	return header, present, nack, present
}

func (replica *Replica) handleJoinView(header protocol.Header, body []byte) {
	if uint8(header.Author) >= replica.membership.ActiveCount {
		return
	}
	if header.View > replica.view {
		replica.beginViewChange(header.View)
		return
	}
	if replica.status == StatusNormal && header.View == replica.view && replica.isPrimary() && replica.durableView == replica.view && replica.logView == replica.view {
		replica.broadcastView()
		return
	}
	if replica.status != StatusViewChange || header.View != replica.view || !replica.isPrimary() || replica.durableView != replica.view {
		return
	}
	replica.recordJoinView(header, body)
}

func (replica *Replica) recordJoinView(header protocol.Header, body []byte) {
	sender := uint8(header.Author)
	bit := uint16(1) << sender
	if replica.joinViewBits&bit != 0 {
		return
	}
	count := len(body) / protocol.HeaderSize
	if count == 0 || count > int(replica.config.Cluster.PipelineMax+1) {
		return
	}
	record := &replica.joins[sender]
	*record = joinRecord{
		valid:      true,
		present:    binary.LittleEndian.Uint16(header.Fields[0:2]),
		nack:       binary.LittleEndian.Uint16(header.Fields[16:18]),
		head:       protocol.Op(binary.LittleEndian.Uint64(header.Fields[32:40])),
		commit:     protocol.Op(binary.LittleEndian.Uint64(header.Fields[40:48])),
		checkpoint: protocol.Op(binary.LittleEndian.Uint64(header.Fields[48:56])),
		logView:    protocol.View(binary.LittleEndian.Uint32(header.Fields[56:60])),
		count:      uint8(count),
	}
	if protocol.Op(count-1) > record.head || record.logView > header.View || record.commit > record.head || record.checkpoint > record.commit {
		*record = joinRecord{}
		return
	}
	replica.observePeerCheckpoint(header.Author, record.checkpoint)
	headers := replica.joinHeaderSlice(sender)
	clear(headers)
	for index := range count {
		encoded := body[index*protocol.HeaderSize : (index+1)*protocol.HeaderSize]
		if allZeroBytes(encoded) {
			continue
		}
		candidate, reason := protocol.DecodeHeader(encoded, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount, replica.config.Cluster.ChecksumAlgorithm)
		if reason != protocol.RejectNone || prepareOp(&candidate) != record.head-protocol.Op(index) {
			*record = joinRecord{}
			return
		}
		headers[index] = candidate
	}
	replica.joinViewBits |= bit
	if countAckBits(replica.joinViewBits) >= replica.quorums.ViewChange {
		replica.tryInstallCanonicalView()
	}
}

func (replica *Replica) joinHeaderSlice(sender uint8) []protocol.Header {
	stride := int(replica.config.Cluster.PipelineMax + 1)
	start := int(sender) * stride
	return replica.joinHeaders[start : start+stride]
}

func (replica *Replica) tryInstallCanonicalView() {
	commit, head, count, resolved := replica.selectCanonicalSuffix()
	if !resolved || commit < replica.commitMin {
		if countAckBits(replica.joinViewBits) == replica.membership.ActiveCount {
			replica.fail(ErrUnrecoverableConsensusState)
		}
		return
	}
	if replica.viewIO != (IOHandle{}) {
		return
	}
	if !replica.validRecoveringView(replica.view, commit, head, replica.canonicalHeaders[:count]) {
		return
	}
	replica.status = StatusRecoveringHead
	replica.recordRecoveringView(replica.view, commit, head, replica.canonicalHeaders[:count])
}

func (replica *Replica) selectCanonicalSuffix() (protocol.Op, protocol.Op, int, bool) {
	maximumHead := replica.checkpoint.PrepareOp()
	committed := max(replica.commitMin, replica.commitMax, replica.checkpoint.PrepareOp())
	for sender := range replica.joins {
		record := &replica.joins[sender]
		if !record.valid {
			continue
		}
		if record.count == 0 || protocol.Op(record.count-1) > record.head {
			return 0, 0, 0, false
		}
		maximumHead = max(maximumHead, record.head)
		committed = max(committed, replica.joinCommittedBound(uint8(sender)))
	}
	if committed > maximumHead || maximumHead-committed >= protocol.Op(len(replica.canonicalHeaders)) {
		return 0, 0, 0, false
	}
	count := 0
	var previous protocol.Header
	for op := committed; op <= maximumHead; op++ {
		candidate, found, conflict := replica.canonicalCandidate(op)
		if conflict {
			replica.fail(errors.Join(ErrUnrecoverableConsensusState, ErrReplicaInvariant))
			return 0, 0, 0, false
		}
		negatives, copies := replica.canonicalEvidence(op, candidate, found)
		if negatives >= replica.quorums.Negative && op > committed {
			maximumHead = op - 1
			break
		}
		if !found || copies == 0 {
			return 0, 0, 0, false
		}
		if count != 0 && prepareParent(&candidate) != previous.HeaderChecksum {
			return 0, 0, 0, false
		}
		replica.canonicalHeaders[count] = candidate
		previous = candidate
		count++
	}
	for left, right := 0, count-1; left < right; left, right = left+1, right-1 {
		replica.canonicalHeaders[left], replica.canonicalHeaders[right] = replica.canonicalHeaders[right], replica.canonicalHeaders[left]
	}
	if count > 0 && committed > replica.commitMin && committed-replica.commitMin > 1 {
		for op := committed - 1; op > replica.commitMin && count < len(replica.canonicalHeaders); op-- {
			expected := prepareParent(&replica.canonicalHeaders[count-1])
			bridged, ok := replica.joinHeaderByChecksum(op, expected)
			if !ok {
				break
			}
			replica.canonicalHeaders[count] = bridged
			count++
		}
	}
	return committed, maximumHead, count, true
}

func (replica *Replica) joinHeaderByChecksum(op protocol.Op, checksum protocol.Checksum) (protocol.Header, bool) {
	for sender := range replica.joins {
		header, present := replica.joinHeaderAt(uint8(sender), op)
		if present && header.HeaderChecksum == checksum {
			return header, true
		}
	}
	return protocol.Header{}, false
}

func (replica *Replica) canonicalCandidate(op protocol.Op) (protocol.Header, bool, bool) {
	latest := replica.canonicalLogView()
	bound := max(replica.commitMin, replica.commitMax, replica.checkpoint.PrepareOp())
	for sender := range replica.joins {
		if replica.joins[sender].valid {
			bound = max(bound, replica.joinCommittedBound(uint8(sender)))
		}
	}
	var selected protocol.Header
	found := false
	selectedLog := protocol.View(0)
	for sender := range replica.joins {
		record := &replica.joins[sender]
		header, present := replica.joinHeaderAt(uint8(sender), op)
		if !present || (record.logView != latest && op > bound) {
			continue
		}
		if record.logView == latest {
			for other := 0; other < sender; other++ {
				prior, exists := replica.joinHeaderAt(uint8(other), op)
				if exists && replica.joins[other].logView == latest && prior.View == header.View && prior.HeaderChecksum != header.HeaderChecksum {
					return protocol.Header{}, false, true
				}
			}
		}
		newerHeader := record.logView == selectedLog && header.View > selected.View
		if !found || record.logView > selectedLog || newerHeader {
			selected, selectedLog, found = header, record.logView, true
			continue
		}
		if record.logView == selectedLog && header.View == selected.View && header.HeaderChecksum != selected.HeaderChecksum {
			return protocol.Header{}, false, true
		}
	}
	return selected, found, false
}

func (replica *Replica) canonicalLogView() protocol.View {
	var latest protocol.View
	for _, record := range replica.joins {
		if record.valid {
			latest = max(latest, record.logView)
		}
	}
	return latest
}

func (replica *Replica) joinCommittedBound(sender uint8) protocol.Op {
	record := &replica.joins[sender]
	bound := max(record.commit, record.checkpoint, record.head-protocol.Op(record.count-1))
	if record.head > protocol.Op(replica.config.Cluster.PipelineMax) {
		bound = max(bound, record.head-protocol.Op(replica.config.Cluster.PipelineMax))
	}
	if head, found := replica.joinHeaderAt(sender, record.head); found {
		bound = max(bound, prepareCommit(&head))
	}
	return bound
}

func (replica *Replica) canonicalEvidence(op protocol.Op, candidate protocol.Header, candidateFound bool) (uint8, uint8) {
	var negatives, copies uint8
	latest := replica.canonicalLogView()
	for sender := range replica.joins {
		record := &replica.joins[sender]
		if !record.valid {
			continue
		}
		if record.head < op {
			negatives++
			continue
		}
		index := record.head - op
		if index >= protocol.Op(record.count) {
			continue
		}
		bit := uint16(1) << uint(index)
		header, present := replica.joinHeaderAt(uint8(sender), op)
		if present && candidateFound && header.HeaderChecksum == candidate.HeaderChecksum && record.present&bit != 0 {
			copies++
		}
		if record.nack&bit != 0 {
			negatives++
			continue
		}
		if !present || record.logView >= latest {
			continue
		}
		differentCandidate := candidateFound && header.HeaderChecksum != candidate.HeaderChecksum
		absentFromLatest := !candidateFound && header.View < latest
		if differentCandidate || absentFromLatest {
			negatives++
		}
	}
	return negatives, copies
}

func (replica *Replica) joinHeaderAt(sender uint8, op protocol.Op) (protocol.Header, bool) {
	record := &replica.joins[sender]
	if !record.valid || record.head < op {
		return protocol.Header{}, false
	}
	index := record.head - op
	if index >= protocol.Op(record.count) {
		return protocol.Header{}, false
	}
	header := replica.joinHeaderSlice(sender)[index]
	return header, header.Command != 0
}

func (replica *Replica) pipelineBusyForRepair() bool {
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if entry.io != (IOHandle{}) || entry.stage != CommitStageIdle {
			return true
		}
	}
	return false
}

func (replica *Replica) canSupersedeRepairView(target protocol.View) bool {
	return replica.repairViewValid && target > replica.repairView && !replica.pipelineBusyForRepair()
}

func (replica *Replica) clearRepairView() {
	for index := range replica.repairFrames {
		if replica.repairFrames[index] != nil {
			replica.repairFrames[index].Release()
			replica.repairFrames[index] = nil
		}
	}
	replica.repairBudget.Reset()
	replica.repairHeader = protocol.Header{}
	replica.repairHeaderValid = false
	replica.repairViewValid = false
	replica.repairViewWindow = false
	replica.repairViewPartial = false
	replica.repairViewRebuilt = false
	replica.repairView = 0
	replica.repairViewCommit = 0
	replica.repairViewHead = 0
	replica.repairViewAncestor = 0
	replica.repairViewPriorHead = 0
	replica.repairViewCount = 0
}

func (replica *Replica) handleView(header protocol.Header, body []byte) {
	invalidSource := header.Author != replica.membership.Primary(header.View) || header.View < replica.view || len(body) < CheckpointStateSize
	busy := replica.viewIO != (IOHandle{}) || replica.repairWrite.busy
	blockedRepair := replica.repairViewValid && !replica.canSupersedeRepairView(header.View)
	if invalidSource || busy || blockedRepair {
		return
	}
	var nonce protocol.Nonce
	copy(nonce[:], header.Fields[:16])
	matched := nonce != (protocol.Nonce{}) && nonce == replica.getViewNonce && header.View >= replica.getViewTarget
	if nonce != (protocol.Nonce{}) && !matched {
		return
	}
	head := protocol.Op(binary.LittleEndian.Uint64(header.Fields[16:24]))
	commit := protocol.Op(binary.LittleEndian.Uint64(header.Fields[24:32]))
	if commit < replica.commitMax || head < commit || head < replica.commitMin {
		return
	}
	if replica.status == StatusRecoveringHead {
		maximum, _ := replica.wal.prepareMaximum(replica.checkpoint.PrepareOp())
		if header.View == replica.view && head <= maximum && !matched {
			return
		}
	} else if replica.status == StatusNormal && header.View == replica.view && !matched {
		return
	}
	validation := checkpointRules{
		Group: replica.config.Group, MessageSizeMax: uint32(replica.config.Cluster.MessageSizeMax),
		MemberCount: replica.membership.ActiveCount + replica.membership.StandbyCount,
		BlockSize:   replica.config.Cluster.BlockSize, ClientsMax: replica.config.Cluster.ClientsMax,
		ChecksumAlgorithm: replica.config.Cluster.ChecksumAlgorithm,
	}
	validation.BlockBase, _ = replica.config.Cluster.BlockBase()
	checkpoint, err := decodeCheckpointState(body[:CheckpointStateSize], validation)
	if err != nil || checkpoint.PrepareOp() < replica.checkpoint.PrepareOp() {
		return
	}
	count := (len(body) - CheckpointStateSize) / protocol.HeaderSize
	maxHeaders := min(len(replica.canonicalHeaders), superblockHeadersMax)
	if count == 0 || count > maxHeaders || CheckpointStateSize+count*protocol.HeaderSize != len(body) {
		return
	}
	var incomingBuf [superblockHeadersMax]protocol.Header
	incoming := incomingBuf[:count]
	for index := range count {
		candidate, reason := protocol.DecodeHeader(body[CheckpointStateSize+index*protocol.HeaderSize:CheckpointStateSize+(index+1)*protocol.HeaderSize], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount, replica.config.Cluster.ChecksumAlgorithm)
		if reason != protocol.RejectNone || candidate.View > header.View {
			return
		}
		incoming[index] = candidate
	}
	if !replica.validStateSyncView(checkpoint, head, commit, incoming) {
		return
	}
	tail := count - 1
	if tail > 0 && prepareOp(&incoming[tail]) == checkpoint.PrepareOp() {
		tail--
	}
	if prepareOp(&incoming[tail]) > commit+1 {
		return
	}
	if replica.stateSyncAwaitingOpen() && checkpoint.PrepareOp() == replica.checkpoint.PrepareOp() {
		return
	}
	if checkpoint.PrepareOp() > replica.checkpoint.PrepareOp() {
		if !replica.beginStateSync(checkpoint, header.View, commit, head, incoming) {
			return
		}
	} else if commit > replica.commitMin && head-replica.commitMin > protocol.Op(len(replica.pipeline)) {
		sameViewPipeline := replica.status == StatusNormal && header.View == replica.view && !replica.isPrimary() && replica.pipelineLen > 0
		if sameViewPipeline {
			replica.commitMax = max(replica.commitMax, commit)
		} else if replica.commitInFlight() || !replica.beginRepairWindow(header.View, commit) {
			return
		}
	} else {
		if !replica.validRecoveringView(header.View, commit, head, incoming) {
			return
		}
		replica.status = StatusRecoveringHead
		replica.releaseQueuedRequests()
		replica.recordRecoveringView(header.View, commit, head, incoming)
	}
	replica.observePeerCheckpoint(header.Author, checkpoint.PrepareOp())
	replica.getViewNonce = protocol.Nonce{}
	replica.getViewLast = 0
}

func (replica *Replica) validStateSyncView(checkpoint CheckpointState, head, commit protocol.Op, headers []protocol.Header) bool {
	checkpointOp := checkpoint.PrepareOp()
	return commit >= checkpointOp && head >= commit && replica.validViewProof(checkpoint, head, headers)
}

func (replica *Replica) validViewProof(checkpoint CheckpointState, head protocol.Op, headers []protocol.Header) bool {
	count := len(headers)
	if count <= 0 || protocol.Op(count-1) > head || prepareOp(&headers[0]) != head {
		return false
	}
	checkpointHeader, reason := protocol.DecodeHeader(checkpoint.Header[:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount, replica.config.Cluster.ChecksumAlgorithm)
	if reason != protocol.RejectNone {
		return false
	}
	for index := range count {
		candidate := headers[index]
		if candidate.HeaderChecksum == checkpointHeader.HeaderChecksum && prepareOp(&candidate) == checkpoint.PrepareOp() {
			return index == count-1
		}
		if prepareOp(&candidate) != head-protocol.Op(index) {
			return false
		}
		if index+1 < count {
			next := headers[index+1]
			if prepareOp(&next) == checkpoint.PrepareOp() && next.HeaderChecksum == checkpointHeader.HeaderChecksum && prepareOp(&candidate) > checkpoint.PrepareOp()+1 {
				continue
			}
			if prepareParent(&candidate) != next.HeaderChecksum {
				return false
			}
		}
	}
	return false
}

func (replica *Replica) validRecoveringView(view protocol.View, commit, head protocol.Op, headers []protocol.Header) bool {
	count := len(headers)
	if replica.commitInFlight() || count == 0 || head < replica.commitMin {
		return false
	}
	if replica.repairWrite.busy || replica.viewIO != (IOHandle{}) {
		return false
	}
	if view < replica.view || commit < replica.commitMax || head < commit {
		return false
	}
	if head-replica.commitMin > protocol.Op(len(replica.pipeline)) {
		return false
	}
	proofCount := count
	if count > 1 && prepareOp(&headers[count-1]) == replica.checkpoint.PrepareOp() {
		proofCount--
	}
	for index := range proofCount {
		header := headers[index]
		expectedOp := head - protocol.Op(index)
		if prepareOp(&header) != expectedOp || header.View > view {
			return false
		}
		if index > 0 && prepareParent(&headers[index-1]) != header.HeaderChecksum {
			return false
		}
	}
	ancestor, found := replica.recoveringCommonAncestor(headers[:proofCount])
	if !found || ancestor < replica.commitMin {
		return false
	}
	return prepareOp(&headers[proofCount-1]) <= ancestor+1
}

func (replica *Replica) recoveringCommonAncestor(headers []protocol.Header) (protocol.Op, bool) {
	count := len(headers)
	if count == 0 {
		return 0, false
	}
	for index := range count {
		canonical := headers[index]
		op := prepareOp(&canonical)
		if op > replica.headOp {
			continue
		}
		local, present, dirty, found := replica.localHeaderEvidence(op)
		if found && present && !dirty && local.HeaderChecksum == canonical.HeaderChecksum {
			return op, true
		}
		if op <= replica.commitMin && found && present {
			return 0, false
		}
	}
	oldest := headers[count-1]
	oldestOp := prepareOp(&oldest)
	if oldestOp > 0 && oldestOp-1 <= replica.headOp {
		parent, present, dirty, found := replica.localHeaderEvidence(oldestOp - 1)
		if found && present && !dirty && prepareParent(&oldest) == parent.HeaderChecksum {
			return oldestOp - 1, true
		}
	}
	return 0, false
}

func (replica *Replica) recordRecoveringView(view protocol.View, commit, head protocol.Op, headers []protocol.Header) {
	replica.clearRepairView()
	copy(replica.canonicalHeaders, headers)
	clear(replica.canonicalHeaders[len(headers):])
	replica.repairViewValid = true
	replica.repairView = view
	proofCount := len(headers)
	if proofCount > 1 && prepareOp(&replica.canonicalHeaders[proofCount-1]) == replica.checkpoint.PrepareOp() {
		proofCount--
	}
	ancestor, _ := replica.recoveringCommonAncestor(replica.canonicalHeaders[:proofCount])
	replica.repairViewAncestor = ancestor
	replica.repairViewCommit = commit
	replica.repairViewHead = head
	replica.repairViewRebuilt = false
	replica.repairViewCount = len(headers)
	replica.commitMax = max(replica.commitMax, commit)
}

func (replica *Replica) broadcastView() {
	message, err := replica.buildView(protocol.Nonce{})
	if err != nil {
		replica.fail(err)
		return
	}
	replica.deps.MessageBus.BroadcastReplicas(message)
	message.Release()
}

func (replica *Replica) handleGetView(request protocol.Header) {
	if !replica.isPrimary() || replica.durableView != replica.view || replica.status != StatusNormal {
		return
	}
	var nonce protocol.Nonce
	copy(nonce[:], request.Fields[:16])
	message, err := replica.buildView(nonce)
	if err != nil {
		return
	}
	replica.deps.MessageBus.SendReplica(request.Author, message)
	message.Release()
}

func (replica *Replica) collectViewSuffix() (int, error) {
	count := replica.collectLocalSuffix(replica.canonicalHeaders)
	checkpointOp := replica.checkpoint.PrepareOp()
	for index := range count {
		if prepareOp(&replica.canonicalHeaders[index]) == checkpointOp {
			return count, nil
		}
	}
	checkpoint, reason := protocol.DecodeHeader(replica.checkpoint.Header[:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount, replica.config.Cluster.ChecksumAlgorithm)
	if reason != protocol.RejectNone {
		return 0, ErrInvalidCheckpoint
	}
	if count == len(replica.canonicalHeaders) {
		count--
	}
	replica.canonicalHeaders[count] = checkpoint
	return count + 1, nil
}

func (replica *Replica) buildView(nonce protocol.Nonce) (*Message, error) {
	count, err := replica.collectViewSuffix()
	if err != nil {
		return nil, err
	}
	message, err := replica.frames.Acquire(uint32(CheckpointStateSize + count*protocol.HeaderSize))
	if err != nil {
		return nil, err
	}
	body, err := message.Body()
	if err != nil {
		message.Release()
		return nil, err
	}
	if err := replica.checkpoint.encode(body[:CheckpointStateSize]); err != nil {
		message.Release()
		return nil, err
	}
	for index := range count {
		header := replica.canonicalHeaders[index]
		if err := protocol.EncodeHeader(body[CheckpointStateSize+index*protocol.HeaderSize:CheckpointStateSize+(index+1)*protocol.HeaderSize], &header, replica.config.Cluster.ChecksumAlgorithm); err != nil {
			message.Release()
			return nil, err
		}
	}
	header := protocol.Header{Group: replica.config.Group, View: replica.view, Protocol: protocol.ProtocolVersion, Command: protocol.CommandView, Author: replica.local}
	copy(header.Fields[:16], nonce[:])
	binary.LittleEndian.PutUint64(header.Fields[16:24], uint64(replica.headOp))
	binary.LittleEndian.PutUint64(header.Fields[24:32], uint64(replica.commitMax))
	binary.LittleEndian.PutUint64(header.Fields[32:40], uint64(replica.checkpoint.PrepareOp()))
	if err := message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm); err != nil {
		message.Release()
		return nil, err
	}
	return message, nil
}
