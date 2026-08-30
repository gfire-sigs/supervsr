package replication

import (
	"encoding/binary"
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func (replica *Replica) handleHigherViewEvidence(header protocol.Header) bool {
	if header.View <= replica.view || uint8(header.Author) >= replica.membership.ActiveCount {
		return false
	}
	switch header.Command {
	case protocol.CommandExitView, protocol.CommandJoinView, protocol.CommandPing, protocol.CommandPong:
		if uint8(replica.local) >= replica.membership.ActiveCount {
			return false
		}
		replica.beginViewChange(header.View)
		return true
	case protocol.CommandRequest, protocol.CommandClientPing:
		return false
	case protocol.CommandPrepare:
		return !(replica.repairHeaderValid &&
			prepareOp(&header) == prepareOp(&replica.repairHeader) &&
			header.HeaderChecksum == replica.repairHeader.HeaderChecksum)
	case protocol.CommandCommit:
		return true
	case protocol.CommandView:
		return false
	default:
		return true
	}
}

func (replica *Replica) handleExitView(header protocol.Header) {
	if header.View > replica.view {
		replica.beginViewChange(header.View)
		return
	}
	if replica.status != StatusNormal || header.View != replica.view || uint8(header.Author) >= replica.membership.ActiveCount {
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

func (replica *Replica) beginViewChange(target protocol.View) {
	if target <= replica.view || replica.status == StatusRecoveringHead {
		return
	}
	if replica.viewIO != (IOHandle{}) || replica.checkpointTransitionActive() {
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
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		entry.acks = 0
		entry.quorum = false
	}
	replica.releaseQueuedRequests()
	count := replica.collectLocalSuffix(replica.canonicalHeaders)
	if err := replica.persistView(false, replica.commitMax, replica.headOp, replica.canonicalHeaders[:count]); err != nil {
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
	next.ViewHeaderCount = uint32(len(headers))
	clear(next.ViewHeaders[:])
	for index := range headers {
		var encoded [protocol.HeaderSize]byte
		header := headers[index]
		if header.Command == 0 {
			continue
		}
		if err := protocol.EncodeHeader(encoded[:], &header); err != nil {
			return err
		}
		next.ViewHeaders[index] = encoded
	}
	handle, err := replica.io.Submit(IOOperation{Kind: IOSuperblockPersist, SuperblockStore: replica.superblocks, Superblock: next})
	if err != nil {
		return err
	}
	replica.viewIO = handle
	replica.viewInstall = install
	replica.viewCommit = commit
	replica.viewHead = head
	return nil
}

func (replica *Replica) handleViewPersistence(completion IOCompletion) bool {
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
	replica.logView = replica.view
	replica.commitMax = max(replica.commitMax, replica.viewCommit)
	replica.status = StatusNormal
	replica.refreshLocalReleaseReport()
	replica.maybeSelectUpgrade()
	replica.viewInstall = false
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if !entry.durable {
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
			if err := protocol.EncodeHeader(body[index*protocol.HeaderSize:(index+1)*protocol.HeaderSize], &header); err != nil {
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
	if err := message.Seal(&header); err != nil {
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
		header, reason := protocol.DecodeHeader(replica.checkpoint.Header[:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
		return header, reason == protocol.RejectNone, false, reason == protocol.RejectNone
	}
	header, present, nack := replica.wal.JoinEvidence(op)
	return header, present, nack, present
}

func (replica *Replica) handleJoinView(header protocol.Header, body []byte) {
	if header.View > replica.view {
		replica.beginViewChange(header.View)
		return
	}
	if replica.status == StatusNormal && header.View == replica.view && replica.isPrimary() && replica.durableView == replica.view && replica.logView == replica.view {
		replica.broadcastView()
		return
	}
	if replica.status != StatusViewChange || header.View != replica.view || !replica.isPrimary() || replica.durableView != replica.view || uint8(header.Author) >= replica.membership.ActiveCount {
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
	replica.observePeerCheckpoint(header.Author, record.checkpoint)
	headers := replica.joinHeaderSlice(sender)
	clear(headers)
	for index := range count {
		encoded := body[index*protocol.HeaderSize : (index+1)*protocol.HeaderSize]
		if allZeroBytes(encoded) {
			continue
		}
		candidate, reason := protocol.DecodeHeader(encoded, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
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
	localAvailable := commit-replica.commitMin <= protocol.Op(replica.pipelineLen) &&
		replica.canonicalAvailable(head, count)
	if commit == replica.commitMin && localAvailable {
		if err := replica.persistView(true, commit, head, replica.canonicalHeaders[:count]); err != nil {
			replica.fail(err)
		}
		return
	}
	if !replica.validRecoveringView(replica.view, commit, head, count) {
		return
	}
	replica.status = StatusRecoveringHead
	replica.recordRecoveringView(replica.view, commit, head, count)
}

func (replica *Replica) selectCanonicalSuffix() (protocol.Op, protocol.Op, int, bool) {
	maximumHead := replica.checkpoint.PrepareOp()
	committed := replica.checkpoint.PrepareOp()
	for sender := range replica.joins {
		record := &replica.joins[sender]
		if !record.valid {
			continue
		}
		maximumHead = max(maximumHead, record.head)
		committed = max(committed, record.commit)
		if record.head > protocol.Op(replica.config.Cluster.PipelineMax) {
			committed = max(committed, record.head-protocol.Op(replica.config.Cluster.PipelineMax))
		}
		if record.count != 0 {
			head := replica.joinHeaderSlice(uint8(sender))[0]
			if head.Command != 0 {
				committed = max(committed, prepareCommit(&head))
			}
		}
	}
	if maximumHead-committed >= protocol.Op(len(replica.canonicalHeaders)) {
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
	return committed, maximumHead, count, true
}

func (replica *Replica) canonicalCandidate(op protocol.Op) (protocol.Header, bool, bool) {
	var selected protocol.Header
	found := false
	for sender := range replica.joins {
		record := &replica.joins[sender]
		header, present := replica.joinHeaderAt(uint8(sender), op)
		if !record.valid || !present {
			continue
		}
		if !found || header.View > selected.View {
			selected = header
			found = true
			continue
		}
		if header.View == selected.View && header.HeaderChecksum != selected.HeaderChecksum {
			return protocol.Header{}, false, true
		}
	}
	return selected, found, false
}

func (replica *Replica) canonicalEvidence(op protocol.Op, candidate protocol.Header, candidateFound bool) (uint8, uint8) {
	var negatives, copies uint8
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
			negatives++
			continue
		}
		bit := uint16(1) << uint(index)
		if record.nack&bit != 0 {
			negatives++
			continue
		}
		header, present := replica.joinHeaderAt(uint8(sender), op)
		if present && candidateFound && header.HeaderChecksum == candidate.HeaderChecksum && record.present&bit != 0 {
			copies++
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

func (replica *Replica) canonicalAvailable(head protocol.Op, count int) bool {
	if count == 0 || prepareOp(&replica.canonicalHeaders[0]) != head {
		return false
	}
	for index := range count {
		header := replica.canonicalHeaders[index]
		op := prepareOp(&header)
		if op <= replica.commitMin {
			continue
		}
		local, present, dirty, found := replica.localHeaderEvidence(op)
		if !found || !present || dirty || local.HeaderChecksum != header.HeaderChecksum {
			return false
		}
	}
	return true
}

func (replica *Replica) handleView(header protocol.Header, body []byte) {
	if header.Author != replica.membership.Primary(header.View) || header.View < replica.view || len(body) < CheckpointStateSize {
		return
	}
	validation := CheckpointValidation{
		Group:          replica.config.Group,
		MessageSizeMax: uint32(replica.config.Cluster.MessageSizeMax),
		MemberCount:    replica.membership.ActiveCount + replica.membership.StandbyCount,
		BlockSize:      replica.config.Cluster.BlockSize,
		ClientsMax:     replica.config.Cluster.ClientsMax,
	}
	validation.BlockBase, _ = replica.config.Cluster.BlockBase()
	checkpoint, err := DecodeCheckpointState(body[:CheckpointStateSize], validation)
	if err != nil {
		return
	}
	replica.observePeerCheckpoint(header.Author, checkpoint.PrepareOp())
	head := protocol.Op(binary.LittleEndian.Uint64(header.Fields[16:24]))
	commit := protocol.Op(binary.LittleEndian.Uint64(header.Fields[24:32]))
	count := (len(body) - CheckpointStateSize) / protocol.HeaderSize
	if count == 0 || count > len(replica.canonicalHeaders) || CheckpointStateSize+count*protocol.HeaderSize != len(body) {
		return
	}
	for index := range count {
		candidate, reason := protocol.DecodeHeader(body[CheckpointStateSize+index*protocol.HeaderSize:CheckpointStateSize+(index+1)*protocol.HeaderSize], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
		if reason != protocol.RejectNone {
			return
		}
		replica.canonicalHeaders[index] = candidate
	}
	if checkpoint.PrepareOp() > replica.checkpoint.PrepareOp() {
		if !replica.validStateSyncView(checkpoint, head, commit, count) {
			return
		}
		replica.beginStateSync(checkpoint, header.View, commit, head, replica.canonicalHeaders[:count])
		replica.getViewNonce = protocol.Nonce{}
		replica.getViewLast = 0
		return
	}
	if checkpoint.PrepareOp() != replica.checkpoint.PrepareOp() {
		return
	}
	if !replica.validViewProof(checkpoint, head, count) {
		return
	}
	if commit > replica.commitMin && head-replica.commitMin > protocol.Op(len(replica.pipeline)) {
		if !replica.beginRepairWindow(header.View, commit) {
			return
		}
		replica.getViewNonce = protocol.Nonce{}
		replica.getViewLast = 0
		return
	}
	recoveringHead := replica.status == StatusRecoveringHead
	if recoveringHead {
		var nonce protocol.Nonce
		copy(nonce[:], header.Fields[:16])
		maximum, _ := replica.wal.prepareMaximum(replica.checkpoint.PrepareOp())
		fresh := header.View > replica.view || head > maximum || (nonce != protocol.Nonce{} && nonce == replica.getViewNonce)
		if !fresh {
			return
		}
	} else {
		if header.View > replica.view {
			replica.beginViewChange(header.View)
		}
		if replica.status != StatusViewChange || replica.durableView != header.View || replica.viewIO != (IOHandle{}) {
			return
		}
	}
	if recoveringHead {
		if !replica.validRecoveringView(header.View, commit, head, count) {
			return
		}
		replica.recordRecoveringView(header.View, commit, head, count)
		replica.getViewNonce = protocol.Nonce{}
		replica.getViewLast = 0
		return
	}
	if commit < replica.commitMin || commit-replica.commitMin > protocol.Op(replica.pipelineLen) || !replica.canonicalAvailable(head, count) {
		replica.status = StatusRecoveringHead
		if !replica.validRecoveringView(header.View, commit, head, count) {
			return
		}
		replica.recordRecoveringView(header.View, commit, head, count)
		replica.getViewNonce = protocol.Nonce{}
		replica.getViewLast = 0
		return
	}
	if err := replica.persistView(true, commit, head, replica.canonicalHeaders[:count]); err != nil {
		replica.fail(err)
		return
	}
	replica.getViewNonce = protocol.Nonce{}
	replica.getViewLast = 0
}

func (replica *Replica) validStateSyncView(checkpoint CheckpointState, head, commit protocol.Op, count int) bool {
	checkpointOp := checkpoint.PrepareOp()
	return commit >= checkpointOp && head >= commit && replica.validViewProof(checkpoint, head, count)
}

func (replica *Replica) validViewProof(checkpoint CheckpointState, head protocol.Op, count int) bool {
	if count <= 0 || protocol.Op(count-1) > head || prepareOp(&replica.canonicalHeaders[0]) != head {
		return false
	}
	checkpointHeader, reason := protocol.DecodeHeader(checkpoint.Header[:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
	if reason != protocol.RejectNone {
		return false
	}
	for index := range count {
		candidate := replica.canonicalHeaders[index]
		if candidate.HeaderChecksum == checkpointHeader.HeaderChecksum && prepareOp(&candidate) == checkpoint.PrepareOp() {
			return index == count-1
		}
		if prepareOp(&candidate) != head-protocol.Op(index) {
			return false
		}
		if index+1 < count {
			next := replica.canonicalHeaders[index+1]
			if prepareOp(&next) == checkpoint.PrepareOp() && next.HeaderChecksum == checkpointHeader.HeaderChecksum {
				continue
			}
			if prepareParent(&candidate) != next.HeaderChecksum {
				return false
			}
		}
	}
	return false
}

func (replica *Replica) validRecoveringView(view protocol.View, commit, head protocol.Op, count int) bool {
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
	if count > 1 && prepareOp(&replica.canonicalHeaders[count-1]) == replica.checkpoint.PrepareOp() {
		proofCount--
	}
	for index := range proofCount {
		header := replica.canonicalHeaders[index]
		expectedOp := head - protocol.Op(index)
		if prepareOp(&header) != expectedOp || header.View > view {
			return false
		}
		if index > 0 && prepareParent(&replica.canonicalHeaders[index-1]) != header.HeaderChecksum {
			return false
		}
	}
	ancestor, found := replica.recoveringCommonAncestor(proofCount)
	if !found || ancestor < replica.commitMin {
		return false
	}
	return prepareOp(&replica.canonicalHeaders[proofCount-1]) <= ancestor+1
}

func (replica *Replica) recoveringCommonAncestor(count int) (protocol.Op, bool) {
	for index := range count {
		canonical := replica.canonicalHeaders[index]
		op := prepareOp(&canonical)
		if op > replica.headOp {
			continue
		}
		local, present, dirty, found := replica.localHeaderEvidence(op)
		if found && present && !dirty && local.HeaderChecksum == canonical.HeaderChecksum {
			return op, true
		}
	}
	oldest := replica.canonicalHeaders[count-1]
	oldestOp := prepareOp(&oldest)
	if oldestOp == replica.headOp+1 && prepareParent(&oldest) == replica.headChecksum {
		return replica.headOp, true
	}
	checkpointOp := replica.checkpoint.PrepareOp()
	if oldestOp == checkpointOp {
		checkpoint, reason := protocol.DecodeHeader(replica.checkpoint.Header[:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
		if reason == protocol.RejectNone && checkpoint.HeaderChecksum == oldest.HeaderChecksum {
			return checkpointOp, true
		}
	}
	return 0, false
}

func (replica *Replica) recordRecoveringView(view protocol.View, commit, head protocol.Op, count int) {
	for index := range replica.repairFrames {
		if replica.repairFrames[index] != nil {
			replica.repairFrames[index].Release()
			replica.repairFrames[index] = nil
		}
	}
	replica.repairBudget.Reset()
	replica.repairHeader = protocol.Header{}
	replica.repairHeaderValid = false
	replica.repairViewWindow = false
	replica.repairViewValid = true
	replica.repairView = view
	ancestor, _ := replica.recoveringCommonAncestor(count)
	replica.repairViewAncestor = ancestor
	replica.repairViewCommit = commit
	replica.repairViewHead = head
	replica.repairViewRebuilt = false
	replica.repairViewCount = count
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
	checkpoint, reason := protocol.DecodeHeader(replica.checkpoint.Header[:], replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
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
	if err := replica.checkpoint.Encode(body[:CheckpointStateSize]); err != nil {
		message.Release()
		return nil, err
	}
	for index := range count {
		header := replica.canonicalHeaders[index]
		if err := protocol.EncodeHeader(body[CheckpointStateSize+index*protocol.HeaderSize:CheckpointStateSize+(index+1)*protocol.HeaderSize], &header); err != nil {
			message.Release()
			return nil, err
		}
	}
	header := protocol.Header{Group: replica.config.Group, View: replica.view, Protocol: protocol.ProtocolVersion, Command: protocol.CommandView, Author: replica.local}
	copy(header.Fields[:16], nonce[:])
	binary.LittleEndian.PutUint64(header.Fields[16:24], uint64(replica.headOp))
	binary.LittleEndian.PutUint64(header.Fields[24:32], uint64(replica.commitMax))
	binary.LittleEndian.PutUint64(header.Fields[32:40], uint64(replica.checkpoint.PrepareOp()))
	if err := message.Seal(&header); err != nil {
		message.Release()
		return nil, err
	}
	return message, nil
}
