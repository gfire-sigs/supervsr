package replication

import (
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

const (
	blockRepairRetryAfter = uint64(100 * time.Millisecond)
	blockRepairExpires    = uint64(250 * time.Millisecond)
)

type blockRepairBudgetEntry struct {
	reference BlockReference
	started   uint64
	busy      bool
}

type blockRepairRequestRecord struct {
	peer       protocol.ReplicaIndex
	started    uint64
	references []BlockReference
	busy       bool
}

type blockRepairBudget struct {
	memberCount       uint8
	local             protocol.ReplicaIndex
	batchMax          uint32
	perPeer           uint32
	requestsAvailable uint32
	available         []uint32
	entries           []blockRepairBudgetEntry
	requests          []blockRepairRequestRecord
}

func newBlockRepairBudget(memberCount uint8, local protocol.ReplicaIndex, batchMax uint32) (blockRepairBudget, error) {
	if memberCount == 0 || memberCount > MembersMax || batchMax == 0 {
		return blockRepairBudget{}, ErrInvalidConfiguration
	}
	perPeer := batchMax + 1
	requests := make([]blockRepairRequestRecord, int(batchMax))
	requestStorage := make([]BlockReference, int(batchMax)*int(batchMax))
	for index := range requests {
		start := index * int(batchMax)
		requests[index].references = requestStorage[start : start+int(batchMax)]
	}
	return blockRepairBudget{
		memberCount:       memberCount,
		local:             local,
		batchMax:          batchMax,
		perPeer:           perPeer,
		requestsAvailable: batchMax,
		available:         filledUint32(int(memberCount), perPeer),
		entries:           make([]blockRepairBudgetEntry, int(memberCount)*int(perPeer)),
		requests:          requests,
	}, nil
}

func (budget *blockRepairBudget) Reset() {
	clear(budget.entries)
	for index := range budget.requests {
		references := budget.requests[index].references
		clear(references)
		budget.requests[index] = blockRepairRequestRecord{references: references}
	}
	for index := range budget.available {
		budget.available[index] = budget.perPeer
	}
	budget.requestsAvailable = budget.batchMax
}

func filledUint32(count int, value uint32) []uint32 {
	values := make([]uint32, count)
	for index := range values {
		values[index] = value
	}
	return values
}

func (budget *blockRepairBudget) Reserve(peer protocol.ReplicaIndex, references []BlockReference, now uint64) bool {
	if uint8(peer) >= budget.memberCount || peer == budget.local || len(references) == 0 || len(references) > int(budget.batchMax) {
		return false
	}
	if budget.requestsAvailable == 0 || budget.available[peer] < uint32(len(references)) || !budget.referencesEligible(peer, references, now) {
		return false
	}
	peerEntries := budget.peerEntries(peer)
	for _, reference := range references {
		for index := range peerEntries {
			entry := &peerEntries[index]
			if entry.busy {
				continue
			}
			*entry = blockRepairBudgetEntry{reference: reference, started: now, busy: true}
			budget.available[peer]--
			break
		}
	}
	for index := range budget.requests {
		record := &budget.requests[index]
		if record.busy {
			continue
		}
		clear(record.references)
		copy(record.references, references)
		record.peer = peer
		record.started = now
		record.busy = true
		budget.requestsAvailable--
		break
	}
	return budget.valid()
}

func (budget *blockRepairBudget) referencesEligible(peer protocol.ReplicaIndex, references []BlockReference, now uint64) bool {
	for index, reference := range references {
		if reference.Address == 0 || reference.Checksum.IsZero() {
			return false
		}
		for prior := range index {
			if references[prior] == reference {
				return false
			}
		}
		for candidatePeer := range budget.memberCount {
			for _, entry := range budget.peerEntries(protocol.ReplicaIndex(candidatePeer)) {
				if !entry.busy || entry.reference != reference {
					continue
				}
				if protocol.ReplicaIndex(candidatePeer) == peer || elapsedSince(entry.started, now) < blockRepairRetryAfter {
					return false
				}
			}
		}
	}
	return true
}

func (budget *blockRepairBudget) Destination(random *DeterministicRandom) (protocol.ReplicaIndex, bool) {

	if random == nil || budget.requestsAvailable == 0 {
		return 0, false
	}
	start := uint8(random.Uniform(uint64(budget.memberCount)))
	step := uint8(1)
	if budget.memberCount > 1 {
		step = uint8(random.Uniform(uint64(budget.memberCount-1))) + 1
		for commonDivisor(step, budget.memberCount) != 1 {
			step++
			if step >= budget.memberCount {
				step = 1
			}
		}
	}
	for offset := range budget.memberCount {
		peer := (start + offset*step) % budget.memberCount
		if protocol.ReplicaIndex(peer) != budget.local && budget.available[peer] >= budget.batchMax {
			return protocol.ReplicaIndex(peer), true
		}
	}
	return 0, false
}
func (budget *blockRepairBudget) CanSend(peer protocol.ReplicaIndex) bool {
	if uint8(peer) >= budget.memberCount || peer == budget.local || budget.requestsAvailable == 0 {
		return false
	}
	return budget.available[peer] >= budget.batchMax
}

func commonDivisor(left, right uint8) uint8 {
	for right != 0 {
		left, right = right, left%right
	}
	return left
}

func (budget *blockRepairBudget) Fulfill(reference BlockReference) int {
	released := 0
	for peer := range budget.memberCount {
		entries := budget.peerEntries(protocol.ReplicaIndex(peer))
		for index := range entries {
			entry := &entries[index]
			if !entry.busy || entry.reference != reference {
				continue
			}
			*entry = blockRepairBudgetEntry{}
			budget.available[peer]++
			released++
		}
	}
	for index := range budget.requests {
		record := &budget.requests[index]
		if !record.busy {
			continue
		}
		for referenceIndex := range record.references {
			if record.references[referenceIndex] == reference {
				record.references[referenceIndex] = BlockReference{}
			}
		}
		if referencesEmpty(record.references) {
			budget.releaseRequest(record)
		}
	}
	if !budget.valid() {
		panic(ErrReplicaInvariant)
	}
	return released
}

func (budget *blockRepairBudget) Expire(now uint64) int {
	expired := 0
	for index := range budget.requests {
		record := &budget.requests[index]
		if !record.busy || elapsedSince(record.started, now) < blockRepairExpires {
			continue
		}
		for _, reference := range record.references {
			if reference == (BlockReference{}) {
				continue
			}
			if budget.releaseEntry(record.peer, reference, record.started) {
				expired++
			}
		}
		budget.releaseRequest(record)
	}
	if !budget.valid() {
		panic(ErrReplicaInvariant)
	}
	return expired
}

func (budget *blockRepairBudget) releaseEntry(peer protocol.ReplicaIndex, reference BlockReference, started uint64) bool {
	entries := budget.peerEntries(peer)
	for index := range entries {
		entry := &entries[index]
		if !entry.busy || entry.reference != reference || entry.started != started {
			continue
		}
		*entry = blockRepairBudgetEntry{}
		budget.available[peer]++
		return true
	}
	return false
}

func (budget *blockRepairBudget) releaseRequest(record *blockRepairRequestRecord) {
	references := record.references
	clear(references)
	*record = blockRepairRequestRecord{references: references}
	budget.requestsAvailable++
}

func referencesEmpty(references []BlockReference) bool {
	for _, reference := range references {
		if reference != (BlockReference{}) {
			return false
		}
	}
	return true
}

func (budget *blockRepairBudget) Outstanding(reference BlockReference) bool {
	for _, entry := range budget.entries {
		if entry.busy && entry.reference == reference {
			return true
		}
	}
	return false
}

func (budget *blockRepairBudget) peerEntries(peer protocol.ReplicaIndex) []blockRepairBudgetEntry {
	start := int(peer) * int(budget.perPeer)
	return budget.entries[start : start+int(budget.perPeer)]
}

func (budget *blockRepairBudget) valid() bool {
	busyRequests := uint32(0)
	for _, request := range budget.requests {
		if request.busy {
			busyRequests++
		}
	}
	if busyRequests+budget.requestsAvailable != budget.batchMax {
		return false
	}
	for peer := range budget.memberCount {
		busy := uint32(0)
		for _, entry := range budget.peerEntries(protocol.ReplicaIndex(peer)) {
			if entry.busy {
				busy++
			}
		}
		if busy+budget.available[peer] != budget.perPeer {
			return false
		}
	}
	return true
}
