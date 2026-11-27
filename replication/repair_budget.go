package replication

import (
	"time"

	"z.gfire.dev/supervsr/replication/protocol"
)

const journalRepairsPerPeer = 2

type journalRepairEntry struct {
	busy    bool
	peer    protocol.ReplicaIndex
	op      protocol.Op
	started uint64
}

type journalRepairBudget struct {
	active      uint8
	local       protocol.ReplicaIndex
	capacity    uint8
	outstanding uint8
	ewma        [ActiveMax]uint64
	entries     [ActiveMax * journalRepairsPerPeer]journalRepairEntry
}

func newJournalRepairBudget(active uint8, local protocol.ReplicaIndex) (journalRepairBudget, error) {
	if active == 0 || active > ActiveMax || uint8(local) >= MembersMax {
		return journalRepairBudget{}, ErrInvalidConfiguration
	}
	peers := active
	if uint8(local) < active {
		peers--
	}
	budget := journalRepairBudget{active: active, local: local, capacity: peers * journalRepairsPerPeer}
	for peer := range active {
		budget.ewma[peer] = uint64(time.Millisecond)
	}
	return budget, nil
}

func (budget *journalRepairBudget) Reserve(op protocol.Op, now uint64, random *deterministicRandom) (protocol.ReplicaIndex, bool, error) {
	if random == nil || budget.outstanding == budget.capacity {
		return 0, false, nil
	}
	explore := random.Uniform(10) == 0
	selected := protocol.ReplicaIndex(0)
	selectedEWMA := ^uint64(0)
	eligible := uint64(0)
	found := false
	for peerValue := range budget.active {
		peer := protocol.ReplicaIndex(peerValue)
		if peer == budget.local || !budget.peerEligible(peer, op) {
			continue
		}
		eligible++
		if explore {
			if random.Uniform(eligible) == 0 {
				selected = peer
				found = true
			}
			continue
		}
		if !found || budget.ewma[peer] < selectedEWMA {
			selected = peer
			selectedEWMA = budget.ewma[peer]
			found = true
		}
	}
	if !found {
		return 0, false, nil
	}
	for index := range budget.entries {
		entry := &budget.entries[index]
		if entry.busy {
			continue
		}
		*entry = journalRepairEntry{busy: true, peer: selected, op: op, started: now}
		budget.outstanding++
		if err := budget.checkInvariant(); err != nil {
			return 0, false, err
		}
		return selected, true, nil
	}
	return 0, false, ErrReplicaInvariant
}

func (budget *journalRepairBudget) Complete(op protocol.Op, now uint64) (uint8, error) {
	released := uint8(0)
	for index := range budget.entries {
		entry := &budget.entries[index]
		if !entry.busy || entry.op != op {
			continue
		}
		budget.observe(entry.peer, elapsedSince(entry.started, now))
		*entry = journalRepairEntry{}
		released++
	}
	if released > budget.outstanding {
		return 0, ErrReplicaInvariant
	}
	budget.outstanding -= released
	if err := budget.checkInvariant(); err != nil {
		return 0, err
	}
	return released, nil
}

func (budget *journalRepairBudget) Expire(now uint64) (uint8, error) {
	expired := uint8(0)
	for index := range budget.entries {
		entry := &budget.entries[index]
		if !entry.busy {
			continue
		}
		elapsed := elapsedSince(entry.started, now)
		deadline := uint64(500 * time.Millisecond)
		if budget.ewma[entry.peer] < deadline/2 {
			deadline = 2 * budget.ewma[entry.peer]
		}
		if elapsed < deadline {
			continue
		}
		budget.observe(entry.peer, elapsed)
		*entry = journalRepairEntry{}
		expired++
	}
	if expired > budget.outstanding {
		return 0, ErrReplicaInvariant
	}
	budget.outstanding -= expired
	if err := budget.checkInvariant(); err != nil {
		return 0, err
	}
	return expired, nil
}

func (budget *journalRepairBudget) peerEligible(peer protocol.ReplicaIndex, op protocol.Op) bool {
	count := 0
	for index := range budget.entries {
		entry := budget.entries[index]
		if !entry.busy || entry.peer != peer {
			continue
		}
		if entry.op == op {
			return false
		}
		count++
	}
	return count < journalRepairsPerPeer
}

func (budget *journalRepairBudget) observe(peer protocol.ReplicaIndex, sample uint64) {
	sample = max(uint64(1), sample)
	old := budget.ewma[peer]
	budget.ewma[peer] = (old/5)*4 + (old%5*4+sample)/5
}

func (budget *journalRepairBudget) checkInvariant() error {
	if budget.outstanding > budget.capacity {
		return ErrReplicaInvariant
	}
	count := uint8(0)
	for index := range budget.entries {
		if budget.entries[index].busy {
			count++
		}
	}
	if count != budget.outstanding {
		return ErrReplicaInvariant
	}
	return nil
}

func (budget *journalRepairBudget) HasPending(op protocol.Op) bool {
	for index := range budget.entries {
		if budget.entries[index].busy && budget.entries[index].op == op {
			return true
		}
	}
	return false
}

func (budget *journalRepairBudget) Reset() error {
	for index := range budget.entries {
		budget.entries[index] = journalRepairEntry{}
	}
	budget.outstanding = 0
	return budget.checkInvariant()
}

func elapsedSince(start, now uint64) uint64 {
	if now <= start {
		return 0
	}
	return now - start
}
