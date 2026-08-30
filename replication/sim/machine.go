package sim

import (
	"sync"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type Commit struct {
	Operation protocol.Operation
	Body      []byte
	Timestamp uint64
	Op        protocol.Op
	Release   protocol.Release
}

type Machine struct {
	mu         sync.Mutex
	capacities replication.StateMachineCapacities
	commits    []Commit
}

func NewMachine(capacities replication.StateMachineCapacities) *Machine {
	return &Machine{capacities: capacities}
}

func (machine *Machine) Capacities() replication.StateMachineCapacities {
	return machine.capacities
}

func (*Machine) Validate(replication.ValidateInput) replication.ValidationResult {
	return replication.ValidationOK
}

func (*Machine) PulseNeeded(uint64) bool {
	return false
}

func (*Machine) StartPrefetch(replication.PrefetchInput, *replication.SMCompletion) (replication.StartResult[replication.PrefetchToken], error) {
	return replication.Ready(replication.PrefetchToken(0)), nil
}

func (machine *Machine) Commit(input replication.CommitInput, _ replication.PrefetchToken, reply []byte) (int, error) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	body := append([]byte(nil), input.Body...)
	machine.commits = append(machine.commits, Commit{
		Operation: input.Operation, Body: body, Timestamp: input.Timestamp, Op: input.Op, Release: input.Release,
	})
	return copy(reply, input.Body), nil
}

func (*Machine) StartCompact(replication.CompactInput, *replication.SMCompletion) (replication.StartResult[replication.CompactResult], error) {
	return replication.Ready(replication.CompactResult{}), nil
}

func (*Machine) StartCheckpoint(replication.CheckpointInput, *replication.SMCompletion) (replication.StartResult[replication.CheckpointManifest], error) {
	return replication.Ready(replication.CheckpointManifest{}), nil
}

func (machine *Machine) StartOpen(replication.OpenCheckpointInput, *replication.SMCompletion) (replication.StartResult[replication.OpenResult], error) {
	machine.mu.Lock()
	machine.commits = machine.commits[:0]
	machine.mu.Unlock()
	return replication.Ready(replication.OpenResult{}), nil
}

func (machine *Machine) StartReset(*replication.SMCompletion) (replication.StartResult[replication.ResetResult], error) {
	machine.mu.Lock()
	machine.commits = machine.commits[:0]
	machine.mu.Unlock()
	return replication.Ready(replication.ResetResult{}), nil
}

func (*Machine) Close() error {
	return nil
}

func (machine *Machine) Commits() []Commit {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	commits := make([]Commit, len(machine.commits))
	for index := range machine.commits {
		commits[index] = machine.commits[index]
		commits[index].Body = append([]byte(nil), machine.commits[index].Body...)
	}
	return commits
}
