package sim

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

const (
	LedgerIncrement      = protocol.OperationApplicationMin
	ledgerSnapshotBytes  = 40
	ledgerSnapshotFormat = uint64(1)
)

var ErrLedgerSnapshot = errors.New("sim: invalid ledger snapshot")

type LedgerSnapshot struct {
	Value  uint64
	LastOp protocol.Op
	Digest protocol.Checksum
}

type LedgerObservation struct {
	Op     protocol.Op
	Token  uint64
	Value  uint64
	Digest protocol.Checksum
}

type frozenLedger struct {
	valid bool
	state LedgerSnapshot
}

type LedgerMachine struct {
	mu            sync.Mutex
	compactionOps uint64
	capacities    replication.StateMachineCapacities
	state         LedgerSnapshot
	frozen        [2]frozenLedger
	recent        [CommitHistoryMax]LedgerObservation
	recentNext    int
	recentCount   int
}

func NewLedgerMachine(config replication.ClusterConfig) (*LedgerMachine, error) {
	invalidCompaction := config.CompactionOps == 0 || config.CompactionOps&(config.CompactionOps-1) != 0
	invalidRequest := config.ApplicationBatchSizeMax < 8 || config.ApplicationBatchSizeMax > uint64(^uint32(0))
	invalidReply := config.ApplicationReplySizeMax < 16 || config.ApplicationReplySizeMax > uint64(^uint32(0))
	invalidPipeline := config.PipelineMax == 0 || config.PipelineMax > uint64(^uint32(0))
	if invalidCompaction || config.BlockSize < protocol.HeaderSize+ledgerSnapshotBytes {
		return nil, replication.ErrInvalidConfiguration
	}
	if invalidRequest || invalidReply || invalidPipeline {
		return nil, replication.ErrInvalidConfiguration
	}
	return &LedgerMachine{
		compactionOps: config.CompactionOps,
		capacities: replication.StateMachineCapacities{
			RequestBytes: uint32(config.ApplicationBatchSizeMax), ReplyBytes: uint32(config.ApplicationReplySizeMax),
			PrefetchMax: uint32(config.PipelineMax), CheckpointMax: 1,
		},
	}, nil
}

func (machine *LedgerMachine) Capacities() replication.StateMachineCapacities {
	return machine.capacities
}

func (*LedgerMachine) Validate(input replication.ValidateInput) replication.ValidationResult {
	if input.Operation != LedgerIncrement {
		return replication.ValidationInvalidOperation
	}
	if len(input.Body) != 8 {
		return replication.ValidationInvalidBodySize
	}
	if binary.LittleEndian.Uint64(input.Body) == 0 {
		return replication.ValidationInvalidBody
	}
	return replication.ValidationOK
}

func (*LedgerMachine) PulseNeeded(uint64) bool { return false }

func (*LedgerMachine) StartPrefetch(replication.PrefetchInput, *replication.SMCompletion) (replication.StartResult[replication.PrefetchToken], error) {
	return replication.Ready(replication.PrefetchToken(0)), nil
}

func EncodeLedgerRequest(token uint64) [8]byte {
	var body [8]byte
	binary.LittleEndian.PutUint64(body[:], token)
	return body
}

func DecodeLedgerReply(body []byte) (token, value uint64, err error) {
	if len(body) != 16 {
		return 0, 0, replication.ErrStateMachine
	}
	token, value = binary.LittleEndian.Uint64(body[:8]), binary.LittleEndian.Uint64(body[8:])
	if token == 0 || value == 0 {
		return 0, 0, replication.ErrStateMachine
	}
	return token, value, nil
}

func ledgerDigest(previous protocol.Checksum, token, value uint64) protocol.Checksum {
	var body [32]byte
	copy(body[:16], previous[:])
	binary.LittleEndian.PutUint64(body[16:24], token)
	binary.LittleEndian.PutUint64(body[24:], value)
	return protocol.ChecksumBytes(body[:])
}

func (machine *LedgerMachine) Commit(input replication.CommitInput, _ replication.PrefetchToken, reply []byte) (int, error) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if machine.Validate(replication.ValidateInput{Operation: input.Operation, Body: input.Body}) != replication.ValidationOK || len(reply) < 16 || input.Op == 0 || uint64(input.Op) == ^uint64(0) || machine.state.Value == ^uint64(0) {
		return 0, replication.ErrStateMachine
	}
	if input.Op > machine.state.LastOp {
		machine.captureThrough(input.Op - 1)
	}
	token := binary.LittleEndian.Uint64(input.Body)
	machine.state.Value++
	machine.state.Digest = ledgerDigest(machine.state.Digest, token, machine.state.Value)
	machine.state.LastOp = max(machine.state.LastOp, input.Op)
	if machine.isBoundary(input.Op) {
		machine.freeze(input.Op)
	}
	machine.recent[machine.recentNext] = LedgerObservation{Op: input.Op, Token: token, Value: machine.state.Value, Digest: machine.state.Digest}
	machine.recentNext = (machine.recentNext + 1) % len(machine.recent)
	machine.recentCount = min(machine.recentCount+1, len(machine.recent))
	binary.LittleEndian.PutUint64(reply[:8], token)
	binary.LittleEndian.PutUint64(reply[8:16], machine.state.Value)
	return 16, nil
}

func (machine *LedgerMachine) StartCompact(input replication.CompactInput, _ *replication.SMCompletion) (replication.StartResult[replication.CompactResult], error) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if input.Op < machine.state.LastOp || uint64(input.Op) == ^uint64(0) {
		return replication.StartResult[replication.CompactResult]{}, ErrLedgerSnapshot
	}
	machine.captureThrough(input.Op)
	if machine.isBoundary(input.Op) {
		machine.freeze(input.Op)
	}
	return replication.Ready(replication.CompactResult{}), nil
}

func (machine *LedgerMachine) isBoundary(op protocol.Op) bool {
	return (uint64(op)+1)%machine.compactionOps == 0
}

// Internal control operations bypass Commit. Freeze crossed boundaries before
// the next application mutation, keeping the target and its compaction trigger.
func (machine *LedgerMachine) captureThrough(through protocol.Op) {
	if through <= machine.state.LastOp {
		return
	}
	completed := uint64(through) + 1
	boundary := completed - completed%machine.compactionOps
	if boundary > machine.compactionOps {
		previous := protocol.Op(boundary - machine.compactionOps - 1)
		if previous > machine.state.LastOp {
			machine.freeze(previous)
		}
	}
	if boundary != 0 {
		last := protocol.Op(boundary - 1)
		if last > machine.state.LastOp {
			machine.freeze(last)
		}
	}
	machine.state.LastOp = through
}

func (machine *LedgerMachine) freeze(op protocol.Op) {
	for _, frozen := range machine.frozen {
		if frozen.valid && frozen.state.LastOp == op {
			return
		}
	}
	state := machine.state
	state.LastOp = op
	machine.frozen[0] = machine.frozen[1]
	machine.frozen[1] = frozenLedger{valid: true, state: state}
}

func (machine *LedgerMachine) StartCheckpoint(input replication.CheckpointInput, _ *replication.SMCompletion) (replication.StartResult[replication.CheckpointManifest], error) {
	machine.mu.Lock()
	var snapshot LedgerSnapshot
	found := false
	for _, frozen := range machine.frozen {
		if frozen.valid && frozen.state.LastOp == input.Op {
			snapshot, found = frozen.state, true
			break
		}
	}
	machine.mu.Unlock()
	if !found || input.Op == 0 || input.Blocks == nil {
		return replication.StartResult[replication.CheckpointManifest]{}, ErrLedgerSnapshot
	}
	block, err := input.Blocks.Reserve(replication.BlockValue)
	if err != nil {
		return replication.StartResult[replication.CheckpointManifest]{}, err
	}
	body := encodeLedgerSnapshot(snapshot)
	reference, err := block.Write(ledgerMetadata(), body[:])
	if err != nil {
		return replication.StartResult[replication.CheckpointManifest]{}, err
	}
	return replication.Ready(replication.CheckpointManifest{Root: reference}), nil
}

func (machine *LedgerMachine) StartOpen(input replication.OpenCheckpointInput, _ *replication.SMCompletion) (replication.StartResult[replication.OpenResult], error) {
	requirement, err := (LedgerValidator{}).CheckpointRoot(input.State)
	if err != nil {
		return replication.StartResult[replication.OpenResult]{}, err
	}
	var state LedgerSnapshot
	if requirement.Reference.Address != 0 {
		var body [ledgerSnapshotBytes]byte
		result, err := input.Blocks.Read(requirement.Reference, requirement.Type, body[:])
		if err != nil {
			return replication.StartResult[replication.OpenResult]{}, err
		}
		if result.BodySize != ledgerSnapshotBytes || result.Snapshot != requirement.Snapshot {
			return replication.StartResult[replication.OpenResult]{}, ErrLedgerSnapshot
		}
		_, err = (LedgerValidator{}).ValidateBlock(replication.BlockValidationInput{Reference: requirement.Reference, NeededAtCheckpoint: input.State.PrepareOp(), Snapshot: result.Snapshot, Type: result.Type, Metadata: result.Metadata, Body: body[:]}, nil)
		if err != nil {
			return replication.StartResult[replication.OpenResult]{}, err
		}
		state, err = decodeLedgerSnapshot(body[:])
		if err != nil {
			return replication.StartResult[replication.OpenResult]{}, err
		}
	}
	machine.mu.Lock()
	machine.state = state
	machine.frozen = [2]frozenLedger{}
	machine.recent = [CommitHistoryMax]LedgerObservation{}
	machine.recentNext, machine.recentCount = 0, 0
	machine.mu.Unlock()
	return replication.Ready(replication.OpenResult{}), nil
}

func (machine *LedgerMachine) StartReset(*replication.SMCompletion) (replication.StartResult[replication.ResetResult], error) {
	machine.mu.Lock()
	machine.state = LedgerSnapshot{}
	machine.frozen = [2]frozenLedger{}
	machine.recent = [CommitHistoryMax]LedgerObservation{}
	machine.recentNext, machine.recentCount = 0, 0
	machine.mu.Unlock()
	return replication.Ready(replication.ResetResult{}), nil
}

func (*LedgerMachine) Close() error { return nil }

func (machine *LedgerMachine) Snapshot() LedgerSnapshot {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	return machine.state
}

// Observations is a bounded diagnostic window, not an exactly-once oracle.
func (machine *LedgerMachine) Observations() []LedgerObservation {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	result := make([]LedgerObservation, machine.recentCount)
	start := (machine.recentNext - machine.recentCount + len(machine.recent)) % len(machine.recent)
	for index := range result {
		result[index] = machine.recent[(start+index)%len(machine.recent)]
	}
	return result
}

func ledgerMetadata() [96]byte {
	var metadata [96]byte
	binary.LittleEndian.PutUint32(metadata[:4], 1)
	binary.LittleEndian.PutUint32(metadata[4:8], 1)
	binary.LittleEndian.PutUint32(metadata[8:12], ledgerSnapshotBytes)
	return metadata
}

func encodeLedgerSnapshot(state LedgerSnapshot) [ledgerSnapshotBytes]byte {
	var body [ledgerSnapshotBytes]byte
	binary.LittleEndian.PutUint64(body[:8], ledgerSnapshotFormat)
	binary.LittleEndian.PutUint64(body[8:16], uint64(state.LastOp))
	binary.LittleEndian.PutUint64(body[16:24], state.Value)
	copy(body[24:], state.Digest[:])
	return body
}

func decodeLedgerSnapshot(body []byte) (LedgerSnapshot, error) {
	if len(body) != ledgerSnapshotBytes || binary.LittleEndian.Uint64(body[:8]) != ledgerSnapshotFormat {
		return LedgerSnapshot{}, ErrLedgerSnapshot
	}
	state := LedgerSnapshot{LastOp: protocol.Op(binary.LittleEndian.Uint64(body[8:16])), Value: binary.LittleEndian.Uint64(body[16:24])}
	copy(state.Digest[:], body[24:])
	if state.LastOp == 0 || uint64(state.LastOp) == ^uint64(0) || state.Value > uint64(state.LastOp) || (state.Value == 0) != state.Digest.IsZero() {
		return LedgerSnapshot{}, ErrLedgerSnapshot
	}
	return state, nil
}

type LedgerValidator struct{}

func (LedgerValidator) CheckpointRoot(checkpoint replication.CheckpointState) (replication.BlockRequirement, error) {
	if checkpoint.ManifestBlockCount != 0 || checkpoint.OldestManifestAddress != 0 || checkpoint.NewestManifestAddress != 0 || !checkpoint.OldestManifestChecksum.IsZero() || !checkpoint.NewestManifestChecksum.IsZero() {
		return replication.BlockRequirement{}, ErrLedgerSnapshot
	}
	reference := replication.BlockReference{Address: checkpoint.SnapshotRootAddress, Checksum: checkpoint.SnapshotRootChecksum}
	if checkpoint.PrepareOp() == 0 {
		if reference != (replication.BlockReference{}) {
			return replication.BlockRequirement{}, ErrLedgerSnapshot
		}
		return replication.BlockRequirement{}, nil
	}
	if reference.Address == 0 || reference.Checksum.IsZero() || uint64(checkpoint.PrepareOp()) == ^uint64(0) {
		return replication.BlockRequirement{}, ErrLedgerSnapshot
	}
	return replication.BlockRequirement{Reference: reference, Type: replication.BlockValue, Snapshot: uint64(checkpoint.PrepareOp()), SnapshotExact: true, BodySize: ledgerSnapshotBytes}, nil
}

func (validator LedgerValidator) ResolveBlock(checkpoint replication.CheckpointState, address uint64) (replication.BlockRequirement, bool, error) {
	requirement, err := validator.CheckpointRoot(checkpoint)
	if err != nil {
		return replication.BlockRequirement{}, false, err
	}
	if address == 0 || address != requirement.Reference.Address {
		return replication.BlockRequirement{}, false, nil
	}
	return requirement, true, nil
}

func (LedgerValidator) ValidateBlock(input replication.BlockValidationInput, _ []replication.BlockRequirement) (int, error) {
	invalidReference := input.Reference.Address == 0 || input.Reference.Checksum.IsZero()
	invalidSchema := input.Type != replication.BlockValue || input.Metadata != ledgerMetadata()
	invalidSnapshot := input.Snapshot == 0 || input.Snapshot > uint64(input.NeededAtCheckpoint)
	if invalidReference || invalidSchema || invalidSnapshot {
		return 0, ErrLedgerSnapshot
	}
	state, err := decodeLedgerSnapshot(input.Body)
	if err != nil || uint64(state.LastOp) != input.Snapshot {
		return 0, ErrLedgerSnapshot
	}
	return 0, nil
}
