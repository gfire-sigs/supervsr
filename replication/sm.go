package replication

import (
	"errors"
	"sync/atomic"
)

var (
	ErrStateMachine         = errors.New("replication: state machine failure")
	ErrCompletionGeneration = errors.New("replication: stale completion generation")
)

type ValidationResult uint8

const (
	ValidationOK ValidationResult = iota
	ValidationInvalidOperation
	ValidationInvalidBody
	ValidationInvalidBodySize
)

type ValidateInput struct {
	Operation Operation
	Body      []byte
}

type StateMachineCapacities struct {
	RequestBytes  uint32
	ReplyBytes    uint32
	PrefetchMax   uint32
	CheckpointMax uint32
}

type PrefetchInput struct {
	Operation Operation
	Body      []byte
	Timestamp uint64
	Op        Op
	Release   Release
}

type CommitInput struct {
	Operation Operation
	Body      []byte
	Timestamp uint64
	Op        Op
	Release   Release
}

type CompactInput struct {
	Op        Op
	Timestamp uint64
}

type CheckpointInput struct {
	Op        Op
	Timestamp uint64
	Release   Release
	Blocks    *CheckpointBlockTransaction
}

type OpenCheckpointInput struct {
	State  CheckpointState
	Blocks *CheckpointBlockReader
}

type PrefetchToken uint64

type CompactResult struct {
	ReleasedBlocks uint32
}

type BlockReference struct {
	Checksum Checksum
	Address  uint64
}

type CheckpointManifest struct {
	Oldest     BlockReference
	Newest     BlockReference
	Root       BlockReference
	BlockCount uint32
}

type OpenResult struct{}
type ResetResult struct{}

type startState uint8

const (
	startReady startState = iota + 1
	startPending
)

type StartResult[T any] struct {
	state startState
	value T
}

func Ready[T any](value T) StartResult[T] {
	return StartResult[T]{state: startReady, value: value}
}

func Pending[T any]() StartResult[T] {
	return StartResult[T]{state: startPending}
}

func (result StartResult[T]) IsReady() bool {
	return result.state == startReady
}

func (result StartResult[T]) IsPending() bool {
	return result.state == startPending
}

func (result StartResult[T]) Value() (T, bool) {
	return result.value, result.IsReady()
}

type SMCompletionKind uint8

const (
	SMCompletionPrefetch SMCompletionKind = iota + 1
	SMCompletionCompact
	SMCompletionCheckpoint
	SMCompletionOpen
	SMCompletionReset
)

type SMResult struct {
	Kind     SMCompletionKind
	Prefetch PrefetchToken
	Compact  CompactResult
	Manifest CheckpointManifest
	Open     OpenResult
	Reset    ResetResult
	Err      error
}

type smCompletionSink interface {
	enqueueSMCompletion(*SMCompletion) error
}

type SMCompletion struct {
	state      atomic.Uint32
	generation uint64
	sink       smCompletionSink
	result     SMResult
}

func (completion *SMCompletion) Complete(result SMResult) error {
	if !completion.state.CompareAndSwap(1, 2) {
		return ErrCompletionGeneration
	}
	completion.result = result
	if err := completion.sink.enqueueSMCompletion(completion); err != nil {
		completion.state.Store(1)
		return err
	}
	return nil
}

func (completion *SMCompletion) prepare(generation uint64, sink smCompletionSink) {
	completion.generation = generation
	completion.sink = sink
	completion.result = SMResult{}
	completion.state.Store(1)
}

func (completion *SMCompletion) release(generation uint64) bool {
	if completion.generation != generation || !completion.state.CompareAndSwap(1, 0) {
		return false
	}
	completion.result = SMResult{}
	completion.sink = nil
	return true
}

func (completion *SMCompletion) take(generation uint64) (SMResult, bool) {
	if completion.generation != generation || !completion.state.CompareAndSwap(2, 0) {
		return SMResult{}, false
	}
	result := completion.result
	completion.result = SMResult{}
	completion.sink = nil
	return result, true
}

type StateMachine interface {
	Capacities() StateMachineCapacities
	Validate(input ValidateInput) ValidationResult
	PulseNeeded(timestamp uint64) bool
	StartPrefetch(input PrefetchInput, completion *SMCompletion) (StartResult[PrefetchToken], error)
	Commit(input CommitInput, token PrefetchToken, reply []byte) (replyLen int, err error)
	StartCompact(input CompactInput, completion *SMCompletion) (StartResult[CompactResult], error)
	StartCheckpoint(input CheckpointInput, completion *SMCompletion) (StartResult[CheckpointManifest], error)
	StartOpen(input OpenCheckpointInput, completion *SMCompletion) (StartResult[OpenResult], error)
	StartReset(completion *SMCompletion) (StartResult[ResetResult], error)
	Close() error
}
