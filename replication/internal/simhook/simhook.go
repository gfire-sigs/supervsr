package simhook

import (
	"context"
	"errors"
)

const (
	sectorSize      = 4 << 10
	superblockBytes = 8 << 10
	headerBytes     = 256
)

var (
	ErrControllerBound = errors.New("replication: I/O controller already bound")
	ErrIOHandle        = errors.New("replication: invalid storage request handle")
)

type IOKind uint8

const (
	IORead IOKind = iota + 1
	IOWrite
	IOSync
	IOResize
	IOWALAppend
	IOReplyWrite
	IOReplyRead
	IOSuperblockPersist
)

type IOPhase uint8

const (
	IOPhaseQueued IOPhase = iota + 1
	IOPhaseRead
	IOPhaseWrite
	IOPhaseSync
	IOPhaseResize
	IOPhaseCompletion
)

type IOHandle struct {
	Index      uint32
	Generation uint64
}

type IOEvent struct {
	Handle IOHandle
	Kind   IOKind
	Phase  IOPhase
	Offset uint64
	Size   uint64
}

type IOBackend interface {
	Capacity() int
	Used() int
	Pending(dst []IOEvent) int
	PendingCount() int
	Advance(handle IOHandle) error
	Drain() error
	Crash() error
}

// IOController is owned by the replica event-loop thread and binds to one
// storage engine incarnation, including after that engine closes.
type IOController struct {
	backend IOBackend
}

func NewIOController() *IOController { return &IOController{} }

func (controller *IOController) Bind(backend IOBackend) error {
	if controller.backend != nil {
		return ErrControllerBound
	}
	controller.backend = backend
	return nil
}

func (controller *IOController) Bound() bool {
	return controller.backend != nil
}

func (controller *IOController) Capacity() int {
	if controller.backend == nil {
		return 0
	}
	return controller.backend.Capacity()
}

func (controller *IOController) Used() int {
	if controller.backend == nil {
		return 0
	}
	return controller.backend.Used()
}

func (controller *IOController) Pending(dst []IOEvent) int {
	if controller.backend == nil {
		return 0
	}
	return controller.backend.Pending(dst)
}

func (controller *IOController) PendingCount() int {
	if controller.backend == nil {
		return 0
	}
	return controller.backend.PendingCount()
}

func (controller *IOController) Advance(handle IOHandle) error {
	if controller.backend == nil {
		return ErrIOHandle
	}
	return controller.backend.Advance(handle)
}

func (controller *IOController) Drain() error {
	if controller.backend == nil {
		return nil
	}
	return controller.backend.Drain()
}

func (controller *IOController) Crash() error {
	if controller.backend == nil {
		return ErrIOHandle
	}
	return controller.backend.Crash()
}

type IOOptions struct {
	SynchronousIO bool
	Controller    *IOController
}

type ioOptionsKey struct{}

func WithIOOptions(ctx context.Context, options IOOptions) context.Context {
	return context.WithValue(ctx, ioOptionsKey{}, options)
}

func IOOptionsFromContext(ctx context.Context) (IOOptions, bool) {
	if ctx == nil {
		return IOOptions{}, false
	}
	options, ok := ctx.Value(ioOptionsKey{}).(IOOptions)
	return options, ok
}

type WALLayout struct {
	HeaderBase    uint64
	HeaderSize    uint64
	PrepareBase   uint64
	PrepareStride uint64
	PrepareSize   uint64
	ReplyBase     uint64
	ReplyStride   uint64
	ReplySize     uint64
	BlockBase     uint64
}

func DeriveWALLayout(superblockCopies, journalSlots, messageSizeMax, clientsMax, blockSize uint64) (WALLayout, bool) {
	superblockSize, ok := checkedMul(superblockCopies, superblockBytes)
	if !ok {
		return WALLayout{}, false
	}
	headerSizeBytes, ok := checkedMul(journalSlots, headerBytes)
	if !ok {
		return WALLayout{}, false
	}
	headerSize, ok := checkedAlignUp(headerSizeBytes, sectorSize)
	if !ok {
		return WALLayout{}, false
	}
	prepareBase, ok := checkedAdd(superblockSize, headerSize)
	if !ok {
		return WALLayout{}, false
	}
	prepareStride, ok := checkedAlignUp(messageSizeMax, sectorSize)
	if !ok {
		return WALLayout{}, false
	}
	prepareSize, ok := checkedMul(journalSlots, prepareStride)
	if !ok {
		return WALLayout{}, false
	}
	replyBase, ok := checkedAdd(prepareBase, prepareSize)
	if !ok {
		return WALLayout{}, false
	}
	replySize, ok := checkedMul(clientsMax, prepareStride)
	if !ok {
		return WALLayout{}, false
	}
	end, ok := checkedAdd(replyBase, replySize)
	if !ok {
		return WALLayout{}, false
	}
	blockBase, ok := checkedAlignUp(end, blockSize)
	if !ok {
		return WALLayout{}, false
	}
	return WALLayout{
		HeaderBase:    superblockSize,
		HeaderSize:    headerSize,
		PrepareBase:   prepareBase,
		PrepareStride: prepareStride,
		PrepareSize:   prepareSize,
		ReplyBase:     replyBase,
		ReplyStride:   prepareStride,
		ReplySize:     replySize,
		BlockBase:     blockBase,
	}, true
}

func checkedAdd(left, right uint64) (uint64, bool) {
	if ^uint64(0)-left < right {
		return 0, false
	}
	return left + right, true
}

func checkedMul(left, right uint64) (uint64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	if ^uint64(0)/left < right {
		return 0, false
	}
	return left * right, true
}

func checkedAlignUp(value, alignment uint64) (uint64, bool) {
	if alignment == 0 {
		return 0, false
	}
	remainder := value % alignment
	if remainder == 0 {
		return value, true
	}
	return checkedAdd(value, alignment-remainder)
}
