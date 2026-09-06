package replication

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

// This models volatile cache loss, not physical filesystem power-loss behavior.
type phaseStorage struct {
	*scriptedStorage
	durable []byte
}

func newPhaseStorage(t testing.TB, size uint64) *phaseStorage {
	t.Helper()
	return &phaseStorage{scriptedStorage: newScriptedStorage(t, size), durable: make([]byte, size)}
}

func (storage *phaseStorage) Sync() error {
	if err := storage.scriptedStorage.Sync(); err != nil {
		return err
	}
	storage.durable = append(storage.durable[:0], storage.bytes...)
	return nil
}

func controlledEngine(t testing.TB, storage Storage, count uint32) (*IOEngine, *IOController) {
	t.Helper()
	controller := NewIOController()
	engine, err := newIOEngine(storage, count, 0, false, controller)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeIOEngine(t, engine) })
	return engine, controller
}

func submitControlled(t testing.TB, engine *IOEngine, operation IOOperation) IOHandle {
	t.Helper()
	handle, err := engine.Submit(operation)
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func advancePhase(t testing.TB, controller *IOController, handle IOHandle, phase IOPhase) {
	t.Helper()
	events := make([]IOEvent, controller.PendingCount())
	n := controller.Pending(events)
	found := false
	for _, event := range events[:n] {
		if event.Handle != handle {
			continue
		}
		if event.Phase != phase {
			t.Fatalf("handle %+v phase = %d, want %d", handle, event.Phase, phase)
		}
		found = true
	}
	if !found {
		t.Fatalf("handle %+v not advanceable: %+v", handle, events[:n])
	}
	if err := controller.Advance(handle); err != nil {
		t.Fatal(err)
	}
}

func pollControlled(t testing.TB, engine *IOEngine, handle IOHandle, want error) {
	t.Helper()
	var completion IOCompletion
	if !engine.Poll(&completion) {
		t.Fatal("missing published completion")
	}
	if completion.Handle != handle || !errors.Is(completion.Err, want) {
		t.Fatalf("completion handle=%+v err=%v, want handle=%+v err=%v", completion.Handle, completion.Err, handle, want)
	}
}

func TestIOControllerWALCrashBoundaries(t *testing.T) {
	for physicalSteps := 0; physicalSteps <= 4; physicalSteps++ {
		t.Run(fmt.Sprintf("after_%d_effects", physicalSteps), func(t *testing.T) {
			config := compactTestClusterConfig()
			layout, _ := DeriveWALLayout(config)
			storage := newPhaseStorage(t, layout.BlockBase)
			wal, err := NewWAL(storage, config, protocol.GroupID{1}, 3)
			if err != nil {
				t.Fatal(err)
			}
			engine, controller := controlledEngine(t, storage, 1)
			frame := validPrepareFrame(t, config, 1)
			handle := submitControlled(t, engine, IOOperation{Kind: IOWALAppend, WAL: wal, Buffer: frame})
			advancePhase(t, controller, handle, IOPhaseQueued)
			phases := [...]IOPhase{IOPhaseWrite, IOPhaseSync, IOPhaseWrite, IOPhaseSync}
			for _, phase := range phases[:physicalSteps] {
				advancePhase(t, controller, handle, phase)
			}
			var completion IOCompletion
			if engine.Poll(&completion) {
				t.Fatal("completion escaped before delivery phase")
			}
			if err := controller.Crash(); err != nil {
				t.Fatal(err)
			}
			if err := controller.Drain(); err != nil {
				t.Fatal(err)
			}
			pollControlled(t, engine, handle, ErrIOCanceled)
			copy(storage.bytes, storage.durable)
			prepareOffset := layout.PrepareBase + layout.PrepareStride
			headerOffset := layout.HeaderBase + protocol.HeaderSize
			bodyPresent := bytes.Equal(storage.bytes[prepareOffset:prepareOffset+uint64(len(frame))], frame)
			headerPresent := bytes.Equal(storage.bytes[headerOffset:headerOffset+protocol.HeaderSize], frame[:protocol.HeaderSize])
			if bodyPresent != (physicalSteps >= 2) || headerPresent != (physicalSteps == 4) {
				t.Fatalf("crash after %d effects: durable body=%t header=%t", physicalSteps, bodyPresent, headerPresent)
			}
			if len(storage.operations) != physicalSteps {
				t.Fatalf("crash cleanup performed I/O: %v", storage.operations)
			}
		})
	}
}

func TestIOControllerReplySyncAndCompletionAreSeparate(t *testing.T) {
	for _, syncReply := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync_%t", syncReply), func(t *testing.T) {
			config := compactTestClusterConfig()
			layout, _ := DeriveWALLayout(config)
			storage := newPhaseStorage(t, layout.BlockBase)
			store, err := NewReplyStore(storage, config, protocol.GroupID{9}, 3)
			if err != nil {
				t.Fatal(err)
			}
			engine, controller := controlledEngine(t, storage, 1)
			frame, header := makeReplyFrame(t, protocol.ClientID{1}, 1, []byte("reply"))
			handle := submitControlled(t, engine, IOOperation{Kind: IOReplyWrite, ReplyStore: store, Buffer: frame})
			advancePhase(t, controller, handle, IOPhaseQueued)
			advancePhase(t, controller, handle, IOPhaseWrite)
			if syncReply {
				advancePhase(t, controller, handle, IOPhaseSync)
			}
			var completion IOCompletion
			if engine.Poll(&completion) {
				t.Fatal("reply completion delivered without scheduler permission")
			}
			if err := controller.Crash(); err != nil {
				t.Fatal(err)
			}
			copy(storage.bytes, storage.durable)
			got, err := store.Read(0, header, make([]byte, layout.ReplyStride))
			if syncReply {
				if err != nil || !bytes.Equal(got, frame) {
					t.Fatalf("synced reply recovery: frame=%x err=%v", got, err)
				}
			} else if err == nil {
				t.Fatal("unsynced reply survived cache loss")
			}
		})
	}
}

func TestIOControllerCancellationReorderingAndStaleHandles(t *testing.T) {
	storage := newPhaseStorage(t, 32)
	engine, controller := controlledEngine(t, storage, 2)
	first := submitControlled(t, engine, IOOperation{Kind: IOWrite, Buffer: []byte{1}})
	second := submitControlled(t, engine, IOOperation{Kind: IOWrite, Offset: 1, Buffer: []byte{2}})
	if !engine.Cancel(first) {
		t.Fatal("queued request not canceled")
	}
	advancePhase(t, controller, second, IOPhaseQueued)
	if engine.Cancel(second) {
		t.Fatal("running request canceled")
	}
	advancePhase(t, controller, second, IOPhaseWrite)
	advancePhase(t, controller, second, IOPhaseCompletion)
	pollControlled(t, engine, second, nil)
	reused := submitControlled(t, engine, IOOperation{Kind: IOWrite, Offset: 2, Buffer: []byte{3}})
	if engine.Cancel(second) || !errors.Is(controller.Advance(second), ErrIOHandle) {
		t.Fatal("stale handle affected reused slot")
	}
	advancePhase(t, controller, first, IOPhaseCompletion)
	pollControlled(t, engine, first, ErrIOCanceled)
	if err := controller.Drain(); err != nil {
		t.Fatal(err)
	}
	pollControlled(t, engine, reused, nil)
	if !bytes.Equal(storage.bytes[:3], []byte{0, 2, 3}) {
		t.Fatalf("canceled or stale effect: %v", storage.bytes[:3])
	}
}

func TestIOControllerStoreScratchWaitsButCompletionDoesNot(t *testing.T) {
	config := compactTestClusterConfig()
	layout, _ := DeriveWALLayout(config)
	storage := newPhaseStorage(t, layout.BlockBase)
	store, err := NewReplyStore(storage, config, protocol.GroupID{9}, 3)
	if err != nil {
		t.Fatal(err)
	}
	engine, controller := controlledEngine(t, storage, 2)
	firstFrame, firstHeader := makeReplyFrame(t, protocol.ClientID{1}, 1, []byte("first"))
	secondFrame, secondHeader := makeReplyFrame(t, protocol.ClientID{2}, 2, []byte("second"))
	first := submitControlled(t, engine, IOOperation{Kind: IOReplyWrite, ReplyStore: store, Buffer: firstFrame})
	second := submitControlled(t, engine, IOOperation{Kind: IOReplyWrite, ReplyStore: store, Offset: 1, Buffer: secondFrame})
	advancePhase(t, controller, first, IOPhaseQueued)
	if !errors.Is(controller.Advance(second), ErrIOBackpressure) {
		t.Fatal("second writer reused scratch before first write")
	}
	advancePhase(t, controller, first, IOPhaseWrite)
	advancePhase(t, controller, first, IOPhaseSync)
	for _, phase := range []IOPhase{IOPhaseQueued, IOPhaseWrite, IOPhaseSync, IOPhaseCompletion} {
		advancePhase(t, controller, second, phase)
	}
	pollControlled(t, engine, second, nil)
	advancePhase(t, controller, first, IOPhaseCompletion)
	pollControlled(t, engine, first, nil)
	for slot, expected := range []struct {
		frame  []byte
		header protocol.Header
	}{{firstFrame, firstHeader}, {secondFrame, secondHeader}} {
		got, err := store.Read(uint32(slot), expected.header, make([]byte, layout.ReplyStride))
		if err != nil || !bytes.Equal(got, expected.frame) {
			t.Fatalf("slot %d frame corrupted: %x, %v", slot, got, err)
		}
	}
}

func TestIOControllerFailedWALPhaseDoesNotPublishDurability(t *testing.T) {
	for failure := 1; failure <= 4; failure++ {
		t.Run(fmt.Sprintf("effect_%d", failure), func(t *testing.T) {
			config := compactTestClusterConfig()
			layout, _ := DeriveWALLayout(config)
			storage := newPhaseStorage(t, layout.BlockBase)
			storage.failAt = failure
			wal, err := NewWAL(storage, config, protocol.GroupID{1}, 3)
			if err != nil {
				t.Fatal(err)
			}
			engine, controller := controlledEngine(t, storage, 1)
			handle := submitControlled(t, engine, IOOperation{Kind: IOWALAppend, WAL: wal, Buffer: validPrepareFrame(t, config, 1)})
			if err := controller.Drain(); err != nil {
				t.Fatal(err)
			}
			pollControlled(t, engine, handle, ErrStorage)
			if !wal.slots[1].Dirty || wal.slots[1].Inhabited {
				t.Fatal("failed append published durable WAL slot")
			}
			if len(storage.operations) != failure {
				t.Fatalf("effects continued after failure: %v", storage.operations)
			}
		})
	}
}

func TestIOControllerOldBlockWriteOverlapsCheckpointPersist(t *testing.T) {
	validation, initial := validSuperblockFixture(t)
	validation.Cluster = compactTestClusterConfig()
	validation.ConfigurationChecksum = validation.Cluster.Fingerprint()
	initial.ConfigurationChecksum = validation.ConfigurationChecksum
	blockOffset, ok := validation.Cluster.BlockBase()
	if !ok {
		t.Fatal("invalid block layout")
	}
	initial.State.Checkpoint.LogicalStorageSize = blockOffset
	storage := newPhaseStorage(t, blockOffset+SectorSize)
	buffer, err := NewAlignedBuffer(SuperblockBytes, SectorSize)
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.Encode(buffer, 0, validation); err != nil {
		t.Fatal(err)
	}
	store := &SuperblockStore{storage: storage, validation: validation, current: SuperblockCandidate{Superblock: initial}, buffer: buffer}
	next := initial
	next.Sequence++
	next.ParentChecksum = initial.Checksum
	engine, controller := controlledEngine(t, storage, 2)
	old := submitControlled(t, engine, IOOperation{Kind: IOWrite, Offset: blockOffset, Buffer: bytes.Repeat([]byte{7}, SectorSize)})
	advancePhase(t, controller, old, IOPhaseQueued)
	checkpoint := submitControlled(t, engine, IOOperation{Kind: IOSuperblockPersist, SuperblockStore: store, Superblock: next})
	advancePhase(t, controller, checkpoint, IOPhaseQueued)
	copies, _ := superblockWriteCopies(validation.Cluster.SuperblockCopies)
	for range copies {
		advancePhase(t, controller, checkpoint, IOPhaseWrite)
		advancePhase(t, controller, checkpoint, IOPhaseSync)
	}
	advancePhase(t, controller, checkpoint, IOPhaseCompletion)
	pollControlled(t, engine, checkpoint, nil)
	if store.Current().Sequence != next.Sequence {
		t.Fatal("checkpoint blocked behind unrelated old block write")
	}
	if !allZeroBytes(storage.bytes[blockOffset:]) {
		t.Fatal("held block write ran during checkpoint work")
	}
	advancePhase(t, controller, old, IOPhaseWrite)
	advancePhase(t, controller, old, IOPhaseCompletion)
	pollControlled(t, engine, old, nil)
	if !bytes.Equal(storage.bytes[blockOffset:], bytes.Repeat([]byte{7}, SectorSize)) {
		t.Fatal("old block write was excluded instead of executed")
	}
	current := store.Current()
	current.Sequence++
	current.ParentChecksum = store.Current().Checksum
	followup := submitControlled(t, engine, IOOperation{Kind: IOSuperblockPersist, SuperblockStore: store, Superblock: current})
	if err := controller.Drain(); err != nil {
		t.Fatal(err)
	}
	pollControlled(t, engine, followup, nil)
}

func TestIOControllerReadAndResizeAwaitPhysicalPhase(t *testing.T) {
	storage := newPhaseStorage(t, 8)
	copy(storage.bytes[2:], []byte{4, 5})
	engine, controller := controlledEngine(t, storage, 1)
	buffer := []byte{9, 9}
	read := submitControlled(t, engine, IOOperation{Kind: IORead, Offset: 2, Buffer: buffer})
	advancePhase(t, controller, read, IOPhaseQueued)
	if !bytes.Equal(buffer, []byte{9, 9}) {
		t.Fatal("read ran in start phase")
	}
	advancePhase(t, controller, read, IOPhaseRead)
	if !bytes.Equal(buffer, []byte{4, 5}) {
		t.Fatalf("read data = %v", buffer)
	}
	var completion IOCompletion
	if engine.Poll(&completion) {
		t.Fatal("read published before completion phase")
	}
	advancePhase(t, controller, read, IOPhaseCompletion)
	pollControlled(t, engine, read, nil)
	resize := submitControlled(t, engine, IOOperation{Kind: IOResize, Size: 16})
	advancePhase(t, controller, resize, IOPhaseQueued)
	if size, _ := storage.Size(); size != 8 {
		t.Fatalf("resize ran in start phase: %d", size)
	}
	advancePhase(t, controller, resize, IOPhaseResize)
	if size, _ := storage.Size(); size != 16 {
		t.Fatalf("resized size = %d", size)
	}
	advancePhase(t, controller, resize, IOPhaseCompletion)
	pollControlled(t, engine, resize, nil)
}

func TestIOControllerReplyFailureIsTerminal(t *testing.T) {
	for failure := 1; failure <= 2; failure++ {
		t.Run(fmt.Sprintf("effect_%d", failure), func(t *testing.T) {
			config := compactTestClusterConfig()
			layout, _ := DeriveWALLayout(config)
			storage := newPhaseStorage(t, layout.BlockBase)
			storage.failAt = failure
			store, err := NewReplyStore(storage, config, protocol.GroupID{9}, 3)
			if err != nil {
				t.Fatal(err)
			}
			engine, controller := controlledEngine(t, storage, 1)
			frame, header := makeReplyFrame(t, protocol.ClientID{1}, 1, []byte("reply"))
			handle := submitControlled(t, engine, IOOperation{Kind: IOReplyWrite, ReplyStore: store, Buffer: frame})
			if err := controller.Drain(); err != nil {
				t.Fatal(err)
			}
			pollControlled(t, engine, handle, ErrStorage)
			copy(storage.bytes, storage.durable)
			if _, err := store.Read(0, header, make([]byte, layout.ReplyStride)); err == nil {
				t.Fatal("failed reply became durable")
			}
			if len(storage.operations) != failure {
				t.Fatalf("effects continued after failure: %v", storage.operations)
			}
		})
	}
}

func TestIOControllerCannotRebindAcrossIncarnations(t *testing.T) {
	storage := newPhaseStorage(t, 8)
	engine, controller := controlledEngine(t, storage, 1)
	if controller.Capacity() != 1 {
		t.Fatalf("capacity = %d", controller.Capacity())
	}
	if err := controller.Crash(); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Submit(IOOperation{Kind: IOSync}); !errors.Is(err, ErrStorageClosed) {
		t.Fatalf("submit after crash: %v", err)
	}
	if _, err := newIOEngine(storage, 1, 0, false, controller); err == nil {
		t.Fatal("controller rebound to new incarnation")
	}
	if _, err := newIOEngine(storage, 1, 0, true, NewIOController()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("mixed synchronous/controlled mode: %v", err)
	}
}
