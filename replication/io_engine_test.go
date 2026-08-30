package replication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestIOEngineBackpressureCancellationAndCompletion(t *testing.T) {
	storage := newBlockingStorage()
	engine, err := NewIOEngine(storage, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIOEngine(t, engine)

	first, err := engine.Submit(IOOperation{Kind: IOWrite, Buffer: []byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-storage.started:
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}
	second, err := engine.Submit(IOOperation{Kind: IOWrite, Buffer: []byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	if !engine.Cancel(second) {
		t.Fatal("queued cancellation failed")
	}
	if _, err := engine.Submit(IOOperation{Kind: IOSync}); !errors.Is(err, ErrIOBackpressure) {
		t.Fatalf("third submit error = %v, want %v", err, ErrIOBackpressure)
	}
	close(storage.release)

	completions := collectIOCompletions(t, engine, 2)
	var firstSeen, secondSeen bool
	for _, completion := range completions {
		switch completion.Handle {
		case first:
			firstSeen = true
			if completion.Err != nil {
				t.Fatalf("first completion: %v", completion.Err)
			}
		case second:
			secondSeen = true
			if !errors.Is(completion.Err, ErrIOCanceled) {
				t.Fatalf("second completion error = %v, want %v", completion.Err, ErrIOCanceled)
			}
		default:
			t.Fatalf("unexpected completion: %+v", completion)
		}
	}
	if !firstSeen || !secondSeen {
		t.Fatalf("completion coverage: first=%t second=%t", firstSeen, secondSeen)
	}
	if engine.Available() != 2 {
		t.Fatalf("available = %d, want 2", engine.Available())
	}
}

func TestIOEngineRejectsStaleHandleAfterReuse(t *testing.T) {
	storage := newBlockingStorage()
	close(storage.release)
	engine, err := NewIOEngine(storage, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIOEngine(t, engine)

	old, err := engine.Submit(IOOperation{Kind: IOSync})
	if err != nil {
		t.Fatal(err)
	}
	_ = collectIOCompletions(t, engine, 1)
	current, err := engine.Submit(IOOperation{Kind: IOSync})
	if err != nil {
		t.Fatal(err)
	}
	if old.Index != current.Index || old.Generation == current.Generation {
		t.Fatalf("handles = old %+v current %+v", old, current)
	}
	if engine.Cancel(old) {
		t.Fatal("stale handle canceled current operation")
	}
	_ = collectIOCompletions(t, engine, 1)
}

func TestSynchronousIOEngineCompletesInline(t *testing.T) {
	storage := &crashStorage{}
	if err := storage.Resize(1); err != nil {
		t.Fatal(err)
	}
	engine, err := newIOEngine(storage, 1, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIOEngine(t, engine)
	handle, err := engine.Submit(IOOperation{Kind: IOWrite, Buffer: []byte{7}})
	if err != nil {
		t.Fatal(err)
	}
	var completion IOCompletion
	if !engine.Poll(&completion) || completion.Handle != handle || completion.Err != nil {
		t.Fatalf("completion=%+v", completion)
	}
	if storage.working[0] != 7 {
		t.Fatalf("stored byte=%d, want 7", storage.working[0])
	}
}

func collectIOCompletions(t testing.TB, engine *IOEngine, count int) []IOCompletion {
	t.Helper()
	completions := make([]IOCompletion, 0, count)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(completions) < count {
		select {
		case <-engine.Ready():
			for {
				var completion IOCompletion
				if !engine.Poll(&completion) {
					break
				}
				completions = append(completions, completion)
			}
		case <-deadline.C:
			t.Fatalf("received %d/%d storage completions", len(completions), count)
		}
	}
	return completions
}

func closeIOEngine(t testing.TB, engine *IOEngine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Close(ctx); err != nil {
		t.Errorf("close IO engine: %v", err)
	}
}

type blockingStorage struct {
	started chan struct{}
	release chan struct{}
	writes  atomic.Uint32
}

func newBlockingStorage() *blockingStorage {
	return &blockingStorage{started: make(chan struct{}), release: make(chan struct{})}
}

func (storage *blockingStorage) ReadAt(_ []byte, _ uint64) error { return nil }

func (storage *blockingStorage) WriteAt(_ []byte, _ uint64) error {
	if storage.writes.Add(1) == 1 {
		close(storage.started)
		<-storage.release
	}
	return nil
}

func (storage *blockingStorage) Sync() error           { return nil }
func (storage *blockingStorage) Resize(_ uint64) error { return nil }
func (storage *blockingStorage) Size() (uint64, error) { return 0, nil }
func (storage *blockingStorage) SyncParent() error     { return nil }
func (storage *blockingStorage) Close() error          { return nil }
