package replication

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

// These faults are injected above FileStorage, not produced by kernel ENOSPC,
// a failing drive, or power loss. Reopen retains whatever bytes the OS accepted;
// a failed Sync never substitutes a successful durability barrier.
type injectedFileStorage struct {
	*FileStorage
	writeAtCall int
	syncAtCall  int
	partial     int
	capacity    uint64
	writes      int
	syncs       int
	injected    bool
}

var errInjectedFileIO = errors.New("injected file I/O failure")

func (storage *injectedFileStorage) WriteAt(buffer []byte, offset uint64) error {
	storage.writes++
	if storage.writes != storage.writeAtCall {
		return storage.FileStorage.WriteAt(buffer, offset)
	}
	storage.injected = true
	if storage.partial != 0 {
		if err := storage.FileStorage.WriteAt(buffer[:storage.partial], offset); err != nil {
			return err
		}
		return errors.Join(errInjectedFileIO, ErrShortIO)
	}
	return errors.Join(errInjectedFileIO, syscall.EIO)
}

func (storage *injectedFileStorage) Sync() error {
	storage.syncs++
	if storage.syncs == storage.syncAtCall {
		storage.injected = true
		return errors.Join(errInjectedFileIO, syscall.EIO)
	}
	return storage.FileStorage.Sync()
}

func (storage *injectedFileStorage) Resize(size uint64) error {
	if storage.capacity != 0 && size > storage.capacity {
		storage.injected = true
		return errors.Join(errInjectedFileIO, syscall.ENOSPC)
	}
	return storage.FileStorage.Resize(size)
}

type fileAppendFault struct {
	write   int
	sync    int
	partial int
}

type fileAppendRecovery struct {
	wal       *WAL
	report    WALRecoveryReport
	err       error
	prior     []byte
	attempted []byte
}

func TestFileWALInjectedAppendFaultRecovery(t *testing.T) {
	for _, test := range []struct {
		name     string
		fault    fileAppendFault
		wantHead protocol.Op
	}{
		{"prepare_write_error", fileAppendFault{write: 1}, 1},
		{"prepare_fsync_error_before_success", fileAppendFault{sync: 1}, 2},
		{"partial_redundant_header_write", fileAppendFault{write: 2, partial: 2*protocol.HeaderSize + protocol.HeaderSize/2}, 2},
		{"redundant_header_write_error", fileAppendFault{write: 2}, 2},
		{"header_fsync_error_before_success", fileAppendFault{sync: 2}, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := exerciseFileAppendFault(t, test.fault)
			if result.err != nil || result.report.HeadOp != test.wantHead || result.report.FaultySlots != 0 {
				t.Fatalf("recovery = (%+v, %v), want head %d without faults", result.report, result.err, test.wantHead)
			}
			assertFilePrepare(t, result.wal, 1, result.prior)
			if test.wantHead == 2 {
				assertFilePrepare(t, result.wal, 2, result.attempted)
			}
		})
	}
}

func TestFileWALInjectedPartialPrepareRefusesRecovery(t *testing.T) {
	result := exerciseFileAppendFault(t, fileAppendFault{write: 1, partial: protocol.HeaderSize / 2})
	if !errors.Is(result.err, ErrWALUncertainSolo) {
		t.Fatalf("recovery error = %v, want explicit solo uncertainty", result.err)
	}
	assertFilePrepare(t, result.wal, 1, result.prior)
}

func exerciseFileAppendFault(t *testing.T, fault fileAppendFault) fileAppendRecovery {
	t.Helper()
	path, config, file, validation := formattedFileWAL(t)
	storage := &injectedFileStorage{FileStorage: file}
	wal, report := recoverFileWAL(t, storage, config, validation)
	prior := connectedPrepareFrame(t, config, report.HeadHeader, 1)
	if err := wal.Append(prior, 0); err != nil {
		t.Fatal(err)
	}
	parent := decodeFilePrepare(t, config, prior)
	attempted := connectedPrepareFrame(t, config, parent, 2)
	layout := wal.Layout()
	offset, length := layout.PrepareBase+2*layout.PrepareStride, layout.PrepareStride
	if fault.write == 2 || fault.sync == 2 {
		offset, length = layout.HeaderBase, SectorSize
	}
	before := make([]byte, length)
	if err := file.ReadAt(before, offset); err != nil {
		t.Fatal(err)
	}
	storage.writes, storage.syncs = 0, 0
	storage.writeAtCall, storage.syncAtCall, storage.partial = fault.write, fault.sync, fault.partial
	engine := fileIOEngine(t, storage, 1, 1)
	completion := executeFileIO(t, engine, IOOperation{Kind: IOWALAppend, WAL: wal, Buffer: attempted})
	if !storage.injected || !errors.Is(completion.Err, errInjectedFileIO) {
		t.Fatalf("append completion = %v, injected = %t", completion.Err, storage.injected)
	}
	closeFileIO(t, engine)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openOwnedFile(t, path, false)
	physical := make([]byte, length)
	if err := reopened.ReadAt(physical, offset); err != nil {
		t.Fatal(err)
	}
	if fault.write != 0 {
		expected := bytes.Clone(before)
		if fault.partial != 0 {
			if fault.write == 1 {
				copy(expected[:fault.partial], attempted)
			} else {
				copy(expected[2*protocol.HeaderSize:fault.partial], attempted)
			}
		}
		if !bytes.Equal(physical, expected) {
			t.Fatal("reopened bytes do not preserve the injected physical write boundary")
		}
	}
	store, err := OpenSuperblockStore(reopened, validation)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := NewWAL(reopened, config, validation.Group, 1)
	if err != nil {
		t.Fatal(err)
	}
	report, err = recovered.Recover(store.Current().State.Checkpoint, 1, WALRecoveryView{})
	return fileAppendRecovery{wal: recovered, report: report, err: err, prior: prior, attempted: attempted}
}

func TestFileWALInjectedCapacityFailurePreservesPrefix(t *testing.T) {
	path, config, file, validation := formattedFileWAL(t)
	wal, report := recoverFileWAL(t, file, config, validation)
	prior := connectedPrepareFrame(t, config, report.HeadHeader, 1)
	if err := wal.Append(prior, 0); err != nil {
		t.Fatal(err)
	}
	size, err := file.Size()
	if err != nil {
		t.Fatal(err)
	}
	storage := &injectedFileStorage{FileStorage: file, capacity: size}
	engine := fileIOEngine(t, storage, 2, 1)
	completion := executeFileIO(t, engine, IOOperation{Kind: IOResize, Size: size + config.BlockSize})
	if !storage.injected || !errors.Is(completion.Err, syscall.ENOSPC) || !errors.Is(completion.Err, errInjectedFileIO) {
		t.Fatalf("extension completion = %v, injected = %t", completion.Err, storage.injected)
	}
	closeFileIO(t, engine)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openOwnedFile(t, path, false)
	assertFileSize(t, reopened, size)
	recovered, report := recoverFileWAL(t, reopened, config, validation)
	if report.HeadOp != 1 {
		t.Fatalf("recovered head = %d, want acknowledged op 1", report.HeadOp)
	}
	assertFilePrepare(t, recovered, 1, prior)
}

func TestFileWALSustainedIOKeepsResourcesBounded(t *testing.T) {
	const slots, rounds = 4, 8
	path, config, file, validation := formattedFileWAL(t)
	wal, report := recoverFileWAL(t, file, config, validation)
	frames := make([][]byte, config.JournalSlots/2)
	parent := report.HeadHeader
	for index := range frames {
		frames[index] = connectedPrepareFrame(t, config, parent, protocol.Op(index+1))
		parent = decodeFilePrepare(t, config, frames[index])
	}
	engine := fileIOEngine(t, file, slots, 2)
	var handles [slots]IOHandle
	var buffers [slots][SectorSize]byte
	for round := range rounds {
		for first := 0; first < len(frames); first += slots {
			for index := range slots {
				handle, err := engine.Submit(IOOperation{Kind: IOWALAppend, WAL: wal, Buffer: frames[first+index]})
				if err != nil {
					t.Fatalf("round %d append %d: %v", round, first+index+1, err)
				}
				handles[index] = handle
			}
			if _, err := engine.Submit(IOOperation{Kind: IOSync}); !errors.Is(err, ErrIOBackpressure) {
				t.Fatalf("exhausted pool accepted work: %v", err)
			}
			finishFileBatch(t, engine, handles[:])
			for index := range slots {
				offset := wal.Layout().PrepareBase + uint64(first+index+1)*wal.Layout().PrepareStride
				handle, err := engine.Submit(IOOperation{Kind: IORead, Offset: offset, Buffer: buffers[index][:]})
				if err != nil {
					t.Fatal(err)
				}
				handles[index] = handle
			}
			finishFileBatch(t, engine, handles[:])
			for index := range slots {
				frame := frames[first+index]
				if !bytes.Equal(buffers[index][:len(frame)], frame) || !allZeroBytes(buffers[index][len(frame):]) {
					t.Fatalf("round %d read op %d differs from its successful append", round, first+index+1)
				}
			}
		}
		assertFileSize(t, file, wal.Layout().BlockBase)
	}
	closeFileIO(t, engine)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openOwnedFile(t, path, false)
	recovered, report := recoverFileWAL(t, reopened, config, validation)
	if report.HeadOp != protocol.Op(len(frames)) {
		t.Fatalf("recovered head = %d, want %d", report.HeadOp, len(frames))
	}
	for index, frame := range frames {
		assertFilePrepare(t, recovered, protocol.Op(index+1), frame)
	}
	assertFileSize(t, reopened, recovered.Layout().BlockBase)
}

// Repeatedly rewriting one retained prepare measures bounded WAL append/read
// traffic; it is not a checkpoint, consensus throughput, or journal-wrap benchmark.
func BenchmarkFileWALAppendRead(b *testing.B) {
	path, config, file, validation := formattedFileWAL(b)
	wal, report := recoverFileWAL(b, file, config, validation)
	frame := connectedPrepareFrame(b, config, report.HeadHeader, 1)
	engine := fileIOEngine(b, file, 4, 1)
	buffer := make([]byte, wal.Layout().PrepareStride)
	appendOperation := IOOperation{Kind: IOWALAppend, WAL: wal, Buffer: frame}
	readOperation := IOOperation{Kind: IORead, Offset: wal.Layout().PrepareBase + wal.Layout().PrepareStride, Buffer: buffer}
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	b.ResetTimer()
	for range b.N {
		if completion := executeFileIO(b, engine, appendOperation); completion.Err != nil {
			b.Fatal(completion.Err)
		}
		if completion := executeFileIO(b, engine, readOperation); completion.Err != nil {
			b.Fatal(completion.Err)
		}
		if !bytes.Equal(buffer[:len(frame)], frame) || !allZeroBytes(buffer[len(frame):]) {
			b.Fatal("read differs from successful append")
		}
	}
	b.StopTimer()
	if !engine.Drained() || engine.Available() != 4 {
		b.Fatalf("pool not reclaimed: %d slots available", engine.Available())
	}
	closeFileIO(b, engine)
	assertFileSize(b, file, wal.Layout().BlockBase)
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
	reopened := openOwnedFile(b, path, false)
	recovered, report := recoverFileWAL(b, reopened, config, validation)
	if report.HeadOp != 1 {
		b.Fatalf("recovered head = %d, want 1", report.HeadOp)
	}
	assertFilePrepare(b, recovered, 1, frame)
	b.ReportMetric(float64(wal.Layout().BlockBase), "file-bytes")
	b.ReportMetric(4, "io-slots")
}

func formattedFileWAL(t testing.TB) (string, ClusterConfig, *FileStorage, SuperblockValidation) {
	t.Helper()
	config := compactTestClusterConfig()
	membership := Membership{Members: [MembersMax]protocol.MemberID{{1}}, ActiveCount: 1, LocalMember: protocol.MemberID{1}}
	path := filepath.Join(t.TempDir(), "replica.data")
	file := openOwnedFile(t, path, true)
	format := FormatConfig{Group: protocol.GroupID{1}, Membership: membership, Cluster: config, CurrentRelease: 1}
	if err := Format(t.Context(), format, FormatDependencies{Storage: file}); err != nil {
		t.Fatal(err)
	}
	validation := SuperblockValidation{Group: format.Group, Membership: membership, ConfigurationChecksum: config.Fingerprint(), Cluster: config}
	return path, config, file, validation
}

func openOwnedFile(t testing.TB, path string, create bool) *FileStorage {
	t.Helper()
	file, err := OpenFileStorage(path, create, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Errorf("close file: %v", err)
		}
	})
	return file
}

func recoverFileWAL(t testing.TB, storage Storage, config ClusterConfig, validation SuperblockValidation) (*WAL, WALRecoveryReport) {
	t.Helper()
	store, err := OpenSuperblockStore(storage, validation)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := NewWAL(storage, config, validation.Group, 1)
	if err != nil {
		t.Fatal(err)
	}
	report, err := wal.Recover(store.Current().State.Checkpoint, 0, WALRecoveryView{})
	if err != nil {
		t.Fatal(err)
	}
	return wal, report
}

func decodeFilePrepare(t testing.TB, config ClusterConfig, frame []byte) protocol.Header {
	t.Helper()
	header, _, reason := protocol.DecodeFrame(frame, protocol.GroupID{1}, uint32(config.MessageSizeMax), 1)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	return header
}

func assertFilePrepare(t testing.TB, wal *WAL, op protocol.Op, expected []byte) {
	t.Helper()
	frame, err := wal.ReadPrepare(op, make([]byte, wal.Layout().PrepareStride))
	if err != nil || !bytes.Equal(frame, expected) {
		t.Fatalf("recovered op %d differs from successful append: %v", op, err)
	}
}

func assertFileSize(t testing.TB, file *FileStorage, expected uint64) {
	t.Helper()
	size, err := file.Size()
	if err != nil || size != expected {
		t.Fatalf("file size = (%d, %v), want %d bytes", size, err, expected)
	}
}

func fileIOEngine(t testing.TB, storage Storage, slots, workers uint32) *IOEngine {
	t.Helper()
	engine, err := NewIOEngine(storage, slots, workers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeFileIO(t, engine) })
	return engine
}

func closeFileIO(t testing.TB, engine *IOEngine) {
	t.Helper()
	if err := engine.Close(context.Background()); err != nil {
		t.Errorf("close file IO workers: %v", err)
	}
}

func awaitFileIO(t testing.TB, engine *IOEngine) IOCompletion {
	t.Helper()
	var completion IOCompletion
	for !engine.Poll(&completion) {
		select {
		case <-engine.Ready():
		case <-t.Context().Done():
			t.Fatal("file IO completion canceled", t.Context().Err())
		}
	}
	return completion
}

func executeFileIO(t testing.TB, engine *IOEngine, operation IOOperation) IOCompletion {
	t.Helper()
	handle, err := engine.Submit(operation)
	if err != nil {
		t.Fatal(err)
	}
	completion := awaitFileIO(t, engine)
	if completion.Handle != handle {
		t.Fatalf("completion handle = %+v, want %+v", completion.Handle, handle)
	}
	return completion
}

func finishFileBatch(t testing.TB, engine *IOEngine, handles []IOHandle) {
	t.Helper()
	var seen uint64
	for range handles {
		completion := awaitFileIO(t, engine)
		if completion.Err != nil {
			t.Fatal(completion.Err)
		}
		found := false
		for index, handle := range handles {
			if completion.Handle == handle && seen&(1<<index) == 0 {
				seen |= 1 << index
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("unexpected or duplicate completion %+v", completion.Handle)
		}
	}
	if !engine.Drained() || engine.Available() != len(handles) {
		t.Fatalf("pool not reclaimed: %d slots available, want %d", engine.Available(), len(handles))
	}
}
