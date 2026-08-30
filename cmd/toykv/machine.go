package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
	"github.com/rs/zerolog"
)

const (
	operationPut protocol.Operation = protocol.OperationApplicationMin + iota
	operationGet
	operationDelete

	maxKeyBytes      = 256
	maxRequestBytes  = 64 << 10
	maxReplyBytes    = 64 << 10
	maxKeys          = 1024
	snapshotFormat   = "toykv-snapshot-v1"
	maxSnapshotBytes = 1<<20 + maxKeys*(((maxKeyBytes+2)/3)*4+((maxRequestBytes+2)/3)*4+64)

	resultOK byte = iota
	resultNotFound
	resultCapacity
)

var (
	errInvalidKVRequest = errors.New("toykv: invalid state-machine request")
	errInvalidSnapshot  = errors.New("toykv: invalid snapshot")
)

type snapshotEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type snapshotPayload struct {
	Format    string          `json:"format"`
	Operation uint64          `json:"operation"`
	Entries   []snapshotEntry `json:"entries"`
}

type snapshotDocument struct {
	Payload  snapshotPayload `json:"payload"`
	Checksum string          `json:"checksum"`
}

type frozenSnapshot struct {
	op      protocol.Op
	encoded []byte
}

type kvMachine struct {
	values          map[string][]byte
	directory       string
	group           protocol.GroupID
	memberCount     uint8
	messageSizeMax  uint32
	compactionOps   uint64
	lastObservedOp  protocol.Op
	pendingSnapshot protocol.Op
	frozenSnapshots [2]frozenSnapshot
	logger          zerolog.Logger
}

func newKVMachine(directory string, group protocol.GroupID, memberCount uint8, messageSizeMax uint32, compactionOps uint64, logger zerolog.Logger) *kvMachine {
	return &kvMachine{
		values: make(map[string][]byte, maxKeys), directory: directory, group: group,
		memberCount: memberCount, messageSizeMax: messageSizeMax,
		compactionOps: compactionOps, logger: logger,
	}
}

func (*kvMachine) Capacities() replication.StateMachineCapacities {
	return replication.StateMachineCapacities{
		RequestBytes: maxRequestBytes, ReplyBytes: maxReplyBytes,
		PrefetchMax: 15, CheckpointMax: 2,
	}
}

func (*kvMachine) Validate(input replication.ValidateInput) replication.ValidationResult {
	switch input.Operation {
	case operationPut:
		key, value, ok := decodePut(input.Body)
		if !ok || len(key) > maxKeyBytes || len(value) > maxRequestBytes {
			return replication.ValidationInvalidBody
		}
		return replication.ValidationOK
	case operationGet, operationDelete:
		key, ok := decodeKey(input.Body)
		if !ok || len(key) > maxKeyBytes {
			return replication.ValidationInvalidBody
		}
		return replication.ValidationOK
	default:
		return replication.ValidationInvalidOperation
	}
}

func (*kvMachine) PulseNeeded(uint64) bool {
	return false
}

func (*kvMachine) StartPrefetch(replication.PrefetchInput, *replication.SMCompletion) (replication.StartResult[replication.PrefetchToken], error) {
	return replication.Ready(replication.PrefetchToken(0)), nil
}

func (machine *kvMachine) Commit(input replication.CommitInput, _ replication.PrefetchToken, reply []byte) (int, error) {
	if input.Op <= machine.lastObservedOp {
		return 0, errInvalidSnapshot
	}
	if err := machine.captureUnobservedThrough(input.Op - 1); err != nil {
		return 0, err
	}
	replyLen := 0
	var err error
	switch input.Operation {
	case protocol.OperationPulse:
	case operationPut:
		keyBytes, value, ok := decodePut(input.Body)
		if !ok {
			return 0, errInvalidKVRequest
		}
		key := string(keyBytes)
		if _, found := machine.values[key]; !found && len(machine.values) == maxKeys {
			replyLen, err = encodeStatus(reply, resultCapacity)
			break
		}
		machine.values[key] = append(machine.values[key][:0], value...)
		replyLen, err = encodeStatus(reply, resultOK)
	case operationGet:
		key, ok := decodeKey(input.Body)
		if !ok {
			return 0, errInvalidKVRequest
		}
		value, found := machine.values[string(key)]
		if !found {
			replyLen, err = encodeStatus(reply, resultNotFound)
			break
		}
		if len(reply) < 1+len(value) {
			return 0, replication.ErrStateMachine
		}
		reply[0] = resultOK
		copy(reply[1:], value)
		replyLen = 1 + len(value)
	case operationDelete:
		key, ok := decodeKey(input.Body)
		if !ok {
			return 0, errInvalidKVRequest
		}
		name := string(key)
		if _, found := machine.values[name]; !found {
			replyLen, err = encodeStatus(reply, resultNotFound)
			break
		}
		delete(machine.values, name)
		replyLen, err = encodeStatus(reply, resultOK)
	default:
		return 0, errInvalidKVRequest
	}
	if err != nil {
		return 0, err
	}
	machine.lastObservedOp = input.Op
	if machine.isCompactionBoundary(input.Op) {
		if err := machine.captureSnapshot(input.Op); err != nil {
			return 0, err
		}
	}
	return replyLen, nil
}

func (machine *kvMachine) StartCompact(input replication.CompactInput, _ *replication.SMCompletion) (replication.StartResult[replication.CompactResult], error) {
	if machine.pendingSnapshot != 0 && input.Op > machine.pendingSnapshot {
		if err := machine.removeOtherSnapshots(machine.pendingSnapshot); err != nil {
			return replication.StartResult[replication.CompactResult]{}, err
		}
		machine.pendingSnapshot = 0
	}
	if input.Op < machine.lastObservedOp {
		return replication.StartResult[replication.CompactResult]{}, errInvalidSnapshot
	}
	if input.Op > machine.lastObservedOp {
		if err := machine.captureUnobservedThrough(input.Op); err != nil {
			return replication.StartResult[replication.CompactResult]{}, err
		}
	} else if machine.isCompactionBoundary(input.Op) {
		if err := machine.captureSnapshot(input.Op); err != nil {
			return replication.StartResult[replication.CompactResult]{}, err
		}
	}
	return replication.Ready(replication.CompactResult{}), nil
}

func (machine *kvMachine) StartCheckpoint(input replication.CheckpointInput, _ *replication.SMCompletion) (replication.StartResult[replication.CheckpointManifest], error) {
	encoded, found := machine.frozenSnapshot(input.Op)
	if !found {
		return replication.StartResult[replication.CheckpointManifest]{}, errInvalidSnapshot
	}
	if err := machine.writeSnapshot(input.Op, encoded); err != nil {
		return replication.StartResult[replication.CheckpointManifest]{}, err
	}
	machine.pendingSnapshot = input.Op
	event := machine.logger.Info()
	event.Uint64("op", uint64(input.Op))
	event.Msg("application checkpoint durable")
	return replication.Ready(replication.CheckpointManifest{}), nil
}

func (machine *kvMachine) StartOpen(input replication.OpenCheckpointInput, _ *replication.SMCompletion) (replication.StartResult[replication.OpenResult], error) {
	header, reason := protocol.DecodeHeader(input.State.Header[:], machine.group, machine.messageSizeMax, machine.memberCount)
	if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare {
		return replication.StartResult[replication.OpenResult]{}, errInvalidSnapshot
	}
	op := protocol.Op(binary.LittleEndian.Uint64(header.Fields[96:104]))
	if op == 0 {
		clear(machine.values)
	} else if err := machine.readSnapshot(op); err != nil {
		return replication.StartResult[replication.OpenResult]{}, err
	}
	if err := machine.removeOtherSnapshots(op); err != nil {
		return replication.StartResult[replication.OpenResult]{}, err
	}
	machine.pendingSnapshot = 0
	machine.lastObservedOp = op
	clear(machine.frozenSnapshots[:])
	return replication.Ready(replication.OpenResult{}), nil
}

func (machine *kvMachine) StartReset(*replication.SMCompletion) (replication.StartResult[replication.ResetResult], error) {
	clear(machine.values)
	machine.pendingSnapshot = 0
	machine.lastObservedOp = 0
	clear(machine.frozenSnapshots[:])
	return replication.Ready(replication.ResetResult{}), nil
}

func (*kvMachine) Close() error {
	return nil
}

func encodePut(key, value string) ([]byte, error) {
	if len(key) == 0 || len(key) > maxKeyBytes || len(value) > maxRequestBytes-6-len(key) {
		return nil, errInvalidKVRequest
	}
	body := make([]byte, 6+len(key)+len(value))
	binary.LittleEndian.PutUint16(body[:2], uint16(len(key)))
	binary.LittleEndian.PutUint32(body[2:6], uint32(len(value)))
	copy(body[6:], key)
	copy(body[6+len(key):], value)
	return body, nil
}

func encodeKey(key string) ([]byte, error) {
	if len(key) == 0 || len(key) > maxKeyBytes {
		return nil, errInvalidKVRequest
	}
	body := make([]byte, 2+len(key))
	binary.LittleEndian.PutUint16(body[:2], uint16(len(key)))
	copy(body[2:], key)
	return body, nil
}

func decodePut(body []byte) ([]byte, []byte, bool) {
	if len(body) < 6 {
		return nil, nil, false
	}
	keySize := int(binary.LittleEndian.Uint16(body[:2]))
	valueSize := int(binary.LittleEndian.Uint32(body[2:6]))
	if keySize == 0 || keySize > len(body)-6 || valueSize != len(body)-6-keySize {
		return nil, nil, false
	}
	return body[6 : 6+keySize], body[6+keySize:], true
}

func decodeKey(body []byte) ([]byte, bool) {
	if len(body) < 2 {
		return nil, false
	}
	size := int(binary.LittleEndian.Uint16(body[:2]))
	if size == 0 || size != len(body)-2 {
		return nil, false
	}
	return body[2:], true
}

func encodeStatus(reply []byte, status byte) (int, error) {
	if len(reply) == 0 {
		return 0, replication.ErrStateMachine
	}
	reply[0] = status
	return 1, nil
}

func (machine *kvMachine) snapshotPath(op protocol.Op) string {
	return filepath.Join(machine.directory, fmt.Sprintf("snapshot-%020d.json", uint64(op)))
}

func (machine *kvMachine) isCompactionBoundary(op protocol.Op) bool {
	return machine.compactionOps != 0 && uint64(op) != ^uint64(0) && (uint64(op)+1)%machine.compactionOps == 0
}

func (machine *kvMachine) captureUnobservedThrough(through protocol.Op) error {
	if through < machine.lastObservedOp || machine.compactionOps == 0 || uint64(through) == ^uint64(0) {
		return errInvalidSnapshot
	}
	if through == machine.lastObservedOp {
		return nil
	}
	completed := uint64(through) + 1
	lastCompleted := completed - completed%machine.compactionOps
	if lastCompleted > machine.compactionOps {
		previous := protocol.Op(lastCompleted - machine.compactionOps - 1)
		if previous > machine.lastObservedOp {
			if err := machine.captureSnapshot(previous); err != nil {
				return err
			}
		}
	}
	if lastCompleted != 0 {
		last := protocol.Op(lastCompleted - 1)
		if last > machine.lastObservedOp {
			if err := machine.captureSnapshot(last); err != nil {
				return err
			}
		}
	}
	machine.lastObservedOp = through
	return nil
}

func (machine *kvMachine) captureSnapshot(op protocol.Op) error {
	if !machine.isCompactionBoundary(op) {
		return errInvalidSnapshot
	}
	for _, snapshot := range machine.frozenSnapshots {
		if snapshot.op == op && len(snapshot.encoded) != 0 {
			return nil
		}
	}
	encoded, err := machine.encodeSnapshot(op)
	if err != nil {
		return err
	}
	machine.frozenSnapshots[0] = machine.frozenSnapshots[1]
	machine.frozenSnapshots[1] = frozenSnapshot{op: op, encoded: encoded}
	return nil
}

func (machine *kvMachine) frozenSnapshot(op protocol.Op) ([]byte, bool) {
	for _, snapshot := range machine.frozenSnapshots {
		if snapshot.op == op && len(snapshot.encoded) != 0 {
			return snapshot.encoded, true
		}
	}
	return nil, false
}

func (machine *kvMachine) encodeSnapshot(op protocol.Op) ([]byte, error) {
	keys := make([]string, 0, len(machine.values))
	for key := range machine.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	payload := snapshotPayload{
		Format: snapshotFormat, Operation: uint64(op),
		Entries: make([]snapshotEntry, 0, len(keys)),
	}
	for _, key := range keys {
		payload.Entries = append(payload.Entries, snapshotEntry{Key: []byte(key), Value: machine.values[key]})
	}
	checksumInput, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Join(errInvalidSnapshot, err)
	}
	checksum := protocol.ChecksumBytes(checksumInput)
	document := snapshotDocument{Payload: payload, Checksum: hex.EncodeToString(checksum[:])}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, errors.Join(errInvalidSnapshot, err)
	}
	if len(encoded) > maxSnapshotBytes {
		return nil, errInvalidSnapshot
	}
	return encoded, nil
}

func (machine *kvMachine) writeSnapshot(op protocol.Op, encoded []byte) error {
	if len(encoded) == 0 || len(encoded) > maxSnapshotBytes {
		return errInvalidSnapshot
	}
	return writeAtomic(machine.snapshotPath(op), encoded, 0o600)
}

func (machine *kvMachine) readSnapshot(op protocol.Op) error {
	path := machine.snapshotPath(op)
	file, err := os.Open(path)
	if err != nil {
		return errors.Join(errInvalidSnapshot, err)
	}
	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return errors.Join(errInvalidSnapshot, statErr)
	}
	if info.Size() <= 0 || info.Size() > maxSnapshotBytes {
		_ = file.Close()
		return errInvalidSnapshot
	}
	encoded, readErr := io.ReadAll(io.LimitReader(file, maxSnapshotBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return errors.Join(errInvalidSnapshot, readErr)
	}
	if closeErr != nil {
		return errors.Join(errInvalidSnapshot, closeErr)
	}
	if len(encoded) == 0 || len(encoded) > maxSnapshotBytes {
		return errInvalidSnapshot
	}

	var document snapshotDocument
	if err := json.Unmarshal(encoded, &document); err != nil {
		return errors.Join(errInvalidSnapshot, err)
	}
	canonical, err := json.Marshal(document)
	if err != nil || !bytes.Equal(encoded, canonical) {
		return errInvalidSnapshot
	}
	payload := document.Payload
	if payload.Format != snapshotFormat || protocol.Op(payload.Operation) != op || len(payload.Entries) > maxKeys {
		return errInvalidSnapshot
	}
	checksumInput, err := json.Marshal(payload)
	if err != nil {
		return errors.Join(errInvalidSnapshot, err)
	}
	var stored protocol.Checksum
	if len(document.Checksum) != hex.EncodedLen(len(stored)) {
		return errInvalidSnapshot
	}
	if _, err := hex.Decode(stored[:], []byte(document.Checksum)); err != nil || hex.EncodeToString(stored[:]) != document.Checksum {
		return errInvalidSnapshot
	}
	if protocol.ChecksumBytes(checksumInput) != stored {
		return errInvalidSnapshot
	}

	values := make(map[string][]byte, len(payload.Entries))
	previous := ""
	for index, entry := range payload.Entries {
		if len(entry.Key) == 0 || len(entry.Key) > maxKeyBytes || len(entry.Value) > maxRequestBytes {
			return errInvalidSnapshot
		}
		key := string(entry.Key)
		if index > 0 && key <= previous {
			return errInvalidSnapshot
		}
		values[key] = append([]byte(nil), entry.Value...)
		previous = key
	}
	machine.values = values
	return nil
}

func (machine *kvMachine) removeOtherSnapshots(keep protocol.Op) error {
	entries, err := os.ReadDir(machine.directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		number := strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), ".json")
		value, parseErr := strconv.ParseUint(number, 10, 64)
		if parseErr != nil || protocol.Op(value) == keep {
			continue
		}
		if err := os.Remove(filepath.Join(machine.directory, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return syncDirectory(machine.directory)
}
