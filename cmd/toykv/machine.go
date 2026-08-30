package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

	maxKeyBytes     = 256
	maxRequestBytes = 64 << 10
	maxReplyBytes   = 64 << 10
	maxKeys         = 1024

	resultOK byte = iota
	resultNotFound
	resultCapacity
)

var (
	errInvalidKVRequest = errors.New("toykv: invalid state-machine request")
	errInvalidSnapshot  = errors.New("toykv: invalid snapshot")
)

var snapshotMagic = [8]byte{'T', 'O', 'Y', 'K', 'V', '0', '0', '1'}

type kvMachine struct {
	values          map[string][]byte
	directory       string
	group           protocol.GroupID
	memberCount     uint8
	messageSizeMax  uint32
	pendingSnapshot protocol.Op
	logger          zerolog.Logger
}

func newKVMachine(directory string, group protocol.GroupID, memberCount uint8, messageSizeMax uint32, logger zerolog.Logger) *kvMachine {
	return &kvMachine{
		values: make(map[string][]byte, maxKeys), directory: directory, group: group,
		memberCount: memberCount, messageSizeMax: messageSizeMax, logger: logger,
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
	switch input.Operation {
	case protocol.OperationPulse:
		return 0, nil
	case operationPut:
		keyBytes, value, ok := decodePut(input.Body)
		if !ok {
			return 0, errInvalidKVRequest
		}
		key := string(keyBytes)
		if _, found := machine.values[key]; !found && len(machine.values) == maxKeys {
			return encodeStatus(reply, resultCapacity)
		}
		machine.values[key] = append(machine.values[key][:0], value...)
		return encodeStatus(reply, resultOK)
	case operationGet:
		key, ok := decodeKey(input.Body)
		if !ok {
			return 0, errInvalidKVRequest
		}
		value, found := machine.values[string(key)]
		if !found {
			return encodeStatus(reply, resultNotFound)
		}
		if len(reply) < 1+len(value) {
			return 0, replication.ErrStateMachine
		}
		reply[0] = resultOK
		copy(reply[1:], value)
		return 1 + len(value), nil
	case operationDelete:
		key, ok := decodeKey(input.Body)
		if !ok {
			return 0, errInvalidKVRequest
		}
		name := string(key)
		if _, found := machine.values[name]; !found {
			return encodeStatus(reply, resultNotFound)
		}
		delete(machine.values, name)
		return encodeStatus(reply, resultOK)
	default:
		return 0, errInvalidKVRequest
	}
}

func (machine *kvMachine) StartCompact(input replication.CompactInput, _ *replication.SMCompletion) (replication.StartResult[replication.CompactResult], error) {
	if machine.pendingSnapshot != 0 && input.Op > machine.pendingSnapshot {
		if err := machine.removeOtherSnapshots(machine.pendingSnapshot); err != nil {
			return replication.StartResult[replication.CompactResult]{}, err
		}
		machine.pendingSnapshot = 0
	}
	return replication.Ready(replication.CompactResult{}), nil
}

func (machine *kvMachine) StartCheckpoint(input replication.CheckpointInput, _ *replication.SMCompletion) (replication.StartResult[replication.CheckpointManifest], error) {
	if err := machine.writeSnapshot(input.Op); err != nil {
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
	return replication.Ready(replication.OpenResult{}), nil
}

func (machine *kvMachine) StartReset(*replication.SMCompletion) (replication.StartResult[replication.ResetResult], error) {
	clear(machine.values)
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
	return filepath.Join(machine.directory, fmt.Sprintf("snapshot-%020d.bin", uint64(op)))
}

func (machine *kvMachine) writeSnapshot(op protocol.Op) error {
	keys := make([]string, 0, len(machine.values))
	for key := range machine.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var encoded bytes.Buffer
	encoded.Grow(8 + 8 + 4 + len(keys)*16)
	_, _ = encoded.Write(snapshotMagic[:])
	_ = binary.Write(&encoded, binary.LittleEndian, uint64(op))
	_ = binary.Write(&encoded, binary.LittleEndian, uint32(len(keys)))
	for _, key := range keys {
		value := machine.values[key]
		_ = binary.Write(&encoded, binary.LittleEndian, uint16(len(key)))
		_ = binary.Write(&encoded, binary.LittleEndian, uint32(len(value)))
		_, _ = encoded.WriteString(key)
		_, _ = encoded.Write(value)
	}
	checksum := protocol.ChecksumBytes(encoded.Bytes())
	_, _ = encoded.Write(checksum[:])
	return writeAtomic(machine.snapshotPath(op), encoded.Bytes(), 0o600)
}

func (machine *kvMachine) readSnapshot(op protocol.Op) error {
	encoded, err := os.ReadFile(machine.snapshotPath(op))
	if err != nil {
		return errors.Join(errInvalidSnapshot, err)
	}
	if len(encoded) < 8+8+4+16 || !bytes.Equal(encoded[:8], snapshotMagic[:]) {
		return errInvalidSnapshot
	}
	payload := encoded[:len(encoded)-16]
	var stored protocol.Checksum
	copy(stored[:], encoded[len(encoded)-16:])
	if protocol.ChecksumBytes(payload) != stored || protocol.Op(binary.LittleEndian.Uint64(payload[8:16])) != op {
		return errInvalidSnapshot
	}
	count := int(binary.LittleEndian.Uint32(payload[16:20]))
	if count > maxKeys {
		return errInvalidSnapshot
	}
	values := make(map[string][]byte, count)
	offset := 20
	for range count {
		if len(payload)-offset < 6 {
			return errInvalidSnapshot
		}
		keySize := int(binary.LittleEndian.Uint16(payload[offset : offset+2]))
		valueSize := int(binary.LittleEndian.Uint32(payload[offset+2 : offset+6]))
		offset += 6
		if keySize == 0 || keySize > maxKeyBytes || valueSize > maxRequestBytes || keySize+valueSize > len(payload)-offset {
			return errInvalidSnapshot
		}
		key := string(payload[offset : offset+keySize])
		offset += keySize
		if _, duplicate := values[key]; duplicate {
			return errInvalidSnapshot
		}
		values[key] = append([]byte(nil), payload[offset:offset+valueSize]...)
		offset += valueSize
	}
	if offset != len(payload) {
		return errInvalidSnapshot
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
		if entry.IsDir() || !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".bin") {
			continue
		}
		number := strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), ".bin")
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
