package replication

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

const (
	SuperblockStateSize      = 2048
	SuperblockReservationEnd = 4096
	SuperblockHeadersOffset  = 4096
	SuperblockHeadersMax     = 16
)

var (
	ErrInvalidSuperblock                  = errors.New("replication: invalid superblock")
	ErrSuperblockFork                     = errors.New("replication: superblock fork")
	ErrSuperblockQuorum                   = errors.New("replication: no superblock open quorum")
	ErrSuperblockInitializationIncomplete = errors.New("replication: incomplete initial superblock quorum")
)

type DurableReplicaState struct {
	Checkpoint   CheckpointState
	LocalMember  protocol.MemberID
	Members      [MembersMax]protocol.MemberID
	CommitMax    protocol.Op
	SyncMin      protocol.Op
	SyncMax      protocol.Op
	LogView      protocol.View
	View         protocol.View
	ActiveCount  uint8
	StandbyCount uint8
}

type Superblock struct {
	Checksum              protocol.Checksum
	FormatVersion         uint16
	Release               protocol.Release
	Sequence              uint64
	Group                 protocol.GroupID
	ParentChecksum        protocol.Checksum
	State                 DurableReplicaState
	ViewHeaderCount       uint32
	ConfigurationChecksum protocol.Checksum
	ProtocolVersion       uint16
	ViewHeaders           [SuperblockHeadersMax][protocol.HeaderSize]byte
}

type SuperblockValidation struct {
	Group                 protocol.GroupID
	Membership            Membership
	ConfigurationChecksum protocol.Checksum
	Cluster               ClusterConfig
}

type SuperblockCandidate struct {
	Superblock       Superblock
	PhysicalIndex    uint16
	MisdirectedIndex bool
}

type superblockLogicalValue struct {
	candidate SuperblockCandidate
	count     uint8
}

func (superblock *Superblock) Encode(destination []byte, physicalIndex uint16, validation SuperblockValidation) error {
	if len(destination) != SuperblockBytes || physicalIndex >= uint16(validation.Cluster.SuperblockCopies) {
		return ErrInvalidSuperblock
	}
	if err := superblock.Validate(validation); err != nil {
		return err
	}
	clear(destination)
	binary.LittleEndian.PutUint16(destination[32:34], physicalIndex)
	binary.LittleEndian.PutUint16(destination[34:36], superblock.FormatVersion)
	binary.LittleEndian.PutUint32(destination[36:40], uint32(superblock.Release))
	binary.LittleEndian.PutUint64(destination[40:48], superblock.Sequence)
	copy(destination[48:64], superblock.Group[:])
	copy(destination[64:80], superblock.ParentChecksum[:])
	if err := superblock.State.encode(destination[96:2144]); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(destination[2152:2156], superblock.ViewHeaderCount)
	copy(destination[2156:2172], superblock.ConfigurationChecksum[:])
	binary.LittleEndian.PutUint16(destination[2172:2174], superblock.ProtocolVersion)
	for index := range int(superblock.ViewHeaderCount) {
		start := SuperblockHeadersOffset + index*protocol.HeaderSize
		copy(destination[start:start+protocol.HeaderSize], superblock.ViewHeaders[index][:])
	}
	superblock.Checksum = protocol.ChecksumBytes(destination[34:])
	copy(destination[:16], superblock.Checksum[:])
	return nil
}

func DecodeSuperblock(source []byte, physicalIndex uint16, validation SuperblockValidation) (SuperblockCandidate, error) {
	if len(source) != SuperblockBytes || !allZeroBytes(source[16:32]) {
		return SuperblockCandidate{}, ErrInvalidSuperblock
	}
	var checksum protocol.Checksum
	copy(checksum[:], source[:16])
	if protocol.ChecksumBytes(source[34:]) != checksum {
		return SuperblockCandidate{}, ErrInvalidSuperblock
	}
	storedIndex := binary.LittleEndian.Uint16(source[32:34])
	var superblock Superblock
	superblock.Checksum = checksum
	superblock.FormatVersion = binary.LittleEndian.Uint16(source[34:36])
	superblock.Release = protocol.Release(binary.LittleEndian.Uint32(source[36:40]))
	superblock.Sequence = binary.LittleEndian.Uint64(source[40:48])
	copy(superblock.Group[:], source[48:64])
	copy(superblock.ParentChecksum[:], source[64:80])
	state, err := decodeDurableReplicaState(source[96:2144], validation)
	if err != nil {
		return SuperblockCandidate{}, err
	}
	superblock.State = state
	if !allZeroBytes(source[80:96]) || binary.LittleEndian.Uint64(source[2144:2152]) != 0 || !allZeroBytes(source[2174:4096]) {
		return SuperblockCandidate{}, ErrInvalidSuperblock
	}
	superblock.ViewHeaderCount = binary.LittleEndian.Uint32(source[2152:2156])
	copy(superblock.ConfigurationChecksum[:], source[2156:2172])
	superblock.ProtocolVersion = binary.LittleEndian.Uint16(source[2172:2174])
	if superblock.ViewHeaderCount > SuperblockHeadersMax || superblock.ViewHeaderCount > uint32(validation.Cluster.PipelineMax+1) {
		return SuperblockCandidate{}, ErrInvalidSuperblock
	}
	for index := range int(superblock.ViewHeaderCount) {
		start := SuperblockHeadersOffset + index*protocol.HeaderSize
		copy(superblock.ViewHeaders[index][:], source[start:start+protocol.HeaderSize])
	}
	unusedStart := SuperblockHeadersOffset + int(superblock.ViewHeaderCount)*protocol.HeaderSize
	if !allZeroBytes(source[unusedStart:]) {
		return SuperblockCandidate{}, ErrInvalidSuperblock
	}
	if err := superblock.Validate(validation); err != nil {
		return SuperblockCandidate{}, err
	}
	return SuperblockCandidate{
		Superblock:       superblock,
		PhysicalIndex:    physicalIndex,
		MisdirectedIndex: storedIndex != physicalIndex,
	}, nil
}

func (superblock *Superblock) Validate(validation SuperblockValidation) error {
	if !superblock.validIdentity(validation) {
		return ErrInvalidSuperblock
	}
	if superblock.State.LocalMember != validation.Membership.LocalMember || superblock.State.Members != validation.Membership.Members || superblock.State.ActiveCount != validation.Membership.ActiveCount || superblock.State.StandbyCount != validation.Membership.StandbyCount {
		return ErrInvalidSuperblock
	}
	checkpointValidation := CheckpointValidation{
		Group:          validation.Group,
		MessageSizeMax: uint32(validation.Cluster.MessageSizeMax),
		MemberCount:    validation.Membership.ActiveCount + validation.Membership.StandbyCount,
		ClientsMax:     validation.Cluster.ClientsMax,
	}
	checkpointValidation.BlockBase, _ = validation.Cluster.BlockBase()
	checkpointValidation.BlockSize = validation.Cluster.BlockSize
	if err := superblock.State.Checkpoint.Validate(checkpointValidation); err != nil {
		return err
	}
	checkpointOp := superblock.State.Checkpoint.PrepareOp()
	if superblock.State.CommitMax < checkpointOp || superblock.State.SyncMax < superblock.State.SyncMin || (superblock.State.SyncMax == 0) != (superblock.State.SyncMin == 0) || superblock.State.View < superblock.State.LogView {
		return ErrInvalidSuperblock
	}
	if superblock.ViewHeaderCount > uint32(validation.Cluster.PipelineMax+1) || superblock.ViewHeaderCount > SuperblockHeadersMax {
		return ErrInvalidSuperblock
	}
	for index := range int(superblock.ViewHeaderCount) {
		header, reason := protocol.DecodeHeader(superblock.ViewHeaders[index][:], validation.Group, uint32(validation.Cluster.MessageSizeMax), validation.Membership.ActiveCount+validation.Membership.StandbyCount)
		if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare {
			return ErrInvalidSuperblock
		}
	}
	return nil
}

func (superblock *Superblock) validIdentity(validation SuperblockValidation) bool {
	return superblock.FormatVersion == protocol.FormatVersion &&
		superblock.Release != 0 &&
		superblock.Sequence != 0 &&
		superblock.Group == validation.Group &&
		superblock.ConfigurationChecksum == validation.ConfigurationChecksum &&
		superblock.ProtocolVersion == protocol.ProtocolVersion
}

func (state *DurableReplicaState) encode(destination []byte) error {
	if len(destination) != SuperblockStateSize {
		return ErrInvalidSuperblock
	}
	clear(destination)
	if err := state.Checkpoint.Encode(destination[:CheckpointStateSize]); err != nil {
		return err
	}
	copy(destination[1024:1040], state.LocalMember[:])
	for index := range state.Members {
		copy(destination[1040+index*16:1056+index*16], state.Members[index][:])
	}
	binary.LittleEndian.PutUint64(destination[1232:1240], uint64(state.CommitMax))
	binary.LittleEndian.PutUint64(destination[1240:1248], uint64(state.SyncMin))
	binary.LittleEndian.PutUint64(destination[1248:1256], uint64(state.SyncMax))
	binary.LittleEndian.PutUint32(destination[1260:1264], uint32(state.LogView))
	binary.LittleEndian.PutUint32(destination[1264:1268], uint32(state.View))
	destination[1268] = state.ActiveCount
	destination[1269] = state.StandbyCount
	return nil
}

func decodeDurableReplicaState(source []byte, validation SuperblockValidation) (DurableReplicaState, error) {
	if len(source) != SuperblockStateSize || !allZeroBytes(source[1256:1260]) || !allZeroBytes(source[1270:]) {
		return DurableReplicaState{}, ErrInvalidSuperblock
	}
	blockBase, ok := validation.Cluster.BlockBase()
	if !ok {
		return DurableReplicaState{}, ErrInvalidSuperblock
	}
	checkpoint, err := DecodeCheckpointState(source[:1024], CheckpointValidation{
		Group:          validation.Group,
		MessageSizeMax: uint32(validation.Cluster.MessageSizeMax),
		MemberCount:    validation.Membership.ActiveCount + validation.Membership.StandbyCount,
		BlockBase:      blockBase,
		BlockSize:      validation.Cluster.BlockSize,
		ClientsMax:     validation.Cluster.ClientsMax,
	})
	if err != nil {
		return DurableReplicaState{}, err
	}
	state := DurableReplicaState{Checkpoint: checkpoint}
	copy(state.LocalMember[:], source[1024:1040])
	for index := range state.Members {
		copy(state.Members[index][:], source[1040+index*16:1056+index*16])
	}
	state.CommitMax = protocol.Op(binary.LittleEndian.Uint64(source[1232:1240]))
	state.SyncMin = protocol.Op(binary.LittleEndian.Uint64(source[1240:1248]))
	state.SyncMax = protocol.Op(binary.LittleEndian.Uint64(source[1248:1256]))
	state.LogView = protocol.View(binary.LittleEndian.Uint32(source[1260:1264]))
	state.View = protocol.View(binary.LittleEndian.Uint32(source[1264:1268]))
	state.ActiveCount = source[1268]
	state.StandbyCount = source[1269]
	return state, nil
}

func SelectSuperblock(candidates []SuperblockCandidate, copies uint64) (SuperblockCandidate, error) {
	openQuorum, ok := superblockOpenQuorum(copies)
	if !ok || len(candidates) == 0 || len(candidates) > int(copies) {
		return SuperblockCandidate{}, ErrSuperblockQuorum
	}
	var values [8]superblockLogicalValue
	valueCount := 0
	for _, candidate := range candidates {
		matched := false
		for index := range valueCount {
			value := &values[index]
			if value.candidate.Superblock.Sequence == candidate.Superblock.Sequence {
				if value.candidate.Superblock.Checksum != candidate.Superblock.Checksum {
					return SuperblockCandidate{}, ErrSuperblockFork
				}
				value.count++
				matched = true
				break
			}
		}
		if !matched {
			values[valueCount] = superblockLogicalValue{candidate: candidate, count: 1}
			valueCount++
		}
	}
	for left := range valueCount {
		for right := left + 1; right < valueCount; right++ {
			if values[right].candidate.Superblock.Sequence < values[left].candidate.Superblock.Sequence {
				values[left], values[right] = values[right], values[left]
			}
		}
	}
	for index := 1; index < valueCount; index++ {
		previous := &values[index-1].candidate.Superblock
		current := &values[index].candidate.Superblock
		if current.Sequence != previous.Sequence+1 || current.ParentChecksum != previous.Checksum || durableStateRegressed(&previous.State, &current.State) {
			return SuperblockCandidate{}, ErrInvalidSuperblock
		}
	}
	selected := -1
	for index := range valueCount {
		requiredQuorum := openQuorum
		block := &values[index].candidate.Superblock
		if block.Sequence == 1 && block.ParentChecksum.IsZero() {
			requiredQuorum, _ = superblockWriteCopies(copies)
		}
		if values[index].count < requiredQuorum {
			continue
		}
		if selected == -1 || betterSuperblock(values[index], values[selected]) {
			selected = index
		}
	}
	if selected == -1 {
		for index := range valueCount {
			block := &values[index].candidate.Superblock
			if block.Sequence == 1 && block.ParentChecksum.IsZero() {
				return SuperblockCandidate{}, ErrSuperblockInitializationIncomplete
			}
		}
		return SuperblockCandidate{}, ErrSuperblockQuorum
	}
	return values[selected].candidate, nil
}

func betterSuperblock(left, right superblockLogicalValue) bool {
	leftBlock := &left.candidate.Superblock
	rightBlock := &right.candidate.Superblock
	if leftBlock.Sequence != rightBlock.Sequence {
		return leftBlock.Sequence > rightBlock.Sequence
	}
	if left.count != right.count {
		return left.count > right.count
	}
	return compareChecksum(leftBlock.Checksum, rightBlock.Checksum) < 0
}

func durableStateRegressed(previous, current *DurableReplicaState) bool {
	return current.Checkpoint.PrepareOp() < previous.Checkpoint.PrepareOp() || current.CommitMax < previous.CommitMax || current.LogView < previous.LogView || current.View < previous.View
}

func superblockOpenQuorum(copies uint64) (uint8, bool) {
	switch copies {
	case 4:
		return 2, true
	case 6:
		return 3, true
	case 8:
		return 4, true
	default:
		return 0, false
	}
}

func superblockWriteCopies(copies uint64) (uint8, bool) {
	switch copies {
	case 4:
		return 3, true
	case 6:
		return 4, true
	case 8:
		return 5, true
	default:
		return 0, false
	}
}

func compareChecksum(left, right protocol.Checksum) int {
	return bytes.Compare(left[:], right[:])
}
