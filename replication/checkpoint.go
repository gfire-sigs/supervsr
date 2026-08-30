package replication

import (
	"encoding/binary"
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

const CheckpointStateSize = 1024

var ErrInvalidCheckpoint = errors.New("replication: invalid checkpoint state")

type CheckpointState struct {
	Header [protocol.HeaderSize]byte

	AcquiredTrailerLastChecksum protocol.Checksum
	ReleasedTrailerLastChecksum protocol.Checksum
	SessionTrailerLastChecksum  protocol.Checksum
	OldestManifestChecksum      protocol.Checksum
	NewestManifestChecksum      protocol.Checksum
	SnapshotRootChecksum        protocol.Checksum
	AcquiredAggregateChecksum   protocol.Checksum
	ReleasedAggregateChecksum   protocol.Checksum
	SessionAggregateChecksum    protocol.Checksum
	ParentID                    protocol.CheckpointID
	GrandparentID               protocol.CheckpointID

	AcquiredTrailerLastAddress uint64
	ReleasedTrailerLastAddress uint64
	SessionTrailerLastAddress  uint64
	OldestManifestAddress      uint64
	NewestManifestAddress      uint64
	SnapshotRootAddress        uint64
	LogicalStorageSize         uint64
	AcquiredTrailerEncodedSize uint64
	ReleasedTrailerEncodedSize uint64
	SessionTrailerEncodedSize  uint64
	ManifestBlockCount         uint32
	Release                    protocol.Release
}

type CheckpointValidation struct {
	Group          protocol.GroupID
	MessageSizeMax uint32
	MemberCount    uint8
	BlockBase      uint64
	BlockSize      uint64
	ClientsMax     uint64
}

func (state *CheckpointState) Encode(destination []byte) error {
	if len(destination) != CheckpointStateSize {
		return ErrInvalidCheckpoint
	}
	clear(destination)
	copy(destination[:256], state.Header[:])
	putChecksum(destination[256:272], state.AcquiredTrailerLastChecksum)
	putChecksum(destination[288:304], state.ReleasedTrailerLastChecksum)
	putChecksum(destination[320:336], state.SessionTrailerLastChecksum)
	putChecksum(destination[352:368], state.OldestManifestChecksum)
	putChecksum(destination[384:400], state.NewestManifestChecksum)
	putChecksum(destination[416:432], state.SnapshotRootChecksum)
	putChecksum(destination[448:464], state.AcquiredAggregateChecksum)
	putChecksum(destination[464:480], state.ReleasedAggregateChecksum)
	putChecksum(destination[480:496], state.SessionAggregateChecksum)
	copy(destination[496:512], state.ParentID[:])
	copy(destination[512:528], state.GrandparentID[:])
	binary.LittleEndian.PutUint64(destination[528:536], state.AcquiredTrailerLastAddress)
	binary.LittleEndian.PutUint64(destination[536:544], state.ReleasedTrailerLastAddress)
	binary.LittleEndian.PutUint64(destination[544:552], state.SessionTrailerLastAddress)
	binary.LittleEndian.PutUint64(destination[552:560], state.OldestManifestAddress)
	binary.LittleEndian.PutUint64(destination[560:568], state.NewestManifestAddress)
	binary.LittleEndian.PutUint64(destination[568:576], state.SnapshotRootAddress)
	binary.LittleEndian.PutUint64(destination[576:584], state.LogicalStorageSize)
	binary.LittleEndian.PutUint64(destination[584:592], state.AcquiredTrailerEncodedSize)
	binary.LittleEndian.PutUint64(destination[592:600], state.ReleasedTrailerEncodedSize)
	binary.LittleEndian.PutUint64(destination[600:608], state.SessionTrailerEncodedSize)
	binary.LittleEndian.PutUint32(destination[608:612], state.ManifestBlockCount)
	binary.LittleEndian.PutUint32(destination[612:616], uint32(state.Release))
	return nil
}

func DecodeCheckpointState(source []byte, validation CheckpointValidation) (CheckpointState, error) {
	if len(source) != CheckpointStateSize || !checkpointReservationsZero(source) {
		return CheckpointState{}, ErrInvalidCheckpoint
	}
	var state CheckpointState
	copy(state.Header[:], source[:256])
	copy(state.AcquiredTrailerLastChecksum[:], source[256:272])
	copy(state.ReleasedTrailerLastChecksum[:], source[288:304])
	copy(state.SessionTrailerLastChecksum[:], source[320:336])
	copy(state.OldestManifestChecksum[:], source[352:368])
	copy(state.NewestManifestChecksum[:], source[384:400])
	copy(state.SnapshotRootChecksum[:], source[416:432])
	copy(state.AcquiredAggregateChecksum[:], source[448:464])
	copy(state.ReleasedAggregateChecksum[:], source[464:480])
	copy(state.SessionAggregateChecksum[:], source[480:496])
	copy(state.ParentID[:], source[496:512])
	copy(state.GrandparentID[:], source[512:528])
	state.AcquiredTrailerLastAddress = binary.LittleEndian.Uint64(source[528:536])
	state.ReleasedTrailerLastAddress = binary.LittleEndian.Uint64(source[536:544])
	state.SessionTrailerLastAddress = binary.LittleEndian.Uint64(source[544:552])
	state.OldestManifestAddress = binary.LittleEndian.Uint64(source[552:560])
	state.NewestManifestAddress = binary.LittleEndian.Uint64(source[560:568])
	state.SnapshotRootAddress = binary.LittleEndian.Uint64(source[568:576])
	state.LogicalStorageSize = binary.LittleEndian.Uint64(source[576:584])
	state.AcquiredTrailerEncodedSize = binary.LittleEndian.Uint64(source[584:592])
	state.ReleasedTrailerEncodedSize = binary.LittleEndian.Uint64(source[592:600])
	state.SessionTrailerEncodedSize = binary.LittleEndian.Uint64(source[600:608])
	state.ManifestBlockCount = binary.LittleEndian.Uint32(source[608:612])
	state.Release = protocol.Release(binary.LittleEndian.Uint32(source[612:616]))
	if err := state.Validate(validation); err != nil {
		return CheckpointState{}, err
	}
	return state, nil
}

func (state *CheckpointState) Validate(validation CheckpointValidation) error {
	header, reason := protocol.DecodeHeader(state.Header[:], validation.Group, validation.MessageSizeMax, validation.MemberCount)
	if reason != protocol.RejectNone || header.Command != protocol.CommandPrepare {
		return ErrInvalidCheckpoint
	}
	if state.Release == 0 || state.LogicalStorageSize < validation.BlockBase || validation.BlockSize == 0 || (state.LogicalStorageSize-validation.BlockBase)%validation.BlockSize != 0 {
		return ErrInvalidCheckpoint
	}
	blockCount := (state.LogicalStorageSize - validation.BlockBase) / validation.BlockSize
	emptyChecksum := protocol.ChecksumBytes(nil)
	if !validEWAHTrailerReference(state.AcquiredTrailerLastAddress, state.AcquiredTrailerEncodedSize, state.AcquiredTrailerLastChecksum, state.AcquiredAggregateChecksum, emptyChecksum, blockCount) {
		return ErrInvalidCheckpoint
	}
	if !validEWAHTrailerReference(state.ReleasedTrailerLastAddress, state.ReleasedTrailerEncodedSize, state.ReleasedTrailerLastChecksum, state.ReleasedAggregateChecksum, emptyChecksum, blockCount) {
		return ErrInvalidCheckpoint
	}
	sessionSize, ok := checkedMul(validation.ClientsMax, protocol.HeaderSize+8)
	if !ok || !validSessionTrailerReference(state.SessionTrailerLastAddress, state.SessionTrailerEncodedSize, state.SessionTrailerLastChecksum, state.SessionAggregateChecksum, emptyChecksum, sessionSize) {
		return ErrInvalidCheckpoint
	}
	references := [...]uint64{
		state.AcquiredTrailerLastAddress, state.ReleasedTrailerLastAddress, state.SessionTrailerLastAddress,
		state.OldestManifestAddress, state.NewestManifestAddress, state.SnapshotRootAddress,
	}
	for _, address := range references {
		if address != 0 && !validCheckpointBlockAddress(address, blockCount) {
			return ErrInvalidCheckpoint
		}
	}
	if !validManifestReferences(state) || !validOptionalReference(state.SnapshotRootAddress, state.SnapshotRootChecksum) {
		return ErrInvalidCheckpoint
	}
	return nil
}

func (state *CheckpointState) ID() (protocol.CheckpointID, error) {
	var encoded [CheckpointStateSize]byte
	if err := state.Encode(encoded[:]); err != nil {
		return protocol.CheckpointID{}, err
	}
	return protocol.CheckpointID(protocol.ChecksumBytes(encoded[:])), nil
}

func (state *CheckpointState) PrepareOp() protocol.Op {
	return protocol.Op(binary.LittleEndian.Uint64(state.Header[224:232]))
}

func validEWAHTrailerReference(address, encodedSize uint64, last, aggregate, empty protocol.Checksum, blockCount uint64) bool {
	if address == 0 {
		return encodedSize == 0 && last.IsZero() && aggregate == empty
	}
	words := blockCount / 64
	if blockCount%64 != 0 {
		words++
	}
	maximumWords, ok := checkedAdd(words, 1)
	if !ok {
		return false
	}
	maximumBytes, ok := checkedMul(maximumWords, 8)
	return ok && encodedSize != 0 && encodedSize%8 == 0 && encodedSize <= maximumBytes && !last.IsZero()
}

func validSessionTrailerReference(address, encodedSize uint64, last, aggregate, empty protocol.Checksum, expectedSize uint64) bool {
	if address == 0 {
		return encodedSize == 0 && last.IsZero() && aggregate == empty
	}
	return encodedSize == expectedSize && expectedSize != 0 && !last.IsZero()
}

func validCheckpointBlockAddress(address, blockCount uint64) bool {
	return address != 0 && address <= blockCount
}

func validManifestReferences(state *CheckpointState) bool {
	switch state.ManifestBlockCount {
	case 0:
		return state.OldestManifestAddress == 0 && state.NewestManifestAddress == 0 && state.OldestManifestChecksum.IsZero() && state.NewestManifestChecksum.IsZero()
	case 1:
		return state.OldestManifestAddress != 0 && state.OldestManifestAddress == state.NewestManifestAddress && state.OldestManifestChecksum == state.NewestManifestChecksum
	default:
		return state.OldestManifestAddress != 0 && state.NewestManifestAddress != 0
	}
}

func validOptionalReference(address uint64, checksum protocol.Checksum) bool {
	return (address == 0) == checksum.IsZero()
}

func putChecksum(destination []byte, checksum protocol.Checksum) {
	copy(destination, checksum[:])
}

func allZeroBytes(input []byte) bool {
	var accumulated byte
	for _, value := range input {
		accumulated |= value
	}
	return accumulated == 0
}

func checkpointReservationsZero(source []byte) bool {
	if len(source) != CheckpointStateSize {
		return false
	}
	return allZeroBytes(source[272:288]) &&
		allZeroBytes(source[304:320]) &&
		allZeroBytes(source[336:352]) &&
		allZeroBytes(source[368:384]) &&
		allZeroBytes(source[400:416]) &&
		allZeroBytes(source[432:448]) &&
		allZeroBytes(source[616:])
}
