package protocol

import "encoding/hex"

type GroupID [16]byte
type MemberID [16]byte
type ClientID [16]byte
type Checksum [16]byte
type Nonce [16]byte
type CheckpointID [16]byte

type ReplicaIndex uint8
type View uint32
type Epoch uint32
type Op uint64
type Session uint64
type RequestNo uint32
type Release uint32
type Operation uint8

type Command uint8

const (
	ProtocolVersion uint16 = 0
	FormatVersion   uint16 = 1
	MaxView                = View(^uint32(0))
)

const (
	CommandReserved Command = iota
	CommandPing
	CommandPong
	CommandClientPing
	CommandClientPong
	CommandRequest
	CommandPrepare
	CommandPrepareOK
	CommandReply
	CommandCommit
	CommandExitView
	CommandJoinView
	CommandRetired12
	CommandGetView
	CommandGetHeaders
	CommandGetPrepare
	CommandGetReply
	CommandHeaders
	CommandEviction
	CommandGetBlocks
	CommandBlock
	CommandRetired21
	CommandRetired22
	CommandRetired23
	CommandView
)

const (
	OperationReserved Operation = iota
	OperationRoot
	OperationRegister
	OperationReconfigure
	OperationPulse
	OperationUpgrade
	OperationNoop
	OperationApplicationMin Operation = 128
)

const (
	EvictionReserved EvictionReason = iota
	EvictionNoSession
	EvictionClientReleaseTooLow
	EvictionClientReleaseTooHigh
	EvictionInvalidOperation
	EvictionInvalidBody
	EvictionInvalidBodySize
	EvictionSessionTooLow
	EvictionSessionReleaseMismatch
)

type EvictionReason uint8

const (
	BlockReserved BlockType = iota
	BlockFreeSet
	BlockClientSessions
	BlockManifest
	BlockIndex
	BlockValue
)

type BlockType uint8

func (checksum Checksum) IsZero() bool {
	return checksum == Checksum{}
}

func (checksum Checksum) String() string {
	return hex.EncodeToString(checksum[:])
}

func (id GroupID) IsZero() bool {
	return id == GroupID{}
}

func (id MemberID) IsZero() bool {
	return id == MemberID{}
}

func (id ClientID) IsZero() bool {
	return id == ClientID{}
}
