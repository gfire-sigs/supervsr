package replication

import "github.com/gfire-sigs/supervsr/replication/protocol"

type (
	GroupID      = protocol.GroupID
	MemberID     = protocol.MemberID
	ClientID     = protocol.ClientID
	Checksum     = protocol.Checksum
	Nonce        = protocol.Nonce
	CheckpointID = protocol.CheckpointID
	ReplicaIndex = protocol.ReplicaIndex
	View         = protocol.View
	Epoch        = protocol.Epoch
	Op           = protocol.Op
	Session      = protocol.Session
	RequestNo    = protocol.RequestNo
	Release      = protocol.Release
	Operation    = protocol.Operation
	Command      = protocol.Command
	BlockType    = protocol.BlockType
)

const (
	ProtocolVersion = protocol.ProtocolVersion
	FormatVersion   = protocol.FormatVersion
	MaxView         = protocol.MaxView
)

const (
	OperationReserved       = protocol.OperationReserved
	OperationRoot           = protocol.OperationRoot
	OperationRegister       = protocol.OperationRegister
	OperationReconfigure    = protocol.OperationReconfigure
	OperationPulse          = protocol.OperationPulse
	OperationUpgrade        = protocol.OperationUpgrade
	OperationNoop           = protocol.OperationNoop
	OperationApplicationMin = protocol.OperationApplicationMin
)

const (
	BlockReserved       = protocol.BlockReserved
	BlockFreeSet        = protocol.BlockFreeSet
	BlockClientSessions = protocol.BlockClientSessions
	BlockManifest       = protocol.BlockManifest
	BlockIndex          = protocol.BlockIndex
	BlockValue          = protocol.BlockValue
)
