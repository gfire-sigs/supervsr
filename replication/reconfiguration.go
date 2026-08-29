package replication

import (
	"encoding/binary"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type ReconfigurationResult uint32

const (
	ReconfigurationReserved ReconfigurationResult = iota
	ReconfigurationOK
	ReconfigurationReplicaCountZero
	ReconfigurationReplicaCountMaxExceeded
	ReconfigurationStandbyCountMaxExceeded
	ReconfigurationMembersInvalid
	ReconfigurationMembersCountInvalid
	ReconfigurationReservedField
	ReconfigurationResultMustBeReserved
	ReconfigurationEpochInPast
	ReconfigurationEpochInFuture
	ReconfigurationDifferentReplicaCount
	ReconfigurationDifferentStandbyCount
	ReconfigurationDifferentMemberSet
	ReconfigurationApplied
	ReconfigurationConflict
	ReconfigurationIsNoOp
)

func ValidateReconfiguration(body []byte, current Membership, currentEpoch protocol.Epoch) ReconfigurationResult {
	if len(body) != 256 {
		return ReconfigurationReservedField
	}
	active := body[196]
	standby := body[197]
	if active == 0 {
		return ReconfigurationReplicaCountZero
	}
	if active > ActiveMax {
		return ReconfigurationReplicaCountMaxExceeded
	}
	if standby > StandbyMax || int(active)+int(standby) > MembersMax {
		return ReconfigurationStandbyCountMaxExceeded
	}
	members, nonzero, valid := decodeReconfigurationMembers(body[:192])
	if !valid {
		return ReconfigurationMembersInvalid
	}
	if nonzero != int(active)+int(standby) {
		return ReconfigurationMembersCountInvalid
	}
	if !zeroBytes(body[198:252]) {
		return ReconfigurationReservedField
	}
	if binary.LittleEndian.Uint32(body[252:]) != 0 {
		return ReconfigurationResultMustBeReserved
	}
	if active != current.ActiveCount {
		return ReconfigurationDifferentReplicaCount
	}
	if standby != current.StandbyCount {
		return ReconfigurationDifferentStandbyCount
	}
	epoch := protocol.Epoch(binary.LittleEndian.Uint32(body[192:196]))
	if epoch < currentEpoch {
		return ReconfigurationEpochInPast
	}
	if uint64(epoch) > uint64(currentEpoch)+1 {
		return ReconfigurationEpochInFuture
	}
	if !sameMemberSet(members, current.Members, nonzero) {
		return ReconfigurationDifferentMemberSet
	}
	identical := members == current.Members
	if epoch == currentEpoch {
		if identical {
			return ReconfigurationApplied
		}
		return ReconfigurationConflict
	}
	if identical {
		return ReconfigurationIsNoOp
	}
	return ReconfigurationOK
}

func decodeReconfigurationMembers(encoded []byte) ([MembersMax]protocol.MemberID, int, bool) {
	var members [MembersMax]protocol.MemberID
	nonzero := 0
	zeroSeen := false
	for index := range MembersMax {
		copy(members[index][:], encoded[index*16:(index+1)*16])
		if members[index].IsZero() {
			zeroSeen = true
			continue
		}
		if zeroSeen {
			return members, 0, false
		}
		for previous := range index {
			if members[index] == members[previous] {
				return members, 0, false
			}
		}
		nonzero++
	}
	return members, nonzero, true
}

func sameMemberSet(proposed, current [MembersMax]protocol.MemberID, count int) bool {
	for index := range count {
		found := false
		for candidate := range count {
			if proposed[index] == current[candidate] {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
