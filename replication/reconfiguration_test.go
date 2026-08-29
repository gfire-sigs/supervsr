package replication

import (
	"encoding/binary"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestValidateReconfigurationPrecedenceAndDisposition(t *testing.T) {
	membership := Membership{Members: [MembersMax]protocol.MemberID{{1}, {2}}, ActiveCount: 2, LocalMember: protocol.MemberID{1}}
	valid := reconfigurationBody(membership, 0)
	tests := []struct {
		name   string
		mutate func([]byte)
		epoch  protocol.Epoch
		expect ReconfigurationResult
	}{
		{"replica-zero-before-members", func(body []byte) { body[196] = 0; body[16] = 1 }, 0, ReconfigurationReplicaCountZero},
		{"replica-max", func(body []byte) { body[196] = ActiveMax + 1 }, 0, ReconfigurationReplicaCountMaxExceeded},
		{"standby-max", func(body []byte) { body[197] = StandbyMax + 1 }, 0, ReconfigurationStandbyCountMaxExceeded},
		{"members-invalid", func(body []byte) { copy(body[16:32], body[:16]) }, 0, ReconfigurationMembersInvalid},
		{"members-count", func(body []byte) { clear(body[16:32]) }, 0, ReconfigurationMembersCountInvalid},
		{"reserved", func(body []byte) { body[198] = 1 }, 0, ReconfigurationReservedField},
		{"result", func(body []byte) { binary.LittleEndian.PutUint32(body[252:], 1) }, 0, ReconfigurationResultMustBeReserved},
		{"different-replica-count", func(body []byte) { body[196] = 1; clear(body[16:32]) }, 0, ReconfigurationDifferentReplicaCount},
		{"epoch-past", func(body []byte) {}, 1, ReconfigurationEpochInPast},
		{"different-standby-count", func(body []byte) { body[32] = 3; body[197] = 1 }, 0, ReconfigurationDifferentStandbyCount},
		{"epoch-future", func(body []byte) { binary.LittleEndian.PutUint32(body[192:196], 2) }, 0, ReconfigurationEpochInFuture},
		{"different-set", func(body []byte) { body[16] = 3 }, 0, ReconfigurationDifferentMemberSet},
		{"applied", func(body []byte) {}, 0, ReconfigurationApplied},
		{"conflict", swapReconfigurationMembers, 0, ReconfigurationConflict},
		{"next-noop", func(body []byte) { binary.LittleEndian.PutUint32(body[192:196], 1) }, 0, ReconfigurationIsNoOp},
		{"next-ok", func(body []byte) { binary.LittleEndian.PutUint32(body[192:196], 1); swapReconfigurationMembers(body) }, 0, ReconfigurationOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := append([]byte(nil), valid...)
			test.mutate(body)
			if result := ValidateReconfiguration(body, membership, test.epoch); result != test.expect {
				t.Fatalf("result=%d want=%d", result, test.expect)
			}
		})
	}
}

func reconfigurationBody(membership Membership, epoch protocol.Epoch) []byte {
	body := make([]byte, 256)
	for index := range membership.Members {
		copy(body[index*16:(index+1)*16], membership.Members[index][:])
	}
	binary.LittleEndian.PutUint32(body[192:196], uint32(epoch))
	body[196] = membership.ActiveCount
	body[197] = membership.StandbyCount
	return body
}

func swapReconfigurationMembers(body []byte) {
	var first [16]byte
	copy(first[:], body[:16])
	copy(body[:16], body[16:32])
	copy(body[16:32], first[:])
}
