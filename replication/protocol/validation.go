package protocol

import "encoding/binary"

type ValidationContext struct {
	Authenticated           bool
	Sender                  ReplicaIndex
	ActiveCount             uint8
	MemberCount             uint8
	PipelineMax             uint8
	ReleaseHistoryMax       uint16
	ApplicationBatchSizeMax uint32
	ApplicationReplySizeMax uint32
	RepairRequestsMax       uint32
	CurrentRelease          Release
	ClientReleaseMin        Release
	Group                   GroupID
	MessageSizeMax          uint32
}

func ValidateSemantics(header *Header, body []byte, context ValidationContext) RejectReason {
	if !context.Authenticated {
		return RejectAuthentication
	}
	if context.ActiveCount == 0 || context.MemberCount < context.ActiveCount || context.Sender >= ReplicaIndex(context.MemberCount) {
		return RejectAuthor
	}
	primary := ReplicaIndex(uint32(header.View) % uint32(context.ActiveCount))
	if !validAuthor(header.Command, header.Author, context.Sender, primary) {
		return RejectAuthor
	}
	if !validRelease(header, context) {
		return RejectCommandFields
	}
	return validateCommand(header, body, context)
}

func validAuthor(command Command, author, sender, primary ReplicaIndex) bool {
	switch command {
	case CommandRequest, CommandClientPing, CommandBlock:
		return author == 0
	case CommandPrepare, CommandReply, CommandCommit, CommandView:
		return author == primary
	case CommandPing, CommandPong, CommandClientPong, CommandPrepareOK, CommandExitView,
		CommandJoinView, CommandGetView, CommandGetHeaders, CommandGetPrepare, CommandGetReply,
		CommandHeaders, CommandEviction, CommandGetBlocks:
		return author == sender
	default:
		return false
	}
}

func validRelease(header *Header, context ValidationContext) bool {
	requiresRelease := false
	switch header.Command {
	case CommandRequest, CommandReply, CommandPing, CommandPong, CommandClientPing, CommandClientPong, CommandEviction, CommandBlock:
		requiresRelease = true
	case CommandPrepare:
		operation := Operation(header.Fields[124])
		requiresRelease = operation != OperationRoot && operation != OperationReserved
	}
	if requiresRelease {
		return header.Release != 0 && header.Release <= context.CurrentRelease
	}
	return header.Release == 0
}

func validateCommand(header *Header, body []byte, context ValidationContext) RejectReason {
	fields := header.Fields[:]
	switch header.Command {
	case CommandPing:
		return validatePing(header, body, context)
	case CommandPong:
		if !allZero(fields[16:]) || len(body) != 0 || binary.LittleEndian.Uint64(fields[:8]) == 0 {
			return RejectCommandFields
		}
	case CommandClientPing:
		if isZero16(fields[:16]) || binary.LittleEndian.Uint64(fields[16:24]) == 0 || !allZero(fields[32:]) || len(body) != 0 {
			return RejectCommandFields
		}
	case CommandClientPong:
		if binary.LittleEndian.Uint64(fields[:8]) == 0 || !allZero(fields[8:]) || len(body) != 0 {
			return RejectCommandFields
		}
	case CommandRequest:
		return validateRequest(header, body, context)
	case CommandPrepare:
		return validatePrepare(header, body, context)
	case CommandPrepareOK:
		return validatePrepareOK(header, body)
	case CommandReply:
		return validateReply(header, body, context)
	case CommandCommit:
		checkpointOp := binary.LittleEndian.Uint64(fields[48:56])
		commit := binary.LittleEndian.Uint64(fields[56:64])
		if commit < checkpointOp || binary.LittleEndian.Uint64(fields[64:72]) == 0 || !allZero(fields[72:]) || len(body) != 0 {
			return RejectCommandFields
		}
	case CommandExitView:
		if !allZero(fields) || len(body) != 0 {
			return RejectCommandFields
		}
	case CommandJoinView:
		return validateJoinView(header, body, context)
	case CommandView:
		return validateView(header, body, context)
	case CommandGetView:
		if isZero16(fields[:16]) || !allZero(fields[16:]) || len(body) != 0 {
			return RejectCommandFields
		}
	case CommandGetHeaders:
		if binary.LittleEndian.Uint64(fields[:8]) > binary.LittleEndian.Uint64(fields[8:16]) || !allZero(fields[16:]) || len(body) != 0 {
			return RejectCommandFields
		}
	case CommandHeaders:
		if !allZero(fields) || len(body) == 0 || len(body)%HeaderSize != 0 || len(body)/HeaderSize > 64 {
			return RejectBodyShape
		}
		return validateEmbeddedPrepareHeaders(body, context, false)
	case CommandGetPrepare:
		checksumZero := isZero16(fields[:16])
		if !allZero(fields[16:32]) || !allZero(fields[40:]) || len(body) != 0 || (header.View == 0) == checksumZero {
			return RejectCommandFields
		}
	case CommandGetReply:
		if !validGetReply(fields, body) {
			return RejectCommandFields
		}
	case CommandEviction:
		reason := EvictionReason(fields[127])
		if isZero16(fields[:16]) || !allZero(fields[16:127]) || reason < EvictionNoSession || reason > EvictionSessionReleaseMismatch || len(body) != 0 {
			return RejectCommandFields
		}
	case CommandGetBlocks:
		return validateGetBlocks(fields, body, context)
	case CommandBlock:
		blockType := BlockType(fields[112])
		if binary.LittleEndian.Uint64(fields[96:104]) == 0 || blockType < BlockFreeSet || blockType > BlockValue || !allZero(fields[113:]) || len(body) == 0 {
			return RejectCommandFields
		}
	default:
		return RejectCommand
	}
	return RejectNone
}

func validatePing(header *Header, body []byte, context ValidationContext) RejectReason {
	fields := header.Fields[:]
	count := binary.LittleEndian.Uint16(fields[32:34])
	if binary.LittleEndian.Uint64(fields[24:32]) == 0 || count == 0 || count > context.ReleaseHistoryMax || !allZero(fields[34:]) || len(body) != int(context.ReleaseHistoryMax)*4 {
		return RejectCommandFields
	}
	var previous Release
	containsHeaderRelease := false
	for index := range int(context.ReleaseHistoryMax) {
		release := Release(binary.LittleEndian.Uint32(body[index*4:]))
		if index < int(count) {
			if release == 0 || release <= previous {
				return RejectBodyShape
			}
			containsHeaderRelease = containsHeaderRelease || release == header.Release
			previous = release
			continue
		}
		if release != 0 {
			return RejectBodyShape
		}
	}
	if !containsHeaderRelease {
		return RejectBodyShape
	}
	return RejectNone
}

func validateRequest(header *Header, body []byte, context ValidationContext) RejectReason {
	fields := header.Fields[:]
	if !allZero(fields[16:32]) || !allZero(fields[69:72]) || !allZero(fields[76:]) {
		return RejectCommandFields
	}
	session := binary.LittleEndian.Uint64(fields[48:56])
	request := binary.LittleEndian.Uint32(fields[64:68])
	operation := Operation(fields[68])
	if operation == OperationRegister {
		invalidIdentity := !isZero16(fields[:16]) || session != 0 || request != 0
		invalidBodyLength := len(body) != 0 && len(body) != 256
		if invalidIdentity || invalidBodyLength {
			return RejectCommandFields
		}
		if len(body) == 256 && !allZero(body) {
			return RejectBodyShape
		}
		return RejectNone
	}
	if session == 0 || request == 0 || !validClientOperation(operation) {
		return RejectCommandFields
	}
	return validateOperationBody(operation, body, context.ApplicationBatchSizeMax, false)
}

func validatePrepare(header *Header, body []byte, context ValidationContext) RejectReason {
	fields := header.Fields[:]
	if !allZero(fields[16:32]) || !allZero(fields[48:64]) || !allZero(fields[125:]) {
		return RejectCommandFields
	}
	op := binary.LittleEndian.Uint64(fields[96:104])
	commit := binary.LittleEndian.Uint64(fields[104:112])
	timestamp := binary.LittleEndian.Uint64(fields[112:120])
	operation := Operation(fields[124])
	if operation == OperationRoot || operation == OperationReserved {
		if !validRootPrepare(fields, body, op, commit, timestamp) {
			return RejectCommandFields
		}
		return RejectNone
	}
	if op == 0 || op <= commit || timestamp == 0 || !validPreparedOperation(operation) {
		return RejectCommandFields
	}
	if operation == OperationRegister {
		if binary.LittleEndian.Uint32(fields[120:124]) != 0 || isZero16(fields[80:96]) || len(body) != 256 || binary.LittleEndian.Uint32(body[:4]) != context.ApplicationBatchSizeMax || !allZero(body[4:]) {
			return RejectBodyShape
		}
		return RejectNone
	}
	return validateOperationBody(operation, body, context.ApplicationBatchSizeMax, true)
}

func validatePrepareOK(header *Header, body []byte) RejectReason {
	fields := header.Fields[:]
	op := binary.LittleEndian.Uint64(fields[96:104])
	commitMin := binary.LittleEndian.Uint64(fields[104:112])
	invalidIdentity := !allZero(fields[16:32]) || !allZero(fields[48:64])
	invalidProgress := op == 0 || op <= commitMin || binary.LittleEndian.Uint64(fields[112:120]) == 0
	invalidOperation := Operation(fields[124]) <= OperationRoot
	if invalidIdentity || invalidProgress || invalidOperation || !allZero(fields[125:]) || len(body) != 0 {
		return RejectCommandFields
	}
	return RejectNone
}

func validateReply(header *Header, body []byte, context ValidationContext) RejectReason {
	fields := header.Fields[:]
	op := binary.LittleEndian.Uint64(fields[80:88])
	commit := binary.LittleEndian.Uint64(fields[88:96])
	invalidIdentity := !allZero(fields[16:32]) || !allZero(fields[48:64]) || isZero16(fields[64:80])
	invalidProgress := op == 0 || op != commit || binary.LittleEndian.Uint64(fields[96:104]) == 0
	invalidOperation := !validPreparedOperation(Operation(fields[108]))
	invalidBody := !allZero(fields[109:]) || uint32(len(body)) > context.ApplicationReplySizeMax
	if invalidIdentity || invalidProgress || invalidOperation || invalidBody {
		return RejectCommandFields
	}
	return RejectNone
}

func validateJoinView(header *Header, body []byte, context ValidationContext) RejectReason {
	fields := header.Fields[:]
	head := binary.LittleEndian.Uint64(fields[32:40])
	commit := binary.LittleEndian.Uint64(fields[40:48])
	checkpoint := binary.LittleEndian.Uint64(fields[48:56])
	entryCount := len(body) / HeaderSize
	invalidProgress := head < commit || commit < checkpoint
	invalidBody := len(body) == 0 || len(body)%HeaderSize != 0 || entryCount > int(context.PipelineMax)+1
	if invalidProgress || invalidBody || !allZero(fields[60:]) {
		return RejectBodyShape
	}
	if !bitsWithin(fields[:16], entryCount) || !bitsWithin(fields[16:32], entryCount) {
		return RejectCommandFields
	}
	return validateEmbeddedPrepareHeaders(body, context, true)
}

func validateView(header *Header, body []byte, context ValidationContext) RejectReason {
	fields := header.Fields[:]
	head := binary.LittleEndian.Uint64(fields[16:24])
	commit := binary.LittleEndian.Uint64(fields[24:32])
	checkpoint := binary.LittleEndian.Uint64(fields[32:40])
	if head < commit || commit < checkpoint || !allZero(fields[40:]) {
		return RejectCommandFields
	}
	if len(body) < 1024 || (len(body)-1024)%HeaderSize != 0 || (len(body)-1024)/HeaderSize > int(context.PipelineMax)+1 {
		return RejectBodyShape
	}
	if len(body) == 1024 {
		return RejectNone
	}
	return validateEmbeddedPrepareHeaders(body[1024:], context, true)
}

func validateEmbeddedPrepareHeaders(body []byte, context ValidationContext, allowBlank bool) RejectReason {
	for offset := 0; offset < len(body); offset += HeaderSize {
		encoded := body[offset : offset+HeaderSize]
		if allowBlank && allZero(encoded) {
			continue
		}
		header, reason := DecodeHeader(encoded, context.Group, context.MessageSizeMax, context.MemberCount)
		if reason != RejectNone || header.Command != CommandPrepare {
			return RejectBodyShape
		}
	}
	return RejectNone
}

func validateGetBlocks(fields, body []byte, context ValidationContext) RejectReason {
	if !allZero(fields) || len(body) == 0 || len(body)%32 != 0 || uint32(len(body)/32) > context.RepairRequestsMax {
		return RejectBodyShape
	}
	for offset := 0; offset < len(body); offset += 32 {
		if binary.LittleEndian.Uint64(body[offset+16:offset+24]) == 0 || !allZero(body[offset+24:offset+32]) {
			return RejectBodyShape
		}
	}
	return RejectNone
}

func bitsWithin(bits []byte, count int) bool {
	for bit := count; bit < len(bits)*8; bit++ {
		if bits[bit/8]&(1<<uint(bit%8)) != 0 {
			return false
		}
	}
	return true
}

func validateOperationBody(operation Operation, body []byte, maximum uint32, prepared bool) RejectReason {
	if uint32(len(body)) > maximum {
		return RejectBodyShape
	}
	switch operation {
	case OperationNoop, OperationPulse:
		if len(body) != 0 {
			return RejectBodyShape
		}
	case OperationUpgrade:
		if len(body) != 16 || binary.LittleEndian.Uint32(body[:4]) == 0 || !allZero(body[4:]) {
			return RejectBodyShape
		}
	case OperationReconfigure:
		if len(body) != 256 {
			return RejectBodyShape
		}
		if !prepared && binary.LittleEndian.Uint32(body[252:]) != 0 {
			return RejectBodyShape
		}
	default:
		if operation < OperationApplicationMin {
			return RejectCommandFields
		}
	}
	return RejectNone
}

func validClientOperation(operation Operation) bool {
	return operation == OperationReconfigure || operation == OperationNoop || operation >= OperationApplicationMin
}

func validPreparedOperation(operation Operation) bool {
	return operation == OperationRegister || operation == OperationReconfigure || operation == OperationPulse || operation == OperationUpgrade || operation == OperationNoop || operation >= OperationApplicationMin
}

func isZero16(input []byte) bool {
	return len(input) == 16 && allZero(input)
}

func validGetReply(fields, body []byte) bool {
	validIdentity := !isZero16(fields[:16]) && allZero(fields[16:32]) && !isZero16(fields[32:48])
	validOperation := binary.LittleEndian.Uint64(fields[48:56]) != 0 && allZero(fields[56:])
	return validIdentity && validOperation && len(body) == 0
}

func validRootPrepare(fields, body []byte, op, commit, timestamp uint64) bool {
	validProgress := op == 0 && commit == 0 && timestamp == 0
	validIdentity := isZero16(fields[:16]) && isZero16(fields[32:48]) && isZero16(fields[64:96])
	return validProgress && validIdentity && allZero(fields[120:124]) && len(body) == 0
}
