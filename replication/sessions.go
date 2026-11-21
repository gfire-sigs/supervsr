package replication

import (
	"bytes"
	"encoding/binary"
	"errors"

	"z.gfire.dev/supervsr/replication/protocol"
)

var (
	ErrSessionCapacity = errors.New("replication: client session capacity exhausted")
	ErrSessionEncoding = errors.New("replication: invalid session table encoding")
	ErrSessionChecksum = errors.New("replication: client session trailer checksum mismatch")
)

type sessionDecision uint8

const (
	sessionAdmit sessionDecision = iota + 1
	sessionDuplicate
	sessionDrop
	sessionClientFork
	sessionNoSession
	sessionTooLow
	sessionBehind
	sessionReleaseMismatch
)

type sessionRequest struct {
	Client          protocol.ClientID
	Session         protocol.Session
	Request         protocol.RequestNo
	Release         protocol.Release
	RequestChecksum protocol.Checksum
	Parent          protocol.Checksum
}

type sessionTableConfig struct {
	ClientsMax              uint32
	Group                   protocol.GroupID
	ActiveCount             uint8
	MessageSizeMax          uint32
	ApplicationReplySizeMax uint32
	ChecksumAlgorithm       ChecksumAlgorithm
}

type sessionRecord struct {
	Reply   protocol.Header
	Session protocol.Session
}

type sessionCommitPlan struct {
	Slot          uint32
	Generation    uint64
	Client        protocol.ClientID
	Session       protocol.Session
	ReplyChecksum protocol.Checksum
}

type sessionTable struct {
	config      sessionTableConfig
	records     []sessionRecord
	busy        []bool
	generations []uint64
	count       uint32
}

func newSessionTable(config sessionTableConfig) (*sessionTable, error) {
	if config.ClientsMax == 0 || config.ActiveCount == 0 || config.MessageSizeMax < protocol.HeaderSize || config.ApplicationReplySizeMax > config.MessageSizeMax-protocol.HeaderSize || !config.ChecksumAlgorithm.Valid() {
		return nil, ErrSessionCapacity
	}
	return &sessionTable{
		config:      config,
		records:     make([]sessionRecord, config.ClientsMax),
		busy:        make([]bool, config.ClientsMax),
		generations: make([]uint64, config.ClientsMax),
	}, nil
}

func (table *sessionTable) Decide(request sessionRequest) sessionDecision {
	record, _, found := table.lookup(request.Client)
	if !found {
		return sessionNoSession
	}
	if request.Session < record.Session {
		return sessionTooLow
	}
	if request.Session > record.Session {
		return sessionBehind
	}
	if request.Release != record.Reply.Release {
		return sessionReleaseMismatch
	}
	latestRequest := replyRequest(&record.Reply)
	if request.Request < latestRequest || request.Request > latestRequest+1 {
		return sessionDrop
	}
	if request.Request == latestRequest {
		if request.RequestChecksum == replyRequestChecksum(&record.Reply) {
			return sessionDuplicate
		}
		return sessionClientFork
	}
	if request.Parent != replyContext(&record.Reply) {
		return sessionClientFork
	}
	return sessionAdmit
}

func (table *sessionTable) PlanCommit(reply protocol.Header, session protocol.Session) (sessionCommitPlan, error) {
	if err := table.validateOccupied(reply, session); err != nil {
		return sessionCommitPlan{}, err
	}
	client := replyClient(&reply)
	_, slot, found := table.lookup(client)
	if found {
		if table.busy[slot] {
			return sessionCommitPlan{}, ErrSessionCapacity
		}
	} else if table.count == uint32(len(table.records)) {
		slot = table.evictionSlot()
		if table.busy[slot] {
			return sessionCommitPlan{}, ErrSessionCapacity
		}
	} else if slot == uint32(len(table.records)) {
		return sessionCommitPlan{}, ErrSessionCapacity
	}
	table.generations[slot]++
	table.busy[slot] = true
	return sessionCommitPlan{
		Slot:          slot,
		Generation:    table.generations[slot],
		Client:        client,
		Session:       session,
		ReplyChecksum: reply.HeaderChecksum,
	}, nil
}

func (table *sessionTable) Session(client protocol.ClientID) (protocol.Session, bool) {
	record, _, found := table.lookup(client)
	if !found {
		return 0, false
	}
	return record.Session, true
}

func (table *sessionTable) Commit(reply protocol.Header, session protocol.Session) (uint32, error) {
	plan, err := table.PlanCommit(reply, session)
	if err != nil {
		return 0, err
	}
	if err := table.CommitAt(plan, reply, session); err != nil {
		table.Abort(plan)
		return 0, err
	}
	return plan.Slot, nil
}

func (table *sessionTable) CommitAt(plan sessionCommitPlan, reply protocol.Header, session protocol.Session) error {
	if int(plan.Slot) >= len(table.records) || !table.busy[plan.Slot] {
		return ErrSessionEncoding
	}
	if table.generations[plan.Slot] != plan.Generation || plan.Client != replyClient(&reply) {
		return ErrSessionEncoding
	}
	if plan.Session != session || plan.ReplyChecksum != reply.HeaderChecksum {
		return ErrSessionEncoding
	}
	if err := table.validateOccupied(reply, session); err != nil {
		return err
	}
	record := &table.records[plan.Slot]
	if record.Session == 0 {
		table.count++
	}
	record.Reply = reply
	record.Session = session
	table.busy[plan.Slot] = false
	return nil
}

func (table *sessionTable) Abort(plan sessionCommitPlan) {
	if int(plan.Slot) < len(table.records) && table.busy[plan.Slot] && table.generations[plan.Slot] == plan.Generation {
		table.busy[plan.Slot] = false
	}
}

func (table *sessionTable) Reply(client protocol.ClientID, session protocol.Session, request protocol.RequestNo) (protocol.Header, uint32, bool) {
	record, slot, found := table.lookup(client)
	if !found || table.busy[slot] || record.Session != session || replyRequest(&record.Reply) != request {
		return protocol.Header{}, 0, false
	}
	return record.Reply, slot, true
}

func (table *sessionTable) RepairReply(client protocol.ClientID, op protocol.Op, checksum protocol.Checksum) (protocol.Header, uint32, bool) {
	record, slot, found := table.lookup(client)
	if !found || table.busy[slot] || replyOp(&record.Reply) != op || record.Reply.HeaderChecksum != checksum {
		return protocol.Header{}, 0, false
	}
	return record.Reply, slot, true
}

func (table *sessionTable) Count() uint32 {
	return table.count
}

func (table *sessionTable) TrailerSize() int {
	return len(table.records)*protocol.HeaderSize + len(table.records)*8
}

func (table *sessionTable) EncodeTrailer(destination []byte) (protocol.Checksum, error) {
	if len(destination) != table.TrailerSize() {
		return protocol.Checksum{}, ErrSessionEncoding
	}
	for _, busy := range table.busy {
		if busy {
			return protocol.Checksum{}, ErrSessionCapacity
		}
	}
	clear(destination)
	sessionsOffset := len(table.records) * protocol.HeaderSize
	for slot := range table.records {
		record := &table.records[slot]
		if record.Session == 0 {
			continue
		}
		header := record.Reply
		start := slot * protocol.HeaderSize
		if err := protocol.EncodeHeader(destination[start:start+protocol.HeaderSize], &header, table.config.ChecksumAlgorithm); err != nil || header.HeaderChecksum != record.Reply.HeaderChecksum {
			return protocol.Checksum{}, ErrSessionEncoding
		}
		binary.LittleEndian.PutUint64(destination[sessionsOffset+slot*8:], uint64(record.Session))
	}
	return protocol.ChecksumBytes(table.config.ChecksumAlgorithm, destination)
}

func (table *sessionTable) DecodeTrailer(source []byte, expectedChecksum protocol.Checksum) error {
	if len(source) != table.TrailerSize() {
		return ErrSessionEncoding
	}
	computed, err := protocol.ChecksumBytes(table.config.ChecksumAlgorithm, source)
	if err != nil || computed != expectedChecksum {
		return ErrSessionChecksum
	}
	decoded := make([]sessionRecord, len(table.records))
	sessionsOffset := len(decoded) * protocol.HeaderSize
	count := uint32(0)
	for slot := range decoded {
		headerBytes := source[slot*protocol.HeaderSize : (slot+1)*protocol.HeaderSize]
		session := protocol.Session(binary.LittleEndian.Uint64(source[sessionsOffset+slot*8:]))
		if session == 0 {
			if !allZeroBytes(headerBytes) {
				return ErrSessionEncoding
			}
			continue
		}
		header, reason := protocol.DecodeHeader(headerBytes, table.config.Group, table.config.MessageSizeMax, table.config.ActiveCount, table.config.ChecksumAlgorithm)
		if reason != protocol.RejectNone || table.validateOccupied(header, session) != nil {
			return ErrSessionEncoding
		}
		client := replyClient(&header)
		for prior := range slot {
			if decoded[prior].Session != 0 && replyClient(&decoded[prior].Reply) == client {
				return ErrSessionEncoding
			}
		}
		decoded[slot] = sessionRecord{Reply: header, Session: session}
		count++
	}
	copy(table.records, decoded)
	table.count = count
	clear(table.busy)
	for slot := range table.generations {
		table.generations[slot]++
	}
	return nil
}

func (table *sessionTable) lookup(client protocol.ClientID) (*sessionRecord, uint32, bool) {
	free := uint32(len(table.records))
	for slot := range table.records {
		record := &table.records[slot]
		if record.Session == 0 {
			if free == uint32(len(table.records)) && !table.busy[slot] {
				free = uint32(slot)
			}
			continue
		}
		if replyClient(&record.Reply) == client {
			return record, uint32(slot), true
		}
	}
	return nil, free, false
}

func (table *sessionTable) evictionSlot() uint32 {
	selected := uint32(0)
	for slot := uint32(1); slot < uint32(len(table.records)); slot++ {
		candidate := &table.records[slot]
		current := &table.records[selected]
		candidateOp := replyOp(&candidate.Reply)
		currentOp := replyOp(&current.Reply)
		candidateClient := replyClient(&candidate.Reply)
		currentClient := replyClient(&current.Reply)
		if candidateOp < currentOp || candidateOp == currentOp && bytes.Compare(candidateClient[:], currentClient[:]) < 0 {
			selected = slot
		}
	}
	return selected
}

func (table *sessionTable) validateOccupied(reply protocol.Header, session protocol.Session) error {
	if session == 0 || reply.Command != protocol.CommandReply || reply.Protocol != protocol.ProtocolVersion || reply.Group != table.config.Group {
		return ErrSessionEncoding
	}
	if reply.Size < protocol.HeaderSize || reply.Size > table.config.MessageSizeMax || uint32(reply.Size-protocol.HeaderSize) > table.config.ApplicationReplySizeMax {
		return ErrSessionEncoding
	}
	if reply.Release == 0 || uint8(reply.Author) >= table.config.ActiveCount || replyClient(&reply).IsZero() {
		return ErrSessionEncoding
	}
	if replyCommit(&reply) < protocol.Op(session) || replyOp(&reply) != replyCommit(&reply) || replyTimestamp(&reply) == 0 {
		return ErrSessionEncoding
	}
	var encoded [protocol.HeaderSize]byte
	copyOfReply := reply
	if err := protocol.EncodeHeader(encoded[:], &copyOfReply, table.config.ChecksumAlgorithm); err != nil || copyOfReply.HeaderChecksum != reply.HeaderChecksum {
		return ErrSessionEncoding
	}
	context := protocol.ValidationContext{
		Authenticated:           true,
		ReplicaSource:           true,
		Sender:                  reply.Author,
		ActiveCount:             table.config.ActiveCount,
		MemberCount:             table.config.ActiveCount,
		ApplicationReplySizeMax: table.config.ApplicationReplySizeMax,
		CurrentRelease:          reply.Release,
		Group:                   table.config.Group,
		MessageSizeMax:          table.config.MessageSizeMax,
		ChecksumAlgorithm:       table.config.ChecksumAlgorithm,
	}
	if protocol.ValidateSemantics(&reply, nil, context) != protocol.RejectNone {
		return ErrSessionEncoding
	}
	return nil
}

func replyRequestChecksum(header *protocol.Header) protocol.Checksum {
	var checksum protocol.Checksum
	copy(checksum[:], header.Fields[0:16])
	return checksum
}

func replyContext(header *protocol.Header) protocol.Checksum {
	var checksum protocol.Checksum
	copy(checksum[:], header.Fields[32:48])
	return checksum
}

func replyClient(header *protocol.Header) protocol.ClientID {
	var client protocol.ClientID
	copy(client[:], header.Fields[64:80])
	return client
}

func replyOp(header *protocol.Header) protocol.Op {
	return protocol.Op(binary.LittleEndian.Uint64(header.Fields[80:88]))
}

func replyCommit(header *protocol.Header) protocol.Op {
	return protocol.Op(binary.LittleEndian.Uint64(header.Fields[88:96]))
}

func replyTimestamp(header *protocol.Header) uint64 {
	return binary.LittleEndian.Uint64(header.Fields[96:104])
}

func replyRequest(header *protocol.Header) protocol.RequestNo {
	return protocol.RequestNo(binary.LittleEndian.Uint32(header.Fields[104:108]))
}
