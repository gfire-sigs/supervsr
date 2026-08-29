package replication

import (
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var (
	ErrClientState        = errors.New("replication: invalid client state")
	ErrClientInFlight     = errors.New("replication: client request already in flight")
	ErrClientBodyTooLarge = errors.New("replication: client request body exceeds negotiated limit")
)

type ClientState uint8

const (
	ClientUnregistered ClientState = iota
	ClientRegistered
	ClientEvicted
	ClientClosed
)

type ClientReply struct {
	Operation protocol.Operation
	Request   protocol.RequestNo
	Body      []byte
}

type ClientEvents interface {
	Reply(reply ClientReply)
	Evicted(reason protocol.EvictionReason)
}

type ClientConfig struct {
	Group          protocol.GroupID
	ID             protocol.ClientID
	Release        protocol.Release
	ActiveCount    uint8
	MessageSizeMax uint32
	Process        ProcessConfig
}

type clientInflight struct {
	frame     *Message
	header    protocol.Header
	checksum  protocol.Checksum
	request   protocol.RequestNo
	operation protocol.Operation
}

type Client struct {
	config ClientConfig
	bus    MessageBus
	clock  Clock
	events ClientEvents
	frames *protocol.FramePool
	random DeterministicRandom

	state      ClientState
	view       protocol.View
	session    protocol.Session
	next       protocol.RequestNo
	parent     protocol.Checksum
	batchLimit uint32
	inflight   clientInflight
	request    Timeout
	ping       Timeout
	lastPing   uint64
	rtt        [ActiveMax]time.Duration
	rttValid   uint8
}

func NewClient(config ClientConfig, bus MessageBus, clock Clock, entropy io.Reader, events ClientEvents) (*Client, error) {
	if config.Group.IsZero() || config.ID.IsZero() || config.Release == 0 || config.ActiveCount == 0 || config.ActiveCount > ActiveMax {
		return nil, ErrInvalidConfiguration
	}
	if bus == nil || clock == nil || entropy == nil || events == nil || config.MessageSizeMax < protocol.HeaderSize+256 {
		return nil, ErrInvalidConfiguration
	}
	if err := validateClientProcess(config.Process); err != nil {
		return nil, err
	}
	frames, err := protocol.NewFramePool(2, config.MessageSizeMax)
	if err != nil {
		return nil, err
	}
	var seedBytes [8]byte
	if _, err := io.ReadFull(entropy, seedBytes[:]); err != nil {
		return nil, err
	}
	requestPeriod := time.Duration(config.Process.RTTMultiplier) * config.Process.InitialRTT
	request, err := NewTimeout(requestPeriod, config.Process.Tick, 0)
	if err != nil {
		return nil, err
	}
	ping, err := NewTimeout(30*time.Second, config.Process.Tick, 0)
	if err != nil {
		return nil, err
	}
	ping.Start()
	return &Client{
		config: config, bus: bus, clock: clock, events: events, frames: frames,
		random: NewDeterministicRandom(binary.LittleEndian.Uint64(seedBytes[:])), state: ClientUnregistered,
		request: request, ping: ping,
	}, nil
}

func validateClientProcess(config ProcessConfig) error {
	if config.Tick <= 0 || config.Tick >= 50*time.Millisecond {
		return ErrInvalidConfiguration
	}
	if config.InitialRTT < config.Tick || config.MaximumRTT < config.InitialRTT || config.RTTMultiplier == 0 {
		return ErrInvalidConfiguration
	}
	if config.BackoffMin <= 0 || config.BackoffMax < config.BackoffMin {
		return ErrInvalidConfiguration
	}
	if config.InitialRTT > time.Duration((1<<63-1)/int64(config.RTTMultiplier)) {
		return ErrInvalidConfiguration
	}
	return nil
}

func (client *Client) Register() error {
	if client.state != ClientUnregistered || client.inflight.frame != nil {
		return ErrClientState
	}
	var body [256]byte
	return client.send(protocol.OperationRegister, 0, body[:])
}

func (client *Client) Submit(operation protocol.Operation, body []byte) error {
	if client.state != ClientRegistered {
		return ErrClientState
	}
	if client.inflight.frame != nil {
		return ErrClientInFlight
	}
	if len(body) > int(client.batchLimit) {
		return ErrClientBodyTooLarge
	}
	if operation < protocol.OperationApplicationMin && operation != protocol.OperationNoop && operation != protocol.OperationReconfigure {
		return ErrClientState
	}
	if client.next == ^protocol.RequestNo(0) {
		return ErrClientState
	}
	client.next++
	if err := client.send(operation, client.next, body); err != nil {
		client.next--
		return err
	}
	return nil
}

func (client *Client) send(operation protocol.Operation, request protocol.RequestNo, body []byte) error {
	message, err := client.frames.Acquire(uint32(len(body)))
	if err != nil {
		return err
	}
	messageBody, err := message.Body()
	if err != nil {
		message.Release()
		return err
	}
	copy(messageBody, body)
	header := protocol.Header{Group: client.config.Group, View: client.view, Release: client.config.Release, Protocol: protocol.ProtocolVersion, Command: protocol.CommandRequest, Author: 0}
	copy(header.Fields[:16], client.parent[:])
	copy(header.Fields[32:48], client.config.ID[:])
	binary.LittleEndian.PutUint64(header.Fields[48:56], uint64(client.session))
	binary.LittleEndian.PutUint32(header.Fields[64:68], uint32(request))
	header.Fields[68] = byte(operation)
	if err := message.Seal(&header); err != nil {
		message.Release()
		return err
	}
	client.inflight = clientInflight{frame: message, header: header, checksum: header.HeaderChecksum, request: request, operation: operation}
	client.request.Start()
	client.routeRequest(message)
	return nil
}

func (client *Client) routeRequest(message *Message) {
	primary := protocol.ReplicaIndex(uint32(client.view) % uint32(client.config.ActiveCount))
	client.bus.SendReplica(primary, message)
	if client.config.ActiveCount == 1 {
		return
	}
	choice := uint8(client.random.Uniform(uint64(client.config.ActiveCount - 1)))
	backup := choice
	if backup >= uint8(primary) {
		backup++
	}
	client.bus.SendReplica(protocol.ReplicaIndex(backup), message)
}

func (client *Client) HandleFrame(sender protocol.ReplicaIndex, frame []byte) {
	if client.state == ClientClosed || uint8(sender) >= client.config.ActiveCount {
		return
	}
	header, body, reason := protocol.DecodeFrame(frame, client.config.Group, client.config.MessageSizeMax, client.config.ActiveCount)
	if reason != protocol.RejectNone {
		return
	}
	context := protocol.ValidationContext{
		Authenticated: true, Sender: sender, ActiveCount: client.config.ActiveCount, MemberCount: client.config.ActiveCount,
		PipelineMax: 15, ReleaseHistoryMax: 1, ApplicationBatchSizeMax: client.config.MessageSizeMax - protocol.HeaderSize,
		ApplicationReplySizeMax: client.config.MessageSizeMax - protocol.HeaderSize, RepairRequestsMax: 1,
		CurrentRelease: protocol.Release(^uint32(0)), ClientReleaseMin: 1, Group: client.config.Group, MessageSizeMax: client.config.MessageSizeMax,
	}
	if protocol.ValidateSemantics(&header, body, context) != protocol.RejectNone {
		return
	}
	switch header.Command {
	case protocol.CommandReply:
		client.handleReply(header, body)
	case protocol.CommandEviction:
		client.handleEviction(header)
	case protocol.CommandClientPong:
		client.handlePong(sender, header)
	}
}

func (client *Client) handleReply(header protocol.Header, body []byte) {
	if client.inflight.frame == nil || header.Release != client.config.Release {
		return
	}
	if replyClient(&header) != client.config.ID || protocol.Operation(header.Fields[108]) != client.inflight.operation || replyRequest(&header) != client.inflight.request {
		return
	}
	if protocol.Checksum(header.Fields[:16]) != client.inflight.checksum || replyOp(&header) != replyCommit(&header) {
		return
	}
	operation := client.inflight.operation
	request := client.inflight.request
	if operation == protocol.OperationRegister {
		if len(body) != 64 || !zeroBytes(body[4:]) {
			return
		}
		limit := binary.LittleEndian.Uint32(body[:4])
		if limit == 0 || limit > client.config.MessageSizeMax-protocol.HeaderSize {
			return
		}
		client.session = protocol.Session(replyCommit(&header))
		client.batchLimit = limit
		client.state = ClientRegistered
	}
	client.parent = replyContext(&header)
	client.view = max(client.view, header.View)
	client.clearInflight()
	client.events.Reply(ClientReply{Operation: operation, Request: request, Body: body})
}

func (client *Client) handleEviction(header protocol.Header) {
	if protocol.ClientID(header.Fields[:16]) != client.config.ID || header.View < client.view {
		return
	}
	client.view = header.View
	client.state = ClientEvicted
	client.clearInflight()
	client.events.Evicted(protocol.EvictionReason(header.Fields[127]))
}

func (client *Client) handlePong(sender protocol.ReplicaIndex, header protocol.Header) {
	ping := binary.LittleEndian.Uint64(header.Fields[:8])
	now := client.clock.Now().Monotonic
	if ping != client.lastPing || ping > now {
		return
	}
	client.view = max(client.view, header.View)
	client.rtt[sender] = time.Duration(now - ping)
	client.rttValid |= 1 << uint8(sender)
}

func (client *Client) Tick() error {
	if client.state == ClientClosed {
		return ErrClientState
	}
	pingFired, err := client.ping.Tick()
	if err != nil {
		return err
	}
	if pingFired {
		client.sendPing()
		client.ping.Reset()
	}
	if client.inflight.frame == nil {
		return nil
	}
	fired, err := client.request.Tick()
	if err != nil {
		return err
	}
	if !fired {
		return nil
	}
	client.routeRequest(client.inflight.frame)
	if err := client.request.Backoff(client.config.Process, client.measuredRTT(), &client.random); err != nil {
		return err
	}
	return nil
}

func (client *Client) sendPing() {
	message, err := client.frames.Acquire(0)
	if err != nil {
		return
	}
	client.lastPing = max(uint64(1), client.clock.Now().Monotonic)
	header := protocol.Header{Group: client.config.Group, View: client.view, Release: client.config.Release, Protocol: protocol.ProtocolVersion, Command: protocol.CommandClientPing, Author: 0}
	copy(header.Fields[:16], client.config.ID[:])
	binary.LittleEndian.PutUint64(header.Fields[16:24], client.lastPing)
	binary.LittleEndian.PutUint64(header.Fields[24:32], uint64(client.session))
	if message.Seal(&header) == nil {
		for member := range client.config.ActiveCount {
			client.bus.SendReplica(protocol.ReplicaIndex(member), message)
		}
	}
	message.Release()
}

func (client *Client) measuredRTT() time.Duration {
	var samples [ActiveMax]time.Duration
	count := 0
	for member := range client.config.ActiveCount {
		if client.rttValid&(1<<member) == 0 {
			continue
		}
		samples[count] = client.rtt[member]
		count++
	}
	if count == 0 {
		return client.config.Process.InitialRTT
	}
	for index := 1; index < count; index++ {
		value := samples[index]
		position := index
		for position > 0 && value < samples[position-1] {
			samples[position] = samples[position-1]
			position--
		}
		samples[position] = value
	}
	return samples[count/2]
}

func (client *Client) clearInflight() {
	client.request.Stop()
	if client.inflight.frame != nil {
		client.inflight.frame.Release()
	}
	client.inflight = clientInflight{}
}

func (client *Client) Close() {
	if client.state == ClientClosed {
		return
	}
	client.clearInflight()
	client.ping.Stop()
	client.state = ClientClosed
}

func (client *Client) State() ClientState            { return client.state }
func (client *Client) View() protocol.View           { return client.view }
func (client *Client) Session() protocol.Session     { return client.session }
func (client *Client) BatchLimit() uint32            { return client.batchLimit }
func (client *Client) RequestNo() protocol.RequestNo { return client.next }

func zeroBytes(value []byte) bool {
	for _, octet := range value {
		if octet != 0 {
			return false
		}
	}
	return true
}
