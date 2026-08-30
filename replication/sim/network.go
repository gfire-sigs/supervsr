package sim

import (
	"errors"
	"sync"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var ErrNetworkBackpressure = errors.New("simulation: network queue exhausted")

type packetKind uint8

const (
	packetReplica packetKind = iota + 1
	packetClient
)

type packet struct {
	frame      []byte
	client     protocol.ClientID
	sequence   uint64
	deliverAt  uint64
	from       protocol.ReplicaIndex
	to         protocol.ReplicaIndex
	kind       packetKind
	fromMember bool
}

type corruption struct {
	offset int
	mask   byte
	armed  bool
}

type misdirection struct {
	to    protocol.ReplicaIndex
	armed bool
}
type Network struct {
	mu          sync.Mutex
	pool        *protocol.FramePool
	memberCount uint8
	maximum     int
	now         uint64
	sequence    uint64
	delay       uint64
	nextDelay   uint64
	delayNext   bool
	drop        uint64
	duplicate   uint64
	corrupt     corruption
	misdirect   misdirection
	fault       error
	queue       []packet
	members     [replication.MembersMax]protocol.MemberID
	links       [replication.MembersMax][replication.MembersMax]bool
	linkDelay   [replication.MembersMax][replication.MembersMax]uint64
	replicas    [replication.MembersMax]func(*protocol.Frame) error
	clients     map[protocol.ClientID]func(protocol.ReplicaIndex, []byte)
}

func NewNetwork(memberCount uint8, messageSizeMax uint32, maximumPackets uint32) (*Network, error) {
	if memberCount == 0 || memberCount > replication.MembersMax || maximumPackets == 0 {
		return nil, replication.ErrInvalidConfiguration
	}
	pool, err := protocol.NewFramePool(maximumPackets, messageSizeMax)
	if err != nil {
		return nil, err
	}
	network := &Network{
		pool: pool, memberCount: memberCount, maximum: int(maximumPackets),
		queue: make([]packet, 0, int(maximumPackets)), clients: make(map[protocol.ClientID]func(protocol.ReplicaIndex, []byte)),
	}
	for from := range memberCount {
		network.members[from][15] = from + 1
		for to := range memberCount {
			network.links[from][to] = true
		}
	}
	return network, nil
}

func (network *Network) ReplicaBus(from protocol.ReplicaIndex) replication.MessageBus {
	return networkBus{network: network, from: from, fromMember: true}
}

func (network *Network) ClientBus() replication.MessageBus {
	return networkBus{network: network}
}

func (network *Network) RegisterReplica(index protocol.ReplicaIndex, submit func(*protocol.Frame) error) error {
	network.mu.Lock()
	defer network.mu.Unlock()
	if uint8(index) >= network.memberCount || submit == nil {
		return replication.ErrInvalidConfiguration
	}
	network.replicas[index] = submit
	return nil
}

func (network *Network) UnregisterReplica(index protocol.ReplicaIndex) {
	network.mu.Lock()
	defer network.mu.Unlock()
	if uint8(index) < network.memberCount {
		network.replicas[index] = nil
	}
}

func (network *Network) RegisterClient(id protocol.ClientID, handle func(protocol.ReplicaIndex, []byte)) error {
	network.mu.Lock()
	defer network.mu.Unlock()
	if id.IsZero() || handle == nil {
		return replication.ErrInvalidConfiguration
	}
	network.clients[id] = handle
	return nil
}

func (network *Network) Partition(left, right protocol.ReplicaIndex) error {
	return network.setLink(left, right, false)
}

func (network *Network) Heal(left, right protocol.ReplicaIndex) error {
	return network.setLink(left, right, true)
}

func (network *Network) PartitionDirected(from, to protocol.ReplicaIndex) error {
	return network.setDirectedLink(from, to, false)
}

func (network *Network) HealDirected(from, to protocol.ReplicaIndex) error {
	return network.setDirectedLink(from, to, true)
}

func (network *Network) Disconnect(index protocol.ReplicaIndex) error {
	network.mu.Lock()
	defer network.mu.Unlock()
	if uint8(index) >= network.memberCount {
		return replication.ErrInvalidConfiguration
	}
	for peer := range network.memberCount {
		if protocol.ReplicaIndex(peer) == index {
			continue
		}
		network.links[index][peer] = false
		network.links[peer][index] = false
	}
	return nil
}

func (network *Network) Reconnect(index protocol.ReplicaIndex) error {
	network.mu.Lock()
	defer network.mu.Unlock()
	if uint8(index) >= network.memberCount {
		return replication.ErrInvalidConfiguration
	}
	for peer := range network.memberCount {
		if protocol.ReplicaIndex(peer) == index {
			continue
		}
		network.links[index][peer] = true
		network.links[peer][index] = true
	}
	return nil
}

func (network *Network) SetLinkDelay(from, to protocol.ReplicaIndex, ticks uint64) error {
	network.mu.Lock()
	defer network.mu.Unlock()
	if uint8(from) >= network.memberCount || uint8(to) >= network.memberCount {
		return replication.ErrInvalidConfiguration
	}
	network.linkDelay[from][to] = ticks
	return nil
}

func (network *Network) DelayNext(ticks uint64) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.nextDelay = ticks
	network.delayNext = true
}

func (network *Network) MisdirectNext(to protocol.ReplicaIndex) error {
	network.mu.Lock()
	defer network.mu.Unlock()
	if uint8(to) >= network.memberCount {
		return replication.ErrInvalidConfiguration
	}
	network.misdirect = misdirection{to: to, armed: true}
	return nil
}

func (network *Network) SetDelay(ticks uint64) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.delay = ticks
}

func (network *Network) DropNext(count uint64) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.drop = count
}

func (network *Network) DuplicateNext(count uint64) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.duplicate = count
}

func (network *Network) CorruptNext(offset int, mask byte) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.corrupt = corruption{offset: offset, mask: mask, armed: true}
}

func (network *Network) ClearFault() {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.fault = nil
}

func (network *Network) Advance() {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.now = saturatingAdd(network.now, 1)
}

func (network *Network) DeliverReady() (int, error) {
	delivered := 0
	for {
		ready, err := network.DeliverOne()
		if err != nil {
			return delivered, err
		}
		if !ready {
			return delivered, nil
		}
		delivered++
	}
}

func (network *Network) DeliverOne() (bool, error) {
	network.mu.Lock()
	fault := network.fault
	network.mu.Unlock()
	if fault != nil {
		return false, fault
	}
	packet, replicaSubmit, clientHandle, ok := network.takeReady()
	if !ok {
		return false, nil
	}
	if replicaSubmit != nil {
		frame, err := network.pool.AcquireEncoded(packet.frame)
		if err == nil {
			if packet.fromMember {
				err = frame.BindReplica(network.members[packet.from], packet.from)
			} else {
				err = frame.BindClient()
			}
		}
		if err != nil {
			if frame != nil {
				frame.Release()
			}
			return false, err
		}
		if err := replicaSubmit(frame); err != nil {
			frame.Release()
			return false, err
		}
		return true, nil
	}
	if clientHandle != nil {
		clientHandle(packet.from, packet.frame)
	}
	return true, nil
}

func (network *Network) Pending() int {
	network.mu.Lock()
	defer network.mu.Unlock()
	return len(network.queue)
}

func (network *Network) setLink(left, right protocol.ReplicaIndex, connected bool) error {
	network.mu.Lock()
	defer network.mu.Unlock()
	if uint8(left) >= network.memberCount || uint8(right) >= network.memberCount || left == right {
		return replication.ErrInvalidConfiguration
	}
	network.links[left][right] = connected
	network.links[right][left] = connected
	return nil
}

func (network *Network) setDirectedLink(from, to protocol.ReplicaIndex, connected bool) error {
	network.mu.Lock()
	defer network.mu.Unlock()
	if uint8(from) >= network.memberCount || uint8(to) >= network.memberCount || from == to {
		return replication.ErrInvalidConfiguration
	}
	network.links[from][to] = connected
	return nil
}

func (network *Network) enqueue(from protocol.ReplicaIndex, fromMember bool, kind packetKind, to protocol.ReplicaIndex, client protocol.ClientID, message *protocol.Frame) {
	encoded, err := message.Bytes()
	if err != nil {
		return
	}
	network.mu.Lock()
	defer network.mu.Unlock()
	if kind == packetReplica && uint8(to) >= network.memberCount {
		return
	}
	if kind == packetReplica && network.misdirect.armed {
		to = network.misdirect.to
		network.misdirect = misdirection{}
	}
	if fromMember && kind == packetReplica && !network.links[from][to] {
		return
	}
	if network.drop != 0 {
		network.drop--
		return
	}
	copies := uint64(1)
	if network.duplicate != 0 {
		network.duplicate--
		copies++
	}
	delay := network.delay
	if fromMember {
		delay = saturatingAdd(delay, network.linkDelay[from][to])
	}
	if network.delayNext {
		delay = saturatingAdd(delay, network.nextDelay)
		network.nextDelay = 0
		network.delayNext = false
	}
	deliverAt := saturatingAdd(network.now, delay)
	for range copies {
		if len(network.queue) == network.maximum {
			network.fault = ErrNetworkBackpressure
			return
		}
		frame := append([]byte(nil), encoded...)
		if network.corrupt.armed {
			if network.corrupt.offset >= 0 && network.corrupt.offset < len(frame) {
				frame[network.corrupt.offset] ^= network.corrupt.mask
			}
			network.corrupt = corruption{}
		}
		network.sequence++
		network.queue = append(network.queue, packet{
			frame: frame, client: client, sequence: network.sequence, deliverAt: deliverAt,
			from: from, to: to, kind: kind, fromMember: fromMember,
		})
	}
}

func (network *Network) takeReady() (packet, func(*protocol.Frame) error, func(protocol.ReplicaIndex, []byte), bool) {
	network.mu.Lock()
	defer network.mu.Unlock()
	selected := -1
	for index := range network.queue {
		candidate := network.queue[index]
		if candidate.deliverAt > network.now {
			continue
		}
		if candidate.kind == packetReplica && candidate.fromMember && !network.links[candidate.from][candidate.to] {
			continue
		}
		if selected == -1 || candidate.sequence < network.queue[selected].sequence {
			selected = index
		}
	}
	if selected == -1 {
		return packet{}, nil, nil, false
	}
	selectedPacket := network.queue[selected]
	copy(network.queue[selected:], network.queue[selected+1:])
	network.queue = network.queue[:len(network.queue)-1]
	if selectedPacket.kind == packetReplica {
		return selectedPacket, network.replicas[selectedPacket.to], nil, true
	}
	return selectedPacket, nil, network.clients[selectedPacket.client], true
}

func saturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

type networkBus struct {
	network    *Network
	from       protocol.ReplicaIndex
	fromMember bool
}

func (bus networkBus) SendReplica(to protocol.ReplicaIndex, message *replication.Message) {
	bus.network.enqueue(bus.from, bus.fromMember, packetReplica, to, protocol.ClientID{}, message)
}

func (bus networkBus) SendClient(to protocol.ClientID, message *replication.Message) {
	bus.network.enqueue(bus.from, bus.fromMember, packetClient, 0, to, message)
}

func (bus networkBus) BroadcastReplicas(message *replication.Message) {
	for index := range bus.network.memberCount {
		to := protocol.ReplicaIndex(index)
		if bus.fromMember && to == bus.from {
			continue
		}
		bus.network.enqueue(bus.from, bus.fromMember, packetReplica, to, protocol.ClientID{}, message)
	}
}
