package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

// The fault/restoration schedule follows https://apple.github.io/foundationdb/testing.html
// and the healthy-recovery oracle in https://sigmodrecord.org/publications/sigmodRecord/2203/pdfs/08_fdb-zhou.pdf §3.
func TestFoundationDBFaultCampaign(t *testing.T) {
	start := campaignEnvironment(t, "VSR_SIM_SEED_START", 1)
	count := campaignEnvironment(t, "VSR_SIM_SEED_COUNT", 3)
	if count == 0 || count-1 > ^uint64(0)-start {
		t.Fatal("seed count must be positive and seed range must not overflow")
	}
	scenarios := []struct {
		name string
		run  func(*faultCampaign)
	}{
		{"swizzle_clog", (*faultCampaign).swizzle},
		{"correlated_reboot", (*faultCampaign).correlatedReboot},
		{"overlapping_faults", (*faultCampaign).overlap},
	}
	storageFaults := []struct {
		name   string
		effect FaultEffect
		kind   replication.IOKind
	}{
		{"read_error", FaultFail, replication.IORead},
		{"write_error", FaultFail, replication.IOWrite},
		{"sync_error", FaultFail, replication.IOSync},
		{"torn_read", FaultTornRead, replication.IORead},
		{"stale_read", FaultStaleRead, replication.IORead},
		{"corrupt_read", FaultCorruptRead, replication.IORead},
		{"misdirected_read", FaultMisdirectedRead, replication.IORead},
		{"torn_write", FaultTornWrite, replication.IOWrite},
		{"lost_write", FaultLostWrite, replication.IOWrite},
		{"corrupt_write", FaultCorruptWrite, replication.IOWrite},
		{"misdirected_write", FaultMisdirectedWrite, replication.IOWrite},
		{"delayed_write", FaultDelayedWrite, replication.IOWrite},
		{"torn_sync", FaultTornSync, replication.IOSync},
		{"lost_sync", FaultLostSync, replication.IOSync},
	}
	for _, fault := range storageFaults {
		scenarios = append(scenarios, struct {
			name string
			run  func(*faultCampaign)
		}{"storage_" + fault.name, func(campaign *faultCampaign) {
			campaign.storageFault(fault.effect, fault.kind)
		}})
	}
	for offset := range count {
		seed := start + offset
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			for _, scenario := range scenarios {
				t.Run(scenario.name, func(t *testing.T) {
					campaign := newFaultCampaign(t, seed)
					campaign.phase = scenario.name
					scenario.run(campaign)
					campaign.recover()
				})
			}
		})
	}
}

func campaignEnvironment(t *testing.T, name string, fallback uint64) uint64 {
	t.Helper()
	value, exists := os.LookupEnv(name)
	if !exists {
		return fallback
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, value, err)
	}
	return parsed
}

type faultCampaign struct {
	t              *testing.T
	cluster        *Cluster
	random         *rand.Rand
	seed           uint64
	step           int
	phase          string
	clients        []*replication.Client
	events         []*clientEvents
	submitted      [][][]byte
	acknowledged   map[string]Commit
	failedNode     *protocol.ReplicaIndex
	injectedErrors int
}

func newFaultCampaign(t *testing.T, seed uint64) *faultCampaign {
	t.Helper()
	random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	config := DefaultConfig(3 + uint8(random.IntN(int(replication.ActiveMax)-2)))
	config.StandbyCount = uint8(random.IntN(int(replication.StandbyMax) + 1))
	// Keep the complete application history below the first checkpoint. Checkpoint/wrap
	// and state-sync recovery have separate fixtures with checkpoint-aware machines.
	cluster, err := NewCluster(context.Background(), config, nil)
	if err != nil {
		t.Fatal(err)
	}
	campaign := &faultCampaign{
		t: t, cluster: cluster, random: random, seed: seed, phase: "warmup",
		acknowledged: make(map[string]Commit),
	}
	t.Cleanup(func() {
		if err := cluster.Close(context.Background()); err != nil && !t.Failed() {
			t.Errorf("seed=%d step=%d close: %v", seed, campaign.step, err)
		}
	})
	campaign.until(4_000, func() bool { return configuredMembersAgree(cluster, uint8(len(cluster.nodes))) })
	for range 2 {
		campaign.addClient()
	}
	campaign.until(4_000, func() bool { return campaign.registered() })
	campaign.pump(1)
	campaign.until(4_000, func() bool { return campaign.completed(1) })
	campaign.until(4_000, func() bool { return configuredMembersAgree(cluster, uint8(len(cluster.nodes))) })
	return campaign
}

func (campaign *faultCampaign) check(err error) {
	campaign.t.Helper()
	if err != nil {
		campaign.fail("%v", err)
	}
}

func (campaign *faultCampaign) fail(format string, args ...any) {
	campaign.t.Helper()
	campaign.t.Fatalf("seed=%d scenario=%s step=%d states=%v: %s", campaign.seed, campaign.phase, campaign.step,
		compactMemberSnapshots(campaign.cluster, uint8(len(campaign.cluster.nodes))), fmt.Sprintf(format, args...))
}

func (campaign *faultCampaign) addClient() {
	id := byte(len(campaign.clients) + 1)
	events := &campaignEvents{campaign: campaign}
	client, err := campaign.cluster.AddClient(protocol.ClientID{id}, events)
	campaign.check(err)
	campaign.clients = append(campaign.clients, client)
	campaign.events = append(campaign.events, &events.clientEvents)
	campaign.submitted = append(campaign.submitted, nil)
	campaign.check(client.Register())
}

func (campaign *faultCampaign) registered() bool {
	for _, events := range campaign.events {
		if events.replyCount() == 0 {
			return false
		}
	}
	return true
}

func (campaign *faultCampaign) completed(count int) bool {
	for _, events := range campaign.events {
		if events.replyCount() != count+1 {
			return false
		}
	}
	return true
}

func (campaign *faultCampaign) pump(limit int) {
	for index, client := range campaign.clients {
		sent := len(campaign.submitted[index])
		if sent == limit || campaign.events[index].replyCount() != sent+1 {
			continue
		}
		body := []byte(fmt.Sprintf("seed=%d/client=%d/request=%d", campaign.seed, index, sent))
		campaign.check(client.Submit(protocol.OperationApplicationMin, body))
		campaign.submitted[index] = append(campaign.submitted[index], body)
	}
}

func (campaign *faultCampaign) tick() {
	campaign.step++
	err := campaign.cluster.Step()
	if err != nil {
		var nodeError *NodeError
		if campaign.failedNode == nil || !errors.As(err, &nodeError) || nodeError.Index != *campaign.failedNode || !errors.Is(err, ErrInjectedFault) {
			campaign.fail("unexpected replica error: %v", err)
		}
		campaign.injectedErrors++
		campaign.check(campaign.cluster.Crash(context.Background(), nodeError.Index))
		campaign.failedNode = nil
	}
	campaign.observeReplies()
}

func (campaign *faultCampaign) until(limit int, reached func() bool) {
	for range limit {
		if reached() {
			return
		}
		campaign.tick()
	}
	campaign.fail("condition not reached after %d steps", limit)
}

func (campaign *faultCampaign) traffic(steps int) {
	for range steps {
		campaign.pump(6)
		campaign.tick()
	}
}

func (campaign *faultCampaign) primary() protocol.ReplicaIndex {
	for index := range campaign.cluster.config.ActiveCount {
		snapshot, ok := campaign.cluster.Snapshot(protocol.ReplicaIndex(index))
		if ok && snapshot.Status == replication.StatusNormal && snapshot.Primary == protocol.ReplicaIndex(index) {
			return protocol.ReplicaIndex(index)
		}
	}
	campaign.fail("normal primary missing")
	return 0
}

type campaignEvents struct {
	clientEvents
	campaign *faultCampaign
}

func (events *campaignEvents) Reply(reply replication.ClientReply) {
	events.clientEvents.Reply(reply)
	if reply.Operation < protocol.OperationApplicationMin {
		return
	}
	key := string(reply.Body)
	if _, exists := events.campaign.acknowledged[key]; exists {
		events.campaign.fail("duplicate successful reply for %q", key)
	}
	// Capture evidence at the callback, before a later event in this Step can crash its source.
	for _, node := range events.campaign.cluster.nodes {
		machine, ok := node.machine.(*Machine)
		if !ok {
			continue
		}
		for _, commit := range machine.Commits() {
			if commit.Operation == reply.Operation && bytes.Equal(commit.Body, reply.Body) {
				events.campaign.acknowledged[key] = commit
				return
			}
		}
	}
	events.campaign.fail("successful reply %q lacks application execution", key)
}

func (campaign *faultCampaign) observeReplies() {
	for index, events := range campaign.events {
		if events.evicted {
			campaign.fail("client %d evicted", index)
		}
		for replyIndex, reply := range events.replies {
			if replyIndex == 0 {
				continue
			}
			if replyIndex > len(campaign.submitted[index]) || reply.Operation != protocol.OperationApplicationMin || !bytes.Equal(reply.Body, campaign.submitted[index][replyIndex-1]) {
				campaign.fail("client %d unexpected reply %d: %q", index, replyIndex, reply.Body)
			}
		}
	}
	for _, node := range campaign.cluster.nodes {
		machine, ok := node.machine.(*Machine)
		if !ok {
			continue
		}
		for _, commit := range machine.Commits() {
			key := string(commit.Body)
			if expected, exists := campaign.acknowledged[key]; exists {
				if commit.Op != expected.Op || commit.Timestamp != expected.Timestamp || commit.Operation != expected.Operation || commit.Release != expected.Release {
					campaign.fail("acknowledged request %q changed: was op=%d timestamp=%d, now op=%d timestamp=%d", key, expected.Op, expected.Timestamp, commit.Op, commit.Timestamp)
				}
			}
		}
	}
}

type campaignLink struct{ from, to protocol.ReplicaIndex }

func (campaign *faultCampaign) swizzle() {
	network := campaign.cluster.Network()
	members := len(campaign.cluster.nodes)
	selected := campaign.random.Perm(members)[:1+campaign.random.IntN(min(3, members))]
	chosen := make([]bool, members)
	for _, index := range selected {
		chosen[index] = true
	}
	var links []campaignLink
	for from := range members {
		for to := range members {
			if from != to && (chosen[from] || chosen[to]) {
				links = append(links, campaignLink{protocol.ReplicaIndex(from), protocol.ReplicaIndex(to)})
			}
		}
	}
	campaign.random.Shuffle(len(links), func(i, j int) { links[i], links[j] = links[j], links[i] })
	for _, link := range links {
		campaign.check(network.PartitionDirected(link.from, link.to))
		campaign.traffic(1 + campaign.random.IntN(4))
	}
	campaign.traffic(40 + campaign.random.IntN(40))
	campaign.random.Shuffle(len(links), func(i, j int) { links[i], links[j] = links[j], links[i] })
	for _, link := range links {
		campaign.check(network.HealDirected(link.from, link.to))
		campaign.traffic(1 + campaign.random.IntN(4))
	}
}

func (campaign *faultCampaign) correlatedReboot() {
	members := len(campaign.cluster.nodes)
	// Loss of availability is intentional; no durable disk is destroyed. Exercise
	// one machine, a shared failure domain, then a whole-cluster power cycle.
	for _, count := range []int{1, max(2, members/2), members} {
		order := campaign.random.Perm(members)[:count]
		for _, index := range order {
			campaign.check(campaign.cluster.Crash(context.Background(), protocol.ReplicaIndex(index)))
		}
		campaign.traffic(20 + campaign.random.IntN(40))
		campaign.random.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		for _, index := range order {
			campaign.check(campaign.cluster.Restart(context.Background(), protocol.ReplicaIndex(index)))
			campaign.traffic(1 + campaign.random.IntN(5))
		}
		campaign.until(6_000, func() bool { return configuredMembersAgree(campaign.cluster, uint8(members)) })
	}
}

func (campaign *faultCampaign) storageFault(effect FaultEffect, kind replication.IOKind) {
	victim := campaign.primary()
	storage := campaign.cluster.Storage(victim)
	if kind == replication.IORead {
		campaign.check(campaign.cluster.Crash(context.Background(), victim))
	}
	campaign.check(storage.Arm(StorageFault{At: storage.NextOperation(), Kind: kind, Effect: effect, Prefix: 17, Target: 4096, Mask: 0x80}))
	campaign.failedNode = &victim
	if kind == replication.IORead {
		err := campaign.cluster.Restart(context.Background(), victim)
		if err != nil {
			if !errors.Is(err, ErrInjectedFault) {
				campaign.fail("restart under injected read fault: %v", err)
			}
			campaign.injectedErrors++
			storage.Crash()
		}
	} else {
		for tries := 0; storage.FaultPending() && tries < 1_000; tries++ {
			campaign.traffic(1)
		}
	}
	if storage.FaultPending() {
		campaign.fail("storage fault %d kind %d was not exercised", effect, kind)
	}
	// Only one member has a bad disk. Never combine lost barriers/corrupt writes
	// with simultaneous destruction of the healthy durable replicas.
	if _, live := campaign.cluster.Snapshot(victim); live {
		if storage.PendingWrites() != 0 {
			campaign.check(storage.ReleaseDelayedWrite(campaign.random.IntN(storage.PendingWrites())))
		}
		campaign.check(campaign.cluster.Crash(context.Background(), victim))
	}
	campaign.failedNode = nil
	campaign.traffic(30)
	campaign.check(campaign.cluster.Restart(context.Background(), victim))
}

func (campaign *faultCampaign) overlap() {
	cluster := campaign.cluster
	victim := campaign.primary()
	slow := protocol.ReplicaIndex((int(victim) + 1) % int(cluster.config.ActiveCount))
	campaign.check(cluster.Pause(slow, 25))
	for index := range cluster.nodes {
		clock := cluster.MemberClock(protocol.ReplicaIndex(index))
		campaign.check(clock.SetDrift(int64(campaign.random.IntN(20_001) - 10_000)))
		jump := time.Duration(campaign.random.IntN(201)-100) * time.Millisecond
		if jump < 0 && uint64(-jump) > clock.Now().Wall {
			jump = -time.Duration(clock.Now().Wall / 2)
		}
		campaign.check(clock.JumpWall(jump))
		campaign.check(clock.JumpMonotonic(time.Duration(campaign.random.IntN(10)) * time.Millisecond))
	}
	cluster.MemberClock(slow).Freeze(true)
	cluster.SetAllClocksSynchronized(false)
	campaign.check(cluster.Storage(victim).Arm(StorageFault{At: cluster.Storage(victim).NextOperation(), Effect: FaultFail}))
	campaign.failedNode = &victim
	cluster.Network().SetDelay(uint64(1 + campaign.random.IntN(3)))
	campaign.check(cluster.Network().SetLinkDelay(victim, slow, 9))
	cluster.Network().DropNext(2)
	cluster.Network().DuplicateNext(3)
	cluster.Network().DelayNext(11)
	cluster.Network().CorruptNext(0, 0x80)
	campaign.check(cluster.Network().MisdirectNext(slow))
	campaign.swizzle()
	// Restore clock admission before waiting for an I/O fault that needs traffic.
	campaign.healClocks()
	for tries := 0; campaign.failedNode != nil && tries < 2_000; tries++ {
		campaign.traffic(1)
	}
	if campaign.injectedErrors == 0 {
		campaign.fail("overlapping I/O fault did not fail-stop its member")
	}
}

func (campaign *faultCampaign) healClocks() {
	now := campaign.cluster.Clock().Now()
	for index := range campaign.cluster.nodes {
		clock := campaign.cluster.MemberClock(protocol.ReplicaIndex(index))
		clock.Freeze(false)
		campaign.check(clock.SetDrift(0))
		clock.SetTime(now.Wall, max(now.Monotonic, clock.Now().Monotonic))
		clock.SetSynchronized(true)
	}
}

func (campaign *faultCampaign) recover() {
	campaign.phase += "/healthy_recovery"
	cluster := campaign.cluster
	campaign.healClocks()
	cluster.Network().HealAll()
	for index := range cluster.nodes {
		member := protocol.ReplicaIndex(index)
		cluster.Storage(member).ClearFault()
		campaign.check(cluster.Storage(member).SetCapacityLimit(0))
		if _, live := cluster.Snapshot(member); !live {
			campaign.check(cluster.Restart(context.Background(), member))
		}
		campaign.check(cluster.Pause(member, 0))
	}
	campaign.failedNode = nil
	for tries := 0; tries < 8_000 && !campaign.completed(6); tries++ {
		campaign.traffic(1)
	}
	if !campaign.completed(6) {
		campaign.fail("previously registered clients did not finish their six requests")
	}
	campaign.until(6_000, func() bool { return configuredMembersAgree(cluster, uint8(len(cluster.nodes))) })
	campaign.addClient()
	campaign.until(4_000, campaign.registered)
	fresh := len(campaign.clients) - 1
	body := []byte(fmt.Sprintf("seed=%d/fresh-client", campaign.seed))
	campaign.check(campaign.clients[fresh].Submit(protocol.OperationApplicationMin, body))
	campaign.submitted[fresh] = append(campaign.submitted[fresh], body)
	campaign.until(4_000, func() bool { return campaign.events[fresh].replyCount() == 2 })
	campaign.until(6_000, func() bool { return configuredMembersAgree(cluster, uint8(len(cluster.nodes))) })
	campaign.observeReplies()
	if len(campaign.acknowledged) != 13 {
		campaign.fail("acknowledged history contains %d requests, want 13", len(campaign.acknowledged))
	}
	for index, node := range cluster.nodes {
		seen := make(map[string]bool)
		for _, commit := range node.machine.(*Machine).Commits() {
			if commit.Operation < protocol.OperationApplicationMin {
				continue
			}
			key := string(commit.Body)
			if _, exists := campaign.acknowledged[key]; !exists || seen[key] {
				campaign.fail("member %d has unacknowledged or duplicate application execution %q", index, key)
			}
			seen[key] = true
		}
		if len(seen) != len(campaign.acknowledged) {
			campaign.fail("member %d recovered %d of %d acknowledged operations", index, len(seen), len(campaign.acknowledged))
		}
	}
	campaign.check(cluster.CheckInvariants())
}

func TestStorageCapacityPreservesPreallocatedZones(t *testing.T) {
	campaign := newFaultCampaign(t, 1)
	campaign.phase = "fixed_zone_disk_full"
	for index := range campaign.cluster.nodes {
		storage := campaign.cluster.Storage(protocol.ReplicaIndex(index))
		size, err := storage.Size()
		campaign.check(err)
		campaign.check(storage.SetCapacityLimit(size))
		if err := storage.Resize(size + 4096); !errors.Is(err, syscall.ENOSPC) {
			campaign.fail("member %d growth at capacity: %v", index, err)
		}
		after, err := storage.Size()
		campaign.check(err)
		if after != size {
			campaign.fail("failed growth changed size from %d to %d", size, after)
		}
	}
	// WAL and reply-zone writes must still work when no space remains for growth.
	for tries := 0; tries < 4_000 && !campaign.completed(6); tries++ {
		campaign.traffic(1)
	}
	if !campaign.completed(6) {
		campaign.fail("preallocated-zone requests stalled at disk capacity")
	}
	campaign.correlatedReboot()
	campaign.recover()
}
