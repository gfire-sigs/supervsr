package sim

import (
	"context"
	"fmt"
	"testing"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type controlledReplacementClient struct {
	world  *ledgerWorld
	client *replication.Client
	count  uint64
	last   replication.ReplacementReply
}

func (client *controlledReplacementClient) Reply(reply replication.ClientReply) {
	client.count++
	client.last = replication.ReplacementReply{View: client.client.View(), Operation: reply.Operation, Request: reply.Request}
}

func (client *controlledReplacementClient) Evicted(reason protocol.EvictionReason) {
	client.world.fail("replacement client evicted: %v", reason)
}

func (client *controlledReplacementClient) wait(ctx context.Context, previous uint64, err error) (replication.ReplacementReply, error) {
	if err != nil {
		return replication.ReplacementReply{}, err
	}
	for range 20_000 {
		if err := ctx.Err(); err != nil {
			return replication.ReplacementReply{}, err
		}
		if client.count != previous {
			return client.last, nil
		}
		client.world.step()
	}
	return replication.ReplacementReply{}, fmt.Errorf("simulation: replacement client did not progress")
}

func (client *controlledReplacementClient) Register(ctx context.Context) (replication.ReplacementReply, error) {
	before := client.count
	return client.wait(ctx, before, client.client.Register())
}

func (client *controlledReplacementClient) Noop(ctx context.Context) (replication.ReplacementReply, error) {
	before := client.count
	return client.wait(ctx, before, client.client.Submit(protocol.OperationNoop, nil))
}

type controlledReplacementFence struct {
	cluster  *Cluster
	member   protocol.ReplicaIndex
	old      *Storage
	verified bool
}

func (fence *controlledReplacementFence) VerifyReplacementFence(ctx context.Context, input replication.ReplacementFenceInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fence.cluster.nodes[fence.member].replica != nil || fence.cluster.stores[fence.member] != fence.old {
		return replication.ErrReplacementNotFenced
	}
	if input.Group != fence.cluster.config.Group || input.Member != fence.cluster.members[fence.member] || input.ConfigurationChecksum != fence.cluster.config.Cluster.Fingerprint() {
		return replication.ErrReplacementNotFenced
	}
	fence.verified = true
	return nil
}

type fencedReplacementStorage struct {
	*Storage
	fence *controlledReplacementFence
}

func (storage *fencedReplacementStorage) WriteAt(buffer []byte, offset uint64) error {
	if !storage.fence.verified {
		return replication.ErrReplacementNotFenced
	}
	return storage.Storage.WriteAt(buffer, offset)
}

func (storage *fencedReplacementStorage) Resize(size uint64) error {
	if !storage.fence.verified {
		return replication.ErrReplacementNotFenced
	}
	return storage.Storage.Resize(size)
}

func TestControlledReplacementPreservesCheckpointHistory(t *testing.T) {
	world := newLedgerWorld(t, 61, DefaultConfig(3))
	client := world.addClient(1)
	world.increments(128, client)
	world.final()
	world.phase = "fenced_replacement"
	victim := world.primary()
	world.check(world.cluster.Crash(t.Context(), victim))
	old := world.cluster.Storage(victim)
	oldChecksum := protocol.ChecksumBytes(old.DurableBytes())
	fence := &controlledReplacementFence{cluster: world.cluster, member: victim, old: old}
	fresh := &fencedReplacementStorage{Storage: NewStorage(), fence: fence}
	adapter := &controlledReplacementClient{world: world}
	var err error
	adapter.client, err = world.cluster.AddClient(protocol.ClientID{2}, adapter)
	world.check(err)
	config := world.cluster.nodes[victim].config
	world.check(replication.ReplaceLostReplica(t.Context(), replication.ReplacementConfig{
		Group: config.Group, Membership: config.Membership, Cluster: config.Cluster,
		CurrentRelease: config.CurrentRelease, ConfigurationChecksum: config.Cluster.Fingerprint(),
	}, replication.ReplacementDependencies{Storage: fresh, Client: adapter, Fence: fence}))
	if !fence.verified || protocol.ChecksumBytes(old.DurableBytes()) != oldChecksum {
		world.fail("replacement bypassed fencing or rewrote the lost image")
	}
	world.cluster.stores[victim] = fresh.Storage
	world.check(world.cluster.Restart(t.Context(), victim))
	world.final()
	world.increments(16, client)
	world.final()
}
