package sim

import (
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication"
	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestControlledIODoesNotAdvanceWithoutScheduler(t *testing.T) {
	world := newLedgerWorld(t, 41, DefaultConfig(1))
	client := world.addClient(1)
	world.submit(client)
	world.check(world.cluster.Step())
	count := world.cluster.PendingIO(world.events)
	if count == 0 {
		t.Fatal("request did not submit controlled IO")
	}
	before := world.history.Completed()
	for range 5 {
		world.check(world.cluster.Step())
	}
	if world.history.Completed() != before {
		t.Fatal("unscheduled IO produced a successful application reply")
	}
	world.until(20_000, func() bool { return client.pending == 0 })
	world.final()
}

func TestClientCloseRefusesInflightWithoutLosingRouting(t *testing.T) {
	config := DefaultConfig(1)
	config.Cluster.ClientsMax = 1
	config.ClientProcessesMax = 1
	world := newLedgerWorld(t, 42, config)
	client := world.addClient(1)
	world.submit(client)
	if err := world.cluster.CloseClient(client.id); !errors.Is(err, replication.ErrRequestInFlight) {
		t.Fatalf("close in-flight client: %v", err)
	}
	world.until(20_000, func() bool { return client.pending == 0 })
	world.check(world.cluster.CloseClient(client.id))
	if world.cluster.Resources().ClientProcesses != 0 || len(world.cluster.network.clients) != 0 {
		t.Fatal("closed client retained process or routing ownership")
	}
	next := world.addClient(2)
	world.increments(1, next)
	world.final()
}

func TestControlledIOScheduleReplaysDeterministically(t *testing.T) {
	var trace protocol.Checksum
	var steps int
	for attempt := range 2 {
		world := newLedgerWorld(t, 51, DefaultConfig(2))
		client := world.addClient(1)
		world.increments(48, client)
		world.final()
		if attempt == 0 {
			trace, steps = world.trace, world.steps
		} else if world.trace != trace || world.steps != steps {
			t.Fatalf("same seed changed IO schedule: trace=%s want=%s steps=%d want=%d", world.trace, trace, world.steps, steps)
		}
	}
}
