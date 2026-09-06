# supervsr

## Contract corrections

The [errata](errata/README.md) record corrected protocol rules, recovery tradeoffs,
and source API compatibility changes.

## Verification

GitHub Actions runs all Go tests on Linux and macOS, static analysis, a separate
race-detection job, and the three checked-in TLA+ configurations. The short
FoundationDB campaign is mandatory in the ordinary test job but excluded from
race instrumentation because its initial full race run exceeded 15 minutes.
The controlled long campaign and the remaining suite also run with the race detector.
TLA+/TLC runs only in CI; model bounds do not establish refinement of the Go implementation.

The FoundationDB-inspired deterministic campaign exercises swizzle-clogging,
correlated reboots, overlapping network/clock/member stalls, and storage faults.
Each scenario checks acknowledged history, exact recovered execution, and fresh
client progress after healing. Disk-capacity tests distinguish failed growth
from writes to preallocated zones.

```sh
go test ./replication/sim -run '^TestFoundationDBFaultCampaign$' -count=1
VSR_SIM_SEED_START=3 VSR_SIM_SEED_COUNT=1 go test ./replication/sim \
  -run '^TestFoundationDBFaultCampaign$/^seed_3$/^overlapping_faults$' -count=1
```

The short campaign defaults to seeds 1–3; the long campaign defaults to seeds 1–2.
Scheduled and ordinary manually dispatched CI runs exercise both campaigns over
32 seeds across four jobs. Selecting `short_seed_sweep=true` runs only one short
campaign seed once, with no duplicate Go, race, or TLA jobs.
Failures report seed, scenario, simulation step, and member state; they are not
retried or skipped.

The short campaign stays below the first checkpoint. The long campaign uses a
checkpoint-backed, non-idempotent ledger and a separate bounded history oracle.
It crosses at least eight checkpoint boundaries and four journal wraps per seed,
isolates an active replica or standby beyond retained WAL, restarts the primary, crashes state sync
at a durability barrier, injects a suffix-write error, restarts every replica, and
checks fresh progress plus the complete counter/digest history. Separate tests
cover controlled one/two-active topologies and bounded client/session churn.

```sh
VSR_LONG_SEED_START=1 VSR_LONG_SEED_COUNT=2 go test ./replication/sim \
  -run '^TestLongDeterministicFaultCampaign$' -count=1 -v
go test ./replication -run '^TestFileWAL' -count=1
go test ./replication -run '^$' -bench '^BenchmarkFileWALAppendRead$' -benchtime=3x -benchmem
```

With `sim.Config.ControlledIO`, `Cluster.Step` advances protocol time/messages but
not I/O. `PendingIO` and `AdvanceIO` select individual effects and completion
delivery; events carry the replica incarnation to reject stale scheduling after
restart. WAL, reply, and superblock operations use the same physical transition
primitives in controlled and production worker modes. Storage errors are delivered
through completions, not converted to successful durability.
The supplied ledger callbacks are synchronous; arbitrary external state-machine
goroutines are not scheduled by the I/O controller.

The controller covers queued IOEngine work. Format/Open and synchronous
state-machine checkpoint-block calls remain immediate and fault-injectable, not
individually scheduled. FileStorage tests inject errors over real files; they do
not simulate physical power loss or prove a device's fsync contract. Process tests
rehearse fenced replacement, quiescent backup/restore, and release handoff/restart
with one executable, not mixed-version binary compatibility. Component benchmarks
and bounded resource checks are not production throughput or tail-latency SLOs.

Failure-model sources: [FoundationDB simulation documentation](https://apple.github.io/foundationdb/testing.html)
and [FoundationDB paper, §3](https://sigmodrecord.org/publications/sigmodRecord/2203/pdfs/08_fdb-zhou.pdf).