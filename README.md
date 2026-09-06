# supervsr

## Contract corrections

The [errata](errata/README.md) record corrected protocol rules, recovery tradeoffs,
and source API compatibility changes.

## Verification

GitHub Actions runs all Go tests on Linux and macOS, static analysis, a separate
race-detection job, and the three checked-in TLA+ configurations. The seed
campaign is mandatory in the ordinary test job but excluded from race
instrumentation: the initial full race run exceeded 15 minutes. Simulator
primitive tests and the remaining suite still run with the race detector.
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

Default runs cover seeds 1–3. Scheduled and manually dispatched CI runs sweep
32 seeds across four jobs and print the seed range for replay. Failures report
seed, scenario, simulation step, and member state; they are not retried or skipped.

Campaign histories stay below the first checkpoint and vary active membership
from 3 through the configured maximum, plus standbys. Separate existing tests
cover solo/two-member topologies, checkpointing, journal wrap, and state sync.
Member stalls pause the event loop, not individual asynchronous I/O completions.
These campaigns are correctness gates, not performance or exhaustive safety proofs.

Failure-model sources: [FoundationDB simulation documentation](https://apple.github.io/foundationdb/testing.html)
and [FoundationDB paper, §3](https://sigmodrecord.org/publications/sigmodRecord/2203/pdfs/08_fdb-zhou.pdf).