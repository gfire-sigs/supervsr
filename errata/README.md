# Implementation contract errata

These corrections resolve the implementation defects and ambiguous contract
readings identified during conformance verification. Each entry names the
applicable specification sections, the corrected rule, and compatibility impact.
The confidential specification is neither modified nor reproduced here.

| Area | Affected sections | Correction |
| --- | --- | --- |
| [WAL recovery](wal-recovery.md) | 10.2–10.3, 16 | Preserve uncertain durable work; apply installed-view exclusion proof. |
| [Consensus](consensus.md) | 8, 12, 16–17 | Canonical suffix evidence, installation barriers, catchup and acknowledgements. |
| [Reply routing](replies.md) | 7.5, 13.4 | Update routing view and author without changing durable reply identity or context. |
| [Reply repair](reply-repair.md) | 13, 17.4, 18.3 | Repair exact latest replies with bounded, generation-safe durable writes. |
| [State recovery](state-recovery.md) | 18, 20–22 | Resume interrupted synchronization and validate persisted trailer chains. |
| [Clock epochs](clock.md) | 15 | Anchor synchronized bounds to monotonic time. |
| [Recovery entropy](entropy.md) | 8.1, 16.6, 23.1 | Seed each replica incarnation from injected entropy. |

The corrections retain the existing persistent and wire layouts. Source API
changes are called out in the affected entry; they do not imply an on-disk
migration. Errata do not establish exhaustive safety or refinement of the Go
implementation by the bounded formal models. TLA+/TLC execution remains CI-only.

## Regression verification

The correction passed `gojgp check` (all Go tests and AST lint), `go vet ./...`,
and the separate race gate:

```sh
go test -race -count=1 -timeout=10m -skip '^TestFoundationDBFaultCampaign$' ./...
VSR_SIM_SEED_START=4 VSR_SIM_SEED_COUNT=8 go test ./replication/sim \
  -run '^TestFoundationDBFaultCampaign$' -count=1 -timeout=15m
```

Together with default seeds 1–3 in the full suite, the campaign covered 187
scenarios across seeds 1–11. The campaign remains mandatory outside race
instrumentation. TLA+/TLC was not run locally.
