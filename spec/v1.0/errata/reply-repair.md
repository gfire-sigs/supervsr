# Latest-reply repair

## Sections 17.4 and 18.3

A cached-reply read failure is local corruption, not permission to execute the request again. The session table's exact `(client, op, header checksum)` identifies the required reply. A bounded serial repair worker requests that identity from peers, validates the complete original reply, and retains its immutable frame through the durable write completion.

The worker reserves the existing session slot using its generation before submitting the write. Normal commit cannot reuse or overwrite that slot while the write runs. Completion releases the reservation without changing session metadata. Fault state is cleared before submission, so completion cannot erase a fault discovered during the write. Late responses and stale I/O handles cannot publish into a newer session generation. Waiting for a peer does not block ordinary commits or consume every WAL-repair tick; only a submitted reply write blocks commit execution.

Startup scans the latest cached bodies. State synchronization first replays the committed suffix, then scans the resulting latest session replies before declaring skipped content repaired. This corrects section 18.3's ordering for a one-latest-reply store: checkpoint-era bodies may already have been overwritten on every peer. Header-only replies need no body repair. Superseding sync and shutdown drain physical writes before releasing their frames or replacing the table. With no other member available as a repair source, a corrupt required reply stops the replica.

Client-envelope changes are specified in [Cached reply envelopes](replies.md).

No disk format, wire layout, or stored-reply migration is required.

Regressions: `TestLatestReplyRepairRestoresDuplicateWithoutExecution`,
`TestReplyRepairReservesSlotUntilCompletion`,
`TestStateSyncWaitsForLatestReplyBeforePublishingRepairedContent`, and
`TestStateSyncScansLatestRepliesAfterCommittedReplay` in
`replication/reply_recovery_test.go`.
