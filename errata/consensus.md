# Consensus and catchup corrections

Affected specification sections: 8.2–8.4, 11, 12.3–12.4, 13.1–13.3, 16.1.1, 16.4–16.6, and 17.1–17.3.

- Select uncommitted candidates only from the maximal reported `LogView`. Older logs may fill the committed prefix, but cannot resurrect an excluded suffix. An explicit tail contributes its positional operation to the committed lower bound. Count an exact `Present` copy independently of a NACK; implicit negatives require the documented older-log evidence. Faulty blank entries remain uncertainty.
- A checkpoint header appended after a bounded suffix is an anchor, not an adjacent prepare. Validate the contiguous suffix and prove any omitted interval is committed before accepting it. Preserve the locally committed prefix; never truncate it to rebuild an uncommitted FIFO.
- Install one canonical head, checksum, timestamp, and pipeline. Drain old appends before repair/replacement; clear old quorum bits. Persist the installed log before normal admission or acknowledgements, then discard obsolete WAL cache entries. Reboot exclusion of the durable rejected tail is described in the WAL errata.
- Higher-view Prepare/Commit evidence requests a nonce-bound View from the evidence view's primary. Recovering-head Ping/Join evidence does likewise. A normal lagging backup or standby can request and accept a same-view response. Validate response nonces before checkpoint observation, state sync, or repair-window mutation. Reset prior-primary commit-heartbeat ordering on installation.
- A durable exact duplicate Prepare resends PrepareOK. Both duplicate handling and delayed append completions obey normal/durable-log-view, active-member, and checkpoint-safe acknowledgement bounds.
- Event-loop processing continues an accepted canonical installation without requiring a timer tick. Checkpoint publication reconsiders withheld local acknowledgements once per entry/view, including the primary's own durable copy; advancing the safety bound must not leave an already-completed append permanently unacknowledged.
- Registrations can evict sessions at commit. Defer application admission behind pending registrations and revalidate queued requests after those registrations execute; receiving a request never mutates the committed session table.

No wire or disk layout changes. Installed view metadata now governs recovery of older-view rejected suffixes; deployments must not mix implementations that can reintroduce those suffixes.

Regression coverage in `replication/consensus_regression_test.go`:

- `TestCanonicalDirtyCopyRemainsRepairSource`
- `TestCanonicalExplicitTailCommitsLowerBound`
- `TestCanonicalLatestLogViewExcludesTruncatedSuffix`
- `TestCanonicalOmittedTailCannotEraseCommittedPrefix`
- `TestCanonicalInstallationPreservesCommittedPrefixAndTruncatesTraffic`
- `TestDurableDuplicatePrepareRestoresLostAcknowledgement`
- `TestOldWALCompletionCannotAcknowledgeDuringViewChange`
- `TestHigherViewEvidenceRequestsEvidencePrimary`
- `TestSameViewLaggingBackupRequestsAndInstallsView`
- `TestViewNonceRejectedBeforeRepairWindowMutation`
- `TestRegistrationAdmissionCannotEvictPendingApplicationSession`
- `TestCheckpointProgressReleasesWithheldPrepareAcknowledgement`

Existing integration coverage: `TestUpgradeHandoffReopensAtTargetRelease` and `TestClusterSustainedTrafficReachesConfiguredPipeline`.
