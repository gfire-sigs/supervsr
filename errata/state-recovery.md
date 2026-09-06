# State recovery and block lifetime

## Sections 9.2, 10.3, 18.3: interrupted synchronization

A nonzero durable sync range is recovery authority, not evidence that the newly installed checkpoint's committed suffix is already present locally. Open validates state-machine capacities before superblock repair or application execution, then uses the checkpoint operation as the WAL recovery commit bound for an interrupted synchronization. It retains the durable `CommitMax` and view suffix as repair targets. Ordinary opens still validate their durable committed bound.

After reopening the checkpoint and repairing referenced blocks, an unavailable committed suffix enters canonical WAL repair instead of application replay against missing prepares. A solo member cannot resolve that uncertainty and stops. Latest-reply verification follows committed replay: peers retain only one latest reply per session and may already have overwritten checkpoint-era bodies. The durable sync range remains outstanding until the resulting latest bodies are verified or repaired. A superseding sync drains physical I/O before replacing the checkpoint and preserves the earliest outstanding sync minimum.

## Sections 9.3, 18.4: trailer chain lengths

Trailer payloads are not required to fill every block before the final block. In particular, the existing writer evenly divides an encoding over its reserved addresses. State synchronization accepts each checksummed block's actual nonempty payload length, subtracts it from the remaining aggregate length, and requires chain termination exactly when that length reaches zero. Opening still verifies the aggregate checksum and trailer schema.

This correction retains the existing writer, physical block layout, and persisted checksums. Existing evenly split chains need no migration, including a 16-client session table spanning two 4096-byte blocks.

## Sections 9.1, 18.2, 18.5: asynchronous repair and reuse

A generation check in a completion callback cannot undo an old write that has already reached storage. Before a checkpoint publishes released addresses as reusable, it joins outstanding block-repair reads, writes, and durability barriers. New repair I/O is held during the checkpoint transition; targets for newly released addresses are discarded after the join. An old write therefore cannot cross the allocator's reuse boundary. Failed repair writes or durability barriers stop the replica rather than being reported as repaired content.

The current checkpoint can still mark its predecessor's trailers acquired: their pending release occurs after checkpoint publication and is not itself durable. Open and state synchronization reconstruct that volatile state by validating the complete current application graph and protocol trailer chains, then marking acquired, nonreleased blocks outside that graph pending-release. Scrubbing excludes only these proven-unreachable blocks; missing or corrupt reachable content remains an error or repair target. Reconstructed releases remain unavailable for allocation until a later checkpoint is durable, preserving the existing release policy and deterministic allocation order.

These changes do not alter disk or wire layouts.

Regressions: `TestOpenRejectsStateMachineCapacitiesBeforeSideEffects`,
`TestOpenResumesInstalledCheckpointWithMissingCommittedSuffix`,
`TestStateSyncAcceptsEvenlySplitClientTrailer`,
`TestCheckpointJoinsLiveRepairBeforeReusingAddress`,
`TestCheckpointRecoveryRestoresOnlyProvenUnreachableReleases`, and
`TestCheckpointRecoveryRejectsCorruptReachabilityBeforeRelease`.
