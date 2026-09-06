# Recovery nonce entropy

Affected contracts: sections 8.1, 16.6, and 23.1.

## Correction

Initialize each replica's deterministic random stream from eight bytes read in
full from `Dependencies.Entropy`, not from its fixed member index. Propagate an
incomplete or failed entropy read; do not substitute the member index. Production
callers must supply fresh entropy at every process start. Replaying the same
injected bytes and event sequence remains deterministic for simulation.

A nonce binds a recovery response to an outstanding request. It is not transport
authentication; the existing authenticated-member boundary remains required.

The simulator encodes both member identity and restart generation into its seed.
It retains 56 generation bits rather than repeating after 256 restarts, and
rejects exhaustion before the generation encoding can wrap.

## Compatibility and verification

No frame or persistent layout changes. Successful construction now consumes
entropy; one-byte dummy readers are invalid fixtures. `TestReplicaRecoveryNonceUsesRestartEntropy`
compares emitted recovery nonces across distinct restart entropy and an exact
replay. `TestReplicaRejectsIncompleteRestartEntropy` verifies the read failure.
