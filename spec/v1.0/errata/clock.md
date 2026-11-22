# Clock epoch anchoring

Affected contract: section 15.

## Correction

An installed clock interval has absolute lower and upper wall-time bounds at its
installation monotonic timestamp. Advance both bounds only by monotonic elapsed
time, then clamp the current local wall clock into that interval. Starting a new
sampling window does not rebase a still-valid installed epoch onto the new local
wall clock. Monotonic rollback or epoch expiry revokes synchronization.

The previous implementation added an old offset to a fresh wall-clock reading,
allowing a local wall jump to escape the interval while still claiming agreement.

## Compatibility and verification

No persistent or wire layout changes. Existing interval selection and quorum
rules remain unchanged. `TestClockSynchronizerClampsWallJumpsAcrossSamplingWindows`
checks forward jumps, rollback, and sampling-window rollover against the same
installed epoch; existing expiry tests cover loss of synchronization.
