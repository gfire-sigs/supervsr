# WAL recovery corrections

## Sections 10.2–10.3: corruption is not crash-order proof

An intact redundant header with a missing or corrupt full prepare does not establish that the append was interrupted before acknowledgement. The same bytes can result from corruption after both append barriers and `PrepareOK`. Neither the latest operation number nor a write-concurrency window distinguishes those histories.

Such a retained slot requires remote repair. Its in-memory authoritative header remains reserved, with dirty and faulty set; it contributes neither an available copy nor a negative acknowledgement. Neighboring header-sector writes preserve the original disk evidence rather than persisting that reserved placeholder. A single-member WAL returns `ErrWALUncertainSolo` because it has no repair source.

This is deliberately less available after an ambiguous solo crash: an unacknowledged torn append may also require operator recovery. Recovery must not regain availability by silently losing a possibly acknowledged operation. Intact full prepares still repair redundant headers locally, and checkpoint-proven future/wrap entries still truncate.

## Sections 10.3 and 16: installed suffix authority

A durably installed log view excludes valid operations beyond its selected head only when their prepare view is older than the installed log view. Appends from the installed or a later view may legitimately extend that head. An excluded old header does not identify a corrupt counterpart: that counterpart may belong to a newer acknowledged append whose redundant header became stale. If either candidate is invalid, retain uncertainty rather than truncate on the other candidate's old view alone.

After append and repair I/O drains and the new view is durable, `WAL.DiscardAfter` invalidates the rejected in-memory suffix and header-sector cache without event-loop storage I/O. Restart uses the durable installed-view proof to avoid resurrecting old full prepares.

## Compatibility

No wire or disk layout changes. `WAL.Recover` replaces its unused process-configuration argument with `WALRecoveryView{LogView, HeadOp}`. The zero value supplies no installed-view exclusion proof. Open derives this proof from the durable view headers; ordinary callers must not invent a head bound. Persisted interrupted state sync uses the checkpoint as the immediate recovery commit bound while retaining the durable commit target for subsequent repair.

`TestWALRecoverStaleExcludedHeaderCannotHideCurrentCorruption` combines a stale
older-view redundant header with a corrupted current-view full header. Recovery
must refuse solo startup rather than lose the newer durable append.
