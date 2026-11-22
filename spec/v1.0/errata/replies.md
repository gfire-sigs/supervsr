# Cached reply envelopes

Affected specification sections: 7.5 and 13.4.

A cached client retransmission in a newer durable log view copies the stored reply and changes both `View` and `Author`, setting `Author = primary(View)`, then recomputes `HeaderChecksum`. It preserves the persisted reply, `Context`, request identity, and body bytes. Updating View alone contradicts the mandatory primary-author validation rule.

Checksum-addressed replica repair replies retain their exact original stored identity; this correction applies only to client retransmissions. It changes no disk or wire layout and does not relax authenticated sender or author checks. Clients continue chaining the original Context across retries and primary changes.

Regression: `TestCachedReplyEnvelopeTracksNewPrimary` covers header-only and body-bearing replies in `replication/consensus_regression_test.go`. Latest-reply body repair and startup/state-sync scanning are covered separately in the recovery errata.
