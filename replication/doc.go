// Package replication implements a Viewstamped Replication (VSR) consensus
// engine, write-ahead log (WAL), block-addressed checkpoint store, and client
// session state machine.
//
// # Lifecycle and Concurrency
//
// Format initializes an empty Storage image with an immutable Group,
// Membership, and ClusterConfig fingerprint before Open recovers or starts a
// Replica. Callers drive a Replica either by running Run in a dedicated
// goroutine or by stepping Process and Tick manually from one event-loop
// goroutine. Replica.Submit and Replica.Snapshot are safe for concurrent use by
// external goroutines; once Open returns a Replica, Replica.Close assumes
// ownership for closing both StateMachine and Storage.
//
// # Frame Ownership and Authentication
//
// Inbound network frames passed to Replica.Submit must be acquired from a
// protocol.FramePool, bound with Frame.BindReplica or Frame.BindClient after
// transport authentication, and released by the caller only if Submit returns a
// non-nil error. On a nil return, ownership transfers to the Replica until it
// calls Frame.Release. Outbound frames emitted to MessageBus must be retained
// with Frame.Retain if queued asynchronously after the send callback returns.
//
// # Clients
//
// ManagedClient provides a goroutine-safe, context-aware request/reply wrapper
// with an internal ticker goroutine. Callers driving a custom single-threaded
// event loop can use Client directly.
package replication
