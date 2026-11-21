// Package protocol defines wire and on-disk VSR frame formats, bounded frame
// pools, semantic header/body validation, and 128-bit frame checksums.
//
// Frames allocated from a FramePool are reference-counted. Senders populate
// Body, call Seal with the configured ChecksumAlgorithm to compute header and
// body digests, and pass immutable bytes via MessageBus. Receivers parse raw
// datagrams with FramePool.AcquireEncoded, authenticate the sender using
// Frame.BindReplica or Frame.BindClient, and release frames via Frame.Release
// when done.
package protocol
