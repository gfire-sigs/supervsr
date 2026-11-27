package replication

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"z.gfire.dev/supervsr/replication/protocol"
)

const (
	ActiveMax       = 6
	StandbyMax      = 6
	MembersMax      = ActiveMax + StandbyMax
	CacheLineSize   = 64
	SectorSize      = 4 << 10
	SuperblockBytes = 8 << 10
	HeaderBytes     = 256
)

var ErrInvalidConfiguration = errors.New("replication: invalid configuration")

type ConfigError struct {
	Field  string
	Reason string
}

func (err *ConfigError) Error() string {
	return fmt.Sprintf("%s: %s: %v", ErrInvalidConfiguration, err.Field, err.Reason)
}

func (err *ConfigError) Unwrap() error {
	return ErrInvalidConfiguration
}

// ClusterConfig specifies cluster-wide immutable layout, quorum, pipeline, and
// checksum parameters included in the superblock ConfigurationChecksum.
type ClusterConfig struct {
	CacheLineSize                   uint64
	ClientsMax                      uint64
	PipelineMax                     uint64
	ViewChangeHeadersSuffixMax      uint64
	ReplicationQuorumMax            uint64
	JournalSlots                    uint64
	MessageSizeMax                  uint64
	ApplicationBatchSizeMax         uint64
	ApplicationReplySizeMax         uint64
	SuperblockCopies                uint64
	BlockSize                       uint64
	StorageLevels                   uint64
	StorageGrowthFactor             uint64
	CompactionOps                   uint64
	StorageSnapshotsMax             uint64
	ManifestCompactExtraBlocks      uint64
	TableCoalescingThresholdPercent uint64
	ReleaseHistoryMax               uint64
	StorageScansMax                 uint64
	ChecksumAlgorithm               ChecksumAlgorithm
}

// DefaultClusterConfig returns validated cluster defaults using ChecksumAEGIS128L.
func DefaultClusterConfig() ClusterConfig {
	const messageSize = 1 << 20
	return ClusterConfig{
		CacheLineSize:                   CacheLineSize,
		ClientsMax:                      64,
		PipelineMax:                     8,
		ViewChangeHeadersSuffixMax:      9,
		ReplicationQuorumMax:            3,
		JournalSlots:                    1024,
		MessageSizeMax:                  messageSize,
		ApplicationBatchSizeMax:         messageSize - HeaderBytes,
		ApplicationReplySizeMax:         messageSize - HeaderBytes,
		SuperblockCopies:                4,
		BlockSize:                       512 << 10,
		StorageLevels:                   7,
		StorageGrowthFactor:             8,
		CompactionOps:                   32,
		StorageSnapshotsMax:             32,
		ManifestCompactExtraBlocks:      1,
		TableCoalescingThresholdPercent: 50,
		ReleaseHistoryMax:               64,
		StorageScansMax:                 6,
	}
}

func (cfg ClusterConfig) Validate(activeCount, standbyCount uint8) error {
	invalid := func(field, reason string) error {
		return &ConfigError{Field: field, Reason: reason}
	}
	if activeCount == 0 || activeCount > ActiveMax {
		return invalid("ActiveCount", "must be between 1 and 6")
	}
	if standbyCount > StandbyMax || uint16(activeCount)+uint16(standbyCount) > MembersMax {
		return invalid("StandbyCount", "active and standby members exceed 12")
	}
	if cfg.CacheLineSize != CacheLineSize {
		return invalid("CacheLineSize", "format 1 requires 64")
	}
	if cfg.ClientsMax == 0 {
		return invalid("ClientsMax", "must be positive")
	}
	if cfg.PipelineMax == 0 || cfg.PipelineMax > 15 {
		return invalid("PipelineMax", "must be between 1 and 15")
	}
	if cfg.ViewChangeHeadersSuffixMax != cfg.PipelineMax+1 {
		return invalid("ViewChangeHeadersSuffixMax", "must equal PipelineMax+1")
	}
	if cfg.ReplicationQuorumMax == 0 {
		return invalid("ReplicationQuorumMax", "must be positive")
	}
	if cfg.JournalSlots <= cfg.PipelineMax {
		return invalid("JournalSlots", "must exceed PipelineMax")
	}
	headerSize, ok := checkedMul(cfg.JournalSlots, HeaderBytes)
	if !ok || headerSize < 2*SectorSize {
		return invalid("JournalSlots", "header ring must span at least two sectors")
	}
	if cfg.MessageSizeMax%SectorSize != 0 {
		return invalid("MessageSizeMax", "must be a sector multiple")
	}
	minimumMessageSize, ok := checkedMul(cfg.PipelineMax+1, HeaderBytes)
	if ok {
		minimumMessageSize, ok = checkedAdd(minimumMessageSize, HeaderBytes+1024)
	}
	if !ok || cfg.MessageSizeMax < minimumMessageSize {
		return invalid("MessageSizeMax", "cannot hold a view and checkpoint")
	}
	maximumBody := cfg.MessageSizeMax - HeaderBytes
	if cfg.ApplicationBatchSizeMax == 0 || cfg.ApplicationBatchSizeMax > maximumBody {
		return invalid("ApplicationBatchSizeMax", "must fit a frame body")
	}
	if cfg.ApplicationReplySizeMax < 64 || cfg.ApplicationReplySizeMax > maximumBody {
		return invalid("ApplicationReplySizeMax", "must be between 64 and the maximum frame body")
	}
	if cfg.SuperblockCopies != 4 && cfg.SuperblockCopies != 6 && cfg.SuperblockCopies != 8 {
		return invalid("SuperblockCopies", "must be 4, 6, or 8")
	}
	if cfg.BlockSize == 0 || cfg.BlockSize%SectorSize != 0 || cfg.BlockSize > cfg.MessageSizeMax {
		return invalid("BlockSize", "must be a sector multiple no larger than MessageSizeMax")
	}
	if cfg.StorageLevels == 0 || cfg.StorageLevels > 64 {
		return invalid("StorageLevels", "must be between 1 and 64")
	}
	if cfg.StorageGrowthFactor < 2 {
		return invalid("StorageGrowthFactor", "must be at least 2")
	}
	if cfg.CompactionOps == 0 || cfg.CompactionOps&(cfg.CompactionOps-1) != 0 {
		return invalid("CompactionOps", "must be a power of two")
	}
	if cfg.StorageSnapshotsMax == 0 {
		return invalid("StorageSnapshotsMax", "must be positive")
	}
	if cfg.ManifestCompactExtraBlocks == 0 {
		return invalid("ManifestCompactExtraBlocks", "must be positive")
	}
	if cfg.TableCoalescingThresholdPercent == 0 || cfg.TableCoalescingThresholdPercent > 100 {
		return invalid("TableCoalescingThresholdPercent", "must be between 1 and 100")
	}
	if cfg.ReleaseHistoryMax == 0 || cfg.ReleaseHistoryMax > uint64(^uint16(0)) || cfg.ReleaseHistoryMax*4 > maximumBody {
		return invalid("ReleaseHistoryMax", "must fit the Ping body and uint16 count")
	}
	if cfg.StorageScansMax == 0 {
		return invalid("StorageScansMax", "must be positive")
	}
	interval, ok := cfg.CheckpointInterval()
	if !ok || interval == 0 || interval%cfg.CompactionOps != 0 {
		return invalid("JournalSlots", "derived checkpoint interval is invalid")
	}
	quorums, ok := QuorumsFor(activeCount, uint8(min(cfg.ReplicationQuorumMax, uint64(^uint8(0)))))
	if !ok || uint16(quorums.Replication)+uint16(quorums.ViewChange) <= uint16(activeCount) || uint16(quorums.Replication)+uint16(quorums.Negative) <= uint16(activeCount) {
		return invalid("ReplicationQuorumMax", "derived quorums do not intersect")
	}
	if !cfg.ChecksumAlgorithm.Valid() {
		return invalid("ChecksumAlgorithm", "must be AEGIS128L or Wyhash128")
	}
	if _, ok := cfg.BlockBase(); !ok {
		return invalid("storage layout", "derived offsets overflow")
	}
	return nil
}

func (cfg ClusterConfig) fingerprintEncoding() [136]byte {
	fields := [...]uint64{
		cfg.CacheLineSize,
		cfg.ClientsMax,
		cfg.PipelineMax,
		cfg.ViewChangeHeadersSuffixMax,
		cfg.ReplicationQuorumMax,
		cfg.JournalSlots,
		cfg.MessageSizeMax,
		cfg.SuperblockCopies,
		cfg.BlockSize,
		cfg.StorageLevels,
		cfg.StorageGrowthFactor,
		cfg.CompactionOps,
		cfg.StorageSnapshotsMax,
		cfg.ManifestCompactExtraBlocks,
		cfg.TableCoalescingThresholdPercent,
		cfg.ReleaseHistoryMax,
		cfg.StorageScansMax,
	}
	var encoded [136]byte
	for index, value := range fields {
		binary.LittleEndian.PutUint64(encoded[index*8:], value)
	}
	return encoded
}

// Fingerprint returns the 128-bit configuration checksum stored in superblocks
// and verified on startup to detect incompatible cluster parameters.
func (cfg ClusterConfig) Fingerprint() protocol.Checksum {
	encoded := cfg.fingerprintEncoding()
	if cfg.ChecksumAlgorithm == ChecksumAEGIS128L {
		checksum, err := protocol.ChecksumBytes(ChecksumAEGIS128L, encoded[:])
		if err != nil {
			return protocol.Checksum{}
		}
		return checksum
	}
	if !cfg.ChecksumAlgorithm.Valid() {
		return protocol.Checksum{}
	}
	var withAlgorithm [137]byte
	copy(withAlgorithm[:], encoded[:])
	withAlgorithm[136] = byte(cfg.ChecksumAlgorithm)
	checksum, err := protocol.ChecksumBytes(cfg.ChecksumAlgorithm, withAlgorithm[:])
	if err != nil {
		return protocol.Checksum{}
	}
	return checksum
}

func (cfg ClusterConfig) CheckpointInterval() (uint64, bool) {
	twicePipeline, ok := checkedMul(2, cfg.PipelineMax)
	if !ok {
		return 0, false
	}
	rounded, ok := checkedRoundUp(twicePipeline, cfg.CompactionOps)
	if !ok {
		return 0, false
	}
	subtrahend, ok := checkedAdd(cfg.CompactionOps, rounded)
	if !ok || cfg.JournalSlots <= subtrahend {
		return 0, false
	}
	return cfg.JournalSlots - subtrahend, true
}

func (cfg ClusterConfig) BlockBase() (uint64, bool) {
	superblockSize, ok := checkedMul(cfg.SuperblockCopies, SuperblockBytes)
	if !ok {
		return 0, false
	}
	headerBytes, ok := checkedMul(cfg.JournalSlots, HeaderBytes)
	if !ok {
		return 0, false
	}
	headerSize, ok := checkedAlignUp(headerBytes, SectorSize)
	if !ok {
		return 0, false
	}
	prepareBase, ok := checkedAdd(superblockSize, headerSize)
	if !ok {
		return 0, false
	}
	prepareStride, ok := checkedAlignUp(cfg.MessageSizeMax, SectorSize)
	if !ok {
		return 0, false
	}
	prepareSize, ok := checkedMul(cfg.JournalSlots, prepareStride)
	if !ok {
		return 0, false
	}
	replyBase, ok := checkedAdd(prepareBase, prepareSize)
	if !ok {
		return 0, false
	}
	replySize, ok := checkedMul(cfg.ClientsMax, prepareStride)
	if !ok {
		return 0, false
	}
	end, ok := checkedAdd(replyBase, replySize)
	if !ok {
		return 0, false
	}
	return checkedAlignUp(end, cfg.BlockSize)
}

func (cfg ClusterConfig) BlockOffset(address uint64) (uint64, bool) {
	if address == 0 || cfg.BlockSize == 0 {
		return 0, false
	}
	base, ok := cfg.BlockBase()
	if !ok {
		return 0, false
	}
	delta, ok := checkedMul(address-1, cfg.BlockSize)
	if !ok {
		return 0, false
	}
	return checkedAdd(base, delta)
}

const (
	defaultPingInterval             = time.Second
	defaultPrepareTimeout           = 250 * time.Millisecond
	defaultAbdicationTimeout        = 10 * time.Second
	defaultCommitInterval           = 500 * time.Millisecond
	defaultExitViewTimeout          = 500 * time.Millisecond
	defaultJoinViewTimeout          = 500 * time.Millisecond
	defaultViewChangeTimeout        = 5 * time.Second
	defaultExitViewCollectionWindow = 5 * time.Second
	defaultPulseInterval            = 100 * time.Millisecond
	defaultRepairInterval           = 100 * time.Millisecond
	defaultJournalRepairTimeout     = 500 * time.Millisecond
	defaultBlockRepairRetryAfter    = 100 * time.Millisecond
	defaultBlockRepairTimeout       = 250 * time.Millisecond
	defaultClientPingInterval       = 30 * time.Second
	defaultFailureIntervalMin       = 100 * time.Millisecond
	defaultFailureIntervalMax       = 2 * time.Second
	defaultFailureYellowPermille    = uint32(1500)
	defaultFailureRedPermille       = uint32(3000)
)

// ProcessConfig defines per-process runtime limits, protocol timers, failure
// detector bounds, retry backoffs, I/O concurrencies, storage ceiling, and
// optional hardware-checksum enforcement.
//
// Zero values on optional protocol timer and failure-detector fields fall back
// to the defaults returned by DefaultProcessConfig so existing v1.0 configs
// keep identical behavior.
type ProcessConfig struct {
	Tick                    time.Duration
	InitialRTT              time.Duration
	MaximumRTT              time.Duration
	RTTMultiplier           uint32
	BackoffMin              time.Duration
	BackoffMax              time.Duration
	PrimaryRequestQueueMax  uint32
	JournalReadConcurrency  uint32
	JournalWriteConcurrency uint32
	ReplyReadConcurrency    uint32
	ReplyWriteConcurrency   uint32
	BlockReadConcurrency    uint32
	BlockWriteConcurrency   uint32
	StorageSizeLimit        uint64
	RepairRequestsMax       uint32
	RepairReadsMax          uint32
	ScrubReadConcurrency    uint32
	ScrubWriteConcurrency   uint32
	ScrubCycle              time.Duration
	ScrubIntervalMin        time.Duration
	ScrubIntervalMax        time.Duration
	ClockOffsetToleranceMax time.Duration
	ClockEpochMax           time.Duration
	ClockWindowMin          time.Duration
	ClockWindowMax          time.Duration
	RequireHardwareChecksum bool

	// Optional protocol timers and failure-detector parameters (0 = default).
	PingInterval             time.Duration
	PrepareTimeout           time.Duration
	AbdicationTimeout        time.Duration
	CommitInterval           time.Duration
	ExitViewTimeout          time.Duration
	JoinViewTimeout          time.Duration
	ViewChangeTimeout        time.Duration
	ExitViewCollectionWindow time.Duration
	PulseInterval            time.Duration
	RepairInterval           time.Duration
	JournalRepairTimeout     time.Duration
	BlockRepairRetryAfter    time.Duration
	BlockRepairTimeout       time.Duration
	ClientPingInterval       time.Duration
	FailureIntervalMin       time.Duration
	FailureIntervalMax       time.Duration
	FailureYellowPermille    uint32
	FailureRedPermille       uint32
}

type resolvedProcessTimers struct {
	pingInterval             time.Duration
	prepareTimeout           time.Duration
	abdicationTimeout        time.Duration
	commitInterval           time.Duration
	exitViewTimeout          time.Duration
	joinViewTimeout          time.Duration
	viewChangeTimeout        time.Duration
	exitViewCollectionWindow time.Duration
	pulseInterval            time.Duration
	repairInterval           time.Duration
	journalRepairTimeout     time.Duration
	blockRepairRetryAfter    time.Duration
	blockRepairTimeout       time.Duration
	clientPingInterval       time.Duration
	failureIntervalMin       time.Duration
	failureIntervalMax       time.Duration
	failureYellowPermille    uint32
	failureRedPermille       uint32
}

func (cfg ProcessConfig) resolvedTimers() resolvedProcessTimers {
	return resolvedProcessTimers{
		pingInterval:             cmp.Or(cfg.PingInterval, defaultPingInterval),
		prepareTimeout:           cmp.Or(cfg.PrepareTimeout, defaultPrepareTimeout),
		abdicationTimeout:        cmp.Or(cfg.AbdicationTimeout, defaultAbdicationTimeout),
		commitInterval:           cmp.Or(cfg.CommitInterval, defaultCommitInterval),
		exitViewTimeout:          cmp.Or(cfg.ExitViewTimeout, defaultExitViewTimeout),
		joinViewTimeout:          cmp.Or(cfg.JoinViewTimeout, defaultJoinViewTimeout),
		viewChangeTimeout:        cmp.Or(cfg.ViewChangeTimeout, defaultViewChangeTimeout),
		exitViewCollectionWindow: cmp.Or(cfg.ExitViewCollectionWindow, defaultExitViewCollectionWindow),
		pulseInterval:            cmp.Or(cfg.PulseInterval, defaultPulseInterval),
		repairInterval:           cmp.Or(cfg.RepairInterval, defaultRepairInterval),
		journalRepairTimeout:     cmp.Or(cfg.JournalRepairTimeout, defaultJournalRepairTimeout),
		blockRepairRetryAfter:    cmp.Or(cfg.BlockRepairRetryAfter, defaultBlockRepairRetryAfter),
		blockRepairTimeout:       cmp.Or(cfg.BlockRepairTimeout, defaultBlockRepairTimeout),
		clientPingInterval:       cmp.Or(cfg.ClientPingInterval, defaultClientPingInterval),
		failureIntervalMin:       cmp.Or(cfg.FailureIntervalMin, defaultFailureIntervalMin),
		failureIntervalMax:       cmp.Or(cfg.FailureIntervalMax, defaultFailureIntervalMax),
		failureYellowPermille:    cmp.Or(cfg.FailureYellowPermille, defaultFailureYellowPermille),
		failureRedPermille:       cmp.Or(cfg.FailureRedPermille, defaultFailureRedPermille),
	}
}

// DefaultProcessConfig returns baseline per-process runtime settings.
func DefaultProcessConfig() ProcessConfig {
	return ProcessConfig{
		Tick:                     10 * time.Millisecond,
		InitialRTT:               300 * time.Millisecond,
		MaximumRTT:               60 * time.Second,
		RTTMultiplier:            2,
		BackoffMin:               10 * time.Millisecond,
		BackoffMax:               10 * time.Second,
		PrimaryRequestQueueMax:   2,
		JournalReadConcurrency:   8,
		JournalWriteConcurrency:  32,
		ReplyReadConcurrency:     1,
		ReplyWriteConcurrency:    2,
		BlockReadConcurrency:     32,
		BlockWriteConcurrency:    32,
		StorageSizeLimit:         16 << 40,
		RepairRequestsMax:        4,
		RepairReadsMax:           4,
		ScrubReadConcurrency:     1,
		ScrubWriteConcurrency:    1,
		ScrubCycle:               180 * 24 * time.Hour,
		ScrubIntervalMin:         50 * time.Millisecond,
		ScrubIntervalMax:         10 * time.Second,
		ClockOffsetToleranceMax:  10 * time.Second,
		ClockEpochMax:            60 * time.Second,
		ClockWindowMin:           2 * time.Second,
		ClockWindowMax:           20 * time.Second,
		PingInterval:             defaultPingInterval,
		PrepareTimeout:           defaultPrepareTimeout,
		AbdicationTimeout:        defaultAbdicationTimeout,
		CommitInterval:           defaultCommitInterval,
		ExitViewTimeout:          defaultExitViewTimeout,
		JoinViewTimeout:          defaultJoinViewTimeout,
		ViewChangeTimeout:        defaultViewChangeTimeout,
		ExitViewCollectionWindow: defaultExitViewCollectionWindow,
		PulseInterval:            defaultPulseInterval,
		RepairInterval:           defaultRepairInterval,
		JournalRepairTimeout:     defaultJournalRepairTimeout,
		BlockRepairRetryAfter:    defaultBlockRepairRetryAfter,
		BlockRepairTimeout:       defaultBlockRepairTimeout,
		ClientPingInterval:       defaultClientPingInterval,
		FailureIntervalMin:       defaultFailureIntervalMin,
		FailureIntervalMax:       defaultFailureIntervalMax,
		FailureYellowPermille:    defaultFailureYellowPermille,
		FailureRedPermille:       defaultFailureRedPermille,
	}
}

func (cfg ProcessConfig) Validate(cluster ClusterConfig) error {
	invalid := func(field, reason string) error {
		return &ConfigError{Field: field, Reason: reason}
	}
	if err := cfg.validateTimers(); err != nil {
		return err
	}
	resolved := cfg.resolvedTimers()
	if cfg.Tick <= 0 || cfg.InitialRTT < cfg.Tick || cfg.MaximumRTT < cfg.InitialRTT {
		return invalid("RTT", "require 0 < Tick <= InitialRTT <= MaximumRTT")
	}
	if cfg.Tick >= resolved.failureIntervalMin/2 {
		return invalid("Tick", "must be less than half of FailureIntervalMin")
	}
	if cfg.RTTMultiplier == 0 {
		return invalid("RTTMultiplier", "must be positive")
	}
	if cfg.BackoffMin <= 0 || cfg.BackoffMax < cfg.BackoffMin {
		return invalid("Backoff", "require 0 < BackoffMin <= BackoffMax")
	}
	if !cfg.concurrencyPositive() {
		return invalid("concurrency", "queue and concurrency values must be positive")
	}
	if cfg.ScrubIntervalMin <= 0 || cfg.ScrubIntervalMax < cfg.ScrubIntervalMin || cfg.ScrubCycle < cfg.ScrubIntervalMax {
		return invalid("ScrubInterval", "require 0 < minimum <= maximum <= cycle")
	}
	if cfg.ClockWindowMin <= 0 || cfg.ClockWindowMax < cfg.ClockWindowMin || cfg.ClockEpochMax < cfg.ClockWindowMax {
		return invalid("ClockWindow", "require 0 < minimum <= maximum <= epoch")
	}
	blockBase, ok := cluster.BlockBase()
	if !ok || cfg.StorageSizeLimit < blockBase || (cfg.StorageSizeLimit-blockBase)%cluster.BlockSize != 0 {
		return invalid("StorageSizeLimit", "must cover BlockBase in whole block extents")
	}
	if cfg.RequireHardwareChecksum && !protocol.HardwareChecksumAvailable() {
		return ErrHardwareChecksumUnavailable
	}
	return nil
}

func (cfg ProcessConfig) validateTimers() error {
	invalid := func(field, reason string) error {
		return &ConfigError{Field: field, Reason: reason}
	}
	rawDurations := [...]time.Duration{
		cfg.PingInterval,
		cfg.PrepareTimeout,
		cfg.AbdicationTimeout,
		cfg.CommitInterval,
		cfg.ExitViewTimeout,
		cfg.JoinViewTimeout,
		cfg.ViewChangeTimeout,
		cfg.ExitViewCollectionWindow,
		cfg.PulseInterval,
		cfg.RepairInterval,
		cfg.JournalRepairTimeout,
		cfg.BlockRepairRetryAfter,
		cfg.BlockRepairTimeout,
		cfg.ClientPingInterval,
		cfg.FailureIntervalMin,
		cfg.FailureIntervalMax,
	}
	for _, value := range rawDurations {
		if value < 0 {
			return invalid("Timers", "must not be negative")
		}
	}
	resolved := cfg.resolvedTimers()
	if resolved.failureIntervalMax < resolved.failureIntervalMin {
		return invalid("FailureInterval", "require 0 < FailureIntervalMin <= FailureIntervalMax")
	}
	if resolved.failureYellowPermille < 1000 || resolved.failureRedPermille <= resolved.failureYellowPermille {
		return invalid("FailureThreshold", "require 1000 <= FailureYellowPermille < FailureRedPermille")
	}
	if resolved.blockRepairTimeout < resolved.blockRepairRetryAfter {
		return invalid("BlockRepair", "require 0 < BlockRepairRetryAfter <= BlockRepairTimeout")
	}
	if cfg.Tick <= 0 {
		return nil
	}
	periods := [...]time.Duration{
		resolved.pingInterval,
		resolved.prepareTimeout,
		resolved.abdicationTimeout,
		resolved.commitInterval,
		resolved.exitViewTimeout,
		resolved.joinViewTimeout,
		resolved.viewChangeTimeout,
		resolved.exitViewCollectionWindow,
		resolved.pulseInterval,
		resolved.repairInterval,
		resolved.journalRepairTimeout,
		resolved.blockRepairRetryAfter,
		resolved.blockRepairTimeout,
		resolved.clientPingInterval,
	}
	for _, period := range periods {
		if period < cfg.Tick {
			return invalid("Timers", "protocol timers must be at least Tick")
		}
	}
	return nil
}

func (cfg ProcessConfig) concurrencyPositive() bool {
	return cfg.PrimaryRequestQueueMax > 0 &&
		cfg.JournalReadConcurrency > 0 &&
		cfg.JournalWriteConcurrency > 0 &&
		cfg.ReplyReadConcurrency > 0 &&
		cfg.ReplyWriteConcurrency > 0 &&
		cfg.BlockReadConcurrency > 0 &&
		cfg.BlockWriteConcurrency > 0 &&
		cfg.RepairRequestsMax > 0 &&
		cfg.RepairReadsMax > 0 &&
		cfg.ScrubReadConcurrency > 0 &&
		cfg.ScrubWriteConcurrency > 0
}

type Quorums struct {
	Replication uint8
	ViewChange  uint8
	Negative    uint8
	Majority    uint8
	Upgrade     uint8
}

func QuorumsFor(activeCount, replicationQuorumMax uint8) (Quorums, bool) {
	if activeCount == 0 || activeCount > ActiveMax || replicationQuorumMax == 0 {
		return Quorums{}, false
	}
	replication := min(replicationQuorumMax, (activeCount+1)/2)
	if activeCount == 2 {
		replication = 2
	}
	viewChange := activeCount - replication + 1
	if activeCount == 2 {
		viewChange = 2
	}
	return Quorums{
		Replication: replication,
		ViewChange:  viewChange,
		Negative:    activeCount - replication + 1,
		Majority:    activeCount/2 + 1,
		Upgrade:     activeCount,
	}, true
}
