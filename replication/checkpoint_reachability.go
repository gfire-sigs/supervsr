package replication

import (
	"encoding/binary"
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type checkpointGraphNode struct {
	requirement BlockRequirement
	processed   bool
}

type checkpointGraph struct {
	replica    *Replica
	checkpoint CheckpointState
	reachable  FixedBitSet
	nodes      []checkpointGraphNode
	byAddress  map[uint64]int
	stack      []int
	outputs    []BlockRequirement
	buffer     []byte
	limit      int
}

func (replica *Replica) resolveCheckpointReachability(manifest CheckpointManifest) (FixedBitSet, FixedBitSet, []uint64, error) {
	if !validCheckpointManifestShape(manifest) {
		return FixedBitSet{}, FixedBitSet{}, nil, ErrInvalidCheckpoint
	}
	blockCount := replica.blockAllocator.blockCount
	reachable, err := NewFixedBitSet(blockCount)
	if err != nil {
		return FixedBitSet{}, FixedBitSet{}, nil, err
	}
	protected, superseded, err := replica.currentCheckpointTrailerBlocks()
	if err != nil {
		return FixedBitSet{}, FixedBitSet{}, nil, err
	}
	if manifest.BlockCount == 0 && manifest.Root == (BlockReference{}) {
		return reachable, protected, superseded, nil
	}
	if replica.deps.BlockValidator == nil {
		return FixedBitSet{}, FixedBitSet{}, nil, ErrInvalidCheckpoint
	}
	checkpoint, err := replica.checkpointApplicationState(manifest)
	if err != nil {
		return FixedBitSet{}, FixedBitSet{}, nil, err
	}
	checkpointMax := replica.checkpointBlockLimit
	if uint64(checkpointMax) > uint64(^uint(0)>>1) {
		return FixedBitSet{}, FixedBitSet{}, nil, ErrCheckpointBlockLimit
	}
	limit := int(checkpointMax)
	if uint64(manifest.BlockCount) > uint64(limit) {
		return FixedBitSet{}, FixedBitSet{}, nil, ErrCheckpointBlockLimit
	}
	graph := checkpointGraph{
		replica: replica, checkpoint: checkpoint, reachable: reachable, limit: limit,
		nodes: make([]checkpointGraphNode, 0, limit), byAddress: make(map[uint64]int, limit),
		stack: make([]int, 0, limit), outputs: make([]BlockRequirement, limit),
		buffer: make([]byte, replica.config.Cluster.BlockSize),
	}
	if err := graph.visitManifestChain(manifest); err != nil {
		return FixedBitSet{}, FixedBitSet{}, nil, err
	}
	if manifest.Root != (BlockReference{}) {
		requirement, err := replica.deps.BlockValidator.CheckpointRoot(checkpoint)
		if err != nil || requirement.Reference != manifest.Root {
			return FixedBitSet{}, FixedBitSet{}, nil, ErrInvalidCheckpoint
		}
		if _, err := graph.enqueue(requirement); err != nil {
			return FixedBitSet{}, FixedBitSet{}, nil, err
		}
	}
	if err := graph.visitStack(); err != nil {
		return FixedBitSet{}, FixedBitSet{}, nil, err
	}
	return graph.reachable, protected, superseded, nil
}

func validCheckpointManifestShape(manifest CheckpointManifest) bool {
	if manifest.BlockCount == 0 {
		return manifest.Oldest == (BlockReference{}) && manifest.Newest == (BlockReference{})
	}
	if manifest.Oldest == (BlockReference{}) || manifest.Newest == (BlockReference{}) {
		return false
	}
	return manifest.BlockCount != 1 || manifest.Oldest == manifest.Newest
}

func (replica *Replica) checkpointApplicationState(manifest CheckpointManifest) (CheckpointState, error) {
	header, found := replica.wal.RecoveredHeader(replica.checkpointTarget)
	if !found {
		return CheckpointState{}, ErrWALRecovery
	}
	var encodedHeader [protocol.HeaderSize]byte
	if err := protocol.EncodeHeader(encodedHeader[:], &header); err != nil {
		return CheckpointState{}, err
	}
	state := replica.checkpoint
	state.Header = encodedHeader
	state.OldestManifestChecksum = manifest.Oldest.Checksum
	state.NewestManifestChecksum = manifest.Newest.Checksum
	state.SnapshotRootChecksum = manifest.Root.Checksum
	state.OldestManifestAddress = manifest.Oldest.Address
	state.NewestManifestAddress = manifest.Newest.Address
	state.SnapshotRootAddress = manifest.Root.Address
	state.ManifestBlockCount = manifest.BlockCount
	state.LogicalStorageSize = replica.blockAllocator.LogicalStorageSize()
	state.Release = replica.checkpointTargetRelease
	validation := CheckpointValidation{
		Group: replica.config.Group, MessageSizeMax: uint32(replica.config.Cluster.MessageSizeMax),
		MemberCount: replica.membership.ActiveCount + replica.membership.StandbyCount,
		BlockSize:   replica.config.Cluster.BlockSize, ClientsMax: replica.config.Cluster.ClientsMax,
	}
	validation.BlockBase, _ = replica.config.Cluster.BlockBase()
	if err := state.Validate(validation); err != nil {
		return CheckpointState{}, err
	}
	return state, nil
}

func (graph *checkpointGraph) visitManifestChain(manifest CheckpointManifest) error {
	current := manifest.Newest
	chain := make(map[uint64]struct{}, manifest.BlockCount)
	for ordinal := uint32(0); ordinal < manifest.BlockCount; ordinal++ {
		if _, found := chain[current.Address]; found {
			return ErrInvalidCheckpoint
		}
		chain[current.Address] = struct{}{}
		index, err := graph.enqueue(BlockRequirement{Reference: current, Type: BlockManifest})
		if err != nil {
			return err
		}
		result, err := graph.process(index)
		if err != nil {
			return err
		}
		var previous BlockReference
		copy(previous.Checksum[:], result.Metadata[:16])
		previous.Address = binary.LittleEndian.Uint64(result.Metadata[32:40])
		last := ordinal+1 == manifest.BlockCount
		if last {
			if current != manifest.Oldest || previous != (BlockReference{}) {
				return ErrInvalidCheckpoint
			}
			continue
		}
		if previous == (BlockReference{}) {
			return ErrInvalidCheckpoint
		}
		current = previous
	}
	return nil
}

func (graph *checkpointGraph) visitStack() error {
	for len(graph.stack) > 0 {
		last := len(graph.stack) - 1
		index := graph.stack[last]
		graph.stack = graph.stack[:last]
		if graph.nodes[index].processed {
			continue
		}
		if _, err := graph.process(index); err != nil {
			return err
		}
	}
	return nil
}

func (graph *checkpointGraph) enqueue(requirement BlockRequirement) (int, error) {
	if requirement.Reference.Address == 0 || requirement.Reference.Checksum.IsZero() || requirement.Type < BlockManifest || requirement.Type > BlockValue {
		return 0, ErrInvalidCheckpoint
	}
	index, ok := graph.replica.blockAllocator.index(requirement.Reference.Address)
	if !ok {
		return 0, ErrInvalidCheckpoint
	}
	if knownIndex, found := graph.byAddress[requirement.Reference.Address]; found {
		known := &graph.nodes[knownIndex].requirement
		if checkpointRequirementsConflict(*known, requirement) {
			return 0, ErrInvalidCheckpoint
		}
		if requirement.SnapshotExact {
			known.Snapshot = requirement.Snapshot
			known.SnapshotExact = true
		}
		if requirement.BodySize != 0 {
			known.BodySize = requirement.BodySize
		}
		return knownIndex, nil
	}
	if !graph.replica.blockAllocator.acquired.Test(index) || graph.replica.blockAllocator.released.Test(index) || graph.replica.blockAllocator.pending.Test(index) {
		return 0, ErrInvalidCheckpoint
	}
	if len(graph.nodes) >= graph.limit {
		return 0, ErrCheckpointBlockLimit
	}
	graph.reachable.Set(index)
	nodeIndex := len(graph.nodes)
	graph.nodes = append(graph.nodes, checkpointGraphNode{requirement: requirement})
	graph.byAddress[requirement.Reference.Address] = nodeIndex
	graph.stack = append(graph.stack, nodeIndex)
	return nodeIndex, nil
}

func checkpointRequirementsConflict(known, candidate BlockRequirement) bool {
	if known.Reference != candidate.Reference || known.Type != candidate.Type {
		return true
	}
	if known.SnapshotExact && candidate.SnapshotExact && known.Snapshot != candidate.Snapshot {
		return true
	}
	return known.BodySize != 0 && candidate.BodySize != 0 && known.BodySize != candidate.BodySize
}

func (graph *checkpointGraph) process(index int) (BlockReadResult, error) {
	node := &graph.nodes[index]
	if node.processed {
		return BlockReadResult{}, nil
	}
	result, err := graph.replica.blocks.Read(node.requirement.Reference, node.requirement.Type, graph.buffer)
	if err != nil {
		return BlockReadResult{}, errors.Join(ErrInvalidCheckpoint, err)
	}
	if node.requirement.SnapshotExact && result.Snapshot != node.requirement.Snapshot {
		return BlockReadResult{}, ErrInvalidCheckpoint
	}
	if node.requirement.BodySize != 0 && result.BodySize != node.requirement.BodySize {
		return BlockReadResult{}, ErrInvalidCheckpoint
	}
	input := BlockValidationInput{
		Reference: node.requirement.Reference, NeededAtCheckpoint: graph.checkpoint.PrepareOp(),
		Snapshot: result.Snapshot, Type: node.requirement.Type, Metadata: result.Metadata,
		Body: graph.buffer[:result.BodySize],
	}
	count, err := graph.replica.deps.BlockValidator.ValidateBlock(input, graph.outputs)
	if err != nil || count < 0 || count > len(graph.outputs) {
		return BlockReadResult{}, ErrInvalidCheckpoint
	}
	if node.requirement.Type == BlockManifest && uint32(count) != binary.LittleEndian.Uint32(result.Metadata[40:44]) {
		return BlockReadResult{}, ErrInvalidCheckpoint
	}
	node.processed = true
	graph.replica.rememberBlock(BlockRequirement{
		Reference: node.requirement.Reference, Type: node.requirement.Type,
		Snapshot: result.Snapshot, SnapshotExact: node.requirement.SnapshotExact, BodySize: result.BodySize,
	})
	for output := range count {
		if _, err := graph.enqueue(graph.outputs[output]); err != nil {
			return BlockReadResult{}, err
		}
	}
	return result, nil
}

func (replica *Replica) currentCheckpointTrailerBlocks() (FixedBitSet, []uint64, error) {
	protected, err := NewFixedBitSet(replica.blockAllocator.blockCount)
	if err != nil {
		return FixedBitSet{}, nil, err
	}
	addresses := make([]uint64, 0, 8)
	chains := [...]struct {
		reference   BlockReference
		encodedSize uint64
		blockType   BlockType
	}{
		{BlockReference{Checksum: replica.checkpoint.AcquiredTrailerLastChecksum, Address: replica.checkpoint.AcquiredTrailerLastAddress}, replica.checkpoint.AcquiredTrailerEncodedSize, BlockFreeSet},
		{BlockReference{Checksum: replica.checkpoint.ReleasedTrailerLastChecksum, Address: replica.checkpoint.ReleasedTrailerLastAddress}, replica.checkpoint.ReleasedTrailerEncodedSize, BlockFreeSet},
		{BlockReference{Checksum: replica.checkpoint.SessionTrailerLastChecksum, Address: replica.checkpoint.SessionTrailerLastAddress}, replica.checkpoint.SessionTrailerEncodedSize, BlockClientSessions},
	}
	buffer := make([]byte, replica.config.Cluster.BlockSize)
	payload := replica.config.Cluster.BlockSize - protocol.HeaderSize
	for _, chain := range chains {
		count := uint64(0)
		if chain.encodedSize != 0 {
			count = (chain.encodedSize + payload - 1) / payload
		}
		current := chain.reference
		var bytesRead uint64
		for range count {
			index, ok := replica.blockAllocator.index(current.Address)
			if !ok || current.Checksum.IsZero() || protected.Test(index) {
				return FixedBitSet{}, nil, ErrInvalidCheckpoint
			}
			result, err := replica.blocks.Read(current, chain.blockType, buffer)
			if err != nil || result.Snapshot != uint64(replica.checkpoint.PrepareOp()) {
				return FixedBitSet{}, nil, ErrInvalidCheckpoint
			}
			bytesRead += uint64(result.BodySize)
			if bytesRead > chain.encodedSize {
				return FixedBitSet{}, nil, ErrInvalidCheckpoint
			}
			protected.Set(index)
			addresses = append(addresses, current.Address)
			copy(current.Checksum[:], result.Metadata[:16])
			current.Address = binary.LittleEndian.Uint64(result.Metadata[32:40])
		}
		if current != (BlockReference{}) || bytesRead != chain.encodedSize {
			return FixedBitSet{}, nil, ErrInvalidCheckpoint
		}
	}
	return protected, addresses, nil
}

func (replica *Replica) stageCheckpointReleases(reachable, protected FixedBitSet) error {
	replica.checkpointReleases = replica.checkpointReleases[:0]
	acquired := replica.blockAllocator.Acquired()
	released := replica.blockAllocator.Released()
	for index := uint64(0); index < acquired.Len(); index++ {
		if !acquired.Test(index) || released.Test(index) || replica.blockAllocator.pending.Test(index) || reachable.Test(index) || protected.Test(index) {
			continue
		}
		address := replica.blockAllocator.address(index)
		if err := replica.blockAllocator.Release(address); err != nil {
			replica.rollbackCheckpointReleases()
			return err
		}
		replica.checkpointReleases = append(replica.checkpointReleases, address)
	}
	return nil
}

func (replica *Replica) rollbackCheckpointReleases() {
	for _, address := range replica.checkpointReleases {
		_ = replica.blockAllocator.cancelRelease(address)
	}
	replica.checkpointReleases = replica.checkpointReleases[:0]
}

func installCheckpointReachability(candidate *BlockCheckpointCandidate, reachable FixedBitSet, allocator *BlockAllocator) error {
	if candidate == nil || reachable.Len() > candidate.reachable.Len() {
		return ErrInvalidCheckpoint
	}
	clear(candidate.reachable.words)
	copy(candidate.reachable.words, reachable.words)
	for _, address := range candidate.addresses {
		index, ok := allocator.index(address)
		if !ok || index >= candidate.reachable.Len() {
			return ErrInvalidCheckpoint
		}
		candidate.reachable.Set(index)
	}
	return nil
}
