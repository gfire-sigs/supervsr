package sim

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var (
	ErrHistory         = errors.New("sim: invalid ledger history")
	ErrHistoryCapacity = errors.New("sim: ledger history capacity exceeded")
)

type historyRequest struct {
	lower uint64
}

// History belongs to the workload, not a replica incarnation. Tokens must increase
// at submission; this proves uniqueness without retaining completed requests.
// Calls are serialized in invocation/response order by the simulation scheduler.
type History struct {
	capacity      int
	lastToken     uint64
	submitted     uint64
	completed     uint64
	maximumResult uint64
	prefix        uint64
	digest        protocol.Checksum
	pending       map[uint64]historyRequest
	results       map[uint64]uint64
}

func NewHistory(capacity int) (*History, error) {
	if capacity <= 0 {
		return nil, ErrHistoryCapacity
	}
	return &History{
		capacity: capacity,
		pending:  make(map[uint64]historyRequest, capacity),
		results:  make(map[uint64]uint64, capacity),
	}, nil
}

func (history *History) Submit(token uint64) error {
	if token == 0 || token <= history.lastToken || history.submitted == ^uint64(0) {
		return fmt.Errorf("%w: non-increasing submission token %d", ErrHistory, token)
	}
	if len(history.pending) == history.capacity {
		return ErrHistoryCapacity
	}
	history.pending[token] = historyRequest{lower: history.maximumResult}
	history.lastToken = token
	history.submitted++
	return nil
}

func (history *History) Complete(token uint64, reply []byte) error {
	request, found := history.pending[token]
	if !found {
		return fmt.Errorf("%w: unknown or completed token %d", ErrHistory, token)
	}
	returned, value, err := DecodeLedgerReply(reply)
	if err != nil || returned != token {
		return fmt.Errorf("%w: reply for token %d contains token %d: %v", ErrHistory, token, returned, err)
	}
	if value <= request.lower || value > history.submitted {
		return fmt.Errorf("%w: token %d result %d outside (%d,%d]", ErrHistory, token, value, request.lower, history.submitted)
	}
	if _, duplicate := history.results[value]; duplicate || value <= history.prefix {
		return fmt.Errorf("%w: duplicate result %d", ErrHistory, value)
	}
	if value != history.prefix+1 && len(history.results) == history.capacity {
		return ErrHistoryCapacity
	}
	delete(history.pending, token)
	history.completed++
	history.maximumResult = max(history.maximumResult, value)
	if value == history.prefix+1 {
		history.digest = historyDigest(history.digest, token, value)
		history.prefix = value
		for {
			next := history.prefix + 1
			token, found := history.results[next]
			if !found {
				break
			}
			history.digest = historyDigest(history.digest, token, next)
			delete(history.results, next)
			history.prefix = next
		}
		return nil
	}
	history.results[value] = token
	return nil
}

func (history *History) Submitted() uint64 { return history.submitted }
func (history *History) Completed() uint64 { return history.completed }
func (history *History) PendingCount() int { return len(history.pending) }

// CheckFinal requires quiescence: every logical request has a successful reply.
// The independent count and ordered digest detect loss, duplicate effects, and
// equal-count substitutions even after all replica observation windows expire.
func (history *History) CheckFinal(snapshot LedgerSnapshot) error {
	if len(history.pending) != 0 || history.completed != history.submitted || history.prefix != history.submitted {
		return fmt.Errorf("%w: incomplete history submitted=%d completed=%d contiguous=%d pending=%d", ErrHistory, history.submitted, history.completed, history.prefix, len(history.pending))
	}
	if snapshot.Value != history.submitted || snapshot.Digest != history.digest {
		return fmt.Errorf("%w: final value=%d expected=%d digest=%s expected=%s", ErrHistory, snapshot.Value, history.submitted, snapshot.Digest, history.digest)
	}
	return nil
}

// Encode oracle records independently of the state machine's digest implementation.
func historyDigest(previous protocol.Checksum, token, value uint64) protocol.Checksum {
	var record [32]byte
	copy(record[:16], previous[:])
	binary.LittleEndian.PutUint64(record[16:24], token)
	binary.LittleEndian.PutUint64(record[24:32], value)
	return protocol.ChecksumBytes(record[:])
}
