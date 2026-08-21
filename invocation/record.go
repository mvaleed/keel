package invocation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"time"
)

var (
	// ErrInvalid is returned when a part of the key is empty, too long,
	// or holds a character that a storage key must not carry.
	ErrInvalid = errors.New("invocation: invalid name")

	// ErrExists is returned by Create when the address is taken. The
	// caller must compare InputHash to tell a retry from a collision.
	ErrExists = errors.New("invocation: already recorded")

	// ErrNotFound is returned by Get for an address that was never
	// recorded.
	ErrNotFound = errors.New("invocation: not recorded")
)

// A Status is the stage of one invocation. It is the only field of a
// Record that ever changes after Create.
type Status string

const (
	Pending   Status = "pending"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
)

// A Record is the durable statement that an invocation must run. It is
// written once on submission, before the caller gets an answer.
type Record struct {
	Invocation
	Status Status `json:"status"`

	// InputHash tells a retried submission from a reused id. It covers
	// the compacted input, so whitespace alone does not make a conflict.
	InputHash string    `json:"input_hash"`
	CreatedAt time.Time `json:"created_at"`
}

// A Store keeps the invocation records. Implementations must be
// concurrency-safe.
type Store interface {
	// Create writes r once, and returns ErrExists if the address is
	// taken. It must be a conditional write, because two submissions
	// of one address must not both succeed.
	Create(ctx context.Context, r Record) error

	// Get returns the record at key, and ErrNotFound if there is none.
	Get(ctx context.Context, key string) (Record, error)
}

// A PendingIndex lists the invocations that still need to run. It is
// separate from Store, because only a dispatcher reads it.
type PendingIndex interface {
	// Pending yields the key of every invocation that is not dispatched.
	// The order is unspecified.
	Pending(ctx context.Context) iter.Seq2[string, error]

	// ClearPending drops key from the index. Clearing a key that is not
	// in the index is not an error.
	ClearPending(ctx context.Context, key string) error
}

// HashInput returns the hash that Record.InputHash holds. Pass the
// compacted input, so that whitespace does not change the answer.
func HashInput(input []byte) string {
	sum := sha256.Sum256(input)
	return hex.EncodeToString(sum[:])
}

// Compact removes the whitespace from raw, so that two submissions
// that differ only in formatting hash the same. It returns nil for no
// input, which makes an absent and an empty input one case.
func Compact(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	var out bytes.Buffer
	if err := json.Compact(&out, raw); err != nil {
		return nil, fmt.Errorf("%w: input is not JSON", ErrInvalid)
	}
	return out.Bytes(), nil
}
