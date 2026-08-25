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

	"github.com/keel/keel/lease"
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

// A Status is the stage of one invocation.
type Status string

const (
	Pending   Status = "pending"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
)

// Terminal reports whether the status is final. A terminal invocation
// never runs again, so a dispatcher drops its marker.
func (s Status) Terminal() bool {
	return s == Succeeded || s == Failed
}

// A Record is the durable statement that an invocation must run. It is
// written once on submission, before the caller gets an answer.
type Record struct {
	Invocation
	Status Status `json:"status"`

	// InputHash tells a retried submission from a reused id. It covers
	// the compacted input, so whitespace alone does not make a conflict.
	InputHash string    `json:"input_hash"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`

	// Epoch is the lease epoch of the holder that wrote this record. A
	// write that carries a lower epoch comes from a stale holder.
	Epoch lease.Epoch `json:"epoch,omitempty"`

	// Attempts counts the runs that reached a worker. A run that found
	// no worker is not an attempt.
	Attempts int `json:"attempts,omitempty"`

	// Failures counts the runs in a row that made no progress. It drives
	// the backoff, and any progress resets it to zero.
	Failures int `json:"failures,omitempty"`

	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
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

	// Update replaces the record. It returns lease.ErrLeaseLost when the
	// stored record carries a later epoch, because a stale holder wrote it.
	Update(ctx context.Context, r Record) error
}

// A WakeupMarker says an invocation must be looked at again at a time.
// The due time is part of the key, so a scan reads only the listing.
type WakeupMarker struct {
	Key string
	Due time.Time
}

// A DueIndex holds the time each unfinished invocation is next due. It
// is separate from Store, because only a dispatcher reads it.
type DueIndex interface {
	// Schedule writes a marker that falls due at the given time. A
	// second Schedule for one key adds a marker and replaces none.
	Schedule(ctx context.Context, key string, due time.Time) error

	// Due yields every marker due at or before now, earliest first.
	Due(ctx context.Context, now time.Time) iter.Seq2[WakeupMarker, error]

	// Forget drops the exact marker, and not every marker that names
	// its invocation. A repeated Schedule can leave a second one.
	Forget(ctx context.Context, m WakeupMarker) error
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
