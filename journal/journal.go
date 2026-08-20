// Package journal defines the durable, append-only log that Keel replays
// to recover a workflow, and the Store interface its backends implement.
//
// # Invariants a backend must hold
//
// The engine depends on all of them, and a backend that breaks one
// corrupts a workflow silently.
//
// One entry per step. Two Append calls for the same step must not both
// succeed, even from different processes at the same moment. Enforce
// this with a conditional write, so every reader sees the invariant.
//
// First writer wins. Once Read returns an entry, that entry must never
// change, or two replays of one invocation disagree.
//
// Append is fenced by the epoch of the lease on the invocation. A
// backend that also stores the lease must reject a stale epoch with
// lease.ErrLeaseLost, and must record the epoch with the entry.
//
// Read needs no lease, because reading cannot conflict. A worker that
// reads in order to replay must still claim the lease first.
//
// # Limits
//
// A step is identified by its position, so an edit that adds, removes,
// or reorders the steps of a live handler shifts every later step.
// VerifyReplay catches this; no backend can.
//
// The log gives exactly-once entries, not exactly-once effects. A crash
// between running a step and appending it runs the step again, so a step
// must be idempotent. The journal never forks.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"

	"github.com/keel/keel/lease"
)

var (
	// ErrStepExists is returned by Append when the step already has an
	// entry of the same name. The caller must adopt the stored entry.
	ErrStepExists = errors.New("journal: step already recorded")

	// ErrNonDeterministic is returned when a replay produces a different
	// step than the journal recorded. The invocation must stop.
	ErrNonDeterministic = errors.New("journal: replay does not match the recorded journal")
)

// Entry is the recorded outcome of one durable step. A replay reads
// entries in order and returns Output instead of re-executing the step.
type Entry struct {
	// Step is the position of the step in the invocation. It identifies
	// the entry, so it must be unique and stable across a replay.
	Step   int             `json:"step"`
	Name   string          `json:"name"`
	Output json.RawMessage `json:"output,omitempty"`
	Err    string          `json:"err,omitempty"`
}

// Store is the durable backing for journals. Each entry is immutable and
// named by its step. Implementations must be concurrency-safe.
type Store interface {
	// Append durably records e at step e.Step under epoch. It returns
	// lease.ErrLeaseLost if epoch is not the current one,
	// ErrStepExists if the same step and name is recorded, and
	// ErrNonDeterministic if the step holds another name.
	Append(ctx context.Context, invocationID string, epoch lease.Epoch, e Entry) error

	// Read yields the invocation's entries in step order, and nothing
	// for an unknown invocation. It needs no lease. The sequence stops
	// at the first error it yields.
	Read(ctx context.Context, invocationID string) iter.Seq2[Entry, error]
}

// Collect drains a Read sequence into a slice. It returns the first
// error, and the entries it read before it, so a caller that needs the
// whole history in one piece does not repeat the loop.
func Collect(seq iter.Seq2[Entry, error]) ([]Entry, error) {
	var entries []Entry
	for e, err := range seq {
		if err != nil {
			return entries, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// VerifyReplay reports whether a replay matches the recorded journal. It
// compares the name at each position that both slices have.
//
// recorded is the journal as stored, and replayed is the steps the
// handler produced this time. A mismatch means the handler changed while
// the invocation was live, so the caller must stop and must not use the
// recorded output. Extra steps on either side are not an error, because
// a replay that is still running has fewer steps than the journal, and a
// handler that grew at the end has more.
func VerifyReplay(recorded, replayed []Entry) error {
	n := min(len(recorded), len(replayed))
	for i := range n {
		if recorded[i].Name != replayed[i].Name {
			return fmt.Errorf("%w: step %d is %q in the journal and %q on replay",
				ErrNonDeterministic, recorded[i].Step, recorded[i].Name, replayed[i].Name)
		}
	}
	return nil
}
