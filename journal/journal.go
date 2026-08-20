// Package journal defines the durable, append-only log that Keel replays
// to recover a workflow, and the Store interface its backends implement.
//
// # Contract for an implementer
//
// A backend must hold these invariants. The engine depends on all of
// them, and a backend that breaks one corrupts a workflow silently.
//
// One entry per step. A step number identifies an entry inside an
// invocation. Two Append calls for the same step must not both succeed,
// even from different processes at the same moment. The second call must
// return ErrStepExists and must not change the stored entry. Enforce
// this at write time with a conditional write, not at read time with a
// filter, so every reader sees the invariant without extra logic.
//
// First writer wins. Once Read returns an entry for a step, that entry
// must never change. A replay gives the recorded output to the caller,
// so a later value would make two replays of one invocation disagree.
// This is why a duplicate Append fails instead of overwriting.
//
// Append is fenced by an epoch. The epoch comes from the lease on the
// invocation, and the backend records it with the entry, because it
// tells an operator which holder wrote the step. A backend that also
// stores the lease must reject an Append whose epoch is not the current
// one with lease.ErrLeaseLost.
//
// Read needs no lease. Reading has no side effect and cannot conflict,
// so an observer never has to contend with the running engine. A worker
// that reads in order to replay must still claim the lease first, or it
// decides what to run from a history another writer is still changing.
//
// # A handler must not change while an invocation is live
//
// A step is identified by its position, so adding, removing, or
// reordering the steps of a handler shifts every step after the change.
// A replay then reads one step's output as another step's, which no
// backend can detect. Treat such an edit as unsafe while an invocation
// of that handler is live. Keeping the previous version of the endpoint
// alive, and pinning a live invocation to it, would remove this limit.
//
// A replay must therefore compare the name it expects at each position
// against the name the journal recorded, and stop with
// ErrNonDeterministic on a mismatch. VerifyReplay does this comparison.
// The name comes from the SDK that ran the step, so the check proves
// only that two runs agree, not that either is right.
//
// # Duplicate work is expected
//
// This interface gives exactly-once entries, not exactly-once effects. A
// process can crash after it runs a step and before it appends. The step
// then runs again on the next attempt, so a step must be idempotent.
// What the interface does guarantee is that the journal never forks.
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
