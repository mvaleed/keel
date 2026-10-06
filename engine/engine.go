// Package engine owns the rules of a submission: what a client may ask
// for, and what it is told. A transport decodes a request and calls the
// engine; it must not hold a rule of its own, or a second transport
// repeats it. Package dispatch runs what this package records.
package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/keel/keel/invocation"
	"github.com/keel/keel/lease"
	"github.com/keel/keel/worker"
)

// ErrInputConflict is returned by Submit when the address holds another
// input. The id is reused, and the caller must pick a new one.
var ErrInputConflict = errors.New("engine: id submitted with a different input")

// A Notifier learns that an invocation is due, so a submission need
// not wait for the next scan. Notify must not block.
type Notifier interface {
	Notify(m invocation.WakeupMarker)
}

// A Canceler stops the attempt it drives for key. It must not block,
// and it may be nil, because the record alone cancels an invocation
// that no attempt is driving.
type Canceler interface {
	CancelRun(key string)
}

// Config holds what an Engine needs. One backend may satisfy every
// store, and the engine must not know whether it does.
type Config struct {
	Records invocation.Store
	Workers worker.Registry

	// Notifier takes a new marker at once. It may be nil, because the
	// handoff is latency and never correctness.
	Notifier Notifier

	// Canceler stops the in-flight attempt of a cancelled invocation.
	// It may be nil, because the record alone is the authority.
	Canceler Canceler
}

// Engine records the invocations a client submits, and answers the
// questions a client asks about them.
type Engine struct {
	cfg Config
}

// New returns an Engine. It returns an error if a required part of cfg
// is missing, because a nil store fails later at an unhelpful place.
func New(cfg Config) (*Engine, error) {
	switch {
	case cfg.Records == nil:
		return nil, errors.New("engine: nil record store")
	case cfg.Workers == nil:
		return nil, errors.New("engine: nil worker registry")
	}
	return &Engine{cfg: cfg}, nil
}

// A Submission is the answer to Submit. Created is false when the call
// repeated a submission that already existed.
type Submission struct {
	Record  invocation.Record
	Created bool
}

// Submit records that inv must run, and returns before it does. The
// caller supplies the id, so a repeat of one call is not a second run.
//
// It returns ErrInputConflict when the address holds another input, and
// invocation.ErrInvalid for an address that cannot be stored. A service
// with no live worker is accepted, because a worker may start later.
func (e *Engine) Submit(ctx context.Context, inv invocation.Invocation) (Submission, error) {
	if err := inv.Validate(); err != nil {
		return Submission{}, err
	}
	input, err := invocation.Compact(inv.Input)
	if err != nil {
		return Submission{}, err
	}
	inv.Input = input

	rec := invocation.Record{
		Invocation: inv,
		Status:     invocation.Pending,
		InputHash:  invocation.HashInput(input),
		CreatedAt:  time.Now().UTC(),
	}

	switch err := e.cfg.Records.Create(ctx, rec); {
	case err == nil:
		e.notify(invocation.WakeupMarker{Key: rec.Key(), Due: rec.CreatedAt})
		return Submission{Record: rec, Created: true}, nil
	case errors.Is(err, invocation.ErrExists):
		return e.settleDuplicate(ctx, rec)
	default:
		return Submission{}, err
	}
}

// notify hands a marker to the notifier. An absent notifier is not
// an error, because the next scan finds the marker anyway.
func (e *Engine) notify(m invocation.WakeupMarker) {
	if e.cfg.Notifier != nil {
		e.cfg.Notifier.Notify(m)
	}
}

// settleDuplicate answers a Submit whose address is taken. The same
// input is a retry, and another input is an id that two callers want.
func (e *Engine) settleDuplicate(ctx context.Context, want invocation.Record) (Submission, error) {
	got, err := e.cfg.Records.Get(ctx, want.Key())
	if err != nil {
		return Submission{}, err
	}
	if got.InputHash != want.InputHash {
		return Submission{}, fmt.Errorf("%w: %s", ErrInputConflict, want.Key())
	}
	return Submission{Record: got}, nil
}

// Lookup returns the recorded invocation, and invocation.ErrNotFound if
// there is none. A caller polls it, because Submit does not wait.
func (e *Engine) Lookup(ctx context.Context, inv invocation.Invocation) (invocation.Record, error) {
	if err := inv.Validate(); err != nil {
		return invocation.Record{}, err
	}
	return e.cfg.Records.Get(ctx, inv.Key())
}

// List yields every recorded invocation under the service and the
// handler, in key order. An empty service or handler widens the list.
func (e *Engine) List(ctx context.Context, service, handler string) ([]invocation.Record, error) {
	var out []invocation.Record
	for r, readErr := range e.cfg.Records.List(ctx, service, handler) {
		if readErr != nil {
			return nil, readErr
		}
		out = append(out, r)
	}
	return out, nil
}

// Cancel marks the invocation cancelled and stops its attempt. The
// record is written first, so the cancellation survives a crash; the
// Canceler then stops the attempt in flight. Cancelling a terminal
// invocation changes nothing.
//
// It returns invocation.ErrNotFound for an address that was never
// recorded.
func (e *Engine) Cancel(ctx context.Context, inv invocation.Invocation) (invocation.Record, error) {
	if err := inv.Validate(); err != nil {
		return invocation.Record{}, err
	}
	key := inv.Key()

	// The attempt in flight may write the record too, so the write can
	// lose the race a few times. A cancelled attempt never revives,
	// because the driver re-reads the record before it writes again.
	for range 3 {
		rec, err := e.cfg.Records.Get(ctx, key)
		if err != nil {
			return invocation.Record{}, err
		}
		if rec.Status.Terminal() {
			return rec, nil
		}
		rec.Status = invocation.Cancelled
		rec.UpdatedAt = time.Now().UTC()
		switch err := e.cfg.Records.Update(ctx, rec); {
		case err == nil:
			if e.cfg.Canceler != nil {
				e.cfg.Canceler.CancelRun(key)
			}
			return rec, nil
		case errors.Is(err, lease.ErrLeaseLost):
			continue
		default:
			return invocation.Record{}, err
		}
	}
	return invocation.Record{}, fmt.Errorf("engine: cancel %s lost the race three times", key)
}

// RegisterWorker adds the worker, or keeps the one that has the same ID
// live. It returns how long the worker may wait before it calls again.
func (e *Engine) RegisterWorker(w worker.Worker) (time.Duration, error) {
	if err := e.cfg.Workers.Register(w); err != nil {
		return 0, err
	}
	return worker.Heartbeat, nil
}

// DeregisterWorker drops the worker, which it calls when it stops. It
// is not an error to drop a worker the registry does not hold.
func (e *Engine) DeregisterWorker(id string) error {
	return e.cfg.Workers.Deregister(id)
}
