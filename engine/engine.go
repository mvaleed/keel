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
	"github.com/keel/keel/worker"
)

// ErrInputConflict is returned by Submit when the address holds another
// input. The id is reused, and the caller must pick a new one.
var ErrInputConflict = errors.New("engine: id submitted with a different input")

// A Dispatcher learns that an invocation is due, so a submission need
// not wait for the next scan. Notify must not block.
type Dispatcher interface {
	Notify(m invocation.WakeupMarker)
}

// Config holds what an Engine needs. One backend may satisfy every
// store, and the engine must not know whether it does.
type Config struct {
	Records invocation.Store
	Workers worker.Registry

	// Dispatcher takes a new marker at once. It may be nil, because the
	// handoff is latency and never correctness.
	Dispatcher Dispatcher
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

// notify hands a marker to the dispatcher. An absent dispatcher is not
// an error, because the next scan finds the marker anyway.
func (e *Engine) notify(m invocation.WakeupMarker) {
	if e.cfg.Dispatcher != nil {
		e.cfg.Dispatcher.Notify(m)
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
