package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/keel/keel/invocation"
	"github.com/keel/keel/lease"
	"github.com/keel/keel/worker"
)

// The dispatcher rests on four rules.
//
//  1. The record is the authority and the marker is a hint. A dispatch
//     reads the record first, and the record wins a disagreement.
//  2. A marker may exist for a finished invocation, but a marker must
//     never be missing for an unfinished one. So the record becomes
//     terminal before its marker goes, and a new marker is written
//     before an old one goes.
//  3. The lease grants the exclusion and the marker does not. A marker
//     that comes due early costs one failed Claim and never a second run.
//  4. Only the lease holder writes the record. The record carries the
//     epoch, so a stale holder's write is rejected.

const (
	// defaultInterval is how long a dispatcher waits between the scans.
	// It also bounds how long an invocation waits for its first worker.
	defaultInterval = 30 * time.Second

	// defaultDispatchConcurrency bounds the claims in flight. Each one is
	// a short run of storage calls, so it follows storage throughput.
	defaultDispatchConcurrency = 32

	// defaultExecuteConcurrency bounds the invocations one engine drives.
	// Each one is mostly idle, so it follows memory.
	defaultExecuteConcurrency = 1000

	// defaultLeaseTTL bounds how long a crashed engine keeps an
	// invocation. The driver renews it while the attempt progresses.
	defaultLeaseTTL = 2 * time.Minute

	// handoffBuffer holds a burst of submissions. A full channel drops
	// the send, because the next scan finds the marker anyway.
	handoffBuffer = 256

	minBackoff = time.Second
	maxBackoff = 5 * time.Minute
)

// Config holds what a Dispatcher needs. Every duration and every bound
// takes a default when it is zero.
type Config struct {
	Records  invocation.Store
	DueIndex invocation.DueIndex
	Locker   lease.Locker
	Workers  worker.Registry
	Executor Executor

	// Owner identifies this engine in a lease. Two engines that share a
	// store must not share an owner, or one takes the other's lease.
	Owner string

	// DispatchConcurrency bounds the claims in flight. Each claim is a
	// short run of storage calls, so this follows storage throughput.
	DispatchConcurrency int

	// ExecuteConcurrency bounds the invocations this engine drives at
	// one time. Each one is mostly idle, so this follows memory.
	ExecuteConcurrency int

	// LeaseTTL is how long a crashed engine keeps an invocation. A
	// shorter ttl recovers sooner and costs more renewal writes.
	LeaseTTL time.Duration

	Interval time.Duration
	Log      *slog.Logger
}

// A Dispatcher finds the invocations that are due and drives them. It
// separates the two costs: a claim is short, and a run may take days.
type Dispatcher struct {
	cfg Config

	// handoff carries a new marker from a submission, so it need not
	// wait for the next scan.
	handoff chan invocation.WakeupMarker

	// dispatchSlots bounds the claims and executeSlots bounds the runs.
	// A dispatch takes an execute slot before it claims, so a saturated
	// engine blocks while it holds nothing.
	dispatchSlots chan struct{}
	executeSlots  chan struct{}

	wg sync.WaitGroup

	// mu guards inflight, which drops a key that the scan and the
	// handoff both yield.
	mu       sync.Mutex
	inflight map[string]bool
}

// New returns a Dispatcher. It returns an error if a required part of
// cfg is missing, because a nil store fails later at an unhelpful place.
func New(cfg Config) (*Dispatcher, error) {
	switch {
	case cfg.Records == nil:
		return nil, errors.New("dispatch: nil record store")
	case cfg.DueIndex == nil:
		return nil, errors.New("dispatch: nil due index")
	case cfg.Locker == nil:
		return nil, errors.New("dispatch: nil locker")
	case cfg.Workers == nil:
		return nil, errors.New("dispatch: nil worker registry")
	case cfg.Executor == nil:
		return nil, errors.New("dispatch: nil executor")
	case cfg.Owner == "":
		return nil, errors.New("dispatch: empty owner")
	}
	if cfg.DispatchConcurrency <= 0 {
		cfg.DispatchConcurrency = defaultDispatchConcurrency
	}
	if cfg.ExecuteConcurrency <= 0 {
		cfg.ExecuteConcurrency = defaultExecuteConcurrency
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultLeaseTTL
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	return &Dispatcher{
		cfg:           cfg,
		handoff:       make(chan invocation.WakeupMarker, handoffBuffer),
		dispatchSlots: make(chan struct{}, cfg.DispatchConcurrency),
		executeSlots:  make(chan struct{}, cfg.ExecuteConcurrency),
		inflight:      make(map[string]bool),
	}, nil
}

// Notify says that an invocation is due now, so it need not wait for the
// next scan. A full channel drops the marker, because the scan finds it.
func (d *Dispatcher) Notify(m invocation.WakeupMarker) {
	select {
	case d.handoff <- m:
	default:
	}
}

// Run drives the invocations until ctx ends. It returns after every
// attempt in flight stops and releases its lease.
func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.cfg.Interval)
	defer ticker.Stop()

	d.startDue(ctx)
	for {
		select {
		case <-ctx.Done():
			d.wg.Wait()
			return nil
		case m := <-d.handoff:
			d.start(ctx, m)
		case <-ticker.C:
			d.startDue(ctx)
		}
	}
}

// startDue starts an attempt for every invocation that is due now. The
// listing is ordered by the due time, so one call reads the ready work
// and not the whole backlog.
func (d *Dispatcher) startDue(ctx context.Context) {
	for m, err := range d.cfg.DueIndex.Due(ctx, time.Now()) {
		if err != nil {
			d.cfg.Log.Error("keel: list the due index", "error", err)
			return
		}
		if ctx.Err() != nil {
			return
		}
		d.start(ctx, m)
	}
}

// start takes one marker through a claim and then a run. It blocks while
// every dispatch slot is busy, which is the backpressure that bounds the
// scan.
func (d *Dispatcher) start(ctx context.Context, m invocation.WakeupMarker) {
	if !d.hold(m.Key) {
		return
	}
	if !acquire(ctx, d.dispatchSlots) {
		d.free(m.Key)
		return
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		dr, err := d.claim(ctx, m)
		// The storage part is over, so give the slot back before the run,
		// which may take days.
		<-d.dispatchSlots
		if err != nil {
			d.cfg.Log.Error("keel: dispatch", "key", m.Key, "error", err)
		}
		if dr == nil {
			d.free(m.Key)
			return
		}

		again, err := dr.drive(ctx)
		<-d.executeSlots
		if err != nil {
			d.cfg.Log.Error("keel: drive", "key", m.Key, "error", err)
		}

		// Free the key first, or the handoff finds the invocation still
		// in flight and drops the marker.
		d.free(m.Key)
		if again {
			d.Notify(dr.marker())
		}
	}()
}

// claim takes one marker up to the point the invocation is running. It
// returns a driver that owns the lease, the marker, and one execute
// slot, and nil when there is nothing to run.
func (d *Dispatcher) claim(ctx context.Context, m invocation.WakeupMarker) (*driver, error) {
	// Rule 1. Create writes the record before the marker, so a marker
	// with no record outlived its invocation. Leave it for an operator.
	rec, err := d.cfg.Records.Get(ctx, m.Key)
	switch {
	case errors.Is(err, invocation.ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("reading the record: %w", err)
	}
	// This is what makes a crash after the terminal write recoverable,
	// and what makes a duplicate marker converge.
	if rec.Status.Terminal() {
		return nil, d.cfg.DueIndex.Forget(ctx, m)
	}

	// Admission comes before the claim, so a saturated engine blocks
	// while it holds nothing. The other order marks invocations running
	// that nobody drives.
	if !acquire(ctx, d.executeSlots) {
		return nil, nil
	}
	driving := false
	defer func() {
		if !driving {
			<-d.executeSlots
		}
	}()

	// No worker is not a failure, and it must not count an attempt. A
	// service may have no worker for days.
	w, err := d.cfg.Workers.Pick(rec.Service, rec.Handler)
	switch {
	case errors.Is(err, worker.ErrNoWorker):
		return nil, d.reschedule(ctx, m, time.Now().Add(d.cfg.Interval))
	case err != nil:
		return nil, fmt.Errorf("picking a worker: %w", err)
	}

	// Rule 3. Another holder has it, so touch nothing.
	l, err := d.cfg.Locker.Claim(ctx, m.Key, d.cfg.Owner, d.cfg.LeaseTTL)
	switch {
	case errors.Is(err, lease.ErrClaimHeld):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("claiming: %w", err)
	}

	// The marker due time and the lease expiry are the same event, so a
	// dead engine returns its work to the scan when the lease frees.
	if err := d.reschedule(ctx, m, l.Expires()); err != nil {
		_ = d.cfg.Locker.Release(ctx, l)
		return nil, err
	}

	// Rule 4. The epoch goes in the record, so a stale holder is fenced.
	rec.Status = invocation.Running
	rec.Epoch = l.Epoch
	rec.Attempts++
	rec.UpdatedAt = time.Now().UTC()
	if err := d.cfg.Records.Update(ctx, rec); err != nil {
		_ = d.cfg.Locker.Release(ctx, l)
		return nil, fmt.Errorf("recording the run: %w", err)
	}

	driving = true
	return newDriver(d, rec, w, l), nil
}

// reschedule rewrites the marker at a new due time. It writes the new
// marker before it drops the old, so no crash between the two loses the
// work.
func (d *Dispatcher) reschedule(ctx context.Context, m invocation.WakeupMarker, due time.Time) error {
	// A marker is named by whole seconds, so the same second is the same
	// object and dropping the old one would drop the new one.
	if due.Unix() == m.Due.Unix() {
		return nil
	}
	if err := d.cfg.DueIndex.Schedule(ctx, m.Key, due); err != nil {
		return fmt.Errorf("scheduling: %w", err)
	}
	if err := d.cfg.DueIndex.Forget(ctx, m); err != nil {
		return fmt.Errorf("unscheduling: %w", err)
	}
	return nil
}

// hold takes the key, and reports false when an attempt already has it.
// The scan and the handoff both yield a new key.
func (d *Dispatcher) hold(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inflight[key] {
		return false
	}
	d.inflight[key] = true
	return true
}

// free gives the key back. It frees a local map, and it is not the lease
// that Locker.Release drops.
func (d *Dispatcher) free(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.inflight, key)
}

// acquire takes one slot, and reports false when ctx ends first.
func acquire(ctx context.Context, slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// backoff grows the wait between the failed attempts of one invocation.
// It never gives up, because a give-up policy is not settled.
func backoff(failures int) time.Duration {
	if failures < 1 {
		return minBackoff
	}
	if failures > 20 {
		return maxBackoff
	}
	d := minBackoff << (failures - 1)
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}
