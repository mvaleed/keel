package dispatch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keel/keel/invocation"
	"github.com/keel/keel/lease"
	"github.com/keel/keel/worker"
)

// errStalled ends an attempt that made no progress for a whole lease
// ttl. The engine cannot tell a stalled worker from a dead one.
var errStalled = errors.New("dispatch: the attempt made no progress")

// A driver owns the lease, the marker, and the record of one attempt for
// the whole run. The executor it calls owns none of the three.
type driver struct {
	d   *Dispatcher
	rec invocation.Record
	w   worker.Worker
	l   *lease.Lease

	// steps counts the progress reports and last holds the time of the
	// newest one. The executor writes both from its own goroutine.
	steps atomic.Uint64
	last  atomic.Int64

	// mu guards held, because the renewer moves the marker while the
	// outcome reads it.
	mu   sync.Mutex
	held invocation.WakeupMarker
}

func newDriver(d *Dispatcher, rec invocation.Record, w worker.Worker, l *lease.Lease) *driver {
	dr := &driver{d: d, rec: rec, w: w, l: l}
	dr.held = invocation.WakeupMarker{Key: rec.Key(), Due: l.Expires()}
	dr.last.Store(time.Now().UnixNano())
	return dr
}

// drive runs one attempt to its end. It reports again when the next
// attempt must start at once instead of after a backoff.
func (dr *driver) drive(ctx context.Context) (again bool, err error) {
	// The renewer cancels this context when the lease is lost or when the
	// attempt stalls, so the executor stops with it.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		// Release under an uncancelled context, or a shutdown leaves the
		// invocation waiting out the whole ttl.
		_ = dr.d.cfg.Locker.Release(context.WithoutCancel(ctx), dr.l)
	}()

	stop := dr.hold(runCtx, cancel)
	res, execErr := dr.d.cfg.Executor.Execute(runCtx, Attempt{
		Record:   dr.rec,
		Worker:   dr.w,
		Epoch:    dr.l.Epoch,
		Progress: dr.progress,
	})
	stop()

	if execErr != nil || !res.Done {
		// The attempt stopped early, which is neither a success nor a
		// failure. Give it back to the scan.
		return dr.retry(ctx, execErr)
	}
	return false, dr.finish(ctx, res)
}

// progress records that the invocation advanced. The executor calls it
// on its own goroutine, so it must not block.
func (dr *driver) progress() {
	dr.steps.Add(1)
	dr.last.Store(time.Now().UnixNano())
}

// hold starts the renewer and returns a function that stops it and waits
// for it. The renewer cancels the attempt when it can no longer hold the
// lease.
func (dr *driver) hold(ctx context.Context, cancel context.CancelFunc) (stop func()) {
	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		// A third of the ttl gives two failed renewals before the expiry.
		ticker := time.NewTicker(dr.d.cfg.LeaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := dr.keepalive(ctx); err != nil {
					dr.d.cfg.Log.Error("keel: hold the lease", "key", dr.rec.Key(), "error", err)
					cancel()
					return
				}
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			<-stopped
		})
	}
}

// keepalive holds the lease for one more period. It renews on evidence:
// a journal entry proves the attempt advanced, and a timer proves only
// that this engine is optimistic.
func (dr *driver) keepalive(ctx context.Context) error {
	idle := time.Since(time.Unix(0, dr.last.Load()))
	if idle >= dr.d.cfg.LeaseTTL {
		return errStalled
	}
	if err := dr.d.cfg.Locker.Renew(ctx, dr.l, dr.d.cfg.LeaseTTL); err != nil {
		return err
	}
	// The marker due time and the lease expiry are one event, so a
	// renewal must move both or neither.
	return dr.move(ctx, dr.l.Expires())
}

// move rewrites the marker this attempt owns at a new due time.
func (dr *driver) move(ctx context.Context, due time.Time) error {
	dr.mu.Lock()
	defer dr.mu.Unlock()
	if err := dr.d.reschedule(ctx, dr.held, due); err != nil {
		return err
	}
	dr.held = invocation.WakeupMarker{Key: dr.held.Key, Due: due}
	return nil
}

// marker returns the marker the attempt left behind.
func (dr *driver) marker() invocation.WakeupMarker {
	dr.mu.Lock()
	defer dr.mu.Unlock()
	return dr.held
}

// retry gives an unfinished invocation back to the scan. An attempt that
// made progress was alive, so it waits for nothing and starts again.
func (dr *driver) retry(ctx context.Context, execErr error) (bool, error) {
	again := dr.steps.Load() > 0
	if again {
		dr.rec.Failures = 0
	} else {
		dr.rec.Failures++
	}
	dr.rec.UpdatedAt = time.Now().UTC()

	wait := time.Duration(0)
	if !again {
		wait = backoff(dr.rec.Failures)
	}
	return again, errors.Join(
		execErr,
		dr.d.cfg.Records.Update(ctx, dr.rec),
		dr.move(ctx, time.Now().Add(wait)),
	)
}

// finish records the outcome and drops the marker. Rule 2: the record
// becomes terminal first, or the other order loses the invocation.
func (dr *driver) finish(ctx context.Context, res Result) error {
	dr.rec.Status = invocation.Succeeded
	dr.rec.Output = res.Output
	if res.Err != nil {
		dr.rec.Status = invocation.Failed
		dr.rec.Error = res.Err.Error()
		dr.rec.Output = nil
	}
	dr.rec.Failures = 0
	dr.rec.UpdatedAt = time.Now().UTC()
	if err := dr.d.cfg.Records.Update(ctx, dr.rec); err != nil {
		return err
	}
	return dr.d.cfg.DueIndex.Forget(ctx, dr.marker())
}
