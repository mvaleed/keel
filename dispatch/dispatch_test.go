package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/keel/keel/dispatch"
	"github.com/keel/keel/invocation"
	"github.com/keel/keel/lease"
)

// build wires a dispatcher to the fakes, and seeds the index with a
// marker that is due for each record. tune adjusts the config first.
func build(t *testing.T, ex dispatch.Executor, tune func(*dispatch.Config), seed ...invocation.Record) (*dispatch.Dispatcher, *fakeIndex, *fakeStore, *fakeLocker) {
	t.Helper()
	store, locker, idx := newStore(), &fakeLocker{}, newIndex()
	for _, r := range seed {
		store.records[r.Key()] = r
		if err := idx.Schedule(t.Context(), r.Key(), r.CreatedAt); err != nil {
			t.Fatalf("seeding %s: %v", r.Key(), err)
		}
	}

	cfg := dispatch.Config{
		Records: store, DueIndex: idx, Locker: locker,
		Workers: registry(t, "http://unused"), Executor: ex,
		Owner: "engine-a", DispatchConcurrency: 4,
		Interval: 20 * time.Millisecond, Log: quiet(),
	}
	if tune != nil {
		tune(&cfg)
	}
	d, err := dispatch.New(cfg)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}
	return d, idx, store, locker
}

// shortLease makes a renewal happen in a test without a wait.
func shortLease(c *dispatch.Config) { c.LeaseTTL = 60 * time.Millisecond }

func dispatcher(t *testing.T, ex dispatch.Executor, seed ...invocation.Record) (*dispatch.Dispatcher, *fakeIndex, *fakeStore, *fakeLocker) {
	t.Helper()
	return build(t, ex, nil, seed...)
}

// start runs the dispatcher, and stops it when the test ends.
func start(t *testing.T, d *dispatch.Dispatcher) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return")
		}
	})
}

func TestDispatcherRunsADueInvocation(t *testing.T) {
	t.Parallel()

	rec := pending("demo", "Charge", "id-1")
	ex := newExecutor(succeeds(`{"ok":true}`))
	d, idx, store, _ := dispatcher(t, ex, rec)
	start(t, d)

	eventually(t, "the invocation to succeed", func() bool {
		return status(t, store, rec.Key()).Status == invocation.Succeeded
	})
	got := status(t, store, rec.Key())
	if string(got.Output) != `{"ok":true}` {
		t.Fatalf("output = %s, want the executor's", got.Output)
	}
	if got.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", got.Attempts)
	}
	if got.Epoch == 0 {
		t.Fatal("record carries no epoch, so no writer is fenced")
	}
	eventually(t, "the marker to go", func() bool { return idx.count() == 0 })
}

func TestDispatcherRecordsAHandlerFailure(t *testing.T) {
	t.Parallel()

	rec := pending("demo", "Charge", "id-2")
	ex := newExecutor(func(dispatch.Attempt) (dispatch.Result, error) {
		return dispatch.Result{Done: true, Err: errors.New("card declined")}, nil
	})
	d, idx, store, _ := dispatcher(t, ex, rec)
	start(t, d)

	eventually(t, "the invocation to fail", func() bool {
		return status(t, store, rec.Key()).Status == invocation.Failed
	})
	if got := status(t, store, rec.Key()); got.Error != "card declined" {
		t.Fatalf("error = %q, want the handler's", got.Error)
	}
	eventually(t, "the marker to go", func() bool { return idx.count() == 0 })
}

func TestDispatcherRetriesAnUnfinishedAttempt(t *testing.T) {
	t.Parallel()

	// A dropped connection is neither a success nor a failure, so the
	// invocation must stay unfinished and keep its marker.
	rec := pending("demo", "Charge", "id-3")
	ex := newExecutor(func(dispatch.Attempt) (dispatch.Result, error) {
		return dispatch.Result{Done: false}, nil
	})
	d, idx, store, _ := dispatcher(t, ex, rec)
	start(t, d)

	eventually(t, "the first attempt", func() bool { return ex.count(rec.Key()) == 1 })
	eventually(t, "the marker to move to the backoff", func() bool {
		markers := idx.all()
		return len(markers) == 1 && markers[0].Due.After(time.Now())
	})
	if got := status(t, store, rec.Key()); got.Status != invocation.Running {
		t.Fatalf("status = %q, want running", got.Status)
	}
}

func TestDispatcherSkipsATerminalRecord(t *testing.T) {
	t.Parallel()

	rec := pending("demo", "Charge", "id-4")
	rec.Status = invocation.Succeeded
	ex := newExecutor(succeeds(`{}`))
	d, idx, _, locker := dispatcher(t, ex, rec)
	start(t, d)

	eventually(t, "the marker to go", func() bool { return idx.count() == 0 })
	if ex.total() != 0 {
		t.Fatalf("executor ran %d times for a terminal record", ex.total())
	}
	if claims, _ := locker.counts(); claims != 0 {
		t.Fatalf("claims = %d, want none for a terminal record", claims)
	}
}

func TestDispatcherWaitsForAWorker(t *testing.T) {
	t.Parallel()

	// A service with no live worker is not a failure. It must not count
	// an attempt, because a worker may start days later.
	rec := pending("unstaffed", "Charge", "id-5")
	ex := newExecutor(succeeds(`{}`))
	d, idx, store, locker := dispatcher(t, ex, rec)
	start(t, d)

	eventually(t, "the marker to move", func() bool {
		markers := idx.all()
		return len(markers) == 1 && markers[0].Due.After(rec.CreatedAt)
	})
	got := status(t, store, rec.Key())
	if got.Status != invocation.Pending || got.Attempts != 0 {
		t.Fatalf("record = %+v, want pending with no attempt", got)
	}
	if claims, _ := locker.counts(); claims != 0 {
		t.Fatalf("claims = %d, want none without a worker", claims)
	}
}

func TestDispatcherLeavesAClaimedInvocation(t *testing.T) {
	t.Parallel()

	rec := pending("demo", "Charge", "id-6")
	ex := newExecutor(succeeds(`{}`))
	d, idx, store, locker := dispatcher(t, ex, rec)
	locker.claimErr = lease.ErrClaimHeld
	start(t, d)

	// Give the scan a few turns, so a wrong write has time to appear.
	time.Sleep(100 * time.Millisecond)
	if ex.total() != 0 {
		t.Fatalf("executor ran %d times under another holder", ex.total())
	}
	if got := status(t, store, rec.Key()); got.Status != invocation.Pending {
		t.Fatalf("status = %q, want pending", got.Status)
	}
	if idx.count() != 1 {
		t.Fatalf("markers = %d, want the one it must not touch", idx.count())
	}
}

func TestDispatcherIgnoresAMarkerThatIsNotDue(t *testing.T) {
	t.Parallel()

	rec := pending("demo", "Charge", "id-7")
	ex := newExecutor(succeeds(`{}`))
	d, idx, _, _ := dispatcher(t, ex)
	if err := idx.Schedule(t.Context(), rec.Key(), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	start(t, d)

	time.Sleep(100 * time.Millisecond)
	if ex.total() != 0 {
		t.Fatalf("executor ran %d times for a future marker", ex.total())
	}
}

func TestDispatcherMovesTheMarkerToTheLeaseExpiry(t *testing.T) {
	t.Parallel()

	// The lease expiry and the marker due time are the same event, so a
	// dead engine returns its work when the lease becomes claimable.
	rec := pending("demo", "Charge", "id-8")
	held := make(chan struct{})
	ex := newExecutor(func(dispatch.Attempt) (dispatch.Result, error) {
		<-held
		return dispatch.Result{Done: true}, nil
	})
	d, idx, _, _ := dispatcher(t, ex, rec)
	start(t, d)

	eventually(t, "the marker to move to the lease expiry", func() bool {
		markers := idx.all()
		return len(markers) == 1 && markers[0].Due.After(time.Now().Add(time.Minute))
	})
	close(held)
}

func TestDispatcherReleasesTheLeaseOnShutdown(t *testing.T) {
	t.Parallel()

	// Without the release, an invocation in flight waits out the whole
	// ttl before another engine may take it.
	rec := pending("demo", "Charge", "id-9")
	ex := newExecutor(succeeds(`{}`))
	d, _, _, locker := dispatcher(t, ex, rec)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	eventually(t, "the first attempt", func() bool { return ex.total() > 0 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	claims, releases := locker.counts()
	if releases != claims {
		t.Fatalf("released %d of %d leases", releases, claims)
	}
}

func TestDispatcherRunsANotifiedMarkerOnce(t *testing.T) {
	t.Parallel()

	// A submission hands the marker over and the scan finds the same
	// marker. The two must not run the invocation twice.
	rec := pending("demo", "Charge", "id-10")
	ex := newExecutor(succeeds(`{}`))
	d, _, store, _ := dispatcher(t, ex, rec)
	start(t, d)

	d.Notify(invocation.WakeupMarker{Key: rec.Key(), Due: rec.CreatedAt})
	eventually(t, "the invocation to succeed", func() bool {
		return status(t, store, rec.Key()).Status == invocation.Succeeded
	})
	time.Sleep(100 * time.Millisecond)
	if got := ex.count(rec.Key()); got != 1 {
		t.Fatalf("executor ran %d times, want 1", got)
	}
}

func TestDispatcherRunsManyInvocations(t *testing.T) {
	t.Parallel()

	var seed []invocation.Record
	for i := range 40 {
		seed = append(seed, pending("demo", "Charge", fmt.Sprintf("bulk-%d", i)))
	}
	ex := newExecutor(succeeds(`{}`))
	d, idx, store, _ := dispatcher(t, ex, seed...)
	start(t, d)

	eventually(t, "every invocation to finish", func() bool {
		for _, r := range seed {
			if status(t, store, r.Key()).Status != invocation.Succeeded {
				return false
			}
		}
		return idx.count() == 0
	})
	if ex.total() != len(seed) {
		t.Fatalf("executor ran %d times, want %d", ex.total(), len(seed))
	}
}

func TestNewNeedsItsParts(t *testing.T) {
	t.Parallel()

	store := newStore()
	full := dispatch.Config{
		Records: store, DueIndex: newIndex(), Locker: &fakeLocker{},
		Workers:  registry(t, "http://unused"),
		Executor: newExecutor(succeeds(`{}`)), Owner: "engine-a",
	}

	tests := map[string]func(*dispatch.Config){
		"no records":  func(c *dispatch.Config) { c.Records = nil },
		"no index":    func(c *dispatch.Config) { c.DueIndex = nil },
		"no locker":   func(c *dispatch.Config) { c.Locker = nil },
		"no workers":  func(c *dispatch.Config) { c.Workers = nil },
		"no executor": func(c *dispatch.Config) { c.Executor = nil },
		"no owner":    func(c *dispatch.Config) { c.Owner = "" },
	}
	for name, break_ := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := full
			break_(&cfg)
			// A nil part must fail here, not at an unhelpful place later.
			if _, err := dispatch.New(cfg); err == nil {
				t.Fatal("New accepted an incomplete config")
			}
		})
	}
	if _, err := dispatch.New(full); err != nil {
		t.Fatalf("New rejected a complete config: %v", err)
	}
}

func TestDispatcherKeepsTheMarkerWhenSchedulingFails(t *testing.T) {
	t.Parallel()

	// A move writes the new marker first, so a failed write must leave
	// the old marker where the next scan finds it.
	rec := pending("demo", "Charge", "id-11")
	ex := newExecutor(succeeds(`{}`))
	d, idx, store, locker := dispatcher(t, ex, rec)
	idx.scheduleErr = errors.New("s3 is down")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	eventually(t, "the first claim", func() bool {
		claims, _ := locker.counts()
		return claims > 0
	})
	// Stop first, so no claim is in flight while the test counts.
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if idx.count() != 1 {
		t.Fatalf("markers = %d, want the one it could not move", idx.count())
	}
	if got := status(t, store, rec.Key()); got.Status == invocation.Succeeded {
		t.Fatalf("status = %q, want an unfinished invocation", got.Status)
	}
	// A claim it could not use must go back at once.
	claims, releases := locker.counts()
	if claims == 0 || claims != releases {
		t.Fatalf("released %d of %d leases", releases, claims)
	}
}

func TestDispatcherSurvivesAFailedScan(t *testing.T) {
	t.Parallel()

	rec := pending("demo", "Charge", "id-12")
	ex := newExecutor(succeeds(`{}`))
	d, idx, store, _ := dispatcher(t, ex, rec)
	idx.dueErr = errors.New("s3 is down")
	start(t, d)

	time.Sleep(60 * time.Millisecond)
	// The scan recovers, so the next one runs the invocation.
	idx.mu.Lock()
	idx.dueErr = nil
	idx.mu.Unlock()

	eventually(t, "the invocation to succeed", func() bool {
		return status(t, store, rec.Key()).Status == invocation.Succeeded
	})
}

func TestDispatcherIgnoresAMarkerWithNoRecord(t *testing.T) {
	t.Parallel()

	// A submission writes the record before the marker, so a marker with
	// no record must be left alone and never deleted.
	ex := newExecutor(succeeds(`{}`))
	d, idx, _, _ := dispatcher(t, ex)
	if err := idx.Schedule(t.Context(), "demo/Charge/ghost", time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	start(t, d)

	time.Sleep(100 * time.Millisecond)
	if ex.total() != 0 {
		t.Fatalf("executor ran %d times without a record", ex.total())
	}
	if idx.count() != 1 {
		t.Fatalf("markers = %d, want the marker left in place", idx.count())
	}
}

func TestDispatcherGrowsTheBackoffOnFailures(t *testing.T) {
	t.Parallel()

	// Each attempt that makes no progress must wait longer than the one
	// before it, and the record must count the failures.
	rec := pending("demo", "Charge", "id-13")
	ex := newExecutor(func(dispatch.Attempt) (dispatch.Result, error) {
		return dispatch.Result{}, errors.New("connection dropped")
	})
	d, idx, store, locker := dispatcher(t, ex, rec)
	start(t, d)

	first := waitForBackoff(t, idx, locker, 1)
	second := waitForBackoff(t, idx, locker, 2)
	if second <= first {
		t.Fatalf("second backoff of %v, want longer than %v", second, first)
	}
	if got := status(t, store, rec.Key()); got.Failures < 2 {
		t.Fatalf("failures = %d, want at least 2", got.Failures)
	}
}

func TestDispatcherRetriesAtOnceAfterProgress(t *testing.T) {
	t.Parallel()

	// An attempt that advanced was alive, so it must not be punished
	// with a backoff and its failure count must go back to zero.
	rec := pending("demo", "Charge", "id-14")
	rec.Failures = 5
	ex := newExecutor(func(a dispatch.Attempt) (dispatch.Result, error) {
		a.Progress()
		return dispatch.Result{Done: false}, nil
	})
	d, _, store, _ := dispatcher(t, ex, rec)
	start(t, d)

	eventually(t, "a second attempt without a backoff", func() bool {
		return ex.count(rec.Key()) >= 2
	})
	if got := status(t, store, rec.Key()); got.Failures != 0 {
		t.Fatalf("failures = %d, want 0 after progress", got.Failures)
	}
}

func TestDispatcherTakesAnExecuteSlotBeforeTheClaim(t *testing.T) {
	t.Parallel()

	// A saturated engine must block while it holds nothing. The other
	// order marks invocations running that nobody drives.
	first, second := pending("demo", "Charge", "slot-1"), pending("demo", "Charge", "slot-2")
	held := make(chan struct{})
	ex := newExecutor(func(dispatch.Attempt) (dispatch.Result, error) {
		<-held
		return dispatch.Result{Done: true}, nil
	})
	d, idx, store, locker := build(t, ex, func(c *dispatch.Config) {
		c.ExecuteConcurrency = 1
	}, first, second)
	before := idx.all()
	start(t, d)

	eventually(t, "the first invocation to start", func() bool { return ex.total() == 1 })
	time.Sleep(100 * time.Millisecond)

	// Exactly one lease, and the waiting invocation is untouched.
	if claims, _ := locker.counts(); claims != 1 {
		t.Fatalf("claims = %d, want 1", claims)
	}
	running := 0
	for _, r := range []invocation.Record{first, second} {
		if status(t, store, r.Key()).Status == invocation.Running {
			running++
		}
	}
	if running != 1 {
		t.Fatalf("%d invocations are running, want 1", running)
	}
	if got := idx.all(); len(got) != 2 || got[0].Due != before[0].Due {
		t.Fatalf("markers = %v, want the waiting one where it was", got)
	}
	close(held)
}

func TestDriverRenewsTheLeaseOnProgress(t *testing.T) {
	t.Parallel()

	// A renewal moves the marker with the lease, because the expiry and
	// the due time are one event.
	rec := pending("demo", "Charge", "id-15")
	done := make(chan struct{})
	ex := newExecutor(func(a dispatch.Attempt) (dispatch.Result, error) {
		for {
			select {
			case <-done:
				return dispatch.Result{Done: true}, nil
			case <-time.After(2 * time.Millisecond):
				a.Progress()
			}
		}
	})
	d, idx, _, locker := build(t, ex, shortLease, rec)
	start(t, d)

	eventually(t, "the lease to be renewed", func() bool { return locker.renewals() >= 2 })
	eventually(t, "the marker to follow the lease", func() bool {
		markers := idx.all()
		return len(markers) == 1 && markers[0].Due.After(time.Now())
	})
	close(done)
}

func TestDriverCancelsAStalledAttempt(t *testing.T) {
	t.Parallel()

	// No journal entry means no evidence the invocation is alive, so the
	// engine must stop the attempt and give the lease back.
	rec := pending("demo", "Charge", "id-16")
	d, _, store, locker := build(t, silent(), shortLease, rec)
	start(t, d)

	eventually(t, "the stalled attempt to end", func() bool {
		_, releases := locker.counts()
		return releases >= 1
	})
	// A stall makes no progress, so it counts as a failure.
	eventually(t, "the failure to be recorded", func() bool {
		return status(t, store, rec.Key()).Failures >= 1
	})
}

func TestDriverStopsOnALostLease(t *testing.T) {
	t.Parallel()

	// The lease package's contract: a holder that loses the lease must
	// stop, because another holder is already doing the work.
	rec := pending("demo", "Charge", "id-17")
	d, _, _, locker := build(t, ticking(), shortLease, rec)
	locker.renewErr = lease.ErrLeaseLost
	start(t, d)

	eventually(t, "the attempt to be cancelled", func() bool {
		_, releases := locker.counts()
		return releases >= 1
	})
}

func TestDispatcherRunsUnderRace(t *testing.T) {
	t.Parallel()

	var seed []invocation.Record
	for i := range 200 {
		seed = append(seed, pending("demo", "Charge", fmt.Sprintf("race-%d", i)))
	}
	ex := newExecutor(func(a dispatch.Attempt) (dispatch.Result, error) {
		a.Progress()
		return dispatch.Result{Done: true, Output: json.RawMessage(`{}`)}, nil
	})
	d, idx, store, _ := build(t, ex, func(c *dispatch.Config) {
		c.DispatchConcurrency = 16
		c.ExecuteConcurrency = 32
	}, seed...)
	start(t, d)

	eventually(t, "every invocation to finish", func() bool {
		for _, r := range seed {
			if status(t, store, r.Key()).Status != invocation.Succeeded {
				return false
			}
		}
		return idx.count() == 0
	})
}

// waitForBackoff returns how long the nth failed attempt asks to wait.
// It reads the marker only while no attempt holds a lease, because a
// claimed invocation carries the lease expiry and not the backoff.
func waitForBackoff(t *testing.T, idx *fakeIndex, locker *fakeLocker, n int) time.Duration {
	t.Helper()
	var got time.Duration
	eventually(t, fmt.Sprintf("the backoff of attempt %d", n), func() bool {
		claims, releases := locker.counts()
		if claims < n || claims != releases {
			return false
		}
		markers := idx.all()
		if len(markers) != 1 {
			return false
		}
		got = time.Until(markers[0].Due)
		return got > 0
	})
	return got
}
