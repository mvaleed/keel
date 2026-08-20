package lease_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/keel/keel/lease"
)

// fakeLocker is a Locker that only counts renewals. renewErr, when set,
// ends the nth renewal and every one after it.
type fakeLocker struct {
	renews   atomic.Int64
	failAt   int64
	renewErr error

	mu       sync.Mutex
	renewTTL time.Duration
}

func (f *fakeLocker) Claim(context.Context, string, string, time.Duration) (*lease.Lease, error) {
	panic("unused")
}

func (f *fakeLocker) Release(context.Context, *lease.Lease) error { return nil }

func (f *fakeLocker) Renew(_ context.Context, l *lease.Lease, ttl time.Duration) error {
	n := f.renews.Add(1)
	f.mu.Lock()
	f.renewTTL = ttl
	f.mu.Unlock()
	if f.renewErr != nil && n >= f.failAt {
		return f.renewErr
	}
	l.Extend(time.Now().Add(ttl))
	return nil
}

func (f *fakeLocker) ttl() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renewTTL
}

// waitFor blocks until cond holds, and fails the test if it does not
// hold in one second.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not hold in one second")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNewKeepsTheFields(t *testing.T) {
	t.Parallel()

	expires := time.Now().Add(time.Minute)
	l := lease.New("inv-1", "worker-a", 7, expires)

	if l.Resource != "inv-1" || l.Owner != "worker-a" || l.Epoch != 7 {
		t.Fatalf("lease = %+v, want inv-1/worker-a/7", l)
	}
	if !l.Expires().Equal(expires) {
		t.Fatalf("expires = %v, want %v", l.Expires(), expires)
	}
}

func TestExpired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expires time.Time
		want    bool
	}{
		{"future", time.Now().Add(time.Minute), false},
		{"past", time.Now().Add(-time.Minute), true},
		{"zero", time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l := lease.New("inv-1", "worker-a", 1, tt.expires)
			if got := l.Expired(); got != tt.want {
				t.Fatalf("Expired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtendOnlyMovesForward(t *testing.T) {
	t.Parallel()

	start := time.Now()
	l := lease.New("inv-1", "worker-a", 1, start)

	later := start.Add(time.Minute)
	l.Extend(later)
	if !l.Expires().Equal(later) {
		t.Fatalf("expires = %v, want %v", l.Expires(), later)
	}

	// A late renewal must not shorten the lease that a newer one already
	// extended, or the holder gives up time it still owns.
	l.Extend(start.Add(time.Second))
	if !l.Expires().Equal(later) {
		t.Fatalf("expires = %v after a backwards Extend, want %v", l.Expires(), later)
	}

	// An Extend to the same instant is not backwards, but changes nothing.
	l.Extend(later)
	if !l.Expires().Equal(later) {
		t.Fatalf("expires = %v after a repeat Extend, want %v", l.Expires(), later)
	}
}

func TestExtendIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	start := time.Now()
	l := lease.New("inv-1", "worker-a", 1, start)

	const writers = 8
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			l.Extend(start.Add(time.Duration(i) * time.Second))
		}()
		go func() {
			defer wg.Done()
			_ = l.Expired()
		}()
	}
	wg.Wait()

	// The largest of the writes must win, whatever order they ran in.
	want := start.Add(time.Duration(writers-1) * time.Second)
	if !l.Expires().Equal(want) {
		t.Fatalf("expires = %v, want %v", l.Expires(), want)
	}
}

func TestJSONRoundTrip(t *testing.T) {
	t.Parallel()

	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Nanosecond)
	want := lease.New("inv-1", "worker-a", 42, expires)

	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got lease.Lease
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.Resource != want.Resource || got.Owner != want.Owner || got.Epoch != want.Epoch {
		t.Fatalf("lease = %+v, want %+v", &got, want)
	}
	// The expiry survives the trip, though the field is unexported.
	if !got.Expires().Equal(expires) {
		t.Fatalf("expires = %v, want %v", got.Expires(), expires)
	}
}

func TestUnmarshalRejectsBadJSON(t *testing.T) {
	t.Parallel()

	var l lease.Lease
	if err := json.Unmarshal([]byte(`{"epoch":"not a number"}`), &l); err == nil {
		t.Fatal("unmarshal of a bad epoch succeeded, want an error")
	}
}

func TestKeepaliveRenewsUntilStop(t *testing.T) {
	t.Parallel()

	const ttl = 60 * time.Millisecond
	f := &fakeLocker{}
	l := lease.New("inv-1", "worker-a", 1, time.Now().Add(ttl))

	stop := lease.Keepalive(t.Context(), f, l, ttl)
	// Three ttls give about nine renewals at one every ttl/3.
	time.Sleep(3 * ttl)
	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	renews := f.renews.Load()
	if renews < 2 {
		t.Fatalf("renewals = %d, want at least 2", renews)
	}
	if f.ttl() != ttl {
		t.Fatalf("renewed with ttl %v, want %v", f.ttl(), ttl)
	}
	// The lease outlives its original ttl, which is the point.
	if l.Expired() {
		t.Fatal("lease expired under a running keepalive")
	}

	// Stop ends the goroutine, so no renewal arrives after it returns.
	time.Sleep(2 * ttl)
	if got := f.renews.Load(); got != renews {
		t.Fatalf("renewals = %d after stop, want %d", got, renews)
	}
}

func TestKeepaliveStopIsIdempotent(t *testing.T) {
	t.Parallel()

	const ttl = 30 * time.Millisecond
	f := &fakeLocker{}
	l := lease.New("inv-1", "worker-a", 1, time.Now().Add(ttl))

	stop := lease.Keepalive(t.Context(), f, l, ttl)
	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	// A second stop must repeat the same answer, not block.
	if err := stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestKeepaliveReportsTheRenewError(t *testing.T) {
	t.Parallel()

	const ttl = 30 * time.Millisecond
	f := &fakeLocker{failAt: 1, renewErr: lease.ErrLeaseLost}
	l := lease.New("inv-1", "worker-a", 1, time.Now().Add(ttl))

	stop := lease.Keepalive(t.Context(), f, l, ttl)
	// Wait for the failed renewal, or stop races the first tick and wins.
	waitFor(t, func() bool { return f.renews.Load() == 1 })

	if err := stop(); !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("stop err = %v, want ErrLeaseLost", err)
	}
	// The goroutine ends on the first failure and stops renewing.
	if got := f.renews.Load(); got != 1 {
		t.Fatalf("renewals = %d, want 1", got)
	}
	// Every later stop repeats the same error.
	if err := stop(); !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("second stop err = %v, want ErrLeaseLost", err)
	}
}

func TestKeepaliveEndsWithTheContext(t *testing.T) {
	t.Parallel()

	const ttl = 30 * time.Millisecond
	f := &fakeLocker{}
	l := lease.New("inv-1", "worker-a", 1, time.Now().Add(ttl))

	ctx, cancel := context.WithCancel(t.Context())
	stop := lease.Keepalive(ctx, f, l, ttl)
	cancel()

	// A cancelled context is not a lost lease, so stop reports no error.
	if err := stop(); err != nil {
		t.Fatalf("stop after cancel: %v", err)
	}

	renews := f.renews.Load()
	time.Sleep(2 * ttl)
	if got := f.renews.Load(); got != renews {
		t.Fatalf("renewals = %d after cancel, want %d", got, renews)
	}
}

func TestKeepaliveWithNonPositiveTTL(t *testing.T) {
	t.Parallel()

	for _, ttl := range []time.Duration{0, -time.Second} {
		f := &fakeLocker{}
		l := lease.New("inv-1", "worker-a", 1, time.Now())

		// A ttl of zero or less gives an interval of zero, which panics a
		// ticker. Keepalive must clamp it instead.
		stop := lease.Keepalive(t.Context(), f, l, ttl)
		time.Sleep(10 * time.Millisecond)
		if err := stop(); err != nil {
			t.Fatalf("stop with ttl %v: %v", ttl, err)
		}
	}
}
