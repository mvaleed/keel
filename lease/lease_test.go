package lease_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/keel/keel/lease"
)

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
