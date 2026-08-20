package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/keel/keel/engine"
	"github.com/keel/keel/journal"
	"github.com/keel/keel/lease"
)

// fakeStore records what the engine appends, and serves a fixed history.
type fakeStore struct {
	mu        sync.Mutex
	history   []journal.Entry
	appended  []journal.Entry
	epochs    []lease.Epoch
	readErr   error
	appendErr error
}

func (f *fakeStore) Append(_ context.Context, _ string, epoch lease.Epoch, e journal.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.appendErr != nil {
		return f.appendErr
	}
	f.appended = append(f.appended, e)
	f.epochs = append(f.epochs, epoch)
	return nil
}

func (f *fakeStore) Read(context.Context, string) iter.Seq2[journal.Entry, error] {
	return func(yield func(journal.Entry, error) bool) {
		f.mu.Lock()
		history, readErr := f.history, f.readErr
		f.mu.Unlock()
		for _, e := range history {
			if !yield(e, nil) {
				return
			}
		}
		if readErr != nil {
			yield(journal.Entry{}, readErr)
		}
	}
}

func (f *fakeStore) writes() []journal.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.appended
}

// fakeLocker hands out one lease and records the calls against it.
type fakeLocker struct {
	mu        sync.Mutex
	epoch     lease.Epoch
	resource  string
	claimErr  error
	claims    int
	releases  int
	lastOwner string
}

func (f *fakeLocker) Claim(_ context.Context, resource, owner string, ttl time.Duration) (*lease.Lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	f.resource, f.lastOwner = resource, owner
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if f.epoch == 0 {
		f.epoch = 1
	}
	return lease.New(resource, owner, f.epoch, time.Now().Add(ttl)), nil
}

func (f *fakeLocker) Renew(context.Context, *lease.Lease, time.Duration) error { return nil }

func (f *fakeLocker) Release(context.Context, *lease.Lease) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
	return nil
}

func (f *fakeLocker) counts() (claims, releases int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claims, f.releases
}

// service starts a stub service that replies with reply, and records the
// last request the engine sent it.
func service(t *testing.T, reply any) (url string, got *map[string]json.RawMessage) {
	t.Helper()
	var (
		mu   sync.Mutex
		last map[string]json.RawMessage
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decoding request: %v", err)
		}
		mu.Lock()
		last = body
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(reply); err != nil {
			t.Errorf("encoding reply: %v", err)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &last
}

func newEngine(t *testing.T, url string) (*engine.Engine, *fakeStore, *fakeLocker) {
	t.Helper()
	store, locker := &fakeStore{}, &fakeLocker{}
	e := engine.New(store, locker, "engine-a", map[string]string{"demo": url})
	return e, store, locker
}

func TestInvokeRejectsAnUnknownService(t *testing.T) {
	t.Parallel()

	e, _, locker := newEngine(t, "http://unused")
	if _, err := e.Invoke(t.Context(), "missing", "id-1", nil); err == nil {
		t.Fatal("Invoke returned nil, want an error")
	}
	// The engine must not claim a lease it cannot use.
	if claims, _ := locker.counts(); claims != 0 {
		t.Fatalf("claims = %d, want 0", claims)
	}
}

func TestInvokeAppendsTheNewEntries(t *testing.T) {
	t.Parallel()

	url, req := service(t, map[string]any{
		"output": json.RawMessage(`{"ok":true}`),
		"new_entries": []journal.Entry{
			{Step: 0, Name: "charge"},
			{Step: 1, Name: "ship"},
		},
	})
	e, store, locker := newEngine(t, url)

	out, err := e.Invoke(t.Context(), "demo", "id-1", json.RawMessage(`{"amount":5}`))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(out) != `{"ok":true}` {
		t.Fatalf("output = %s, want {\"ok\":true}", out)
	}

	writes := store.writes()
	if len(writes) != 2 || writes[0].Name != "charge" || writes[1].Name != "ship" {
		t.Fatalf("appended %+v, want charge then ship", writes)
	}
	// Every append carries the epoch of the lease the engine holds.
	for i, epoch := range store.epochs {
		if epoch != 1 {
			t.Fatalf("append %d used epoch %d, want 1", i, epoch)
		}
	}

	claims, releases := locker.counts()
	if claims != 1 || releases != 1 {
		t.Fatalf("claims = %d, releases = %d, want 1 and 1", claims, releases)
	}
	// The lease names the service and the id together, so two services
	// never share one journal.
	if locker.resource != "demo-id-1" {
		t.Fatalf("resource = %q, want demo-id-1", locker.resource)
	}
	if locker.lastOwner != "engine-a" {
		t.Fatalf("owner = %q, want engine-a", locker.lastOwner)
	}

	// The service gets the bare id, not the lease key.
	if got := string((*req)["invocation_id"]); got != `"id-1"` {
		t.Fatalf("invocation_id = %s, want \"id-1\"", got)
	}
	if got := string((*req)["input"]); got != `{"amount":5}` {
		t.Fatalf("input = %s, want {\"amount\":5}", got)
	}
}

func TestInvokeSendsTheRecordedHistory(t *testing.T) {
	t.Parallel()

	url, req := service(t, map[string]any{"output": json.RawMessage(`null`)})
	e, store, _ := newEngine(t, url)
	store.history = []journal.Entry{
		{Step: 0, Name: "charge", Output: json.RawMessage(`{"id":"ch_1"}`)},
	}

	if _, err := e.Invoke(t.Context(), "demo", "id-1", nil); err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	var sent []journal.Entry
	if err := json.Unmarshal((*req)["journal"], &sent); err != nil {
		t.Fatalf("decoding journal: %v", err)
	}
	// A resume replays from the recorded history, so it must arrive whole.
	if len(sent) != 1 || sent[0].Name != "charge" || string(sent[0].Output) != `{"id":"ch_1"}` {
		t.Fatalf("journal = %+v, want the recorded charge", sent)
	}
}

func TestInvokeReturnsTheServiceError(t *testing.T) {
	t.Parallel()

	url, _ := service(t, map[string]any{
		"error":       "card declined",
		"new_entries": []journal.Entry{{Step: 0, Name: "charge"}},
	})
	e, store, _ := newEngine(t, url)

	_, err := e.Invoke(t.Context(), "demo", "id-1", nil)
	if err == nil || !strings.Contains(err.Error(), "card declined") {
		t.Fatalf("err = %v, want card declined", err)
	}
	// A failed invocation still records the steps that did run.
	if len(store.writes()) != 1 {
		t.Fatalf("appended %d entries, want 1", len(store.writes()))
	}
}

func TestInvokeStopsOnALostLease(t *testing.T) {
	t.Parallel()

	url, _ := service(t, map[string]any{
		"new_entries": []journal.Entry{{Step: 0, Name: "charge"}},
	})
	e, store, locker := newEngine(t, url)
	store.appendErr = lease.ErrLeaseLost

	_, err := e.Invoke(t.Context(), "demo", "id-1", nil)
	if !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("err = %v, want ErrLeaseLost", err)
	}
	// The message must name the invocation an operator has to look at.
	if !strings.Contains(err.Error(), "demo-id-1") {
		t.Fatalf("err %q does not name the invocation", err)
	}
	// The lease is released even when the invocation ends badly.
	if _, releases := locker.counts(); releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
}

func TestInvokeFailsWhenTheClaimFails(t *testing.T) {
	t.Parallel()

	url, _ := service(t, map[string]any{})
	e, store, locker := newEngine(t, url)
	locker.claimErr = lease.ErrClaimHeld

	_, err := e.Invoke(t.Context(), "demo", "id-1", nil)
	if !errors.Is(err, lease.ErrClaimHeld) {
		t.Fatalf("err = %v, want ErrClaimHeld", err)
	}
	// Another engine owns the invocation, so this one must not run it.
	if len(store.writes()) != 0 {
		t.Fatalf("appended %d entries, want 0", len(store.writes()))
	}
}

func TestInvokeFailsWhenTheReadFails(t *testing.T) {
	t.Parallel()

	url, _ := service(t, map[string]any{})
	e, store, _ := newEngine(t, url)
	store.readErr = errors.New("bucket unreachable")

	// A partial history would make the service replay the wrong steps.
	_, err := e.Invoke(t.Context(), "demo", "id-1", nil)
	if err == nil || !strings.Contains(err.Error(), "bucket unreachable") {
		t.Fatalf("err = %v, want the read error", err)
	}
}

func TestInvokeFailsWhenTheServiceIsUnreachable(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	e, _, locker := newEngine(t, url)
	if _, err := e.Invoke(t.Context(), "demo", "id-1", nil); err == nil {
		t.Fatal("Invoke returned nil, want an error")
	}
	// The lease is released, so the next attempt need not wait it out.
	if _, releases := locker.counts(); releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
}

func TestInvokeFailsOnAnUndecodableReply(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("not json"))
	}))
	t.Cleanup(srv.Close)

	e, _, _ := newEngine(t, srv.URL)
	_, err := e.Invoke(t.Context(), "demo", "id-1", nil)
	if err == nil || !strings.Contains(err.Error(), "decoding response") {
		t.Fatalf("err = %v, want a decode error", err)
	}
}
