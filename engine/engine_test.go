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
	"github.com/keel/keel/invocation"
	"github.com/keel/keel/journal"
	"github.com/keel/keel/lease"
	"github.com/keel/keel/worker"
)

// fakeStore records what the engine appends, and serves a fixed history.
type fakeStore struct {
	mu        sync.Mutex
	records   map[string]invocation.Record
	createErr error
	getErr    error
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

func (f *fakeStore) Create(_ context.Context, r invocation.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return f.createErr
	}
	if _, ok := f.records[r.Key()]; ok {
		return invocation.ErrExists
	}
	f.records[r.Key()] = r
	return nil
}

func (f *fakeStore) Get(_ context.Context, key string) (invocation.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return invocation.Record{}, f.getErr
	}
	r, ok := f.records[key]
	if !ok {
		return invocation.Record{}, invocation.ErrNotFound
	}
	return r, nil
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

func inv(service, handler, id string, input json.RawMessage) invocation.Invocation {
	return invocation.Invocation{
		Service: service, Handler: handler,
		ID: invocation.ID(id), Input: input,
	}
}

func newEngine(t *testing.T, url string) (*engine.Engine, *fakeStore, *fakeLocker) {
	t.Helper()
	store, locker := &fakeStore{records: map[string]invocation.Record{}}, &fakeLocker{}
	// A real registry, because it is in-process and holds no state that
	// a fake would make simpler.
	reg := worker.NewMemory()
	if err := reg.Register(worker.Worker{
		ID: "worker-1", Service: "demo",
		Handlers: []string{"Charge", "Ship"}, Address: url,
	}); err != nil {
		t.Fatalf("registering the worker: %v", err)
	}
	e, err := engine.New(engine.Config{
		Records: store, Journal: store, Locker: locker,
		Workers: reg, Owner: "engine-a",
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return e, store, locker
}

func TestInvokeNeedsALiveWorker(t *testing.T) {
	t.Parallel()

	e, _, locker := newEngine(t, "http://unused")
	_, err := e.Invoke(t.Context(), inv("missing", "Charge", "id-1", nil))
	if !errors.Is(err, worker.ErrNoWorker) {
		t.Fatalf("err = %v, want %v", err, worker.ErrNoWorker)
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

	out, err := e.Invoke(t.Context(), inv("demo", "Charge", "id-1", json.RawMessage(`{"amount":5}`)))
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
	if locker.resource != "demo/Charge/id-1" {
		t.Fatalf("resource = %q, want demo/Charge/id-1", locker.resource)
	}
	if locker.lastOwner != "engine-a" {
		t.Fatalf("owner = %q, want engine-a", locker.lastOwner)
	}

	// The service gets the bare id, not the lease key.
	if got := string((*req)["invocation_id"]); got != `"id-1"` {
		t.Fatalf("invocation_id = %s, want \"id-1\"", got)
	}
	// It also needs the handler, because one service hosts many.
	if got := string((*req)["handler"]); got != `"Charge"` {
		t.Fatalf("handler = %s, want \"Charge\"", got)
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

	if _, err := e.Invoke(t.Context(), inv("demo", "Charge", "id-1", nil)); err != nil {
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

	_, err := e.Invoke(t.Context(), inv("demo", "Charge", "id-1", nil))
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

	_, err := e.Invoke(t.Context(), inv("demo", "Charge", "id-1", nil))
	if !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("err = %v, want ErrLeaseLost", err)
	}
	// The message must name the invocation an operator has to look at.
	if !strings.Contains(err.Error(), "demo/Charge/id-1") {
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

	_, err := e.Invoke(t.Context(), inv("demo", "Charge", "id-1", nil))
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
	_, err := e.Invoke(t.Context(), inv("demo", "Charge", "id-1", nil))
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
	if _, err := e.Invoke(t.Context(), inv("demo", "Charge", "id-1", nil)); err == nil {
		t.Fatal("Invoke returned nil, want an error")
	}
	// The lease is released, so the next attempt need not wait it out.
	if _, releases := locker.counts(); releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
}

func TestInvokeRejectsAnInvalidAddress(t *testing.T) {
	t.Parallel()

	url, _ := service(t, map[string]any{})
	e, _, locker := newEngine(t, url)

	// The id reaches a storage key, so the engine must not run an
	// invocation it could not have stored.
	_, err := e.Invoke(t.Context(), inv("demo", "Charge", "../escape", nil))
	if !errors.Is(err, invocation.ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if claims, _ := locker.counts(); claims != 0 {
		t.Fatalf("claims = %d, want 0", claims)
	}
}

func TestInvokeFailsOnAnUndecodableReply(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("not json"))
		if err != nil {
			t.Fatalf("unable to write")
		}
	}))
	t.Cleanup(srv.Close)

	e, _, _ := newEngine(t, srv.URL)
	_, err := e.Invoke(t.Context(), inv("demo", "Charge", "id-1", nil))
	if err == nil || !strings.Contains(err.Error(), "decoding response") {
		t.Fatalf("err = %v, want a decode error", err)
	}
}

func TestNewRejectsAnIncompleteConfig(t *testing.T) {
	t.Parallel()

	store, locker := &fakeStore{}, &fakeLocker{}
	full := engine.Config{
		Records: store, Journal: store, Locker: locker,
		Workers: worker.NewMemory(), Owner: "engine-a",
	}

	tests := map[string]func(*engine.Config){
		"no records": func(c *engine.Config) { c.Records = nil },
		"no journal": func(c *engine.Config) { c.Journal = nil },
		"no locker":  func(c *engine.Config) { c.Locker = nil },
		"no workers": func(c *engine.Config) { c.Workers = nil },
		"no owner":   func(c *engine.Config) { c.Owner = "" },
	}
	for name, break_ := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := full
			break_(&cfg)
			// A nil store must fail here, not at an unhelpful place later.
			if _, err := engine.New(cfg); err == nil {
				t.Fatal("New accepted an incomplete config")
			}
		})
	}
	if _, err := engine.New(full); err != nil {
		t.Fatalf("New rejected a complete config: %v", err)
	}
}

func TestSubmitRecordsAPendingInvocation(t *testing.T) {
	t.Parallel()

	e, store, locker := newEngine(t, "http://unused")
	sub, err := e.Submit(t.Context(), inv("demo", "Charge", "order-1", json.RawMessage(`{"amount":5}`)))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	if !sub.Created {
		t.Fatal("Created is false for a new invocation")
	}
	if sub.Record.Status != invocation.Pending {
		t.Fatalf("status = %q, want pending", sub.Record.Status)
	}
	if sub.Record.CreatedAt.IsZero() {
		t.Fatal("CreatedAt is zero")
	}
	if _, ok := store.records["demo/Charge/order-1"]; !ok {
		t.Fatalf("stored %v, want demo/Charge/order-1", store.records)
	}
	// Submission records the invocation; it must not start it.
	if claims, _ := locker.counts(); claims != 0 {
		t.Fatalf("claims = %d, want 0", claims)
	}
}

func TestSubmitIsIdempotent(t *testing.T) {
	t.Parallel()

	e, store, _ := newEngine(t, "http://unused")
	in := inv("demo", "Charge", "order-1", json.RawMessage(`{"amount":5}`))

	first, err := e.Submit(t.Context(), in)
	if err != nil {
		t.Fatalf("first Submit: %v", err)
	}

	// A retry must find the invocation, not start a second run of it.
	second, err := e.Submit(t.Context(), in)
	if err != nil {
		t.Fatalf("second Submit: %v", err)
	}
	if second.Created {
		t.Fatal("Created is true for a repeat")
	}
	if !second.Record.CreatedAt.Equal(first.Record.CreatedAt) {
		t.Fatal("the repeat returned a different record")
	}
	if len(store.records) != 1 {
		t.Fatalf("stored %d records, want 1", len(store.records))
	}
}

func TestSubmitIgnoresInputWhitespace(t *testing.T) {
	t.Parallel()

	e, _, _ := newEngine(t, "http://unused")
	if _, err := e.Submit(t.Context(), inv("demo", "Charge", "order-1", json.RawMessage(`{"amount":5}`))); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Formatting alone must not look like a conflict.
	spaced := inv("demo", "Charge", "order-1", json.RawMessage("{ \"amount\" :\n5 }"))
	sub, err := e.Submit(t.Context(), spaced)
	if err != nil {
		t.Fatalf("Submit with spacing: %v", err)
	}
	if sub.Created {
		t.Fatal("Created is true for a reformatted repeat")
	}
}

func TestSubmitRejectsAReusedID(t *testing.T) {
	t.Parallel()

	e, _, _ := newEngine(t, "http://unused")
	if _, err := e.Submit(t.Context(), inv("demo", "Charge", "order-1", json.RawMessage(`{"amount":5}`))); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	other := inv("demo", "Charge", "order-1", json.RawMessage(`{"amount":9999}`))
	if _, err := e.Submit(t.Context(), other); !errors.Is(err, engine.ErrInputConflict) {
		t.Fatalf("err = %v, want ErrInputConflict", err)
	}
}

func TestSubmitRejectsBadInvocations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   invocation.Invocation
		want error
	}{
		{"no id", inv("demo", "Charge", "", nil), invocation.ErrInvalid},
		{"no handler", inv("demo", "", "order-1", nil), invocation.ErrInvalid},
		{"traversing id", inv("demo", "Charge", "../../x", nil), invocation.ErrInvalid},
		{"input is not json", inv("demo", "Charge", "order-1", json.RawMessage(`{`)), invocation.ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e, store, _ := newEngine(t, "http://unused")
			if _, err := e.Submit(t.Context(), tt.in); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			// A rejected submission must record nothing.
			if len(store.records) != 0 {
				t.Fatalf("stored %d records, want 0", len(store.records))
			}
		})
	}
}

func TestSubmitReportsAStoreFailure(t *testing.T) {
	t.Parallel()

	e, store, _ := newEngine(t, "http://unused")
	store.createErr = errors.New("bucket unreachable")

	// The caller must never be told a record is durable when it is not.
	_, err := e.Submit(t.Context(), inv("demo", "Charge", "order-1", nil))
	if err == nil || !strings.Contains(err.Error(), "bucket unreachable") {
		t.Fatalf("err = %v, want the store error", err)
	}
}

func TestLookup(t *testing.T) {
	t.Parallel()

	e, _, _ := newEngine(t, "http://unused")
	in := inv("demo", "Charge", "order-1", json.RawMessage(`{"a":1}`))
	if _, err := e.Submit(t.Context(), in); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	got, err := e.Lookup(t.Context(), in)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	// Nothing runs the invocation yet, so it stays pending.
	if got.Status != invocation.Pending {
		t.Fatalf("status = %q, want pending", got.Status)
	}
}

func TestLookupRejectsBadAddresses(t *testing.T) {
	t.Parallel()

	e, _, _ := newEngine(t, "http://unused")
	if _, err := e.Lookup(t.Context(), inv("demo", "Charge", "a/b", nil)); !errors.Is(err, invocation.ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if _, err := e.Lookup(t.Context(), inv("demo", "Charge", "never", nil)); !errors.Is(err, invocation.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestSubmitAcceptsAServiceWithNoWorker(t *testing.T) {
	t.Parallel()

	// No worker serves "later" yet. The invocation must still be
	// recorded, because a worker may start after the submission.
	e, store, _ := newEngine(t, "http://unused")
	sub, err := e.Submit(t.Context(), inv("later", "Charge", "order-1", nil))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !sub.Created {
		t.Fatal("Created = false, want true")
	}
	if got := sub.Record.Status; got != invocation.Pending {
		t.Fatalf("status = %q, want %q", got, invocation.Pending)
	}
	if len(store.records) != 1 {
		t.Fatalf("records = %d, want 1", len(store.records))
	}
}

func TestInvokeDialsThePickedWorker(t *testing.T) {
	t.Parallel()

	var (
		mu   sync.Mutex
		path string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		path = r.URL.Path
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"output":{"ok":true}}`)); err != nil {
			t.Errorf("writing reply: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	// The worker announces a base address only, so the engine must add
	// the path of the invoke protocol itself.
	e, _, _ := newEngine(t, srv.URL)
	if _, err := e.Invoke(t.Context(), inv("demo", "Charge", "order-1", nil)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if path != "/keel/v1/invoke" {
		t.Fatalf("path = %q, want %q", path, "/keel/v1/invoke")
	}
}

func TestRegisterWorkerReachesTheRegistry(t *testing.T) {
	t.Parallel()

	e, _, _ := newEngine(t, "http://unused")
	beat, err := e.RegisterWorker(worker.Worker{
		ID: "worker-2", Service: "billing",
		Handlers: []string{"Charge"}, Address: "http://localhost:9000",
	})
	if err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if beat != worker.Heartbeat {
		t.Fatalf("heartbeat = %v, want %v", beat, worker.Heartbeat)
	}

	// The engine can now reach the service it could not reach before.
	if _, err := e.Invoke(t.Context(), inv("billing", "Charge", "order-1", nil)); errors.Is(err, worker.ErrNoWorker) {
		t.Fatal("Invoke returned ErrNoWorker after the worker registered")
	}
}

func TestRegisterWorkerRejectsABadAddress(t *testing.T) {
	t.Parallel()

	e, _, _ := newEngine(t, "http://unused")
	_, err := e.RegisterWorker(worker.Worker{
		ID: "worker-3", Service: "billing",
		Handlers: []string{"Charge"}, Address: "localhost:9000",
	})
	if !errors.Is(err, worker.ErrInvalid) {
		t.Fatalf("err = %v, want %v", err, worker.ErrInvalid)
	}
}

func TestDeregisterWorkerRemovesIt(t *testing.T) {
	t.Parallel()

	e, _, _ := newEngine(t, "http://unused")
	if err := e.DeregisterWorker("worker-1"); err != nil {
		t.Fatalf("DeregisterWorker: %v", err)
	}
	_, err := e.Invoke(t.Context(), inv("demo", "Charge", "order-1", nil))
	if !errors.Is(err, worker.ErrNoWorker) {
		t.Fatalf("err = %v, want %v", err, worker.ErrNoWorker)
	}
}
