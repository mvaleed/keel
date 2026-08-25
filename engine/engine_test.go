package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/keel/keel/engine"
	"github.com/keel/keel/invocation"
	"github.com/keel/keel/worker"
)

// fakeStore is a record store in memory.
type fakeStore struct {
	mu        sync.Mutex
	records   map[string]invocation.Record
	createErr error
}

func newStore() *fakeStore {
	return &fakeStore{records: map[string]invocation.Record{}}
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
	r, ok := f.records[key]
	if !ok {
		return invocation.Record{}, invocation.ErrNotFound
	}
	return r, nil
}

func (f *fakeStore) Update(_ context.Context, r invocation.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records[r.Key()] = r
	return nil
}

func (f *fakeStore) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

// fakeDispatcher records the markers a submission hands over.
type fakeDispatcher struct {
	mu      sync.Mutex
	markers []invocation.WakeupMarker
}

func (f *fakeDispatcher) Notify(m invocation.WakeupMarker) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markers = append(f.markers, m)
}

func (f *fakeDispatcher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.markers)
}

func inv(service, handler, id string, input json.RawMessage) invocation.Invocation {
	return invocation.Invocation{
		Service: service, Handler: handler,
		ID: invocation.ID(id), Input: input,
	}
}

func newEngine(t *testing.T) (*engine.Engine, *fakeStore, worker.Registry) {
	t.Helper()
	store := newStore()
	// A real registry, because it is in-process and holds no state that
	// a fake would make simpler.
	reg := worker.NewMemory()
	if err := reg.Register(worker.Worker{
		ID: "worker-1", Service: "demo",
		Handlers: []string{"Charge", "Ship"}, Address: "http://localhost:9000",
	}); err != nil {
		t.Fatalf("registering the worker: %v", err)
	}
	e, err := engine.New(engine.Config{Records: store, Workers: reg})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return e, store, reg
}

func TestNewRejectsAnIncompleteConfig(t *testing.T) {
	t.Parallel()

	full := engine.Config{Records: newStore(), Workers: worker.NewMemory()}

	tests := map[string]func(*engine.Config){
		"no records": func(c *engine.Config) { c.Records = nil },
		"no workers": func(c *engine.Config) { c.Workers = nil },
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
	// A dispatcher is optional, because the handoff is latency only.
	if _, err := engine.New(full); err != nil {
		t.Fatalf("New rejected a complete config: %v", err)
	}
}

func TestSubmitRecordsAPendingInvocation(t *testing.T) {
	t.Parallel()

	e, store, _ := newEngine(t)
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
}

func TestSubmitHandsTheMarkerToTheDispatcher(t *testing.T) {
	t.Parallel()

	d := &fakeDispatcher{}
	e, err := engine.New(engine.Config{
		Records: newStore(), Workers: worker.NewMemory(), Dispatcher: d,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	in := inv("demo", "Charge", "order-1", nil)
	if _, err := e.Submit(t.Context(), in); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if d.count() != 1 {
		t.Fatalf("handed over %d markers, want 1", d.count())
	}
	if got := d.markers[0].Key; got != "demo/Charge/order-1" {
		t.Fatalf("marker key = %q, want demo/Charge/order-1", got)
	}

	// A repeat is not new work, so it must not be handed over again.
	if _, err := e.Submit(t.Context(), in); err != nil {
		t.Fatalf("second Submit: %v", err)
	}
	if d.count() != 1 {
		t.Fatalf("handed over %d markers, want 1", d.count())
	}
}

func TestSubmitWithoutADispatcher(t *testing.T) {
	t.Parallel()

	// The handoff is latency and never correctness, so a nil dispatcher
	// must not fail a submission.
	e, _, _ := newEngine(t)
	if _, err := e.Submit(t.Context(), inv("demo", "Charge", "order-1", nil)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
}

func TestSubmitIsIdempotent(t *testing.T) {
	t.Parallel()

	e, store, _ := newEngine(t)
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
	if store.count() != 1 {
		t.Fatalf("stored %d records, want 1", store.count())
	}
}

func TestSubmitIgnoresInputWhitespace(t *testing.T) {
	t.Parallel()

	e, _, _ := newEngine(t)
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

	e, _, _ := newEngine(t)
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
			e, store, _ := newEngine(t)
			if _, err := e.Submit(t.Context(), tt.in); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			// A rejected submission must record nothing.
			if store.count() != 0 {
				t.Fatalf("stored %d records, want 0", store.count())
			}
		})
	}
}

func TestSubmitReportsAStoreFailure(t *testing.T) {
	t.Parallel()

	e, store, _ := newEngine(t)
	store.createErr = errors.New("bucket unreachable")

	// The caller must never be told a record is durable when it is not.
	_, err := e.Submit(t.Context(), inv("demo", "Charge", "order-1", nil))
	if err == nil || !strings.Contains(err.Error(), "bucket unreachable") {
		t.Fatalf("err = %v, want the store error", err)
	}
}

func TestSubmitAcceptsAServiceWithNoWorker(t *testing.T) {
	t.Parallel()

	// No worker serves "later" yet. The invocation must still be
	// recorded, because a worker may start after the submission.
	e, store, _ := newEngine(t)
	sub, err := e.Submit(t.Context(), inv("later", "Charge", "order-1", nil))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !sub.Created || sub.Record.Status != invocation.Pending {
		t.Fatalf("submission = %+v, want a new pending record", sub)
	}
	if store.count() != 1 {
		t.Fatalf("records = %d, want 1", store.count())
	}
}

func TestLookup(t *testing.T) {
	t.Parallel()

	e, _, _ := newEngine(t)
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

	e, _, _ := newEngine(t)
	if _, err := e.Lookup(t.Context(), inv("demo", "Charge", "a/b", nil)); !errors.Is(err, invocation.ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if _, err := e.Lookup(t.Context(), inv("demo", "Charge", "never", nil)); !errors.Is(err, invocation.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestRegisterWorkerReachesTheRegistry(t *testing.T) {
	t.Parallel()

	e, _, reg := newEngine(t)
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

	// The registry can now serve a handler it could not serve before.
	if _, err := reg.Pick("billing", "Charge"); err != nil {
		t.Fatalf("Pick: %v", err)
	}
}

func TestRegisterWorkerRejectsABadAddress(t *testing.T) {
	t.Parallel()

	e, _, _ := newEngine(t)
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

	e, _, reg := newEngine(t)
	if err := e.DeregisterWorker("worker-1"); err != nil {
		t.Fatalf("DeregisterWorker: %v", err)
	}
	if _, err := reg.Pick("demo", "Charge"); !errors.Is(err, worker.ErrNoWorker) {
		t.Fatalf("err = %v, want %v", err, worker.ErrNoWorker)
	}
}
