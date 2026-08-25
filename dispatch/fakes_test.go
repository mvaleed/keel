package dispatch_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/keel/keel/dispatch"
	"github.com/keel/keel/invocation"
	"github.com/keel/keel/journal"
	"github.com/keel/keel/lease"
	"github.com/keel/keel/worker"
)

// fakeStore is the record store and the journal in memory. One backend
// satisfies both in production, so one fake does here.
type fakeStore struct {
	mu        sync.Mutex
	records   map[string]invocation.Record
	history   []journal.Entry
	appended  []journal.Entry
	epochs    []lease.Epoch
	readErr   error
	appendErr error
	updateErr error
}

func newStore() *fakeStore {
	return &fakeStore{records: map[string]invocation.Record{}}
}

func (f *fakeStore) Create(_ context.Context, r invocation.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	if f.updateErr != nil {
		return f.updateErr
	}
	stored, ok := f.records[r.Key()]
	if !ok {
		return invocation.ErrNotFound
	}
	if stored.Epoch > r.Epoch {
		return lease.ErrLeaseLost
	}
	f.records[r.Key()] = r
	return nil
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

// fakeLocker hands out one lease per resource and counts the calls.
type fakeLocker struct {
	mu       sync.Mutex
	epoch    lease.Epoch
	claimErr error
	renewErr error
	claims   int
	renews   int
	releases int
	ttl      time.Duration
}

func (f *fakeLocker) Claim(_ context.Context, resource, owner string, ttl time.Duration) (*lease.Lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	f.epoch++
	if f.ttl > 0 {
		ttl = f.ttl
	}
	return lease.New(resource, owner, f.epoch, time.Now().Add(ttl)), nil
}

func (f *fakeLocker) Renew(_ context.Context, l *lease.Lease, ttl time.Duration) error {
	f.mu.Lock()
	err, own := f.renewErr, f.ttl
	f.renews++
	f.mu.Unlock()
	if err != nil {
		return err
	}
	if own > 0 {
		ttl = own
	}
	l.Extend(time.Now().Add(ttl))
	return nil
}

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

func (f *fakeLocker) renewals() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renews
}

// fakeIndex is a due index in memory. It keys a marker by its key and
// its due second, so a duplicate marker is two entries and not one.
type fakeIndex struct {
	mu          sync.Mutex
	markers     map[string]invocation.WakeupMarker
	scheduleErr error
	dueErr      error
}

func newIndex() *fakeIndex {
	return &fakeIndex{markers: map[string]invocation.WakeupMarker{}}
}

func markerID(m invocation.WakeupMarker) string {
	return fmt.Sprintf("%s@%d", m.Key, m.Due.Unix())
}

func (f *fakeIndex) Schedule(_ context.Context, key string, due time.Time) error {
	m := invocation.WakeupMarker{Key: key, Due: due.Truncate(time.Second)}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.scheduleErr != nil {
		return f.scheduleErr
	}
	f.markers[markerID(m)] = m
	return nil
}

func (f *fakeIndex) Due(_ context.Context, now time.Time) iter.Seq2[invocation.WakeupMarker, error] {
	f.mu.Lock()
	if err := f.dueErr; err != nil {
		f.mu.Unlock()
		return func(yield func(invocation.WakeupMarker, error) bool) { yield(invocation.WakeupMarker{}, err) }
	}
	ready := make([]invocation.WakeupMarker, 0, len(f.markers))
	for _, m := range f.markers {
		if !m.Due.After(now) {
			ready = append(ready, m)
		}
	}
	f.mu.Unlock()

	slices.SortFunc(ready, func(a, b invocation.WakeupMarker) int { return a.Due.Compare(b.Due) })
	return func(yield func(invocation.WakeupMarker, error) bool) {
		for _, m := range ready {
			if !yield(m, nil) {
				return
			}
		}
	}
}

func (f *fakeIndex) Forget(_ context.Context, m invocation.WakeupMarker) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.markers, markerID(m))
	return nil
}

func (f *fakeIndex) all() []invocation.WakeupMarker {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]invocation.WakeupMarker, 0, len(f.markers))
	for _, m := range f.markers {
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b invocation.WakeupMarker) int { return a.Due.Compare(b.Due) })
	return out
}

func (f *fakeIndex) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.markers)
}

// fakeExecutor answers with what the test supplies, and counts the calls
// per invocation key.
type fakeExecutor struct {
	mu    sync.Mutex
	calls map[string]int
	reply func(dispatch.Attempt) (dispatch.Result, error)
}

func newExecutor(reply func(dispatch.Attempt) (dispatch.Result, error)) *fakeExecutor {
	return &fakeExecutor{calls: map[string]int{}, reply: reply}
}

func (f *fakeExecutor) Execute(_ context.Context, a dispatch.Attempt) (dispatch.Result, error) {
	f.mu.Lock()
	f.calls[a.Record.Key()]++
	f.mu.Unlock()
	return f.reply(a)
}

func (f *fakeExecutor) count(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[key]
}

func (f *fakeExecutor) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		n += c
	}
	return n
}

func succeeds(output string) func(dispatch.Attempt) (dispatch.Result, error) {
	return func(dispatch.Attempt) (dispatch.Result, error) {
		return dispatch.Result{Done: true, Output: json.RawMessage(output)}, nil
	}
}

// quiet keeps the dispatcher's expected errors out of the test output.
func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func inv(service, handler, id string, input json.RawMessage) invocation.Invocation {
	return invocation.Invocation{
		Service: service, Handler: handler,
		ID: invocation.ID(id), Input: input,
	}
}

// pending returns a record that a dispatcher may pick up.
func pending(service, handler, id string) invocation.Record {
	i := inv(service, handler, id, json.RawMessage(`{}`))
	return invocation.Record{
		Invocation: i,
		Status:     invocation.Pending,
		InputHash:  invocation.HashInput(i.Input),
		CreatedAt:  time.Now().UTC(),
	}
}

// registry returns a registry with one worker that serves demo at url.
func registry(t *testing.T, url string) worker.Registry {
	t.Helper()
	reg := worker.NewMemory()
	if err := reg.Register(worker.Worker{
		ID: "worker-1", Service: "demo",
		Handlers: []string{"Charge", "Ship"}, Address: url,
	}); err != nil {
		t.Fatalf("registering the worker: %v", err)
	}
	return reg
}

// service starts a stub worker that replies with reply, and records the
// last request the executor sent it.
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

// eventually waits for cond, because the dispatcher works in the
// background and a test cannot see the moment it finishes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func status(t *testing.T, store *fakeStore, key string) invocation.Record {
	t.Helper()
	r, err := store.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return r
}

// silent is an executor that never returns and never reports progress.
// It stands for a worker that hung.
func silent() dispatch.Executor {
	return ctxExecutor(func(ctx context.Context, _ dispatch.Attempt) (dispatch.Result, error) {
		<-ctx.Done()
		return dispatch.Result{}, ctx.Err()
	})
}

// ticking is an executor that reports progress until it is cancelled. It
// stands for a long invocation that runs well.
func ticking() dispatch.Executor {
	return ctxExecutor(func(ctx context.Context, a dispatch.Attempt) (dispatch.Result, error) {
		for {
			select {
			case <-ctx.Done():
				return dispatch.Result{}, ctx.Err()
			case <-time.After(2 * time.Millisecond):
				a.Progress()
			}
		}
	})
}

// ctxExecutor adapts a function to the Executor interface.
type ctxExecutor func(context.Context, dispatch.Attempt) (dispatch.Result, error)

func (f ctxExecutor) Execute(ctx context.Context, a dispatch.Attempt) (dispatch.Result, error) {
	return f(ctx, a)
}
