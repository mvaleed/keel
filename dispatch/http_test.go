package dispatch_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/keel/keel/dispatch"
	"github.com/keel/keel/journal"
	"github.com/keel/keel/lease"
	"github.com/keel/keel/worker"
)

// attempt returns what the driver hands an executor for one record.
func attempt(rec string, url string) dispatch.Attempt {
	return dispatch.Attempt{
		Record:   pending("demo", "Charge", rec),
		Worker:   worker.Worker{Address: url},
		Epoch:    1,
		Progress: func() {},
	}
}

func TestHTTPExecutorAppendsTheNewEntries(t *testing.T) {
	t.Parallel()

	url, req := service(t, map[string]any{
		"output": json.RawMessage(`{"ok":true}`),
		"new_entries": []journal.Entry{
			{Step: 0, Name: "charge"},
			{Step: 1, Name: "ship"},
		},
	})
	store := newStore()
	a := attempt("id-1", url)
	a.Record.Input = json.RawMessage(`{"amount":5}`)

	res, err := dispatch.NewHTTPExecutor(store).Execute(t.Context(), a)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.Done || string(res.Output) != `{"ok":true}` {
		t.Fatalf("result = %+v, want a finished invocation", res)
	}

	writes := store.writes()
	if len(writes) != 2 || writes[0].Name != "charge" || writes[1].Name != "ship" {
		t.Fatalf("appended %+v, want charge then ship", writes)
	}
	// Every append carries the epoch of the lease the driver holds.
	for i, epoch := range store.epochs {
		if epoch != 1 {
			t.Fatalf("append %d used epoch %d, want 1", i, epoch)
		}
	}

	// The service gets the bare id, not the storage key.
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

func TestHTTPExecutorReportsProgressPerEntry(t *testing.T) {
	t.Parallel()

	// A journal entry is the evidence that holds the lease, so each one
	// must reach the driver as it lands.
	url, _ := service(t, map[string]any{
		"new_entries": []journal.Entry{{Step: 0, Name: "charge"}, {Step: 1, Name: "ship"}},
	})
	var steps int
	a := attempt("id-2", url)
	a.Progress = func() { steps++ }

	if _, err := dispatch.NewHTTPExecutor(newStore()).Execute(t.Context(), a); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if steps != 2 {
		t.Fatalf("progress reported %d times, want 2", steps)
	}
}

func TestHTTPExecutorSendsTheRecordedHistory(t *testing.T) {
	t.Parallel()

	url, req := service(t, map[string]any{"output": json.RawMessage(`null`)})
	store := newStore()
	store.history = []journal.Entry{
		{Step: 0, Name: "charge", Output: json.RawMessage(`{"id":"ch_1"}`)},
	}

	if _, err := dispatch.NewHTTPExecutor(store).Execute(t.Context(), attempt("id-3", url)); err != nil {
		t.Fatalf("Execute: %v", err)
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

func TestHTTPExecutorReturnsTheServiceError(t *testing.T) {
	t.Parallel()

	url, _ := service(t, map[string]any{
		"error":       "card declined",
		"new_entries": []journal.Entry{{Step: 0, Name: "charge"}},
	})
	store := newStore()

	res, err := dispatch.NewHTTPExecutor(store).Execute(t.Context(), attempt("id-4", url))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// A handler error ends the invocation, so it is Done and not an error.
	if !res.Done || !errors.Is(res.Err, dispatch.ErrHandler) {
		t.Fatalf("result = %+v, want a failed invocation", res)
	}
	if !strings.Contains(res.Err.Error(), "card declined") {
		t.Fatalf("err = %v, want card declined", res.Err)
	}
	// A failed invocation still records the steps that did run.
	if len(store.writes()) != 1 {
		t.Fatalf("appended %d entries, want 1", len(store.writes()))
	}
}

func TestHTTPExecutorStopsOnALostLease(t *testing.T) {
	t.Parallel()

	url, _ := service(t, map[string]any{
		"new_entries": []journal.Entry{{Step: 0, Name: "charge"}},
	})
	store := newStore()
	store.appendErr = lease.ErrLeaseLost

	res, err := dispatch.NewHTTPExecutor(store).Execute(t.Context(), attempt("id-5", url))
	if !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("err = %v, want ErrLeaseLost", err)
	}
	// The invocation is not finished, so the dispatcher must retry it.
	if res.Done {
		t.Fatalf("result = %+v, want an unfinished invocation", res)
	}
	// The message must name the invocation an operator has to look at.
	if !strings.Contains(err.Error(), "demo/Charge/id-5") {
		t.Fatalf("err %q does not name the invocation", err)
	}
}

func TestHTTPExecutorFailsWhenTheReadFails(t *testing.T) {
	t.Parallel()

	url, _ := service(t, map[string]any{})
	store := newStore()
	store.readErr = errors.New("bucket unreachable")

	// A partial history would make the service replay the wrong steps.
	_, err := dispatch.NewHTTPExecutor(store).Execute(t.Context(), attempt("id-6", url))
	if err == nil || !strings.Contains(err.Error(), "bucket unreachable") {
		t.Fatalf("err = %v, want the read error", err)
	}
}

func TestHTTPExecutorFailsWhenTheServiceIsUnreachable(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	res, err := dispatch.NewHTTPExecutor(newStore()).Execute(t.Context(), attempt("id-7", url))
	if err == nil {
		t.Fatal("Execute accepted an unreachable worker")
	}
	if res.Done {
		t.Fatalf("result = %+v, want an unfinished invocation", res)
	}
}

func TestHTTPExecutorFailsOnAnUndecodableReply(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write([]byte("not json")); err != nil {
			t.Errorf("writing reply: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	_, err := dispatch.NewHTTPExecutor(newStore()).Execute(t.Context(), attempt("id-8", srv.URL))
	if err == nil || !strings.Contains(err.Error(), "decoding response") {
		t.Fatalf("err = %v, want a decode error", err)
	}
}

func TestHTTPExecutorDialsThePathOfTheProtocol(t *testing.T) {
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

	// A worker announces a base address only, so the engine must add the
	// path of the invoke protocol itself.
	if _, err := dispatch.NewHTTPExecutor(newStore()).Execute(t.Context(), attempt("id-9", srv.URL)); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if path != "/keel/v1/invoke" {
		t.Fatalf("path = %q, want %q", path, "/keel/v1/invoke")
	}
}
