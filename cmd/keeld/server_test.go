package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/keel/keel/engine"
	"github.com/keel/keel/invocation"
)

// fakeRegistrar stands in for the engine. The server holds no rule, so
// every case here is about HTTP.
type fakeRegistrar struct {
	reg     engine.Registration
	rec     invocation.Record
	err     error
	lastInv invocation.Invocation
}

func (f *fakeRegistrar) Register(_ context.Context, inv invocation.Invocation) (engine.Registration, error) {
	f.lastInv = inv
	if f.err != nil {
		return engine.Registration{}, f.err
	}
	return f.reg, nil
}

func (f *fakeRegistrar) Lookup(_ context.Context, inv invocation.Invocation) (invocation.Record, error) {
	f.lastInv = inv
	if f.err != nil {
		return invocation.Record{}, f.err
	}
	return f.rec, nil
}

func record() invocation.Record {
	return invocation.Record{
		Invocation: invocation.Invocation{
			ID: "order-1", Service: "billing", Handler: "Charge",
		},
		Status:    invocation.Pending,
		CreatedAt: time.Now().UTC(),
	}
}

func newServer(reg *fakeRegistrar) http.Handler {
	return (&server{engine: reg}).routes()
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/invocations", strings.NewReader(body)))
	return w
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

const valid = `{"id":"order-1","service":"billing","handler":"Charge","input":{"amount":5}}`

func TestRegisterAnswers202ForANewInvocation(t *testing.T) {
	t.Parallel()

	reg := &fakeRegistrar{reg: engine.Registration{Record: record(), Created: true}}
	w := post(t, newServer(reg), valid)

	if w.Code != http.StatusAccepted {
		t.Fatalf("code = %d, want 202: %s", w.Code, w.Body)
	}
	// The client polls the Location it is given.
	if got := w.Header().Get("Location"); got != "/v1/invocations/billing/Charge/order-1" {
		t.Fatalf("Location = %q", got)
	}

	var body invocationResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if body.ID != "order-1" || body.Status != invocation.Pending {
		t.Fatalf("body = %+v", body)
	}
}

func TestRegisterPassesTheInvocationThrough(t *testing.T) {
	t.Parallel()

	reg := &fakeRegistrar{reg: engine.Registration{Record: record(), Created: true}}
	post(t, newServer(reg), valid)

	// The body maps onto the invocation without the server reading it.
	got := reg.lastInv
	if got.ID != "order-1" || got.Service != "billing" || got.Handler != "Charge" {
		t.Fatalf("invocation = %+v", got)
	}
	if string(got.Input) != `{"amount":5}` {
		t.Fatalf("input = %s", got.Input)
	}
}

func TestRegisterAnswers200ForARepeat(t *testing.T) {
	t.Parallel()

	reg := &fakeRegistrar{reg: engine.Registration{Record: record()}}
	w := post(t, newServer(reg), valid)

	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200: %s", w.Code, w.Body)
	}
	// A repeat created nothing, so there is nothing new to point at.
	if got := w.Header().Get("Location"); got != "" {
		t.Fatalf("Location = %q, want none", got)
	}
}

func TestRegisterMapsTheEngineErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want int
	}{
		{"invalid", invocation.ErrInvalid, http.StatusBadRequest},
		{"unknown service", engine.ErrUnknownService, http.StatusNotFound},
		{"reused id", engine.ErrInputConflict, http.StatusConflict},
		{"store failed", errors.New("bucket unreachable"), http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := post(t, newServer(&fakeRegistrar{err: tt.err}), valid)
			if w.Code != tt.want {
				t.Fatalf("code = %d, want %d: %s", w.Code, tt.want, w.Body)
			}
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Fatalf("body = %s, want an error message", w.Body)
			}
		})
	}
}

func TestRegisterRejectsAnUndecodableBody(t *testing.T) {
	t.Parallel()

	reg := &fakeRegistrar{reg: engine.Registration{Record: record(), Created: true}}
	if w := post(t, newServer(reg), `{`); w.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400", w.Code)
	}
	// A body the server cannot read must never reach the engine.
	if reg.lastInv.Service != "" {
		t.Fatalf("the engine saw %+v", reg.lastInv)
	}
}

func TestRegisterRejectsAnOversizeBody(t *testing.T) {
	t.Parallel()

	reg := &fakeRegistrar{reg: engine.Registration{Record: record(), Created: true}}
	big := `{"id":"a","service":"billing","handler":"Charge","input":{"pad":"` +
		strings.Repeat("x", maxRequestSize) + `"}}`

	if w := post(t, newServer(reg), big); w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code = %d, want 413", w.Code)
	}
	if reg.lastInv.Service != "" {
		t.Fatalf("the engine saw %+v", reg.lastInv)
	}
}

func TestGetReturnsTheRecord(t *testing.T) {
	t.Parallel()

	reg := &fakeRegistrar{rec: record()}
	w := get(t, newServer(reg), "/v1/invocations/billing/Charge/order-1")

	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200: %s", w.Code, w.Body)
	}
	// The path maps onto the address the engine looks up.
	if reg.lastInv.Key() != "billing/Charge/order-1" {
		t.Fatalf("looked up %q", reg.lastInv.Key())
	}
}

func TestGetMapsTheEngineErrors(t *testing.T) {
	t.Parallel()

	reg := &fakeRegistrar{err: invocation.ErrNotFound}
	if w := get(t, newServer(reg), "/v1/invocations/billing/Charge/never"); w.Code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404", w.Code)
	}

	bad := &fakeRegistrar{err: invocation.ErrInvalid}
	if w := get(t, newServer(bad), "/v1/invocations/billing/Charge/a%2Fb"); w.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400", w.Code)
	}
}

func TestGetDoesNotServeATraversal(t *testing.T) {
	t.Parallel()

	reg := &fakeRegistrar{rec: record()}
	// The mux cleans the path before it matches, so a traversal never
	// reaches a handler at all.
	if w := get(t, newServer(reg), "/v1/invocations/billing/Charge/.."); w.Code == http.StatusOK {
		t.Fatalf("code = %d, want anything but 200", w.Code)
	}
}

func TestParseServices(t *testing.T) {
	t.Parallel()

	got, err := parseServices("a=http://a,b=http://b")
	if err != nil {
		t.Fatalf("parseServices: %v", err)
	}
	if got["a"] != "http://a" || got["b"] != "http://b" {
		t.Fatalf("services = %v", got)
	}
	if _, err := parseServices("broken"); err == nil {
		t.Fatal("parseServices accepted an entry with no url")
	}
	empty, err := parseServices("")
	if err != nil || len(empty) != 0 {
		t.Fatalf("parseServices(\"\") = %v, %v", empty, err)
	}
}
