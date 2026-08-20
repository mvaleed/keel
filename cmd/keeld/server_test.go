package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/keel/keel/invocation"
)

// memStore is an invocation.Store that keeps records in a map. Create is
// write-once, like every real implementation.
type memStore struct {
	mu        sync.Mutex
	records   map[string]invocation.Record
	createErr error
	getErr    error
}

func newMemStore() *memStore {
	return &memStore{records: map[string]invocation.Record{}}
}

func (m *memStore) Create(_ context.Context, r invocation.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createErr != nil {
		return m.createErr
	}
	if _, ok := m.records[r.Key()]; ok {
		return invocation.ErrExists
	}
	m.records[r.Key()] = r
	return nil
}

func (m *memStore) Get(_ context.Context, key string) (invocation.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return invocation.Record{}, m.getErr
	}
	r, ok := m.records[key]
	if !ok {
		return invocation.Record{}, invocation.ErrNotFound
	}
	return r, nil
}

func newServer(t *testing.T) (http.Handler, *memStore) {
	t.Helper()
	store := newMemStore()
	s := &server{records: store, services: map[string]string{"billing": "http://svc"}}
	return s.routes(), store
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/invocations", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

const valid = `{"id":"order-1","service":"billing","handler":"Charge","input":{"amount":5}}`

func TestRegisterRecordsTheInvocation(t *testing.T) {
	t.Parallel()

	h, store := newServer(t)
	w := post(t, h, valid)

	if w.Code != http.StatusAccepted {
		t.Fatalf("code = %d, want 202: %s", w.Code, w.Body)
	}
	// The client polls the Location it is given.
	if got := w.Header().Get("Location"); got != "/v1/invocations/billing/Charge/order-1" {
		t.Fatalf("Location = %q", got)
	}

	var body registerResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if body.ID != "order-1" || body.Status != invocation.Pending {
		t.Fatalf("body = %+v, want order-1 pending", body)
	}
	if body.CreatedAt.IsZero() {
		t.Fatal("created_at is zero")
	}

	rec, err := store.Get(t.Context(), "billing/Charge/order-1")
	if err != nil {
		t.Fatalf("record was not stored: %v", err)
	}
	// The record must be durable before the client is told 202.
	if string(rec.Input) != `{"amount":5}` {
		t.Fatalf("input = %s", rec.Input)
	}
	if rec.Status != invocation.Pending {
		t.Fatalf("status = %q, want pending", rec.Status)
	}
}

func TestRegisterIsIdempotent(t *testing.T) {
	t.Parallel()

	h, _ := newServer(t)
	if w := post(t, h, valid); w.Code != http.StatusAccepted {
		t.Fatalf("first code = %d, want 202", w.Code)
	}

	// A retried registration must find the invocation, not start a
	// second run of it.
	w := post(t, h, valid)
	if w.Code != http.StatusOK {
		t.Fatalf("second code = %d, want 200: %s", w.Code, w.Body)
	}
}

func TestRegisterIgnoresInputWhitespace(t *testing.T) {
	t.Parallel()

	h, _ := newServer(t)
	post(t, h, valid)

	spaced := `{"id":"order-1","service":"billing","handler":"Charge","input":{ "amount" : 5 }}`
	if w := post(t, h, spaced); w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200: %s", w.Code, w.Body)
	}
}

func TestRegisterRejectsAReusedID(t *testing.T) {
	t.Parallel()

	h, _ := newServer(t)
	post(t, h, valid)

	other := `{"id":"order-1","service":"billing","handler":"Charge","input":{"amount":9999}}`
	w := post(t, h, other)
	if w.Code != http.StatusConflict {
		t.Fatalf("code = %d, want 409: %s", w.Code, w.Body)
	}
}

func TestRegisterRejectsBadRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{"not json", `{`, http.StatusBadRequest},
		{"no id", `{"service":"billing","handler":"Charge"}`, http.StatusBadRequest},
		{"no service", `{"id":"a","handler":"Charge"}`, http.StatusBadRequest},
		{"no handler", `{"id":"a","service":"billing"}`, http.StatusBadRequest},
		{"unknown service", `{"id":"a","service":"nope","handler":"Charge"}`, http.StatusNotFound},
		// The id becomes a storage key, so it must never escape.
		{"traversing id", `{"id":"../../x","service":"billing","handler":"Charge"}`, http.StatusBadRequest},
		{"slash in id", `{"id":"a/b","service":"billing","handler":"Charge"}`, http.StatusBadRequest},
		{"slash in service", `{"id":"a","service":"bill/ing","handler":"Charge"}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, store := newServer(t)
			if w := post(t, h, tt.body); w.Code != tt.want {
				t.Fatalf("code = %d, want %d: %s", w.Code, tt.want, w.Body)
			}
			// A rejected registration must record nothing.
			if len(store.records) != 0 {
				t.Fatalf("stored %d records, want 0", len(store.records))
			}
		})
	}
}

func TestRegisterRejectsAnOversizeBody(t *testing.T) {
	t.Parallel()

	h, store := newServer(t)
	big := `{"id":"a","service":"billing","handler":"Charge","input":{"pad":"` +
		strings.Repeat("x", maxInputSize) + `"}}`

	if w := post(t, h, big); w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code = %d, want 413", w.Code)
	}
	if len(store.records) != 0 {
		t.Fatalf("stored %d records, want 0", len(store.records))
	}
}

func TestRegisterReportsAStoreFailure(t *testing.T) {
	t.Parallel()

	h, store := newServer(t)
	store.createErr = errors.New("bucket unreachable")

	// The client must never be told 202 for something that is not
	// durable.
	if w := post(t, h, valid); w.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500", w.Code)
	}
}

func TestGetReturnsTheRecord(t *testing.T) {
	t.Parallel()

	h, _ := newServer(t)
	post(t, h, valid)

	w := get(t, h, "/v1/invocations/billing/Charge/order-1")
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200: %s", w.Code, w.Body)
	}

	var body registerResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	// Nothing runs the invocation yet, so it stays pending.
	if body.Status != invocation.Pending {
		t.Fatalf("status = %q, want pending", body.Status)
	}
}

func TestGetUnknownInvocation(t *testing.T) {
	t.Parallel()

	h, _ := newServer(t)
	if w := get(t, h, "/v1/invocations/billing/Charge/never"); w.Code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404", w.Code)
	}
}

func TestGetRejectsAnEncodedSeparator(t *testing.T) {
	t.Parallel()

	h, _ := newServer(t)
	// The mux decodes %2F after it matches, so the id reaches the
	// handler holding a separator. Validate is what stops it.
	if w := get(t, h, "/v1/invocations/billing/Charge/a%2Fb"); w.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400: %s", w.Code, w.Body)
	}
}

func TestGetDoesNotServeATraversal(t *testing.T) {
	t.Parallel()

	h, _ := newServer(t)
	// The mux cleans the path before it matches, so a traversal never
	// reaches a handler at all.
	w := get(t, h, "/v1/invocations/billing/Charge/..")
	if w.Code == http.StatusOK {
		t.Fatalf("code = %d, want anything but 200: %s", w.Code, w.Body)
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
