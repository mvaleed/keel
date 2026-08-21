package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/keel/keel/engine"
	"github.com/keel/keel/invocation"
	"github.com/keel/keel/worker"
)

// maxRequestSize bounds one submission. The whole input goes to the
// worker in one request body, so it must stay small.
const maxRequestSize = 1 << 20

// invocations is the half of the engine that answers a client.
type invocations interface {
	Submit(context.Context, invocation.Invocation) (engine.Submission, error)
	Lookup(context.Context, invocation.Invocation) (invocation.Record, error)
}

// workers is the half of the engine that answers a worker.
type workers interface {
	RegisterWorker(worker.Worker) (time.Duration, error)
	DeregisterWorker(id string) error
}

// A coordinator is what the server needs of the engine. The server
// declares it, so a test needs no engine to exercise a route.
type coordinator interface {
	invocations
	workers
}

// server maps HTTP onto the engine. It holds no rule of its own, so the
// SDK protocol can reach the same rules another way.
type server struct {
	engine coordinator
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/invocations", s.submit)
	mux.HandleFunc("GET /v1/invocations/{service}/{handler}/{id}", s.get)
	mux.HandleFunc("POST /v1/workers", s.registerWorker)
	mux.HandleFunc("DELETE /v1/workers/{id}", s.deregisterWorker)
	return mux
}

// submitRequest is the body a client sends to submit an invocation. The
// client supplies the id, which makes a retry safe to repeat.
type submitRequest struct {
	ID      string          `json:"id"`
	Service string          `json:"service"`
	Handler string          `json:"handler"`
	Input   json.RawMessage `json:"input,omitempty"`
}

// invocationResponse tells the client the address it can poll.
type invocationResponse struct {
	ID        string            `json:"id"`
	Service   string            `json:"service"`
	Handler   string            `json:"handler"`
	Status    invocation.Status `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
}

// workerResponse tells a worker how long it may wait before it must
// announce itself again.
type workerResponse struct {
	HeartbeatSeconds int `json:"heartbeat_seconds"`
}

// registerWorker adds the worker, or keeps a registered one live. One
// route serves both, so a worker recovers from an engine restart.
func (s *server) registerWorker(w http.ResponseWriter, r *http.Request) {
	var req worker.Worker
	if err := decode(w, r, &req); err != nil {
		return
	}

	beat, err := s.engine.RegisterWorker(req)
	if err != nil {
		writeError(w, statusFor(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, workerResponse{HeartbeatSeconds: int(beat.Seconds())})
}

// deregisterWorker drops the worker, which it calls when it stops.
func (s *server) deregisterWorker(w http.ResponseWriter, r *http.Request) {
	if err := s.engine.DeregisterWorker(r.PathValue("id")); err != nil {
		writeError(w, statusFor(err), err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// submit records the invocation and answers before anything runs it.
func (s *server) submit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := decode(w, r, &req); err != nil {
		return
	}

	sub, err := s.engine.Submit(r.Context(), invocation.Invocation{
		ID:      invocation.ID(req.ID),
		Service: req.Service,
		Handler: req.Handler,
		Input:   req.Input,
	})
	if err != nil {
		writeError(w, statusFor(err), err.Error())
		return
	}

	// A repeat of a submission is not a new invocation, and the code is
	// what tells the client which one it got.
	code := http.StatusOK
	if sub.Created {
		code = http.StatusAccepted
		w.Header().Set("Location", "/v1/invocations/"+sub.Record.Key())
	}
	writeJSON(w, code, response(sub.Record))
}

// get returns the recorded invocation, which a client polls because
// registration does not wait for the run.
func (s *server) get(w http.ResponseWriter, r *http.Request) {
	rec, err := s.engine.Lookup(r.Context(), invocation.Invocation{
		ID:      invocation.ID(r.PathValue("id")),
		Service: r.PathValue("service"),
		Handler: r.PathValue("handler"),
	})
	if err != nil {
		writeError(w, statusFor(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response(rec))
}

// statusFor maps a domain error onto a status code. It is the only
// place that knows both, so a new transport maps the same errors again.
func statusFor(err error) int {
	switch {
	case errors.Is(err, invocation.ErrInvalid), errors.Is(err, worker.ErrInvalid):
		return http.StatusBadRequest
	case errors.Is(err, invocation.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, engine.ErrInputConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

// decode reads one bounded JSON body into v. It answers the request
// itself on failure, so a handler only returns.
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestSize+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "reading the request body")
		return err
	}
	if len(body) > maxRequestSize {
		writeError(w, http.StatusRequestEntityTooLarge, "the request is larger than 1 MiB")
		return errors.New("request too large")
	}
	if err := json.Unmarshal(body, v); err != nil {
		writeError(w, http.StatusBadRequest, "the request is not valid JSON")
		return err
	}
	return nil
}

func response(r invocation.Record) invocationResponse {
	return invocationResponse{
		ID:        string(r.ID),
		Service:   r.Service,
		Handler:   r.Handler,
		Status:    r.Status,
		CreatedAt: r.CreatedAt,
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
