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
)

// maxRequestSize bounds one registration. The whole input goes to the
// service in one request body, so it must stay small.
const maxRequestSize = 1 << 20

// A registrar records invocations. The server declares what it needs, so
// a test needs no engine.
type registrar interface {
	Register(context.Context, invocation.Invocation) (engine.Registration, error)
	Lookup(context.Context, invocation.Invocation) (invocation.Record, error)
}

// server maps HTTP onto the engine. It holds no rule of its own, so the
// SDK protocol can reach the same rules another way.
type server struct {
	engine registrar
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/invocations", s.register)
	mux.HandleFunc("GET /v1/invocations/{service}/{handler}/{id}", s.get)
	return mux
}

// registerRequest is the body a client sends to register an invocation.
// The client supplies the id, which makes a retry safe to repeat.
type registerRequest struct {
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

// register records the invocation and answers before anything runs it.
func (s *server) register(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestSize+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "reading the request body")
		return
	}
	if len(body) > maxRequestSize {
		writeError(w, http.StatusRequestEntityTooLarge, "the request is larger than 1 MiB")
		return
	}

	var req registerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "the request is not valid JSON")
		return
	}

	reg, err := s.engine.Register(r.Context(), invocation.Invocation{
		ID:      invocation.ID(req.ID),
		Service: req.Service,
		Handler: req.Handler,
		Input:   req.Input,
	})
	if err != nil {
		writeError(w, statusFor(err), err.Error())
		return
	}

	// A repeat of a registration is not a new invocation, and the code
	// is what tells the client which one it got.
	code := http.StatusOK
	if reg.Created {
		code = http.StatusAccepted
		w.Header().Set("Location", "/v1/invocations/"+reg.Record.Key())
	}
	writeJSON(w, code, response(reg.Record))
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
	case errors.Is(err, invocation.ErrInvalid):
		return http.StatusBadRequest
	case errors.Is(err, engine.ErrUnknownService), errors.Is(err, invocation.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, engine.ErrInputConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
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
