package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/keel/keel/invocation"
)

// maxInputSize bounds the input of one invocation. The whole input goes
// to the service in one request body, so it must stay small.
const maxInputSize = 1 << 20

// server registers invocations. It does not run them; a dispatcher reads
// the pending index and does that.
type server struct {
	records  invocation.Store
	services map[string]string // service name -> invoke URL
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

// registerResponse tells the client the address it can poll.
type registerResponse struct {
	ID        string            `json:"id"`
	Service   string            `json:"service"`
	Handler   string            `json:"handler"`
	Status    invocation.Status `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
}

// register records the invocation and returns before anything runs. It
// answers 202 for a new invocation and 200 for a repeat of one.
func (s *server) register(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxInputSize+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "reading the request body")
		return
	}
	if len(body) > maxInputSize {
		writeError(w, http.StatusRequestEntityTooLarge, "the request is larger than 1 MiB")
		return
	}

	var req registerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "the request is not valid JSON")
		return
	}

	// Compact the input before it is hashed, so that whitespace alone
	// does not make one registration look like a conflict with itself.
	input, err := compact(req.Input)
	if err != nil {
		writeError(w, http.StatusBadRequest, "the input is not valid JSON")
		return
	}

	inv := invocation.Invocation{
		ID:      invocation.ID(req.ID),
		Service: req.Service,
		Handler: req.Handler,
		Input:   input,
	}
	if err := inv.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// An invocation for a service the engine cannot reach is a promise
	// that nothing can keep, so it must not be recorded.
	if _, ok := s.services[inv.Service]; !ok {
		writeError(w, http.StatusNotFound, "unknown service "+inv.Service)
		return
	}

	rec := invocation.Record{
		Invocation: inv,
		Status:     invocation.Pending,
		InputHash:  invocation.HashInput(input),
		CreatedAt:  time.Now().UTC(),
	}

	switch err := s.records.Create(r.Context(), rec); {
	case err == nil:
		w.Header().Set("Location", "/v1/invocations/"+inv.Key())
		writeJSON(w, http.StatusAccepted, response(rec))
	case errors.Is(err, invocation.ErrExists):
		s.repeat(w, r, rec)
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// repeat answers a registration whose address is already taken. The same
// input is a retry, and a different input is a reused id.
func (s *server) repeat(w http.ResponseWriter, r *http.Request, want invocation.Record) {
	got, err := s.records.Get(r.Context(), want.Key())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if got.InputHash != want.InputHash {
		writeError(w, http.StatusConflict,
			"invocation "+want.Key()+" is registered with a different input")
		return
	}
	writeJSON(w, http.StatusOK, response(got))
}

// get returns the recorded invocation, which a client polls because
// registration does not wait for the run.
func (s *server) get(w http.ResponseWriter, r *http.Request) {
	inv := invocation.Invocation{
		ID:      invocation.ID(r.PathValue("id")),
		Service: r.PathValue("service"),
		Handler: r.PathValue("handler"),
	}
	if err := inv.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	rec, err := s.records.Get(r.Context(), inv.Key())
	switch {
	case errors.Is(err, invocation.ErrNotFound):
		writeError(w, http.StatusNotFound, "no invocation "+inv.Key())
	case err != nil:
		writeError(w, http.StatusInternalServerError, err.Error())
	default:
		writeJSON(w, http.StatusOK, response(rec))
	}
}

func response(r invocation.Record) registerResponse {
	return registerResponse{
		ID:        string(r.ID),
		Service:   r.Service,
		Handler:   r.Handler,
		Status:    r.Status,
		CreatedAt: r.CreatedAt,
	}
}

// compact removes the whitespace from raw. It returns nil for no input,
// so an absent and an empty input hash the same.
func compact(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	var out bytes.Buffer
	if err := json.Compact(&out, raw); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		return
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
