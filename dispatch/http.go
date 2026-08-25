package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/keel/keel/journal"
	"github.com/keel/keel/lease"
)

// invokePath is where a worker serves the engine. A worker announces a
// base address, so the engine owns the path and the operator does not.
const invokePath = "/keel/v1/invoke"

// invokeRequest is what the engine sends a service to run, or resume,
// one invocation.
type invokeRequest struct {
	InvocationID string          `json:"invocation_id"`
	Handler      string          `json:"handler"`
	Input        json.RawMessage `json:"input"`
	Journal      []journal.Entry `json:"journal"`
}

// invokeResponse is a service's reply. NewEntries are the steps it ran,
// which the engine persists before it reports the outcome.
type invokeResponse struct {
	Output     json.RawMessage `json:"output,omitempty"`
	Error      string          `json:"error,omitempty"`
	NewEntries []journal.Entry `json:"new_entries,omitempty"`
}

// httpExecutor runs one attempt with one HTTP call to the worker. It
// reports progress only at the end, so a handler longer than the lease
// ttl is cancelled.
type httpExecutor struct {
	journal journal.Store
}

// NewHTTPExecutor returns the executor that calls a worker over HTTP. It
// is the one implementation until the engine holds a connection.
func NewHTTPExecutor(j journal.Store) Executor {
	return httpExecutor{journal: j}
}

// Execute makes the call and sorts the three outcomes. Only a handler
// error ends the invocation; every other error leaves it unfinished.
func (x httpExecutor) Execute(ctx context.Context, a Attempt) (Result, error) {
	out, err := x.run(ctx, a)
	switch {
	case err == nil:
		return Result{Done: true, Output: out}, nil
	case errors.Is(err, ErrHandler):
		return Result{Done: true, Err: err}, nil
	default:
		return Result{}, err
	}
}

// run replays the recorded journal into the worker and appends the steps
// the worker ran. It returns ErrHandler when the handler itself failed.
func (x httpExecutor) run(ctx context.Context, a Attempt) (json.RawMessage, error) {
	inv := a.Record.Invocation
	key := inv.Key()

	// The whole history goes in one request body, so collect it eagerly.
	// Read is an iterator so this can become a stream later.
	history, err := journal.Collect(x.journal.Read(ctx, key))
	if err != nil {
		return nil, err
	}

	out, err := x.call(ctx, a.Worker.Address+invokePath, invokeRequest{
		InvocationID: string(inv.ID),
		Handler:      inv.Handler,
		Input:        inv.Input,
		Journal:      history,
	})
	if err != nil {
		return nil, err
	}

	for _, entry := range out.NewEntries {
		// A lost lease means another engine took the invocation over.
		// Stop, or the remaining steps interleave into its journal.
		if err := x.journal.Append(ctx, key, a.Epoch, entry); err != nil {
			if errors.Is(err, lease.ErrLeaseLost) {
				return nil, fmt.Errorf("lost %s mid-invocation: %w", key, err)
			}
			return nil, err
		}
		a.Progress()
	}

	if out.Error != "" {
		return nil, fmt.Errorf("%w: %s", ErrHandler, out.Error)
	}
	return out.Output, nil
}

// call posts one invoke request to a service and decodes its reply.
func (x httpExecutor) call(ctx context.Context, url string, body invokeRequest) (*invokeResponse, error) {
	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling %q: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	var out invokeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decoding response from %q: %w", url, err)
	}
	return &out, nil
}
