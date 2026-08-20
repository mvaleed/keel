// Package engine owns each invocation's durable journal and drives
// execution by calling the HTTP service that hosts the workflow code.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/keel/keel/invocation"
	"github.com/keel/keel/journal"
	"github.com/keel/keel/lease"
)

// leaseTTL bounds how long a crashed engine keeps an invocation. It must
// exceed one service call, but is downtime before another engine takes over.
const leaseTTL = 2 * time.Minute

// Engine invokes registered services and persists the journal entries
// they report back.
type Engine struct {
	store    journal.Store
	locker   lease.Locker
	owner    string            // identifies this engine in leases
	services map[string]string // service name -> base invoke URL
}

// New returns an Engine persisting to store and fencing with locker.
// owner identifies this engine in leases; two engines that share a store
// must not share an owner.
func New(store journal.Store, locker lease.Locker, owner string, services map[string]string) *Engine {
	return &Engine{store: store, locker: locker, owner: owner, services: services}
}

// invokeRequest is what the engine sends a service to run, or resume,
// one invocation.
type invokeRequest struct {
	InvocationID string          `json:"invocation_id"`
	Input        json.RawMessage `json:"input"`
	Journal      []journal.Entry `json:"journal"`
}

// invokeResponse is a service's reply. NewEntries are the steps it ran,
// which the engine persists before it returns Output to the caller.
type invokeResponse struct {
	Output     json.RawMessage `json:"output,omitempty"`
	Error      string          `json:"error,omitempty"`
	NewEntries []journal.Entry `json:"new_entries,omitempty"`
}

// Invoke runs, or resumes, the invocation service/id. It hands the
// journal to the service and appends the new steps the service ran.
func (e *Engine) Invoke(ctx context.Context, service, id string, input json.RawMessage) (json.RawMessage, error) {
	url, ok := e.services[service]
	if !ok {
		return nil, fmt.Errorf("unknown service %q", service)
	}

	inv := invocation.Invocation{ID: invocation.ID(id), Service: service, Input: input}
	return e.attempt(ctx, inv, url)
}

// attempt is one try at an invocation: claim, replay, run, append,
// release. It ends when the service replies or the lease is lost.
func (e *Engine) attempt(ctx context.Context, inv invocation.Invocation, url string) (json.RawMessage, error) {
	key := inv.Key()

	// Claim before reading, so the history stays stable and a second
	// engine cannot drive the same invocation.
	l, err := e.locker.Claim(ctx, key, e.owner, leaseTTL)
	if err != nil {
		return nil, fmt.Errorf("claiming %s: %w", key, err)
	}
	defer func() {
		_ = e.locker.Release(ctx, l)
	}()

	// The whole history goes in one request body, so collect it eagerly.
	// Read is an iterator so this can become a stream later.
	history, err := journal.Collect(e.store.Read(ctx, key))
	if err != nil {
		return nil, err
	}

	out, err := e.call(ctx, url, invokeRequest{
		InvocationID: string(inv.ID),
		Input:        inv.Input,
		Journal:      history,
	})
	if err != nil {
		return nil, err
	}

	for _, entry := range out.NewEntries {
		// A lost lease means another engine took the invocation over.
		// Stop, or the remaining steps interleave into its journal.
		if err := e.store.Append(ctx, key, l.Epoch, entry); err != nil {
			if errors.Is(err, lease.ErrLeaseLost) {
				return nil, fmt.Errorf("lost %s mid-invocation: %w", key, err)
			}
			return nil, err
		}
	}

	if out.Error != "" {
		return nil, fmt.Errorf("%s", out.Error)
	}
	return out.Output, nil
}

// call posts one invoke request to a service and decodes its reply.
func (e *Engine) call(ctx context.Context, url string, body invokeRequest) (*invokeResponse, error) {
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
