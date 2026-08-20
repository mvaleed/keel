// Package engine owns the rules of an invocation: how one is registered,
// and how one runs. A transport decodes a request and calls the engine;
// it must not hold a rule of its own, or a second transport repeats it.
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

var (
	// ErrUnknownService is returned when no service hosts the handler.
	// The engine must not record an invocation it cannot reach.
	ErrUnknownService = errors.New("engine: unknown service")

	// ErrInputConflict is returned by Register when the address holds
	// another input. The id is reused, and the caller must pick a new one.
	ErrInputConflict = errors.New("engine: id registered with a different input")
)

// Config holds what an Engine needs. One backend may satisfy all three
// stores, and the engine must not know whether it does.
type Config struct {
	Records  invocation.Store
	Journal  journal.Store
	Locker   lease.Locker
	Services map[string]string // service name -> invoke URL

	// Owner identifies this engine in a lease. Two engines that share a
	// store must not share an owner, or one takes the other's lease.
	Owner string
}

// Engine registers invocations, and runs the ones a dispatcher gives it.
type Engine struct {
	cfg Config
}

// New returns an Engine. It returns an error if a required part of cfg
// is missing, because a nil store fails later at an unhelpful place.
func New(cfg Config) (*Engine, error) {
	switch {
	case cfg.Records == nil:
		return nil, errors.New("engine: nil record store")
	case cfg.Journal == nil:
		return nil, errors.New("engine: nil journal store")
	case cfg.Locker == nil:
		return nil, errors.New("engine: nil locker")
	case cfg.Owner == "":
		return nil, errors.New("engine: empty owner")
	}
	return &Engine{cfg: cfg}, nil
}

// A Registration is the answer to Register. Created is false when the
// call repeated a registration that already existed.
type Registration struct {
	Record  invocation.Record
	Created bool
}

// Register records that inv must run, and returns before it does. The
// caller supplies the id, so a repeat of one call is not a second run.
//
// It returns ErrInputConflict when the address holds another input,
// invocation.ErrInvalid for an address that cannot be stored, and
// ErrUnknownService when no service hosts the handler.
func (e *Engine) Register(ctx context.Context, inv invocation.Invocation) (Registration, error) {
	if err := inv.Validate(); err != nil {
		return Registration{}, err
	}
	if _, ok := e.cfg.Services[inv.Service]; !ok {
		return Registration{}, fmt.Errorf("%w %q", ErrUnknownService, inv.Service)
	}

	input, err := invocation.Compact(inv.Input)
	if err != nil {
		return Registration{}, err
	}
	inv.Input = input

	rec := invocation.Record{
		Invocation: inv,
		Status:     invocation.Pending,
		InputHash:  invocation.HashInput(input),
		CreatedAt:  time.Now().UTC(),
	}

	switch err := e.cfg.Records.Create(ctx, rec); {
	case err == nil:
		return Registration{Record: rec, Created: true}, nil
	case errors.Is(err, invocation.ErrExists):
		return e.repeat(ctx, rec)
	default:
		return Registration{}, err
	}
}

// repeat answers a Register whose address is taken. The same input is a
// retry, and another input is an id that two invocations want.
func (e *Engine) repeat(ctx context.Context, want invocation.Record) (Registration, error) {
	got, err := e.cfg.Records.Get(ctx, want.Key())
	if err != nil {
		return Registration{}, err
	}
	if got.InputHash != want.InputHash {
		return Registration{}, fmt.Errorf("%w: %s", ErrInputConflict, want.Key())
	}
	return Registration{Record: got}, nil
}

// Lookup returns the recorded invocation, and invocation.ErrNotFound if
// there is none. A caller polls it, because Register does not wait.
func (e *Engine) Lookup(ctx context.Context, inv invocation.Invocation) (invocation.Record, error) {
	if err := inv.Validate(); err != nil {
		return invocation.Record{}, err
	}
	return e.cfg.Records.Get(ctx, inv.Key())
}

// invokeRequest is what the engine sends a service to run, or resume,
// one invocation.
type invokeRequest struct {
	InvocationID string          `json:"invocation_id"`
	Handler      string          `json:"handler"`
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

// Invoke runs, or resumes, inv. It hands the journal to the service that
// hosts the handler, and appends the new steps the service ran.
func (e *Engine) Invoke(ctx context.Context, inv invocation.Invocation) (json.RawMessage, error) {
	if err := inv.Validate(); err != nil {
		return nil, err
	}
	url, ok := e.cfg.Services[inv.Service]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownService, inv.Service)
	}
	return e.attempt(ctx, inv, url)
}

// attempt is one try at an invocation: claim, replay, run, append,
// release. It ends when the service replies or the lease is lost.
func (e *Engine) attempt(ctx context.Context, inv invocation.Invocation, url string) (json.RawMessage, error) {
	key := inv.Key()

	// Claim before reading, so the history stays stable and a second
	// engine cannot drive the same invocation.
	l, err := e.cfg.Locker.Claim(ctx, key, e.cfg.Owner, leaseTTL)
	if err != nil {
		return nil, fmt.Errorf("claiming %s: %w", key, err)
	}
	defer func() {
		_ = e.cfg.Locker.Release(ctx, l)
	}()

	// The whole history goes in one request body, so collect it eagerly.
	// Read is an iterator so this can become a stream later.
	history, err := journal.Collect(e.cfg.Journal.Read(ctx, key))
	if err != nil {
		return nil, err
	}

	out, err := e.call(ctx, url, invokeRequest{
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
		if err := e.cfg.Journal.Append(ctx, key, l.Epoch, entry); err != nil {
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
