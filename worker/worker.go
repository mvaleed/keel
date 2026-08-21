// Package worker keeps the set of processes that can run a handler. A
// worker announces itself, so the engine holds no address in its config.
package worker

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"time"

	"github.com/keel/keel/invocation"
)

const (
	// TTL is how long one announcement keeps a worker live. It must
	// exceed Heartbeat, or a healthy worker drops out between beats.
	TTL = 30 * time.Second

	// Heartbeat is the longest gap the engine accepts between two
	// announcements. A worker must call Register more often than this.
	Heartbeat = 10 * time.Second
)

var (
	// ErrInvalid is returned by Register for an announcement the engine
	// cannot use, such as an address it cannot dial.
	ErrInvalid = errors.New("worker: invalid registration")

	// ErrNoWorker is returned by Pick when no live worker serves the
	// handler. It is a transient state, not a permanent failure.
	ErrNoWorker = errors.New("worker: no live worker")
)

// A Worker is one process that announced itself to the engine. The
// worker supplies Address, because it cannot read its own from a socket.
type Worker struct {
	ID       string   `json:"id"`
	Service  string   `json:"service"`
	Handlers []string `json:"handlers"`
	Address  string   `json:"address"`
}

// Validate reports whether the engine can store and dial the worker.
func (w Worker) Validate() error {
	if err := invocation.ValidName(w.ID); err != nil {
		return fmt.Errorf("%w: id: %s", ErrInvalid, err)
	}
	if err := invocation.ValidName(w.Service); err != nil {
		return fmt.Errorf("%w: service: %s", ErrInvalid, err)
	}
	if len(w.Handlers) == 0 {
		return fmt.Errorf("%w: no handlers", ErrInvalid)
	}
	for _, h := range w.Handlers {
		if err := invocation.ValidName(h); err != nil {
			return fmt.Errorf("%w: handler: %s", ErrInvalid, err)
		}
	}
	return validAddress(w.Address)
}

// Serves reports whether the worker announced handler.
func (w Worker) Serves(handler string) bool {
	return slices.Contains(w.Handlers, handler)
}

// validAddress rejects an address the engine cannot dial. A worker
// behind a mapped port must announce the address, so a guess is wrong.
func validAddress(addr string) error {
	u, err := url.Parse(addr)
	if err != nil {
		return fmt.Errorf("%w: address %q: %s", ErrInvalid, addr, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: address %q needs an http scheme", ErrInvalid, addr)
	}
	if u.Host == "" {
		return fmt.Errorf("%w: address %q has no host", ErrInvalid, addr)
	}
	return nil
}

// A Registry holds the live workers. Implementations must be
// concurrency-safe, because every dispatch reads one.
type Registry interface {
	// Register adds the worker, or extends the one that has the same
	// ID. A worker calls it to announce itself and to stay live.
	Register(w Worker) error

	// Deregister drops the worker at once. Dropping an unknown worker
	// is not an error, because a shutdown may repeat the call.
	Deregister(id string) error

	// Pick returns one live worker that serves the handler, and
	// ErrNoWorker if there is none.
	Pick(service, handler string) (Worker, error)
}
