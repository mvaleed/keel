package worker

import (
	"sort"
	"sync"
	"time"
)

// Memory holds the live workers in this process. It is deliberately not
// durable: a lost registry costs one heartbeat, and no invocation.
type Memory struct {
	// now reads the clock. A test replaces it to age an entry without
	// waiting for the TTL.
	now func() time.Time

	mu      sync.Mutex
	workers map[string]entry
	next    map[string]int // service -> round-robin cursor
}

// entry is one announcement and the time it stops counting as live.
type entry struct {
	worker  Worker
	expires time.Time
}

// NewMemory returns an empty registry.
func NewMemory() *Memory {
	return &Memory{
		now:     time.Now,
		workers: map[string]entry{},
		next:    map[string]int{},
	}
}

// Register adds w, or extends the worker that has the same ID. One call
// serves both the first announcement and every heartbeat after it.
func (m *Memory) Register(w Worker) error {
	if err := w.Validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	m.dropExpired(now)
	m.workers[w.ID] = entry{worker: w, expires: now.Add(TTL)}
	return nil
}

// Deregister drops the worker at once, which a worker calls when it
// stops. Dropping an unknown worker is not an error.
func (m *Memory) Deregister(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.workers, id)
	return nil
}

// Pick returns one live worker that serves the handler. It takes the
// candidates in turn, so replicas of one service share the load.
func (m *Memory) Pick(service, handler string) (Worker, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	var live []Worker
	for _, e := range m.workers {
		if e.worker.Service == service && e.worker.Serves(handler) && now.Before(e.expires) {
			live = append(live, e.worker)
		}
	}
	if len(live) == 0 {
		return Worker{}, ErrNoWorker
	}

	// Sort by ID, so the cursor names the same worker on every call and
	// the map's random order does not reach the caller.
	sort.Slice(live, func(i, j int) bool { return live[i].ID < live[j].ID })

	i := m.next[service] % len(live)
	m.next[service] = i + 1
	return live[i], nil
}

// dropExpired removes the workers that stopped beating. Pick ignores
// them already, so this only keeps the registry from growing.
func (m *Memory) dropExpired(now time.Time) {
	for id, e := range m.workers {
		if !now.Before(e.expires) {
			delete(m.workers, id)
		}
	}
}
