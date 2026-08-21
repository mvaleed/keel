package worker_test

import (
	"sync"
	"time"

	"github.com/keel/keel/worker"
)

// clock drives the registry's view of time, so a test does not wait for
// a TTL to pass.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock(m *worker.Memory) *clock {
	c := &clock{now: time.Now()}
	worker.SetClock(m, c.read)
	return c
}

func (c *clock) read() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
