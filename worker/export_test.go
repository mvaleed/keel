package worker

import "time"

// SetClock replaces the clock of m, so a test ages an entry without a
// wait. It is only for the tests of this package.
func SetClock(m *Memory, now func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = now
}

// Size reports how many workers m holds, live or not. It is only for
// the tests of this package.
func Size(m *Memory) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.workers)
}
