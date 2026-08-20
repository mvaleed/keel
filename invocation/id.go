package invocation

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// An ID names one invocation. A caller supplies its own id, so NewID is
// only the default for a caller that has none.
type ID string

func (i ID) String() string { return string(i) }

// gen holds the id generator state. The mutex covers the timestamp and
// the entropy source together, so ids come out in the order they are made.
var gen struct {
	sync.Mutex
	// entropy increments the random component for ids that share a
	// millisecond, so ids are strictly increasing within a tick.
	entropy *ulid.MonotonicEntropy
	// lastMS is the timestamp of the previous id. It is used again if
	// the clock goes backwards, so ids never sort backwards.
	lastMS uint64
}

func init() {
	gen.entropy = ulid.Monotonic(rand.Reader, 0)
}

// NewID returns a fresh ULID. It returns an error if too many ids were
// made in one millisecond.
func NewID() (ID, error) {
	ms := ulid.Timestamp(time.Now())

	gen.Lock()
	defer gen.Unlock()

	if ms < gen.lastMS {
		ms = gen.lastMS
	}

	id, err := ulid.New(ms, gen.entropy)
	if err != nil {
		return "", err
	}
	gen.lastMS = ms
	return ID(id.String()), nil
}
