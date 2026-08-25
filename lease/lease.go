// Package lease gives exclusive, time-bounded permission to act on a
// named resource, and the fencing token that orders the holders.
//
// # Contract for an implementer
//
// One holder per resource. Claim gives exclusive permission. Two live
// leases for one resource must never exist, so the backend needs an
// atomic compare-and-set on the stored lease. A backend with no such
// primitive cannot implement Locker correctly.
//
// Epoch orders the holders. Each successful Claim returns an epoch
// strictly greater than every epoch before it, and a released lease
// keeps its epoch so a later holder never repeats one. A holder gives
// its epoch to the resource it writes, which lets that resource name
// the writer and reject an obviously stale one.
//
// An epoch is not a substitute for an atomic write. A resource that
// checks the epoch separately from the write leaves a gap in which the
// lease changes hands, so a resource must protect itself and must treat
// the epoch as a hint. This package grants exclusion, not safety.
//
// A lease can be lost at any moment. The local expiry is a hint; the
// authority is the stored lease. Renew reports ErrLeaseLost, and the
// holder must then stop its work.
package lease

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

var (
	// ErrClaimHeld is returned by Claim when another owner holds an
	// unexpired lease on the resource.
	ErrClaimHeld = errors.New("lease: resource claimed by another owner")

	// ErrLeaseLost is returned by Renew, and by a write that checks the
	// epoch, when the lease expired or another owner took over. The
	// caller must stop and must re-Claim before it continues.
	ErrLeaseLost = errors.New("lease: lease no longer held")
)

// An Epoch orders the holders of one resource. It only ever increases,
// so a write that carries a lower epoch comes from a stale holder.
type Epoch uint64

// A Lease is exclusive, time-bounded permission to act on one resource.
// A Lease is safe for concurrent use.
type Lease struct {
	Resource string
	Owner    string
	Epoch    Epoch

	// mu guards expires, because Renew writes it while the holder reads
	// it.
	mu      sync.RWMutex
	expires time.Time
}

// A Locker hands out the leases for its resources. Implementations must
// be concurrency-safe.
type Locker interface {
	// Claim takes the exclusive lease on a resource for ttl, and returns
	// ErrClaimHeld if a live lease belongs to another owner.
	Claim(ctx context.Context, resource, owner string, ttl time.Duration) (*Lease, error)

	// Renew extends the lease by ttl under the same epoch, and returns
	// ErrLeaseLost if the stored lease is no longer this holder's.
	Renew(ctx context.Context, l *Lease, ttl time.Duration) error

	// Release drops a lease so the next claimant need not wait out the
	// ttl. Releasing a lost lease is not an error.
	Release(ctx context.Context, l *Lease) error
}

// New returns a lease. Only a Locker implementation calls it.
func New(resource, owner string, epoch Epoch, expires time.Time) *Lease {
	return &Lease{Resource: resource, Owner: owner, Epoch: epoch, expires: expires}
}

// Expires returns the time the lease stops being valid.
func (l *Lease) Expires() time.Time {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.expires
}

// Expired reports whether the local clock is past the expiry. It is a
// hint, because the authority is the stored lease.
func (l *Lease) Expired() bool {
	return time.Now().After(l.Expires())
}

// Extend moves the expiry later. Only a Locker implementation calls it,
// from Renew, and it never moves the expiry backwards.
func (l *Lease) Extend(expires time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if expires.After(l.expires) {
		l.expires = expires
	}
}

// leaseJSON is the wire shape of a Lease. It keeps expires readable
// after the field became unexported.
type leaseJSON struct {
	Resource string    `json:"resource"`
	Owner    string    `json:"owner"`
	Epoch    Epoch     `json:"epoch"`
	Expires  time.Time `json:"expires"`
}

func (l *Lease) MarshalJSON() ([]byte, error) {
	return json.Marshal(leaseJSON{
		Resource: l.Resource,
		Owner:    l.Owner,
		Epoch:    l.Epoch,
		Expires:  l.Expires(),
	})
}

func (l *Lease) UnmarshalJSON(b []byte) error {
	var v leaseJSON
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	l.Resource = v.Resource
	l.Owner = v.Owner
	l.Epoch = v.Epoch
	l.mu.Lock()
	defer l.mu.Unlock()
	l.expires = v.Expires
	return nil
}
