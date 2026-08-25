// Package dispatch runs the invocations that are due. It finds the work,
// claims it, and drives it to the end, while engine owns the submission.
package dispatch

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/keel/keel/invocation"
	"github.com/keel/keel/lease"
	"github.com/keel/keel/worker"
)

// ErrHandler wraps the error a handler returned. It ends the invocation,
// unlike a transport error, which the dispatcher retries.
var ErrHandler = errors.New("dispatch: handler failed")

// An Attempt is one run of one invocation on one worker. The epoch
// fences the journal, so an executor must carry it into every append.
type Attempt struct {
	Record invocation.Record
	Worker worker.Worker
	Epoch  lease.Epoch

	// Progress reports that the invocation advanced. It holds the lease
	// and the marker, so an executor that never calls it is cancelled.
	Progress func()
}

// A Result is what one attempt produced. Done separates a finished
// invocation from one that must run again, because a dropped connection
// is neither a success nor a failure.
type Result struct {
	Done   bool
	Output json.RawMessage

	// Err is set when the handler itself failed, which is terminal. A
	// transport error is not this; an executor returns that instead.
	Err error
}

// An Executor runs one attempt of an invocation. An implementation must
// replay the journal it is given, and must not assume it starts at zero.
//
// It must not touch the lease, the marker, or the record. The driver
// owns all three, and it reads Progress to know the attempt is alive.
type Executor interface {
	Execute(ctx context.Context, a Attempt) (Result, error)
}
