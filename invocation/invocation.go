// Package invocation identifies one run of a durable function, and the
// durable record that says the run must happen.
package invocation

import (
	"encoding/json"
	"path"
)

// An Invocation names one run of a handler that a service hosts. The ID
// comes from the caller, because a resume must name the run that
// already exists, and a retried registration must not start a new one.
type Invocation struct {
	ID      ID              `json:"id"`
	Service string          `json:"service"`
	Handler string          `json:"handler"`
	Input   json.RawMessage `json:"input,omitempty"`
}

// Key is the address of the invocation in a store or a lease. Validate
// rejects a separator in any part, so one key has one reading.
func (i Invocation) Key() string {
	return path.Join(i.Service, i.Handler, string(i.ID))
}
