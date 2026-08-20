// Package invocation identifies one run of a workflow handler. An
// Invocation is a value: it holds no store, no lease, and no state.
package invocation

import "encoding/json"

// An Invocation names one run of a service handler. The ID comes from
// the caller, because a resume must name the run that already exists.
type Invocation struct {
	ID      ID
	Service string
	Input   json.RawMessage
}

// Key is the name of the invocation in a store or a lease. It joins the
// service, so two services never share one journal.
func (i Invocation) Key() string {
	return i.Service + "-" + string(i.ID)
}
