package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/keel/keel/journal"
	"github.com/keel/keel/lease"
)

const workerWebSocketEndpoint = "/keel/v1/invoke"

// protocolV1 is the subprotocol the engine offers in the handshake, and
// the worker must accept. See docs/worker-protocol.md.
const protocolV1 = "keel.v1"

const (
	frameStart     = "start"
	frameEntry     = "entry"
	frameAccepted  = "accepted"
	frameSucceeded = "succeeded"
	frameFailed    = "failed"
	frameCancel    = "cancel"
)

// streamFrame is one message in a connection for one invocation.
type streamFrame struct {
	Type         string          `json:"type"`
	InvocationID string          `json:"invocation_id,omitempty"`
	Handler      string          `json:"handler,omitempty"`
	Input        json.RawMessage `json:"input,omitempty"`
	// Journal is the whole history, and only the start frame carries
	// it. Execute always sets the pointer, so an empty history is an
	// empty array and never an absent field.
	Journal      *[]journal.Entry `json:"journal,omitempty"`
	Entry        *journal.Entry  `json:"entry,omitempty"`
	Step         *int            `json:"step,omitempty"`
	Output       json.RawMessage `json:"output,omitempty"`
	Error        string          `json:"error,omitempty"`
}

// wsExecutor runs one attempt on one WebSocket connection.
type wsExecutor struct {
	journal journal.Store
}

// NewWSExecutor returns an executor that receives journal entries as frames.
func NewWSExecutor(j journal.Store) Executor {
	return wsExecutor{journal: j}
}

func (e wsExecutor) Execute(ctx context.Context, a Attempt) (Result, error) {
	inv := a.Record.Invocation
	key := inv.Key()

	history, err := journal.Collect(e.journal.Read(ctx, key))
	if err != nil {
		return Result{}, err
	}

	address, err := websocketURL(a.Worker.Address)
	if err != nil {
		return Result{}, err
	}
	conn, _, err := websocket.Dial(ctx, address, &websocket.DialOptions{
		Subprotocols: []string{protocolV1},
	})
	if err != nil {
		return Result{}, fmt.Errorf("connecting to worker %q: %w", address, err)
	}
	defer conn.CloseNow()
	// A worker that accepts the connection without the subprotocol does
	// not speak a version the engine offered, so the frames mean nothing.
	if conn.Subprotocol() != protocolV1 {
		return Result{}, fmt.Errorf("worker %q answered with subprotocol %q, want %q",
			address, conn.Subprotocol(), protocolV1)
	}

	if history == nil {
		history = []journal.Entry{}
	}
	start := streamFrame{
		Type:         frameStart,
		InvocationID: string(inv.ID),
		Handler:      inv.Handler,
		Input:        inv.Input,
		Journal:      &history,
	}
	if err := wsjson.Write(ctx, conn, start); err != nil {
		return Result{}, fmt.Errorf("sending start to worker: %w", err)
	}

	for {
		var frame streamFrame
		if err := wsjson.Read(ctx, conn, &frame); err != nil {
			return Result{}, fmt.Errorf("reading worker frame before terminal result: %w", err)
		}

		switch frame.Type {
		case frameEntry:
			if frame.Entry == nil {
				return Result{}, errors.New("dispatch: entry frame has no entry")
			}
			if err := e.journal.Append(ctx, key, a.Epoch, *frame.Entry); err != nil {
				if errors.Is(err, lease.ErrLeaseLost) {
					e.cancel(conn, "lease lost")
				}
				return Result{}, fmt.Errorf("appending step %d for %s: %w", frame.Entry.Step, key, err)
			}
			a.Progress()

			step := frame.Entry.Step
			if err := wsjson.Write(ctx, conn, streamFrame{Type: frameAccepted, Step: &step}); err != nil {
				return Result{}, fmt.Errorf("accepting step %d for %s: %w", step, key, err)
			}

		case frameSucceeded:
			_ = conn.Close(websocket.StatusNormalClosure, "done")
			return Result{Done: true, Output: frame.Output}, nil

		case frameFailed:
			if frame.Error == "" {
				return Result{}, errors.New("dispatch: failed frame has no error")
			}
			_ = conn.Close(websocket.StatusNormalClosure, "done")
			return Result{Done: true, Err: fmt.Errorf("%w: %s", ErrHandler, frame.Error)}, nil

		default:
			return Result{}, fmt.Errorf("dispatch: unexpected worker frame %q", frame.Type)
		}
	}
}

// cancel makes a best effort after the attempt loses its lease.
func (e wsExecutor) cancel(conn *websocket.Conn, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = wsjson.Write(ctx, conn, streamFrame{Type: frameCancel, Error: reason})
}

func websocketURL(address string) (string, error) {
	u, err := url.Parse(address)
	if err != nil {
		return "", fmt.Errorf("parsing worker address %q: %w", address, err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("worker address %q has unsupported scheme %q", address, u.Scheme)
	}
	u.Path = workerWebSocketEndpoint
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}
