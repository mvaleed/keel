package dispatch_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/keel/keel/dispatch"
	"github.com/keel/keel/journal"
)

type testStreamFrame struct {
	Type         string          `json:"type"`
	InvocationID string          `json:"invocation_id,omitempty"`
	Handler      string          `json:"handler,omitempty"`
	Journal      []journal.Entry `json:"journal,omitempty"`
	Entry        *journal.Entry  `json:"entry,omitempty"`
	Step         *int            `json:"step,omitempty"`
	Output       json.RawMessage `json:"output,omitempty"`
}

func TestWSExecutorAcknowledgesEachDurableEntry(t *testing.T) {
	t.Parallel()

	observed := make(chan []testStreamFrame, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{"keel.v1"},
		})
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		defer conn.CloseNow()

		var frames []testStreamFrame
		var start testStreamFrame
		if err := wsjson.Read(r.Context(), conn, &start); err != nil {
			t.Errorf("read start: %v", err)
			return
		}
		frames = append(frames, start)

		entry := journal.Entry{Step: 0, Name: "charge", Output: json.RawMessage(`{"id":"ch_1"}`)}
		if err := wsjson.Write(r.Context(), conn, testStreamFrame{Type: "entry", Entry: &entry}); err != nil {
			t.Errorf("write entry: %v", err)
			return
		}
		var accepted testStreamFrame
		if err := wsjson.Read(r.Context(), conn, &accepted); err != nil {
			t.Errorf("read accepted: %v", err)
			return
		}
		frames = append(frames, accepted)

		if err := wsjson.Write(r.Context(), conn, testStreamFrame{
			Type: "succeeded", Output: json.RawMessage(`{"ok":true}`),
		}); err != nil {
			t.Errorf("write success: %v", err)
			return
		}
		observed <- frames
	}))
	t.Cleanup(srv.Close)

	store := newStore()
	store.history = []journal.Entry{{Step: 0, Name: "replayed"}}
	res, err := dispatch.NewWSExecutor(store).Execute(t.Context(), attempt("ws-1", srv.URL))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.Done || string(res.Output) != `{"ok":true}` {
		t.Fatalf("result = %+v, want success", res)
	}
	if writes := store.writes(); len(writes) != 1 || writes[0].Name != "charge" {
		t.Fatalf("writes = %+v, want charge", writes)
	}

	frames := <-observed
	if frames[0].Type != "start" || frames[0].InvocationID != "ws-1" || frames[0].Handler != "Charge" {
		t.Fatalf("start = %+v, want invocation details", frames[0])
	}
	if len(frames[0].Journal) != 1 || frames[0].Journal[0].Name != "replayed" {
		t.Fatalf("journal = %+v, want replayed history", frames[0].Journal)
	}
	if frames[1].Type != "accepted" || frames[1].Step == nil || *frames[1].Step != 0 {
		t.Fatalf("acceptance = %+v, want step 0", frames[1])
	}
}

func TestWSExecutorTreatsCloseAsUnfinished(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{"keel.v1"},
		})
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		defer conn.CloseNow()
		var start testStreamFrame
		if err := wsjson.Read(r.Context(), conn, &start); err != nil {
			t.Errorf("read start: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "closed without a result")
	}))
	t.Cleanup(srv.Close)

	res, err := dispatch.NewWSExecutor(newStore()).Execute(t.Context(), attempt("ws-2", srv.URL))
	if err == nil {
		t.Fatal("Execute accepted a close without a terminal frame")
	}
	if res.Done {
		t.Fatalf("result = %+v, want unfinished", res)
	}
}

func TestWSExecutorRefusesAWorkerWithoutTheSubprotocol(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Sec-WebSocket-Protocol") != "keel.v1" {
			t.Errorf("the engine offered %q", r.Header.Get("Sec-WebSocket-Protocol"))
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		defer conn.CloseNow()
	}))
	t.Cleanup(srv.Close)

	if _, err := dispatch.NewWSExecutor(newStore()).Execute(t.Context(), attempt("ws-3", srv.URL)); err == nil {
		t.Fatal("Execute accepted a worker that echoed no subprotocol")
	}
}

// TestWorkerProtocolFrameJSON pins the exact bytes of every frame. An
// SDK in another language reads docs/worker-protocol.md, and this test
// is what keeps the page and the wire in agreement.
func TestWorkerProtocolFrameJSON(t *testing.T) {
	t.Parallel()

	observed := make(chan []string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{"keel.v1"},
		})
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		defer conn.CloseNow()

		var raw []string
		kind, start, err := conn.Read(r.Context())
		if err != nil {
			t.Errorf("read start: %v", err)
			return
		}
		raw = append(raw, string(start))

		if err := conn.Write(r.Context(), websocket.MessageText, []byte(
			`{"type":"entry","entry":{"step":0,"name":"charge","output":{"id":"ch_1"}}}`)); err != nil {
			t.Errorf("write entry: %v", err)
			return
		}
		_, accepted, err := conn.Read(r.Context())
		if err != nil {
			t.Errorf("read accepted: %v", err)
			return
		}
		raw = append(raw, string(accepted))

		if err := conn.Write(r.Context(), websocket.MessageText, []byte(
			`{"type":"succeeded","output":{"ok":true}}`)); err != nil {
			t.Errorf("write success: %v", err)
			return
		}
		if kind != websocket.MessageText {
			t.Errorf("frame kind = %v, want text", kind)
		}
		observed <- raw
	}))
	t.Cleanup(srv.Close)

	res, err := dispatch.NewWSExecutor(newStore()).Execute(t.Context(), attempt("ws-4", srv.URL))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.Done {
		t.Fatalf("result = %+v, want success", res)
	}

	raw := <-observed
	// wsjson frames carry one trailing newline. A receiver must accept
	// one, so the golden bytes keep it out of the comparison.
	for i := range raw {
		raw[i] = strings.TrimRight(raw[i], "\n")
	}
	if raw[0] != `{"type":"start","invocation_id":"ws-4","handler":"Charge","input":{},"journal":[]}` {
		t.Fatalf("start = %s", raw[0])
	}
	if raw[1] != `{"type":"accepted","step":0}` {
		t.Fatalf("accepted = %s", raw[1])
	}
}
