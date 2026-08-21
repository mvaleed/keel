package worker_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/keel/keel/worker"
)

func w(id, service string, handlers ...string) worker.Worker {
	return worker.Worker{
		ID: id, Service: service,
		Handlers: handlers, Address: "http://" + id + ".local:8080",
	}
}

func mustRegister(t *testing.T, r *worker.Memory, ws ...worker.Worker) {
	t.Helper()
	for _, each := range ws {
		if err := r.Register(each); err != nil {
			t.Fatalf("Register(%s): %v", each.ID, err)
		}
	}
}

func TestPickReturnsARegisteredWorker(t *testing.T) {
	t.Parallel()

	r := worker.NewMemory()
	mustRegister(t, r, w("a", "demo", "Charge"))

	got, err := r.Pick("demo", "Charge")
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.ID != "a" {
		t.Fatalf("ID = %q, want %q", got.ID, "a")
	}
}

func TestPickNeedsALiveWorker(t *testing.T) {
	t.Parallel()

	r := worker.NewMemory()
	mustRegister(t, r, w("a", "demo", "Charge"))

	tests := map[string]struct{ service, handler string }{
		"unknown service": {"billing", "Charge"},
		"unknown handler": {"demo", "Refund"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := r.Pick(tt.service, tt.handler); !errors.Is(err, worker.ErrNoWorker) {
				t.Fatalf("err = %v, want %v", err, worker.ErrNoWorker)
			}
		})
	}
}

func TestRegisterKeepsOneEntryPerID(t *testing.T) {
	t.Parallel()

	// A heartbeat repeats the announcement. It must not add a second
	// worker, or one process takes two shares of the load.
	r := worker.NewMemory()
	first := w("a", "demo", "Charge")
	moved := first
	moved.Address = "http://moved.local:9090"
	mustRegister(t, r, first, moved)

	got, err := r.Pick("demo", "Charge")
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.Address != moved.Address {
		t.Fatalf("address = %q, want the newest %q", got.Address, moved.Address)
	}
}

func TestPickTakesTheReplicasInTurn(t *testing.T) {
	t.Parallel()

	r := worker.NewMemory()
	mustRegister(t, r, w("a", "demo", "Charge"), w("b", "demo", "Charge"), w("c", "demo", "Charge"))

	var got []string
	for range 6 {
		picked, err := r.Pick("demo", "Charge")
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		got = append(got, picked.ID)
	}

	want := []string{"a", "b", "c", "a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("picks = %v, want %v", got, want)
		}
	}
}

func TestPickSkipsAWorkerThatStoppedBeating(t *testing.T) {
	t.Parallel()

	r := worker.NewMemory()
	clock := newClock(r)
	mustRegister(t, r, w("a", "demo", "Charge"))

	clock.advance(worker.TTL)
	if _, err := r.Pick("demo", "Charge"); !errors.Is(err, worker.ErrNoWorker) {
		t.Fatalf("err = %v, want %v", err, worker.ErrNoWorker)
	}
}

func TestRegisterKeepsAWorkerLive(t *testing.T) {
	t.Parallel()

	r := worker.NewMemory()
	clock := newClock(r)
	mustRegister(t, r, w("a", "demo", "Charge"))

	// One beat inside the TTL must extend the worker past it.
	clock.advance(worker.Heartbeat)
	mustRegister(t, r, w("a", "demo", "Charge"))
	clock.advance(worker.TTL - time.Second)

	if _, err := r.Pick("demo", "Charge"); err != nil {
		t.Fatalf("Pick: %v", err)
	}
}

func TestRegisterDropsTheExpiredWorkers(t *testing.T) {
	t.Parallel()

	r := worker.NewMemory()
	clock := newClock(r)
	mustRegister(t, r, w("gone", "demo", "Charge"))

	clock.advance(worker.TTL)
	mustRegister(t, r, w("here", "demo", "Charge"))

	// Register drops the expired workers, so the registry holds no dead one.
	if got := worker.Size(r); got != 1 {
		t.Fatalf("size = %d, want 1", got)
	}
}

func TestDeregisterRemovesTheWorkerAtOnce(t *testing.T) {
	t.Parallel()

	r := worker.NewMemory()
	mustRegister(t, r, w("a", "demo", "Charge"))

	if err := r.Deregister("a"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if _, err := r.Pick("demo", "Charge"); !errors.Is(err, worker.ErrNoWorker) {
		t.Fatalf("err = %v, want %v", err, worker.ErrNoWorker)
	}
}

func TestDeregisterAnUnknownWorkerIsNotAnError(t *testing.T) {
	t.Parallel()

	// A shutdown may repeat the call, and a restarted engine never held
	// the worker at all.
	if err := worker.NewMemory().Deregister("never-registered"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
}

func TestRegisterRejectsAnAnnouncementItCannotUse(t *testing.T) {
	t.Parallel()

	good := w("a", "demo", "Charge")
	tests := map[string]func(*worker.Worker){
		"no id":          func(x *worker.Worker) { x.ID = "" },
		"no service":     func(x *worker.Worker) { x.Service = "" },
		"no handlers":    func(x *worker.Worker) { x.Handlers = nil },
		"traversing id":  func(x *worker.Worker) { x.ID = "../../x" },
		"separator":      func(x *worker.Worker) { x.Service = "a/b" },
		"bad handler":    func(x *worker.Worker) { x.Handlers = []string{"a/b"} },
		"no address":     func(x *worker.Worker) { x.Address = "" },
		"no scheme":      func(x *worker.Worker) { x.Address = "localhost:8080" },
		"unknown scheme": func(x *worker.Worker) { x.Address = "tcp://localhost:8080" },
		"no host":        func(x *worker.Worker) { x.Address = "http://" },
		"broken address": func(x *worker.Worker) { x.Address = "http://a b" },
	}
	for name, break_ := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bad := good
			break_(&bad)
			if err := worker.NewMemory().Register(bad); !errors.Is(err, worker.ErrInvalid) {
				t.Fatalf("err = %v, want %v", err, worker.ErrInvalid)
			}
		})
	}
}

func TestConcurrentUseIsSafe(t *testing.T) {
	t.Parallel()

	r := worker.NewMemory()
	mustRegister(t, r, w("a", "demo", "Charge"))

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if i%2 == 0 {
					_ = r.Register(w("a", "demo", "Charge"))
					continue
				}
				_, _ = r.Pick("demo", "Charge")
			}
		}()
	}
	wg.Wait()
}
