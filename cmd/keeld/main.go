// Command keeld runs the Keel engine as a standalone process. It accepts
// invocations over HTTP, and runs the ones that are due.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/keel/keel/dispatch"
	"github.com/keel/keel/engine"
	"github.com/keel/keel/s3store"
	"github.com/keel/keel/worker"
)

func main() {
	addr := flag.String("addr", ":7070", "address to listen on")
	bucket := flag.String("bucket", "", "S3 bucket to store invocations in")
	prefix := flag.String("prefix", "keel", "key prefix inside the bucket")
	owner := flag.String("owner", "", "unique id for this engine in leases (default: hostname-pid)")
	interval := flag.Duration("dispatch-interval", 30*time.Second, "how long the dispatcher waits between scans")
	dispatches := flag.Int("dispatch-concurrency", 32, "how many invocations may be claimed at one time")
	executions := flag.Int("execute-concurrency", 1000, "how many invocations this engine may drive at one time")
	leaseTTL := flag.Duration("lease-ttl", 2*time.Minute, "how long a crashed engine keeps an invocation")
	flag.Parse()

	var err error
	if *bucket == "" {
		log.Fatal("-bucket is required")
	}
	if *owner == "" {
		if *owner, err = defaultOwner(); err != nil {
			log.Fatalf("determining owner: %v", err)
		}
	}

	store, err := s3store.NewFromEnv(*bucket, *prefix)
	if err != nil {
		log.Fatalf("opening store: %v", err)
	}

	// One backend satisfies every durable store. This is the only place
	// that knows which backend it is. The worker registry is not
	// durable, because a lost registry costs one heartbeat and no work.
	registry := worker.NewMemory()

	// The dispatcher comes first, because a submission hands it the new
	// marker and so the engine needs it.
	d, err := dispatch.New(dispatch.Config{
		Records:             store,
		DueIndex:            store,
		Locker:              store,
		Workers:             registry,
		Executor:            dispatch.NewHTTPExecutor(store),
		Owner:               *owner,
		DispatchConcurrency: *dispatches,
		ExecuteConcurrency:  *executions,
		LeaseTTL:            *leaseTTL,
		Interval:            *interval,
	})
	if err != nil {
		log.Fatal(err)
	}

	e, err := engine.New(engine.Config{
		Records:    store,
		Workers:    registry,
		Dispatcher: d,
	})
	if err != nil {
		log.Fatal(err)
	}

	// One signal stops both parts. The dispatcher returns only after
	// every attempt in flight releases its lease.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dispatched := make(chan struct{})
	go func() {
		defer close(dispatched)
		if err := d.Run(ctx); err != nil {
			log.Printf("dispatcher: %v", err)
		}
	}()

	srv := &http.Server{Addr: *addr, Handler: (&server{engine: e}).routes()}
	go func() {
		<-ctx.Done()
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Printf("shutting down: %v", err)
		}
	}()

	log.Printf("keeld listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	<-dispatched
}

// defaultOwner names this process. Two engines that share a bucket must
// not share an owner, or one takes the other's lease.
func defaultOwner() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid()), nil
}
