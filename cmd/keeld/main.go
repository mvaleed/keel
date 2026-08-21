// Command keeld runs the Keel engine as a standalone process. It accepts
// invocations over HTTP and records them for a dispatcher to run.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/keel/keel/engine"
	"github.com/keel/keel/s3store"
	"github.com/keel/keel/worker"
)

func main() {
	addr := flag.String("addr", ":7070", "address to listen on")
	bucket := flag.String("bucket", "", "S3 bucket to store invocations in")
	prefix := flag.String("prefix", "keel", "key prefix inside the bucket")
	owner := flag.String("owner", "", "unique id for this engine in leases (default: hostname-pid)")
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
	e, err := engine.New(engine.Config{
		Records: store,
		Journal: store,
		Locker:  store,
		Workers: worker.NewMemory(),
		Owner:   *owner,
	})
	if err != nil {
		log.Fatal(err)
	}

	srv := &server{engine: e}
	log.Printf("keeld listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, srv.routes()))
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
