// Command keeld runs the Keel engine as a standalone process. It accepts
// invocations over HTTP and records them for a dispatcher to run.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/keel/keel/s3store"
)

func main() {
	addr := flag.String("addr", ":7070", "address to listen on")
	bucket := flag.String("bucket", "", "S3 bucket to store invocations in")
	prefix := flag.String("prefix", "keel", "key prefix inside the bucket")
	owner := flag.String("owner", "", "unique id for this engine in leases (default: hostname-pid)")
	services := flag.String("services", "", "comma-separated name=url pairs, e.g. demo=http://localhost:8081/invoke")
	flag.Parse()

	svcMap, err := parseServices(*services)
	if err != nil {
		log.Fatal(err)
	}
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

	srv := &server{records: store, services: svcMap}
	log.Printf("keeld listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, srv.routes()))
}

// parseServices reads the name=url pairs that name every service the
// engine may invoke.
func parseServices(s string) (map[string]string, error) {
	out := map[string]string{}
	if s == "" {
		return out, nil
	}
	for pair := range strings.SplitSeq(s, ",") {
		name, url, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("invalid -services entry %q, want name=url", pair)
		}
		out[name] = url
	}
	return out, nil
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
