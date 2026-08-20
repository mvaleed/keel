// Command keeld runs the Keel engine as a standalone process, reachable
// over HTTP by clients and by the services it invokes.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/keel/keel/engine"
	"github.com/keel/keel/journal/s3journal"
)

func main() {
	addr := flag.String("addr", ":7070", "address to listen on")
	bucket := flag.String("bucket", "", "S3 bucket to store journals in")
	prefix := flag.String("prefix", "keel", "key prefix inside the bucket")
	owner := flag.String("owner", "", "unique id for this engine in leases (default: hostname-pid)")
	services := flag.String("services", "", "comma-separated name=url pairs, e.g. demo=http://localhost:8081/invoke")
	flag.Parse()

	svcMap := map[string]string{}
	if *services != "" {
		for pair := range strings.SplitSeq(*services, ",") {
			name, url, ok := strings.Cut(pair, "=")
			if !ok {
				log.Fatalf("invalid -services entry %q, want name=url", pair)
			}
			svcMap[name] = url
		}
	}

	if *owner == "" {
		host, err := os.Hostname()
		if err != nil {
			log.Fatalf("determining owner: %v", err)
		}
		*owner = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	if *bucket == "" {
		log.Fatal("-bucket is required")
	}
	store, err := s3journal.NewS3Journal(*bucket, *prefix)
	if err != nil {
		log.Fatalf("opening journal store: %v", err)
	}
	// One backend gives both the log and the leases that fence it.
	e := engine.New(store, store, *owner, svcMap)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /invoke/{service}/{id}", func(w http.ResponseWriter, r *http.Request) {
		var input json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		out, err := e.Invoke(r.Context(), r.PathValue("service"), r.PathValue("id"), input)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(out)
	})

	log.Printf("keeld listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
