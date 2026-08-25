# keel

Durable execution for Go. A workflow survives a crash, a restart, and a
deploy, because every step is journaled before it counts.

```
   client ──POST──► keeld ──POST──► your worker
                      │                 │
                      └──── S3 ◄────────┘
                   records, leases, journals
```

This repository is the engine only. `keeld` is one process that owns each
invocation's durable journal and calls your service over HTTP, so a
worker needs no queue and no database of its own. The SDK that makes
writing a workflow pleasant lives in a separate repository.

> Early work. The API and the on-disk layout still change, and one engine
> runs at a time. Read [the limits](docs/operating.md#limits-today)
> before you rely on it.

## Quick start

```sh
git clone https://github.com/mvaleed/keel && cd keel
go build ./cmd/keeld
./keeld -bucket my-keel-bucket        # uses the default AWS credentials
```

Announce a worker, and repeat the call every 10 seconds:

```sh
curl -X POST localhost:7070/v1/workers -d \
  '{"id":"w1","service":"demo","handlers":["Charge"],"address":"http://localhost:8081"}'
```

Submit an invocation and poll it:

```sh
curl -X POST localhost:7070/v1/invocations -d \
  '{"id":"order-1","service":"demo","handler":"Charge","input":{"amount":5}}'

curl localhost:7070/v1/invocations/demo/Charge/order-1
```

The caller picks the id, so sending that submission twice is not two
runs. See [the HTTP API](docs/http-api.md) for what a worker must serve.

## What it gives you

- **A step runs once, and its answer is durable.** A replay reads the
  journal instead of running the step again.
- **A crash costs one step, not a workflow.** Delivery to a handler is
  at-least-once, so a step must be idempotent.
- **A submission is idempotent.** The same id and the same input is the
  same invocation, whoever asks and however often.
- **Work is never lost while nothing runs it.** A dead engine returns its
  invocations the instant its lease becomes claimable.
- **The store is one seam.** S3 today, behind interfaces that name the
  rule and not the vendor.

## Vision

Keel aims to make a durable workflow as ordinary to write as a function.
You write plain Go, you mark the steps that must not repeat, and the
engine keeps the rest: the history, the retries, and the recovery.

The engine works today. The parts that make it pleasant do not exist yet.

**It does this now.** A client submits an invocation and the engine
records it before it answers. A dispatcher finds the work, leases it, and
calls a worker. Each step is journaled once, so a resumed invocation
replays instead of repeating. A dead engine gives its work back.

**Next.** A worker connection that streams each journal entry as it
happens, which removes the limit on how long one handler may run. A
durable sleep, so a workflow can wait for days without holding anything.
A signal, so a workflow can wait for the outside world. The SDK that
hides this protocol.

**Later.** More than one engine. A journal that compacts, so a replay
does not cost one read for each step. A worker that keeps the invocations
it already replayed.

This is a plan and not a promise. Nothing above is dated, and the order
may change.

## Documentation

[The design, the diagrams, and the flags](docs/) — or jump to
[what Keel stores](docs/storage.md), which everything else assumes.

## License

Apache 2.0. See [LICENSE](LICENSE).
