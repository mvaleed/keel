# keel

Durable execution engine for building reliable, crash-proof workflows.

This repo is the engine only: `keeld`, a standalone process that owns
each invocation's durable journal and drives execution by calling out
to your service over HTTP (a push model, like Restate). The SDK that
makes writing services pleasant lives in a separate repo, `go-keel`.

## Workers

A worker is one process that runs your handlers. The engine holds no
worker address in its config. Each worker announces itself instead:

```
POST /v1/workers
{"id":"w1","service":"demo","handlers":["Charge"],"address":"http://10.0.0.4:8081"}
```

The engine answers with `heartbeat_seconds`. The worker must repeat the
same call more often than that, or the engine drops it after 30 seconds.
A worker that stops cleanly calls `DELETE /v1/workers/{id}`.

The worker supplies its own address. A process behind a mapped port or a
NAT cannot read the address that the engine must dial, so the SDK makes
you give it.

A `service` names a set of handlers. Many workers may serve one service,
and the engine shares the invocations between them. There is no load
balancer between the engine and a worker.

## Run one engine

Run exactly one `keeld` against one bucket.

The worker registry is in memory. Two engines do not share it, so a
worker is visible only to the engine it announces itself to, and the
other engine cannot dispatch to it.

The consequences of one engine:

- The engine is a single point of failure for dispatch. No invocation
  starts or continues while the engine is down.
- Nothing durable is lost. The journal, the leases, and the invocation
  records are all in S3.
- A restart rebuilds the registry from the next heartbeat of each
  worker, which takes up to 10 seconds.
- An invocation that was running when the engine stopped continues after
  the lease expires.

To submit an invocation you do not need a running worker. The engine
records it as pending, and runs it when a worker for the service starts.
