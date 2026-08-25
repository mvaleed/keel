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
  the lease expires, which is `-lease-ttl` and 2 minutes by default.

To submit an invocation you do not need a running worker. The engine
records it as pending, and runs it when a worker for the service starts.

## What Keel stores

One invocation has four durable parts. Each one has a different job, and
a reader who mixes them up cannot follow the rest of this file.

```
keel/invocations/<service>/<handler>/<id>/
    invocation.json                        the record
    lease.json                             the lease
    entries/00000000000000000000.json      the journal
keel/due/<padded-due-seconds>/<service>/<handler>/<id>    the wakeup marker
```

**The record** is the authority. It says the invocation exists, what its
input was, which stage it is at, and what it produced. Every other part
may be stale or missing; the record decides.

**The wakeup marker** says an invocation must be looked at again at a
time. It is an empty object, because the two facts it carries are in the
name: the due time first, then the invocation. A `LIST` returns keys and
not bodies, so a scan reads the whole to-do list in one request. The due
time is padded to 20 digits, so alphabetical order is time order.

**The lease** says who may act on the invocation now. It grants exclusion
between two engines, and between one engine and its own frozen self after
a pause or a restart. It carries a ttl, and the holder must renew it.

**The epoch** is not a fifth object. It is a number inside the lease that
only ever grows, and each holder writes its epoch into the record and
into every journal entry. A write that carries an older epoch comes from
a holder that lost the lease, so the write is rejected. The lease keeps
two writers apart; the epoch catches the one that did not notice it lost.

**The journal** is the append-only log of the steps the handler ran. It
is what a replay reads, so a resumed invocation does not repeat work.

Two diagrams show these parts in motion:
[the states of one invocation](docs/invocation-states.md), and
[a long run, second by second](docs/long-run-timeline.md).

## How an invocation runs

A scan lists `keel/due/` and stops at the first marker that is not due
yet. The cost of a scan follows the work that is ready, and not the size
of the backlog.

The dispatcher scans every 30 seconds. A submission also hands its key
over at once, so a normal invocation starts in milliseconds. The handoff
is latency only, because the next scan finds the same marker.

One attempt does this, and the order is the design:

1. Read the record. A terminal record means the work is done, so drop the
   marker and stop.
2. Take a place in the execute pool. It waits here while the engine is
   full, and it holds nothing while it waits.
3. Pick a worker. No live worker is not a failure. The marker moves to
   the next scan, and the invocation stays pending for as long as it must.
4. Claim the lease.
5. Move the marker to the lease expiry.
6. Record the run, then hand the invocation to the driver.
7. The driver calls the worker, holds the lease, and writes the outcome.
8. Write the terminal record, then drop the marker.
9. Release the lease.

**Two pools, because the two costs differ.** A claim is about five
storage calls and it takes milliseconds, so `-dispatch-concurrency`
follows the throughput of the store. A run may take days and is mostly
idle, so `-execute-concurrency` follows memory. The place in the execute
pool is taken before the claim: a full engine must wait while it holds
nothing, because the other order marks invocations running that nobody
drives.

**The lease is renewed on evidence.** A journal entry proves that the
invocation advanced. A timer proves only that this engine is optimistic.
So the driver renews the lease while the entries arrive, and it moves the
marker with each renewal. An attempt that reports nothing for a whole
lease ttl is stopped, and its work returns to the scan.

**The lease expiry and the marker due time are the same event.** An
engine that dies mid-invocation returns its work to the scan at the
instant its lease becomes claimable. There is no separate recovery pass.

**The record is the authority and the marker is a hint.** A marker may
survive a finished invocation, and step 1 drops it. A marker must never
go missing for an unfinished one, which is why the record becomes
terminal before its marker goes, and a new marker is written before an
old one goes.

**A worker that fails or drops the connection is not a failure of the
invocation.** The attempt is rescheduled. An attempt that advanced starts
again at once, because it was alive. An attempt that advanced nothing
raises the failure count of the record, and the backoff grows from 1
second to 5 minutes. It never gives up. Only a handler that returns an
error ends the invocation as `failed`.

### What a crash costs

- **Between the journal append and the record write.** One invocation
  runs one extra time. The replay is deterministic and the journal is
  fenced by a conditional write, so the answer is the same. Keel gives
  at-least-once delivery to a handler, and exactly-once journal entries.
- **Between the record write and the marker write in a submission.** No
  scan finds the invocation. The submission is safe to repeat, and a
  repeat writes the marker.
- **Between the two writes that move a marker.** The invocation has two
  markers. The extra one costs one read and then goes at step 1.

### Limits today

- A replay costs one storage round trip for each journal step. At
  hundreds of steps this is a second or two. At hundreds of thousands it
  is the first wall an operator meets.
- The engine picks a worker in turn and keeps no affinity. A resumed
  invocation may reach a worker that must replay from step zero.
- The HTTP executor reports progress only when the worker replies, so a
  handler that runs longer than the lease ttl is stopped. A worker
  connection that streams its journal entries removes this limit.
