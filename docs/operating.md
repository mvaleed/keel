# Operating keeld

## Run exactly one engine

Run one `keeld` against one bucket.

The worker registry is in memory. Two engines do not share it, so a
worker is visible only to the engine it announces itself to, and the
other engine cannot dispatch to it.

The consequences of one engine:

- The engine is a single point of failure for dispatch. No invocation
  starts or continues while the engine is down.
- Nothing durable is lost. The journal, the leases, and the invocation
  records are all in S3.
- A restart rebuilds the registry from the next heartbeat of each worker,
  which takes up to 10 seconds.
- An invocation that was running when the engine stopped continues after
  the lease expires, which is `-lease-ttl` and 2 minutes by default.

To submit an invocation you do not need a running worker. The engine
records it as pending, and runs it when a worker for the service starts.

## Flags

| flag | default | what it sets |
| --- | --- | --- |
| `-addr` | `:7070` | the address to listen on |
| `-bucket` | required | the S3 bucket that holds the invocations |
| `-prefix` | `keel` | the key prefix inside the bucket |
| `-owner` | hostname and pid | this engine's name in a lease |
| `-dispatch-interval` | `30s` | how long to wait between the scans |
| `-dispatch-concurrency` | `32` | how many invocations may be claimed at one time |
| `-execute-concurrency` | `1000` | how many invocations this engine may drive at one time |
| `-lease-ttl` | `2m` | how long a crashed engine keeps an invocation |

Two engines that share a bucket must not share an `-owner`, or one takes
the other's lease.

## Choosing the two concurrency bounds

They bound different things, and one number cannot serve both.

```
-dispatch-concurrency        -execute-concurrency
a claim: ~5 storage calls    a run: possibly days
bounded by the store         bounded by memory
right size: tens             right size: thousands
```

A dispatch takes its place in the execute pool before it claims, so a
full engine waits while it holds nothing.

## Choosing the lease ttl

The lease ttl is how long a dead engine keeps work away from a live one.

```
SHORTER                        LONGER
recovers sooner                fewer renewal writes
more renewal writes            recovers later
```

The driver renews every third of the ttl. At the 2 minute default and
1000 concurrent invocations, that is about 50 storage writes each second,
which is what argues against a much shorter ttl.

## Credentials

`keeld` loads the default AWS configuration, so it reads the usual
environment variables, the shared profile, or the instance role. It has
no flag for an endpoint or a key today, so an S3-compatible store that
needs a custom endpoint needs a code change.

## Limits today

- A replay costs one storage round trip for each journal step. At
  hundreds of steps this is a second or two. At hundreds of thousands it
  is the first wall an operator meets.
- The engine picks a worker in turn and keeps no affinity. A resumed
  invocation may reach a worker that must replay from step zero.
- The HTTP executor reports progress only when the worker replies, so a
  handler that runs longer than the lease ttl is stopped. A worker
  connection that streams its journal entries removes this limit.
- `GET /v1/invocations/...` reports the status and not the output.
- One engine. There is no leader election and no shared registry.
