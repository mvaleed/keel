# A long run, second by second

A durable function may run for hours. The lease that protects it lasts
two minutes. This page shows how the two fit together, and what happens
when they stop fitting.

Three numbers drive everything here, and all three are defaults:

| | value | flag |
| --- | --- | --- |
| lease ttl | 2 minutes | `-lease-ttl` |
| renew every | ttl / 3, so 40 seconds | follows the ttl |
| stall limit | one whole ttl of silence | follows the ttl |

## The healthy case

The renewer holds the lease on evidence. Every journal entry the
worker produces calls `Progress`, and that is what earns the next 2
minutes. A timer alone would prove nothing.

Watch the lease expiry and the marker due time: they are the same
instant, and they move together.

```mermaid
sequenceDiagram
    autonumber
    participant D as driver
    participant R as renewer
    participant W as worker
    participant S as S3

    D->>S: Claim lease, ttl 2m
    Note over D,S: lease expires 16:42<br/>marker due 16:42
    D->>W: invoke, with the journal so far
    D->>R: start

    loop every 40 seconds
        W-->>D: journal entry
        D->>S: Append entry, fenced by the epoch
        D->>D: Progress
        R->>R: entries since the last check?
        R->>S: Renew lease, ttl 2m
        R->>S: PUT new marker, then DELETE old
        Note over R,S: lease expires 16:43<br/>marker due 16:43
    end

    W-->>D: the handler returned
    D->>R: stop
    D->>S: write the terminal record
    D->>S: delete the marker
    D->>S: release the lease
```

The order at the end is fixed. Terminal record first, marker second,
lease last. Deleting the marker before the record is terminal would lose
the invocation for good.

## When the worker stops answering

Now the worker hangs. No journal entry arrives, so nothing calls
`Progress`. The renewer notices after one whole ttl of silence.

```mermaid
sequenceDiagram
    autonumber
    participant D as driver
    participant R as renewer
    participant W as worker
    participant S as S3

    D->>S: Claim lease, ttl 2m
    D->>W: invoke
    D->>R: start
    Note over W: the worker hangs.<br/>no entries, no Progress

    R->>R: 40s: silent for 40s, still under the ttl
    R->>S: Renew lease
    R->>R: 80s: silent for 80s, still under the ttl
    R->>S: Renew lease
    R->>R: 120s: silent for a whole ttl

    R-->>D: cancel the attempt
    D->>W: the request context is cancelled
    D->>S: Failures + 1, marker moves to now + backoff
    D->>S: release the lease
    Note over D,S: the work is claimable again<br/>about one second later
```

This is the reason the renewer reads evidence and not a clock. A
timer-based keepalive would renew this lease forever, and no other
engine could ever rescue the invocation.

## When the engine itself dies

Nothing runs the cleanup above, because there is no engine left to run
it. Recovery falls back to the one guarantee that needs no live process:

```mermaid
sequenceDiagram
    autonumber
    participant E1 as engine (dying)
    participant S as S3
    participant E2 as the next scan

    E1->>S: Claim lease, ttl 2m
    Note over E1,S: lease expires 16:42<br/>marker due 16:42
    E1->>S: Renew at 16:40:40
    Note over E1,S: lease expires 16:42:40<br/>marker due 16:42:40
    Note over E1: the process is killed
    Note over S: nothing is released.<br/>the lease and the marker<br/>both say 16:42:40

    E2->>S: at 16:42:40 the marker comes due
    E2->>S: read the record: still running
    E2->>S: Claim the lease, which has just expired
    Note over E2,S: epoch is now N + 1.<br/>any write from the dead<br/>holder is refused
    E2->>S: the invocation continues from its journal
```

**The lease expiry and the marker due time are the same event.** That
single equality is what makes crash recovery need no recovery process, no
repair job, and no extra state. Work returns to the queue at the exact
moment it becomes legal to take it.

The cost of a crash is therefore bounded by `-lease-ttl`. A shorter ttl
recovers sooner and writes more renewals. Two minutes at 1000 concurrent
invocations is already about 50 storage writes per second, which is what
argues against going much shorter.

## The limit that is still open

`httpExecutor` only calls `Progress` when the worker replies, at the very
end of the call. So a handler that runs longer than the lease ttl over
plain HTTP is treated as a stall and cancelled.

That is correct for a streaming transport and premature for a
request and response one. The bidirectional stream, which reports each
journal entry as it happens, is what closes it.
