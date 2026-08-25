# How an invocation runs

A submission records the invocation and answers. Nothing has run yet.
The dispatcher is what turns a record into a run.

## Finding the work

A scan lists `keel/due/` and stops at the first marker that is not due
yet. The cost of a scan follows the work that is ready, and not the size
of the backlog.

The dispatcher scans every 30 seconds. A submission also hands its marker
over at once, so a normal invocation starts in milliseconds. The handoff
is latency only, because the next scan finds the same marker.

## One attempt, in order

The order of the writes is the design.

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

## The rules behind that order

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

## What a crash costs

- **Between the journal append and the record write.** One invocation
  runs one extra time. The replay is deterministic and the journal is
  fenced by a conditional write, so the answer is the same. Keel gives
  at-least-once delivery to a handler, and exactly-once journal entries.
- **Between the record write and the marker write in a submission.** No
  scan finds the invocation. The submission is safe to repeat, and a
  repeat writes the marker.
- **Between the two writes that move a marker.** The invocation has two
  markers. The extra one costs one read and then goes at step 1.

## See also

- [The states of one invocation](invocation-states.md).
- [A long run, second by second](long-run-timeline.md), which shows the
  renewals, a hung worker, and a dead engine.
- [What Keel stores](storage.md).
