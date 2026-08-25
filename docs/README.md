# Keel documentation

Start with [what Keel stores](storage.md). Every other page assumes it.

## The design

- [What Keel stores](storage.md) — the record, the wakeup marker, the
  lease, the epoch, and the journal.
- [How an invocation runs](dispatch.md) — the scan, the order of one
  attempt, and what a crash costs.

## The diagrams

- [The states of one invocation](invocation-states.md) — each state with
  its durable parts, and where a crash leads.
- [A long run, second by second](long-run-timeline.md) — the renewals, a
  hung worker, and a dead engine.

## Using it

- [The HTTP API](http-api.md) — the four routes, and the protocol a
  worker must serve.
- [Operating keeld](operating.md) — the flags, the bounds, and the
  limits today.
