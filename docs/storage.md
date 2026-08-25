# What Keel stores

One invocation has four durable parts. Each one has a different job, and
a reader who mixes them up cannot follow the rest of the design.

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

## Why the store must be conditional

Keel needs one primitive from its backend: an atomic conditional write.

- A record is created with "write only if absent", so two submissions of
  one id cannot both succeed.
- A journal entry is written the same way, so one step gets one entry
  even when two writers race.
- A lease is taken with a compare and set, so two live leases for one
  invocation cannot exist.

A backend with no such primitive cannot hold these rules. S3 is the one
backend today, and every store sits behind an interface that names the
rule and not the vendor.

## See also

- [The states of one invocation](invocation-states.md), which shows all
  four parts in each state.
- [How an invocation runs](dispatch.md).
