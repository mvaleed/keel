# The HTTP API

`keeld` serves six routes. Three are for a client, two are for a
worker, and one is for both.

## Submit an invocation

```
POST /v1/invocations
{"id":"order-1","service":"demo","handler":"Charge","input":{"amount":5}}
```

The caller supplies the id. This is what makes a retry safe: a repeat of
one call lands on the same address and is not a second run.

| code | meaning |
| --- | --- |
| 202 | recorded. A `Location` header names the address to poll. |
| 200 | the same id and the same input were already recorded. |
| 400 | the id, the service, or the handler cannot be stored, or the input is not JSON. |
| 409 | the id is taken by another input. Pick a new id. |

The engine answers before anything runs. A service with no live worker is
accepted, because a worker may start later.

Whitespace in the input does not count. The engine compacts the JSON
before it hashes it, so a reformatted retry is still a retry.

## Read an invocation

```
GET /v1/invocations/{service}/{handler}/{id}
{"id":"order-1","service":"demo","handler":"Charge","status":"succeeded","created_at":"..."}
```

`status` is one of `pending`, `running`, `succeeded`, `failed`, or
`cancelled`.

**This route does not return the output today.** The record holds the
output and the error, and this response does not carry either. A client
that needs the result must read the record from the store.

## List the invocations

```
GET /v1/invocations?service=demo&handler=Charge
[{"id":"order-1","service":"demo","handler":"Charge","status":"succeeded","created_at":"..."}]
```

The service and the handler parameters are optional, and either may be
left out to widen the list. The list is in key order and is not paged,
so a client that expects millions of invocations must narrow it.

## Cancel an invocation

```
DELETE /v1/invocations/{service}/{handler}/{id}
```

The answer is the record with `status` set to `cancelled`. The record
is written first, so the cancellation survives an engine crash, and the
attempt in flight stops after it. A worker that follows the protocol
stops when it receives the cancel frame.

Cancelling an invocation that already ended answers the record it
ended with, so a repeated call is safe. A cancelled invocation never
runs again.

## Register a worker

```
POST /v1/workers
{"id":"w1","service":"demo","handlers":["Charge"],"address":"http://10.0.0.4:8081"}
```

The answer is `{"heartbeat_seconds":10}`. The worker must repeat the same
call more often than that, or the engine drops it after 30 seconds. One
route serves the first announcement and every heartbeat, so a worker
recovers from an engine restart without special handling.

The worker supplies its own address. A process behind a mapped port or a
NAT cannot read the address that the engine must dial.

A `service` names a set of handlers. Many workers may serve one service,
and the engine shares the invocations between them in turn. There is no
load balancer between the engine and a worker.

## Deregister a worker

```
DELETE /v1/workers/{id}
```

The answer is 204. Dropping an unknown worker is not an error, because a
shutdown may repeat the call.

## What a worker must serve

The engine opens one WebSocket connection per attempt, at the address
the worker announced. The engine offers the subprotocol `keel.v1` in
the handshake, and a worker must accept it or refuse the connection.

The engine and the worker then trade JSON frames. The engine opens with
a `start` frame, which carries the invocation id, the handler, the
input, and the whole recorded journal. The worker replays the journal:
for each recorded step it returns the stored output instead of running
the step again. That is what makes a resumed invocation skip work it
already did.

Each frame the worker sends carries one journal entry, and the engine
appends the entry before it answers. This is what lets the engine renew
the lease on evidence, so a handler that runs for days keeps its lease.
The worker ends the attempt with a `succeeded` frame, which carries the
output, or a `failed` frame, which carries the error. A `failed` frame
ends the invocation as `failed`. The engine may send a `cancel` frame
when the invocation is cancelled or the lease is lost, and the worker
must stop.

An SDK writes this protocol for you. It lives in a separate
repository. [The worker protocol](worker-protocol.md) is the full
contract, and it is what an SDK implements.
