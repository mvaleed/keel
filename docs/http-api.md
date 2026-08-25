# The HTTP API

`keeld` serves four routes. Two are for a client, and two are for a
worker.

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

`status` is one of `pending`, `running`, `succeeded`, or `failed`.

**This route does not return the output today.** The record holds the
output and the error, and this response does not carry either. A client
that needs the result must read the record from the store.

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

The engine calls one route on the worker, at the address the worker
announced:

```
POST <address>/keel/v1/invoke
{"invocation_id":"order-1","handler":"Charge","input":{...},"journal":[...]}
```

`journal` is the whole recorded history. The worker replays it: for each
recorded step it returns the stored output instead of running the step
again. That is what makes a resumed invocation skip work it already did.

The reply carries the steps it ran this time:

```
{"output":{...},"error":"","new_entries":[{"step":0,"name":"charge","output":{...}}]}
```

A non-empty `error` ends the invocation as `failed`. The engine appends
every entry in `new_entries` before it reports the outcome.

An SDK writes this protocol for you. It lives in a separate repository.
