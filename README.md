# keel

Durable execution engine for building reliable, crash-proof workflows.

This repo is the engine only: `keeld`, a standalone process that owns
each invocation's durable journal and drives execution by calling out
to your service over HTTP (a push model, like Restate). The SDK that
makes writing services pleasant lives in a separate repo, `go-keel`.
