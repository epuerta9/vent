# vent worker contract (read before writing any worker)

`vent` is a composable agent harness. Every "harness job" (the loop, provider
routing, credentials, policy, approvals, budget, skills, events, …) is an
independent **worker**: a process that connects to the embedded NATS engine and
registers function subjects and/or subscribes to event streams. Workers are
decoupled — they only agree on the JSON shapes in `pkg/types` travelling over
the subjects in `pkg/bus/subjects.go`.

## The two packages every worker imports

- `github.com/epuerta/vent/pkg/types` — all wire shapes (Message, Event,
  ToolSpec, RunRequest, TurnState, PolicyRequest, etc). **Do not add fields**;
  use what's there.
- `github.com/epuerta/vent/pkg/bus` — the primitive. Key methods:
  - `b.Register(subject string, fn bus.HandlerFunc) (bus.Unsub, error)` — become
    the implementation of a function subject. `HandlerFunc` is
    `func(ctx context.Context, data []byte) (any, error)`; unmarshal `data` into
    your request type, return the response value (it is JSON-marshalled for you).
  - `b.Trigger(ctx, subject, req, &out) error` — call another worker.
  - `b.PublishEvent(ctx, types.Event) error` — emit an agent event.
  - `b.SubscribeEvents(sessionID string, func(types.Event)) (bus.Unsub, error)` —
    `sessionID==""` subscribes to all sessions.
  - `b.GetJSON(ctx, bucket, key, &out) (found bool, err error)` /
    `b.PutJSON(ctx, bucket, key, val) error` — KV state.
  - `b.Blobs(ctx) (jetstream.ObjectStore, error)` — large payloads.
  - `b.Conn()` — raw `*nats.Conn` (for live core subscriptions, e.g. streaming deltas).
  - Subject constants & helpers live in `pkg/bus`: `bus.SubjRunStart`,
    `bus.ProviderStreamSubject("anthropic")`, `bus.ToolSubject("bash")`,
    `bus.BucketTools`, etc.

## Required shape of every worker package

Each worker lives in its own directory `workers/<name>/` as package `<name>`
and exposes exactly this entrypoint:

```go
// Start registers the worker's subjects on the bus. It is non-blocking:
// registrations live for the lifetime of the connection. Return an error only
// if registration itself fails.
func Start(ctx context.Context, b *bus.Bus) error
```

The composite binary (`cmd/vent`) calls every worker's `Start`. Do not block,
do not call `os.Exit`, do not start the engine — `cmd/vent` owns that.

## Compile rules

- Module is fully tidied. **Do not run `go mod tidy` and do not add third-party
  dependencies.** Standard library only (the Anthropic provider uses `net/http`).
- Your package must compile with `go build ./workers/<name>/...`.
- Match the surrounding Go style: small files, clear names, doc comment on `Start`.
