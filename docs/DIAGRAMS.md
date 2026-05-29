# vent, from several angles

A visual tour of the harness. Each diagram looks at the same system through a
different lens — the substrate, one turn over time, the state machine, the loop
seams, the human gate, streaming, tracing, deployment, and multiplayer. Read
top-to-bottom or jump to the angle you care about.

- [1. The substrate — one NATS primitive per harness concern](#1-the-substrate)
- [2. One turn, over time (sequence)](#2-one-turn-over-time)
- [3. The turn as a state machine](#3-the-turn-as-a-state-machine)
- [4. The loop's four seams](#4-the-loops-four-seams)
- [5. Human-in-the-loop, via KV watch](#5-human-in-the-loop-via-kv-watch)
- [6. Streaming tokens](#6-streaming-tokens)
- [7. One turn = one trace](#7-one-turn--one-trace)
- [8. Three deployment modes, same code](#8-three-deployment-modes-same-code)
- [9. Multiplayer](#9-multiplayer)

---

## 1. The substrate

The bet in one picture: most of "the engine" is just NATS features. Each harness
concern is a different way of *using* the embedded broker — not a service you
build.

```mermaid
flowchart LR
    subgraph NATS["embedded NATS + JetStream (in-process)"]
      direction TB
      RR[/"request / reply<br/>core subjects"/]
      ST[("stream<br/>AGENT_EVENTS")]
      WQ[("work queue<br/>TURN_STEPS")]
      KV[("KV buckets")]
      OS[("object store")]
    end

    RR --> F["function calls<br/>fn.provider.* · fn.policy.* · fn.tool.* · fn.auth.*"]
    ST --> EV["durable event log + live fanout<br/>evt.&lt;session&gt;"]
    WQ --> FSM["durable turn-step wakeups<br/>(survives restarts)"]
    KV --> S2["sessions · turn_state · approvals<br/>budgets · tools · skills"]
    OS --> BL["large blobs / artifacts"]
```

> The harness never imports a queue library, a pub/sub library, a KV client, or
> a blob client. It imports `pkg/bus`, which is a thin wrapper over these five.

---

## 2. One turn, over time

The same flow as the README's component diagram, but as a timeline — who calls
whom, in what order, for a turn that makes one tool call.

```mermaid
sequenceDiagram
    autonumber
    participant C as client / CLI
    participant O as orchestrator
    participant P as provider worker
    participant Pol as policy worker
    participant T as tool worker (fs/bash)
    participant E as evt.session stream

    C->>O: fn.harness.trigger (RunRequest)
    O->>O: fn.run.start — seed TurnState, ACK now
    Note over O: turn continues on a detached goroutine
    O->>P: fn.provider.*.stream (messages + tools)
    P-->>E: token deltas (message_update)
    P-->>O: AssistantMessage with a tool call
    O->>Pol: fn.policy.check_permissions
    Pol-->>O: allow
    O->>T: fn.tool.ls
    T-->>O: ToolResponse
    O->>P: fn.provider.*.stream (now with the tool result)
    P-->>E: token deltas
    P-->>O: AssistantMessage (final text)
    O-->>E: turn_end, agent_end
    E-->>C: live SSE the whole time
```

---

## 3. The turn as a state machine

The orchestrator owns a small FSM (`types.TurnState.Phase`) but executes none of
the work — each transition is driven by a worker's reply.

```mermaid
stateDiagram-v2
    [*] --> Provisioning
    Provisioning --> Assistant: model + system prompt + tools resolved
    Assistant --> Stopped: no tool calls (final answer)
    Assistant --> FunctionExecute: assistant returned tool call(s)
    FunctionExecute --> AwaitingApproval: policy = needs_approval
    AwaitingApproval --> FunctionExecute: resolved = allow
    AwaitingApproval --> Stopped: denied / timeout
    FunctionExecute --> Assistant: tool results fed back to model
    Assistant --> Failed: provider or loop error
    Stopped --> [*]
    Failed --> [*]
```

---

## 4. The loop's four seams

`pkg/agentloop` is transport-agnostic — it knows nothing about NATS. It exposes
four callbacks; the orchestrator wires each to a bus subject. That wiring *is*
the harness's policy/provider/tool/observability stack.

```mermaid
flowchart TB
    L["pkg/agentloop.Run<br/>(pure; no NATS)"]
    L --> SM["Stream"]
    L --> BF["Before"]
    L --> EX["Execute"]
    L --> AF["After"]

    SM -. "wired by orchestrator" .-> sP["fn.provider.&lt;name&gt;.stream"]
    BF -. "policy + approval gate" .-> sPol["fn.policy.check_permissions<br/>(+ approvals KV gate)"]
    EX -. "tool dispatch" .-> sT["fn.tool.&lt;name&gt;"]
    AF -. "hooks + budget" .-> sH["fn.hook.publish_collect<br/>+ fn.budget.record"]
```

> Swapping any layer = changing which worker answers that subject. The loop is
> untouched.

---

## 5. Human-in-the-loop, via KV watch

Approvals need no bespoke callback channel. The gate is a worker that writes a KV
key; the orchestrator watches the bucket. State change *is* the event.

```mermaid
sequenceDiagram
    autonumber
    participant O as orchestrator (Before gate)
    participant Pol as policy worker
    participant KV as approvals KV bucket
    participant A as approval worker
    participant H as human (console / Slack / …)

    O->>Pol: check_permissions(write/bash …)
    Pol-->>O: needs_approval
    O->>O: Phase = AwaitingApproval
    loop until decision or 5-min deadline
        O->>KV: get approvals/sid.cid
    end
    H->>A: fn.approval.resolve(allow | deny)
    A->>KV: put approvals/sid.cid = {decision}
    KV-->>O: decision present
    O->>O: dispatch tool (allow) or block (deny/timeout)
```

> Any surface can drive this — a console, a Slack slash-command worker, a CI bot —
> as long as it calls `fn.approval.resolve`. The orchestrator never knows which.

---

## 6. Streaming tokens

The provider streams deltas onto a per-message core subject the orchestrator
subscribes to, and returns the finalized message as the reply. Live tokens *and*
a clean result, with no extra plumbing.

```mermaid
flowchart LR
    O["orchestrator<br/>Stream seam"]
    P["provider worker"]
    SUB[/"core subject<br/>stream.&lt;sid&gt;.&lt;msg&gt;"/]
    EV[("evt.&lt;session&gt;")]
    UI["SSE clients"]

    O -- "fn.provider.*.stream (StreamSubject set)" --> P
    P -- "Delta: text / toolcall" --> SUB
    SUB -- "relayed as message_update" --> O
    O --> EV
    P -- "final AssistantMessage (the reply)" --> O
    EV --> UI
```

---

## 7. One turn = one trace

Trace context rides the NATS message headers, so a turn is one connected OTel
trace across every worker — with zero per-worker tracing code.

```mermaid
sequenceDiagram
    autonumber
    participant O as orchestrator
    participant B as bus.Trigger
    participant H as NATS headers
    participant R as bus.Register (any worker)

    O->>B: ctx = root span "turn" + vent.session.id baggage
    B->>H: inject traceparent + baggage
    H->>R: message delivered with headers
    R->>R: extract → start child span "handle (subject)"
    Note over O,R: same TraceID throughout → group-by-session for free
```

> `VENT_TRACE=stdout ./vent doctor` prints these spans; the stdout exporter is a
> one-line swap to OTLP.

---

## 8. Three deployment modes, same code

The worker code never changes across these. Only how the engine is started does.

```mermaid
flowchart TB
    subgraph M1["1 · in-process (default / dev)"]
      direction TB
      E1{{"embedded NATS<br/>DontListen — no socket"}}
      W1["all workers in one binary"]
      W1 --- E1
    end

    subgraph M2["2 · networked (multiplayer)"]
      direction TB
      E2{{"embedded NATS<br/>:4222"}}
      W2["in-binary workers"]
      X2["external worker<br/>(separate process / host)"]
      W2 --- E2
      X2 -. "nats://" .-> E2
    end

    subgraph M3["3 · clustered (HA / scale)"]
      direction LR
      A{{"node A"}}
      Bn{{"node B"}}
      Cn{{"node C"}}
      A --- Bn
      Bn --- Cn
      Cn --- A
    end

    M1 -->|"flip on a listener"| M2
    M2 -->|"add cluster routes"| M3
```

---

## 9. Multiplayer

One bus, many participants: workers that answer function subjects, and any
number of observers tailing the same event stream.

```mermaid
flowchart TB
    subgraph BUS["vent serve — NATS socket :4222"]
      direction TB
      CORE{{"the bus"}}
    end

    O["orchestrator"] --- CORE
    PV["providers / tools<br/>(in-binary)"] --- CORE
    EW["external worker<br/>Go · Python · Rust"] -- "registers fn.tool.echo" --- CORE

    CORE -. "evt.&lt;session&gt;" .-> U1["UI 1 (SSE)"]
    CORE -. "evt.&lt;session&gt;" .-> U2["UI 2 (SSE)"]
    CORE -. "evt.&lt;session&gt;" .-> REC["recorder / audit log"]
```

> Function calls are queue-group **load-balanced** across instances of a worker
> (scale by starting more); events are **broadcast** to every subscriber
> (multiplayer by default).
