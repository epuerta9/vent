// Package bus is the single primitive the whole vent harness is built on.
//
// In iii terms this is iii.trigger(): a worker connects to the engine, then
// Register()s function subjects (becoming an implementation) and/or
// Subscribe()s to event streams. Calling another worker is Trigger(). State
// lives in KV buckets; large payloads live in the object store. The harness
// is whatever set of workers you connect — nothing else couples them.
package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/epuerta/vent/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracer is the bus tracer. Spans are non-recording unless an exporter is
// installed via pkg/obs, so this is free on the default path.
var tracer = otel.Tracer("vent/bus")

// headerCarrier adapts nats.Header to the OTel TextMapCarrier interface so the
// globally-installed propagator can inject/extract trace context over the bus.
type headerCarrier nats.Header

func (h headerCarrier) Get(key string) string  { return nats.Header(h).Get(key) }
func (h headerCarrier) Set(key, val string)    { nats.Header(h).Set(key, val) }
func (h headerCarrier) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}

// Bus is a worker's handle to the engine.
type Bus struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// Connect wraps an existing NATS connection (typically the in-process one from
// the embedded engine) into a Bus.
func Connect(nc *nats.Conn) (*Bus, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	return &Bus{nc: nc, js: js}, nil
}

// Conn exposes the raw NATS connection for advanced subscriptions (e.g. live
// streaming deltas on a core subject).
func (b *Bus) Conn() *nats.Conn { return b.nc }

// JS exposes the JetStream context.
func (b *Bus) JS() jetstream.JetStream { return b.js }

// Unsub cancels a subscription.
type Unsub func()

// HandlerFunc implements a registered function. It receives the raw request
// bytes and returns the response value (JSON-encoded by Register) or an error.
type HandlerFunc func(ctx context.Context, data []byte) (any, error)

// Register makes this worker the implementation of a function subject. Multiple
// instances that Register the same subject form a queue group and load-balance;
// to *swap* an implementation, stop the old worker and start a new one that
// Registers the same subject.
func (b *Bus) Register(subject string, fn HandlerFunc) (Unsub, error) {
	sub, err := b.nc.QueueSubscribe(subject, "q."+subject, func(msg *nats.Msg) {
		// Continue the caller's trace: extract context from the message headers
		// so this handler's span nests under the trigger that produced it.
		ctx := context.Background()
		if msg.Header != nil {
			ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier(msg.Header))
		}
		ctx, span := tracer.Start(ctx, "handle "+subject, trace.WithSpanKind(trace.SpanKindServer))
		span.SetAttributes(attribute.String("vent.subject", subject))
		defer span.End()

		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		resp, err := fn(ctx, msg.Data)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			b.replyError(msg, err)
			return
		}
		out, mErr := json.Marshal(resp)
		if mErr != nil {
			span.RecordError(mErr)
			span.SetStatus(codes.Error, mErr.Error())
			b.replyError(msg, mErr)
			return
		}
		_ = msg.Respond(out)
	})
	if err != nil {
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}

// errEnvelope is returned over the wire when a handler errors, so Trigger can
// surface it on the caller side.
type errEnvelope struct {
	Error string `json:"__vent_error"`
}

func (b *Bus) replyError(msg *nats.Msg, err error) {
	out, _ := json.Marshal(errEnvelope{Error: err.Error()})
	_ = msg.Respond(out)
}

// Trigger calls a function subject (request/reply) and unmarshals the reply
// into out (may be nil to ignore the reply). req may be nil for empty payloads.
func (b *Bus) Trigger(ctx context.Context, subject string, req any, out any) error {
	ctx, span := tracer.Start(ctx, "trigger "+subject, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(attribute.String("vent.subject", subject))
	defer span.End()

	var data []byte
	if req != nil {
		var err error
		if data, err = json.Marshal(req); err != nil {
			span.RecordError(err)
			return fmt.Errorf("marshal request: %w", err)
		}
	}

	// Carry trace context + session baggage to the responding worker.
	msg := nats.NewMsg(subject)
	msg.Data = data
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier(msg.Header))

	reply, err := b.nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("trigger %s: %w", subject, err)
	}
	// Detect handler-side errors.
	var env errEnvelope
	if json.Unmarshal(reply.Data, &env) == nil && env.Error != "" {
		span.SetStatus(codes.Error, env.Error)
		return fmt.Errorf("%s: %s", subject, env.Error)
	}
	if out != nil {
		if err := json.Unmarshal(reply.Data, out); err != nil {
			span.RecordError(err)
			return fmt.Errorf("unmarshal reply from %s: %w", subject, err)
		}
	}
	return nil
}

// PublishEvent emits an agent event onto the AGENT_EVENTS stream. The event is
// retained durably by JetStream and simultaneously delivered to any live core
// subscribers (the events gateway), so UIs see tokens as they are produced.
func (b *Bus) PublishEvent(ctx context.Context, ev types.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return b.nc.Publish(EventSubject(ev.SessionID), data)
}

// SubscribeEvents delivers live events for one session (or all sessions when
// sessionID is empty) to fn. Uses a plain core subscription for low-latency
// fanout; history can be read from the JetStream stream separately.
func (b *Bus) SubscribeEvents(sessionID string, fn func(types.Event)) (Unsub, error) {
	subject := "evt.*"
	if sessionID != "" {
		subject = EventSubject(sessionID)
	}
	sub, err := b.nc.Subscribe(subject, func(msg *nats.Msg) {
		var ev types.Event
		if json.Unmarshal(msg.Data, &ev) == nil {
			fn(ev)
		}
	})
	if err != nil {
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}

// PublishStep enqueues a durable turn-step wakeup onto the TURN_STEPS work
// queue. The orchestrator consumes these to advance the per-session FSM.
func (b *Bus) PublishStep(ctx context.Context, sessionID string) error {
	_, err := b.js.Publish(ctx, "turn.step."+sessionID, []byte(sessionID))
	return err
}

// --- State helpers -------------------------------------------------------

// KV opens (without creating) a key-value bucket.
func (b *Bus) KV(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	return b.js.KeyValue(ctx, bucket)
}

// GetJSON reads key from bucket and unmarshals it into out. Returns
// (false, nil) when the key is absent.
func (b *Bus) GetJSON(ctx context.Context, bucket, key string, out any) (bool, error) {
	kv, err := b.js.KeyValue(ctx, bucket)
	if err != nil {
		return false, err
	}
	entry, err := kv.Get(ctx, key)
	if err == jetstream.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(entry.Value(), out)
}

// PutJSON marshals val and stores it at bucket/key.
func (b *Bus) PutJSON(ctx context.Context, bucket, key string, val any) error {
	kv, err := b.js.KeyValue(ctx, bucket)
	if err != nil {
		return err
	}
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	_, err = kv.Put(ctx, key, data)
	return err
}

// Blobs opens the object store bucket for large payloads.
func (b *Bus) Blobs(ctx context.Context) (jetstream.ObjectStore, error) {
	return b.js.ObjectStore(ctx, ObjectBlobs)
}
