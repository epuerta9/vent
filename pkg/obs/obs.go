// Package obs wires OpenTelemetry for the harness. It is the one place that
// touches the OTel SDK; everything else (notably pkg/bus) only uses the OTel
// API and the globally-installed propagator, so workers stay light.
//
// The propagator is always installed, so trace context + the session baggage
// flow across every bus hop and a single turn shows up as one connected trace
// spanning every worker that participated — the property iii gets from wrapping
// each registerFunction in a span. An exporter is only attached when VENT_TRACE
// is set (stdout today; OTLP is a drop-in), so the default path is near-zero
// overhead.
package obs

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// SessionBaggageKey is the baggage member carrying the session id across hops,
// so traces can be grouped by session (iii's "group by session" view).
const SessionBaggageKey = "vent.session.id"

// Init installs the composite propagator and, when VENT_TRACE requests it, a
// span exporter. It returns a shutdown func to flush on exit.
//
//	VENT_TRACE=stdout  pretty-print spans to stdout
//	VENT_TRACE=off|""  propagate context but export nothing (default)
func Init(service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	switch os.Getenv("VENT_TRACE") {
	case "stdout":
		exp, err := stdouttrace.New(stdouttrace.WithoutTimestamps())
		if err != nil {
			return nil, err
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
		)
		otel.SetTracerProvider(tp)
		return tp.Shutdown, nil
	default:
		// No exporter: spans are created but non-recording, so propagation has
		// nothing to inject and overhead stays negligible.
		return func(context.Context) error { return nil }, nil
	}
}

// StartTurn opens the per-turn root span and seeds the session id into baggage
// so every downstream bus hop carries it. The orchestrator calls this on its
// detached turn goroutine; the returned context must be threaded into the loop.
func StartTurn(ctx context.Context, sessionID, messageID string) (context.Context, trace.Span) {
	if sessionID != "" {
		if m, err := baggage.NewMember(SessionBaggageKey, sessionID); err == nil {
			if bg, err := baggage.New(m); err == nil {
				ctx = baggage.ContextWithBaggage(ctx, bg)
			}
		}
	}
	ctx, span := otel.Tracer("vent/orchestrator").Start(ctx, "turn",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String(SessionBaggageKey, sessionID),
			attribute.String("vent.message.id", messageID),
		),
	)
	return ctx, span
}

// SessionFromContext reads the session id out of baggage, if present.
func SessionFromContext(ctx context.Context) string {
	return baggage.FromContext(ctx).Member(SessionBaggageKey).Value()
}
