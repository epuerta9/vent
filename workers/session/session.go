// Package session is a pure event-stream worker: it persists each session's
// message transcript by listening to the agent event stream. It registers no
// function subjects and has no orchestrator coupling — it participates only by
// subscribing to events, demonstrating the decoupled worker model.
package session

import (
	"context"
	"log"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// Start subscribes to all agent events. On every message_end event it appends
// the completed Message to the session's stored transcript in the sessions KV
// bucket. It is non-blocking: the subscription lives for the connection's
// lifetime. It returns an error only if registering the subscription fails.
func Start(ctx context.Context, b *bus.Bus) error {
	_, err := b.SubscribeEvents("", func(ev types.Event) {
		if ev.Type != types.EvMessageEnd || ev.Message == nil {
			return
		}
		appendMessage(ctx, b, ev.SessionID, *ev.Message)
	})
	if err != nil {
		return err
	}
	return nil
}

// appendMessage reads the current transcript, appends msg, and writes it back.
// It is best-effort: errors are logged, never fatal.
func appendMessage(ctx context.Context, b *bus.Bus, sessionID string, msg types.Message) {
	var transcript []types.Message
	if _, err := b.GetJSON(ctx, bus.BucketSessions, sessionID, &transcript); err != nil {
		log.Printf("session: read transcript %q: %v", sessionID, err)
		return
	}
	transcript = append(transcript, msg)
	if err := b.PutJSON(ctx, bus.BucketSessions, sessionID, transcript); err != nil {
		log.Printf("session: write transcript %q: %v", sessionID, err)
	}
}
