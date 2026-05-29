// Package hookfanout implements the generic before/after-call hook that every
// custom hook subscribes to. It registers fn.hook.publish_collect and acts as a
// clean passthrough: it logs the incoming HookCall (the place where redaction or
// other side-effects would hook in) and acknowledges that it was published.
package hookfanout

import (
	"context"
	"encoding/json"
	"log"

	"github.com/epuerta/vent/pkg/bus"
)

// HookCall is the payload sent to fn.hook.publish_collect describing one
// before/after tool-call hook point.
type HookCall struct {
	SessionID string          `json:"sessionId"`
	Phase     string          `json:"phase"`
	ToolName  string          `json:"toolName"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// Start registers the hook fanout subject on the bus. It is non-blocking; the
// registration lives for the lifetime of the connection.
func Start(ctx context.Context, b *bus.Bus) error {
	_, err := b.Register(bus.SubjHookPublish, func(ctx context.Context, data []byte) (any, error) {
		var call HookCall
		if err := json.Unmarshal(data, &call); err != nil {
			return nil, err
		}
		// This is where redaction / side-effects would hook in.
		log.Printf("hookfanout: session=%s phase=%s tool=%s payload=%s",
			call.SessionID, call.Phase, call.ToolName, string(call.Payload))
		return struct {
			Published bool `json:"published"`
		}{true}, nil
	})
	return err
}
