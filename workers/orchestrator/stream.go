package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/epuerta/vent/pkg/agentloop"
	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
	"github.com/nats-io/nats.go"
)

// makeStream wires one assistant turn to the provider worker. It owns the
// message_start / message_update / message_end events for the assistant message
// and subscribes to a per-message core subject to relay live deltas.
func makeStream(
	b *bus.Bus,
	req types.RunRequest,
	provider string,
	model types.Model,
	systemPrompt string,
	tools []types.ToolSpec,
	st *types.TurnState,
) agentloop.StreamFn {
	return func(ctx context.Context, messages []types.Message, emit agentloop.Emit) (types.Message, error) {
		msgID := fmt.Sprintf("m-%d", time.Now().UnixNano())
		streamSubj := "stream." + req.SessionID + "." + msgID

		// Accumulate streamed text so message_update carries a best-effort
		// partial assistant message alongside the raw delta.
		var mu sync.Mutex
		var acc string

		sub, _ := b.Conn().Subscribe(streamSubj, func(m *nats.Msg) {
			var d types.Delta
			if json.Unmarshal(m.Data, &d) != nil {
				return
			}
			mu.Lock()
			if d.Kind == "text" {
				acc += d.Text
			}
			partial := types.Message{
				Role:    types.RoleAssistant,
				Content: []types.ContentBlock{{Type: "text", Text: acc}},
			}
			delta := d
			mu.Unlock()
			emit(types.Event{
				Type:      types.EvMessageUpdate,
				SessionID: req.SessionID,
				Message:   &partial,
				Delta:     &delta,
			})
		})

		// message_start with an empty assistant message, before triggering.
		start := types.Message{Role: types.RoleAssistant}
		emit(types.Event{Type: types.EvMessageStart, SessionID: req.SessionID, Message: &start})

		sreq := types.StreamRequest{
			SessionID:     req.SessionID,
			MessageID:     msgID,
			Model:         model,
			SystemPrompt:  systemPrompt,
			Messages:      messages,
			Tools:         tools,
			StreamSubject: streamSubj,
		}
		var sresp types.StreamResponse
		err := b.Trigger(ctx, bus.ProviderStreamSubject(provider), sreq, &sresp)

		if sub != nil {
			_ = sub.Unsubscribe()
		}

		if err != nil {
			// Returning the error lets the loop encode it as a clean stop.
			return types.Message{}, err
		}

		final := sresp.Message
		final.Role = types.RoleAssistant
		emit(types.Event{Type: types.EvMessageEnd, SessionID: req.SessionID, Message: &final})

		// Fire-and-forget budget record.
		if final.Usage != nil {
			go func(u types.Usage) {
				_ = b.Trigger(context.Background(), bus.SubjBudgetRecord, types.BudgetRecord{
					SessionID: req.SessionID,
					Workspace: req.Workspace,
					Usage:     u,
					Model:     model.ID,
				}, nil)
			}(*final.Usage)
		}

		// Persist the conversation so far (the loop hands us the running set).
		st.Messages = append(messages, final)
		persist(b, st)

		return final, nil
	}
}
