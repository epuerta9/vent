// Package compaction is a pure event-stream worker that keeps a session's
// context window from overflowing. It registers no function subjects; it only
// subscribes to the agent event stream. When a turn finishes (agent_end) it
// estimates the turn's context size and, if it crosses a threshold, summarizes
// the older messages into a single synthetic message — preserving recent
// history verbatim. Like the session worker, it is fully decoupled and its work
// is best-effort: failures are logged, never fatal, and it never destroys
// history it cannot first summarize.
package compaction

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

const (
	// keepRecent is how many of the most recent messages are preserved verbatim.
	keepRecent = 6
	// defaultContextWindow is used when the turn's model has no context window.
	defaultContextWindow = 200000
	// thresholdRatio is the fraction of the context window that triggers compaction.
	thresholdRatio = 0.8
	// fallbackModelID mirrors the orchestrator's fallback model.
	fallbackModelID = "claude-opus-4-8"
)

// Start subscribes to all agent events. On every agent_end event it attempts to
// compact the session's turn state if its estimated context size exceeds the
// threshold. It is non-blocking: the subscription lives for the connection's
// lifetime. It returns an error only if registering the subscription fails.
func Start(ctx context.Context, b *bus.Bus) error {
	_, err := b.SubscribeEvents("", func(ev types.Event) {
		if ev.Type != types.EvAgentEnd {
			return
		}
		compact(ctx, b, ev.SessionID)
	})
	if err != nil {
		return err
	}
	return nil
}

// compact loads the turn state, estimates its context size, and — if over the
// threshold — replaces the older messages with a model-produced summary. It is
// best-effort: every failure is logged and returns without mutating state.
func compact(ctx context.Context, b *bus.Bus, sessionID string) {
	var st types.TurnState
	found, err := b.GetJSON(ctx, bus.BucketTurnState, sessionID, &st)
	if err != nil {
		log.Printf("compaction: read turn state %q: %v", sessionID, err)
		return
	}
	if !found {
		return
	}

	// Guard against repeated compaction loops: there must be more than one
	// summary message plus the recent window to be worth compacting.
	if len(st.Messages) <= keepRecent+1 {
		return
	}

	window := st.Model.ContextWindow
	if window == 0 {
		window = defaultContextWindow
	}
	threshold := int(float64(window) * thresholdRatio)

	estimate := 0
	for _, msg := range st.Messages {
		estimate += len(textOf(msg)) / 4
	}
	if estimate < threshold {
		return
	}

	older := st.Messages[:len(st.Messages)-keepRecent]
	recent := st.Messages[len(st.Messages)-keepRecent:]

	provider := st.Provider
	if provider == "" {
		provider = "anthropic"
	}
	model := st.Model
	if model.ID == "" {
		model.ID = fallbackModelID
	}

	req := types.StreamRequest{
		SessionID:    sessionID,
		MessageID:    st.MessageID,
		Model:        model,
		SystemPrompt: "You are a summarizer. Produce a concise summary of the conversation so far, preserving decisions made, facts established, file paths, and open tasks. Output only the summary.",
		Messages: []types.Message{{
			Role:    types.RoleUser,
			Content: []types.ContentBlock{{Type: "text", Text: serializeMessages(older)}},
		}},
	}

	var resp types.StreamResponse
	if err := b.Trigger(ctx, bus.ProviderStreamSubject(provider), req, &resp); err != nil {
		// Never destroy history we could not summarize.
		log.Printf("compaction: summarize session %q: %v (leaving history intact)", sessionID, err)
		return
	}

	summaryText := resp.Message.TextContent()
	summaryMsg := types.Message{
		Role:    types.RoleUser,
		Content: []types.ContentBlock{{Type: "text", Text: "[Summary of earlier conversation]\n" + summaryText}},
	}

	compacted := len(older)
	st.Messages = append([]types.Message{summaryMsg}, recent...)
	if err := b.PutJSON(ctx, bus.BucketTurnState, sessionID, st); err != nil {
		log.Printf("compaction: write turn state %q: %v", sessionID, err)
		return
	}
	log.Printf("compaction: session %q compacted %d messages into a summary", sessionID, compacted)
}

// textOf returns all human-readable text in a message: the text-type content
// blocks plus, for tool results, their text content. Used for token estimation.
func textOf(msg types.Message) string {
	var sb strings.Builder
	for _, c := range msg.Content {
		if c.Type == "text" {
			sb.WriteString(c.Text)
		}
	}
	return sb.String()
}

// serializeMessages renders older messages as "role: text" lines for the
// summarizer to read.
func serializeMessages(msgs []types.Message) string {
	var sb strings.Builder
	for _, msg := range msgs {
		fmt.Fprintf(&sb, "%s: %s\n", msg.Role, textOf(msg))
	}
	return sb.String()
}
