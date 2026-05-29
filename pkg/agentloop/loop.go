// Package agentloop is a faithful Go port of pi's agent loop
// (pi-mono/packages/agent/src/agent-loop.ts), kept deliberately
// transport-agnostic: it knows nothing about NATS.
//
// It drives the per-turn state machine — stream an assistant response, run its
// tool calls (sequentially or in parallel), inject steering messages mid-run,
// continue on follow-ups — and emits the same event lifecycle pi does. The
// orchestrator wires the StreamFn, tool Execute, and before/after hooks to the
// bus, which is where credentials, policy, approvals, budget and hook fanout
// plug in. Swapping any of those is swapping a worker, not editing this loop.
package agentloop

import (
	"context"
	"encoding/json"

	"github.com/epuerta/vent/pkg/types"
)

// StreamFn produces one assistant message. The implementation owns the
// assistant message_start / message_update / message_end events (it has the
// streaming detail) and returns the finalised message. It must not return an
// error for model/runtime failures — encode those as a Message with StopReason
// "error" or "aborted" so the loop can terminate cleanly.
type StreamFn func(ctx context.Context, messages []types.Message, emit Emit) (types.Message, error)

// Execute runs a single tool call and returns its result.
type Execute func(ctx context.Context, call types.ContentBlock) (types.ToolResponse, error)

// BeforeResult is returned by the Before hook. Block prevents execution; the
// loop substitutes an error tool result carrying Reason.
type BeforeResult struct {
	Block  bool
	Reason string
}

// AfterResult optionally overrides parts of an executed tool result. Nil fields
// keep the original value (no deep merge), mirroring pi's afterToolCall.
type AfterResult struct {
	Content []types.ContentBlock
	Details json.RawMessage
	IsError *bool
}

// Emit delivers an event to subscribers.
type Emit func(types.Event)

// Config wires the loop to its environment.
type Config struct {
	SessionID string
	MaxTurns  int  // 0 means unlimited
	Parallel  bool // tool execution mode; default sequential

	Stream  StreamFn
	Execute Execute

	// Before runs after a tool call is identified, before it executes. Return
	// Block=true to deny. This is where policy + approval gating live.
	Before func(ctx context.Context, call types.ContentBlock) (BeforeResult, error)
	// After runs once a tool result is available, before the result event is
	// emitted. This is where the hook fanout + budget side effects live.
	After func(ctx context.Context, call types.ContentBlock, res types.ToolResponse) (*AfterResult, error)

	// GetSteering returns messages to inject before the next assistant turn
	// (mid-run steering). Return nil when none.
	GetSteering func(ctx context.Context) []types.Message
	// GetFollowUp returns messages to continue with after the agent would stop.
	GetFollowUp func(ctx context.Context) []types.Message

	Emit Emit
}

// Run executes the loop with a fresh prompt added to history. It returns the
// messages produced during this run (prompt + assistant + tool results).
func Run(ctx context.Context, prompts []types.Message, history []types.Message, cfg Config) []types.Message {
	emit := cfg.Emit
	if emit == nil {
		emit = func(types.Event) {}
	}

	newMessages := append([]types.Message{}, prompts...)
	messages := append(append([]types.Message{}, history...), prompts...)

	emit(cfg.ev(types.EvAgentStart))
	emit(cfg.ev(types.EvTurnStart))
	for i := range prompts {
		emit(cfg.evMsg(types.EvMessageStart, &prompts[i]))
		emit(cfg.evMsg(types.EvMessageEnd, &prompts[i]))
	}

	messages, newMessages = cfg.runLoop(ctx, messages, newMessages, true, emit)
	emit(types.Event{Type: types.EvAgentEnd, SessionID: cfg.SessionID, Messages: newMessages})
	return newMessages
}

func (cfg Config) runLoop(ctx context.Context, messages, newMessages []types.Message, firstTurn bool, emit Emit) ([]types.Message, []types.Message) {
	var pending []types.Message
	if cfg.GetSteering != nil {
		pending = cfg.GetSteering(ctx)
	}

	turn := 0
	for { // outer loop: resumes on follow-up messages
		hasMoreToolCalls := true

		for hasMoreToolCalls || len(pending) > 0 { // inner loop
			if !firstTurn {
				emit(cfg.ev(types.EvTurnStart))
			}
			firstTurn = false

			// Inject any pending steering messages.
			for i := range pending {
				m := pending[i]
				emit(cfg.evMsg(types.EvMessageStart, &m))
				emit(cfg.evMsg(types.EvMessageEnd, &m))
				messages = append(messages, m)
				newMessages = append(newMessages, m)
			}
			pending = nil

			// Stream the assistant response.
			msg, err := cfg.Stream(ctx, messages, emit)
			if err != nil {
				msg = types.Message{Role: types.RoleAssistant, StopReason: types.StopError, ErrorMessage: err.Error()}
			}
			messages = append(messages, msg)
			newMessages = append(newMessages, msg)

			if msg.StopReason == types.StopError || msg.StopReason == types.StopAborted {
				emit(types.Event{Type: types.EvTurnEnd, SessionID: cfg.SessionID, Message: &msg})
				return messages, newMessages
			}

			toolCalls := msg.ToolCalls()
			hasMoreToolCalls = len(toolCalls) > 0

			var toolResults []types.Message
			if hasMoreToolCalls {
				toolResults = cfg.executeToolCalls(ctx, toolCalls, emit)
				messages = append(messages, toolResults...)
				newMessages = append(newMessages, toolResults...)
			}

			emit(types.Event{Type: types.EvTurnEnd, SessionID: cfg.SessionID, Message: &msg, ToolResults: toolResults})

			turn++
			if cfg.MaxTurns > 0 && turn >= cfg.MaxTurns {
				return messages, newMessages
			}

			if cfg.GetSteering != nil {
				pending = cfg.GetSteering(ctx)
			}
		}

		// Agent would stop. Check for follow-up work.
		if cfg.GetFollowUp != nil {
			if followUps := cfg.GetFollowUp(ctx); len(followUps) > 0 {
				pending = followUps
				continue
			}
		}
		return messages, newMessages
	}
}

func (cfg Config) executeToolCalls(ctx context.Context, calls []types.ContentBlock, emit Emit) []types.Message {
	if cfg.Parallel {
		return cfg.executeParallel(ctx, calls, emit)
	}
	return cfg.executeSequential(ctx, calls, emit)
}

func (cfg Config) executeSequential(ctx context.Context, calls []types.ContentBlock, emit Emit) []types.Message {
	results := make([]types.Message, 0, len(calls))
	for _, call := range calls {
		emit(cfg.evToolStart(call))
		res, isErr := cfg.prepareAndRun(ctx, call)
		results = append(results, cfg.finalize(ctx, call, res, isErr, emit))
	}
	return results
}

func (cfg Config) executeParallel(ctx context.Context, calls []types.ContentBlock, emit Emit) []types.Message {
	type slot struct {
		call  types.ContentBlock
		res   types.ToolResponse
		isErr bool
		done  chan struct{}
	}
	// Preflight (Before hooks) run in source order; allowed tools then run
	// concurrently; results are collected back in source order.
	slots := make([]*slot, 0, len(calls))
	for _, call := range calls {
		emit(cfg.evToolStart(call))
		s := &slot{call: call, done: make(chan struct{})}
		slots = append(slots, s)
		before := cfg.runBefore(ctx, call)
		if before.blocked {
			s.res, s.isErr = before.res, true
			close(s.done)
			continue
		}
		go func(s *slot) {
			s.res, s.isErr = cfg.runTool(ctx, s.call)
			close(s.done)
		}(s)
	}
	results := make([]types.Message, 0, len(slots))
	for _, s := range slots {
		<-s.done
		results = append(results, cfg.finalize(ctx, s.call, s.res, s.isErr, emit))
	}
	return results
}

type beforeOutcome struct {
	blocked bool
	res     types.ToolResponse
}

func (cfg Config) runBefore(ctx context.Context, call types.ContentBlock) beforeOutcome {
	if cfg.Before == nil {
		return beforeOutcome{}
	}
	br, err := cfg.Before(ctx, call)
	if err != nil {
		return beforeOutcome{blocked: true, res: errResult(err.Error())}
	}
	if br.Block {
		reason := br.Reason
		if reason == "" {
			reason = "Tool execution was blocked"
		}
		return beforeOutcome{blocked: true, res: errResult(reason)}
	}
	return beforeOutcome{}
}

func (cfg Config) runTool(ctx context.Context, call types.ContentBlock) (types.ToolResponse, bool) {
	res, err := cfg.Execute(ctx, call)
	if err != nil {
		return errResult(err.Error()), true
	}
	return res, res.IsError
}

// prepareAndRun is the sequential path: Before, then execute.
func (cfg Config) prepareAndRun(ctx context.Context, call types.ContentBlock) (types.ToolResponse, bool) {
	if b := cfg.runBefore(ctx, call); b.blocked {
		return b.res, true
	}
	return cfg.runTool(ctx, call)
}

func (cfg Config) finalize(ctx context.Context, call types.ContentBlock, res types.ToolResponse, isErr bool, emit Emit) types.Message {
	if cfg.After != nil {
		if override, err := cfg.After(ctx, call, res); err == nil && override != nil {
			if override.Content != nil {
				res.Content = override.Content
			}
			if override.Details != nil {
				res.Details = override.Details
			}
			if override.IsError != nil {
				isErr = *override.IsError
			}
		}
	}
	resBytes, _ := json.Marshal(res)
	emit(types.Event{
		Type: types.EvToolEnd, SessionID: cfg.SessionID,
		ToolCallID: call.ID, ToolName: call.Name, Result: resBytes, IsError: isErr,
	})
	msg := types.Message{
		Role:       types.RoleToolResult,
		ToolCallID: call.ID,
		ToolName:   call.Name,
		Content:    res.Content,
		IsError:    isErr,
	}
	emit(cfg.evMsg(types.EvMessageStart, &msg))
	emit(cfg.evMsg(types.EvMessageEnd, &msg))
	return msg
}

func errResult(text string) types.ToolResponse {
	return types.ToolResponse{Content: []types.ContentBlock{{Type: "text", Text: text}}, IsError: true}
}

// --- small event constructors ------------------------------------------

func (cfg Config) ev(t types.EventType) types.Event {
	return types.Event{Type: t, SessionID: cfg.SessionID}
}

func (cfg Config) evMsg(t types.EventType, m *types.Message) types.Event {
	return types.Event{Type: t, SessionID: cfg.SessionID, Message: m}
}

func (cfg Config) evToolStart(call types.ContentBlock) types.Event {
	return types.Event{
		Type: types.EvToolStart, SessionID: cfg.SessionID,
		ToolCallID: call.ID, ToolName: call.Name, Args: call.Arguments,
	}
}
