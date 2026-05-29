// Package orchestrator is the durable turn-loop worker: the heart of the
// harness that ties the bus, the persisted FSM state, and the transport-agnostic
// agentloop together.
//
// It registers two subjects. fn.harness.trigger is the public entrypoint a
// client hits; it forwards straight to fn.run.start (the hop where OTel baggage
// would be seeded). fn.run.start seeds the initial TurnState, launches the turn
// on a detached goroutine, and acknowledges immediately so the request returns
// while the turn runs durably in the background.
//
// The goroutine provisions a model and system prompt, loads the tool catalogue,
// then drives agentloop.Run with Stream/Execute/Before/After wired to the bus:
// provider streaming, tool dispatch, the policy + approval gate, and hook/budget
// side effects. Every optional bus hop is defensive — a failing trigger falls
// back rather than aborting the turn.
package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/epuerta/vent/pkg/agentloop"
	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/obs"
	"github.com/epuerta/vent/pkg/types"
)

const defaultMaxTurns = 20

// fallbackModel is used whenever the model catalogue is unreachable.
var fallbackModel = types.Model{
	ID:            "claude-opus-4-8",
	Provider:      "anthropic",
	ContextWindow: 200000,
	MaxTokens:     64000,
	SupportsTools: true,
}

// Start registers the harness-trigger and run-start subjects on the bus. It is
// non-blocking: registrations live for the lifetime of the connection and the
// turn itself runs on a detached goroutine launched from the run-start handler.
func Start(ctx context.Context, b *bus.Bus) error {
	// fn.harness.trigger: the public entrypoint. Forward to fn.run.start. This
	// hop is where OTel baggage would be seeded onto the outgoing context.
	if _, err := b.Register(bus.SubjHarnessTrigger, func(ctx context.Context, data []byte) (any, error) {
		var req types.RunRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, err
		}
		var resp types.RunResponse
		if err := b.Trigger(ctx, bus.SubjRunStart, req, &resp); err != nil {
			return nil, err
		}
		return resp, nil
	}); err != nil {
		return err
	}

	// fn.run.start: seed initial state, launch the turn, return immediately.
	if _, err := b.Register(bus.SubjRunStart, func(ctx context.Context, data []byte) (any, error) {
		var req types.RunRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, err
		}

		state := types.TurnState{
			SessionID: req.SessionID,
			MessageID: req.MessageID,
			Phase:     types.PhaseProvisioning,
			Mode:      req.Mode,
			Provider:  req.Provider,
			Workspace: req.Workspace,
			MaxTurns:  req.MaxTurns,
		}
		// Persist the seed using the request ctx; the turn itself detaches.
		if err := b.PutJSON(ctx, bus.BucketTurnState, req.SessionID, state); err != nil {
			return nil, err
		}

		// Detach: the turn outlives this request, so it must not use ctx.
		go runTurn(b, req)

		return types.RunResponse{SessionID: req.SessionID, Accepted: true}, nil
	}); err != nil {
		return err
	}

	return nil
}

// persist writes the current TurnState, swallowing errors (best-effort; the
// turn must keep running even if a state write transiently fails).
func persist(b *bus.Bus, st *types.TurnState) {
	_ = b.PutJSON(context.Background(), bus.BucketTurnState, st.SessionID, *st)
}

// runTurn provisions and runs a single durable turn on a detached context.
func runTurn(b *bus.Bus, req types.RunRequest) {
	// Root the per-turn trace and seed the session id into baggage so every
	// downstream bus hop (provider, policy, tools, budget, hooks) nests under
	// one connected trace — the "carry one trace across every step" property.
	ctx, span := obs.StartTurn(context.Background(), req.SessionID, req.MessageID)
	defer span.End()

	// Load (or re-seed) the turn state.
	var st types.TurnState
	if found, err := b.GetJSON(ctx, bus.BucketTurnState, req.SessionID, &st); err != nil || !found {
		st = types.TurnState{SessionID: req.SessionID, MessageID: req.MessageID, Phase: types.PhaseProvisioning}
	}
	st.Mode = req.Mode
	st.Workspace = req.Workspace

	// Recover from any panic so the UI unblocks and state reflects the failure.
	defer func() {
		if r := recover(); r != nil {
			st.Phase = types.PhaseFailed
			st.Error = fmt.Sprintf("orchestrator panic: %v", r)
			persist(b, &st)
			_ = b.PublishEvent(ctx, types.Event{Type: types.EvAgentEnd, SessionID: req.SessionID})
		}
	}()

	// --- PROVISIONING ----------------------------------------------------
	provider := req.Provider
	if provider == "" {
		provider = "anthropic"
	}
	model := resolveModel(ctx, b, req, provider)
	provider = model.Provider // honour whatever the catalogue returned
	if provider == "" {
		provider = "anthropic"
	}

	systemPrompt := assembleSystemPrompt(ctx, b, req)
	tools := loadTools(ctx, b)

	st.Phase = types.PhaseAssistant
	st.Provider = provider
	st.Model = model
	st.SystemPrompt = systemPrompt
	if st.MaxTurns == 0 {
		st.MaxTurns = req.MaxTurns
	}
	persist(b, &st)

	maxTurns := req.MaxTurns
	if maxTurns == 0 {
		maxTurns = defaultMaxTurns
	}

	cfg := agentloop.Config{
		SessionID: req.SessionID,
		MaxTurns:  maxTurns,
		Parallel:  true,
		Emit: func(ev types.Event) {
			_ = b.PublishEvent(context.Background(), ev)
		},
		Stream:  makeStream(b, req, provider, model, systemPrompt, tools, &st),
		Execute: makeExecute(b, req, &st),
		Before:  makeBefore(b, req, &st),
		After:   makeAfter(b, req),
	}

	// History is whatever the loaded state already carried (nil for fresh).
	history := st.Messages

	final := agentloop.Run(ctx, req.Prompt, history, cfg)

	st.Phase = types.PhaseStopped
	st.Messages = final
	persist(b, &st)
}
