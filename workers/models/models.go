// Package models is the static model catalogue worker. It registers the
// fn.models.* subjects with a hard-coded set of Anthropic models. A live-API
// worker can replace it by registering the same subjects.
package models

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// catalogue is the static set of models this worker serves, indexed by ID.
var catalogue = []types.Model{
	{
		ID:             "claude-opus-4-8",
		Provider:       "anthropic",
		ContextWindow:  200000,
		MaxTokens:      64000,
		SupportsTools:  true,
		SupportsVision: true,
		Reasoning:      true,
	},
	{
		ID:             "claude-sonnet-4-6",
		Provider:       "anthropic",
		ContextWindow:  200000,
		MaxTokens:      64000,
		SupportsTools:  true,
		SupportsVision: true,
		Reasoning:      true,
	},
	{
		ID:             "claude-haiku-4-5",
		Provider:       "anthropic",
		ContextWindow:  200000,
		MaxTokens:      32000,
		SupportsTools:  true,
		SupportsVision: true,
		Reasoning:      false,
	},
	// OpenAI-compatible models. The openai provider worker reads OPENAI_BASE_URL,
	// so these route to OpenAI directly or to any compatible gateway (e.g.
	// OpenCode Zen at https://opencode.ai/zen/v1).
	{
		ID:            "gpt-5.5",
		Provider:      "openai",
		ContextWindow: 400000,
		MaxTokens:     64000,
		SupportsTools: true,
		Reasoning:     true,
	},
	{
		ID:            "gpt-5.4",
		Provider:      "openai",
		ContextWindow: 400000,
		MaxTokens:     64000,
		SupportsTools: true,
		Reasoning:     true,
	},
}

// getRequest selects one model by ID.
type getRequest struct {
	ID string `json:"id"`
}

// supportsRequest asks whether a model has a named capability.
type supportsRequest struct {
	ID         string `json:"id"`
	Capability string `json:"capability"` // tools | vision | reasoning
}

// supportsResponse is the verdict for a capability query.
type supportsResponse struct {
	Supported bool `json:"supported"`
}

func lookup(id string) (types.Model, bool) {
	for _, m := range catalogue {
		if m.ID == id {
			return m, true
		}
	}
	return types.Model{}, false
}

// Start registers the model-catalogue subjects on the bus. It is non-blocking;
// the registrations live for the lifetime of the connection.
func Start(ctx context.Context, b *bus.Bus) error {
	if _, err := b.Register(bus.SubjModelsList, func(ctx context.Context, data []byte) (any, error) {
		return catalogue, nil
	}); err != nil {
		return err
	}

	if _, err := b.Register(bus.SubjModelsGet, func(ctx context.Context, data []byte) (any, error) {
		var req getRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, fmt.Errorf("decode request: %w", err)
		}
		m, ok := lookup(req.ID)
		if !ok {
			return nil, fmt.Errorf("unknown model: %q", req.ID)
		}
		return m, nil
	}); err != nil {
		return err
	}

	if _, err := b.Register(bus.SubjModelsSupports, func(ctx context.Context, data []byte) (any, error) {
		var req supportsRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, fmt.Errorf("decode request: %w", err)
		}
		m, ok := lookup(req.ID)
		if !ok {
			return nil, fmt.Errorf("unknown model: %q", req.ID)
		}
		var supported bool
		switch req.Capability {
		case "tools":
			supported = m.SupportsTools
		case "vision":
			supported = m.SupportsVision
		case "reasoning":
			supported = m.Reasoning
		default:
			return nil, fmt.Errorf("unknown capability: %q", req.Capability)
		}
		return supportsResponse{Supported: supported}, nil
	}); err != nil {
		return err
	}

	return nil
}
