package orchestrator

import (
	"context"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// resolveModel picks the model for this turn, falling back to a hardcoded one
// on any failure so provisioning never aborts the turn.
func resolveModel(ctx context.Context, b *bus.Bus, req types.RunRequest, provider string) types.Model {
	if req.ModelID != "" {
		var m types.Model
		reqGet := struct {
			ID       string `json:"id"`
			Provider string `json:"provider"`
		}{req.ModelID, provider}
		if err := b.Trigger(ctx, bus.SubjModelsGet, reqGet, &m); err == nil && m.ID != "" {
			return m
		}
		// Catalogue miss: trust the request so any provider/model routes, even
		// ones not in the static catalogue (e.g. a gateway's model id).
		return types.Model{ID: req.ModelID, Provider: provider, ContextWindow: 200000, MaxTokens: 16000, SupportsTools: true}
	}

	var models []types.Model
	if err := b.Trigger(ctx, bus.SubjModelsList, struct{}{}, &models); err == nil {
		for _, m := range models {
			if m.Provider == provider {
				return m
			}
		}
	}
	return fallbackModel
}

// assembleSystemPrompt builds the system prompt. An explicit override is used
// verbatim; otherwise it is layered from mode + identity + skill appendix, with
// every optional bus hop ignored on error.
func assembleSystemPrompt(ctx context.Context, b *bus.Bus, req types.RunRequest) string {
	if req.SystemPrompt != "" {
		return req.SystemPrompt
	}

	prompt := modeParagraph(req.Mode)

	// Identity preamble (optional skill body).
	var skill types.Skill
	if err := b.Trigger(ctx, bus.SubjSkillsGet, types.SkillsGetRequest{
		Namespace: "vent", Name: "identity",
	}, &skill); err == nil && skill.Body != "" {
		prompt += "\n\n" + skill.Body
	} else {
		prompt += "\n\nYou are vent, a composable agent harness. Be precise, honest, and concise."
	}

	// Appendix listing available skills (optional).
	var skills []types.Skill
	if err := b.Trigger(ctx, bus.SubjSkillsList, struct{}{}, &skills); err == nil && len(skills) > 0 {
		prompt += "\n\nAvailable skills:"
		for _, s := range skills {
			name := s.Name
			if s.Namespace != "" {
				name = s.Namespace + "/" + s.Name
			}
			prompt += "\n- " + name
		}
	}

	return prompt
}

func modeParagraph(mode types.Mode) string {
	switch mode {
	case types.ModePlan:
		return "You are operating in PLAN mode. Produce a clear, ordered plan for the user's request. Do not mutate any state, edit files, or run side-effecting tools — planning only."
	case types.ModeAsk:
		return "You are operating in ASK mode. Answer the user's question directly. Prefer read-only tools; avoid any action that mutates state."
	default: // agent
		return "You are operating in AGENT mode. Act autonomously to accomplish the user's request end to end, using the available tools as needed."
	}
}

// loadTools reads every advertised ToolSpec from the tools KV bucket. Returns an
// empty slice on any error.
func loadTools(ctx context.Context, b *bus.Bus) []types.ToolSpec {
	kv, err := b.KV(ctx, bus.BucketTools)
	if err != nil {
		return nil
	}
	keys, err := kv.Keys(ctx)
	if err != nil {
		return nil
	}
	specs := make([]types.ToolSpec, 0, len(keys))
	for _, k := range keys {
		var spec types.ToolSpec
		if found, err := b.GetJSON(ctx, bus.BucketTools, k, &spec); err == nil && found {
			specs = append(specs, spec)
		}
	}
	return specs
}
