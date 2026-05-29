// Package openai is a provider worker for any OpenAI-compatible Chat
// Completions endpoint. It registers fn.provider.openai.stream and talks to the
// base URL in OPENAI_BASE_URL (default https://api.openai.com/v1), so the same
// worker drives OpenAI, OpenCode Zen, Ollama, vLLM, or any compatible gateway —
// proving the harness's provider layer is genuinely swappable. Credentials come
// from the auth worker (provider "openai" -> OPENAI_API_KEY).
package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

const defaultBaseURL = "https://api.openai.com/v1"

// Start registers the provider's stream function on the bus.
func Start(ctx context.Context, b *bus.Bus) error {
	_, err := b.Register(bus.ProviderStreamSubject("openai"), func(ctx context.Context, data []byte) (any, error) {
		var req types.StreamRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, fmt.Errorf("decode stream request: %w", err)
		}
		msg, err := stream(ctx, b, req)
		if err != nil {
			return nil, err
		}
		return types.StreamResponse{Message: msg}, nil
	})
	return err
}

func baseURL() string {
	if u := strings.TrimSpace(os.Getenv("OPENAI_BASE_URL")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return defaultBaseURL
}

func stream(ctx context.Context, b *bus.Bus, req types.StreamRequest) (types.Message, error) {
	var tok types.TokenResponse
	if err := b.Trigger(ctx, bus.SubjAuthGetToken, types.TokenRequest{Provider: "openai"}, &tok); err != nil {
		return types.Message{}, fmt.Errorf("auth: %w", err)
	}

	body := buildRequest(req)
	raw, err := json.Marshal(body)
	if err != nil {
		return types.Message{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL()+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		return types.Message{}, err
	}
	httpReq.Header.Set("authorization", "Bearer "+tok.Token)
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("accept", "text/event-stream")

	resp, err := (&http.Client{Timeout: 10 * time.Minute}).Do(httpReq)
	if err != nil {
		return types.Message{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		return types.Message{}, fmt.Errorf("openai %d: %s", resp.StatusCode, strings.TrimSpace(buf.String()))
	}
	return parseSSE(b, req, resp.Body)
}

// --- request construction ------------------------------------------------

type apiRequest struct {
	Model         string        `json:"model"`
	Messages      []apiMessage  `json:"messages"`
	Tools         []apiTool     `json:"tools,omitempty"`
	Stream        bool          `json:"stream"`
	StreamOptions *streamOption `json:"stream_options,omitempty"`
	MaxTokens     int           `json:"max_completion_tokens,omitempty"`
}

type streamOption struct {
	IncludeUsage bool `json:"include_usage"`
}

type apiMessage struct {
	Role       string        `json:"role"`
	Content    string        `json:"content,omitempty"`
	ToolCalls  []apiToolCall `json:"tool_calls,omitempty"`
	ToolCallID string        `json:"tool_call_id,omitempty"`
}

type apiToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Index    int    `json:"index,omitempty"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type apiTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

func buildRequest(req types.StreamRequest) apiRequest {
	out := apiRequest{
		Model:         req.Model.ID,
		Messages:      convertMessages(req.SystemPrompt, req.Messages),
		Stream:        true,
		StreamOptions: &streamOption{IncludeUsage: true},
	}
	if req.Model.MaxTokens > 0 {
		out.MaxTokens = req.Model.MaxTokens
	}
	for _, t := range req.Tools {
		schema := t.Schema
		if len(schema) == 0 {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		var at apiTool
		at.Type = "function"
		at.Function.Name = t.Name
		at.Function.Description = t.Description
		at.Function.Parameters = schema
		out.Tools = append(out.Tools, at)
	}
	return out
}

func convertMessages(systemPrompt string, msgs []types.Message) []apiMessage {
	out := make([]apiMessage, 0, len(msgs)+1)
	if systemPrompt != "" {
		out = append(out, apiMessage{Role: "system", Content: systemPrompt})
	}
	for _, m := range msgs {
		switch m.Role {
		case types.RoleUser:
			out = append(out, apiMessage{Role: "user", Content: m.TextContent()})
		case types.RoleAssistant:
			am := apiMessage{Role: "assistant", Content: m.TextContent()}
			for _, c := range m.Content {
				if c.Type == "toolCall" {
					tc := apiToolCall{ID: c.ID, Type: "function"}
					tc.Function.Name = c.Name
					tc.Function.Arguments = string(c.Arguments)
					if tc.Function.Arguments == "" {
						tc.Function.Arguments = "{}"
					}
					am.ToolCalls = append(am.ToolCalls, tc)
				}
			}
			out = append(out, am)
		case types.RoleToolResult:
			out = append(out, apiMessage{Role: "tool", ToolCallID: m.ToolCallID, Content: m.TextContent()})
		}
	}
	return out
}

// --- SSE parsing ---------------------------------------------------------

type toolAcc struct {
	id   string
	name string
	args strings.Builder
}

func parseSSE(b *bus.Bus, req types.StreamRequest, body interface{ Read([]byte) (int, error) }) (types.Message, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	var text strings.Builder
	tools := map[int]*toolAcc{}
	var toolOrder []int
	finish := ""
	usage := types.Usage{}

	publishDelta := func(d types.Delta) {
		if req.StreamSubject == "" {
			return
		}
		if data, err := json.Marshal(d); err == nil {
			_ = b.Conn().Publish(req.StreamSubject, data)
		}
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			break
		}
		var chunk sseChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			continue
		}
		if chunk.Usage != nil {
			usage.InputTokens = chunk.Usage.PromptTokens
			usage.OutputTokens = chunk.Usage.CompletionTokens
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		ch := chunk.Choices[0]
		if ch.FinishReason != "" {
			finish = ch.FinishReason
		}
		if ch.Delta.Content != "" {
			text.WriteString(ch.Delta.Content)
			publishDelta(types.Delta{Kind: "text", Text: ch.Delta.Content})
		}
		for _, tc := range ch.Delta.ToolCalls {
			acc := tools[tc.Index]
			if acc == nil {
				acc = &toolAcc{}
				tools[tc.Index] = acc
				toolOrder = append(toolOrder, tc.Index)
			}
			if tc.ID != "" {
				acc.id = tc.ID
			}
			if tc.Function.Name != "" {
				acc.name = tc.Function.Name
			}
			if tc.Function.Arguments != "" {
				acc.args.WriteString(tc.Function.Arguments)
				publishDelta(types.Delta{Kind: "toolcall", Text: tc.Function.Arguments, Index: tc.Index})
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return types.Message{}, fmt.Errorf("read stream: %w", err)
	}

	msg := types.Message{Role: types.RoleAssistant, StopReason: mapStop(finish), Usage: &usage, Timestamp: time.Now().UnixMilli()}
	if text.Len() > 0 {
		msg.Content = append(msg.Content, types.ContentBlock{Type: "text", Text: text.String()})
	}
	for _, idx := range toolOrder {
		acc := tools[idx]
		args := acc.args.String()
		if strings.TrimSpace(args) == "" {
			args = "{}"
		}
		msg.Content = append(msg.Content, types.ContentBlock{
			Type: "toolCall", ID: acc.id, Name: acc.name, Arguments: json.RawMessage(args),
		})
	}
	return msg, nil
}

func mapStop(reason string) types.StopReason {
	switch reason {
	case "tool_calls":
		return types.StopToolUse
	case "length":
		return types.StopMaxTokens
	default:
		return types.StopEnd
	}
}

type sseChunk struct {
	Choices []struct {
		Delta struct {
			Content   string        `json:"content"`
			ToolCalls []apiToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}
