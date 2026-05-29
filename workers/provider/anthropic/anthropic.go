// Package anthropic is the Anthropic provider worker. It registers
// fn.provider.anthropic.stream, resolves its credential through the auth
// worker, calls the Anthropic Messages API with streaming enabled, relays
// incremental deltas onto the caller's stream subject, and returns the
// finalised assistant message. Adding another provider is writing a sibling
// package that registers fn.provider.<name>.stream — nothing else changes.
package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

const (
	apiURL     = "https://api.anthropic.com/v1/messages"
	apiVersion = "2023-06-01"
)

// Start registers the provider's stream function on the bus.
func Start(ctx context.Context, b *bus.Bus) error {
	_, err := b.Register(bus.ProviderStreamSubject("anthropic"), func(ctx context.Context, data []byte) (any, error) {
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

func stream(ctx context.Context, b *bus.Bus, req types.StreamRequest) (types.Message, error) {
	// Resolve credential via the swappable auth worker.
	var tok types.TokenResponse
	if err := b.Trigger(ctx, bus.SubjAuthGetToken, types.TokenRequest{Provider: "anthropic"}, &tok); err != nil {
		return types.Message{}, fmt.Errorf("auth: %w", err)
	}

	body := buildRequest(req)
	raw, err := json.Marshal(body)
	if err != nil {
		return types.Message{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(raw))
	if err != nil {
		return types.Message{}, err
	}
	httpReq.Header.Set("x-api-key", tok.Token)
	httpReq.Header.Set("anthropic-version", apiVersion)
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("accept", "text/event-stream")

	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(httpReq)
	if err != nil {
		return types.Message{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := bufio.NewReader(resp.Body).ReadString(0)
		return types.Message{}, fmt.Errorf("anthropic %d: %s", resp.StatusCode, strings.TrimSpace(errBody))
	}

	return parseSSE(b, req, resp.Body)
}

// --- request construction ------------------------------------------------

type apiRequest struct {
	Model     string         `json:"model"`
	MaxTokens int            `json:"max_tokens"`
	System    string         `json:"system,omitempty"`
	Messages  []apiMessage   `json:"messages"`
	Tools     []apiTool      `json:"tools,omitempty"`
	Stream    bool           `json:"stream"`
}

type apiMessage struct {
	Role    string     `json:"role"`
	Content []apiBlock `json:"content"`
}

type apiBlock struct {
	Type string `json:"type"`
	// text
	Text string `json:"text,omitempty"`
	// tool_use
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
	// tool_result
	ToolUseID string     `json:"tool_use_id,omitempty"`
	Content   []apiBlock `json:"content,omitempty"`
	IsError   bool       `json:"is_error,omitempty"`
	// image
	Source *apiSource `json:"source,omitempty"`
}

type apiSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
}

type apiTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

func buildRequest(req types.StreamRequest) apiRequest {
	maxTokens := req.Model.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}
	out := apiRequest{
		Model:     req.Model.ID,
		MaxTokens: maxTokens,
		System:    req.SystemPrompt,
		Messages:  convertMessages(req.Messages),
		Stream:    true,
	}
	for _, t := range req.Tools {
		schema := t.Schema
		if len(schema) == 0 {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		out.Tools = append(out.Tools, apiTool{Name: t.Name, Description: t.Description, InputSchema: schema})
	}
	return out
}

// convertMessages maps vent messages to the Anthropic wire format. Tool results
// must ride inside a user message, and consecutive tool results coalesce into a
// single user message, which is what the API expects after a tool_use turn.
func convertMessages(msgs []types.Message) []apiMessage {
	var out []apiMessage
	pendingToolUser := -1 // index in out of the user message accumulating tool_results

	for _, m := range msgs {
		switch m.Role {
		case types.RoleToolResult:
			block := apiBlock{
				Type:      "tool_result",
				ToolUseID: m.ToolCallID,
				Content:   convertContent(m.Content),
				IsError:   m.IsError,
			}
			if pendingToolUser >= 0 {
				out[pendingToolUser].Content = append(out[pendingToolUser].Content, block)
			} else {
				out = append(out, apiMessage{Role: "user", Content: []apiBlock{block}})
				pendingToolUser = len(out) - 1
			}
		case types.RoleUser:
			out = append(out, apiMessage{Role: "user", Content: convertContent(m.Content)})
			pendingToolUser = -1
		case types.RoleAssistant:
			out = append(out, apiMessage{Role: "assistant", Content: convertContent(m.Content)})
			pendingToolUser = -1
		}
	}
	return out
}

func convertContent(blocks []types.ContentBlock) []apiBlock {
	var out []apiBlock
	for _, c := range blocks {
		switch c.Type {
		case "text":
			out = append(out, apiBlock{Type: "text", Text: c.Text})
		case "toolCall":
			input := c.Arguments
			if len(input) == 0 {
				input = json.RawMessage(`{}`)
			}
			out = append(out, apiBlock{Type: "tool_use", ID: c.ID, Name: c.Name, Input: input})
		case "image":
			out = append(out, apiBlock{Type: "image", Source: &apiSource{Type: "base64", MediaType: c.MimeType, Data: c.Data}})
		}
	}
	if out == nil {
		out = []apiBlock{}
	}
	return out
}

// --- SSE parsing ---------------------------------------------------------

// blockAcc accumulates one streaming content block.
type blockAcc struct {
	typ       string
	text      strings.Builder
	toolID    string
	toolName  string
	toolInput strings.Builder
}

func parseSSE(b *bus.Bus, req types.StreamRequest, body interface{ Read([]byte) (int, error) }) (types.Message, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	blocks := map[int]*blockAcc{}
	var order []int
	stopReason := ""
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
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		var ev sseEvent
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			continue
		}
		switch ev.Type {
		case "message_start":
			if ev.Message != nil {
				usage.InputTokens = ev.Message.Usage.InputTokens
			}
		case "content_block_start":
			acc := &blockAcc{typ: ev.ContentBlock.Type}
			if acc.typ == "tool_use" {
				acc.toolID = ev.ContentBlock.ID
				acc.toolName = ev.ContentBlock.Name
			}
			blocks[ev.Index] = acc
			order = append(order, ev.Index)
		case "content_block_delta":
			acc := blocks[ev.Index]
			if acc == nil {
				continue
			}
			switch ev.Delta.Type {
			case "text_delta":
				acc.text.WriteString(ev.Delta.Text)
				publishDelta(types.Delta{Kind: "text", Text: ev.Delta.Text, Index: ev.Index})
			case "input_json_delta":
				acc.toolInput.WriteString(ev.Delta.PartialJSON)
				publishDelta(types.Delta{Kind: "toolcall", Text: ev.Delta.PartialJSON, Index: ev.Index})
			case "thinking_delta":
				acc.text.WriteString(ev.Delta.Thinking)
				publishDelta(types.Delta{Kind: "thinking", Text: ev.Delta.Thinking, Index: ev.Index})
			}
		case "message_delta":
			if ev.Delta.StopReason != "" {
				stopReason = ev.Delta.StopReason
			}
			if ev.Usage != nil {
				usage.OutputTokens = ev.Usage.OutputTokens
			}
		case "message_stop":
			// done
		case "error":
			return types.Message{}, fmt.Errorf("anthropic stream error: %s", payload)
		}
	}
	if err := scanner.Err(); err != nil {
		return types.Message{}, fmt.Errorf("read stream: %w", err)
	}

	msg := types.Message{Role: types.RoleAssistant, StopReason: mapStop(stopReason), Usage: &usage, Timestamp: nowMillis()}
	for _, idx := range order {
		acc := blocks[idx]
		switch acc.typ {
		case "text", "thinking":
			t := "text"
			if acc.typ == "thinking" {
				t = "thinking"
			}
			msg.Content = append(msg.Content, types.ContentBlock{Type: t, Text: acc.text.String()})
		case "tool_use":
			args := acc.toolInput.String()
			if strings.TrimSpace(args) == "" {
				args = "{}"
			}
			msg.Content = append(msg.Content, types.ContentBlock{
				Type: "toolCall", ID: acc.toolID, Name: acc.toolName, Arguments: json.RawMessage(args),
			})
		}
	}
	return msg, nil
}

func mapStop(reason string) types.StopReason {
	switch reason {
	case "end_turn", "stop_sequence":
		return types.StopEnd
	case "tool_use":
		return types.StopToolUse
	case "max_tokens":
		return types.StopMaxTokens
	default:
		return types.StopEnd
	}
}

func nowMillis() int64 { return time.Now().UnixMilli() }

// sseEvent is the union of fields across Anthropic SSE event types.
type sseEvent struct {
	Type    string `json:"type"`
	Index   int    `json:"index"`
	Message *struct {
		Usage struct {
			InputTokens int `json:"input_tokens"`
		} `json:"usage"`
	} `json:"message"`
	ContentBlock struct {
		Type string `json:"type"`
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"content_block"`
	Delta struct {
		Type        string `json:"type"`
		Text        string `json:"text"`
		PartialJSON string `json:"partial_json"`
		Thinking    string `json:"thinking"`
		StopReason  string `json:"stop_reason"`
	} `json:"delta"`
	Usage *struct {
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}
