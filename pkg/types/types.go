// Package types defines the wire contract shared by every vent worker.
//
// The whole harness is decoupled by these types travelling over NATS as JSON.
// A worker is just a process that registers function subjects and/or subscribes
// to event streams; the only thing it must agree on with its neighbours is the
// shapes in this file. Replace any worker with one that speaks the same shapes
// and the rest of the stack does not change.
package types

import "encoding/json"

// ---------------------------------------------------------------------------
// Messages
//
// Ported from pi's AgentMessage union. Go has no discriminated unions, so a
// message is one struct whose Role selects which fields are meaningful.
// ---------------------------------------------------------------------------

// Role identifies the speaker of a Message.
type Role string

const (
	RoleUser       Role = "user"
	RoleAssistant  Role = "assistant"
	RoleToolResult Role = "toolResult"
)

// StopReason explains why an assistant turn ended.
type StopReason string

const (
	StopEnd       StopReason = "stop"
	StopToolUse   StopReason = "toolUse"
	StopMaxTokens StopReason = "maxTokens"
	StopError     StopReason = "error"
	StopAborted   StopReason = "aborted"
)

// Message is a single entry in the conversation transcript.
type Message struct {
	Role    Role           `json:"role"`
	Content []ContentBlock `json:"content"`

	// Assistant-only fields.
	StopReason   StopReason `json:"stopReason,omitempty"`
	ErrorMessage string     `json:"errorMessage,omitempty"`
	Usage        *Usage     `json:"usage,omitempty"`

	// ToolResult-only fields.
	ToolCallID string `json:"toolCallId,omitempty"`
	ToolName   string `json:"toolName,omitempty"`
	IsError    bool   `json:"isError,omitempty"`

	Timestamp int64 `json:"timestamp,omitempty"`
}

// ContentBlock is one piece of a message. Type selects the active fields.
type ContentBlock struct {
	Type string `json:"type"` // text | thinking | toolCall | image

	// text / thinking
	Text string `json:"text,omitempty"`

	// toolCall
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`

	// image
	MimeType string `json:"mimeType,omitempty"`
	Data     string `json:"data,omitempty"` // base64
}

// ToolCalls returns the toolCall blocks in an assistant message.
func (m Message) ToolCalls() []ContentBlock {
	var calls []ContentBlock
	for _, c := range m.Content {
		if c.Type == "toolCall" {
			calls = append(calls, c)
		}
	}
	return calls
}

// Text concatenates all text blocks in the message.
func (m Message) TextContent() string {
	out := ""
	for _, c := range m.Content {
		if c.Type == "text" {
			out += c.Text
		}
	}
	return out
}

// Usage records token consumption for budgeting and observability.
type Usage struct {
	InputTokens       int     `json:"inputTokens"`
	OutputTokens      int     `json:"outputTokens"`
	CacheReadTokens   int     `json:"cacheReadTokens,omitempty"`
	CacheWriteTokens  int     `json:"cacheWriteTokens,omitempty"`
	CostUSD           float64 `json:"costUsd,omitempty"`
}

// ---------------------------------------------------------------------------
// Tools
//
// A tool is itself a worker that registers fn.tool.<name>. On startup it
// advertises its schema into the `tools` KV bucket (the local analogue of an
// iii skill body). The orchestrator reads that bucket to tell the model which
// tools exist, then dispatches calls back over the bus.
// ---------------------------------------------------------------------------

// ToolSpec is the advertisement a tool worker writes to the `tools` KV bucket.
type ToolSpec struct {
	Name        string          `json:"name"`
	Label       string          `json:"label"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"` // JSON Schema for the parameters
}

// ToolRequest is the payload sent to fn.tool.<name>.
type ToolRequest struct {
	ToolCallID string          `json:"toolCallId"`
	SessionID  string          `json:"sessionId"`
	Arguments  json.RawMessage `json:"arguments"`
}

// ToolResponse is what a tool worker returns.
type ToolResponse struct {
	Content []ContentBlock  `json:"content"`
	Details json.RawMessage `json:"details,omitempty"`
	IsError bool            `json:"isError,omitempty"`
}

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

// Model describes a model the harness can drive.
type Model struct {
	ID             string `json:"id"`
	Provider       string `json:"provider"`
	ContextWindow  int    `json:"contextWindow"`
	MaxTokens      int    `json:"maxTokens"`
	SupportsTools  bool   `json:"supportsTools"`
	SupportsVision bool   `json:"supportsVision"`
	Reasoning      bool   `json:"reasoning"`
}

// ---------------------------------------------------------------------------
// Provider streaming
//
// The orchestrator asks a provider worker to run one assistant turn. The
// provider publishes incremental deltas to StreamSubject (so the UI sees
// tokens live) and returns the finalised AssistantMessage as the reply.
// ---------------------------------------------------------------------------

// StreamRequest is the payload sent to fn.provider.<name>.stream.
type StreamRequest struct {
	SessionID    string    `json:"sessionId"`
	MessageID    string    `json:"messageId"`
	Model        Model     `json:"model"`
	SystemPrompt string    `json:"systemPrompt"`
	Messages     []Message `json:"messages"`
	Tools        []ToolSpec `json:"tools"`
	Thinking     string    `json:"thinking,omitempty"`
	// StreamSubject is a core-NATS subject the provider publishes Delta events
	// to while generating. Empty means the caller does not want live deltas.
	StreamSubject string `json:"streamSubject,omitempty"`
}

// Delta is one incremental streaming event published to StreamRequest.StreamSubject.
type Delta struct {
	Kind  string `json:"kind"` // text | thinking | toolcall
	Text  string `json:"text,omitempty"`
	Index int    `json:"index,omitempty"`
}

// StreamResponse is the reply from a provider worker: the completed turn.
type StreamResponse struct {
	Message Message `json:"message"`
}

// ---------------------------------------------------------------------------
// Policy & approvals
// ---------------------------------------------------------------------------

// PolicyDecision is the outcome of fn.policy.check_permissions.
type PolicyDecision string

const (
	PolicyAllow         PolicyDecision = "allow"
	PolicyDeny          PolicyDecision = "deny"
	PolicyNeedsApproval PolicyDecision = "needs_approval"
)

// PolicyRequest asks whether a tool call may run.
type PolicyRequest struct {
	SessionID  string          `json:"sessionId"`
	ToolCallID string          `json:"toolCallId"`
	FunctionID string          `json:"functionId"` // the tool name
	Arguments  json.RawMessage `json:"arguments"`
}

// PolicyResult is the verdict.
type PolicyResult struct {
	Decision         PolicyDecision `json:"decision"`
	RuleID           string         `json:"ruleId,omitempty"`
	MatchedConstraint string        `json:"matchedConstraint,omitempty"`
	Reason           string         `json:"reason,omitempty"`
}

// ApprovalResolve is the payload sent to fn.approval.resolve.
type ApprovalResolve struct {
	SessionID      string `json:"sessionId"`
	FunctionCallID string `json:"functionCallId"`
	Decision       string `json:"decision"` // allow | deny | aborted
	Reason         string `json:"reason,omitempty"`
}

// ---------------------------------------------------------------------------
// Budget
// ---------------------------------------------------------------------------

// BudgetRecord reports spend after a model call.
type BudgetRecord struct {
	SessionID string `json:"sessionId"`
	Workspace string `json:"workspace,omitempty"`
	Usage     Usage  `json:"usage"`
	Model     string `json:"model"`
}

// BudgetCheck asks whether a session/workspace is still within budget.
type BudgetCheck struct {
	SessionID string `json:"sessionId"`
	Workspace string `json:"workspace,omitempty"`
}

// BudgetStatus is the reply to a BudgetCheck.
type BudgetStatus struct {
	WithinBudget bool    `json:"withinBudget"`
	SpentUSD     float64 `json:"spentUsd"`
	LimitUSD     float64 `json:"limitUsd"`
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

// TokenRequest asks the credential vault for a provider token.
type TokenRequest struct {
	Provider string `json:"provider"`
}

// TokenResponse carries the resolved token.
type TokenResponse struct {
	Token string `json:"token"`
}

// ---------------------------------------------------------------------------
// Skills / directory
// ---------------------------------------------------------------------------

// Skill is a per-function usage document, served by the directory worker.
type Skill struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Body      string `json:"body"`
}

// SkillsGetRequest fetches one skill body.
type SkillsGetRequest struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// ---------------------------------------------------------------------------
// Turn lifecycle
// ---------------------------------------------------------------------------

// Mode selects the system-prompt persona / autonomy level.
type Mode string

const (
	ModeAgent Mode = "agent"
	ModePlan  Mode = "plan"
	ModeAsk   Mode = "ask"
)

// RunRequest is the payload a client POSTs to start a turn (fn.run.start,
// usually via fn.harness.trigger).
type RunRequest struct {
	SessionID    string    `json:"sessionId"`
	MessageID    string    `json:"messageId"`
	Mode         Mode      `json:"mode,omitempty"`
	Provider     string    `json:"provider,omitempty"`
	ModelID      string    `json:"modelId,omitempty"`
	SystemPrompt string    `json:"systemPrompt,omitempty"` // override; skips assembly
	Prompt       []Message `json:"prompt"`
	MaxTurns     int       `json:"maxTurns,omitempty"`
	Workspace    string    `json:"workspace,omitempty"`
}

// RunResponse acknowledges a started run.
type RunResponse struct {
	SessionID string `json:"sessionId"`
	Accepted  bool   `json:"accepted"`
}

// Phase is a turn-orchestrator FSM state, persisted in the turn_state KV bucket.
type Phase string

const (
	PhaseProvisioning      Phase = "provisioning"
	PhaseAssistant         Phase = "assistant_streaming"
	PhaseFunctionExecute   Phase = "function_execute"
	PhaseAwaitingApproval  Phase = "function_awaiting_approval"
	PhaseStopped           Phase = "stopped"
	PhaseFailed            Phase = "failed"
)

// TurnState is the durable record the orchestrator advances. It lives in the
// turn_state KV bucket keyed by session id, so a turn can be resumed or
// inspected by any worker.
type TurnState struct {
	SessionID    string    `json:"sessionId"`
	MessageID    string    `json:"messageId"`
	Phase        Phase     `json:"phase"`
	Mode         Mode      `json:"mode"`
	Provider     string    `json:"provider"`
	Model        Model     `json:"model"`
	SystemPrompt string    `json:"systemPrompt"`
	Workspace    string    `json:"workspace,omitempty"`
	Messages     []Message `json:"messages"`
	Turn         int       `json:"turn"`
	MaxTurns     int       `json:"maxTurns"`
	Error        string    `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// Events
//
// Ported from pi's AgentEvent union. Every worker emits these onto the
// AGENT_EVENTS JetStream stream (subject evt.<sessionId>) and the events
// gateway fans them out to subscribed UIs.
// ---------------------------------------------------------------------------

// EventType enumerates the agent event kinds.
type EventType string

const (
	EvAgentStart        EventType = "agent_start"
	EvAgentEnd          EventType = "agent_end"
	EvTurnStart         EventType = "turn_start"
	EvTurnEnd           EventType = "turn_end"
	EvMessageStart      EventType = "message_start"
	EvMessageUpdate     EventType = "message_update"
	EvMessageEnd        EventType = "message_end"
	EvToolStart         EventType = "tool_execution_start"
	EvToolUpdate        EventType = "tool_execution_update"
	EvToolEnd           EventType = "tool_execution_end"
)

// Event is a single agent event. Type selects which fields are meaningful.
type Event struct {
	Type      EventType `json:"type"`
	SessionID string    `json:"sessionId"`

	Message     *Message  `json:"message,omitempty"`
	Messages    []Message `json:"messages,omitempty"` // agent_end
	ToolResults []Message `json:"toolResults,omitempty"` // turn_end

	// tool execution
	ToolCallID string          `json:"toolCallId,omitempty"`
	ToolName   string          `json:"toolName,omitempty"`
	Args       json.RawMessage `json:"args,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	IsError    bool            `json:"isError,omitempty"`

	// streaming
	Delta *Delta `json:"delta,omitempty"`
}
