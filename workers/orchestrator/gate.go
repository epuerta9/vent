package orchestrator

import (
	"context"
	"time"

	"github.com/epuerta/vent/pkg/agentloop"
	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// approvalPollInterval is how often we re-check the approvals bucket while
// parked in PhaseAwaitingApproval; approvalDeadline caps the wait.
const (
	approvalPollInterval = 500 * time.Millisecond
	approvalDeadline     = 5 * time.Minute
)

// makeExecute dispatches a tool call to its worker over fn.tool.<name>.
func makeExecute(b *bus.Bus, req types.RunRequest, st *types.TurnState) agentloop.Execute {
	return func(ctx context.Context, call types.ContentBlock) (types.ToolResponse, error) {
		st.Phase = types.PhaseFunctionExecute
		persist(b, st)

		var resp types.ToolResponse
		err := b.Trigger(ctx, bus.ToolSubject(call.Name), types.ToolRequest{
			ToolCallID: call.ID,
			SessionID:  req.SessionID,
			Arguments:  call.Arguments,
		}, &resp)
		if err != nil {
			return types.ToolResponse{
				Content: []types.ContentBlock{{Type: "text", Text: err.Error()}},
				IsError: true,
			}, nil
		}
		return resp, nil
	}
}

// makeBefore is the policy + approval gate. It fails closed: any policy-engine
// error blocks the call.
func makeBefore(b *bus.Bus, req types.RunRequest, st *types.TurnState) func(context.Context, types.ContentBlock) (agentloop.BeforeResult, error) {
	return func(ctx context.Context, call types.ContentBlock) (agentloop.BeforeResult, error) {
		var result types.PolicyResult
		if err := b.Trigger(ctx, bus.SubjPolicyCheck, types.PolicyRequest{
			SessionID:  req.SessionID,
			ToolCallID: call.ID,
			FunctionID: call.Name,
			Arguments:  call.Arguments,
		}, &result); err != nil {
			return agentloop.BeforeResult{Block: true, Reason: "policy gate unavailable"}, nil
		}

		switch result.Decision {
		case types.PolicyAllow:
			return agentloop.BeforeResult{}, nil
		case types.PolicyDeny:
			return agentloop.BeforeResult{Block: true, Reason: result.Reason}, nil
		case types.PolicyNeedsApproval:
			return awaitApproval(b, req, st, call)
		default:
			// Unknown decision: fail closed.
			return agentloop.BeforeResult{Block: true, Reason: "unknown policy decision"}, nil
		}
	}
}

// awaitApproval parks the turn until a human resolves the approvals KV key, or
// the deadline elapses (treated as a block).
func awaitApproval(b *bus.Bus, req types.RunRequest, st *types.TurnState, call types.ContentBlock) (agentloop.BeforeResult, error) {
	st.Phase = types.PhaseAwaitingApproval
	persist(b, st)

	key := req.SessionID + "." + call.ID
	deadline := time.NewTimer(approvalDeadline)
	defer deadline.Stop()
	ticker := time.NewTicker(approvalPollInterval)
	defer ticker.Stop()

	resolve := func(res agentloop.BeforeResult) (agentloop.BeforeResult, error) {
		st.Phase = types.PhaseFunctionExecute
		persist(b, st)
		return res, nil
	}

	ctx := context.Background()
	for {
		var decision struct {
			Decision string `json:"decision"`
			Reason   string `json:"reason"`
		}
		if found, err := b.GetJSON(ctx, bus.BucketApprovals, key, &decision); err == nil && found {
			switch decision.Decision {
			case "allow":
				return resolve(agentloop.BeforeResult{})
			default: // deny | aborted | anything else
				reason := decision.Reason
				if reason == "" {
					reason = "approval denied"
				}
				return resolve(agentloop.BeforeResult{Block: true, Reason: reason})
			}
		}

		select {
		case <-deadline.C:
			return resolve(agentloop.BeforeResult{Block: true, Reason: "approval timed out"})
		case <-ticker.C:
		}
	}
}

// makeAfter fires the post-tool hook fanout (best-effort).
func makeAfter(b *bus.Bus, req types.RunRequest) func(context.Context, types.ContentBlock, types.ToolResponse) (*agentloop.AfterResult, error) {
	return func(ctx context.Context, call types.ContentBlock, res types.ToolResponse) (*agentloop.AfterResult, error) {
		_ = b.Trigger(ctx, bus.SubjHookPublish, struct {
			SessionID string `json:"sessionId"`
			Phase     string `json:"phase"`
			ToolName  string `json:"toolName"`
		}{req.SessionID, "after", call.Name}, nil)
		return nil, nil
	}
}
