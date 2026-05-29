// Package approval is the human-in-the-loop gate worker.
//
// It registers fn.approval.resolve and persists the human's decision into the
// approvals KV bucket. The turn orchestrator polls/watches that bucket to wake
// up a turn that is parked in PhaseAwaitingApproval. Any front-end (Slack,
// console, web UI) drives the gate by calling this same subject with a
// types.ApprovalResolve payload.
package approval

import (
	"context"
	"encoding/json"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// Start registers the approval-resolve subject on the bus. It is non-blocking:
// the registration lives for the lifetime of the connection.
func Start(ctx context.Context, b *bus.Bus) error {
	_, err := b.Register(bus.SubjApprovalResolve, func(ctx context.Context, data []byte) (any, error) {
		var r types.ApprovalResolve
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, err
		}

		// Key the decision exactly as the orchestrator reads it.
		key := r.SessionID + "." + r.FunctionCallID
		decision := struct {
			Decision string `json:"decision"`
			Reason   string `json:"reason"`
		}{r.Decision, r.Reason}
		if err := b.PutJSON(ctx, bus.BucketApprovals, key, decision); err != nil {
			return nil, err
		}

		return struct {
			Resolved bool `json:"resolved"`
		}{true}, nil
	})
	return err
}
