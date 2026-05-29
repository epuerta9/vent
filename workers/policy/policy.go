// Package policy is the default vent policy engine. It registers
// fn.policy.check_permissions and classifies a tool call by its name into an
// allow / needs-approval verdict. This is the layer an OPA or Cedar worker
// would replace: swap it out by registering the same subject. Fail-closed
// semantics (what to do on deny/error) live in the orchestrator, not here —
// this worker only returns the verdict.
package policy

import (
	"context"
	"encoding/json"
	"os"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// readOnlyTools allow immediately: they cannot mutate state.
var readOnlyTools = map[string]bool{
	"read": true,
	"ls":   true,
	"list": true,
	"glob": true,
	"grep": true,
	"cat":  true,
}

// dangerousTools mutate the workspace or run arbitrary code and require a human.
var dangerousTools = map[string]bool{
	"bash":  true,
	"write": true,
	"edit":  true,
	"rm":    true,
	"apply": true,
}

// Start registers the policy engine on the bus. It is non-blocking: the
// registration lives for the lifetime of the connection.
func Start(ctx context.Context, b *bus.Bus) error {
	_, err := b.Register(bus.SubjPolicyCheck, handle)
	return err
}

// handle unmarshals a PolicyRequest and returns the PolicyResult verdict.
func handle(ctx context.Context, data []byte) (any, error) {
	var req types.PolicyRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}

	// Escape hatch for autonomous runs: allow everything.
	if os.Getenv("VENT_POLICY") == "allow_all" {
		return types.PolicyResult{
			Decision: types.PolicyAllow,
			RuleID:   "allow_all",
			Reason:   "VENT_POLICY=allow_all",
		}, nil
	}

	switch {
	case dangerousTools[req.FunctionID]:
		return types.PolicyResult{
			Decision: types.PolicyNeedsApproval,
			RuleID:   "mutating_tool",
			Reason:   "tool " + req.FunctionID + " can mutate state or run code; requires human approval",
		}, nil
	case readOnlyTools[req.FunctionID]:
		return types.PolicyResult{
			Decision: types.PolicyAllow,
			RuleID:   "read_only",
		}, nil
	default:
		return types.PolicyResult{
			Decision: types.PolicyAllow,
			RuleID:   "default_allow",
		}, nil
	}
}
