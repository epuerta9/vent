// Package budget is the spend tracker worker. It accumulates per-key cost and
// token usage in the budgets KV bucket and answers whether a session or
// workspace is still within its configured limit.
package budget

import (
	"context"
	"encoding/json"
	"os"
	"strconv"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// defaultLimitUSD is used when VENT_BUDGET_USD is unset or unparseable.
const defaultLimitUSD = 1000.0

// bucketTotals is the accumulated spend stored under each budget key.
type bucketTotals struct {
	SpentUSD     float64 `json:"spentUsd"`
	InputTokens  int     `json:"inputTokens"`
	OutputTokens int     `json:"outputTokens"`
}

// budgetKey selects the workspace if set, else falls back to the session id.
func budgetKey(workspace, sessionID string) string {
	if workspace != "" {
		return workspace
	}
	return sessionID
}

// limitUSD reads the budget ceiling from VENT_BUDGET_USD, defaulting if absent.
func limitUSD() float64 {
	if v := os.Getenv("VENT_BUDGET_USD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return defaultLimitUSD
}

// Start registers the budget record and check subjects on the bus. It is
// non-blocking: the registrations live for the lifetime of the connection.
func Start(ctx context.Context, b *bus.Bus) error {
	if _, err := b.Register(bus.SubjBudgetRecord, func(ctx context.Context, data []byte) (any, error) {
		var r types.BudgetRecord
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, err
		}
		key := budgetKey(r.Workspace, r.SessionID)

		// Best-effort read-modify-write of the accumulated totals.
		var t bucketTotals
		_, _ = b.GetJSON(ctx, bus.BucketBudgets, key, &t)
		t.SpentUSD += r.Usage.CostUSD
		t.InputTokens += r.Usage.InputTokens
		t.OutputTokens += r.Usage.OutputTokens
		_ = b.PutJSON(ctx, bus.BucketBudgets, key, t)

		return struct{ Ok bool }{true}, nil
	}); err != nil {
		return err
	}

	if _, err := b.Register(bus.SubjBudgetCheck, func(ctx context.Context, data []byte) (any, error) {
		var c types.BudgetCheck
		if err := json.Unmarshal(data, &c); err != nil {
			return nil, err
		}
		key := budgetKey(c.Workspace, c.SessionID)

		var t bucketTotals
		_, _ = b.GetJSON(ctx, bus.BucketBudgets, key, &t)
		limit := limitUSD()

		return types.BudgetStatus{
			WithinBudget: t.SpentUSD < limit,
			SpentUSD:     t.SpentUSD,
			LimitUSD:     limit,
		}, nil
	}); err != nil {
		return err
	}

	return nil
}
