// Package main is a standalone vent tool worker that lives in its own process.
//
// It is proof of the harness's core promise: anyone can write a worker, connect
// to the engine over the network, register their own function subjects, and the
// orchestrator dispatches to them exactly as it would an in-tree worker. The
// only contract is the JSON shapes in pkg/types travelling over pkg/bus
// subjects — nothing else couples this process to the engine.
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// registerEcho advertises the "echo" tool into the tools KV bucket and registers
// its fn.tool.echo handler on the bus. It is non-blocking: the registration
// lives for the lifetime of the connection b was built from.
func registerEcho(ctx context.Context, b *bus.Bus) error {
	spec := types.ToolSpec{
		Name:        "echo",
		Label:       "Echo",
		Description: "Echo back the given text.",
		Schema: json.RawMessage(
			`{"type":"object","properties":{"text":{"type":"string","description":"text to echo back"}},"required":["text"]}`,
		),
	}
	if err := b.PutJSON(ctx, bus.BucketTools, spec.Name, spec); err != nil {
		return fmt.Errorf("advertise echo: %w", err)
	}

	if _, err := b.Register(bus.ToolSubject("echo"), func(ctx context.Context, data []byte) (any, error) {
		var req types.ToolRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return types.ToolResponse{
				Content: []types.ContentBlock{{Type: "text", Text: fmt.Sprintf("invalid request: %v", err)}},
				IsError: true,
			}, nil
		}
		var args struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(req.Arguments, &args); err != nil {
			return types.ToolResponse{
				Content: []types.ContentBlock{{Type: "text", Text: fmt.Sprintf("invalid arguments: %v", err)}},
				IsError: true,
			}, nil
		}
		return types.ToolResponse{
			Content: []types.ContentBlock{{Type: "text", Text: args.Text}},
		}, nil
	}); err != nil {
		return fmt.Errorf("register echo: %w", err)
	}
	return nil
}
