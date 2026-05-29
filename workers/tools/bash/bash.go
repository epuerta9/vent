// Package bash is the bash tool worker. It advertises a "bash" tool into the
// tools KV bucket and implements fn.tool.bash, running a shell command and
// returning its combined stdout/stderr.
package bash

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// Start advertises the bash tool spec into BucketTools and registers the
// fn.tool.bash function subject. It is non-blocking.
func Start(ctx context.Context, b *bus.Bus) error {
	spec := types.ToolSpec{
		Name:        "bash",
		Label:       "Bash",
		Description: "Run a shell command and return combined stdout/stderr.",
		Schema:      json.RawMessage(`{"type":"object","properties":{"command":{"type":"string","description":"The shell command to run"}},"required":["command"]}`),
	}
	if err := b.PutJSON(ctx, bus.BucketTools, "bash", spec); err != nil {
		return err
	}

	_, err := b.Register(bus.ToolSubject("bash"), handle)
	return err
}

// handle runs the requested command. A failed command is never surfaced as a
// Go error; the failure is encoded in the ToolResponse (IsError + output).
func handle(ctx context.Context, data []byte) (any, error) {
	var req types.ToolRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}

	var args struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(req.Arguments, &args); err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, "bash", "-lc", args.Command)
	if wd := os.Getenv("VENT_WORKDIR"); wd != "" {
		cmd.Dir = wd
	}
	out, err := cmd.CombinedOutput()

	return types.ToolResponse{
		Content: []types.ContentBlock{{Type: "text", Text: string(out)}},
		IsError: err != nil,
	}, nil
}
