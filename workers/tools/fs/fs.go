// Package fs is a vent tool worker exposing filesystem operations.
//
// It advertises four tools — read, ls, write, edit — into the tools KV bucket
// and registers a handler for each on its fn.tool.<name> subject. Paths are
// resolved relative to $VENT_WORKDIR when that env var is set, otherwise taken
// as given.
package fs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// Start advertises the four filesystem tools and registers their handlers. It
// is non-blocking: registrations live for the lifetime of the connection.
func Start(ctx context.Context, b *bus.Bus) error {
	tools := []struct {
		spec    types.ToolSpec
		handler bus.HandlerFunc
	}{
		{
			spec: toolSpec("read", "Read file",
				"Read a file and return its contents as text.",
				`{"path":{"type":"string","description":"path to the file"}}`,
				`["path"]`),
			handler: handleRead,
		},
		{
			spec: toolSpec("ls", "List directory",
				"List the entries of a directory (defaults to the working directory).",
				`{"path":{"type":"string","description":"directory to list","default":"."}}`,
				`[]`),
			handler: handleLs,
		},
		{
			spec: toolSpec("write", "Write file",
				"Write content to a file, creating or truncating it.",
				`{"path":{"type":"string","description":"path to the file"},"content":{"type":"string","description":"content to write"}}`,
				`["path","content"]`),
			handler: handleWrite,
		},
		{
			spec: toolSpec("edit", "Edit file",
				"Replace the first occurrence of old with new in a file.",
				`{"path":{"type":"string","description":"path to the file"},"old":{"type":"string","description":"text to find"},"new":{"type":"string","description":"replacement text"}}`,
				`["path","old","new"]`),
			handler: handleEdit,
		},
	}

	for _, t := range tools {
		if err := b.PutJSON(ctx, bus.BucketTools, t.spec.Name, t.spec); err != nil {
			return fmt.Errorf("advertise %s: %w", t.spec.Name, err)
		}
		if _, err := b.Register(bus.ToolSubject(t.spec.Name), t.handler); err != nil {
			return fmt.Errorf("register %s: %w", t.spec.Name, err)
		}
	}
	return nil
}

// toolSpec builds a ToolSpec with an object JSON Schema from the given property
// and required JSON fragments.
func toolSpec(name, label, desc, properties, required string) types.ToolSpec {
	schema := fmt.Sprintf(`{"type":"object","properties":%s,"required":%s}`, properties, required)
	return types.ToolSpec{
		Name:        name,
		Label:       label,
		Description: desc,
		Schema:      json.RawMessage(schema),
	}
}

// resolve maps a tool path through $VENT_WORKDIR when set.
func resolve(path string) string {
	if wd := os.Getenv("VENT_WORKDIR"); wd != "" && !filepath.IsAbs(path) {
		return filepath.Join(wd, path)
	}
	return path
}

// textResp wraps text as a successful ToolResponse.
func textResp(text string) types.ToolResponse {
	return types.ToolResponse{
		Content: []types.ContentBlock{{Type: "text", Text: text}},
	}
}

// errResp wraps a message as a ToolResponse flagged IsError.
func errResp(format string, args ...any) types.ToolResponse {
	return types.ToolResponse{
		Content: []types.ContentBlock{{Type: "text", Text: fmt.Sprintf(format, args...)}},
		IsError: true,
	}
}

func handleRead(ctx context.Context, data []byte) (any, error) {
	var req types.ToolRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return errResp("invalid request: %v", err), nil
	}
	var args struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(req.Arguments, &args); err != nil {
		return errResp("invalid arguments: %v", err), nil
	}
	if args.Path == "" {
		return errResp("path is required"), nil
	}
	b, err := os.ReadFile(resolve(args.Path))
	if err != nil {
		return errResp("read %s: %v", args.Path, err), nil
	}
	return textResp(string(b)), nil
}

func handleLs(ctx context.Context, data []byte) (any, error) {
	var req types.ToolRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return errResp("invalid request: %v", err), nil
	}
	var args struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(req.Arguments, &args); err != nil {
		return errResp("invalid arguments: %v", err), nil
	}
	if args.Path == "" {
		args.Path = "."
	}
	entries, err := os.ReadDir(resolve(args.Path))
	if err != nil {
		return errResp("ls %s: %v", args.Path, err), nil
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return textResp(strings.Join(names, "\n")), nil
}

func handleWrite(ctx context.Context, data []byte) (any, error) {
	var req types.ToolRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return errResp("invalid request: %v", err), nil
	}
	var args struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(req.Arguments, &args); err != nil {
		return errResp("invalid arguments: %v", err), nil
	}
	if args.Path == "" {
		return errResp("path is required"), nil
	}
	if err := os.WriteFile(resolve(args.Path), []byte(args.Content), 0o644); err != nil {
		return errResp("write %s: %v", args.Path, err), nil
	}
	return textResp(fmt.Sprintf("wrote %d bytes", len(args.Content))), nil
}

func handleEdit(ctx context.Context, data []byte) (any, error) {
	var req types.ToolRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return errResp("invalid request: %v", err), nil
	}
	var args struct {
		Path string `json:"path"`
		Old  string `json:"old"`
		New  string `json:"new"`
	}
	if err := json.Unmarshal(req.Arguments, &args); err != nil {
		return errResp("invalid arguments: %v", err), nil
	}
	if args.Path == "" {
		return errResp("path is required"), nil
	}
	full := resolve(args.Path)
	b, err := os.ReadFile(full)
	if err != nil {
		return errResp("read %s: %v", args.Path, err), nil
	}
	content := string(b)
	if !strings.Contains(content, args.Old) {
		return errResp("old text not found in %s", args.Path), nil
	}
	updated := strings.Replace(content, args.Old, args.New, 1)
	if err := os.WriteFile(full, []byte(updated), 0o644); err != nil {
		return errResp("write %s: %v", args.Path, err), nil
	}
	return textResp("ok"), nil
}
