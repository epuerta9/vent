package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/epuerta/vent/internal/engine"
	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
	"github.com/nats-io/nats.go"
)

// TestEchoOverNetwork is the multiplayer proof: a function registered on one
// client connection is reachable from a different connection over the bus —
// exactly cross-process dispatch. The engine listens on a real socket, a second
// independent client (simulating a remote worker process) connects and registers
// fn.tool.echo, and the engine's own bus triggers it and reads back the result.
func TestEchoOverNetwork(t *testing.T) {
	ctx := context.Background()

	eng, err := engine.Start(ctx, engine.Options{
		Listen:   "127.0.0.1:14222",
		StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("engine start: %v", err)
	}
	defer eng.Close()

	// A second, independent client connection — a stand-in for a remote process.
	nc, err := nats.Connect(eng.ClientURL())
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer nc.Close()

	b2, err := bus.Connect(nc)
	if err != nil {
		t.Fatalf("client bus connect: %v", err)
	}
	if err := registerEcho(ctx, b2); err != nil {
		t.Fatalf("register echo: %v", err)
	}

	// Give the registration a moment to land on the server.
	time.Sleep(200 * time.Millisecond)

	b, err := eng.Bus()
	if err != nil {
		t.Fatalf("engine bus: %v", err)
	}

	var resp types.ToolResponse
	if err := b.Trigger(ctx, bus.ToolSubject("echo"), types.ToolRequest{
		ToolCallID: "t1",
		SessionID:  "s1",
		Arguments:  json.RawMessage(`{"text":"hello multiplayer"}`),
	}, &resp); err != nil {
		t.Fatalf("trigger echo: %v", err)
	}

	if len(resp.Content) == 0 {
		t.Fatalf("empty response content")
	}
	if got := resp.Content[0].Text; got != "hello multiplayer" {
		t.Fatalf("echo mismatch: got %q want %q", got, "hello multiplayer")
	}
}
