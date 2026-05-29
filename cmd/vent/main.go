// Command vent is the composite entrypoint for the harness: it boots the
// embedded engine and every worker in one process, then exposes three
// subcommands.
//
//	vent doctor   prove the wiring offline (no API key) and exit
//	vent serve    run the harness and the events gateway until SIGINT
//	vent run …    run one turn against the prompt and stream a live trace
//
// The whole harness is "just workers on a bus": main wires them together but
// owns none of their logic.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/epuerta/vent/internal/engine"
	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/obs"
	"github.com/epuerta/vent/pkg/types"

	"github.com/epuerta/vent/workers/approval"
	"github.com/epuerta/vent/workers/auth"
	"github.com/epuerta/vent/workers/budget"
	"github.com/epuerta/vent/workers/compaction"
	"github.com/epuerta/vent/workers/directory"
	"github.com/epuerta/vent/workers/events"
	"github.com/epuerta/vent/workers/hookfanout"
	"github.com/epuerta/vent/workers/models"
	"github.com/epuerta/vent/workers/orchestrator"
	"github.com/epuerta/vent/workers/policy"
	anthropic "github.com/epuerta/vent/workers/provider/anthropic"
	openai "github.com/epuerta/vent/workers/provider/openai"
	"github.com/epuerta/vent/workers/session"
	bashtool "github.com/epuerta/vent/workers/tools/bash"
	fstool "github.com/epuerta/vent/workers/tools/fs"
)

func main() {
	// Install the OTel propagator (and a stdout exporter when VENT_TRACE=stdout)
	// so a turn shows up as one trace across every worker. Flush before exit.
	shutdown, _ := obs.Init("vent")
	os.Exit(runMain(shutdown))
}

func runMain(shutdown func(context.Context) error) int {
	args := os.Args[1:]
	cmd := "doctor"
	if len(args) > 0 {
		cmd = args[0]
		args = args[1:]
	}

	var err error
	switch cmd {
	case "doctor":
		err = doctor()
	case "serve":
		err = serve()
	case "run":
		err = run(args)
	default:
		fmt.Fprintf(os.Stderr, "vent: unknown command %q\n", cmd)
		fmt.Fprintln(os.Stderr, "usage: vent [doctor|serve|run <prompt...>]")
		_ = shutdown(context.Background())
		return 2
	}
	_ = shutdown(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "vent: %v\n", err)
		return 1
	}
	return 0
}

// startAll calls Start on every worker and returns the first error. Workers are
// non-blocking: Start just registers subjects / subscriptions on the bus.
func startAll(ctx context.Context, b *bus.Bus) error {
	type worker struct {
		name  string
		start func(context.Context, *bus.Bus) error
	}
	workers := []worker{
		{"auth", auth.Start},
		{"models", models.Start},
		{"provider/anthropic", anthropic.Start},
		{"provider/openai", openai.Start},
		{"policy", policy.Start},
		{"approval", approval.Start},
		{"budget", budget.Start},
		{"directory", directory.Start},
		{"hookfanout", hookfanout.Start},
		{"session", session.Start},
		{"compaction", compaction.Start},
		{"events", events.Start},
		{"tools/bash", bashtool.Start},
		{"tools/fs", fstool.Start},
		{"orchestrator", orchestrator.Start},
	}
	for _, w := range workers {
		if err := w.start(ctx, b); err != nil {
			return fmt.Errorf("start worker %s: %w", w.name, err)
		}
	}
	return nil
}

// boot starts the engine with a temp store and brings up every worker. When
// listen is non-empty the engine also opens a TCP socket so external workers
// (other processes, other machines) can connect via nats://<addr> and register
// their own functions. The caller owns the returned engine and must Close it.
func boot(ctx context.Context, listen string) (*engine.Engine, *bus.Bus, error) {
	storeDir, err := os.MkdirTemp("", "vent-store-*")
	if err != nil {
		return nil, nil, fmt.Errorf("temp store dir: %w", err)
	}
	eng, err := engine.Start(ctx, engine.Options{
		StoreDir:      storeDir,
		ServerName:    os.Getenv("VENT_SERVER_NAME"),
		Listen:        listen,
		ClusterName:   os.Getenv("VENT_CLUSTER_NAME"),
		ClusterListen: os.Getenv("VENT_CLUSTER_LISTEN"),
		Routes:        splitRoutes(os.Getenv("VENT_CLUSTER_ROUTES")),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("engine start: %w", err)
	}
	b, err := eng.Bus()
	if err != nil {
		eng.Close()
		return nil, nil, fmt.Errorf("bus connect: %w", err)
	}
	if err := startAll(ctx, b); err != nil {
		eng.Close()
		return nil, nil, err
	}
	return eng, b, nil
}

// doctor proves the wiring is live entirely offline: no ANTHROPIC_API_KEY is
// needed because we only exercise catalogue/registry subjects, never a model.
func doctor() error {
	ctx := context.Background()
	eng, b, err := boot(ctx, "")
	if err != nil {
		return err
	}
	defer eng.Close()

	// Give every worker's Register/Subscribe a moment to land.
	time.Sleep(300 * time.Millisecond)

	// 1. Models catalogue.
	var modelList []types.Model
	if err := b.Trigger(ctx, bus.SubjModelsList, nil, &modelList); err != nil {
		return fmt.Errorf("models.list: %w", err)
	}
	fmt.Println("models:")
	for _, m := range modelList {
		fmt.Printf("  - %s (%s)\n", m.ID, m.Provider)
	}

	// 2. Advertised tools, read straight from the tools KV bucket.
	toolNames, err := listToolNames(ctx, b)
	if err != nil {
		return fmt.Errorf("tools bucket: %w", err)
	}
	fmt.Println("tools:")
	for _, name := range toolNames {
		fmt.Printf("  - %s\n", name)
	}

	// 3. Skills directory.
	var skillList []types.Skill
	if err := b.Trigger(ctx, bus.SubjSkillsList, nil, &skillList); err != nil {
		return fmt.Errorf("skills.list: %w", err)
	}
	fmt.Println("skills:")
	for _, s := range skillList {
		fmt.Printf("  - %s::%s\n", s.Namespace, s.Name)
	}

	fmt.Printf("vent: harness OK (%d workers, %d tools, %d models)\n",
		workerCount, len(toolNames), len(modelList))
	return nil
}

// workerCount is the number of workers startAll brings up (kept in sync with
// the slice in startAll).
const workerCount = 15

// listToolNames reads every key in the tools KV bucket and returns the
// advertised tool names.
func listToolNames(ctx context.Context, b *bus.Bus) ([]string, error) {
	kv, err := b.KV(ctx, bus.BucketTools)
	if err != nil {
		return nil, err
	}
	keys, err := kv.Keys(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		var spec types.ToolSpec
		if found, gErr := b.GetJSON(ctx, bus.BucketTools, key, &spec); gErr == nil && found && spec.Name != "" {
			names = append(names, spec.Name)
		} else {
			names = append(names, key)
		}
	}
	return names, nil
}

// serve runs the harness and the events gateway until SIGINT/SIGTERM.
func serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Open a client socket so external workers can join the bus. Default to a
	// local port; override with VENT_NATS_LISTEN (e.g. "0.0.0.0:4222").
	listen := os.Getenv("VENT_NATS_LISTEN")
	if listen == "" {
		listen = "127.0.0.1:4222"
	}

	eng, _, err := boot(ctx, listen)
	if err != nil {
		return err
	}
	defer eng.Close()

	addr := os.Getenv("VENT_EVENTS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8088"
	}
	fmt.Printf("vent: serving\n")
	fmt.Printf("  bus      %s   (external workers: nats.Connect then bus.Connect, register fn.tool.<name>)\n", eng.ClientURL())
	fmt.Printf("  events   http://%s/events\n", addr)
	fmt.Println("vent: press Ctrl-C to stop")

	<-ctx.Done()
	fmt.Println("\nvent: shutting down")
	return nil
}

// splitRoutes parses a comma-separated list of nats-route:// URLs for clustering.
func splitRoutes(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// run executes a single agent turn against the joined prompt args and prints a
// readable live trace, blocking until the turn ends or a timeout fires.
func run(args []string) error {
	prompt := strings.TrimSpace(strings.Join(args, " "))
	if prompt == "" {
		return fmt.Errorf("run needs a prompt: vent run <text...>")
	}

	ctx := context.Background()
	eng, b, err := boot(ctx, "")
	if err != nil {
		return err
	}
	defer eng.Close()

	time.Sleep(300 * time.Millisecond)

	sessionID := "s-" + time.Now().Format("20060102-150405.000")
	messageID := "m-" + time.Now().Format("150405.000")

	provider := os.Getenv("VENT_PROVIDER")
	if provider == "" {
		provider = "anthropic"
	}
	keyEnv := map[string]string{"anthropic": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY"}[provider]
	if keyEnv != "" && os.Getenv(keyEnv) == "" {
		fmt.Printf("vent: note — 'run' drives the %q provider and needs %s set.\n", provider, keyEnv)
		fmt.Println("vent: without it the turn will fail; try 'vent doctor' to prove the wiring offline.")
	}

	// Subscribe to events before triggering so we miss nothing.
	done := make(chan struct{})
	var once sync.Once
	var inText bool
	unsub, err := b.SubscribeEvents(sessionID, func(ev types.Event) {
		switch ev.Type {
		case types.EvAgentStart:
			fmt.Printf("\n[agent start] %s\n", ev.SessionID)
		case types.EvTurnStart:
			fmt.Println("[turn start]")
		case types.EvTurnEnd:
			if inText {
				fmt.Println()
				inText = false
			}
			fmt.Println("[turn end]")
		case types.EvMessageUpdate:
			if ev.Delta != nil && ev.Delta.Kind == "text" && ev.Delta.Text != "" {
				fmt.Print(ev.Delta.Text)
				inText = true
			}
		case types.EvMessageEnd:
			if ev.Message != nil && ev.Message.Role == types.RoleAssistant {
				if ev.Message.StopReason == types.StopError {
					fmt.Printf("[error] %s\n", ev.Message.ErrorMessage)
				} else if txt := ev.Message.TextContent(); txt != "" && !inText {
					// Provider didn't stream deltas; surface the final answer.
					fmt.Printf("[assistant] %s\n", txt)
				}
			}
			inText = false
		case types.EvToolStart:
			if inText {
				fmt.Println()
				inText = false
			}
			fmt.Printf("[tool start] %s\n", ev.ToolName)
		case types.EvToolEnd:
			status := "ok"
			if ev.IsError {
				status = "error"
			}
			fmt.Printf("[tool end] %s (%s)\n", ev.ToolName, status)
		case types.EvAgentEnd:
			if inText {
				fmt.Println()
				inText = false
			}
			fmt.Println("[agent end]")
			once.Do(func() { close(done) })
		}
	})
	if err != nil {
		return fmt.Errorf("subscribe events: %w", err)
	}
	defer unsub()

	req := types.RunRequest{
		SessionID: sessionID,
		MessageID: messageID,
		Mode:      types.ModeAgent,
		Provider:  os.Getenv("VENT_PROVIDER"), // e.g. "openai"; empty -> orchestrator default (anthropic)
		ModelID:   os.Getenv("VENT_MODEL"),    // e.g. "gpt-5.5"
		Prompt: []types.Message{{
			Role:    types.RoleUser,
			Content: []types.ContentBlock{{Type: "text", Text: prompt}},
		}},
	}
	var resp types.RunResponse
	if err := b.Trigger(ctx, bus.SubjHarnessTrigger, req, &resp); err != nil {
		return fmt.Errorf("harness.trigger: %w", err)
	}

	select {
	case <-done:
		fmt.Println("vent: run complete")
		return nil
	case <-time.After(5 * time.Minute):
		return fmt.Errorf("run timed out after 5m waiting for agent_end (session %s)", sessionID)
	}
}
