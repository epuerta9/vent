// Package engine boots an embedded NATS server with JetStream and provisions
// the streams, KV buckets, and object store the harness relies on. Running the
// engine in-process means the whole harness is a single Go binary with no
// external broker to operate, yet every worker still talks over the same bus
// it would use against a remote cluster.
package engine

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Engine owns the embedded server and a client connection.
type Engine struct {
	ns *server.Server
	nc *nats.Conn
}

// Options configure the embedded engine.
type Options struct {
	// StoreDir is where JetStream persists data. Empty uses an in-memory store.
	StoreDir string

	// ServerName uniquely identifies this node (required to be unique within a
	// cluster). Defaults to "vent-engine".
	ServerName string

	// Listen is the client host:port (e.g. ":4222" or "0.0.0.0:4222"). When
	// empty the engine runs in-process only (no socket) and external workers
	// cannot connect. Set it to let workers on other processes/machines join
	// the bus via nats://<addr>.
	Listen string

	// ClusterName, ClusterListen and Routes form a NATS cluster: run several
	// `vent serve` engines, point each one's Routes at the others' ClusterListen
	// URLs (nats-route://host:port), and JetStream state, subjects and the event
	// log replicate across them. This is how the harness scales horizontally and
	// goes multiplayer across machines.
	ClusterName   string
	ClusterListen string
	Routes        []string
}

// Start launches the embedded server, connects in-process, and provisions
// streams/buckets/object store.
func Start(ctx context.Context, opts Options) (*Engine, error) {
	name := opts.ServerName
	if name == "" {
		name = "vent-engine"
	}
	sopts := &server.Options{
		ServerName: name,
		JetStream:  true,
	}
	if opts.Listen == "" {
		sopts.DontListen = true // in-process only; no TCP socket
	} else {
		host, port, err := splitHostPort(opts.Listen)
		if err != nil {
			return nil, fmt.Errorf("listen addr %q: %w", opts.Listen, err)
		}
		sopts.Host, sopts.Port = host, port
	}
	if opts.ClusterListen != "" {
		host, port, err := splitHostPort(opts.ClusterListen)
		if err != nil {
			return nil, fmt.Errorf("cluster listen addr %q: %w", opts.ClusterListen, err)
		}
		sopts.Cluster = server.ClusterOpts{Name: opts.ClusterName, Host: host, Port: port}
		for _, r := range opts.Routes {
			u, err := url.Parse(r)
			if err != nil {
				return nil, fmt.Errorf("route %q: %w", r, err)
			}
			sopts.Routes = append(sopts.Routes, u)
		}
	}
	if opts.StoreDir != "" {
		sopts.StoreDir = opts.StoreDir
	} else {
		sopts.JetStreamMaxMemory = 1 << 30
		sopts.StoreDir = ""
	}

	ns, err := server.NewServer(sopts)
	if err != nil {
		return nil, fmt.Errorf("new server: %w", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		return nil, fmt.Errorf("nats server not ready")
	}

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		ns.Shutdown()
		return nil, fmt.Errorf("connect in-process: %w", err)
	}

	e := &Engine{ns: ns, nc: nc}
	if err := e.provision(ctx); err != nil {
		e.Close()
		return nil, err
	}
	return e, nil
}

func (e *Engine) provision(ctx context.Context) error {
	js, err := jetstream.New(e.nc)
	if err != nil {
		return err
	}

	// Event log: every agent event, retained for replay/observability.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      bus.StreamEvents,
		Subjects:  []string{"evt.>"},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour,
	}); err != nil {
		return fmt.Errorf("events stream: %w", err)
	}

	// Turn steps: a durable work queue that wakes the orchestrator FSM.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      bus.StreamTurns,
		Subjects:  []string{"turn.step.>"},
		Retention: jetstream.WorkQueuePolicy,
	}); err != nil {
		return fmt.Errorf("turn steps stream: %w", err)
	}

	// State buckets.
	for _, b := range []string{
		bus.BucketSessions, bus.BucketTurnState, bus.BucketApprovals,
		bus.BucketBudgets, bus.BucketTools, bus.BucketSkills,
	} {
		if _, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: b}); err != nil {
			return fmt.Errorf("kv %s: %w", b, err)
		}
	}

	// Blob store.
	if _, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bus.ObjectBlobs}); err != nil {
		return fmt.Errorf("object store: %w", err)
	}
	return nil
}

// Conn returns the in-process client connection. Each worker should call
// bus.Connect on a connection from NewConn (or share this one).
func (e *Engine) Conn() *nats.Conn { return e.nc }

// NewConn opens an additional in-process connection (useful so each worker has
// its own client, mirroring separate processes against a remote cluster).
func (e *Engine) NewConn() (*nats.Conn, error) {
	return nats.Connect("", nats.InProcessServer(e.ns))
}

// Bus returns a Bus over the engine's shared connection.
func (e *Engine) Bus() (*bus.Bus, error) { return bus.Connect(e.nc) }

// ClientURL is the nats:// address external workers connect to. Only meaningful
// when the engine was started with a Listen address.
func (e *Engine) ClientURL() string { return e.ns.ClientURL() }

// splitHostPort parses "host:port" (host may be empty, meaning all interfaces).
func splitHostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("port: %w", err)
	}
	return host, port, nil
}

// Close drains the connection and shuts the server down.
func (e *Engine) Close() {
	if e.nc != nil {
		_ = e.nc.Drain()
	}
	if e.ns != nil {
		e.ns.Shutdown()
		e.ns.WaitForShutdown()
	}
}
