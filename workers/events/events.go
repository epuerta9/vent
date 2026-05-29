// Package events is the events gateway: it bridges the bus to browsers.
//
// It runs an HTTP server that exposes a Server-Sent Events endpoint and
// subscribes to every agent event on the bus, fanning each one out as a
// "data: <json>\n\n" frame to all connected SSE clients (optionally filtered
// to a single session).
package events

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

const defaultAddr = "127.0.0.1:8088"

// subscriber is a single connected SSE client. session is the optional session
// filter ("" means all sessions); ch carries pre-marshalled JSON event frames.
type subscriber struct {
	session string
	ch      chan []byte
}

// hub holds the set of connected SSE subscribers, guarded by a mutex.
type hub struct {
	mu   sync.Mutex
	subs map[*subscriber]struct{}
}

func newHub() *hub { return &hub{subs: make(map[*subscriber]struct{})} }

func (h *hub) add(s *subscriber) {
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) remove(s *subscriber) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
}

// broadcast delivers a marshalled event to every matching subscriber. Sends are
// non-blocking: a subscriber whose buffer is full drops the event rather than
// stalling the whole fanout.
func (h *hub) broadcast(session string, data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs {
		if s.session != "" && s.session != session {
			continue
		}
		select {
		case s.ch <- data:
		default:
		}
	}
}

// Start launches the events gateway. It starts an HTTP server in a goroutine on
// VENT_EVENTS_ADDR (default 127.0.0.1:8088) and subscribes to all bus events,
// fanning them out to connected SSE clients. It is non-blocking.
func Start(ctx context.Context, b *bus.Bus) error {
	h := newHub()

	_, err := b.SubscribeEvents("", func(ev types.Event) {
		data, err := json.Marshal(ev)
		if err != nil {
			return
		}
		h.broadcast(ev.SessionID, data)
	})
	if err != nil {
		return err
	}

	addr := os.Getenv("VENT_EVENTS_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/events", h.handleEvents)

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("events: listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("events: server error: %v", err)
		}
	}()

	// Shut the server down when the worker's context is cancelled.
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	return nil
}

// handleEvents serves one SSE client. It registers a subscriber, streams
// matching events until the request context is done, then deregisters.
func (h *hub) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	s := &subscriber{
		session: r.URL.Query().Get("session"),
		ch:      make(chan []byte, 64),
	}
	h.add(s)
	defer h.remove(s)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-s.ch:
			if _, err := w.Write([]byte("data: ")); err != nil {
				return
			}
			if _, err := w.Write(data); err != nil {
				return
			}
			if _, err := w.Write([]byte("\n\n")); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
