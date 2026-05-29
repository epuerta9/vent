package main

import (
	"context"
	"log"
	"os"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/nats-io/nats.go"
)

func main() {
	url := os.Getenv("VENT_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}

	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatalf("external worker: connect %s: %v (is the engine up? run 'vent serve' first)", url, err)
	}

	b, err := bus.Connect(nc)
	if err != nil {
		log.Fatalf("external worker: bus connect: %v", err)
	}

	if err := registerEcho(context.Background(), b); err != nil {
		log.Fatalf("external worker: register echo: %v", err)
	}

	log.Printf("external worker connected to %s, registered fn.tool.echo", url)

	// Block forever; the registration lives for the lifetime of the process.
	select {}
}
