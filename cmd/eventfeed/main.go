// Command eventfeed runs the read-only live event-feed service: it tails every
// Kafka topic into a bounded buffer and serves it to the frontend.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/akydd/stripe_metronome/internal/app"
	"github.com/akydd/stripe_metronome/internal/config"
	"github.com/akydd/stripe_metronome/internal/eventfeed"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load("EVENTFEED", ":8086")
	svc, err := eventfeed.New(cfg, log)
	if err != nil {
		log.Error("failed to start eventfeed", "err", err)
		os.Exit(1)
	}
	defer svc.Close()

	mux := http.NewServeMux()
	svc.Register(mux)

	if err := app.Serve(ctx, "eventfeed", cfg.HTTPAddr, log, mux); err != nil {
		log.Error("service exited with error", "err", err)
		os.Exit(1)
	}
}
