// Command metronome runs the Metronome metering service.
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
	"github.com/akydd/stripe_metronome/internal/events"
	"github.com/akydd/stripe_metronome/internal/metronome"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load("METRONOME", ":8082")
	bus, err := events.NewBus(cfg.BusKind, cfg.KafkaSeeds, "metronome", log)
	if err != nil {
		log.Error("failed to create event bus", "err", err)
		os.Exit(1)
	}
	defer bus.Close()

	mux := http.NewServeMux()
	metronome.New(cfg, bus, log).Register(mux)

	if err := app.Serve(ctx, "metronome", cfg.HTTPAddr, log, mux); err != nil {
		log.Error("service exited with error", "err", err)
		os.Exit(1)
	}
}
