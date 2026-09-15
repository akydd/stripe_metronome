// Command metering runs the metering service (the Metronome integration).
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
	"github.com/akydd/stripe_metronome/internal/metering"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load("METERING", ":8082")
	bus, err := events.NewBus(cfg.BusKind, cfg.KafkaSeeds, "metering", log)
	if err != nil {
		log.Error("failed to create event bus", "err", err)
		os.Exit(1)
	}
	defer bus.Close()

	mux := http.NewServeMux()
	metering.New(cfg, bus, log).Register(mux)

	if err := app.Serve(ctx, "metering", cfg.HTTPAddr, log, mux); err != nil {
		log.Error("service exited with error", "err", err)
		os.Exit(1)
	}
}
