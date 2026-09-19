// Command controlplane runs the control plane for usage-generator processes (and
// relays flat-fee subscription commands to the invoicing service).
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
	"github.com/akydd/stripe_metronome/internal/controlplane"
	"github.com/akydd/stripe_metronome/internal/events"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load("CONTROLPLANE", ":8085")
	bus, err := events.NewBus(cfg.BusKind, cfg.KafkaSeeds, "controlplane", log)
	if err != nil {
		log.Error("failed to create event bus", "err", err)
		os.Exit(1)
	}
	defer bus.Close()

	mgr := controlplane.New(bus, log, cfg.BillingURL, cfg.InvoicingURL)
	defer mgr.Close()

	mux := http.NewServeMux()
	mgr.Register(mux)

	if err := app.Serve(ctx, "controlplane", cfg.HTTPAddr, log, mux); err != nil {
		log.Error("service exited with error", "err", err)
		os.Exit(1)
	}
}
