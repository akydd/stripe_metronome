// Command fakestripe runs a local stand-in for the Stripe API.
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
	"github.com/akydd/stripe_metronome/internal/fakestripe"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load("FAKESTRIPE", ":8084")

	mux := http.NewServeMux()
	fakestripe.New(log, cfg.StripeWebhookURL, cfg.StripeWebhookSecret).Register(mux)

	if err := app.Serve(ctx, "fakestripe", cfg.HTTPAddr, log, mux); err != nil {
		log.Error("service exited with error", "err", err)
		os.Exit(1)
	}
}
