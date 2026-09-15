// Command fakemetronome runs a local stand-in for the Metronome API.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/akydd/stripe_metronome/internal/app"
	"github.com/akydd/stripe_metronome/internal/config"
	"github.com/akydd/stripe_metronome/internal/fakemetronome"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load("FAKEMETRONOME", ":8083")

	// The billable metric's group_by dimensions (comma-separated event
	// properties to aggregate usage by). Empty groups all usage into one line.
	var groupBy []string
	for _, k := range strings.Split(os.Getenv("METRONOME_GROUP_BY"), ",") {
		if k = strings.TrimSpace(k); k != "" {
			groupBy = append(groupBy, k)
		}
	}

	mux := http.NewServeMux()
	fakemetronome.New(log, groupBy).Register(mux)

	if err := app.Serve(ctx, "fakemetronome", cfg.HTTPAddr, log, mux); err != nil {
		log.Error("service exited with error", "err", err)
		os.Exit(1)
	}
}
