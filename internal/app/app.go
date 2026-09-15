// Package app provides the shared HTTP run loop used by every service binary.
package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// Serve runs an HTTP server that shuts down gracefully when ctx is cancelled.
// It registers a /healthz endpoint on the provided mux before listening.
func Serve(ctx context.Context, name, addr string, log *slog.Logger, mux *http.ServeMux) error {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: mux}

	errCh := make(chan error, 1)
	go func() {
		log.Info("service listening", "service", name, "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down", "service", name)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
