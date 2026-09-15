// Package metering implements the metering integration with Metronome:
// forwarding ingested usage to the Metronome /ingest API, exposing mid-billing-
// period usage, and closing a period on request (returning its priced usage).
// Invoicing is disabled here — Stripe owns the invoice and pulls usage+price via
// the close-period endpoint at cycle end. Talks to METRONOME_BASE_URL (the local
// fakemetronome by default, or the real API).
package metering

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/akydd/stripe_metronome/internal/config"
	"github.com/akydd/stripe_metronome/internal/events"
)

// ingestBatchMax is Metronome's per-request event cap.
const ingestBatchMax = 100

// Service is the Metronome metering service.
type Service struct {
	cfg     config.Config
	bus     events.Bus
	log     *slog.Logger
	http    *http.Client
	baseURL string

	// Usage events are buffered and flushed to /ingest in batches for throughput.
	bufMu sync.Mutex
	buf   []ingestEvent

	// Current billing-period start per customer (learned from billing_cycle.ended),
	// used to determine whether an incoming usage event arrived late.
	periodMu    sync.Mutex
	periodStart map[string]time.Time
}

// New constructs the Metronome service and starts the ingest flusher.
func New(cfg config.Config, bus events.Bus, log *slog.Logger) *Service {
	s := &Service{
		cfg:         cfg,
		bus:         bus,
		log:         log,
		http:        &http.Client{Timeout: 10 * time.Second},
		baseURL:     cfg.MetronomeBaseURL,
		periodStart: map[string]time.Time{},
	}
	go s.flushLoop()
	return s
}

// Register wires the service's event subscriptions and HTTP routes.
func (s *Service) Register(mux *http.ServeMux) {
	// Meter each ingested usage event in Metronome. (Flat-fee subscriptions are
	// managed by Stripe, not Metronome — see the stripe service.)
	s.bus.Subscribe(events.UsageIngested, s.onUsageIngested)

	// Track billing-period boundaries so ingest can flag late-arriving usage.
	s.bus.Subscribe(events.BillingCycleEnded, s.onCycleClosed)

	// Mid-billing-period usage visibility (proxies the Metronome draft usage).
	mux.HandleFunc("GET /v1/customers/{id}/usage", s.handleCurrentUsage)

	// Close the period and return its priced usage (called by the stripe service
	// at cycle end). Late usage after this rolls over to the next period.
	mux.HandleFunc("POST /v1/customers/{id}/close-period", s.handleClosePeriod)

	// Inbound Metronome webhooks (kept for completeness; invoicing is disabled).
	mux.HandleFunc("POST /webhooks/metronome", s.handleWebhook)
}

// ingestEvent mirrors Metronome's usage event shape.
type ingestEvent struct {
	TransactionID string         `json:"transaction_id"`
	CustomerID    string         `json:"customer_id"`
	Timestamp     string         `json:"timestamp"`
	EventType     string         `json:"event_type"`
	Properties    map[string]any `json:"properties,omitempty"`
}

// onUsageIngested buffers a usage event; the buffer is flushed to Metronome's
// /ingest in batches (at ingestBatchMax, or on a timer — see flushLoop).
func (s *Service) onUsageIngested(ctx context.Context, e events.Event) error {
	var p struct {
		EventType   string  `json:"event_type"`
		Quantity    float64 `json:"quantity"`
		GeneratorID string  `json:"generator_id"`
	}
	if len(e.Payload) > 0 {
		_ = json.Unmarshal(e.Payload, &p)
	}
	if p.EventType == "" {
		p.EventType = "api_requests"
	}
	if p.Quantity == 0 {
		p.Quantity = 1
	}

	// Determine lateness here (not at the source): usage is late if it occurred
	// before the customer's current billing period began.
	late := s.isLate(e.CustomerID, e.OccurredAt)

	// transaction_id == event ID makes retries idempotent. generator_id and late
	// are dimensions the billable metric groups by (per generator, on-time vs
	// late) so they appear as distinct invoice lines.
	ev := ingestEvent{
		TransactionID: e.ID,
		CustomerID:    e.CustomerID,
		Timestamp:     e.OccurredAt.Format(time.RFC3339),
		EventType:     p.EventType,
		Properties: map[string]any{
			"quantity":     p.Quantity,
			"generator_id": p.GeneratorID,
			"late":         late,
		},
	}
	s.bufMu.Lock()
	s.buf = append(s.buf, ev)
	full := len(s.buf) >= ingestBatchMax
	s.bufMu.Unlock()
	if full {
		s.flush(ctx)
	}
	return nil
}

// onCycleClosed records when a customer's new billing period began (the close
// time), so later usage timestamped before it can be flagged late.
func (s *Service) onCycleClosed(_ context.Context, e events.Event) error {
	s.periodMu.Lock()
	s.periodStart[e.CustomerID] = e.OccurredAt
	s.periodMu.Unlock()
	return nil
}

// isLate reports whether usage that occurred at occurredAt is late — i.e. it
// falls before the customer's current period start. Before any cycle has closed
// there's no boundary, so nothing is late.
func (s *Service) isLate(customer string, occurredAt time.Time) bool {
	s.periodMu.Lock()
	start, ok := s.periodStart[customer]
	s.periodMu.Unlock()
	return ok && occurredAt.Before(start)
}

// flushLoop periodically flushes partial batches.
func (s *Service) flushLoop() {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		s.flush(context.Background())
	}
}

// flush posts buffered usage to Metronome's /ingest in chunks of ingestBatchMax.
func (s *Service) flush(ctx context.Context) {
	s.bufMu.Lock()
	if len(s.buf) == 0 {
		s.bufMu.Unlock()
		return
	}
	batch := s.buf
	s.buf = nil
	s.bufMu.Unlock()

	for start := 0; start < len(batch); start += ingestBatchMax {
		end := start + ingestBatchMax
		if end > len(batch) {
			end = len(batch)
		}
		chunk := batch[start:end]
		if err := s.postJSON(ctx, "/ingest", chunk, nil); err != nil {
			// TODO: retry with backoff; transaction_id dedup makes retries safe.
			s.log.Error("ingest flush failed", "count", len(chunk), "err", err)
			continue
		}
		s.log.Info("metered usage batch", "count", len(chunk))
	}
}

// handleCurrentUsage reads the customer's priced usage from Metronome's list-costs
// endpoint and adapts it into the grouped shape the stripe service consumes.
func (s *Service) handleCurrentUsage(w http.ResponseWriter, r *http.Request) {
	body, status, err := s.getUpstream(r.Context(), "/v1/customers/"+r.PathValue("id")+"/costs")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if status >= 300 {
		http.Error(w, string(body), status)
		return
	}

	var costs struct {
		Data []struct {
			LineItemBreakdown []struct {
				Groups    map[string]string `json:"groups"`
				Quantity  float64           `json:"quantity"`
				UnitPrice float64           `json:"unit_price"`
				Cost      float64           `json:"cost"`
			} `json:"line_item_breakdown"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &costs); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	type outGroup struct {
		Dimensions      map[string]string `json:"dimensions"`
		Quantity        float64           `json:"quantity"`
		UnitPriceMicros int64             `json:"unit_price_micros"`
		UsageMicros     int64             `json:"usage_micros"`
	}
	groups := []outGroup{}
	for _, win := range costs.Data {
		for _, li := range win.LineItemBreakdown {
			groups = append(groups, outGroup{
				Dimensions:      li.Groups,
				Quantity:        li.Quantity,
				UnitPriceMicros: int64(math.Round(li.UnitPrice * 1e6)),
				UsageMicros:     int64(math.Round(li.Cost * 1e6)),
			})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"currency": "usd", "groups": groups})
}

// handleClosePeriod closes the period upstream and returns its priced usage.
func (s *Service) handleClosePeriod(w http.ResponseWriter, r *http.Request) {
	var result json.RawMessage
	if err := s.postJSON(r.Context(), "/v1/customers/"+r.PathValue("id")+"/close-period", nil, &result); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(result)
}

// handleWebhook receives Metronome webhook callbacks. Not used (invoicing off).
func (s *Service) handleWebhook(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

// --- upstream helpers ---

func (s *Service) postJSON(ctx context.Context, path string, in, out any) error {
	var buf io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		buf = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+path, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("upstream POST %s: %s: %s", path, resp.Status, string(body))
	}
	if p, ok := out.(*json.RawMessage); ok {
		*p = json.RawMessage(body)
	}
	return nil
}

func (s *Service) getUpstream(ctx context.Context, path string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+path, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	return body, resp.StatusCode, nil
}
