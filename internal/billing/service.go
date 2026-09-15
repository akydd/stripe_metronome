// Package billing implements the business/orchestration service. It owns the
// canonical customer registry (the unique-customer identity + cross-provider id
// mapping that ties usage, cycles, and invoices together), the billing-cycle
// boundary (billing_cycle.ended), and payment-outcome tracking. It is not in the
// usage-data path, does not manage generator resources, and does not assemble
// invoices (Stripe owns the invoice).
//
// Customers are held in an in-memory map — not durable across restarts. The map
// is deliberately behind a couple of small methods so it can be swapped for a
// real store later without touching the handlers.
package billing

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/akydd/stripe_metronome/internal/config"
	"github.com/akydd/stripe_metronome/internal/events"
)

// Customer is the canonical record for a unique customer. The provider ids are
// the mapping used to tie metering (Metronome) and invoicing (Stripe) together.
type Customer struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	StripeCustomerID    string `json:"stripe_customer_id"`
	MetronomeCustomerID string `json:"metronome_customer_id"`
	CreatedAt           string `json:"created_at"`
}

// Service is the billing orchestration service.
type Service struct {
	cfg config.Config
	bus events.Bus
	log *slog.Logger

	mu        sync.Mutex
	customers map[string]*Customer
}

// New constructs the billing service.
func New(cfg config.Config, bus events.Bus, log *slog.Logger) *Service {
	return &Service{cfg: cfg, bus: bus, log: log, customers: map[string]*Customer{}}
}

// Register wires the service's event subscriptions and HTTP routes.
func (s *Service) Register(mux *http.ServeMux) {
	// Canonical customer registry.
	mux.HandleFunc("POST /v1/customers", s.createCustomer)
	mux.HandleFunc("GET /v1/customers", s.listCustomers)
	mux.HandleFunc("GET /v1/customers/{id}", s.getCustomer)

	// Close a customer's billing cycle — Stripe reacts and invoices.
	mux.HandleFunc("POST /v1/billing-cycles/{customer}/close", s.handleCloseCycle)

	// Track payment outcomes.
	s.bus.Subscribe(events.PaymentSucceeded, s.onPaymentSucceeded)
	s.bus.Subscribe(events.PaymentFailed, s.onPaymentFailed)
}

// --- customer registry ---

func (s *Service) createCustomer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}

	short := events.NewID()[:8]
	c := &Customer{
		// The id embeds a slug of the name so it's human-readable (and so a demo
		// customer named e.g. "Fail Co" produces an id the fake Stripe fails on).
		ID:   "cus_" + slug(req.Name) + "_" + short,
		Name: req.Name,
		// Stand-ins for the ids a real integration would create in each provider.
		StripeCustomerID:    "stripe_cus_" + short,
		MetronomeCustomerID: "metronome_cust_" + short,
		CreatedAt:           time.Now().UTC().Format(time.RFC3339),
	}
	s.mu.Lock()
	s.customers[c.ID] = c
	s.mu.Unlock()

	s.log.Info("created customer", "id", c.ID, "name", c.Name)
	writeJSON(w, http.StatusCreated, c)
}

func (s *Service) getCustomer(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	c := s.customers[r.PathValue("id")]
	s.mu.Unlock()
	if c == nil {
		http.Error(w, "customer not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Service) listCustomers(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := make([]Customer, 0, len(s.customers))
	for _, c := range s.customers {
		out = append(out, *c)
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	writeJSON(w, http.StatusOK, out)
}

// --- billing cycle ---

// handleCloseCycle ends a customer's billing cycle by publishing billing_cycle.ended.
func (s *Service) handleCloseCycle(w http.ResponseWriter, r *http.Request) {
	customer := r.PathValue("customer")
	if customer == "" {
		http.Error(w, "missing customer", http.StatusBadRequest)
		return
	}
	s.log.Info("closing billing cycle", "customer", customer)
	if err := s.bus.Publish(r.Context(), events.Event{
		ID:         events.NewID(),
		Type:       events.BillingCycleEnded,
		CustomerID: customer,
		OccurredAt: time.Now().UTC(),
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":      "cycle_closing",
		"customer_id": customer,
	})
}

// --- payment outcomes ---

func (s *Service) onPaymentSucceeded(_ context.Context, e events.Event) error {
	s.log.Info("payment succeeded", "customer", e.CustomerID)
	// TODO: reconcile payment status into local state.
	return nil
}

func (s *Service) onPaymentFailed(_ context.Context, e events.Event) error {
	s.log.Info("payment failed", "customer", e.CustomerID)
	// TODO: kick off dunning / customer notification.
	return nil
}

// slug turns a name into a short, lowercase, url-safe token for the customer id.
func slug(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		case r == ' ' || r == '-' || r == '_':
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 20 {
		out = out[:20]
	}
	if out == "" {
		out = "cust"
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
