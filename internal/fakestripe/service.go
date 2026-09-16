// Package fakestripe is a stand-in for the Stripe API. Stripe owns the invoice
// AND flat-fee subscriptions: subscriptions are registered/cancelled here, and
// when an invoice is created it pulls the customer's pending invoice items (e.g.
// metered usage) plus the active subscription fees — like a real Stripe
// Subscription billing on invoice creation. Swap for real Stripe (test mode) via
// STRIPE_BASE_URL.
//
// Demo behavior: a customer whose id contains "fail" doesn't pay.
package fakestripe

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

// subscriptionPriceCents is the fixed flat-fee price for a full period, owned by
// Stripe (like a Stripe Price on a plan). It is NOT set by the frontend.
// subscriptionProratedCents is the default prorated amount shown for a partial
// (mid-cycle) period. A finalized full period bills subscriptionPriceCents.
const (
	subscriptionPriceCents    = 5000 // $50.00 / full period
	subscriptionProratedCents = 2500 // $25.00 default prorated (partial period)
)

// proratedCents scales a full-period amount to the default prorated fraction.
func proratedCents(fullCents int) int {
	if subscriptionPriceCents == 0 {
		return 0
	}
	return fullCents * subscriptionProratedCents / subscriptionPriceCents
}

// Service holds pending invoice items, active subscriptions, and finalized
// invoices per customer.
type Service struct {
	log      *slog.Logger
	mu       sync.Mutex
	seq      int
	items    map[string][]line
	subs     map[string]map[string]int // customer -> subscription id -> amount_cents
	invoices map[string][]map[string]any
}

type line struct {
	ID              string  `json:"id"`
	Customer        string  `json:"customer"`
	AmountMicros    int64   `json:"amount_micros"` // 1 USD = 1,000,000 micros
	Currency        string  `json:"currency"`
	Description     string  `json:"description"`
	Quantity        float64 `json:"quantity,omitempty"`
	UnitPriceMicros int64   `json:"unit_price_micros,omitempty"`
}

// microsPerCent converts the micros money unit to cents (1 cent = 10,000 micros).
const microsPerCent = 10000

// New constructs the fake Stripe service.
func New(log *slog.Logger) *Service {
	return &Service{
		log:      log,
		items:    map[string][]line{},
		subs:     map[string]map[string]int{},
		invoices: map[string][]map[string]any{},
	}
}

// Register wires the fake's HTTP routes.
func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/invoiceitems", s.handleCreateItem)
	mux.HandleFunc("POST /v1/invoices", s.handleCreateInvoice)
	mux.HandleFunc("GET /v1/subscription-plan", s.handleSubscriptionPlan)
	mux.HandleFunc("POST /v1/customers/{id}/subscriptions", s.handleAddSubscription)
	mux.HandleFunc("DELETE /v1/customers/{id}/subscriptions/{sid}", s.handleDeleteSubscription)
	mux.HandleFunc("GET /v1/customers/{id}/subscriptions", s.handleListSubscriptions)
	mux.HandleFunc("GET /v1/customers/{id}/invoices", s.handleListInvoices)
}

// nextID returns a Stripe-like id. Caller must hold s.mu.
func (s *Service) nextID(prefix string) string {
	s.seq++
	return fmt.Sprintf("%s_%d", prefix, s.seq)
}

// handleCreateItem adds a pending invoice item to the customer.
func (s *Service) handleCreateItem(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Customer        string  `json:"customer"`
		AmountMicros    int64   `json:"amount_micros"`
		Currency        string  `json:"currency"`
		Description     string  `json:"description"`
		Quantity        float64 `json:"quantity"`
		UnitPriceMicros int64   `json:"unit_price_micros"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Currency == "" {
		req.Currency = "usd"
	}

	s.mu.Lock()
	item := line{
		ID:              s.nextID("ii"),
		Customer:        req.Customer,
		AmountMicros:    req.AmountMicros,
		Currency:        req.Currency,
		Description:     req.Description,
		Quantity:        req.Quantity,
		UnitPriceMicros: req.UnitPriceMicros,
	}
	s.items[req.Customer] = append(s.items[req.Customer], item)
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, item)
}

// handleCreateInvoice creates an invoice from the customer's pending items plus
// active subscription fees, finalizes it, and reports whether payment succeeded.
func (s *Service) handleCreateInvoice(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Customer string `json:"customer"`
		Period   int    `json:"period"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	lines := s.items[req.Customer]
	delete(s.items, req.Customer) // items are one-shot; subscriptions recur
	// Add a line for each active subscription (recurring flat fee). Subscription
	// prices are stored in cents; convert to micros for the line.
	for sid, cents := range s.subs[req.Customer] {
		lines = append(lines, line{
			ID:           s.nextID("sub_ii"),
			Customer:     req.Customer,
			AmountMicros: int64(cents) * microsPerCent,
			Currency:     "usd",
			Description:  "Subscription " + sid,
		})
	}
	id := s.nextID("in")
	s.mu.Unlock()

	// Sum line amounts in micros, then round the invoice total to cents ONCE.
	var totalMicros int64
	currency := "usd"
	for _, l := range lines {
		totalMicros += l.AmountMicros
		if l.Currency != "" {
			currency = l.Currency
		}
	}
	amountDue := int(math.Round(float64(totalMicros) / float64(microsPerCent)))

	// Simulate the payment outcome: customers whose id contains "fail" don't pay.
	paid := !strings.Contains(strings.ToLower(req.Customer), "fail")
	status := "paid"
	if !paid {
		status = "open"
	}

	inv := map[string]any{
		"id":                id,
		"object":            "invoice",
		"customer":          req.Customer,
		"period":            req.Period,
		"amount_due":        amountDue,   // cents, rounded once from the micros total
		"amount_due_micros": totalMicros, // exact
		"currency":          currency,
		"status":            status,
		"paid":              paid,
		"lines":             lines,
		"created":           time.Now().UTC().Format(time.RFC3339),
	}
	// Persist invoices that actually bill a cent or more.
	if amountDue > 0 {
		s.mu.Lock()
		s.invoices[req.Customer] = append(s.invoices[req.Customer], inv)
		s.mu.Unlock()
	}

	s.log.Info("created invoice", "id", id, "customer", req.Customer, "amount_due", amountDue, "paid", paid)
	writeJSON(w, http.StatusOK, inv)
}

// handleListInvoices returns the customer's finalized invoices (most recent last).
func (s *Service) handleListInvoices(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	out := append([]map[string]any{}, s.invoices[id]...)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// handleListSubscriptions returns the customer's active subscriptions and their total.
func (s *Service) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	type sub struct {
		SubscriptionID string `json:"subscription_id"`
		AmountCents    int    `json:"amount_cents"`   // full period
		ProratedCents  int    `json:"prorated_cents"` // partial (mid-cycle) period
	}
	subs := make([]sub, 0, len(s.subs[id]))
	total, proratedTotal := 0, 0
	for sid, amt := range s.subs[id] {
		p := proratedCents(amt)
		subs = append(subs, sub{SubscriptionID: sid, AmountCents: amt, ProratedCents: p})
		total += amt
		proratedTotal += p
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"subscriptions":        subs,
		"total_cents":          total,
		"prorated_total_cents": proratedTotal,
	})
}

// handleSubscriptionPlan returns the fixed flat-fee price, so the frontend can
// display it (it can't set it).
func (s *Service) handleSubscriptionPlan(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"amount_cents": subscriptionPriceCents, "currency": "usd"})
}

// handleAddSubscription registers a flat-fee subscription. The price is the fixed
// plan price owned here — the caller does not supply an amount.
func (s *Service) handleAddSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req struct {
		SubscriptionID string `json:"subscription_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SubscriptionID == "" {
		http.Error(w, "subscription_id required", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if s.subs[id] == nil {
		s.subs[id] = map[string]int{}
	}
	s.subs[id][req.SubscriptionID] = subscriptionPriceCents
	s.mu.Unlock()

	s.log.Info("subscription registered", "customer", id, "subscription", req.SubscriptionID, "amount_cents", subscriptionPriceCents)
	writeJSON(w, http.StatusOK, map[string]any{"customer": id, "subscription_id": req.SubscriptionID, "amount_cents": subscriptionPriceCents})
}

// handleDeleteSubscription cancels a flat-fee subscription.
func (s *Service) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sid := r.PathValue("sid")
	s.mu.Lock()
	delete(s.subs[id], sid)
	s.mu.Unlock()
	s.log.Info("subscription cancelled", "customer", id, "subscription", sid)
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
