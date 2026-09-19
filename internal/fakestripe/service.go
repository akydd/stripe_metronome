// Package fakestripe is a stand-in for the Stripe API. Stripe owns the invoice
// AND flat-fee subscriptions: a subscription is created here with its own unique
// id, and when an invoice is created it pulls the customer's pending invoice
// items (e.g. metered usage) plus that customer's subscription fees — like real
// Stripe subscription billing on invoice creation. Subscription lifecycle changes
// are reported back to the invoicing service via webhooks, mirroring how real
// Stripe notifies your integration. Swap for real Stripe (test mode) via
// STRIPE_BASE_URL.
//
// Demo behavior: a customer whose id contains "fail" doesn't pay.
package fakestripe

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
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

// Service holds pending invoice items, subscriptions, and finalized invoices.
type Service struct {
	log           *slog.Logger
	http          *http.Client
	webhookURL    string // invoicing service base URL for subscription webhooks
	webhookSecret string // shared secret sent on webhooks (empty = none)

	mu       sync.Mutex
	seq      int
	items    map[string][]line
	subs     map[string]*subState // subscription id -> state
	subIdem  map[string]string    // idempotency key -> subscription id
	invoices map[string][]map[string]any
}

// subState is a flat-fee subscription, keyed by its own unique Stripe id. A
// cancelled subscription is NOT removed immediately: it stays until the next
// invoice, which bills it at the prorated amount (for the partial period it was
// active) and then drops it. Cancellation is terminal — a cancelled subscription
// is never reactivated; a new one must be created in its place.
type subState struct {
	ID          string
	Customer    string
	AmountCents int
	Cancelled   bool
	Metadata    map[string]string
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

// New constructs the fake Stripe service. webhookURL is the invoicing service it
// delivers subscription lifecycle webhooks to; webhookSecret (if set) is sent on
// each webhook for the receiver to verify.
func New(log *slog.Logger, webhookURL, webhookSecret string) *Service {
	return &Service{
		log:           log,
		http:          &http.Client{Timeout: 10 * time.Second},
		webhookURL:    webhookURL,
		webhookSecret: webhookSecret,
		items:         map[string][]line{},
		subs:          map[string]*subState{},
		subIdem:       map[string]string{},
		invoices:      map[string][]map[string]any{},
	}
}

// Register wires the fake's HTTP routes.
func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/invoiceitems", s.handleCreateItem)
	mux.HandleFunc("POST /v1/invoices", s.handleCreateInvoice)
	mux.HandleFunc("GET /v1/subscription-plan", s.handleSubscriptionPlan)
	// Subscriptions are top-level resources with their own minted ids.
	mux.HandleFunc("POST /v1/subscriptions", s.handleCreateSubscription)
	mux.HandleFunc("DELETE /v1/subscriptions/{id}", s.handleCancelSubscription)
	// Read a customer's subscriptions (used by invoicing to price the invoice).
	mux.HandleFunc("GET /v1/customers/{id}/subscriptions", s.handleListSubscriptions)
	mux.HandleFunc("GET /v1/customers/{id}/invoices", s.handleListInvoices)
}

// nextID returns a sequential Stripe-like id. Caller must hold s.mu.
func (s *Service) nextID(prefix string) string {
	s.seq++
	return fmt.Sprintf("%s_%d", prefix, s.seq)
}

// randID returns a random Stripe-like id (e.g. sub_ab12cd34...).
func randID(prefix string) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
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
// their subscription fees, finalizes it, and reports whether payment succeeded.
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
	// Add a line for each of this customer's subscriptions. An active subscription
	// bills the full period; one cancelled mid-cycle bills the prorated amount for
	// the partial period and is then dropped (terminal). Prices are in cents.
	for sid, st := range s.subs {
		if st.Customer != req.Customer {
			continue
		}
		cents := st.AmountCents
		desc := "Flat-fee Subscription"
		if st.Cancelled {
			cents = proratedCents(st.AmountCents)
			desc = "Flat-fee Subscription (cancelled, prorated)"
			delete(s.subs, sid) // final billing — remove it
		}
		lines = append(lines, line{
			ID:           s.nextID("sub_ii"),
			Customer:     req.Customer,
			AmountMicros: int64(cents) * microsPerCent,
			Currency:     "usd",
			Description:  desc,
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

// handleListSubscriptions returns a customer's subscriptions (active and
// cancelled-but-not-yet-invoiced) and their totals — used by invoicing to price
// the invoice and the mid-cycle preview.
func (s *Service) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	cust := r.PathValue("id")
	s.mu.Lock()
	type sub struct {
		SubscriptionID string `json:"subscription_id"`
		AmountCents    int    `json:"amount_cents"`   // full period
		ProratedCents  int    `json:"prorated_cents"` // partial (mid-cycle) period
		Cancelled      bool   `json:"cancelled"`
	}
	subs := make([]sub, 0)
	// total_cents is the full-period charge (active subs only — a cancelled sub
	// never bills a full period). prorated_total_cents is what a close-now would
	// bill: the prorated amount for every subscription, active or cancelled.
	total, proratedTotal := 0, 0
	for _, st := range s.subs {
		if st.Customer != cust {
			continue
		}
		p := proratedCents(st.AmountCents)
		subs = append(subs, sub{SubscriptionID: st.ID, AmountCents: st.AmountCents, ProratedCents: p, Cancelled: st.Cancelled})
		if !st.Cancelled {
			total += st.AmountCents
		}
		proratedTotal += p
	}
	s.mu.Unlock()
	sort.Slice(subs, func(i, j int) bool { return subs[i].SubscriptionID < subs[j].SubscriptionID })
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

// handleCreateSubscription mints a new subscription for a customer at the fixed
// plan price and reports it via a customer.subscription.created webhook. Honors
// the Idempotency-Key header: a repeat with the same key returns the original
// subscription and does not re-fire the webhook (matching Stripe).
func (s *Service) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Customer string            `json:"customer"`
		Metadata map[string]string `json:"metadata"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Customer == "" {
		http.Error(w, "customer required", http.StatusBadRequest)
		return
	}
	idemKey := r.Header.Get("Idempotency-Key")

	s.mu.Lock()
	if idemKey != "" {
		if existingID, ok := s.subIdem[idemKey]; ok {
			st := s.subs[existingID]
			s.mu.Unlock()
			if st != nil {
				writeJSON(w, http.StatusOK, subObject(st)) // idempotent replay: no new webhook
				return
			}
			// The subscription was already invoiced away; fall through to recreate.
			s.mu.Lock()
		}
	}
	st := &subState{
		ID:          randID("sub"),
		Customer:    req.Customer,
		AmountCents: subscriptionPriceCents,
		Metadata:    req.Metadata,
	}
	s.subs[st.ID] = st
	if idemKey != "" {
		s.subIdem[idemKey] = st.ID
	}
	obj := subObject(st)
	s.mu.Unlock()

	s.log.Info("subscription created", "id", st.ID, "customer", st.Customer, "amount_cents", st.AmountCents)
	s.fireWebhook("customer.subscription.created", obj)
	writeJSON(w, http.StatusCreated, obj)
}

// handleCancelSubscription cancels a subscription by id and reports it via a
// customer.subscription.deleted webhook. It is marked cancelled (not removed) so
// the next invoice can bill the prorated partial period; the invoice then drops
// it. Idempotent — cancelling an already-cancelled subscription is a no-op.
func (s *Service) handleCancelSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	st := s.subs[id]
	if st == nil {
		s.mu.Unlock()
		http.Error(w, "subscription not found", http.StatusNotFound)
		return
	}
	alreadyCancelled := st.Cancelled
	st.Cancelled = true
	obj := subObject(st)
	s.mu.Unlock()

	s.log.Info("subscription cancelled", "id", id, "customer", st.Customer)
	if !alreadyCancelled {
		s.fireWebhook("customer.subscription.deleted", obj)
	}
	writeJSON(w, http.StatusOK, obj)
}

// subObject renders a subscription as the Stripe-like object used in responses
// and webhooks. Caller must hold s.mu (reads subState fields).
func subObject(st *subState) map[string]any {
	status := "active"
	if st.Cancelled {
		status = "canceled"
	}
	return map[string]any{
		"id":           st.ID,
		"object":       "subscription",
		"customer":     st.Customer,
		"status":       status,
		"amount_cents": st.AmountCents,
		"metadata":     st.Metadata,
	}
}

// fireWebhook delivers a subscription lifecycle event to the invoicing service,
// asynchronously (like real Stripe, which posts webhooks out of band).
func (s *Service) fireWebhook(eventType string, object map[string]any) {
	if s.webhookURL == "" {
		return
	}
	event := map[string]any{
		"id":      randID("evt"),
		"object":  "event",
		"type":    eventType,
		"created": time.Now().UTC().Format(time.RFC3339),
		"data":    map[string]any{"object": object},
	}
	go func() {
		// Small delay so the synchronous create/cancel response returns first.
		time.Sleep(150 * time.Millisecond)
		body, _ := json.Marshal(event)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL+"/webhooks/stripe", bytes.NewReader(body))
		if err != nil {
			s.log.Error("build webhook request failed", "err", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if s.webhookSecret != "" {
			req.Header.Set("X-Webhook-Secret", s.webhookSecret)
		}
		resp, err := s.http.Do(req)
		if err != nil {
			s.log.Error("deliver webhook failed", "type", eventType, "err", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			s.log.Error("webhook rejected", "type", eventType, "status", resp.Status)
		}
	}()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
