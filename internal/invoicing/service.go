// Package invoicing implements the invoicing integration with Stripe. Stripe owns
// the invoice: at the end of a billing cycle (billing_cycle.ended) this service
// queries Metronome for the period's usage + price, creates the invoice items and
// invoice in Stripe, and maps the payment outcome back onto the bus. It talks to
// STRIPE_BASE_URL (fakestripe by default, or real Stripe test mode) and to the
// metronome service at METRONOME_SERVICE_URL.
package invoicing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/akydd/stripe_metronome/internal/config"
	"github.com/akydd/stripe_metronome/internal/events"
)

// Service is the Stripe integration service.
type Service struct {
	cfg         config.Config
	bus         events.Bus
	log         *slog.Logger
	http        *http.Client
	baseURL     string // Stripe API (fakestripe)
	meteringURL string // metering service, for the usage+price query
}

// New constructs the Stripe service.
func New(cfg config.Config, bus events.Bus, log *slog.Logger) *Service {
	return &Service{
		cfg:         cfg,
		bus:         bus,
		log:         log,
		http:        &http.Client{Timeout: 10 * time.Second},
		baseURL:     cfg.StripeBaseURL,
		meteringURL: cfg.MeteringServiceURL,
	}
}

// Register wires the service's event subscriptions and HTTP routes.
func (s *Service) Register(mux *http.ServeMux) {
	// At cycle end, pull usage from Metronome and invoice it.
	s.bus.Subscribe(events.BillingCycleEnded, s.onBillingCycleEnded)

	// Flat-fee subscriptions are managed in Stripe (fixed plan price).
	s.bus.Subscribe(events.SubscriptionActivated, s.onSubscriptionActivated)
	s.bus.Subscribe(events.SubscriptionCancelled, s.onSubscriptionCancelled)

	// Surface the fixed subscription plan price to the frontend (read-only).
	mux.HandleFunc("GET /v1/subscription-plan", s.handleSubscriptionPlan)

	// Invoice views for the frontend: the mid-cycle preview and finalized history.
	mux.HandleFunc("GET /v1/customers/{id}/upcoming-invoice", s.handleUpcomingInvoice)
	mux.HandleFunc("GET /v1/customers/{id}/invoices", s.handleListInvoices)

	// Inbound Stripe webhooks (payment status, etc.).
	mux.HandleFunc("POST /webhooks/stripe", s.handleWebhook)
}

// handleUpcomingInvoice previews what the customer would be billed if the cycle
// closed now: current usage read from Metronome (no close) + active Stripe
// subscriptions. Read-only — it does not close the period.
func (s *Service) handleUpcomingInvoice(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var usage struct {
		Currency string       `json:"currency"`
		Groups   []usageGroup `json:"groups"`
	}
	if err := s.getJSON(r.Context(), s.meteringURL+"/v1/customers/"+id+"/usage", &usage); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	var subs struct {
		Subscriptions []struct {
			SubscriptionID string `json:"subscription_id"`
			ProratedCents  int    `json:"prorated_cents"`
			Cancelled      bool   `json:"cancelled"`
		} `json:"subscriptions"`
		TotalCents         int `json:"total_cents"`
		ProratedTotalCents int `json:"prorated_total_cents"`
	}
	if err := s.getJSON(r.Context(), s.baseURL+"/v1/customers/"+id+"/subscriptions", &subs); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	currency := usage.Currency
	if currency == "" {
		currency = "usd"
	}

	// Mid-cycle shows each subscription PRORATED for the partial period; the
	// finalized invoice bills active subs the full period. A subscription
	// cancelled mid-cycle stays on the preview at its prorated amount, labelled
	// cancelled — that's what the close will bill.
	subscriptionProrated := subs.ProratedTotalCents > 0
	lines := []map[string]any{}
	for _, sub := range subs.Subscriptions {
		if sub.ProratedCents <= 0 {
			continue
		}
		desc := "Subscription (prorated)"
		if sub.Cancelled {
			desc = "Subscription (cancelled, prorated)"
		}
		lines = append(lines, map[string]any{
			"description":   desc,
			"amount_micros": int64(sub.ProratedCents) * microsPerCent,
		})
	}
	var usageMicros int64
	for _, g := range usage.Groups {
		// Preview shows accruing usage even when it's below a cent (so freshly
		// emitted usage is visible); amounts stay in micros here.
		if g.Quantity <= 0 {
			continue
		}
		lines = append(lines, map[string]any{
			"description":       usageDesc(g.Dimensions, -1),
			"amount_micros":     g.UsageMicros,
			"quantity":          g.Quantity,
			"unit_price_micros": g.UnitPriceMicros,
		})
		usageMicros += g.UsageMicros
	}
	subMicros := int64(subs.ProratedTotalCents) * microsPerCent
	totalMicros := usageMicros + subMicros
	writeJSON(w, http.StatusOK, map[string]any{
		"customer_id":           id,
		"usage_micros":          usageMicros,
		"subscription_micros":   subMicros,
		"subscription_prorated": subscriptionProrated,
		"amount_micros":         totalMicros,
		"amount_cents":          roundCents(totalMicros), // rounded once
		"currency":              currency,
		"lines":                 lines,
		"upcoming":              true,
	})
}

// microsPerCent converts the micros money unit to cents (1 cent = 10,000 micros).
const microsPerCent = 10000

// roundCents rounds a micros amount to whole cents (once, for the invoice total).
func roundCents(micros int64) int {
	return int(math.Round(float64(micros) / float64(microsPerCent)))
}

// usageGroup is one dimension-tuple's priced usage (micros), from Metronome.
type usageGroup struct {
	Dimensions      map[string]string `json:"dimensions"`
	Quantity        float64           `json:"quantity"`
	UnitPriceMicros int64             `json:"unit_price_micros"`
	UsageMicros     int64             `json:"usage_micros"`
}

// usageDesc builds an invoice line description from a group's dimensions. The
// "late" dimension is surfaced as a distinct "Late usage" line.
func usageDesc(dims map[string]string, period int) string {
	label := "API requests"
	if dims["late"] == "true" {
		label = "Late usage"
	}
	suffix := ""
	if period >= 0 { // mid-cycle (list-costs) has no period; finalized invoices do
		suffix = fmt.Sprintf(" (period %d)", period)
	}
	if gen := dims["generator_id"]; gen != "" {
		return fmt.Sprintf("%s · gen %s%s", label, short(gen), suffix)
	}
	return fmt.Sprintf("%s%s", label, suffix)
}

// short truncates an id for display in a line description.
func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// handleListInvoices proxies the customer's finalized invoices from Stripe.
func (s *Service) handleListInvoices(w http.ResponseWriter, r *http.Request) {
	s.proxyGET(w, r, s.baseURL+"/v1/customers/"+r.PathValue("id")+"/invoices")
}

// onSubscriptionActivated registers a flat-fee subscription in Stripe. The price
// is Stripe's fixed plan price, so no amount is sent.
func (s *Service) onSubscriptionActivated(ctx context.Context, e events.Event) error {
	var p struct {
		SubscriptionID string `json:"subscription_id"`
	}
	if len(e.Payload) > 0 {
		_ = json.Unmarshal(e.Payload, &p)
	}
	s.log.Info("registering subscription", "customer", e.CustomerID, "subscription", p.SubscriptionID)
	return s.postJSON(ctx, s.baseURL+"/v1/customers/"+e.CustomerID+"/subscriptions", map[string]any{
		"subscription_id": p.SubscriptionID,
	}, nil)
}

// handleSubscriptionPlan proxies the fixed plan price from Stripe for display.
func (s *Service) handleSubscriptionPlan(w http.ResponseWriter, r *http.Request) {
	s.proxyGET(w, r, s.baseURL+"/v1/subscription-plan")
}

// onSubscriptionCancelled cancels a flat-fee subscription in Stripe.
func (s *Service) onSubscriptionCancelled(ctx context.Context, e events.Event) error {
	var p struct {
		SubscriptionID string `json:"subscription_id"`
	}
	if len(e.Payload) > 0 {
		_ = json.Unmarshal(e.Payload, &p)
	}
	s.log.Info("cancelling subscription", "customer", e.CustomerID, "subscription", p.SubscriptionID)
	return s.deleteURL(ctx, s.baseURL+"/v1/customers/"+e.CustomerID+"/subscriptions/"+p.SubscriptionID)
}

// onBillingCycleEnded queries Metronome for the closed period's usage + price and
// invoices it through Stripe.
func (s *Service) onBillingCycleEnded(ctx context.Context, e events.Event) error {
	// 1. Query Metronome: close the period and get its priced usage. This also
	//    rolls later usage into the next period (next invoice).
	var billed struct {
		Period   int          `json:"period"`
		Currency string       `json:"currency"`
		Groups   []usageGroup `json:"groups"`
	}
	if err := s.postJSON(ctx, s.meteringURL+"/v1/customers/"+e.CustomerID+"/close-period", nil, &billed); err != nil {
		return fmt.Errorf("query metronome usage: %w", err)
	}
	currency := billed.Currency
	if currency == "" {
		currency = "usd"
	}

	// 2. Add one metered usage line per group_by value (micros, with quantity +
	//    unit price). Flat-fee subscriptions are added by Stripe when the invoice
	//    is made; the invoice total is rounded to cents once, by Stripe.
	for _, g := range billed.Groups {
		if g.UsageMicros <= 0 {
			continue
		}
		desc := usageDesc(g.Dimensions, billed.Period)
		if err := s.addInvoiceItem(ctx, e.CustomerID, g.UsageMicros, currency, desc, g.Quantity, g.UnitPriceMicros); err != nil {
			return err
		}
	}

	// 3. Create + finalize the invoice (pulls pending usage items + active
	//    subscriptions) and collect payment.
	var inv struct {
		ID        string `json:"id"`
		AmountDue int    `json:"amount_due"`
		Paid      bool   `json:"paid"`
		Status    string `json:"status"`
	}
	if err := s.postJSON(ctx, s.baseURL+"/v1/invoices", map[string]any{
		"customer": e.CustomerID,
		"period":   billed.Period,
	}, &inv); err != nil {
		return fmt.Errorf("create invoice: %w", err)
	}

	// Nothing to bill (no usage and no active subscription) — skip.
	if inv.AmountDue <= 0 {
		s.log.Info("nothing to bill for period; skipping invoice", "customer", e.CustomerID, "period", billed.Period)
		return nil
	}
	s.log.Info("stripe invoice created", "id", inv.ID, "customer", e.CustomerID,
		"amount_cents", inv.AmountDue, "paid", inv.Paid)

	// 4. Map the provider outcome onto domain events.
	payload, _ := json.Marshal(map[string]any{
		"invoice_id":   inv.ID,
		"status":       inv.Status,
		"amount_cents": inv.AmountDue,
	})
	if err := s.bus.Publish(ctx, events.Event{
		ID:         events.NewID(),
		Type:       events.InvoiceFinalized,
		CustomerID: e.CustomerID,
		OccurredAt: time.Now().UTC(),
		Payload:    payload,
	}); err != nil {
		return err
	}

	outcome := events.PaymentSucceeded
	if !inv.Paid {
		outcome = events.PaymentFailed
	}
	return s.bus.Publish(ctx, events.Event{
		ID:         events.NewID(),
		Type:       outcome,
		CustomerID: e.CustomerID,
		OccurredAt: time.Now().UTC(),
		Payload:    payload,
	})
}

// addInvoiceItem creates a pending invoice item on the customer in Stripe (amount
// in micros), including the metered quantity and unit price for an itemized line.
func (s *Service) addInvoiceItem(ctx context.Context, customer string, amountMicros int64, currency, description string, quantity float64, unitPriceMicros int64) error {
	if err := s.postJSON(ctx, s.baseURL+"/v1/invoiceitems", map[string]any{
		"customer":          customer,
		"amount_micros":     amountMicros,
		"currency":          currency,
		"description":       description,
		"quantity":          quantity,
		"unit_price_micros": unitPriceMicros,
	}, nil); err != nil {
		return fmt.Errorf("create invoice item %q: %w", description, err)
	}
	return nil
}

func (s *Service) getJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s: %s: %s", url, resp.Status, string(body))
	}
	if out != nil {
		return json.Unmarshal(body, out)
	}
	return nil
}

// proxyGET forwards a GET to an upstream and streams the response back.
func (s *Service) proxyGET(w http.ResponseWriter, r *http.Request, url string) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp, err := s.http.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Service) deleteURL(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("DELETE %s: %s", url, resp.Status)
	}
	return nil
}

// handleWebhook receives Stripe webhook callbacks. Not yet implemented.
func (s *Service) handleWebhook(w http.ResponseWriter, _ *http.Request) {
	// TODO: verify signature, map Stripe webhook events to domain events, publish.
	w.WriteHeader(http.StatusNotImplemented)
}

func (s *Service) postJSON(ctx context.Context, url string, in, out any) error {
	var buf io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		buf = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, buf)
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
		return fmt.Errorf("POST %s: %s: %s", url, resp.Status, string(body))
	}
	if out != nil {
		return json.Unmarshal(body, out)
	}
	return nil
}
