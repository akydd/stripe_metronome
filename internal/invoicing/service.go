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
	"sort"
	"sync"
	"time"

	"github.com/akydd/stripe_metronome/internal/config"
	"github.com/akydd/stripe_metronome/internal/events"
)

// Service is the Stripe integration service.
type Service struct {
	cfg           config.Config
	bus           events.Bus
	log           *slog.Logger
	http          *http.Client
	baseURL       string // Stripe API (fakestripe)
	meteringURL   string // metering service, for the usage+price query
	webhookSecret string // shared secret expected on inbound Stripe webhooks

	// Flat-fee subscription link records: the invoicing-side entity tying a
	// customer to a Stripe subscription. Owned here; created via HTTP command and
	// confirmed via the fakestripe webhook.
	subMu   sync.Mutex
	subs    map[string]*Subscription // invoicing id -> record
	subIdem map[string]string        // Idempotency-Key -> invoicing id
}

// Subscription is the invoicing-side link record for a flat-fee subscription.
type Subscription struct {
	ID                   string `json:"id"`                     // invoicing's own id (isub_…)
	CustomerID           string `json:"customer_id"`            // billing customer id
	StripeSubscriptionID string `json:"stripe_subscription_id"` // fakestripe id (sub_…)
	Status               string `json:"status"`                 // pending -> active -> canceled
	CreatedAt            string `json:"created_at"`
}

// New constructs the Stripe service.
func New(cfg config.Config, bus events.Bus, log *slog.Logger) *Service {
	return &Service{
		cfg:           cfg,
		bus:           bus,
		log:           log,
		http:          &http.Client{Timeout: 10 * time.Second},
		baseURL:       cfg.StripeBaseURL,
		meteringURL:   cfg.MeteringServiceURL,
		webhookSecret: cfg.StripeWebhookSecret,
		subs:          map[string]*Subscription{},
		subIdem:       map[string]string{},
	}
}

// Register wires the service's event subscriptions and HTTP routes.
func (s *Service) Register(mux *http.ServeMux) {
	// At cycle end, pull usage from Metronome and invoice it.
	s.bus.Subscribe(events.BillingCycleEnded, s.onBillingCycleEnded)

	// Surface the fixed subscription plan price to the frontend (read-only).
	mux.HandleFunc("GET /v1/subscription-plan", s.handleSubscriptionPlan)

	// Flat-fee subscription lifecycle (created/cancelled over HTTP; the control
	// plane delegates here). The Stripe subscription lives in fakestripe; this
	// service owns the link record tying it to the customer.
	mux.HandleFunc("POST /v1/subscriptions", s.handleCreateSubscription)
	mux.HandleFunc("POST /v1/subscriptions/{id}/cancel", s.handleCancelSubscription)
	mux.HandleFunc("GET /v1/customers/{id}/subscriptions", s.handleListSubscriptions)

	// Invoice views for the frontend: the mid-cycle preview and finalized history.
	mux.HandleFunc("GET /v1/customers/{id}/upcoming-invoice", s.handleUpcomingInvoice)
	mux.HandleFunc("GET /v1/customers/{id}/invoices", s.handleListInvoices)

	// Inbound Stripe webhooks — subscription lifecycle confirmations from fakestripe.
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
		desc := "Flat-fee Subscription (prorated)"
		if sub.Cancelled {
			desc = "Flat-fee Subscription (cancelled, prorated)"
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

// usageDesc builds an invoice line description from a group's dimensions. Late
// usage (the "late" dimension) is surfaced by appending "(late)" to the label.
func usageDesc(dims map[string]string, period int) string {
	label := "Usage-based Billing"
	if dims["late"] == "true" {
		label += " (late)"
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

// handleCreateSubscription creates a flat-fee subscription: it records a pending
// link entity, then asks Stripe (fakestripe) to create the subscription, tagging
// it with this record's id so the async webhook can be correlated. The record
// flips to "active" when that webhook arrives. Honors the Idempotency-Key header
// end-to-end so a retried create can't produce a duplicate.
func (s *Service) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID string `json:"customer_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CustomerID == "" {
		http.Error(w, "customer_id required", http.StatusBadRequest)
		return
	}
	idemKey := r.Header.Get("Idempotency-Key")

	s.subMu.Lock()
	if idemKey != "" {
		if id, ok := s.subIdem[idemKey]; ok {
			if sub := s.subs[id]; sub != nil {
				out := *sub
				s.subMu.Unlock()
				writeJSON(w, http.StatusOK, &out) // idempotent replay
				return
			}
		}
	}
	sub := &Subscription{
		ID:         "isub_" + events.NewID()[:12],
		CustomerID: req.CustomerID,
		Status:     "pending",
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	s.subs[sub.ID] = sub
	if idemKey != "" {
		s.subIdem[idemKey] = sub.ID
	}
	s.subMu.Unlock()

	stripeID, err := s.createStripeSubscription(r.Context(), req.CustomerID, sub.ID, idemKey)
	if err != nil {
		s.log.Error("create stripe subscription failed", "customer", req.CustomerID, "err", err)
		http.Error(w, "failed to create subscription: "+err.Error(), http.StatusBadGateway)
		return
	}
	s.subMu.Lock()
	sub.StripeSubscriptionID = stripeID
	out := *sub
	s.subMu.Unlock()

	s.log.Info("subscription requested", "id", sub.ID, "stripe_id", stripeID, "customer", req.CustomerID)
	writeJSON(w, http.StatusCreated, &out)
}

// handleCancelSubscription cancels a flat-fee subscription in Stripe. The record
// flips to "canceled" when the deletion webhook arrives. Cancellation is terminal.
func (s *Service) handleCancelSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.subMu.Lock()
	sub := s.subs[id]
	var stripeID string
	if sub != nil {
		stripeID = sub.StripeSubscriptionID
	}
	s.subMu.Unlock()
	if sub == nil {
		http.Error(w, "subscription not found", http.StatusNotFound)
		return
	}
	if stripeID == "" {
		http.Error(w, "subscription not yet confirmed by Stripe; retry shortly", http.StatusConflict)
		return
	}
	if err := s.deleteURL(r.Context(), s.baseURL+"/v1/subscriptions/"+stripeID); err != nil {
		s.log.Error("cancel stripe subscription failed", "stripe_id", stripeID, "err", err)
		http.Error(w, "failed to cancel subscription", http.StatusBadGateway)
		return
	}
	s.subMu.Lock()
	out := *sub // webhook will flip status to "canceled"
	s.subMu.Unlock()
	writeJSON(w, http.StatusAccepted, &out)
}

// handleListSubscriptions returns a customer's link records (the invoicing-side
// subscription entities) for the frontend.
func (s *Service) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	cust := r.PathValue("id")
	s.subMu.Lock()
	out := make([]Subscription, 0)
	for _, sub := range s.subs {
		if sub.CustomerID == cust {
			out = append(out, *sub)
		}
	}
	s.subMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	writeJSON(w, http.StatusOK, out)
}

// handleSubscriptionPlan proxies the fixed plan price from Stripe for display.
func (s *Service) handleSubscriptionPlan(w http.ResponseWriter, r *http.Request) {
	s.proxyGET(w, r, s.baseURL+"/v1/subscription-plan")
}

// createStripeSubscription creates the Stripe subscription in fakestripe, tagging
// it with this service's link id so the async webhook can be correlated back, and
// forwarding the idempotency key. Returns the minted Stripe subscription id.
func (s *Service) createStripeSubscription(ctx context.Context, customerID, invoicingSubID, idemKey string) (string, error) {
	body, err := json.Marshal(map[string]any{
		"customer": customerID,
		"metadata": map[string]string{"invoicing_subscription_id": invoicingSubID},
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/v1/subscriptions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("stripe create subscription: %s: %s", resp.Status, string(respBody))
	}
	var obj struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(respBody, &obj); err != nil {
		return "", err
	}
	return obj.ID, nil
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

// handleWebhook receives subscription lifecycle webhooks from fakestripe, updates
// the link record, and publishes the corresponding domain event on the bus (the
// observable "fact" of the create/cancel).
func (s *Service) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if s.webhookSecret != "" && r.Header.Get("X-Webhook-Secret") != s.webhookSecret {
		http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
		return
	}
	var evt struct {
		Type string `json:"type"`
		Data struct {
			Object struct {
				ID       string            `json:"id"`
				Customer string            `json:"customer"`
				Status   string            `json:"status"`
				Metadata map[string]string `json:"metadata"`
			} `json:"object"`
		} `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
		http.Error(w, "bad webhook payload", http.StatusBadRequest)
		return
	}
	obj := evt.Data.Object

	// Correlate to our link record by the metadata id set at create time, falling
	// back to the Stripe subscription id.
	s.subMu.Lock()
	sub := s.subs[obj.Metadata["invoicing_subscription_id"]]
	if sub == nil {
		for _, candidate := range s.subs {
			if candidate.StripeSubscriptionID == obj.ID {
				sub = candidate
				break
			}
		}
	}
	if sub == nil {
		s.subMu.Unlock()
		s.log.Error("webhook for unknown subscription", "type", evt.Type, "stripe_id", obj.ID)
		w.WriteHeader(http.StatusOK) // ack so the provider doesn't retry forever
		return
	}
	if sub.StripeSubscriptionID == "" {
		sub.StripeSubscriptionID = obj.ID
	}
	var busType events.Type
	switch evt.Type {
	case "customer.subscription.created":
		sub.Status = "active"
		busType = events.SubscriptionCreated
	case "customer.subscription.deleted":
		sub.Status = "canceled"
		busType = events.SubscriptionCancelled
	default:
		s.subMu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	}
	record := *sub
	s.subMu.Unlock()

	s.log.Info("subscription webhook", "type", evt.Type, "id", record.ID, "stripe_id", record.StripeSubscriptionID)
	payload, _ := json.Marshal(map[string]any{
		"subscription_id":        record.ID,
		"stripe_subscription_id": record.StripeSubscriptionID,
	})
	if err := s.bus.Publish(r.Context(), events.Event{
		ID:         events.NewID(),
		Type:       busType,
		CustomerID: record.CustomerID,
		OccurredAt: time.Now().UTC(),
		Payload:    payload,
	}); err != nil {
		s.log.Error("publish subscription event failed", "err", err)
	}
	w.WriteHeader(http.StatusOK)
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
