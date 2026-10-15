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
	"net/url"
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

	// Subscription link records: the invoicing-side entity tying a customer to a
	// Stripe subscription container. One per customer (combined billing). Owned
	// here; synced from fakestripe webhooks.
	subMu         sync.Mutex
	subs          map[string]*Subscription // invoicing id -> record
	subByCustomer map[string]string        // customer -> invoicing id
	subIdem       map[string]string        // Idempotency-Key -> invoicing id
}

// Subscription is the invoicing-side link record for a customer's subscription
// container (which holds flat_fee and/or usage items).
type Subscription struct {
	ID                   string   `json:"id"`                     // invoicing's own id (isub_…)
	CustomerID           string   `json:"customer_id"`            // billing customer id
	StripeSubscriptionID string   `json:"stripe_subscription_id"` // fakestripe id (sub_…)
	Status               string   `json:"status"`                 // pending -> active -> canceled
	Items                []string `json:"items"`                  // flat_fee, usage
	CurrentPeriodEnd     string   `json:"current_period_end"`     // when the next invoice finalizes
	Period               int      `json:"period"`                 // current period number
	CreatedAt            string   `json:"created_at"`
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
		subByCustomer: map[string]string{},
		subIdem:       map[string]string{},
	}
}

// Register wires the service's event subscriptions and HTTP routes.
func (s *Service) Register(mux *http.ServeMux) {
	// Surface the fixed subscription plan price to the frontend (read-only).
	mux.HandleFunc("GET /v1/subscription-plan", s.handleSubscriptionPlan)

	// Subscription item lifecycle (add/remove an item over HTTP; the control plane
	// delegates here). The Stripe subscription container lives in fakestripe; this
	// service owns the link record tying it to the customer.
	mux.HandleFunc("POST /v1/subscriptions", s.handleAddItem)
	mux.HandleFunc("POST /v1/subscriptions/remove-item", s.handleRemoveItem)
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
			HasFlat           bool `json:"has_flat"`
			FlatProratedCents int  `json:"flat_prorated_cents"`
		} `json:"subscriptions"`
	}
	if err := s.getJSON(r.Context(), s.baseURL+"/v1/customers/"+id+"/subscriptions", &subs); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	currency := usage.Currency
	if currency == "" {
		currency = "usd"
	}

	// Mid-cycle shows the flat fee accrued so far this period ($1/s), plus current
	// usage. The finalized invoice at the period boundary bills the full flat fee.
	subscriptionProrated := false
	lines := []map[string]any{}
	var subMicros int64
	for _, sub := range subs.Subscriptions {
		if !sub.HasFlat || sub.FlatProratedCents <= 0 {
			continue
		}
		m := int64(sub.FlatProratedCents) * microsPerCent
		lines = append(lines, map[string]any{
			"description":   "Flat-fee Subscription (prorated)",
			"amount_micros": m,
		})
		subMicros += m
		subscriptionProrated = true
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

// handleAddItem adds an item (flat_fee or usage) to the customer's subscription
// container, creating the container's link record if the customer doesn't have one.
// It tags the Stripe request with this record's id so the async webhook correlates
// back. Honors the Idempotency-Key header end-to-end.
func (s *Service) handleAddItem(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID string `json:"customer_id"`
		ItemType   string `json:"item_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CustomerID == "" {
		http.Error(w, "customer_id required", http.StatusBadRequest)
		return
	}
	if req.ItemType == "" {
		req.ItemType = "flat_fee"
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
	var sub *Subscription
	if id, ok := s.subByCustomer[req.CustomerID]; ok && s.subs[id] != nil {
		sub = s.subs[id] // reuse the customer's existing container
	} else {
		sub = &Subscription{
			ID: "isub_" + events.NewID()[:12], CustomerID: req.CustomerID,
			Status: "pending", CreatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		s.subs[sub.ID] = sub
		s.subByCustomer[req.CustomerID] = sub.ID
	}
	if idemKey != "" {
		s.subIdem[idemKey] = sub.ID
	}
	invoicingSubID := sub.ID
	s.subMu.Unlock()

	stripeID, err := s.createStripeItem(r.Context(), req.CustomerID, invoicingSubID, req.ItemType, idemKey)
	if err != nil {
		s.log.Error("add subscription item failed", "customer", req.CustomerID, "item", req.ItemType, "err", err)
		http.Error(w, "failed to add item: "+err.Error(), http.StatusBadGateway)
		return
	}
	s.subMu.Lock()
	if sub.StripeSubscriptionID == "" {
		sub.StripeSubscriptionID = stripeID
	}
	out := *sub
	s.subMu.Unlock()

	s.log.Info("subscription item requested", "id", invoicingSubID, "stripe_id", stripeID, "customer", req.CustomerID, "item", req.ItemType)
	writeJSON(w, http.StatusCreated, &out)
}

// handleRemoveItem removes an item from the customer's container in Stripe. The
// record syncs (item dropped / container ends) when the webhook arrives.
func (s *Service) handleRemoveItem(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID string `json:"customer_id"`
		ItemType   string `json:"item_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CustomerID == "" || req.ItemType == "" {
		http.Error(w, "customer_id and item_type required", http.StatusBadRequest)
		return
	}
	s.subMu.Lock()
	var stripeID string
	if id, ok := s.subByCustomer[req.CustomerID]; ok {
		if sub := s.subs[id]; sub != nil {
			stripeID = sub.StripeSubscriptionID
		}
	}
	s.subMu.Unlock()
	if stripeID == "" {
		http.Error(w, "no subscription for customer (or not yet confirmed)", http.StatusConflict)
		return
	}
	if err := s.deleteURL(r.Context(), s.baseURL+"/v1/subscriptions/"+stripeID+"/items/"+req.ItemType); err != nil {
		s.log.Error("remove subscription item failed", "stripe_id", stripeID, "item", req.ItemType, "err", err)
		http.Error(w, "failed to remove item", http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "removing", "item_type": req.ItemType})
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

// createStripeItem adds an item to the customer's Stripe subscription container
// (creating it if needed), tagging it with this service's link id so the async
// webhook correlates back, and forwarding the idempotency key. Returns the Stripe
// container id.
func (s *Service) createStripeItem(ctx context.Context, customerID, invoicingSubID, itemType, idemKey string) (string, error) {
	body, err := json.Marshal(map[string]any{
		"customer":  customerID,
		"item_type": itemType,
		"metadata":  map[string]string{"invoicing_subscription_id": invoicingSubID},
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
		return "", fmt.Errorf("stripe add item: %s: %s", resp.Status, string(respBody))
	}
	var obj struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(respBody, &obj); err != nil {
		return "", err
	}
	return obj.ID, nil
}

// finalizeSubscriptionInvoice bills one subscription's billing period: it pulls
// that period's usage from Metronome for the exact window (so Metronome's own
// schedule need not match Stripe's), pushes it to Stripe as invoice items, then
// finalizes ONE invoice = the subscription fee + those usage items. The provider
// outcome is mapped onto the bus. `final` marks a cancellation's prorated closer.
func (s *Service) finalizeSubscriptionInvoice(ctx context.Context, customerID, stripeSubID string, periodStart, periodEnd time.Time, period int, final, billUsage bool) error {
	// 1. If the container bills usage, pull this period's usage from Metronome for
	//    the exact [start, end) window; finalize=true advances Metronome's watermark.
	if billUsage {
		q := url.Values{}
		q.Set("start", periodStart.Format(time.RFC3339))
		q.Set("end", periodEnd.Format(time.RFC3339))
		q.Set("finalize", "true")
		var usage struct {
			Currency string       `json:"currency"`
			Groups   []usageGroup `json:"groups"`
		}
		if err := s.getJSON(ctx, s.meteringURL+"/v1/customers/"+customerID+"/usage?"+q.Encode(), &usage); err != nil {
			return fmt.Errorf("query usage: %w", err)
		}
		currency := usage.Currency
		if currency == "" {
			currency = "usd"
		}
		// 2. Push the period's usage to Stripe as pending invoice items.
		for _, g := range usage.Groups {
			if g.UsageMicros <= 0 {
				continue
			}
			desc := usageDesc(g.Dimensions, period)
			if err := s.addInvoiceItem(ctx, customerID, g.UsageMicros, currency, desc, g.Quantity, g.UnitPriceMicros); err != nil {
				return err
			}
		}
	}

	// 3. Finalize ONE combined invoice for the container: flat fee (computed by
	//    Stripe from its items + the period window) + the pushed usage items.
	var inv struct {
		ID        string `json:"id"`
		AmountDue int    `json:"amount_due"`
		Paid      bool   `json:"paid"`
		Status    string `json:"status"`
	}
	if err := s.postJSON(ctx, s.baseURL+"/v1/invoices", map[string]any{
		"customer":        customerID,
		"subscription_id": stripeSubID,
		"period":          period,
		"period_start":    periodStart.Format(time.RFC3339),
		"period_end":      periodEnd.Format(time.RFC3339),
		"final":           final,
	}, &inv); err != nil {
		return fmt.Errorf("create invoice: %w", err)
	}
	if inv.AmountDue <= 0 {
		s.log.Info("nothing to bill for period; skipping invoice", "customer", customerID, "period", period)
		return nil
	}
	s.log.Info("stripe invoice finalized", "id", inv.ID, "customer", customerID,
		"period", period, "amount_cents", inv.AmountDue, "paid", inv.Paid)

	// 4. Map the provider outcome onto domain events.
	payload, _ := json.Marshal(map[string]any{
		"invoice_id":   inv.ID,
		"status":       inv.Status,
		"amount_cents": inv.AmountDue,
	})
	if err := s.bus.Publish(ctx, events.Event{
		ID:         events.NewID(),
		Type:       events.InvoiceFinalized,
		CustomerID: customerID,
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
		CustomerID: customerID,
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

// handleWebhook receives Stripe webhooks from fakestripe:
//   - customer.subscription.created: mark the link record active, publish the fact.
//   - invoice.upcoming: finalize the invoice for the subscription's ended (or, when
//     final, partial) period — pulling that window's usage and billing it with the
//     subscription fee. A final one also marks the record canceled.
func (s *Service) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if s.webhookSecret != "" && r.Header.Get("X-Webhook-Secret") != s.webhookSecret {
		http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
		return
	}
	var evt struct {
		Type string `json:"type"`
		Data struct {
			Object struct {
				ID               string            `json:"id"`
				Customer         string            `json:"customer"`
				Status           string            `json:"status"`
				Items            []string          `json:"items"`
				Metadata         map[string]string `json:"metadata"`
				CurrentPeriodEnd string            `json:"current_period_end"`
				Period           int               `json:"period"`
			} `json:"object"`
		} `json:"data"`
		BillingPeriod struct {
			Start     string `json:"start"`
			End       string `json:"end"`
			Period    int    `json:"period"`
			Final     bool   `json:"final"`
			BillUsage bool   `json:"bill_usage"`
		} `json:"billing_period"`
	}
	if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
		http.Error(w, "bad webhook payload", http.StatusBadRequest)
		return
	}
	obj := evt.Data.Object

	// Correlate to our link record by the metadata id set at create time, then the
	// Stripe subscription id, then the customer (one container per customer).
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
		if id, ok := s.subByCustomer[obj.Customer]; ok {
			sub = s.subs[id]
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
	// Sync the container's items + period from Stripe.
	sub.Items = obj.Items
	if obj.CurrentPeriodEnd != "" {
		sub.CurrentPeriodEnd = obj.CurrentPeriodEnd
		sub.Period = obj.Period
	}

	switch evt.Type {
	case "customer.subscription.created":
		sub.Status = "active"
		record := *sub
		s.subMu.Unlock()
		s.publishSubscriptionFact(r.Context(), events.SubscriptionCreated, record)

	case "customer.subscription.updated":
		if obj.Status != "" {
			sub.Status = obj.Status
		}
		s.subMu.Unlock()

	case "invoice.upcoming":
		if evt.BillingPeriod.Final {
			sub.Status = "canceled"
			sub.Items = nil
		}
		record := *sub
		s.subMu.Unlock()
		start, _ := time.Parse(time.RFC3339, evt.BillingPeriod.Start)
		end, _ := time.Parse(time.RFC3339, evt.BillingPeriod.End)
		if err := s.finalizeSubscriptionInvoice(r.Context(), record.CustomerID, record.StripeSubscriptionID,
			start, end, evt.BillingPeriod.Period, evt.BillingPeriod.Final, evt.BillingPeriod.BillUsage); err != nil {
			s.log.Error("finalize subscription invoice failed", "err", err)
		}
		if evt.BillingPeriod.Final {
			s.publishSubscriptionFact(r.Context(), events.SubscriptionCancelled, record)
			s.subMu.Lock()
			delete(s.subByCustomer, record.CustomerID)
			s.subMu.Unlock()
		}

	default:
		s.subMu.Unlock()
	}
	w.WriteHeader(http.StatusOK)
}

// publishSubscriptionFact emits a subscription lifecycle event on the bus.
func (s *Service) publishSubscriptionFact(ctx context.Context, t events.Type, record Subscription) {
	payload, _ := json.Marshal(map[string]any{
		"subscription_id":        record.ID,
		"stripe_subscription_id": record.StripeSubscriptionID,
	})
	if err := s.bus.Publish(ctx, events.Event{
		ID:         events.NewID(),
		Type:       t,
		CustomerID: record.CustomerID,
		OccurredAt: time.Now().UTC(),
		Payload:    payload,
	}); err != nil {
		s.log.Error("publish subscription event failed", "err", err)
	}
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
