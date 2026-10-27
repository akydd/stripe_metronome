// Package fakestripe is a stand-in for the Stripe API. Stripe owns the invoice AND
// subscriptions. A subscription is a **container with a billing period and items**
// (like a real Stripe Subscription with SubscriptionItems): a `flat_fee` item
// and/or a `usage` (metered) item. The billing cycle belongs to the container, so
// one customer has one container that bills all its items on a single combined
// invoice each period. Removing the flat-fee item keeps the container alive as long
// as a usage item remains; the subscription ends only when it has no items.
//
// Subscription/invoice lifecycle changes are reported to the invoicing service via
// webhooks, mirroring real Stripe. Swap for real Stripe (test mode) via
// STRIPE_BASE_URL. Demo behavior: a customer whose id contains "fail" doesn't pay.
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

// Item types.
const (
	itemFlatFee = "flat_fee"
	itemUsage   = "usage"
)

// subscriptionPriceCents is the flat-fee item's price for a full period ($30).
const subscriptionPriceCents = 3000

// subscriptionPeriod is each subscription's billing period length. Real Stripe
// periods are month-scale; this is compressed so a cycle is watchable in the demo.
const subscriptionPeriod = 30 * time.Second

// prorationCentsPerSecond is the time-based proration rate: $1/second, which sums
// to the full $30 over the 30-second period.
const prorationCentsPerSecond = 100

// microsPerCent converts the micros money unit to cents (1 cent = 10,000 micros).
const microsPerCent = 10000

// flatFeeForWindow returns the flat-fee charge for the portion of [start, end)
// during which the item was active (added at addedAt), at $1/second, capped at the
// full-period fee. An item present for the whole period bills the full $30.
func flatFeeForWindow(addedAt, start, end time.Time) int {
	activeStart := addedAt
	if activeStart.Before(start) {
		activeStart = start
	}
	secs := int(end.Sub(activeStart).Seconds())
	if secs < 0 {
		secs = 0
	}
	c := secs * prorationCentsPerSecond
	if c > subscriptionPriceCents {
		c = subscriptionPriceCents
	}
	return c
}

// Service holds pending invoice items, subscription containers, and finalized invoices.
type Service struct {
	log           *slog.Logger
	http          *http.Client
	webhookURL    string
	webhookSecret string

	mu         sync.Mutex
	seq        int
	subs       map[string]*subState     // container id -> container
	byCustomer map[string]string        // customer -> container id (one container/customer)
	subIdem    map[string]string        // idempotency key -> container id
	drafts     map[string]*draftInvoice // invoice id -> open draft awaiting finalize
	invoices   map[string][]map[string]any
}

// draftInvoice is an invoice Stripe has created but not yet finalized. At creation
// it already carries the subscription's own lines (flat fee + any proration); the
// integration attaches metered usage items to it during the finalization window,
// then finalizes it. Mirrors a real Stripe draft invoice (auto_advance=false).
type draftInvoice struct {
	ID             string
	Customer       string
	SubscriptionID string
	Period         int
	PeriodStart    time.Time
	PeriodEnd      time.Time
	Final          bool // closes the container when finalized
	BillUsage      bool // whether the integration should attach usage to this draft
	Lines          []line
}

// subState is a subscription container: a billing period plus a set of items.
type subState struct {
	ID          string
	Customer    string
	PeriodStart time.Time
	PeriodEnd   time.Time
	Period      int
	Items       map[string]*subItem // type -> item (at most one flat_fee, one usage)
	// PendingProrationCents holds the flat-fee proration owed from a flat-fee item
	// that was removed mid-period; it's billed on the next combined invoice.
	PendingProrationCents int
	Metadata              map[string]string
}

type subItem struct {
	Type    string
	AddedAt time.Time
}

func (st *subState) has(t string) bool { _, ok := st.Items[t]; return ok }

type line struct {
	ID              string  `json:"id"`
	Customer        string  `json:"customer"`
	AmountMicros    int64   `json:"amount_micros"`
	Currency        string  `json:"currency"`
	Description     string  `json:"description"`
	Quantity        float64 `json:"quantity,omitempty"`
	UnitPriceMicros int64   `json:"unit_price_micros,omitempty"`
}

// New constructs the fake Stripe service.
func New(log *slog.Logger, webhookURL, webhookSecret string) *Service {
	s := &Service{
		log:           log,
		http:          &http.Client{Timeout: 10 * time.Second},
		webhookURL:    webhookURL,
		webhookSecret: webhookSecret,
		subs:          map[string]*subState{},
		byCustomer:    map[string]string{},
		subIdem:       map[string]string{},
		drafts:        map[string]*draftInvoice{},
		invoices:      map[string][]map[string]any{},
	}
	go s.advancePeriods()
	return s
}

// advancePeriods rolls each container onto its next billing period when the current
// one elapses and, for the period that just ended, opens a draft invoice and fires
// an invoice.created webhook — the real-Stripe signal that an invoice now exists and
// is open for line items before it finalizes. Containers with no items are skipped
// (they're awaiting their final invoice from cancellation).
func (s *Service) advancePeriods() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for now := range t.C {
		now = now.UTC()
		var opened []*draftInvoice
		s.mu.Lock()
		for _, st := range s.subs {
			if len(st.Items) == 0 {
				continue
			}
			for !now.Before(st.PeriodEnd) {
				endedStart, endedEnd, endedPeriod := st.PeriodStart, st.PeriodEnd, st.Period
				st.PeriodStart = st.PeriodEnd
				st.PeriodEnd = st.PeriodStart.Add(subscriptionPeriod)
				st.Period++
				s.log.Info("subscription period ended", "id", st.ID, "period", endedPeriod, "items", len(st.Items))
				opened = append(opened, s.openDraftLocked(st, endedStart, endedEnd, endedPeriod, false, st.has(itemUsage)))
			}
		}
		s.mu.Unlock()
		for _, d := range opened {
			s.fireInvoiceCreated(d)
		}
	}
}

// openDraftLocked creates a draft invoice for a container's billing window, already
// populated with the subscription's own lines (flat-fee item for [start, end), plus
// any proration owed from a flat-fee item removed mid-period). Metered usage is
// attached later, before finalize. Caller must hold s.mu.
func (s *Service) openDraftLocked(st *subState, start, end time.Time, period int, final, billUsage bool) *draftInvoice {
	var lines []line
	if fi := st.Items[itemFlatFee]; fi != nil {
		if cents := flatFeeForWindow(fi.AddedAt, start, end); cents > 0 {
			lines = append(lines, s.subLine(st.Customer, cents, "Flat-fee Subscription"))
		}
	}
	if st.PendingProrationCents > 0 {
		lines = append(lines, s.subLine(st.Customer, st.PendingProrationCents, "Flat-fee Subscription (proration)"))
		st.PendingProrationCents = 0
	}
	d := &draftInvoice{
		ID: s.nextID("in"), Customer: st.Customer, SubscriptionID: st.ID,
		Period: period, PeriodStart: start, PeriodEnd: end, Final: final, BillUsage: billUsage,
		Lines: lines,
	}
	s.drafts[d.ID] = d
	return d
}

// Register wires the fake's HTTP routes.
func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/invoiceitems", s.handleCreateItem)
	mux.HandleFunc("POST /v1/invoices/{id}/finalize", s.handleFinalizeInvoice)
	mux.HandleFunc("GET /v1/subscription-plan", s.handleSubscriptionPlan)
	// A subscription is a container with items. POST adds an item (creating the
	// container if needed); DELETE removes one item by type.
	mux.HandleFunc("POST /v1/subscriptions", s.handleAddItem)
	mux.HandleFunc("DELETE /v1/subscriptions/{id}/items/{type}", s.handleRemoveItem)
	mux.HandleFunc("GET /v1/customers/{id}/subscriptions", s.handleListSubscriptions)
	mux.HandleFunc("GET /v1/customers/{id}/invoices", s.handleListInvoices)
}

func (s *Service) nextID(prefix string) string {
	s.seq++
	return fmt.Sprintf("%s_%d", prefix, s.seq)
}

func randID(prefix string) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

// handleCreateItem adds an invoice item (metered usage) to an open draft invoice,
// identified by the `invoice` field — like a real Stripe invoice item scoped to a
// specific draft. The item appears on that invoice when it finalizes.
func (s *Service) handleCreateItem(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Invoice         string  `json:"invoice"`
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
	d := s.drafts[req.Invoice]
	if d == nil {
		s.mu.Unlock()
		http.Error(w, "no such open invoice", http.StatusNotFound)
		return
	}
	item := line{
		ID: s.nextID("ii"), Customer: req.Customer, AmountMicros: req.AmountMicros,
		Currency: req.Currency, Description: req.Description, Quantity: req.Quantity, UnitPriceMicros: req.UnitPriceMicros,
	}
	d.Lines = append(d.Lines, item)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, item)
}

// handleAddItem adds an item (flat_fee or usage) to the customer's container,
// creating the container if they don't have one. A flat-fee item added mid-period
// is prorated for the remaining period at finalize (via its AddedAt). Honors the
// Idempotency-Key header. Fires customer.subscription.created (new container) or
// customer.subscription.updated (existing).
func (s *Service) handleAddItem(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Customer string            `json:"customer"`
		ItemType string            `json:"item_type"`
		Metadata map[string]string `json:"metadata"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Customer == "" {
		http.Error(w, "customer required", http.StatusBadRequest)
		return
	}
	if req.ItemType == "" {
		req.ItemType = itemFlatFee
	}
	if req.ItemType != itemFlatFee && req.ItemType != itemUsage {
		http.Error(w, "item_type must be flat_fee or usage", http.StatusBadRequest)
		return
	}
	idemKey := r.Header.Get("Idempotency-Key")
	// Align to the second: period boundaries round-trip through RFC3339 (second
	// precision), so an item's AddedAt must be second-aligned too, else a
	// whole-period item looks a fraction of a second short and under-bills.
	now := time.Now().UTC().Truncate(time.Second)

	s.mu.Lock()
	if idemKey != "" {
		if id, ok := s.subIdem[idemKey]; ok {
			if st := s.subs[id]; st != nil {
				obj := subObject(st)
				s.mu.Unlock()
				writeJSON(w, http.StatusOK, obj) // idempotent replay
				return
			}
		}
	}
	created := false
	var st *subState
	if id, ok := s.byCustomer[req.Customer]; ok && s.subs[id] != nil {
		st = s.subs[id]
		if req.Metadata != nil && st.Metadata == nil {
			st.Metadata = req.Metadata
		}
	} else {
		st = &subState{
			ID: randID("sub"), Customer: req.Customer, Metadata: req.Metadata,
			PeriodStart: now, PeriodEnd: now.Add(subscriptionPeriod), Period: 1,
			Items: map[string]*subItem{},
		}
		s.subs[st.ID] = st
		s.byCustomer[req.Customer] = st.ID
		created = true
	}
	if !st.has(req.ItemType) {
		st.Items[req.ItemType] = &subItem{Type: req.ItemType, AddedAt: now}
	}
	if idemKey != "" {
		s.subIdem[idemKey] = st.ID
	}
	obj := subObject(st)
	s.mu.Unlock()

	s.log.Info("subscription item added", "id", st.ID, "customer", req.Customer, "item", req.ItemType, "new_container", created)
	if created {
		s.fireWebhook("customer.subscription.created", obj)
	} else {
		s.fireWebhook("customer.subscription.updated", obj)
	}
	writeJSON(w, http.StatusCreated, obj)
}

// handleRemoveItem removes one item from a container. Removing the flat-fee item
// stashes its prorated partial-period charge to bill on the next combined invoice,
// and the container keeps running if a usage item remains. Removing the last item
// ends the subscription: a final invoice is opened for the closing amount, and the
// container is dropped when that invoice finalizes.
func (s *Service) handleRemoveItem(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	itemType := r.PathValue("type")
	now := time.Now().UTC()

	s.mu.Lock()
	st := s.subs[id]
	if st == nil {
		s.mu.Unlock()
		http.Error(w, "subscription not found", http.StatusNotFound)
		return
	}
	item := st.Items[itemType]
	if item == nil {
		obj := subObject(st)
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, obj) // idempotent: already absent
		return
	}
	if itemType == itemFlatFee {
		st.PendingProrationCents += flatFeeForWindow(item.AddedAt, st.PeriodStart, now)
	}
	delete(st.Items, itemType)
	empty := len(st.Items) == 0
	obj := subObject(st)
	var finalDraft *draftInvoice
	if empty {
		// End the subscription: open a final invoice for the partial period. Bill
		// usage only if the item just removed was the usage item (otherwise none).
		finalDraft = s.openDraftLocked(st, st.PeriodStart, now, st.Period, true, itemType == itemUsage)
	}
	s.mu.Unlock()

	s.log.Info("subscription item removed", "id", id, "item", itemType, "container_empty", empty)
	if finalDraft != nil {
		s.fireInvoiceCreated(finalDraft)
	} else {
		s.fireWebhook("customer.subscription.updated", obj)
	}
	writeJSON(w, http.StatusOK, obj)
}

// handleFinalizeInvoice finalizes an open draft invoice (mirrors Stripe's finalize
// endpoint). It totals the draft's lines — the subscription fee set at creation plus
// any usage items attached since — stores the finalized invoice, closes the
// container if this was its final invoice, and fires invoice.finalized followed by
// invoice.payment_succeeded or invoice.payment_failed. A demo customer whose id
// contains "fail" doesn't pay.
func (s *Service) handleFinalizeInvoice(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	s.mu.Lock()
	d := s.drafts[id]
	if d == nil {
		s.mu.Unlock()
		http.Error(w, "no such open invoice", http.StatusNotFound)
		return
	}
	delete(s.drafts, id)

	var totalMicros int64
	currency := "usd"
	for _, l := range d.Lines {
		totalMicros += l.AmountMicros
		if l.Currency != "" {
			currency = l.Currency
		}
	}
	amountDue := int(math.Round(float64(totalMicros) / float64(microsPerCent)))
	paid := !strings.Contains(strings.ToLower(d.Customer), "fail")
	status := "paid"
	if !paid {
		status = "open"
	}
	inv := map[string]any{
		"id": d.ID, "object": "invoice", "customer": d.Customer, "period": d.Period,
		"amount_due": amountDue, "amount_due_micros": totalMicros, "currency": currency,
		"status": status, "paid": paid, "lines": d.Lines,
		"created": time.Now().UTC().Format(time.RFC3339),
	}
	if amountDue > 0 {
		s.invoices[d.Customer] = append(s.invoices[d.Customer], inv)
	}
	if d.Final {
		delete(s.subs, d.SubscriptionID)
		delete(s.byCustomer, d.Customer)
	}
	s.mu.Unlock()

	s.log.Info("finalized invoice", "id", d.ID, "customer", d.Customer, "amount_due", amountDue, "paid", paid)
	writeJSON(w, http.StatusOK, inv)

	s.fireInvoiceEvent("invoice.finalized", inv, d.Final)
	if paid {
		s.fireInvoiceEvent("invoice.payment_succeeded", inv, d.Final)
	} else {
		s.fireInvoiceEvent("invoice.payment_failed", inv, d.Final)
	}
}

// subLine builds a subscription fee line. Caller must hold s.mu.
func (s *Service) subLine(customer string, cents int, desc string) line {
	return line{ID: s.nextID("sub_ii"), Customer: customer, AmountMicros: int64(cents) * microsPerCent, Currency: "usd", Description: desc}
}

// handleListInvoices returns the customer's finalized invoices (most recent last).
func (s *Service) handleListInvoices(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	out := append([]map[string]any{}, s.invoices[id]...)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// handleListSubscriptions returns the customer's container with its items and the
// flat-fee amount accrued so far this period (for the mid-cycle preview).
func (s *Service) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	cust := r.PathValue("id")
	now := time.Now().UTC()
	s.mu.Lock()
	type sub struct {
		SubscriptionID    string   `json:"subscription_id"`
		Items             []string `json:"items"`
		HasFlat           bool     `json:"has_flat"`
		HasUsage          bool     `json:"has_usage"`
		FlatProratedCents int      `json:"flat_prorated_cents"` // accrued so far this period
		FlatFullCents     int      `json:"flat_full_cents"`
		CurrentPeriodEnd  string   `json:"current_period_end"`
		Period            int      `json:"period"`
	}
	out := make([]sub, 0)
	if id, ok := s.byCustomer[cust]; ok {
		if st := s.subs[id]; st != nil && len(st.Items) > 0 {
			flatProrated := 0
			if fi := st.Items[itemFlatFee]; fi != nil {
				flatProrated = flatFeeForWindow(fi.AddedAt, st.PeriodStart, now)
			}
			out = append(out, sub{
				SubscriptionID: st.ID, Items: itemTypes(st), HasFlat: st.has(itemFlatFee), HasUsage: st.has(itemUsage),
				FlatProratedCents: flatProrated, FlatFullCents: subscriptionPriceCents,
				CurrentPeriodEnd: st.PeriodEnd.Format(time.RFC3339), Period: st.Period,
			})
		}
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": out})
}

func (s *Service) handleSubscriptionPlan(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"amount_cents": subscriptionPriceCents, "currency": "usd"})
}

// itemTypes returns a container's item types, sorted. Caller must hold s.mu.
func itemTypes(st *subState) []string {
	out := make([]string, 0, len(st.Items))
	for t := range st.Items {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// subObject renders a container as the Stripe-like object used in responses and
// webhooks. Caller must hold s.mu.
func subObject(st *subState) map[string]any {
	status := "active"
	if len(st.Items) == 0 {
		status = "canceled"
	}
	return map[string]any{
		"id": st.ID, "object": "subscription", "customer": st.Customer, "status": status,
		"items": itemTypes(st), "metadata": st.Metadata,
		"current_period_start": st.PeriodStart.Format(time.RFC3339),
		"current_period_end":   st.PeriodEnd.Format(time.RFC3339),
		"period":               st.Period,
	}
}

func (s *Service) fireWebhook(eventType string, object map[string]any) {
	s.deliver(map[string]any{
		"id": randID("evt"), "object": "event", "type": eventType,
		"created": time.Now().UTC().Format(time.RFC3339),
		"data":    map[string]any{"object": object},
	})
}

// fireInvoiceCreated delivers an invoice.created event: a draft invoice now exists
// for a just-ended (or, when final, partial) period and is open for line items. The
// billing_period block tells the receiver the window to pull usage for, whether to
// bill usage at all, and whether this is the container's final invoice.
func (s *Service) fireInvoiceCreated(d *draftInvoice) {
	s.deliver(map[string]any{
		"id": randID("evt"), "object": "event", "type": "invoice.created",
		"created": time.Now().UTC().Format(time.RFC3339),
		"data": map[string]any{"object": map[string]any{
			"id": d.ID, "object": "invoice", "customer": d.Customer, "status": "draft", "period": d.Period,
		}},
		"billing_period": map[string]any{
			"start": d.PeriodStart.Format(time.RFC3339), "end": d.PeriodEnd.Format(time.RFC3339),
			"period": d.Period, "final": d.Final, "bill_usage": d.BillUsage,
		},
	})
}

// fireInvoiceEvent delivers a finalized-invoice lifecycle event (invoice.finalized,
// invoice.payment_succeeded, invoice.payment_failed) carrying the invoice object.
func (s *Service) fireInvoiceEvent(eventType string, inv map[string]any, final bool) {
	s.deliver(map[string]any{
		"id": randID("evt"), "object": "event", "type": eventType,
		"created":        time.Now().UTC().Format(time.RFC3339),
		"data":           map[string]any{"object": inv},
		"billing_period": map[string]any{"final": final},
	})
}

func (s *Service) deliver(event map[string]any) {
	if s.webhookURL == "" {
		return
	}
	eventType, _ := event["type"].(string)
	go func() {
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
