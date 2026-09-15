// Package fakemetronome is a stand-in for the real Metronome API (metering only —
// invoicing is disabled, Stripe owns the invoice; flat-fee subscriptions are
// managed by Stripe, not here).
//
// Like a real Metronome billable metric, it aggregates usage generically: events
// carry arbitrary properties, and usage is summed and grouped by a configured set
// of group_by dimensions — the fake has no knowledge of what those properties
// mean. Amounts are carried in micros (1 USD = 1,000,000 micros) with no rounding;
// Stripe rounds the invoice total to cents once. close-period returns the grouped
// priced breakdown and opens a fresh period so late usage rolls over. Swap it for
// the real API via METRONOME_BASE_URL.
package fakemetronome

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// costPerRequestMicros is the price per API request in micros ($0.0001 = 100).
const costPerRequestMicros = 100

// Service holds in-memory draft usage per customer.
type Service struct {
	log     *slog.Logger
	groupBy []string // event properties to group usage by (nil = no grouping)
	mu      sync.Mutex
	drafts  map[string]*draft
	seen    map[string]bool // transaction_id dedup (real Metronome dedups 34 days)
}

// draft is the accruing usage for a customer's open period, bucketed by the tuple
// of group_by dimension values.
type draft struct {
	Period    int
	UpdatedAt string
	groups    map[string]*groupAgg // composite key -> aggregate
}

type groupAgg struct {
	Dimensions map[string]string // dimension name -> value for this bucket
	Events     int
	Quantity   float64
}

// group is one dimension-tuple's priced usage for the period, in micros.
type group struct {
	Dimensions      map[string]string `json:"dimensions"`
	Events          int               `json:"events"`
	Quantity        float64           `json:"quantity"`
	UnitPriceMicros int64             `json:"unit_price_micros"`
	UsageMicros     int64             `json:"usage_micros"`
}

// snapshot is the priced usage view returned to callers (amounts in micros).
type snapshot struct {
	CustomerID      string   `json:"customer_id"`
	Period          int      `json:"period"`
	GroupBy         []string `json:"group_by"`
	Groups          []group  `json:"groups"`
	Quantity        float64  `json:"quantity"`
	UnitPriceMicros int64    `json:"unit_price_micros"`
	UsageMicros     int64    `json:"usage_micros"`
	Currency        string   `json:"currency"`
	Finalized       bool     `json:"finalized"`
	UpdatedAt       string   `json:"updated_at"`
}

// New constructs the fake Metronome service. groupBy names the event properties
// the billable metric aggregates by (empty = a single aggregate, no grouping).
func New(log *slog.Logger, groupBy []string) *Service {
	return &Service{log: log, groupBy: groupBy, drafts: map[string]*draft{}, seen: map[string]bool{}}
}

// Register wires the fake's HTTP routes.
func (s *Service) Register(mux *http.ServeMux) {
	// POST /v1/ingest and the costs read mirror real Metronome endpoints
	// (v1.usage.ingest and v1.customers.listCosts).
	mux.HandleFunc("POST /ingest", s.handleIngest)
	mux.HandleFunc("GET /v1/customers/{id}/costs", s.handleCosts)

	// DEMO SHIM: real Metronome has NO on-demand "close period" endpoint —
	// billing periods are contract/schedule-driven and invoices auto-finalize
	// after the grace period. This exists only so the demo can bill on demand
	// instead of waiting for a real period boundary.
	mux.HandleFunc("POST /v1/customers/{id}/close-period", s.handleClosePeriod)
}

// costLine mirrors an entry in Metronome's list-costs line_item_breakdown
// (a faithful subset: name + group_by values + quantity + priced cost in USD).
type costLine struct {
	Name      string            `json:"name"`
	Groups    map[string]string `json:"groups,omitempty"`
	Quantity  float64           `json:"quantity"`
	UnitPrice float64           `json:"unit_price"` // USD per unit
	Cost      float64           `json:"cost"`       // USD
}

type costWindow struct {
	StartingOn        string     `json:"starting_on"`
	EndingBefore      string     `json:"ending_before"`
	LineItemBreakdown []costLine `json:"line_item_breakdown"`
	TotalCost         float64    `json:"total_cost"`
}

// costsResponse mirrors the shape of Metronome's list-costs response (subset).
type costsResponse struct {
	Data []costWindow `json:"data"`
}

type ingestEvent struct {
	TransactionID string         `json:"transaction_id"`
	CustomerID    string         `json:"customer_id"`
	Timestamp     string         `json:"timestamp"`
	EventType     string         `json:"event_type"`
	Properties    map[string]any `json:"properties"`
}

func (s *Service) current(customer string) *draft {
	d := s.drafts[customer]
	if d == nil {
		d = &draft{groups: map[string]*groupAgg{}}
		s.drafts[customer] = d
	}
	return d
}

// dimsFor returns the group_by dimension values for an event and a stable
// composite key. With no group_by configured, everything falls into one bucket.
func (s *Service) dimsFor(e ingestEvent) (map[string]string, string) {
	dims := make(map[string]string, len(s.groupBy))
	parts := make([]string, 0, len(s.groupBy))
	for _, k := range s.groupBy {
		v := propString(e.Properties[k])
		if v == "" {
			v = "unknown"
		}
		dims[k] = v
		parts = append(parts, k+"="+v)
	}
	return dims, strings.Join(parts, "|")
}

// propString coerces a property value to a string for grouping.
func propString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func (s *Service) snapshotLocked(customer string, finalized bool) snapshot {
	d := s.current(customer)
	keys := make([]string, 0, len(d.groups))
	for k := range d.groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	groups := make([]group, 0, len(keys))
	totalQty := 0.0
	var totalMicros int64
	for _, k := range keys {
		g := d.groups[k]
		micros := int64(g.Quantity) * costPerRequestMicros
		groups = append(groups, group{
			Dimensions:      g.Dimensions,
			Events:          g.Events,
			Quantity:        g.Quantity,
			UnitPriceMicros: costPerRequestMicros,
			UsageMicros:     micros,
		})
		totalQty += g.Quantity
		totalMicros += micros
	}
	return snapshot{
		CustomerID:      customer,
		Period:          d.Period,
		GroupBy:         s.groupBy,
		Groups:          groups,
		Quantity:        totalQty,
		UnitPriceMicros: costPerRequestMicros,
		UsageMicros:     totalMicros,
		Currency:        "usd",
		Finalized:       finalized,
		UpdatedAt:       d.UpdatedAt,
	}
}

// handleIngest accepts a batch (JSON array) of usage events, dedups by
// transaction_id, and accrues them onto the customer's open period, grouped by
// the configured group_by dimensions.
func (s *Service) handleIngest(w http.ResponseWriter, r *http.Request) {
	var batch []ingestEvent
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(batch) > 100 {
		http.Error(w, "batch exceeds 100 events", http.StatusBadRequest)
		return
	}

	var accepted, dupes int
	s.mu.Lock()
	for _, e := range batch {
		if e.TransactionID == "" || e.CustomerID == "" {
			continue
		}
		if s.seen[e.TransactionID] {
			dupes++
			continue
		}
		s.seen[e.TransactionID] = true

		d := s.current(e.CustomerID)
		dims, key := s.dimsFor(e)
		q := 1.0
		if v, ok := e.Properties["quantity"].(float64); ok && v != 0 {
			q = v
		}
		g := d.groups[key]
		if g == nil {
			g = &groupAgg{Dimensions: dims}
			d.groups[key] = g
		}
		g.Events++
		g.Quantity += q
		d.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		accepted++
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]int{"accepted": accepted, "duplicates": dupes})
}

// handleCosts returns the customer's current priced usage as a list-costs
// response (like Metronome's v1.customers.listCosts): one window covering the
// open period, with a per-group line_item_breakdown.
func (s *Service) handleCosts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	unitPrice := float64(costPerRequestMicros) / 1e6

	s.mu.Lock()
	d := s.current(id)
	keys := make([]string, 0, len(d.groups))
	for k := range d.groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	lines := make([]costLine, 0, len(keys))
	var total float64
	for _, k := range keys {
		g := d.groups[k]
		cost := float64(int64(g.Quantity)*costPerRequestMicros) / 1e6
		lines = append(lines, costLine{
			Name:      "API requests",
			Groups:    g.Dimensions,
			Quantity:  g.Quantity,
			UnitPrice: unitPrice,
			Cost:      cost,
		})
		total += cost
	}
	updated := d.UpdatedAt
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, costsResponse{Data: []costWindow{{
		StartingOn:        updated,
		EndingBefore:      time.Now().UTC().Format(time.RFC3339),
		LineItemBreakdown: lines,
		TotalCost:         total,
	}}})
}

// handleClosePeriod snapshots the priced usage for the closing period and opens a
// fresh period, so late usage rolls over to the next invoice.
func (s *Service) handleClosePeriod(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	snap := s.snapshotLocked(id, true)
	s.drafts[id] = &draft{Period: snap.Period + 1, groups: map[string]*groupAgg{}}
	s.mu.Unlock()

	s.log.Info("closed period", "customer", id, "period", snap.Period,
		"usage_micros", snap.UsageMicros, "groups", len(snap.Groups))
	writeJSON(w, http.StatusOK, snap)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
