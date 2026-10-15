// Package fakemetronome is a stand-in for the real Metronome API (metering only —
// invoicing is disabled, Stripe owns the invoice; flat-fee subscriptions are
// managed by Stripe, not here).
//
// Like a real Metronome billable metric, it aggregates usage generically: events
// carry arbitrary properties and a timestamp, and usage is summed and grouped by a
// configured set of group_by dimensions — the fake has no knowledge of what those
// properties mean. Amounts are carried in micros (1 USD = 1,000,000 micros) with
// no rounding; Stripe rounds the invoice total to cents once.
//
// Crucially, usage is stored per-event with its timestamp, so costs can be queried
// for an ARBITRARY time window (like Metronome's list-costs with a range). That's
// what lets a caller bill usage for a period defined elsewhere (e.g. a Stripe
// subscription's cycle) without Metronome's own schedule needing to match. Swap it
// for the real API via METRONOME_BASE_URL.
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

// Service holds in-memory per-event usage per customer.
type Service struct {
	log     *slog.Logger
	groupBy []string // event properties to group usage by (nil = no grouping)
	mu      sync.Mutex
	events  map[string][]usageRecord // customer -> timestamped usage records
	// billedThrough is the end of the last window billed for a customer; the
	// unwindowed costs read (the mid-cycle preview) returns only usage after it,
	// so the preview resets to ~zero once a period is finalized.
	billedThrough map[string]time.Time
	seen          map[string]bool // transaction_id dedup (real Metronome dedups 34 days)
}

// usageRecord is one metered event: when it occurred, its group_by dimensions,
// and its quantity. Stored per-event so costs can be aggregated over any window.
type usageRecord struct {
	At       time.Time
	DimKey   string
	Dims     map[string]string
	Quantity float64
}

type groupAgg struct {
	Dimensions map[string]string
	Events     int
	Quantity   float64
}

// New constructs the fake Metronome service. groupBy names the event properties
// the billable metric aggregates by (empty = a single aggregate, no grouping).
func New(log *slog.Logger, groupBy []string) *Service {
	return &Service{
		log:           log,
		groupBy:       groupBy,
		events:        map[string][]usageRecord{},
		billedThrough: map[string]time.Time{},
		seen:          map[string]bool{},
	}
}

// Register wires the fake's HTTP routes: POST /ingest and the costs read mirror
// real Metronome (v1.usage.ingest and v1.customers.listCosts). The costs read
// accepts an optional ?start=&end= window and ?finalize=true.
func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /ingest", s.handleIngest)
	mux.HandleFunc("GET /v1/customers/{id}/costs", s.handleCosts)
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

// handleIngest accepts a batch (JSON array) of usage events, dedups by
// transaction_id, and stores each as a timestamped record grouped by the
// configured group_by dimensions.
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

		at, err := time.Parse(time.RFC3339, e.Timestamp)
		if err != nil {
			at = time.Now().UTC()
		}
		dims, key := s.dimsFor(e)
		q := 1.0
		if v, ok := e.Properties["quantity"].(float64); ok && v != 0 {
			q = v
		}
		s.events[e.CustomerID] = append(s.events[e.CustomerID], usageRecord{
			At: at.UTC(), DimKey: key, Dims: dims, Quantity: q,
		})
		accepted++
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]int{"accepted": accepted, "duplicates": dupes})
}

// handleCosts returns priced usage as a list-costs response. With ?start=&end=
// (RFC3339) it aggregates only events whose timestamp is in [start, end); without
// a window it returns usage since the customer's billedThrough watermark (the
// current, not-yet-billed usage — used by the mid-cycle preview). ?finalize=true
// advances the watermark to `end`, so the just-billed usage drops off the preview.
func (s *Service) handleCosts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := r.URL.Query()
	start, hasStart := parseTime(q.Get("start"))
	end, hasEnd := parseTime(q.Get("end"))
	finalize := q.Get("finalize") == "true"

	s.mu.Lock()
	lo := start
	if !hasStart {
		lo = s.billedThrough[id] // preview: everything since the last finalize
	}
	agg := map[string]*groupAgg{}
	for _, rec := range s.events[id] {
		if rec.At.Before(lo) {
			continue
		}
		if hasEnd && !rec.At.Before(end) { // rec.At >= end
			continue
		}
		g := agg[rec.DimKey]
		if g == nil {
			g = &groupAgg{Dimensions: rec.Dims}
			agg[rec.DimKey] = g
		}
		g.Events++
		g.Quantity += rec.Quantity
	}
	if finalize && hasEnd {
		s.billedThrough[id] = end
	}
	s.mu.Unlock()

	unitPrice := float64(costPerRequestMicros) / 1e6
	keys := make([]string, 0, len(agg))
	for k := range agg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	lines := make([]costLine, 0, len(keys))
	var total float64
	for _, k := range keys {
		g := agg[k]
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

	endStr := time.Now().UTC().Format(time.RFC3339)
	if hasEnd {
		endStr = end.Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, costsResponse{Data: []costWindow{{
		StartingOn:        lo.Format(time.RFC3339),
		EndingBefore:      endStr,
		LineItemBreakdown: lines,
		TotalCost:         total,
	}}})
}

// parseTime parses an RFC3339 timestamp, reporting whether it was present/valid.
func parseTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
