// Package controlplane is the control plane for two kinds of provisionable
// processes:
//
//   - "usage"        — ticks on an interval and publishes usage.ingested events
//     (metronome meters + prices them per unit).
//   - "subscription" — a flat recurring fee; it doesn't tick. Start activates it
//     (subscription.activated), stop cancels it
//     (subscription.cancelled); while active its fee is billed
//     each cycle.
//
// The frontend provisions and starts/stops processes here. This is a demo
// simulator, so there is no per-user access control and state is in-memory.
package controlplane

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/akydd/stripe_metronome/internal/events"
)

// Process types.
const (
	TypeUsage        = "usage"
	TypeSubscription = "subscription"
)

// Manager owns the set of provisioned processes.
type Manager struct {
	bus        events.Bus
	log        *slog.Logger
	http       *http.Client
	billingURL string // for validating a process's customer against the registry
	mu         sync.Mutex
	procs      map[string]*process
}

// process is a provisioned generator. Fields are guarded by Manager.mu.
// A subscription's price is not stored here — it's Stripe's fixed plan price.
type process struct {
	ID            string `json:"id"`
	Type          string `json:"type"`
	CustomerID    string `json:"customer_id"`
	IntervalMS    int    `json:"interval_ms,omitempty"`     // usage only
	EventsPerTick int    `json:"events_per_tick,omitempty"` // usage only
	Running       bool   `json:"running"`
	Emitted       int    `json:"emitted"` // usage: events emitted

	cancel context.CancelFunc
}

// New constructs a Manager. billingURL is the base URL of the customer registry
// used to validate a process's customer at provision time.
func New(bus events.Bus, log *slog.Logger, billingURL string) *Manager {
	return &Manager{
		bus:        bus,
		log:        log,
		http:       &http.Client{Timeout: 10 * time.Second},
		billingURL: billingURL,
		procs:      map[string]*process{},
	}
}

// Register wires the manager's HTTP routes. The frontend reaches these directly
// (proxied by nginx), not through the billing service.
func (m *Manager) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/generators", m.handleProvision)
	mux.HandleFunc("GET /v1/generators", m.handleList)
	mux.HandleFunc("GET /v1/generators/{id}", m.handleGet)
	mux.HandleFunc("POST /v1/generators/{id}/start", m.handleStart)
	mux.HandleFunc("POST /v1/generators/{id}/stop", m.handleStop)
	mux.HandleFunc("POST /v1/generators/{id}/emit", m.handleEmit)
	mux.HandleFunc("DELETE /v1/generators/{id}", m.handleDelete)
}

// Close stops all running usage processes (used on shutdown).
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.procs {
		if p.cancel != nil {
			p.cancel()
		}
	}
}

func (m *Manager) handleProvision(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type          string `json:"type"`
		CustomerID    string `json:"customer_id"`
		IntervalMS    int    `json:"interval_ms"`
		EventsPerTick int    `json:"events_per_tick"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Type == "" {
		req.Type = TypeUsage
	}
	if req.Type != TypeUsage && req.Type != TypeSubscription {
		http.Error(w, "type must be 'usage' or 'subscription'", http.StatusBadRequest)
		return
	}
	if req.CustomerID == "" {
		http.Error(w, "customer_id required", http.StatusBadRequest)
		return
	}
	if !m.customerExists(r.Context(), req.CustomerID) {
		http.Error(w, "unknown customer", http.StatusBadRequest)
		return
	}

	p := &process{ID: events.NewID(), Type: req.Type, CustomerID: req.CustomerID}
	if req.Type == TypeUsage {
		p.IntervalMS = req.IntervalMS
		if p.IntervalMS <= 0 {
			p.IntervalMS = 5000
		}
		p.EventsPerTick = req.EventsPerTick
		if p.EventsPerTick <= 0 {
			p.EventsPerTick = 1
		}
		if p.EventsPerTick > 5000 {
			p.EventsPerTick = 5000
		}
	}
	// A subscription carries no amount — Stripe owns the fixed plan price.

	m.mu.Lock()
	m.procs[p.ID] = p
	out := *p
	m.mu.Unlock()

	m.log.Info("provisioned process", "id", p.ID, "type", p.Type, "customer", p.CustomerID)
	writeJSON(w, http.StatusCreated, &out)
}

func (m *Manager) handleStart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	m.mu.Lock()
	p := m.procs[id]
	if p == nil {
		m.mu.Unlock()
		http.Error(w, "process not found", http.StatusNotFound)
		return
	}
	started := false
	if !p.Running {
		p.Running = true
		started = true
		if p.Type == TypeUsage {
			ctx, cancel := context.WithCancel(context.Background())
			p.cancel = cancel
			go m.run(ctx, id, p.CustomerID, time.Duration(p.IntervalMS)*time.Millisecond, p.EventsPerTick)
		}
	}
	typ, cust := p.Type, p.CustomerID
	out := *p
	m.mu.Unlock()

	if started && typ == TypeSubscription {
		m.publishSubscription(r.Context(), events.SubscriptionActivated, id, cust)
	}
	m.log.Info("started process", "id", id, "type", typ)
	writeJSON(w, http.StatusOK, &out)
}

func (m *Manager) handleStop(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	m.mu.Lock()
	p := m.procs[id]
	if p == nil {
		m.mu.Unlock()
		http.Error(w, "process not found", http.StatusNotFound)
		return
	}
	stopped := false
	if p.Running {
		p.Running = false
		stopped = true
		if p.cancel != nil {
			p.cancel()
			p.cancel = nil
		}
	}
	typ, cust := p.Type, p.CustomerID
	out := *p
	m.mu.Unlock()

	if stopped && typ == TypeSubscription {
		m.publishSubscription(r.Context(), events.SubscriptionCancelled, id, cust)
	}
	m.log.Info("stopped process", "id", id, "type", typ)
	writeJSON(w, http.StatusOK, &out)
}

func (m *Manager) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	m.mu.Lock()
	p := m.procs[id]
	if p == nil {
		m.mu.Unlock()
		http.Error(w, "process not found", http.StatusNotFound)
		return
	}
	wasRunningSub := p.Running && p.Type == TypeSubscription
	cust := p.CustomerID
	if p.cancel != nil {
		p.cancel()
	}
	delete(m.procs, id)
	m.mu.Unlock()

	if wasRunningSub {
		m.publishSubscription(r.Context(), events.SubscriptionCancelled, id, cust)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *Manager) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	m.mu.Lock()
	p := m.procs[id]
	if p == nil {
		m.mu.Unlock()
		http.Error(w, "process not found", http.StatusNotFound)
		return
	}
	out := *p
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, &out)
}

func (m *Manager) handleList(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	out := make([]process, 0, len(m.procs))
	for _, p := range m.procs {
		out = append(out, *p)
	}
	m.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	writeJSON(w, http.StatusOK, out)
}

// publishSubscription emits a subscription lifecycle event to the bus. No amount
// is carried — Stripe owns the fixed plan price.
func (m *Manager) publishSubscription(ctx context.Context, t events.Type, id, customer string) {
	payload, _ := json.Marshal(map[string]any{"subscription_id": id})
	if err := m.bus.Publish(ctx, events.Event{
		ID:         events.NewID(),
		Type:       t,
		CustomerID: customer,
		OccurredAt: time.Now().UTC(),
		Payload:    payload,
	}); err != nil {
		m.log.Error("publish subscription event failed", "type", t, "err", err)
	}
}

// customerExists checks the billing customer registry. Fails closed: if the
// registry can't confirm the customer, provisioning is rejected.
func (m *Manager) customerExists(ctx context.Context, id string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.billingURL+"/v1/customers/"+id, nil)
	if err != nil {
		return false
	}
	resp, err := m.http.Do(req)
	if err != nil {
		m.log.Error("customer lookup failed", "customer", id, "err", err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// run emits usage events until ctx is cancelled (usage process stopped).
func (m *Manager) run(ctx context.Context, id, customer string, interval time.Duration, perTick int) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.emitBatch(ctx, id, customer, perTick, time.Now().UTC())
		}
	}
}

// emitBatch publishes n usage.ingested events in a single batched produce and
// bumps the process's emitted count. occurredAt is when the usage happened;
// backdating it simulates a late arrival. The source does NOT decide lateness —
// the metronome ingestion service determines it from occurred_at vs the period.
func (m *Manager) emitBatch(ctx context.Context, id, customer string, n int, occurredAt time.Time) {
	if n < 1 {
		n = 1
	}
	batch := make([]events.Event, 0, n)
	for i := 0; i < n; i++ {
		payload, _ := json.Marshal(map[string]any{
			"event_type":   "api_requests",
			"quantity":     1 + rand.Intn(5),
			"generator_id": id,
		})
		batch = append(batch, events.Event{
			ID:         events.NewID(),
			Type:       events.UsageIngested,
			CustomerID: customer,
			OccurredAt: occurredAt,
			Payload:    payload,
		})
	}
	if err := m.bus.PublishBatch(ctx, batch); err != nil {
		m.log.Error("publish usage batch failed", "process", id, "err", err)
		return
	}
	m.mu.Lock()
	if p := m.procs[id]; p != nil {
		p.Emitted += n
	}
	m.mu.Unlock()
}

// handleEmit fires one or more usage events on demand (?count=N), independent of
// the tick loop and regardless of whether the process is running. Useful for
// generating late-arriving usage after a billing cycle has been closed.
func (m *Manager) handleEmit(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	m.mu.Lock()
	p := m.procs[id]
	if p == nil {
		m.mu.Unlock()
		http.Error(w, "process not found", http.StatusNotFound)
		return
	}
	if p.Type != TypeUsage {
		m.mu.Unlock()
		http.Error(w, "emit is only valid for usage generators", http.StatusBadRequest)
		return
	}
	cust := p.CustomerID
	m.mu.Unlock()

	count := 1
	if q := r.URL.Query().Get("count"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			count = n
		}
	}
	if count > 5000 {
		count = 5000
	}
	// The emit endpoint simulates late-arriving usage by backdating when it
	// occurred (an hour ago). Whether that's actually "late" is decided
	// downstream by comparing this timestamp to the customer's period boundary.
	occurredAt := time.Now().UTC().Add(-time.Hour)
	m.emitBatch(r.Context(), id, cust, count, occurredAt)
	writeJSON(w, http.StatusOK, map[string]any{"process": id, "emitted": count, "occurred_at": occurredAt})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
