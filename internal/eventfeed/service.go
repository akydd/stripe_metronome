// Package eventfeed provides a read-only, live view of the events flowing across
// the Kafka bus. It runs its own consumer that tails every domain-event topic
// into a bounded in-memory ring buffer and serves that buffer over HTTP for the
// frontend's "Live Events" panel.
//
// There is deliberately no produce path here: the service only consumes, and it
// exposes only GET endpoints. Nothing a viewer does can mutate state, so it is
// safe to expose publicly (unlike the Redpanda Console, whose UI can create and
// delete topics). Each buffered entry carries the record's Kafka-native metadata
// (partition, offset, broker timestamp) — the offsets increase monotonically, so
// a technical viewer can confirm the events genuinely traversed the broker rather
// than being fabricated by the frontend.
package eventfeed

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/akydd/stripe_metronome/internal/config"
	"github.com/akydd/stripe_metronome/internal/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

// maxBuffered caps the in-memory ring of recent events served to the UI. At
// ~1 KB/event this is well under a megabyte.
const maxBuffered = 500

// entry is one observed record: the decoded event plus the Kafka-native metadata
// that proves it really traversed the broker.
type entry struct {
	Seq        int64           `json:"seq"`
	Topic      string          `json:"topic"`
	Partition  int32           `json:"partition"`
	Offset     int64           `json:"offset"`
	Timestamp  time.Time       `json:"timestamp"`
	CustomerID string          `json:"customer_id,omitempty"`
	Type       string          `json:"type,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// Service tails every event topic into a bounded ring buffer and serves it
// read-only.
type Service struct {
	log *slog.Logger

	mu     sync.Mutex
	ring   []entry
	seq    int64
	client *kgo.Client
}

// New constructs the service. When the bus is Kafka it starts a consumer that
// tails every event topic from the current end of the log; for any other bus
// (e.g. the in-memory bus used in single-process local dev) there are no broker
// topics to tail, so the feed simply stays empty.
func New(cfg config.Config, log *slog.Logger) (*Service, error) {
	s := &Service{log: log}
	if cfg.BusKind != "kafka" {
		log.Warn("eventfeed: non-kafka bus, live feed will be empty", "bus", cfg.BusKind)
		return s, nil
	}

	topics := make([]string, 0, len(events.AllTypes()))
	for _, t := range events.AllTypes() {
		topics = append(topics, string(t))
	}

	// Direct (group-less) consumption from the end of each topic: a pure tail
	// with no committed offsets, so every restart starts fresh at "now".
	// AllowAutoTopicCreation makes every event topic exist from the moment this
	// consumer starts, so "AtEnd" resolves to offset 0 and the first message on a
	// freshly-produced topic isn't skipped (the same trick the bus subscriber uses).
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaSeeds...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	s.client = cl
	go s.consume()
	return s, nil
}

// consume polls the broker and appends each record to the ring buffer until the
// client is closed (which unblocks PollFetches).
func (s *Service) consume() {
	ctx := context.Background()
	for {
		fetches := s.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, _ int32, err error) {
			s.log.Error("eventfeed fetch error", "topic", topic, "err", err)
		})
		fetches.EachRecord(s.append)
	}
}

// append records one Kafka message. The payload is decoded best-effort; even if
// decoding fails the broker metadata is still shown.
func (s *Service) append(r *kgo.Record) {
	var ev events.Event
	_ = json.Unmarshal(r.Value, &ev)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	s.ring = append(s.ring, entry{
		Seq:        s.seq,
		Topic:      r.Topic,
		Partition:  r.Partition,
		Offset:     r.Offset,
		Timestamp:  r.Timestamp,
		CustomerID: ev.CustomerID,
		Type:       string(ev.Type),
		Payload:    ev.Payload,
	})
	if len(s.ring) > maxBuffered {
		// Reallocate exactly maxBuffered so the backing array can't grow forever.
		s.ring = append(s.ring[:0:0], s.ring[len(s.ring)-maxBuffered:]...)
	}
}

// Register wires the read-only HTTP routes.
func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/events", s.handleList)
}

type listResponse struct {
	Events  []entry `json:"events"`
	LastSeq int64   `json:"last_seq"`
}

// handleList returns buffered entries with Seq greater than the "since" query
// param (0/absent returns the whole buffer), plus the newest Seq for the caller
// to poll from next.
func (s *Service) handleList(w http.ResponseWriter, r *http.Request) {
	var since int64
	if v := r.URL.Query().Get("since"); v != "" {
		since, _ = strconv.ParseInt(v, 10, 64)
	}

	s.mu.Lock()
	out := make([]entry, 0, len(s.ring))
	for _, e := range s.ring {
		if e.Seq > since {
			out = append(out, e)
		}
	}
	last := s.seq
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(listResponse{Events: out, LastSeq: last})
}

// Close stops the consumer.
func (s *Service) Close() error {
	if s.client != nil {
		s.client.Close()
	}
	return nil
}
