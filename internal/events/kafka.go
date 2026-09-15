package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaBus is a Bus backed by a Kafka-API broker (Redpanda in this project).
// Every event Type maps to a same-named topic, so the broker UI mirrors the
// architecture. Each subscription consumes under its own group so services stay
// independent and every subscriber sees every event of its type.
type KafkaBus struct {
	seeds    []string
	group    string
	log      *slog.Logger
	producer *kgo.Client

	mu      sync.Mutex
	cancels []context.CancelFunc
	clients []*kgo.Client
	wg      sync.WaitGroup
}

// NewKafkaBus connects a producer to the given brokers. Connections are lazy, so
// this succeeds even if the broker isn't up yet.
func NewKafkaBus(seeds []string, group string, log *slog.Logger) (*KafkaBus, error) {
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaBus{seeds: seeds, group: group, log: log, producer: producer}, nil
}

// Publish produces e to the topic named after its Type, keyed by customer.
func (b *KafkaBus) Publish(ctx context.Context, e Event) error {
	val, err := json.Marshal(e)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: string(e.Type), Key: []byte(e.CustomerID), Value: val}
	return b.producer.ProduceSync(ctx, rec).FirstErr()
}

// PublishBatch produces all events in a single ProduceSync — far higher
// throughput than one-by-one for high-volume usage.
func (b *KafkaBus) PublishBatch(ctx context.Context, es []Event) error {
	if len(es) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(es))
	for _, e := range es {
		val, err := json.Marshal(e)
		if err != nil {
			return err
		}
		recs = append(recs, &kgo.Record{Topic: string(e.Type), Key: []byte(e.CustomerID), Value: val})
	}
	return b.producer.ProduceSync(ctx, recs...).FirstErr()
}

// Subscribe starts a consumer goroutine that delivers events of type t to h.
func (b *KafkaBus) Subscribe(t Type, h Handler) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(b.seeds...),
		kgo.ConsumerGroup(b.group+"."+string(t)),
		kgo.ConsumeTopics(string(t)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		b.log.Error("kafka subscribe failed", "topic", t, "err", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.mu.Lock()
	b.cancels = append(b.cancels, cancel)
	b.clients = append(b.clients, cl)
	b.mu.Unlock()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			fetches := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			fetches.EachError(func(topic string, _ int32, err error) {
				b.log.Error("kafka fetch error", "topic", topic, "err", err)
			})
			fetches.EachRecord(func(r *kgo.Record) {
				var e Event
				if err := json.Unmarshal(r.Value, &e); err != nil {
					b.log.Error("bad event payload", "topic", r.Topic, "err", err)
					return
				}
				if err := h(ctx, e); err != nil {
					b.log.Error("handler error", "topic", r.Topic, "err", err)
				}
			})
		}
	}()
}

// Close stops all consumers and the producer.
func (b *KafkaBus) Close() error {
	b.mu.Lock()
	for _, c := range b.cancels {
		c()
	}
	clients := append([]*kgo.Client(nil), b.clients...)
	b.mu.Unlock()

	b.wg.Wait()
	for _, cl := range clients {
		cl.Close()
	}
	b.producer.Close()
	return nil
}
