package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// Handler processes a single event.
type Handler func(ctx context.Context, e Event) error

// Bus is the event-driven backbone shared by all services. Each service runs as
// its own process, so cross-service events require a shared broker: the Kafka
// implementation (see kafka.go) is used in deployment, while InMemoryBus below
// serves single-process local development and tests.
type Bus interface {
	Publish(ctx context.Context, e Event) error
	PublishBatch(ctx context.Context, es []Event) error
	Subscribe(t Type, h Handler)
	Close() error
}

// NewBus constructs the bus selected by kind ("memory" or "kafka"). group names
// the consumer group prefix used by the Kafka implementation (typically the
// service name).
func NewBus(kind string, seeds []string, group string, log *slog.Logger) (Bus, error) {
	switch kind {
	case "kafka":
		return NewKafkaBus(seeds, group, log)
	case "", "memory":
		return NewInMemoryBus(), nil
	default:
		return nil, fmt.Errorf("unknown bus kind %q", kind)
	}
}

// InMemoryBus is a simple synchronous, in-process Bus for scaffolding and tests.
type InMemoryBus struct {
	mu       sync.RWMutex
	handlers map[Type][]Handler
}

// NewInMemoryBus returns an empty in-process bus.
func NewInMemoryBus() *InMemoryBus {
	return &InMemoryBus{handlers: make(map[Type][]Handler)}
}

// Subscribe registers h to receive events of type t.
func (b *InMemoryBus) Subscribe(t Type, h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[t] = append(b.handlers[t], h)
}

// Publish delivers e to every subscribed handler, aggregating their errors.
func (b *InMemoryBus) Publish(ctx context.Context, e Event) error {
	b.mu.RLock()
	hs := append([]Handler(nil), b.handlers[e.Type]...)
	b.mu.RUnlock()

	var errs []error
	for _, h := range hs {
		if err := h(ctx, e); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// PublishBatch delivers each event in turn (in-process; no real batching win).
func (b *InMemoryBus) PublishBatch(ctx context.Context, es []Event) error {
	var errs []error
	for _, e := range es {
		if err := b.Publish(ctx, e); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close releases bus resources. No-op for the in-memory implementation.
func (b *InMemoryBus) Close() error { return nil }
