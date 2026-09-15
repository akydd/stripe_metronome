// Package events defines the event-driven backbone shared by every service.
//
// Services never call each other directly for the billing pipeline; they publish
// and subscribe to the domain events declared here. This keeps the stripe,
// metronome, and billing services decoupled and independently deployable.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

// Type identifies the kind of domain event flowing across services.
// Each Type is also used verbatim as the Kafka topic name, so the broker's
// topic list reads like the architecture: usage.ingested, billing_cycle.ended, ...
type Type string

const (
	// UsageIngested: raw usage received from a product/app (generator -> bus).
	UsageIngested Type = "usage.ingested"
	// SubscriptionActivated / SubscriptionCancelled: a flat-fee subscription was
	// started/stopped (generator -> bus); metronome registers/cancels it.
	SubscriptionActivated Type = "subscription.activated"
	SubscriptionCancelled Type = "subscription.cancelled"
	// BillingCycleEnded: a customer's billing cycle has closed (billing -> bus).
	// Stripe reacts by pulling the period's usage+price from Metronome and
	// invoicing it.
	BillingCycleEnded Type = "billing_cycle.ended"
	// InvoiceFinalized: Stripe created + finalized the invoice (stripe -> bus).
	InvoiceFinalized Type = "invoice.finalized"
	// PaymentSucceeded / PaymentFailed: payment outcome (stripe -> bus).
	PaymentSucceeded Type = "payment.succeeded"
	PaymentFailed    Type = "payment.failed"
)

// Event is the envelope carried on the bus. Payload holds the type-specific body.
type Event struct {
	ID         string          `json:"id"`
	Type       Type            `json:"type"`
	CustomerID string          `json:"customer_id,omitempty"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// NewID returns a random hex identifier for a new event.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
