// Package config loads service settings from the environment.
package config

import (
	"os"
	"strings"
)

// Config holds service-wide settings.
type Config struct {
	// HTTPAddr is the listen address for the service's HTTP server.
	HTTPAddr string

	// BusKind selects the event bus implementation: "memory" or "kafka".
	BusKind string
	// KafkaSeeds is the list of broker addresses used when BusKind == "kafka".
	KafkaSeeds []string

	// MetronomeBaseURL is the base URL of the Metronome API (the local
	// fakemetronome by default, or the real API).
	MetronomeBaseURL string
	// StripeBaseURL is the base URL of the Stripe API (the local fakestripe by
	// default, or real Stripe test mode).
	StripeBaseURL string
	// MeteringServiceURL is the base URL of the metering service, which the
	// invoicing service queries for usage+price at cycle end.
	MeteringServiceURL string
	// BillingURL is the base URL of the billing service, which the generator
	// uses to validate a process's customer against the registry.
	BillingURL string
	// InvoicingURL is the base URL of the invoicing service, which the control
	// plane calls to create/cancel flat-fee subscriptions.
	InvoicingURL string

	// StripeWebhookURL is where fakestripe delivers subscription lifecycle
	// webhooks (the invoicing service). StripeWebhookSecret is a shared secret
	// sent/verified on those webhooks; empty disables verification.
	StripeWebhookURL    string
	StripeWebhookSecret string

	// Placeholder credentials for the external providers. Empty for now.
	StripeAPIKey    string
	MetronomeAPIKey string
}

// Load reads configuration from the environment, applying defaults.
// prefix scopes the per-service HTTP address (e.g. "BILLING" -> BILLING_HTTP_ADDR),
// and defaultAddr is used when that variable is unset.
func Load(prefix, defaultAddr string) Config {
	return Config{
		HTTPAddr:            env(prefix+"_HTTP_ADDR", defaultAddr),
		BusKind:             env("BUS", "memory"),
		KafkaSeeds:          splitSeeds(env("KAFKA_SEEDS", "localhost:9092")),
		MetronomeBaseURL:    env("METRONOME_BASE_URL", "http://localhost:8083"),
		StripeBaseURL:       env("STRIPE_BASE_URL", "http://localhost:8084"),
		MeteringServiceURL:  env("METERING_SERVICE_URL", "http://localhost:8082"),
		BillingURL:          env("BILLING_URL", "http://localhost:8080"),
		InvoicingURL:        env("INVOICING_URL", "http://localhost:8081"),
		StripeWebhookURL:    env("STRIPE_WEBHOOK_URL", "http://localhost:8081"),
		StripeWebhookSecret: os.Getenv("STRIPE_WEBHOOK_SECRET"),
		StripeAPIKey:        os.Getenv("STRIPE_API_KEY"),
		MetronomeAPIKey:     os.Getenv("METRONOME_API_KEY"),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitSeeds(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
