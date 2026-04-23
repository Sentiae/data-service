// Package messaging wires the data-service to the shared platform-kit Kafka
// publisher. It exposes a thin wrapper so the rest of the service can depend
// on a single, narrow type regardless of the underlying transport.
package messaging

import (
	"log"

	"github.com/sentiae/platform-kit/kafka"
)

// Publisher is the publisher interface used by the data-service. It is an
// alias for the platform-kit Publisher to keep the dependency explicit while
// allowing local tests to swap in a noop implementation.
type Publisher = kafka.Publisher

// Config holds the minimum settings required to build a Kafka publisher.
type Config struct {
	Enabled     bool
	Brokers     []string
	TopicPrefix string
}

// NewKafkaPublisher builds a real Kafka publisher. Callers should fall back
// to NewNoopPublisher when Kafka is disabled or initialization fails.
func NewKafkaPublisher(cfg Config) (Publisher, error) {
	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:     cfg.Brokers,
		Source:      "data-service",
		TopicPrefix: cfg.TopicPrefix,
	})
	if err != nil {
		return nil, err
	}
	return pub, nil
}

// NewNoopPublisher returns a publisher that silently drops all events. Useful
// for local development where Kafka is not running.
func NewNoopPublisher() Publisher {
	return kafka.NewNoopPublisher()
}

// InitFromEnv inspects the provided config and returns either a real Kafka
// publisher or a noop fallback. Errors during real publisher construction are
// logged but never fatal — the service keeps running with the noop.
func InitFromEnv(cfg Config) Publisher {
	if !cfg.Enabled || len(cfg.Brokers) == 0 {
		log.Println("data-service: Kafka disabled, using noop publisher")
		return NewNoopPublisher()
	}
	pub, err := NewKafkaPublisher(cfg)
	if err != nil {
		log.Printf("data-service: failed to initialize Kafka publisher (%v); falling back to noop", err)
		return NewNoopPublisher()
	}
	log.Printf("data-service: Kafka publisher initialized (brokers=%v)", cfg.Brokers)
	return pub
}
