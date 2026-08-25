package main

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

// EventPublisher keeps event delivery infrastructure outside the HTTP handlers.
// Production publishes to Kafka; tests can inject a controlled test implementation.
type EventPublisher interface {
	PublishShipmentCreated(event ShipmentCreatedEvent) error
}

// NoOpEventPublisher is used when event publication should have no side effects.
type NoOpEventPublisher struct {
}

type KafkaEventPublisher struct {
	writer *kafka.Writer
}

func NewKafkaEventPublisher(brokerAddress string, topic string) *KafkaEventPublisher {
	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokerAddress),
		Topic: topic,
	}

	return &KafkaEventPublisher{
		writer: writer,
	}
}

func (p *NoOpEventPublisher) PublishShipmentCreated(event ShipmentCreatedEvent) error {
	return nil
}

func (p *KafkaEventPublisher) PublishShipmentCreated(event ShipmentCreatedEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Use shipment_id as the message key so events for the same shipment
	// can be routed to the same partition and preserve their relative order.
	message := kafka.Message{
		Key:   []byte(event.ShipmentID),
		Value: value,
	}

	return p.writer.WriteMessages(
		context.Background(),
		message,
	)
}

func (p *KafkaEventPublisher) Close() error {
	return p.writer.Close()
}
